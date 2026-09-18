"""
Per-item / per-asset analysis of a STAC `ItemCollection`.

Iterates over the collected STAC Items and their band Assets to determine
per-asset projection metadata (EPSG, bbox, shape/resolution), which bands each
asset contributes, pixel value scale/offset, and datatype/nodata; tracks
resolutions and EPSGs for later target-CRS/cell-size determination; and
collects all of it into a plain `AssetTable`, one `AssetTableItem` per
surviving STAC Item.

Main entry point: `build_asset_table`. Contains no JVM/GeoPySpark dependency,
so it can be unit-tested without a Spark context; translating an `AssetTable`
into a JVM raster-loading representation is `openeogeotrellis.load_stac`'s job.
"""
from __future__ import annotations

import collections
import dataclasses
import enum
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

import pystac
import shapely.geometry

from openeo.metadata import _StacMetadataParser
from openeo_driver.util.geometry import BoundingBox

from openeogeotrellis.stac.extents import SpatioTemporalExtent
from openeogeotrellis.stac.item_collection import ItemCollection
from openeogeotrellis.stac.projection import compute_cellsize, get_asset_property, get_proj_metadata
from openeogeotrellis.util.datastructures import NoveltyTracker
from openeogeotrellis.util.projection import is_utm_epsg_code

logger = logging.getLogger(__name__)


class PixelValueScalingMode(enum.Enum):
    """
    Modes of how to handle pixel value scaling
    based on raster:scale and raster:offset metadata
    """

    # Legacy default mode: no pixel scaling (keep digital number)
    NO_SCALING = "NO_SCALING"

    # Special (legacy) Sentinel 2 Reflectance mode:
    # just offset value with ratio of raster:offset and raster:scale
    S2_REFLECTANCE_SCALED_OFFSET = "S2_REFLECTANCE_SCALED_OFFSET"

    # Normal mode: apply scale and offset to convert to physical quantities
    SCALE_AND_OFFSET = "SCALE_AND_OFFSET"


class ResolutionTracker:
    """
    Track resolution (cell size in a CRS), keyed on something (e.g. asset id, band, ...)
    and allow to determine the finest one afterwards based on subselection
    """

    def __init__(self):
        # Mapping of key -> set of (epsg_code, resolution) tuples (with resolution as (x_size, y_size) tuple)
        self._resolutions: Dict[str, Set[Tuple[int, Tuple[float, float]]]] = collections.defaultdict(set)

    def track(self, *, key: str = "_default", epsg: int, res: Tuple[float, float]):
        self._resolutions[key].add((epsg, res))

    def finest_for(self, keys: Iterable[str] = ("_default",)) -> Tuple[Set[int], Optional[Tuple[float, float]]]:
        """
        Determine finest resolution (smallest cell size) associated with the given keys,
        but only if that makes sense: there is just a single EPSG code in play
        or all are UTM zones.

        return set of EPSG codes and finest resolution (if any)
        """
        selection = set(r for k in keys if k in self._resolutions for r in self._resolutions[k])

        epsgs = set(epsg for (epsg, _) in selection)
        if selection and (len(epsgs) == 1 or all(is_utm_epsg_code(e) for e in epsgs)):
            finest_res = min(res for (_, res) in selection)
        else:
            finest_res = None

        return epsgs, finest_res


def get_pixel_value_scaling_mode(*, feature_flags: dict, url: str) -> PixelValueScalingMode:
    """
    Determine pixel value scaling mode from feature flags or STAC URL

    :param feature_flags: feature flags from collection metadata
    :param url: STAC URL
    """
    if feature_flags.get("apply_sentinel2_reflectance_offset"):
        return PixelValueScalingMode.S2_REFLECTANCE_SCALED_OFFSET

    if feature_flags.get("apply_raster_scale_and_offset"):
        return PixelValueScalingMode.SCALE_AND_OFFSET

    # Guess mode from STAC url
    # TODO: possible to eliminate the need for this ad-hoc URL-based guessing? E.g. discover from STAC metadata itself?
    if any(
        [
            re.match(r"^https?://stac\.dataspace\.copernicus\.eu/v\d+/collections/sentinel-2-l[12][ac]", url),
            re.match(r"^https?://stac\.terrascope\.be/collections/terrascope-s2-toc-v\d+", url),
            url == "https://stac.test/collections/sentinel-2-l2a",
        ]
    ):
        logger.warning(f"Inferred S2_REFLECTANCE_SCALED_OFFSET mode from URL {url=}.")
        return PixelValueScalingMode.S2_REFLECTANCE_SCALED_OFFSET

    # For now, default to legacy mode: no scaling
    # TODO: make this default configurable?
    return PixelValueScalingMode.NO_SCALING


def _is_sentinel2_reflectance_asset(asset: pystac.Asset) -> bool:
    """
    Helper to determine if the given asset is a Sentinel-2 reflectance asset,
    based on the presence of "eo:center_wavelength" band metadata.
    """
    if bands := asset.extra_fields.get("bands"):
        return any("eo:center_wavelength" in b for b in bands)
    elif bands := asset.extra_fields.get("eo:bands"):
        return any("center_wavelength" in b for b in bands)
    return False


def _get_raster_scale_and_offset(*, item: pystac.Item, asset: pystac.Asset) -> Tuple[float, float]:
    """
    Get raster:scale and raster:offset metadata from asset or its parent item
    """
    # TODO: get parent item through asset.owner, instead of expecting caller to provide it
    raster_scale = asset.extra_fields.get("raster:scale", item.properties.get("raster:scale", 1.0))
    raster_offset = asset.extra_fields.get("raster:offset", item.properties.get("raster:offset", 0.0))
    return raster_scale, raster_offset


def _get_pixel_value_scale_and_offset(
    *, asset: pystac.Asset, item: pystac.Item, pixel_value_scaling_mode: PixelValueScalingMode
) -> Tuple[float, float]:
    """
    Get pixel value scale and offset based on:
    raster:scale, raster:offset metadata and pixel value scaling mode
    """
    if pixel_value_scaling_mode == PixelValueScalingMode.SCALE_AND_OFFSET:
        raster_scale, raster_offset = _get_raster_scale_and_offset(item=item, asset=asset)
        pixel_value_scale, pixel_value_offset = raster_scale, raster_offset
    elif pixel_value_scaling_mode == PixelValueScalingMode.S2_REFLECTANCE_SCALED_OFFSET:
        if _is_sentinel2_reflectance_asset(asset=asset):
            raster_scale, raster_offset = _get_raster_scale_and_offset(item=item, asset=asset)
            pixel_value_scale, pixel_value_offset = 1.0, raster_offset / raster_scale
        else:
            pixel_value_scale, pixel_value_offset = 1.0, 0.0
    elif pixel_value_scaling_mode == PixelValueScalingMode.NO_SCALING:
        # Legacy mode: don't apply scale nor offset
        pixel_value_scale, pixel_value_offset = 1.0, 0.0
    else:
        logger.warning(f"Unknown {pixel_value_scaling_mode=}, defaulting to no scaling.")
        pixel_value_scale, pixel_value_offset = 1.0, 0.0
    return pixel_value_scale, pixel_value_offset


def _get_datatype_and_no_data(asset: pystac.Asset) -> Tuple[Optional[str], Optional[float]]:
    nodata = get_asset_property(asset, "nodata")
    datatype = get_asset_property(asset, "data_type")
    return datatype, nodata


def get_best_url(
    asset: pystac.Asset,
    *,
    with_vsis3: bool = True,
    preferred_url_prefix: Optional[str] = None,
    use_raw_asset_href: bool = False,
) -> str:
    """
    Relevant doc: https://github.com/stac-extensions/alternate-assets
    """

    if use_raw_asset_href:
        return asset.href

    for key, alternate_asset in asset.extra_fields.get("alternate", {}).items():
        href = alternate_asset["href"]
        if preferred_url_prefix and href.lower().startswith(preferred_url_prefix.lower()):
            return href
        if key in {"local", "s3"}:
            # Checking if file exists takes around 10ms on /data/MTDA mounted on laptop
            # Checking if URL exists takes around 100ms on https://services.terrascope.be
            # Checking if URL exists depends also on what Datasource is used in the scala code.
            # That would be hacky to predict here.
            url = urlparse(href)
            # Support paths like "file:///data/MTDA", but also "//data/MTDA" just in case.

            file_path = None
            if url.scheme in ["", "file"]:
                file_path = url.path
            elif url.scheme == "s3":
                file_path = f"/{url.netloc}{url.path}"

            if file_path and Path(file_path).exists():
                logger.debug(f"Using local alternate file path {file_path}")
                return file_path
            else:
                logger.warning(f"Only support file paths as local alternate urls, but found {href}")

    href = asset.get_absolute_href() or asset.href

    # TODO: this vsis3 upper-lower-case juggling should be moved to geotrellis extensions instead of this undocumented coupling (and hardcoded deployment details)
    return (
        href.replace("s3://eodata/", "/vsis3/eodata/")
        if (with_vsis3 and os.environ.get("AWS_DIRECT") == "TRUE")
        else href.replace("s3://eodata/", "/eodata/")
    )


@dataclasses.dataclass
class AssetLink:
    """One band-asset's worth of decisions, ready to become one `addLink(...)` call."""

    href: str
    asset_id: str
    pixel_value_scale: float
    pixel_value_offset: float
    band_names: List[str]
    data_type: Optional[str] = None
    nodata: Optional[float] = None


@dataclasses.dataclass
class MetadataAssetLink:
    """A non-band asset link (e.g. `granule_metadata`, geometry): no pixel scaling or datatype."""

    href: str
    asset_id: str
    band_names: List[str]


@dataclasses.dataclass
class AssetTableItem:
    """All decisions made for one STAC Item, ready to be translated into one raster-loading feature."""

    item_id: str
    collection_id: str
    nominal_date: str
    links: List[AssetLink]
    metadata_links: List[MetadataAssetLink]
    crs_epsg: Optional[int]
    raster_extent: Optional[Tuple[float, float, float, float]]
    resolution: Optional[float]
    bbox_wsen: Optional[Tuple[float, float, float, float]]
    geometry_wkt: Optional[str]
    self_url: Optional[str]


@dataclasses.dataclass
class AssetTable:
    """
    Plain, engine-agnostic result of analyzing an `ItemCollection`'s band assets:
    one `AssetTableItem` per surviving STAC Item, plus the accumulated
    per-band resolution/EPSG tracking and overall bounding box.

    No `pyspark`/`py4j`/JVM dependency: a consumer translates this into whatever
    engine-specific raster-loading representation it needs
    (e.g. `openeogeotrellis.load_stac.build_opensearch_features`).
    """

    items: List[AssetTableItem]
    opensearch_link_titles_map: Dict[str, str]
    resolution_tracker: "ResolutionTracker"
    observed_epsgs: Set[int]
    stac_bbox: Optional[BoundingBox]
    asset_band_names: Optional[List[str]]
    collected_link_band_names: Set[str]


def build_asset_table(
    *,
    item_collection: ItemCollection,
    band_selection: Optional[List[str]],
    available_band_names: List[str],
    spatiotemporal_extent: SpatioTemporalExtent,
    pixel_value_scaling_mode: PixelValueScalingMode,
    use_raw_asset_href: bool,
    feature_flags: Dict[str, Any],
) -> AssetTable:
    """
    Iterate over all items and their band assets to:
    - select which bands each asset contributes, and under what band names
    - determine pixel value scale/offset and datatype/nodata per asset
    - determine per-item CRS/raster-extent/resolution/bbox/geometry
    - track per-band resolution and EPSG info
    - collect the overall STAC bounding box

    Pure decision logic: no JVM/engine-specific object is built or touched here.
    """
    opensearch_link_titles_map: Dict[str, str] = {}
    opensearch_stats = collections.defaultdict(int)

    asset_band_names = None
    stac_bbox = None
    proj_epsg = None
    proj_bbox = None
    proj_shape = None
    asset_table_items: List[AssetTableItem] = []

    stac_metadata_parser = _StacMetadataParser(logger=logger)
    resolution_tracker = ResolutionTracker()
    observed_epsgs: Set[int] = set()
    collected_link_band_names: Set[str] = set()

    # layercatalog feature flag to handle "granule_metadata" assets.
    # E.g. for azimuth/zenith "bands" in SENTINEL2_L2A:
    #     {
    #         "sunAzimuthAngles": "granule_metadata##0",
    #         "sunZenithAngles": "granule_metadata##1",
    #         ...
    granule_metadata_band_map = feature_flags.get("granule_metadata_band_map")
    if granule_metadata_band_map:
        # Add "granule_metadata" based bands (as they were probably not generically declared in STAC metadata).
        # TODO: possible to move this logic to the level of `construct_item_collection`/`StacMetadataParser`?
        available_band_names.extend(b for b in granule_metadata_band_map.keys() if b not in available_band_names)

    cellsize_override = feature_flags.get("cellsize_override")
    fix_proj_transform = feature_flags.get("fix_proj_transform", False)
    skipped_assets = feature_flags.get("skipped_assets", [])
    preferred_url_prefix = feature_flags.get("preferred_url_prefix")

    logger.info(f"Building asset table for {len(item_collection.items)} items (band_selection={band_selection})")
    for itm, band_assets in item_collection.iter_items_with_band_assets():
        opensearch_stats["items"] += 1
        opensearch_stats[f"items with {len(band_assets)=}"] += 1

        item_nominal_date = itm.properties.get("datetime") or itm.properties["start_datetime"]
        links: List[AssetLink] = []
        metadata_links: List[MetadataAssetLink] = []

        band_names_tracker = NoveltyTracker()
        for asset_id, asset in sorted(
            # Go through assets ordered by asset GSD (from finer to coarser) if possible,
            # falling back on deterministic alphabetical asset_id order.
            # see https://github.com/Open-EO/openeo-geopyspark-driver/pull/1213#discussion_r2107353442
            # TODO: move this sorting feature inside iter_items_with_band_assets
            band_assets.items(),
            key=lambda kv: (
                float(kv[1].extra_fields.get("gsd") or itm.properties.get("gsd") or 40e6),
                kv[0],
            ),
        ):
            if asset_id in skipped_assets:
                continue
            opensearch_stats["assets"] += 1

            proj_epsg, proj_bbox, proj_shape = get_proj_metadata(
                asset=asset, item=itm, fix_proj_transform=fix_proj_transform
            )
            opensearch_stats[f"assets with {proj_epsg=}"] += 1
            if proj_epsg:
                observed_epsgs.add(proj_epsg)

            asset_band_names_from_metadata: List[str] = stac_metadata_parser.bands_from_stac_asset(asset=asset).band_names()
            opensearch_stats[f"assets with {len(asset_band_names_from_metadata)=}"] += 1

            if not asset_band_names_from_metadata:
                asset_band_names_from_metadata = feature_flags.get("asset_id_to_bands_map", {}).get(asset_id, [])
                logger.debug(f"using `asset_id_to_bands_map`: mapping {asset_id} to {asset_band_names_from_metadata}")
            logger.debug(f"from intersecting_items: {itm.id=} {asset_id=} {asset_band_names_from_metadata=}")

            if not band_selection:
                # No user-specified band filtering: follow band names from metadata (if possible)
                asset_band_names = asset_band_names_from_metadata or [asset_id]
            elif set(asset_band_names_from_metadata).intersection(band_selection or []):
                # User-specified bands match with band names in metadata
                asset_band_names = asset_band_names_from_metadata
            elif isinstance(band_selection, list) and asset_id in band_selection:
                # User-specified asset_id as band name: use that directly
                if asset_id not in available_band_names and asset_id not in collected_link_band_names:
                    logger.warning(f"Using {asset_id=} as band name (while not in {available_band_names=}).")
                asset_band_names = [asset_id]
            else:
                # No match with band_selection in some way -> skip this asset
                continue

            opensearch_stats[f"assets with {len(asset_band_names)=}"] += 1

            if band_names_tracker.already_seen(sorted(asset_band_names)):
                # We've already seen this set of bands (e.g. at finer GSD), so skip this asset.
                continue

            if proj_epsg and proj_bbox and proj_shape:
                asset_cell_size = compute_cellsize(proj_bbox, proj_shape)
                for asset_band_name in asset_band_names:
                    resolution_tracker.track(key=asset_band_name, epsg=proj_epsg, res=asset_cell_size)

            pixel_value_scale, pixel_value_offset = _get_pixel_value_scale_and_offset(
                asset=asset, item=itm, pixel_value_scaling_mode=pixel_value_scaling_mode
            )
            asset_href = get_best_url(
                asset=asset,
                preferred_url_prefix=preferred_url_prefix,
                use_raw_asset_href=use_raw_asset_href,
            )
            logger.debug(
                f"AssetLink {itm.id=} {asset_id=} {asset_href=} {asset_band_names_from_metadata=} {asset_band_names=}"
                f" {pixel_value_scale=} {pixel_value_offset=}"
            )

            opensearch_stats["links"] += 1
            data_type, nodata = _get_datatype_and_no_data(asset=asset)
            if data_type is not None and data_type == "uint32":
                data_type = "float64"

            links.append(
                AssetLink(
                    href=asset_href,
                    asset_id=asset_id,
                    pixel_value_scale=float(pixel_value_scale),
                    pixel_value_offset=float(pixel_value_offset),
                    band_names=asset_band_names,
                    data_type=data_type,
                    nodata=float(nodata) if data_type is not None and nodata is not None else None,
                )
            )

            collected_link_band_names.update(asset_band_names)

        # Optionally include additional special assets
        for asset_id, asset in itm.assets.items():
            # "granule_metadata" with S2 azimuth/zenit angle data
            if (
                granule_metadata_band_map
                # TODO: less strict checking for wider applicability?
                and asset_id == "granule_metadata"
                and asset.title == "MTD_TL.xml"
                and "metadata" in (asset.roles or [])
                and (asset_href := get_best_url(asset, with_vsis3=False, preferred_url_prefix=preferred_url_prefix)).endswith("/MTD_TL.xml")
                and (not band_selection or set(band_selection).intersection(granule_metadata_band_map.keys()))
            ):
                # TODO: avoid ad-hoc `sorted` and make sure granule_metadata_band_map has intrinsic/intended order from the start
                link_band_names = sorted(granule_metadata_band_map.values())
                opensearch_link_titles_map.update(granule_metadata_band_map)
                logger.debug(
                    f"MetadataAssetLink {itm.id=} {asset_id=} {asset_href=} {link_band_names=} from {granule_metadata_band_map=}"
                )
                opensearch_stats["links"] += 1
                metadata_links.append(MetadataAssetLink(href=asset_href, asset_id=asset_id, band_names=link_band_names))
            # ProbaV Geometry asset
            elif (
                granule_metadata_band_map
                and asset_id == "GEOMETRY"
            ):
                asset_href = get_best_url(asset, with_vsis3=False, preferred_url_prefix=preferred_url_prefix)
                link_band_names = sorted(granule_metadata_band_map.values())
                opensearch_link_titles_map.update(granule_metadata_band_map)
                logger.debug(
                    f"MetadataAssetLink {itm.id=} {asset_id=} {asset_href=} {link_band_names=} from {granule_metadata_band_map=}"
                )
                opensearch_stats["links"] += 1
                metadata_links.append(MetadataAssetLink(href=asset_href, asset_id=asset_id, band_names=link_band_names))

        # Skip item if no assets/links were collected
        link_count = len(links) + len(metadata_links)
        opensearch_stats[f"item with {link_count=}"] += 1
        if link_count == 0:
            opensearch_stats["item skip: no links"] += 1
            continue

        # TODO: the proj_* values are assigned in inner per-asset loop,
        #       so the values here are ill-defined (the values might even come from another item)

        item_bbox_is_implausible = (
            # Implausible bounding box: far too wide longitude span for UTM item/assets,
            # likely due poor antimeridian handling.
            itm.bbox
            and proj_epsg
            and is_utm_epsg_code(proj_epsg)
            and itm.bbox[2] - itm.bbox[0] > 180
        )
        if item_bbox_is_implausible:
            opensearch_stats["implausible bbox"] += 1
            # Check other metadata for match with spatial extent to decide to keep or skip item.
            # TODO: hopefully this special handling can be eliminated once STAC API implementations mature.
            try:
                if proj_bbox:
                    fallback_geometry = BoundingBox.from_wsen_tuple(proj_bbox, crs=proj_epsg)
                elif itm.geometry:
                    # TODO: check if geometry is valid wrt antimeridian handling?
                    fallback_geometry = shapely.geometry.shape(itm.geometry)
                else:
                    fallback_geometry = None
            except Exception as e:
                logger.error(
                    f"Failed to obtain fallback geometry for {itm.id!r} with implausible bbox {itm.bbox!r}",
                    exc_info=True,
                )
                fallback_geometry = None

            if fallback_geometry and spatiotemporal_extent.spatial_extent.intersects(fallback_geometry):
                opensearch_stats["implausible bbox: keep"] += 1
                logger.warning(
                    f"Detected implausible bbox {itm.bbox!r} in item {itm.id!r} ({proj_epsg=} {proj_bbox=} {fallback_geometry=})"
                )
            else:
                opensearch_stats["implausible bbox: skip"] += 1
                logger.warning(
                    f"Skipping item {itm.id!r} with implausible bbox {itm.bbox!r} ({proj_epsg=} {proj_bbox=} {fallback_geometry=})"
                )
                continue

        raster_extent = tuple(float(b) for b in proj_bbox) if proj_bbox else None

        resolution = None
        if proj_bbox and proj_shape:
            cell_width, cell_height = cellsize_override or compute_cellsize(proj_bbox, proj_shape)
            resolution = cell_width

        if proj_bbox and proj_epsg:
            item_bbox = BoundingBox.from_wsen_tuple(proj_bbox, crs=proj_epsg)
            latlon_bbox = item_bbox.reproject(4326)
        elif itm.bbox and not item_bbox_is_implausible:
            item_bbox = latlon_bbox = BoundingBox.from_wsen_tuple(itm.bbox, 4326)
        else:
            latlon_bbox = item_bbox = None

        bbox_wsen = None
        if latlon_bbox is not None:
            w, s, e, n = latlon_bbox.as_wsen_tuple()
            if e < w:
                # Workaround for `withBBox` not properly supporting bounding boxes across antimeridian
                e += 360
            opensearch_stats["bbox_wsen"] += 1
            bbox_wsen = (float(w), float(s), float(e), float(n))

        geometry_wkt = None
        if itm.geometry is not None:
            opensearch_stats["geometry_wkt"] += 1
            geometry_wkt = str(shapely.geometry.shape(itm.geometry))

        self_url = None
        self_links = itm.get_links(rel="self")
        if self_links and (self_url := self_links[0].get_href(transform_href=False)):
            opensearch_stats["self_url"] += 1

        logger.debug(f"AssetTableItem {itm.id=}")
        asset_table_items.append(
            AssetTableItem(
                item_id=itm.id,
                collection_id=itm.collection_id,
                nominal_date=item_nominal_date,
                links=links,
                metadata_links=metadata_links,
                crs_epsg=proj_epsg,
                raster_extent=raster_extent,
                resolution=resolution,
                bbox_wsen=bbox_wsen,
                geometry_wkt=geometry_wkt,
                self_url=self_url,
            )
        )
        opensearch_stats["items kept"] += 1

        if item_bbox:
            stac_bbox = (
                item_bbox
                if stac_bbox is None
                else BoundingBox.from_wsen_tuple(
                    item_bbox.as_polygon().union(stac_bbox.as_polygon()).bounds, stac_bbox.crs
                )
            )

    opensearch_stats = dict(sorted(opensearch_stats.items()))
    logger.info(f"{opensearch_stats=}")

    return AssetTable(
        items=asset_table_items,
        opensearch_link_titles_map=opensearch_link_titles_map,
        resolution_tracker=resolution_tracker,
        observed_epsgs=observed_epsgs,
        stac_bbox=stac_bbox,
        asset_band_names=asset_band_names,
        collected_link_band_names=collected_link_band_names,
    )
