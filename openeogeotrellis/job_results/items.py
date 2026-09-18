"""
Item/asset dict construction for ``GeopysparkDataCube.write_assets``.

Turns the raw output of the GeoTrellis/GeoPySpark writers -- expressed here
as plain ``WrittenItem``/``WrittenAsset`` values -- into the STAC-like
item/asset dicts ``write_assets`` has always returned. Every output shape
(one per combination of format/stitch/tile_grid/batch_mode/sample_by_feature),
including its small format-specific inconsistencies (e.g. which variant sets
``properties.datetime``, or drops empty ``band_indices`` differently), is
kept unchanged.
"""
import os
import pathlib
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

from openeo.util import dict_no_none
from openeo_driver.errors import OpenEOApiException
from openeo_driver.utils import smart_bool
from shapely.geometry import box, mapping
from shapely.geometry.polygon import Polygon

from .util import GDALINFO_SUFFIX, reproject_geometry

_SUPPORTED_FORMATS = ("GTIFF", "PNG", "NETCDF", "JSON", "ZARR", "DEBUG_GENERAL")


@dataclass(frozen=True)
class SaveResultFormatOptions:
    """Parsed and validated ``format``/``format_options`` for ``write_assets``."""

    format: str
    tiled: bool = False
    stitch: bool = False
    tile_grid: Optional[str] = None
    sample_by_feature: bool = False
    feature_id_property: Optional[str] = None
    batch_mode: bool = False
    overviews: str = "AUTO"
    overview_resample: str = "near"
    colormap: Optional[dict] = None
    description: str = ""
    filename_prefix: Optional[str] = None
    separate_asset_per_band: bool = False
    bands_metadata: dict = field(default_factory=dict)
    file_metadata: dict = field(default_factory=dict)
    attach_gdalinfo_assets: bool = False
    retain_nodata_tiles: bool = False
    filepath_per_band: Optional[List[str]] = None
    strict_cropping: bool = True
    compression: str = "deflate"
    predictor: int = 1
    zlevel: int = 6
    tile_size: Optional[int] = None
    bigtiff: bool = True
    add_bands_statistics: bool = False
    to_zip: bool = True

    @staticmethod
    def parse(format: str, format_options: Optional[dict], *, has_temporal_dimension: bool) -> "SaveResultFormatOptions":
        """
        Parse and validate ``format``/``format_options``, raising the same
        ``OpenEOApiException``s (same messages) as the original inline checks.

        Only the checks that don't depend on the cube's actual layer type
        (spatial vs. temporal, which can change during writing) are done
        eagerly here. ``validate_tile_grid_with_separate_asset_per_band``,
        ``validate_filepath_per_band_with_temporal_dimension`` and
        ``validate_sample_by_feature_with_separate_asset_per_band`` must be
        called from the exact spots the original code checked them, to avoid
        rejecting combinations that used to be silently ignored (e.g.
        ``filepath_per_band`` combined with ``stitch``).
        """
        format = format.upper()
        format_options = format_options or {}
        file_metadata = format_options.get("file_metadata", {})
        attach_gdalinfo_assets = format_options.get("attach_gdalinfo_assets", False)
        separate_asset_per_band = smart_bool(format_options.get("separate_asset_per_band", False))

        if format not in _SUPPORTED_FORMATS:
            raise OpenEOApiException(
                message="Format {f!r} is not supported".format(f=format), code="FormatUnsupported", status_code=400
            )
        if attach_gdalinfo_assets and format != "GTIFF":
            raise OpenEOApiException(f"attach_gdalinfo_assets is only supported with format GTIFF. Was: {format}")
        if separate_asset_per_band and format != "GTIFF":
            raise OpenEOApiException(f"separate_asset_per_band is only supported with format GTIFF. Was: {format}")

        options = SaveResultFormatOptions(
            format=format,
            tiled=format_options.get("tiled", False),
            stitch=format_options.get("stitch", False),
            tile_grid=format_options.get("tile_grid", None),
            sample_by_feature=format_options.get("sample_by_feature", False),
            feature_id_property=format_options.get("feature_id_property", None),
            batch_mode=format_options.get("batch_mode", False),
            overviews=format_options.get("overviews", "AUTO"),
            overview_resample=format_options.get("overview_method", "near"),
            colormap=format_options.get("colormap", None),
            description=file_metadata.get("description", ""),
            filename_prefix=format_options.get("filename_prefix", None),
            separate_asset_per_band=separate_asset_per_band,
            bands_metadata=format_options.get("bands_metadata", {}),
            file_metadata=file_metadata,
            attach_gdalinfo_assets=attach_gdalinfo_assets,
            retain_nodata_tiles=format_options.get("retain_nodata_tiles", False),
            filepath_per_band=format_options.get("filepath_per_band", None),
            strict_cropping=format_options.get("strict_cropping", True),
            compression=format_options.get("compression", "deflate"),
            predictor=format_options.get("predictor", 1),
            zlevel=format_options.get("ZLEVEL", 6),
            tile_size=format_options.get("tile_size"),
            bigtiff=format_options.get("bigtiff", True),
            add_bands_statistics=format_options.get("add_bands_statistics", False),
            to_zip=format_options.get("to_zip", True),
        )

        if options.format == "GTIFF" and not options.stitch:
            options.validate_filepath_per_band_with_temporal_dimension(has_temporal_dimension=has_temporal_dimension)
            options.validate_tile_grid_with_separate_asset_per_band()

        return options

    def validate_filepath_per_band_with_temporal_dimension(self, *, has_temporal_dimension: bool) -> None:
        if self.filepath_per_band and has_temporal_dimension:
            raise OpenEOApiException("filepath_per_band is not supported with temporal dimension")

    def validate_tile_grid_with_separate_asset_per_band(self) -> None:
        if self.tile_grid and self.separate_asset_per_band:
            raise OpenEOApiException(message="separate_asset_per_band is not supported with tile_grid")

    def validate_sample_by_feature_with_separate_asset_per_band(self) -> None:
        if self.separate_asset_per_band:
            raise OpenEOApiException(message="separate_asset_per_band is not supported with sample_by_feature")


@dataclass(frozen=True)
class WrittenAsset:
    key: str
    path: str
    band_indices: Optional[List[int]] = None
    proj_bbox: Optional[Tuple[float, ...]] = None
    proj_shape: Optional[Tuple[int, ...]] = None
    proj_epsg: Optional[int] = None
    nc_bands: Optional[List[dict]] = None


@dataclass(frozen=True)
class WrittenItem:
    id: str
    datetime: Optional[str] = None
    bbox: Optional[Tuple[float, float, float, float]] = None
    crs: Optional[Any] = None
    assets: List[WrittenAsset] = field(default_factory=list)


class Variant(str, Enum):
    """Which item/asset shape to build from a list of ``WrittenItem``s."""

    STITCH = "stitch"  # stitched GTIFF, with or without tile_grid
    BATCH = "batch"  # batch-mode GTIFF, temporal or spatial+sample_by_feature
    PLAIN = "plain"  # non-stitched, non-batch GTIFF, with or without tile_grid
    NETCDF = "netcdf"  # NetCDF written through the JVM writers


def select_variant(
    *, stitch: bool, tile_grid: Optional[str], batch_mode: bool, is_temporal_layer: bool, sample_by_feature: bool
) -> Variant:
    if stitch:
        return Variant.STITCH
    if batch_mode and is_temporal_layer:
        return Variant.BATCH
    if batch_mode and not is_temporal_layer and sample_by_feature:
        return Variant.BATCH
    return Variant.PLAIN


def _to_latlng_geometry(bbox: Optional[Tuple[float, float, float, float]], crs: Any) -> Optional[Polygon]:
    if bbox is None:
        return None
    xmin, ymin, xmax, ymax = bbox
    return reproject_geometry(box(xmin, ymin, xmax, ymax), src_crs=crs, dst_crs="EPSG:4326")


def _stitch_item(item: WrittenItem) -> dict:
    geometry = _to_latlng_geometry(item.bbox, item.crs)
    assets = {}
    for a in item.assets:
        assets[a.key] = dict_no_none(
            {
                "href": a.path,
                "geometry": mapping(geometry),
                "bbox": geometry.bounds,
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
                "proj:bbox": a.proj_bbox,
                "proj:shape": a.proj_shape,
                "proj:epsg": a.proj_epsg,
            }
        )
    return {
        "id": item.id,
        "properties": {"datetime": item.datetime},
        "geometry": mapping(geometry),
        "bbox": geometry.bounds,
        "assets": assets,
    }


def _batch_item(item: WrittenItem, bands: List[dict], nodata: Any) -> dict:
    geometry = _to_latlng_geometry(item.bbox, item.crs)
    assets = {}
    for a in item.assets:
        assets[a.key] = dict_no_none(
            {
                "href": a.path,
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
                "bands": ([band for i, band in enumerate(bands) if i in a.band_indices] if a.band_indices else bands),
                "nodata": nodata,
                "datetime": item.datetime,
                "geometry": mapping(geometry),
                "bbox": geometry.bounds,
                "proj:bbox": a.proj_bbox,
                "proj:shape": a.proj_shape,
                "proj:epsg": a.proj_epsg,
            }
        )
    return {
        "id": item.id,
        "properties": {"datetime": item.datetime},
        "geometry": mapping(geometry),
        "bbox": geometry.bounds,
        "assets": assets,
    }


def _plain_item(item: WrittenItem, bands: List[dict], nodata: Any) -> dict:
    geometry = _to_latlng_geometry(item.bbox, item.crs)
    assets = {}
    for a in item.assets:
        asset = dict_no_none(
            {
                "href": a.path,
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
                "nodata": nodata,
                "proj:bbox": a.proj_bbox,
                "proj:shape": a.proj_shape,
                "proj:epsg": a.proj_epsg,
            }
        )
        if a.band_indices is not None:
            asset["bands"] = [band for i, band in enumerate(bands) if i in a.band_indices]
        asset["geometry"] = mapping(geometry)
        asset["bbox"] = geometry.bounds
        assets[a.key] = asset
    return {
        "id": item.id,
        "geometry": mapping(geometry),
        "bbox": geometry.bounds,
        "assets": assets,
    }


def _netcdf_item(item: WrittenItem, nodata: Any) -> dict:
    geometry = _to_latlng_geometry(item.bbox, item.crs)
    bbox = geometry.bounds if geometry is not None else None
    assets = {}
    for a in item.assets:
        assets[a.key] = dict_no_none(
            {
                "href": a.path,
                "type": "application/x-netcdf",
                "roles": ["data"],
                "nodata": nodata,
                "geometry": mapping(geometry) if geometry is not None else None,
                "bbox": bbox,
                "bands": a.nc_bands,
                "raster:bands": a.nc_bands,
                "proj:bbox": a.proj_bbox,
                "proj:shape": a.proj_shape,
                "proj:epsg": a.proj_epsg,
            }
        )
    return {
        "id": item.id,
        "geometry": mapping(geometry) if geometry is not None else None,
        "bbox": bbox,
        "assets": assets,
    }


_BUILDERS = {
    Variant.STITCH: lambda item, bands, nodata: _stitch_item(item),
    Variant.BATCH: _batch_item,
    Variant.PLAIN: _plain_item,
    Variant.NETCDF: lambda item, bands, nodata: _netcdf_item(item, nodata),
}


def build_items(
    written: Sequence[WrittenItem], *, variant: Variant, bands: Optional[List[dict]] = None, nodata: Any = None
) -> Dict[str, dict]:
    builder = _BUILDERS[variant]
    return {item.id: builder(item, bands, nodata) for item in written}


def single_asset_item(*, asset_key: str, asset: dict, item_extra: Optional[dict] = None) -> Dict[str, dict]:
    """Build the single-uuid-item, single-asset shape used by PNG, stitched batch NetCDF and the fallback format."""
    item_id = str(uuid.uuid4())
    item = {"id": item_id, "assets": {asset_key: asset}}
    if item_extra:
        item.update(item_extra)
    return {item_id: item}


def add_gdalinfo_objects(assets: Dict[str, dict], *, attach_gdalinfo_assets: bool, save_directory: str) -> Dict[str, dict]:
    if not attach_gdalinfo_assets:
        return assets
    assets_to_add = {}
    for value in assets.values():
        href_path = str(value["href"])
        gdalinfo_path = href_path + GDALINFO_SUFFIX
        if os.path.exists(gdalinfo_path):
            obj = {"href": gdalinfo_path, "type": "application/json", "roles": ["metadata"]}
            if "bbox" in value:
                obj["bbox"] = value["bbox"]
            if "geometry" in value:
                obj["geometry"] = value["geometry"]
            if "datetime" in value:
                obj["datetime"] = value["datetime"]
            name_key = str(pathlib.Path(gdalinfo_path).relative_to(save_directory))
            assets_to_add[name_key] = obj
    return {**assets, **assets_to_add}
