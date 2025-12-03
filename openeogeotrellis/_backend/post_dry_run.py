from __future__ import annotations

import collections
import logging
import math
from typing import Callable, Dict, List, Optional, Tuple, Union

from openeo.util import deep_get
from openeo_driver.backend import AbstractCollectionCatalog, LoadParameters
from openeo_driver.dry_run import SourceConstraint
from openeo_driver.errors import CollectionNotFoundException
from openeo_driver.util.geometry import BoundingBox, epsg_code_or_none, spatial_extent_union
from openeo_driver.util.utm import is_auto_utm_crs, is_utm_crs

from openeogeotrellis.load_stac import (
    _ProjectionMetadata,
    _spatiotemporal_extent_from_load_params,
    construct_item_collection,
)

_log = logging.getLogger(__name__)


def two_float_tuple(value) -> Tuple[float, float]:
    res = tuple(value)
    if not (len(res) == 2 and all(isinstance(v, (float, int)) for v in res)):
        raise ValueError(f"Expected a tuple of two floats, got: {value!r}")
    return res


class _GridInfo:
    """
    Container for basic sampling info: CRS, resolution, grid bounds
    """

    def __init__(
        self,
        *,
        crs: Union[str, int],
        resolution: Optional[Tuple[float, float]] = None,
        extent_x: Optional[Tuple[float, float]] = None,
        extent_y: Optional[Tuple[float, float]] = None,
    ):
        self.crs_raw: Union[str, int] = crs
        self.crs_epsg = epsg_code_or_none(crs)
        self.resolution = two_float_tuple(resolution) if resolution else None
        self.extent_x = two_float_tuple(extent_x) if extent_x else None
        self.extent_y = two_float_tuple(extent_y) if extent_y else None

    def __repr__(self) -> str:
        return f"_GridInfo(crs={self.crs_raw!r}, resolution={self.resolution!r}, extent_x={self.extent_x!r}, extent_y={self.extent_y!r})"

    @classmethod
    def from_datacube_metadata(cls, metadata: dict) -> _GridInfo:
        """Construct from STAC-style 'datacube' metadata ("cube:dimensions")."""
        # TODO: leverage pystac here instead of DIY parsing?
        # TODO: "cube:dimensions" is not ideal for precise pixel grid deduction
        #       this path should be eliminated in favor of more precise
        #       metadata (e.g. proj:bbox/proj:transform etc) from individual items.
        [dim_x] = [d for d in metadata["cube:dimensions"].values() if d["type"] == "spatial" and d["axis"] == "x"]
        [dim_y] = [d for d in metadata["cube:dimensions"].values() if d["type"] == "spatial" and d["axis"] == "y"]

        # reference_system is optional, but defaults to EPSG code 4326.
        crs_x = dim_x.get("reference_system", 4326)
        crs_y = dim_y.get("reference_system", 4326)
        if crs_x != crs_y:
            _log.warning(f"Different CRS for x and y dimension ({crs_x=}, {crs_y=}). Using CRS from x dimension.")
        crs = crs_x

        # Extent is required for both dimensions
        extent_x = dim_x.get("extent", None)
        extent_y = dim_y.get("extent", None)
        if not extent_x or not extent_y:
            _log.warning(f"No missing x/y extent in {dim_x=}, {dim_y=}")

        # Step is optional
        if "step" in dim_x and "step" in dim_y:
            resolution = dim_x.get("step"), dim_y.get("step")
        else:
            resolution = None

        return cls(crs=crs, extent_x=extent_x, extent_y=extent_y, resolution=resolution)


def _snap_bbox(
    bbox: BoundingBox,
    *,
    resolution: tuple[float, float],
    extent_x: Tuple[float, float],
    extent_y: Tuple[float, float],
) -> BoundingBox:
    """
    Snap (aka align) given bbox bounds to grid defined by the resolution and extents
    """
    # TODO: for small resolutions (e.g. when working with fractional lonlat degrees),
    #       the numerical precision of this implementation is poor (as observed in unit tests)

    def snap(v: float, extent_v: Tuple[float, float], rounding: Callable, resolution: float):
        v_min, vmax = extent_v
        if v < v_min:
            v = v_min
        elif v > vmax:
            v = vmax
        else:
            v = v_min + resolution * rounding((v - v_min) / resolution)
        return v

    aligned = BoundingBox(
        west=snap(bbox.west, extent_x, math.floor, resolution[0]),
        east=snap(bbox.east, extent_x, math.ceil, resolution[0]),
        south=snap(bbox.south, extent_y, math.floor, resolution[1]),
        north=snap(bbox.north, extent_y, math.ceil, resolution[1]),
        crs=bbox.crs,
    )
    return aligned


def _align_extent(
    extent: BoundingBox,
    *,
    source: _GridInfo,
    target: _GridInfo,
) -> BoundingBox:
    if target.resolution is None and source.resolution is None:
        _log.info(f"Not realigning {extent=} ({source=})")
        return extent

    if (
        source.crs_epsg == 4326
        and target.crs_epsg == 4326
        and extent.crs == "EPSG:4326"
        and source.extent_x
        and source.extent_y
        and (target.resolution is None or target.resolution == source.resolution)
    ):
        aligned = _snap_bbox(
            bbox=extent,
            resolution=target.resolution or source.resolution,
            extent_x=source.extent_x,
            extent_y=source.extent_y,
        )
        _log.info(f"Realigned input extent {extent} into {aligned}")
        return aligned
    elif is_auto_utm_crs(source.crs_raw):
        # TODO: also align non-auto UTM
        # TODO: why not realign above 20m?
        if source.resolution and source.resolution[0] <= 20:
            res = target.resolution if all(target.resolution) else source.resolution
            # TODO: support reprojection to user specified UTM instead of "best" UTM?
            aligned = extent.reproject_to_best_utm().round_to_resolution(res[0], res[1])
            _log.info(f"Realigned input extent {extent} into {aligned}")
            return aligned
        else:
            _log.info(f"Not realigning {extent=} because auto-UTM (AUTO:42001) and {source.resolution=}")
            return extent
    else:
        _log.info(f"Not realigning {extent=} ({source=})")
        return extent


def _buffer_extent(
    extent: BoundingBox, *, buffer: Union[Tuple[float, float], int, float], sampling: _GridInfo
) -> BoundingBox:
    if isinstance(buffer, (int, float)):
        buffer = (buffer, buffer)
    if sampling.crs_epsg and sampling.resolution:
        # Scale buffer from pixels to target CRS units
        dx, dy = [r * math.ceil(b) for r, b in zip(sampling.resolution, buffer)]
        extent = extent.reproject(sampling.crs_epsg).buffer(dx=dx, dy=dy)
    else:
        _log.warning(f"Not buffering extent with {buffer=} because incomplete {sampling=}.")
    return extent


def _extract_spatial_extent_from_constraint(
    source_constraint: SourceConstraint, *, catalog: AbstractCollectionCatalog
) -> Union[None, Tuple[BoundingBox, BoundingBox]]:
    """
    Extract spatial extent from given source constraint (if any), and align it to target grid.

    If no spatial extent is found, returns None.
    Otherwise returns a tuple of (original_extent, aligned_extent).
    """
    source_id, constraint = source_constraint
    source_process = source_id[0]
    if source_process == "load_collection":
        collection_id = source_id[1][0]
        return _extract_spatial_extent_from_constraint_load_collection(
            collection_id=collection_id, constraint=constraint, catalog=catalog
        )
    elif source_process == "load_stac":
        url = source_id[1][0]
        return _extract_spatial_extent_from_constraint_load_stac(stac_url=url, constraint=constraint)
    else:
        # TODO?
        return None


def _extract_spatial_extent_from_constraint_load_collection(
    collection_id: str, *, constraint: dict, catalog: AbstractCollectionCatalog
) -> Union[None, Tuple[BoundingBox, BoundingBox]]:
    try:
        metadata = catalog.get_collection_metadata(collection_id)
    except CollectionNotFoundException:
        metadata = {}
    # TODO Extracting pixel grid info from collection metadata might might be unreliable
    #       and should be replaced by more precise item-level metadata where possible.
    source_grid = _GridInfo.from_datacube_metadata(metadata=metadata)
    # TODO #275 eliminate this VITO specific handling?
    do_realign = deep_get(metadata, "_vito", "data_source", "realign", default=True)

    extent_from_pg = constraint.get("spatial_extent") or constraint.get("weak_spatial_extent")
    if not extent_from_pg:
        return None

    extent_orig: BoundingBox = BoundingBox.from_dict(extent_from_pg, default_crs=4326)
    extent_aligned = extent_orig

    target_grid = _GridInfo(
        crs=constraint.get("resample", {}).get("target_crs", source_grid.crs_raw),
        resolution=constraint.get("resample", {}).get("resolution", source_grid.resolution),
    )

    # TODO: shouldn't the pixel buffering be applied after the alignment?
    if pixel_buffer_size := deep_get(constraint, "pixel_buffer", "buffer_size", default=None):
        extent_aligned = _buffer_extent(extent_aligned, buffer=pixel_buffer_size, sampling=target_grid)

    load_in_native_grid = (target_grid.crs_raw == source_grid.crs_raw) or (
        is_auto_utm_crs(source_grid.crs_raw) and (is_utm_crs(target_grid.crs_epsg))
    )
    if load_in_native_grid and do_realign:
        extent_aligned = _align_extent(extent=extent_aligned, source=source_grid, target=target_grid)

    return extent_orig, extent_aligned


def _extract_spatial_extent_from_constraint_load_stac(
    stac_url: str, *, constraint: dict
) -> Union[None, Tuple[BoundingBox, BoundingBox]]:
    extent_from_pg = constraint.get("spatial_extent") or constraint.get("weak_spatial_extent")
    if not extent_from_pg:
        return None

    extent_orig: BoundingBox = BoundingBox.from_dict(extent_from_pg, default_crs=4326)

    spatiotemporal_extent = _spatiotemporal_extent_from_load_params(
        # TODO: eliminate this silly `LoadParameters` roundtrip and avoid duplication with _extract_load_parameters
        LoadParameters(
            spatial_extent=extent_from_pg,
            temporal_extent=constraint.get("temporal_extent") or (None, None),
        )
    )
    property_filter_pg_map = None  # TODO
    item_collection, _, _, _ = construct_item_collection(
        url=stac_url, spatiotemporal_extent=spatiotemporal_extent, property_filter_pg_map=property_filter_pg_map
    )

    # Collect bounding boxes (per projection) to determine overall extent
    bboxes_per_proj: Dict[str, List[dict]] = collections.defaultdict(list)
    for item, band_assets in item_collection.iter_items_with_band_assets():
        for asset_id, asset in band_assets.items():
            projection_metadata = _ProjectionMetadata.from_asset(asset=asset, item=item)
            bboxes_per_proj[projection_metadata.code].append(projection_metadata.to_bounding_box().as_dict())

    if len(bboxes_per_proj) == 1:
        # Simple case: all assets share the same projection
        [bboxes] = bboxes_per_proj.values()
        extent_aligned = BoundingBox.from_dict(spatial_extent_union(*bboxes))
    else:
        raise NotImplementedError("Merging extents from multiple projections is not yet implemented.")

    return extent_orig, extent_aligned


def determine_global_extent(
    *,
    source_constraints: List[SourceConstraint],
    catalog: AbstractCollectionCatalog,
) -> dict:
    orig_extents = []
    aligned_extents = []
    for source_id, constraint in source_constraints:
        extents = _extract_spatial_extent_from_constraint((source_id, constraint), catalog=catalog)
        if extents:
            extent_orig, extent_aligned = extents
            orig_extents.append(extent_orig.as_dict())
            aligned_extents.append(extent_aligned.as_dict())

    # TODO: support BoundingBox directly in `spatial_extent_union`?
    global_extent_original = spatial_extent_union(*orig_extents) if orig_extents else None
    global_extent_aligned = spatial_extent_union(*aligned_extents) if aligned_extents else None

    return {
        "global_extent_original": global_extent_original,
        "global_extent_aligned": global_extent_aligned,
    }


def post_dry_run(
    *,
    source_constraints: List[SourceConstraint],
    catalog: AbstractCollectionCatalog,
) -> dict:
    global_extent = determine_global_extent(source_constraints=source_constraints, catalog=catalog)
    return {
        **global_extent,
    }
