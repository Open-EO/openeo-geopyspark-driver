from __future__ import annotations

import logging
import math
from typing import Union, Optional, List, Tuple, Callable

from openeo.util import deep_get
from openeo_driver.backend import AbstractCollectionCatalog
from openeo_driver.dry_run import SourceConstraint
from openeo_driver.errors import CollectionNotFoundException
from openeo_driver.util.geometry import BoundingBox, spatial_extent_union, epsg_code_or_none
from openeo_driver.util.utm import is_auto_utm_crs, is_utm_crs

_log = logging.getLogger(__name__)


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
        if epsg := epsg_code_or_none(crs):
            self.crs_epsg = epsg
        else:
            self.crs_epsg = None
        self.resolution = tuple(resolution) if resolution else None
        self.extent_x = tuple(extent_x) if extent_x else None
        self.extent_y = tuple(extent_y) if extent_y else None

    @classmethod
    def from_datacube_metadata(cls, metadata: dict) -> _GridInfo:
        """Construct from STAC-style 'datacube' metadata ("cube:dimensions")."""
        # TODO: leverage pystac here instead of DIY parsing?
        [dim_x] = [d for d in metadata["cube:dimensions"].values() if d["type"] == "spatial" and d["axis"] == "x"]
        [dim_y] = [d for d in metadata["cube:dimensions"].values() if d["type"] == "spatial" and d["axis"] == "y"]

        # reference_system is optional, but defaults to EPSG code 4326.
        crses = {dim_x.get("reference_system", 4326), dim_y.get("reference_system", 4326)}
        if len(crses) != 1:
            raise ValueError(f"Found multiple CRSes across spatial dimensions: {crses}")
        crs = crses.pop()

        # Extent is required for both dimensions
        if extent_x := dim_x.get("extent"):
            extent_x = tuple(extent_x)
        else:
            _log.warning(f"No 'x' extent found ({dim_x=})")
            extent_x = (None, None)
        if extent_y := dim_y.get("extent"):
            extent_y = tuple(extent_y)
        else:
            extent_y = (None, None)
            _log.warning(f"No 'y' extent found ({dim_y=})")

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
        if not source.resolution or any(r > 20 for r in source.resolution):
            _log.info(f"Not realigning {extent=} because auto-UTM (AUTO:42001) and {source.resolution}")
            return extent
        res = target.resolution if all(target.resolution) else source.resolution
        aligned = extent.reproject_to_best_utm().round_to_resolution(res[0], res[1])
        _log.info(f"Realigned input extent {extent} into {aligned}")
        return aligned
    else:
        _log.info(f"Not realigning {extent=} ({source=})")
        return extent


def _buffer_extent(extent: BoundingBox, *, buffer: Tuple[float, float], sampling: _GridInfo) -> BoundingBox:
    if sampling.crs and sampling.resolution:
        # Scale buffer from pixels to target CRS units
        dx, dy = [r * math.ceil(s) for r, s in zip(sampling.resolution, buffer)]
        extent = extent.reproject(sampling.crs).buffer(dx=dx, dy=dy)
    else:
        _log.warning(f"Not buffering extent with {buffer=} because incomplete {sampling=}.")
    return extent


def _extract_target_extent_from_constraint(
    source_constraint: SourceConstraint, *, catalog: AbstractCollectionCatalog
) -> Union[BoundingBox, None]:
    source_id, constraint = source_constraint

    source_process = source_id[0]
    do_realign = True
    if source_process == "load_collection":
        collection_id = source_id[1][0]
        try:
            metadata = catalog.get_collection_metadata(collection_id)
        except CollectionNotFoundException:
            metadata = {}
        source_grid = _GridInfo.from_datacube_metadata(metadata=metadata)
        # TODO #275 eliminate this VITO specific handling?
        do_realign = deep_get(metadata, "_vito", "data_source", "realign", default=True)
    elif source_process == "load_stac":
        raise NotImplementedError("TODO")
    else:
        raise NotImplementedError("TODO")

    extent_from_pg = constraint.get("spatial_extent") or constraint.get("weak_spatial_extent")
    if not extent_from_pg:
        return None
    extent: BoundingBox = BoundingBox.from_dict(extent_from_pg, default_crs=4326)

    target_sampling = _SamplingInfo(
        crs=constraint.get("resample", {}).get("target_crs", source_grid.crs),
        resolution=constraint.get("resample", {}).get("resolution", source_grid.resolution),
    )

    # TODO: shouldn't the pixel buffering be applied after the alignment?
    if pixel_buffer_size := deep_get(constraint, "pixel_buffer", "buffer_size", default=None):
        extent = _buffer_extent(extent, buffer=pixel_buffer_size, sampling=target_sampling)

    load_in_native_grid = (target_sampling.crs == source_grid.crs) or (
        is_auto_utm_crs(source_grid.crs) and is_utm_crs(target_sampling.crs)
    )
    if load_in_native_grid and do_realign:
        extent = _align_extent(extent=extent, source=source_grid, target=target_sampling)

    return extent



def determine_global_extent(
    *, source_constraints: List[SourceConstraint], catalog: AbstractCollectionCatalog
) -> Optional[dict]:
    global_extent = None

    for source_id, constraint in source_constraints:
        extent = _extract_target_extent_from_constraint((source_id, constraint), catalog=catalog)
        if extent:
            # TODO spatial_extent_union does not support BoundingBox yet
            global_extent = spatial_extent_union(global_extent, extent) if global_extent else extent

    return global_extent
