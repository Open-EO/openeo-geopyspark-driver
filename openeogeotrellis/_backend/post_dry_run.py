from __future__ import annotations
import dataclasses

import logging

import math

from typing import Union, Optional, List, Tuple, Callable
import pyproj.exceptions
import pyproj

from openeo.util import deep_get
from openeo_driver.backend import AbstractCollectionCatalog
from openeo_driver.dry_run import SourceConstraint
from openeo_driver.errors import CollectionNotFoundException
from openeo_driver.util.geometry import BoundingBox, spatial_extent_union
from openeo_driver.util.utm import is_auto_utm_crs, is_utm_crs

_log = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class _GridInfo:
    """
    Container of raster/sampling/grid info
    in style of the STAC datacube extension ("cube:dimensions").
    """

    crs: Union[str, int]
    extent_x: Tuple[float, float]
    extent_y: Tuple[float, float]
    resolution: Union[Tuple[float, float], None] = None

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
        extent_x = tuple(dim_x.get("extent"))
        extent_y = tuple(dim_y.get("extent"))
        if not extent_x:
            _log.warning(f"No 'x' extent found ({dim_x=})")
        if not extent_y:
            _log.warning(f"No 'y' extent found ({dim_y=})")

        # Step is optional
        if "step" in dim_x and "step" in dim_y:
            resolution = dim_x.get("step"), dim_y.get("step")
        else:
            resolution = None

        return cls(crs=crs, extent_x=extent_x, extent_y=extent_y, resolution=resolution)


def _align_extent(
    extent: BoundingBox,
    *,
    source_grid: _GridInfo,
    target_grid: _GridInfo,
) -> BoundingBox:

    if target_grid.resolution is None and source_grid.resolution is None:
        _log.info(f"Not realigning {extent=} ({source_grid=})")
        return extent

    if (
        source_grid.crs == 4326
        and extent.crs == "EPSG:4326"
        and source_grid.extent_x
        and source_grid.extent_y
        and (target_grid.resolution is None or target_grid.resolution == source_grid.resolution)
    ):
        aligned = _snap_bbox(
            bbox=extent,
            resolution=target_grid.resolution or source_grid.resolution,
            extent_x=source_grid.extent_x,
            extent_y=source_grid.extent_y,
        )
        _log.info(f"Realigned input extent {extent} into {aligned}")
        return aligned
    elif is_auto_utm_crs(source_grid.crs):
        if not source_grid.resolution or any(r > 20 for r in source_grid.resolution):
            _log.info(f"Not realigning {extent=} because auto-UTM (AUTO:42001) and {source_grid.resolution}")
            return extent
        res = target_grid.resolution if all(target_grid.resolution) else source_grid.resolution
        aligned = extent.reproject_to_best_utm().round_to_resolution(res[0], res[1])
        _log.info(f"Realigned input extent {extent} into {aligned}")
        return aligned
    else:
        _log.info(f"Not realigning {extent=} ({source_grid=})")
        return extent


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

    target_grid = _GridInfo(
        crs=constraint.get("resample", {}).get("target_crs", source_grid.crs),
        extent_x=["TODO?", "TODO?"],
        extent_y=["TODO?", "TODO?"],
        resolution=constraint.get("resample", {}).get("resolution", source_grid.resolution),
    )

    # TODO: shouldn't the pixel buffering be applied after the alignment?
    if pixel_buffer_size := deep_get(constraint, "pixel_buffer", "buffer_size", default=None):
        extent = _buffer_extent(extent, buffer=pixel_buffer_size, grid=target_grid)

    load_in_native_grid = (target_grid.crs == source_grid.crs) or (
        is_auto_utm_crs(source_grid.crs) and is_utm_crs(target_grid.crs)
    )
    if load_in_native_grid and do_realign:
        extent = _align_extent(extent=extent, source_grid=source_grid, target_grid=target_grid)

    return extent


def _buffer_extent(extent: BoundingBox, *, buffer: Tuple[float, float], grid: _GridInfo) -> BoundingBox:
    if grid.crs and grid.resolution:
        # Scale buffer from pixels to target CRS units
        dx, dy = [r * math.ceil(s) for r, s in zip(grid.resolution, buffer)]
        extent = extent.reproject(grid.crs).buffer(dx=dx, dy=dy)
    else:
        _log.warning(f"Not buffering extent with {buffer=} because incomplete {grid=}.")
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
