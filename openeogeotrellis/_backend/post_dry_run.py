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
class _SamplingInfo:
    """
    Container of raster/sampling info in style of the STAC datacube extension ("cube:dimensions").
    """

    crs: Union[str, int]
    extent_x: Tuple[float, float]
    extent_y: Tuple[float, float]
    resolution: Union[Tuple[float, float], None] = None

    @classmethod
    def from_datacube_metadata(cls, metadata: dict) -> _SamplingInfo:
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
    source_sampling: _SamplingInfo,
    target_sampling: _SamplingInfo,
) -> BoundingBox:

    if target_sampling.resolution is not None and source_sampling.resolution is None:
        return extent

    if (
        source_sampling.crs == 4326
        and extent.crs == "EPSG:4326"
        and source_sampling.extent_x
        and source_sampling.extent_y
        and (target_sampling.resolution is None or target_sampling.resolution == source_sampling.resolution)
    ):

        target_resolution = target_sampling.resolution or source_sampling.resolution

        def snap(v: float, v_extent: Tuple[float, float], rounding: Callable, resolution: float):
            vmin, vmax = v_extent
            if v < vmin:
                v = vmin
            elif v > vmax:
                v = vmax
            else:
                v = vmin + resolution * rounding((v - vmin) / resolution)
            return v

        aligned = BoundingBox(
            west=snap(extent.west, source_sampling.extent_x, math.floor, target_resolution[0]),
            east=snap(extent.east, source_sampling.extent_x, math.ceil, target_resolution[0]),
            south=snap(extent.south, source_sampling.extent_y, math.floor, target_resolution[1]),
            north=snap(extent.north, source_sampling.extent_y, math.ceil, target_resolution[1]),
            crs=extent.crs,
        )

        _log.info(f"Realigned input extent {extent} into {aligned}")

        return aligned
    elif is_auto_utm_crs(source_sampling.crs):
        if collection_resolution[0] <= 20 and target_resolution[0] <= 20:
            bbox = BoundingBox.from_dict(extent, default_crs=4326)
            bbox_utm = bbox.reproject_to_best_utm()

            res = target_resolution if target_resolution[0] > 0 else collection_resolution

            new_extent = bbox_utm.round_to_resolution(res[0], res[1])

            _log.info(f"Realigned input extent {extent} into {new_extent}")
            return new_extent.as_dict()
        else:
            _log.info(f"Not realigning input extent {extent} because crs is UTM and resolution > 20m")
            return extent
    else:
        _log.info(f"Not realigning input extent {extent} (collection crs: {crs}, resolution: {collection_resolution})")
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
        source_sampling = _SamplingInfo.from_datacube_metadata(metadata=metadata)
        # TODO #275 eliminate this VITO specific handling?
        do_realign = deep_get(metadata, "_vito", "data_source", "realign", default=True)

    elif source_process == "load_stac":
        raise NotImplementedError  # TODO?
    else:
        raise NotImplementedError  # TODO?

    extent_from_pg = constraint.get("spatial_extent") or constraint.get("weak_spatial_extent")
    if not extent_from_pg:
        return None
    extent: BoundingBox = BoundingBox.from_dict(extent_from_pg, default_crs=4326)

    target_sampling = _SamplingInfo(
        crs=constraint.get("resample", {}).get("target_crs", source_sampling.crs),
        resolution=constraint.get("resample", {}).get("resolution", source_sampling.resolution),
    )

    # TODO: shouldn't the pixel buffering be applied after the alignment?
    if pixel_buffer_size := deep_get(constraint, "pixel_buffer", "buffer_size", default=None):
        extent = _buffer_extent(extent, buffer=pixel_buffer_size, sampling=target_sampling)

    load_in_native_grid = (target_sampling.crs == source_sampling.crs) or (
        is_auto_utm_crs(source_sampling.crs) and is_utm_crs(target_sampling.crs)
    )
    if load_in_native_grid and do_realign:
        extent = _align_extent(extent=extent)

    return extent


def _buffer_extent(extent: BoundingBox, *, buffer: Tuple[float, float], sampling: _SamplingInfo) -> BoundingBox:
    if sampling.crs and sampling.resolution:
        # Scale buffer from pixels to target CRS units
        dx, dy = [r * math.ceil(s) for r, s in zip(sampling.resolution, buffer)]
        extent = extent.reproject(sampling.crs).buffer(dx=dx, dy=dy)
    else:
        _log.warning(f"Not buffering extent with {buffer=} because incomplete {sampling=}.")
    return extent


def determine_global_extent(
    *, source_constraints: List[SourceConstraint], catalog: AbstractCollectionCatalog
) -> Optional[dict]:
    global_extent = None

    for source_id, constraint in source_constraints:
        if source_id[0] == "load_collection":
            collection_id = source_id[1][0]
        elif source_id[0] == "load_stac":
            # TODO: cover this case better?
            collection_id = "_load_stac_no_collection_id"
        else:
            # TODO: cover this as well?
            collection_id = "_unknown_no_collection_id"

        extent = None
        if "spatial_extent" in constraint:
            extent = BoundingBox.from_dict(constraint["spatial_extent"])
        elif "weak_spatial_extent" in constraint:
            extent = BoundingBox.from_dict(constraint["weak_spatial_extent"])

        if extent is not None:
            collection_crs = _collection_crs(collection_id=collection_id, catalog=catalog)
            target_crs = constraint.get("resample", {}).get("target_crs", collection_crs) or collection_crs
            target_resolution = constraint.get("resample", {}).get("resolution", None) or _collection_resolution(
                collection_id=collection_id, catalog=catalog
            )

            if "pixel_buffer" in constraint:
                buffer = constraint["pixel_buffer"]["buffer_size"]

                if (target_crs is not None) and target_resolution:
                    bbox = BoundingBox.from_dict(extent, default_crs=4326)
                    extent = bbox.reproject(target_crs).as_dict()

                    extent = {
                        "west": extent["west"] - target_resolution[0] * math.ceil(buffer[0]),
                        "east": extent["east"] + target_resolution[0] * math.ceil(buffer[0]),
                        "south": extent["south"] - target_resolution[1] * math.ceil(buffer[1]),
                        "north": extent["north"] + target_resolution[1] * math.ceil(buffer[1]),
                        "crs": extent["crs"],
                    }
                else:
                    _log.warning("Not applying buffer to extent because the target CRS is not known.")

            load_collection_in_native_grid = "resample" not in constraint or target_crs == collection_crs
            if (not load_collection_in_native_grid) and collection_crs is not None and ("42001" in str(collection_crs)):
                # resampling auto utm to utm means we are loading in native grid
                try:
                    load_collection_in_native_grid = "UTM zone" in pyproj.CRS.from_user_input(target_crs).to_wkt()
                except pyproj.exceptions.CRSError as e:
                    pass

            if load_collection_in_native_grid:
                # Ensure that the extent that the user provided is aligned with the collection's native grid.
                extent = _align_extent(
                    extent=extent, collection_id=collection_id, catalog=catalog, target_resolution=target_resolution
                )

            global_extent = spatial_extent_union(global_extent, extent) if global_extent else extent

    return global_extent
