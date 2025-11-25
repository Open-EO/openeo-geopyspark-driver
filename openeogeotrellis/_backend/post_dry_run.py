import logging

import math

from typing import Union, Optional, List
import pyproj.exceptions
import pyproj

from openeo_driver.backend import AbstractCollectionCatalog
from openeo_driver.dry_run import SourceConstraint
from openeo_driver.errors import CollectionNotFoundException
from openeo_driver.util.geometry import BoundingBox, spatial_extent_union


_log = logging.getLogger(__name__)


def _collection_crs(collection_id: str, *, catalog: AbstractCollectionCatalog) -> Union[None, str, int]:
    """
    Get spatial reference system from of the data in openEO collection metadata (based on datacube STAC extension)
    """
    # TODO: this only works for predefined openEO collections,
    #       so this is source of inconsistency with direct `load_stac` usage
    try:
        metadata = catalog.get_collection_metadata(collection_id)
    except CollectionNotFoundException:
        return None
    # TODO: the default reference_system according to the datacube spec is 4326 (EPSG code as integer)
    #       but changing that here might have unintended side-effects.
    crs = metadata.get("cube:dimensions", {}).get("x", {}).get("reference_system", None)
    return crs


def _collection_resolution(collection_id: str, *, catalog: AbstractCollectionCatalog) -> Optional[List[int]]:
    # TODO: this only works for predefined openEO collections,
    #       so this is source of inconsistency with direct `load_stac` usage
    try:
        metadata = catalog.get_collection_metadata(collection_id)
    except CollectionNotFoundException:
        return None
    x = metadata.get("cube:dimensions", {}).get("x", {})
    y = metadata.get("cube:dimensions", {}).get("y", {})
    if "step" in x and "step" in y:
        return [x["step"], y["step"]]
    else:
        return None


def _align_extent(
    extent: dict, collection_id: str, *, catalog: AbstractCollectionCatalog, target_resolution=None
) -> dict:
    try:
        metadata = catalog.get_collection_metadata(collection_id)
    except CollectionNotFoundException:
        metadata = None

    # TODO #275 eliminate this VITO specific handling?
    if metadata is None or not metadata.get("_vito", {}).get("data_source", {}).get("realign", True):
        return extent

    crs = _collection_crs(collection_id=collection_id, catalog=catalog)
    collection_resolution = _collection_resolution(collection_id=collection_id, catalog=catalog)
    isUTM = crs == "AUTO:42001" or "Auto42001" in str(crs)

    x = metadata.get("cube:dimensions", {}).get("x", {})
    y = metadata.get("cube:dimensions", {}).get("y", {})

    if target_resolution is None and collection_resolution is None:
        return extent

    if (
        crs == 4326
        and extent.get("crs", "") == "EPSG:4326"
        and "extent" in x
        and "extent" in y
        and (target_resolution is None or target_resolution == collection_resolution)
    ):
        # only align to collection resolution
        target_resolution = collection_resolution

        def align(v, dimension, rounding, resolution):
            range = dimension.get("extent", [])
            if v < range[0]:
                v = range[0]
            elif v > range[1]:
                v = range[1]
            else:
                index = rounding((v - range[0]) / resolution)
                v = range[0] + index * resolution
            return v

        new_extent = {
            "west": align(extent["west"], x, math.floor, target_resolution[0]),
            "east": align(extent["east"], x, math.ceil, target_resolution[0]),
            "south": align(extent["south"], y, math.floor, target_resolution[1]),
            "north": align(extent["north"], y, math.ceil, target_resolution[1]),
            "crs": extent["crs"],
        }
        _log.info(f"Realigned input extent {extent} into {new_extent}")

        return new_extent
    elif isUTM:
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


def determine_global_extent(
    *, source_constraints: List[SourceConstraint], catalog: AbstractCollectionCatalog
) -> Optional[dict]:
    global_extent = None

    for sid, constraint in source_constraints:
        if sid[0] == "load_collection":
            collection_id = sid[1][0]
        elif sid[0] == "load_stac":
            # TODO: cover this case better?
            collection_id = "_load_stac_no_collection_id"
        else:
            # TODO: cover this as well?
            collection_id = "_unknown_no_collection_id"

        extent = None
        if "spatial_extent" in constraint:
            extent = constraint["spatial_extent"]
        elif "weak_spatial_extent" in constraint:
            extent = constraint["weak_spatial_extent"]

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
