import datetime as dt
import logging
from copy import copy, deepcopy
from typing import Callable, Iterable, List, Optional, Tuple, Union

import dateutil.parser
import pyproj
from openeo_driver.backend import LoadParameters
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.errors import InternalException
from openeo_driver.filter_properties import extract_literal_match
from openeo_driver.util.geometry import reproject_bounding_box
from openeo_driver.util.utm import auto_utm_epsg_for_geometry
from openeo_driver.utils import EvalEnv, smart_bool
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry

from openeogeotrellis.util.datetime import normalize_temporal_extent
from openeogeotrellis.util.geometry import calculate_rough_area, health_check_extent
from openeogeotrellis.util.projection import reproject_cellsize

from .collection_metadata import GeopysparkCubeMetadata

logger = logging.getLogger(__name__)

LARGE_LAYER_THRESHOLD_IN_PIXELS = pow(10, 11)
LARGE_LAYER_THRESHOLD_IN_PIXELS_SENTINELHUB = pow(10, 10)

# Signature matches `openeogeotrellis._backend.post_dry_run.get_global_extent`.
GlobalExtentProvider = Callable[..., object]


def potential_sentinelhub(catalog, collection_id) -> bool:
    metadata_json = catalog.get_collection_metadata(collection_id=collection_id)
    metadata = GeopysparkCubeMetadata(metadata_json)
    if metadata.provider_backend() == "sentinelhub":
        return True
    for col in metadata.get("_vito", "data_source", "merged_collections", default=[]):
        if potential_sentinelhub(catalog, col):
            return True
    return False


def check_missing_products(
        collection_metadata: Union[GeopysparkCubeMetadata, dict],
        temporal_extent: Tuple[str, str], spatial_extent: dict,
        properties: Optional[dict] = None,
) -> Union[List[str], None]:
    """
    Query catalogs to figure out if the data provider/source does not fully cover the desired spatiotemporal extent.
    """
    if not isinstance(collection_metadata, GeopysparkCubeMetadata):
        collection_metadata = GeopysparkCubeMetadata(collection_metadata)
    check_data = collection_metadata.get("_vito", "data_source", "check_missing_products", default=None)

    if check_data:
        logger.info(f"Check missing products for {collection_metadata.get('id')} using {check_data}")
        temporal_extent = [dateutil.parser.parse(t) if t is not None else None for t in temporal_extent]

        if "crs" in spatial_extent and spatial_extent["crs"] != 4326 and spatial_extent["crs"] != "EPSG:4326":
            spatial_extent = reproject_bounding_box(spatial_extent,from_crs=spatial_extent["crs"],to_crs="EPSG:4326")
        # Merge given properties with global layer properties
        properties = {
            **collection_metadata.get("_vito", "properties", default={}),
            **(properties or {})
        }
        query_kwargs = {
            "start_date": dt.datetime.combine(temporal_extent[0], dt.time.min),
            "end_date": dt.datetime.combine(temporal_extent[1], dt.time.max) if temporal_extent[1] is not None else None,
            "ulx": spatial_extent["west"],
            "brx": spatial_extent["east"],
            "bry": spatial_extent["south"],
            "uly": spatial_extent["north"],
        }

        if "eo:cloud_cover" in properties:
            cloud_cover_condition = extract_literal_match(properties["eo:cloud_cover"])
            cloud_cover_op, = cloud_cover_condition.keys()
            if cloud_cover_op in {"lte", "eq"}:
                query_kwargs["cldPrcnt"] = cloud_cover_condition[cloud_cover_op]
            else:
                logger.error(f"Failed to handle cloud cover condition {properties['eo:cloud_cover']}")
                raise InternalException("Failed to handle cloud cover condition")

        method = check_data.get("method")
        if method in {"creo", "terrascope"}:
            logger.warning(
                f"check_missing_products with {method=} is now unsupported. https://github.com/Open-EO/openeo-geopyspark-driver/issues/1572"
            )
            missing = []
        else:
            logger.error(f"Invalid check_missing_products data {check_data}")
            raise InternalException("Invalid check_missing_products data")

        logger.info(
            f"check_missing_products ({method}) on {collection_metadata.get('id')} detected {len(missing)} missing products."
        )
        return missing


def extra_validation_load_collection(
    collection_id: str,
    load_params: LoadParameters,
    env: EvalEnv,
    *,
    global_extent_provider: Optional[GlobalExtentProvider] = None,
) -> Iterable[dict]:
    if collection_id == "TestCollection-LonLat4x4":
        # No need to check on artificial debug layer
        return
    if "backend_implementation" not in env:
        yield {"code": "NoBackendImplementation", "message": "It seems like you are running in a test environment"}
        return
    catalog = env.backend_implementation.catalog
    allow_check_missing_products = smart_bool(env.get("allow_check_missing_products", True))
    sync_job = smart_bool(env.get("sync_job", False))
    metadata_json = catalog.get_collection_metadata(collection_id=collection_id)
    metadata = GeopysparkCubeMetadata(metadata_json)
    large_layer_threshold_in_pixels = int(
        float(
            env.get(
                "large_layer_threshold_in_pixels",
                (
                    LARGE_LAYER_THRESHOLD_IN_PIXELS_SENTINELHUB
                    if potential_sentinelhub(catalog, collection_id)
                    else LARGE_LAYER_THRESHOLD_IN_PIXELS
                ),
            )
        )
    )
    load_params_tmp = copy(load_params)  # Make shallow copy, so we can remove data_mask
    load_params_tmp.data_mask = {}  # Clear to avoid SPARK-5063 error
    load_params = deepcopy(load_params_tmp)  # deepcopy so we can modify it
    load_params.temporal_extent = normalize_temporal_extent(catalog.derive_temporal_extent(collection_id, load_params))
    temporal_extent = load_params.temporal_extent

    spatial_extent = load_params.spatial_extent
    if spatial_extent is None or len(spatial_extent) == 0:
        if global_extent_provider is not None:
            spatial_extent = global_extent_provider(load_params=load_params, env=env)
            if spatial_extent:
                spatial_extent = spatial_extent.as_dict()
    if spatial_extent is None or len(spatial_extent) == 0:
        spatial_extent = metadata.get_overall_spatial_extent()
    load_params.spatial_extent = spatial_extent

    native_crs = metadata.get("cube:dimensions", "x", "reference_system", default="EPSG:4326")
    if isinstance(native_crs, dict):
        native_crs = native_crs.get("id", {}).get("code", None)
    if isinstance(native_crs, int):
        native_crs = f"EPSG:{native_crs}"
    if not isinstance(native_crs, str):
        yield {
            "code": "InvalidNativeCRS",
            "message": f"Invalid native CRS {native_crs!r} for " f"collection {collection_id!r}",
        }
        return

    number_of_temporal_observations: int = catalog.estimate_number_of_temporal_observations(
        collection_id,
        load_params,
    )

    band_names = load_params.bands
    if band_names:
        # Will convert aliases:
        band_names = metadata.filter_bands(band_names).band_names
    else:
        band_names = metadata.get("cube:dimensions", "bands", "values", default=["_at_least_assume_one_band_"])
    nr_bands = len(band_names)

    if collection_id == "TestCollection-LonLat4x4":
        # This layer is always 4x4 pixels, adapt resolution accordingly
        bbox_width = abs(spatial_extent["east"] - spatial_extent["west"])
        bbox_height = abs(spatial_extent["north"] - spatial_extent["south"])
        cell_width_latlon = bbox_width / 4
        cell_height_latlon = bbox_height / 4
        cell_width, cell_height = reproject_cellsize(
            spatial_extent,
            (cell_width_latlon, cell_height_latlon),
            "EPSG:4326",
            "Auto42001",
        )
    else:
        # The largest GSD I encountered was 25km. Double it as very permissive guess:
        default_gsd = (50000, 50000)
        gsd_object = metadata.get_GSD_in_meters()
        if isinstance(gsd_object, dict):
            gsd_in_meter_list = list(map(lambda x: gsd_object.get(x), band_names))
            gsd_in_meter_list = list(filter(lambda x: x is not None, gsd_in_meter_list))
            if not gsd_in_meter_list:
                gsd_in_meter_list = [default_gsd] * nr_bands
        elif isinstance(gsd_object, tuple):
            gsd_in_meter_list = [gsd_object] * nr_bands
        else:
            gsd_in_meter_list = [default_gsd] * nr_bands

        # We need to convert GSD to resolution in order to take an average:
        px_per_m2_average_band = sum(map(lambda x: 1 / (x[0] * x[1]), gsd_in_meter_list)) / len(gsd_in_meter_list)
        px_per_m_average_band = pow(px_per_m2_average_band, 0.5)
        m_per_px_average_band = 1 / px_per_m_average_band

        res = (m_per_px_average_band, m_per_px_average_band)
        # Auto42001 is in meter
        cell_width, cell_height = reproject_cellsize(spatial_extent, res, "Auto42001", native_crs)

    is_layer_too_large_message = is_layer_too_large(
        load_params=load_params,
        number_of_temporal_observations=number_of_temporal_observations,
        nr_bands=nr_bands,
        cell_width=cell_width,
        cell_height=cell_height,
        native_crs=native_crs,
        threshold_pixels=large_layer_threshold_in_pixels,
        sync_job=sync_job,
    )
    if is_layer_too_large_message:
        yield {"code": "ExtentTooLarge", "message": f"collection_id {collection_id!r}: {is_layer_too_large_message}"}
    elif allow_check_missing_products and metadata.get("_vito", "data_source", "check_missing_products", default=None):
        # Only check missing products when extent is not too large
        properties = load_params.properties

        products = check_missing_products(
            collection_metadata=metadata,
            temporal_extent=temporal_extent,
            spatial_extent=spatial_extent,
            properties=properties,
        )
        if products:
            for p in products:
                yield {
                    "code": "MissingProduct",
                    "message": f"Tile {p!r} in collection {collection_id!r} is not available."
                }

def is_layer_too_large(
        load_params: LoadParameters,
        number_of_temporal_observations: int,
        nr_bands: int,
        cell_width: float,
        cell_height: float,
        native_crs: str,
        threshold_pixels: int = LARGE_LAYER_THRESHOLD_IN_PIXELS,
        sync_job: bool = False,
):
    """
    Estimates the number of pixels that will be required to load this layer
    and returns True if it exceeds the threshold.

    :param load_params: Requested load parameters.
    :param number_of_temporal_observations: Requested number of temporal observations.
    :param nr_bands: Requested number of bands.
    :param cell_width: Width of the cells/pixels.
    :param cell_height: Height of the cells/pixels.
    :param native_crs: Native CRS of the layer.
    :param threshold_pixels: Threshold in pixels.
    :param sync_job: Is sync job.

    :return: A message if the layer exceeds the threshold in pixels. None otherwise.
             Also returns the estimated number of pixels and the threshold.
    """
    geometries = load_params.aggregate_spatial_geometries
    spatial_extent = load_params.spatial_extent
    srs = spatial_extent.get("crs", 'EPSG:4326')
    if isinstance(srs, int):
        srs = 'EPSG:%s' % str(srs)
    elif isinstance(srs, dict):
        if srs["name"] == 'AUTO 42001 (Universal Transverse Mercator)':
            srs = 'Auto42001'

    spatial_extent["crs"] = srs
    if not health_check_extent(spatial_extent):
        return f"Unsupported spatial extent: {spatial_extent}"

    target_crs = native_crs
    # Resampling process overwrites native_crs and resolution from metadata.
    if load_params.target_resolution is not None:
        # This can happen with e.g. resample_spatial(resolution=0, projection=4326)
        if load_params.target_resolution[0] != 0.0 and load_params.target_resolution[1] != 0.0:
            cell_width = float(load_params.target_resolution[0])
            cell_height = float(load_params.target_resolution[1])
            if load_params.target_crs is not None:
                target_crs = load_params.target_crs

    if isinstance(target_crs, dict):
        target_crs = target_crs.get("id", {}).get("code", None)
    if target_crs is None:
        raise InternalException("No native CRS found during is_layer_too_large check.")
    if target_crs == "Auto42001":
        west, south = spatial_extent["west"], spatial_extent["south"]
        east, north = spatial_extent["east"], spatial_extent["north"]
        target_crs = "EPSG:%s" % str(auto_utm_epsg_for_geometry(box(west, south, east, north), srs))
    else:
        target_crs = pyproj.CRS.from_user_input(target_crs).to_epsg()
    if isinstance(target_crs, int):
        target_crs = "EPSG:%s" % str(target_crs)
    if srs != target_crs:
        spatial_extent = reproject_bounding_box(spatial_extent, from_crs=srs, to_crs=target_crs)

    bbox_width = abs(spatial_extent["east"] - spatial_extent["west"])
    bbox_height = abs(spatial_extent["north"] - spatial_extent["south"])

    pixels_width = bbox_width / cell_width
    pixels_height = bbox_height / cell_height
    if sync_job and (pixels_width > 20000 or pixels_height > 20000) and not geometries:
        return f"Requested spatial extent is too large for a sync job {pixels_width:.0f}x{pixels_height:.0f} pixels. Max size: (20000x20000)."

    estimated_pixels = (bbox_width * bbox_height) / (cell_width * cell_height) * number_of_temporal_observations * nr_bands
    logger.debug(
        f"is_layer_too_large {estimated_pixels=} {threshold_pixels=} ({bbox_width=} {bbox_height=} {cell_width=} {cell_height=} {number_of_temporal_observations=} {nr_bands=})"
    )
    if estimated_pixels > threshold_pixels:
        if geometries and not isinstance(geometries, dict):
            # Threshold is exceeded, but only the pixels in the geometries will be loaded if they are provided.
            # For performance, we estimate the area using a simple bounding box around each polygon.
            if isinstance(geometries, DriverVectorCube):
                geometries_area = calculate_rough_area([geometries.to_multipolygon()])
            elif isinstance(geometries, DelayedVector):
                geometries_area = calculate_rough_area(geometries.geometries)
            elif isinstance(geometries, BaseGeometry):
                geometries_area = calculate_rough_area([geometries])
            else:
                raise TypeError(f"Unsupported geometry type: {type(geometries)}")
            if target_crs != "EPSG:4326":
                # Geojson is always in 4326. Reproject the cell bbox from native to 4326 so we can calculate the area.
                cell_bbox = {"west": 0, "east": cell_width, "south": 0, "north": cell_height, "crs": target_crs}
                cell_bbox = reproject_bounding_box(cell_bbox, from_crs=target_crs, to_crs="EPSG:4326")
                cell_width = abs(cell_bbox["east"] - cell_bbox["west"])
                cell_height = abs(cell_bbox["north"] - cell_bbox["south"])
            surface_area_pixels = geometries_area / (cell_width * cell_height)
            estimated_pixels = surface_area_pixels * number_of_temporal_observations * nr_bands
            logger.debug(
                f"is_layer_too_large {estimated_pixels=} {threshold_pixels=} ({geometries_area=} {cell_width=} {cell_height=} {number_of_temporal_observations=} {nr_bands=})"
            )
            if estimated_pixels <= threshold_pixels:
                return None
        return f"Requested extent is too large to process. Estimated number of pixels: {estimated_pixels:.2e}, " + \
            f"threshold: {threshold_pixels:.2e}."
    return None
