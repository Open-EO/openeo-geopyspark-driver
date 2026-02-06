import json
import logging
import os
import pathlib
import sys
from datetime import datetime
from functools import partial
from glob import glob
from typing import Dict, List, Optional, Tuple, Union

import geopyspark
import numpy as np
import pyspark
import shapely.geometry
import xarray as xr
from openeo_driver.errors import InternalException, OpenEOApiException
from openeo_driver.util.geometry import BoundingBox
from pyspark import SparkContext, find_spark_home
from scipy.spatial import cKDTree  # used for tuning the griddata interpolation settings

from openeogeotrellis.collections import convert_scala_metadata
from openeogeotrellis.load_stac import PropertyFilterPGMap
from openeogeotrellis.utils import ensure_executor_logging, get_jvm
from openeogeotrellis.collections import binning

OLCI_PRODUCT_TYPE = "OL_1_EFR___"
SYNERGY_PRODUCT_TYPE = "SY_2_SYN___"
SLSTR_PRODUCT_TYPE = "SL_2_LST___"

REPROJECTION_TYPE_BINNING = "binning"
KEY_REPROJECTION_TYPE = "reprojection_type"
KEY_SUPER_SAMPLING = "super_sampling"
KEY_FLAG_BAND = "flag_band"
KEY_FLAG_BITMASK = "flag_bitmask"

RIM_PIXELS = 60
DEFAULT_REPROJECTION_TYPE = "nearest_neighbor"
DEFAULT_SUPER_SAMPLING = 1 # no super sampling
DEFAULT_FLAG_BITMASK = 0xff


logger = logging.getLogger(__name__)


def _map_attributes_for_stac(attribute_values: Dict[str, any]) -> Dict[str, any]:
    """
    Map opensearch attribute keys and values to STAC equivalents for Sentinel-3.

    :param attribute_values: Dictionary of opensearch attribute key-value pairs
    :return: Dictionary with STAC-equivalent keys and values
    """
    attribute_keys_mapping = {
        "productType": "product:type",
    }
    # No value transformations needed for Sentinel-3 currently
    attribute_values_mapping = {
        "productType": lambda v: v,
    }

    mapped = {}
    for k, v in attribute_values.items():
        if k in attribute_keys_mapping:
            mapped_key = attribute_keys_mapping[k]
            mapped_value = (
                attribute_values_mapping[k](v)
                if k in attribute_values_mapping
                else v
            )
            mapped[mapped_key] = mapped_value
        else:
            logger.warning(f"sentinel3: No mapping for attribute key {k!r}")
    return mapped


def _map_attributes_to_property_filter(attribute_values: Dict[str, any]) -> PropertyFilterPGMap:
    """Convert attribute values to openEO style process graph property filters."""
    mapped = {}
    for k, v in attribute_values.items():
        # Note, only `eq` operator is supported.
        mapped[k] = {
            "process_graph": {
                "eq": {
                    "process_id": "eq",
                    "arguments": {"x": {"from_parameter": "value"}, "y": v},
                    "result": True,
                }
            }
        }
    return mapped


def _get_acquisition_key(item) -> tuple:
    """
    Get unique acquisition key from STAC item for deduplication.

    Uses platform and absolute orbit number, which uniquely identify a specific
    satellite pass regardless of processing timeliness category (NRT/NTC).
    This is more robust than parsing product IDs as it works regardless of
    product ID format changes and is available for all Sentinel-3 products.

    :param item: STAC item with properties
    :return: Tuple of (platform, sat:absolute_orbit) that uniquely identifies the acquisition
    """
    return (
        item.properties.get("platform"),
        item.properties.get("sat:absolute_orbit")
    )


def deduplicate_items_prefer_ntc(items_by_collection: Dict[str, list]) -> list:
    """
    Deduplicate STAC items with priority: NTC > STC > NRT.

    When the same acquisition exists in multiple collections (different timeliness),
    only the highest priority version is kept:
    - NTC (Non Time Critical): Highest quality, best calibration
    - STC (Short Time Critical): Medium priority, rapid delivery
    - NRT (Near Real Time): Lowest priority, fastest availability

    Example:
        >>> items = deduplicate_items_prefer_ntc({
        ...     "NTC": [ntc_item_tuple],
        ...     "STC": [stc_item_tuple],
        ...     "NRT": [nrt_item_tuple_1, nrt_item_tuple_2]
        ... })

    :param items_by_collection: Dictionary with keys "NRT", "STC", and/or "NTC",
        values are lists of (item, band_assets) tuples
    :return: List of deduplicated (item, band_assets) tuples
    """
    # Build lookup of higher priority acquisitions
    ntc_acquisition_keys = set()
    for itm, _ in items_by_collection.get("NTC", []):
        ntc_acquisition_keys.add(_get_acquisition_key(itm))

    stc_acquisition_keys = set()
    for itm, _ in items_by_collection.get("STC", []):
        stc_acquisition_keys.add(_get_acquisition_key(itm))

    # Collect deduplicated items with priority order
    deduplicated_items = []

    # 1. Add all NTC items (highest priority)
    ntc_items = items_by_collection.get("NTC", [])
    deduplicated_items.extend(ntc_items)

    # 2. Add STC items only if not already in NTC
    stc_items = items_by_collection.get("STC", [])
    stc_skipped = 0
    for itm, band_assets in stc_items:
        acquisition_key = _get_acquisition_key(itm)
        if acquisition_key in ntc_acquisition_keys:
            stc_skipped += 1
            logger.debug(f"Skipping STC item {itm.id} - NTC version exists")
        else:
            deduplicated_items.append((itm, band_assets))

    # 3. Add NRT items only if not already in NTC or STC
    nrt_items = items_by_collection.get("NRT", [])
    nrt_skipped = 0
    for itm, band_assets in nrt_items:
        acquisition_key = _get_acquisition_key(itm)
        if acquisition_key in ntc_acquisition_keys:
            nrt_skipped += 1
            logger.debug(f"Skipping NRT item {itm.id} - NTC version exists")
        elif acquisition_key in stc_acquisition_keys:
            nrt_skipped += 1
            logger.debug(f"Skipping NRT item {itm.id} - STC version exists")
        else:
            deduplicated_items.append((itm, band_assets))

    # Log deduplication summary
    if stc_skipped > 0:
        logger.info(f"Deduplicated: skipped {stc_skipped} STC items that exist as NTC (higher quality)")
    if nrt_skipped > 0:
        logger.info(f"Deduplicated: skipped {nrt_skipped} NRT items that exist as NTC/STC (higher quality)")

    stc_kept = len(stc_items) - stc_skipped
    nrt_kept = len(nrt_items) - nrt_skipped
    logger.info(f"Total deduplicated items: {len(deduplicated_items)} (NTC: {len(ntc_items)}, STC: {stc_kept}, NRT: {nrt_kept})")

    return deduplicated_items


def _get_stac_collection_urls(product_type: str) -> List[str]:
    """Get STAC collection URLs for the given Sentinel-3 product type.

    Returns a list of URLs because some products have multiple timeliness categories:
    - NRT (Near Real Time): Available within ~3 hours, for recent data
    - NTC (Non Time Critical): Available after days/weeks, consolidated/higher quality
    - STC (Short Time Critical): Available within hours/days, rapid delivery

    Different products use different timeliness combinations:
    - SLSTR LST: NRT + NTC (no STC)
    - SYNERGY SYN: NTC + STC (no NRT - synergy requires consolidated L1B inputs)
    """
    # TODO: move this to layercatalog.json
    if product_type == SLSTR_PRODUCT_TYPE:
        return [
            "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-3-sl-2-lst-nrt",
            "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-3-sl-2-lst-ntc",
        ]
    elif product_type == SYNERGY_PRODUCT_TYPE:
        return [
            "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-3-syn-2-syn-stc",
            "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-3-syn-2-syn-ntc",
        ]
    else:
        raise ValueError(f"STAC not yet supported for Sentinel-3 product type: {product_type}")


def _build_stac_opensearch_client(
    metadata_properties: Dict[str, any],
    spatial_extent: Union[Dict, BoundingBox, None],
    temporal_extent: Tuple[Optional[str], Optional[str]],
    jvm,
) -> any:
    """Build a FixedFeaturesOpenSearchClient populated with features from STAC API.

    Queries multiple STAC collections (e.g., NRT and NTC for SLSTR LST) and merges results.
    """
    from openeogeotrellis.load_stac import _spatiotemporal_extent_from_load_params
    from openeogeotrellis.load_stac import construct_item_collection

    product_type = metadata_properties["productType"]
    urls = _get_stac_collection_urls(product_type)

    # Map opensearch attributes to STAC properties
    stac_attributes = _map_attributes_for_stac(metadata_properties)
    property_filter_pg_map = _map_attributes_to_property_filter(stac_attributes)

    # Build spatiotemporal extent
    spatiotemporal_extent = _spatiotemporal_extent_from_load_params(
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent
    )

    # Build FixedFeaturesOpenSearchClient from STAC items
    opensearch_client = jvm.org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient()

    # Query all STAC collections and collect items with their timeliness category
    items_by_collection = {}  # collection_name -> list of (item, band_assets)
    for url in urls:
        # Detect collection type from URL
        url_lower = url.lower()
        if "nrt" in url_lower:
            collection_name = "NRT"
        elif "stc" in url_lower:
            collection_name = "STC"
        else:
            collection_name = "NTC"
        logger.info(f"Querying STAC collection {collection_name}: {url}")
        try:
            # Query STAC API
            item_collection, metadata, collection_band_names, netcdf_with_time_dimension = construct_item_collection(
                url=url,
                spatiotemporal_extent=spatiotemporal_extent,
                property_filter_pg_map=property_filter_pg_map,
            )
            logger.info(f"Found {len(item_collection.items)} items in {collection_name}")
            items_by_collection[collection_name] = list(item_collection.iter_items_with_band_assets())
        except Exception as e:
            # Log but don't fail if one collection fails - the other might still have data
            logger.warning(f"Failed to query STAC collection {collection_name}: {e}")
            items_by_collection[collection_name] = []

    # Deduplicate: NTC takes precedence over NRT
    all_items = deduplicate_items_prefer_ntc(items_by_collection)

    # Build features from all items
    for itm, band_assets in all_items:
        builder = (
            jvm.org.openeo.opensearch.OpenSearchResponses.featureBuilder()
            .withNominalDate(itm.properties.get("datetime") or itm.properties["start_datetime"])
            .withGeometryFromWkt(str(shapely.geometry.shape(itm.geometry)))
        )
        if not itm.bbox:
            raise OpenEOApiException(f"Item {itm.id} has no bbox")
        latlon_bbox = BoundingBox.from_wsen_tuple(itm.bbox, 4326)
        builder = builder.withBBox(*map(float, latlon_bbox.as_wsen_tuple()))

        # Extract product ID from asset href
        # Example: s3://eodata/Sentinel-3/SLSTR/SL_2_LST___/2026/01/10/S3B_SL_2_LST____20260110T142415_20260110T142715_20260111T184448_0179_115_238_5400_ESA_O_NT_004.SEN3/LST_in.nc
        product_id = None
        for asset_id, asset in band_assets.items():
            href = asset.href
            # Convert s3:// to path
            href = href.replace("s3://", "/")
            if ".SEN3" in href:
                product_id = href.split(".SEN3")[0] + ".SEN3"
                break

        if product_id is None:
            raise OpenEOApiException(f"No .SEN3 product path found in item {itm.id} assets")

        builder = builder.withId(product_id)
        opensearch_client.addFeature(builder.build())

    return opensearch_client


def _build_legacy_opensearch_client(jvm) -> any:
    """Build the legacy OpenSearch client for Copernicus Dataspace."""
    return jvm.org.openeo.opensearch.OpenSearchClient.apply(
        "https://catalogue.dataspace.copernicus.eu/resto", False, "", [], "",
    )


def main(product_type, native_resolution, bbox, from_date, to_date, band_names, use_stac_client: bool = False):
    spark_python = os.path.join(find_spark_home._find_spark_home(), 'python')
    py4j = glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
    sys.path[:0] = [spark_python, py4j]

    from geopyspark.geotrellis import Extent, LayoutDefinition, TileLayout

    conf = geopyspark.geopyspark_conf(master="local[1]", appName=__file__)
    conf.set('spark.kryo.registrator', 'geotrellis.spark.store.kryo.KryoRegistrator')
    conf.set('spark.driver.extraJavaOptions', "-Dlog4j2.debug=false -Dlog4j2.configurationFile=file:/home/bossie/PycharmProjects/openeo/openeo-python-driver/log4j2.xml")
    conf.set("spark.ui.enabled", "true")

    with SparkContext(conf=conf):
        jvm = get_jvm()

        metadata_properties = {"productType": product_type}

        extent = jvm.geotrellis.vector.Extent(bbox.west, bbox.south, bbox.east, bbox.north)
        epsg_code = bbox.crs
        projected_polygons_native_crs = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(extent, epsg_code)

        data_cube_parameters = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
        getattr(data_cube_parameters, "tileSize_$eq")(256)
        getattr(data_cube_parameters, "layoutScheme_$eq")("FloatingLayoutScheme")
        data_cube_parameters.setGlobalExtent(extent.xmin(), extent.ymin(), extent.xmax(), extent.ymax(), epsg_code)
        native_cell_size = jvm.geotrellis.raster.CellSize(native_resolution, native_resolution)
        feature_flags = {}

        layer = pyramid(metadata_properties, projected_polygons_native_crs, from_date, to_date, band_names,
                        data_cube_parameters, native_cell_size, feature_flags, jvm,
                        spatial_extent=bbox, use_stac_client=use_stac_client)[0]
        layer_crs = layer.srdd.rdd().metadata().crs().epsgCode().get()
        layer.to_spatial_layer().save_stitched(f"/tmp/{product_type}_{from_date}_{to_date}.tif",
                                               crop_bounds=geopyspark.geotrellis.Extent(*bbox.reproject(layer_crs).as_wsen_tuple()))
        return

        target_extent = Extent(*bbox.as_wsen_tuple())
        target_tileLayout = TileLayout(layoutCols=4, layoutRows=4, tileCols=256, tileRows=256)
        reprojected_layer = layer.tile_to_layout(LayoutDefinition(target_extent, target_tileLayout), target_crs=epsg_code)

        reprojected_layer_crs = reprojected_layer.srdd.rdd().metadata().crs().epsgCode().get()
        reprojected_layer.to_spatial_layer().save_stitched(
            f"/tmp/{product_type}_{from_date}_{to_date}_reprojected.tif",
            crop_bounds=geopyspark.geotrellis.Extent(*bbox.reproject(reprojected_layer_crs).as_wsen_tuple()),
        )


def pyramid(metadata_properties, projected_polygons_native_crs, from_date, to_date, band_names, data_cube_parameters,
            native_cell_size, feature_flags, jvm, spatial_extent=None, use_stac_client: bool = False):

    reprojection_type = feature_flags.get(KEY_REPROJECTION_TYPE, DEFAULT_REPROJECTION_TYPE)
    binning_args = {
        KEY_SUPER_SAMPLING: feature_flags.get(KEY_SUPER_SAMPLING, DEFAULT_SUPER_SAMPLING),
        KEY_FLAG_BAND: feature_flags.get(KEY_FLAG_BAND, None),
    }
    if KEY_FLAG_BITMASK in feature_flags:
        binning_args[KEY_FLAG_BITMASK] = feature_flags[KEY_FLAG_BITMASK]

    # Build opensearch client based on selection
    if use_stac_client:
        logger.info("Using STAC-based opensearch client (FixedFeaturesOpenSearchClient)")
        opensearch_client = _build_stac_opensearch_client(
            metadata_properties=metadata_properties,
            spatial_extent=spatial_extent,
            temporal_extent=(from_date, to_date),
            jvm=jvm
        )
    else:
        logger.info("Using legacy opensearch client")
        opensearch_client = _build_legacy_opensearch_client(jvm)

    collection_id = "Sentinel3"
    product_type = metadata_properties["productType"]
    correlation_id = ""

    latlng_crs = jvm.geotrellis.proj4.CRS.fromEpsgCode(4326)

    if projected_polygons_native_crs.crs() != latlng_crs:
        projected_polygons_native_crs = projected_polygons_native_crs.reproject(latlng_crs)

        if data_cube_parameters.globalExtent().isDefined():
            global_extent_latlng = data_cube_parameters.globalExtent().get().reproject(latlng_crs)
            data_cube_parameters.setGlobalExtent(
                global_extent_latlng.xmin(),
                global_extent_latlng.ymin(),
                global_extent_latlng.xmax(),
                global_extent_latlng.ymax(),
                "EPSG:4326",
            )

    file_rdd_factory = jvm.org.openeo.geotrellis.file.FileRDDFactory(
        opensearch_client, collection_id, metadata_properties, correlation_id, native_cell_size
    )

    zoom = 0
    tile_size = data_cube_parameters.tileSize()

    # loadSpatialFeatureJsonRDD returns the (id, bbox, nominalDate, links) for each feature,
    # as well as the metadata (extent and crs_epsg) necessary to construct the layer metadata in Python.
    # Note that the feature.id is assumed to be the creo_path to the product, which is used in the read_product function to read the data from disk.
    keyed_feature_rdd = file_rdd_factory.loadSpatialFeatureJsonRDD(
        projected_polygons_native_crs, from_date, to_date, zoom, tile_size, data_cube_parameters
    )

    jrdd = keyed_feature_rdd._1()
    metadata_sc = keyed_feature_rdd._2()

    j2p_rdd = jvm.SerDe.javaToPython(jrdd)
    serializer = pyspark.serializers.PickleSerializer()
    pyrdd = geopyspark.create_python_rdd(j2p_rdd, serializer=serializer)
    pyrdd = pyrdd.map(json.loads)

    layer_metadata_py = convert_scala_metadata(metadata_sc, epoch_ms_to_datetime=_instant_ms_to_minute, logger=logger)

    # -----------------------------------
    prefix = ""

    def process_feature(feature: dict) -> Tuple[str, dict]:
        creo_path = prefix + feature["feature"]["id"]
        return creo_path, {
            "key": feature["key"],
            "key_extent": feature["key_extent"],
            "bbox": feature["feature"]["bbox"],
            "key_epsg": feature["metadata"]["crs_epsg"]
        }

    per_product = pyrdd.map(process_feature).groupByKey().mapValues(list)

    creo_paths = per_product.keys().collect()

    assert native_cell_size.width() == native_cell_size.height()
    tile_rdd = per_product.partitionBy(numPartitions=len(creo_paths), partitionFunc=creo_paths.index).flatMap(
        partial(
            read_product,
            product_type=product_type,
            band_names=band_names,
            tile_size=tile_size,
            resolution=native_cell_size.width(),
            reprojection_type=reprojection_type,
            binning_args=binning_args,

        )
    )

    logger.info("Constructing TiledRasterLayer from numpy rdd, with metadata {m!r}".format(m=layer_metadata_py))

    tile_layer = geopyspark.TiledRasterLayer.from_numpy_rdd(
        layer_type=geopyspark.LayerType.SPACETIME, numpy_rdd=tile_rdd, metadata=layer_metadata_py
    )
    # Merge any keys that have more than one tile.
    contextRDD = jvm.org.openeo.geotrellis.OpenEOProcesses().mergeTiles(tile_layer.srdd.rdd())
    temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
    srdd = temporal_tiled_raster_layer.apply(jvm.scala.Option.apply(zoom), contextRDD)
    merged_tile_layer = geopyspark.TiledRasterLayer(geopyspark.LayerType.SPACETIME, srdd)

    return {zoom: merged_tile_layer}


def _instant_ms_to_hour(instant: int) -> datetime:
    """
    Convert Geotrellis SpaceTimeKey instant (Scala Long, millisecond resolution) to Python datetime object,
    rounded down to hour resolution, a convention used in other places
    of our openEO backend implementation and necessary to follow, for example
    to ensure that timeseries related data joins work properly.

    Sentinel-3 can have many observations per day, warranting the choice of hourly rather than daily aggregation
    """
    return datetime(*(datetime.utcfromtimestamp(instant // 1000).timetuple()[:4]))

def _instant_ms_to_minute(instant: int) -> datetime:
    """
    Convert Geotrellis SpaceTimeKey instant (Scala Long, millisecond resolution) to Python datetime object,
    rounded down to minute resolution, a convention used in other places
    of our openEO backend implementation and necessary to follow, for example
    to ensure that timeseries related data joins work properly.

    Sentinel-3 can have many observations per day, warranting the choice of fine grained aggregation rather than daily aggregation
    """
    return datetime(*(datetime.utcfromtimestamp(instant // 1000).timetuple()[:5]))


@ensure_executor_logging
def read_product(product, product_type, band_names, tile_size, resolution, reprojection_type=DEFAULT_REPROJECTION_TYPE, binning_args=None):
    from openeogeotrellis.collections.s1backscatter_orfeo import get_total_extent

    creo_path, features = product  # better: "tiles"
    log_prefix = ""

    creo_path = pathlib.Path(creo_path)
    if not creo_path.exists():
        raise Exception(f"read_product: path {creo_path} does not exist.")

    # Get whole extent of tile layout
    col_min = min(f["key"]["col"] for f in features)
    col_max = max(f["key"]["col"] for f in features)
    cols = col_max - col_min + 1
    row_min = min(f["key"]["row"] for f in features)
    row_max = max(f["key"]["row"] for f in features)
    rows = row_max - row_min + 1
    MAX_KEYS=int(2048/tile_size)
    instants = set(f["key"]["instant"] for f in features)
    assert len(instants) == 1, f"Not single instant: {instants}"
    instant = instants.pop()
    logger.info(
        f"{log_prefix} Layout key extent: col[{col_min}:{col_max}] row[{row_min}:{row_max}]"
        f" ({cols}x{rows}={cols * rows} tiles) instant[{instant}]."
    )

    key_epsgs = set(f["key_epsg"] for f in features)
    assert len(key_epsgs) == 1, f"Multiple key CRSs {key_epsgs}"
    layout_epsg = key_epsgs.pop()

    tiles = []

    for col_start in range(col_min,col_max+1,MAX_KEYS):
        for row_start in range(row_min, row_max+1, MAX_KEYS):
            col_end = min(col_start+MAX_KEYS-1,col_max)
            row_end = min(row_start+MAX_KEYS-1,row_max)

            tiles_subset = [ f for f in features if f["key"]["col"] >= col_start and f["key"]["col"] <= col_end and f["key"]["row"] >= row_start and f["key"]["row"] <= row_end]

            if len(tiles_subset) == 0:
                continue

            # it is possible that the bounds of subset are smaller than the iteration bounds
            col_start = min(f["key"]["col"] for f in tiles_subset)
            col_end = max(f["key"]["col"] for f in tiles_subset)
            row_start = min(f["key"]["row"] for f in tiles_subset)
            row_end = max(f["key"]["row"] for f in tiles_subset)

            layout_extent = get_total_extent(tiles_subset)

            logger.info(
                f"{log_prefix} Layout extent {layout_extent} EPSG {layout_epsg}:"
            )


            digital_numbers = product_type == OLCI_PRODUCT_TYPE

            try:
                orfeo_bands = create_s3_toa(
                    product_type,
                    creo_path,
                    band_names,
                    [layout_extent["xmin"], layout_extent["ymin"], layout_extent["xmax"], layout_extent["ymax"]],
                    digital_numbers=digital_numbers,
                    final_grid_resolution=resolution,
                    reprojection_type=reprojection_type,
                    binning_args=binning_args or {},
                )
                if orfeo_bands is None:
                    continue
            except Exception as e:
                ex_type, ex_value, ex_traceback = sys.exc_info()
                msg = f"Failed to read Sentinel-3 {product_type} {band_names} for {creo_path} and extent {layout_extent}. Error: {ex_value}"
                logger.error(msg, exc_info=True)
                raise InternalException(msg)

            # Split output in tiles
            logger.info(f"{log_prefix} Split {orfeo_bands.shape} in tiles of {tile_size}")
            if not digital_numbers:
                nodata = np.nan

            elif product_type == OLCI_PRODUCT_TYPE:
                nodata = 65535
            elif product_type == SYNERGY_PRODUCT_TYPE:
                nodata = -10000
            elif product_type == SLSTR_PRODUCT_TYPE:
                nodata = -32768
            else:
                raise ValueError(product_type)

            if digital_numbers:
                cell_type = geopyspark.CellType.create_user_defined_celltype("int32", nodata)  # TODO: check if right
            else:
                cell_type = geopyspark.CellType.FLOAT32

            for f in tiles_subset:
                col = f["key"]["col"]
                row = f["key"]["row"]
                c = col - col_start
                r = row - row_start

                key = geopyspark.SpaceTimeKey(col=col, row=row, instant=_instant_ms_to_minute(instant))
                tile = orfeo_bands[:, r * tile_size:(r + 1) * tile_size, c * tile_size:(c + 1) * tile_size]
                if not (tile == nodata).all():
                    logger.debug(f"{log_prefix} Create Tile for key {key} from {tile.shape}")
                    tile = geopyspark.Tile(tile, cell_type, no_data_value=nodata)
                    tiles.append((key, tile))

            logger.info(f"{log_prefix} Layout extent split in {len(tiles)} tiles")

    return tiles


def create_s3_toa(product_type, creo_path, band_names, bbox_tile, digital_numbers: bool, final_grid_resolution: float, reprojection_type=DEFAULT_REPROJECTION_TYPE, binning_args=None):
    if product_type == OLCI_PRODUCT_TYPE:
        geofile = 'geo_coordinates.nc'
        lat_band = 'latitude'
        lon_band = 'longitude'
    elif product_type == SYNERGY_PRODUCT_TYPE:
        geofile = 'geolocation.nc'
        lat_band = 'lat'
        lon_band = 'lon'
    elif product_type == SLSTR_PRODUCT_TYPE:
        geofile = 'geodetic_in.nc'
        lat_band = 'latitude_in'
        lon_band = 'longitude_in'
    elif product_type in ["OL_2_LFR___", "OL_2_WFR___"]:
        geofile = 'geo_coordinates.nc'
        lat_band = 'latitude'
        lon_band = 'longitude'
    elif product_type == "SY_2_AOD___":
        geofile = 'NTC_AOD.nc'
        lat_band = 'latitude'
        lon_band = 'longitude'
    else:
        raise ValueError(product_type)

    tile_coordinates, tile_shape = create_final_grid(bbox_tile, resolution=final_grid_resolution)
    tile_coordinates.shape = tile_shape + (2,)

    geofile = os.path.join(creo_path, geofile)

    flatten_lat_lon = reprojection_type != REPROJECTION_TYPE_BINNING
    bbox_original, source_coordinates, data_mask = _read_latlonfile(bbox_tile, geofile, lat_band, lon_band, flatten=flatten_lat_lon)
    if source_coordinates is None:
        return None
    logger.info(f"{bbox_original=} {source_coordinates=}")

    if product_type == SLSTR_PRODUCT_TYPE:
        angle_geofile = os.path.join(creo_path, 'geodetic_tx.nc')
        _, angle_source_coordinates, angle_data_mask = _read_latlonfile(
            bbox_tile, angle_geofile, lat_band='latitude_tx', lon_band='longitude_tx',
            interpolation_margin=final_grid_resolution * RIM_PIXELS)

        tile_coordinates_with_rim, tile_shape_with_rim = create_final_grid(bbox_tile, resolution=final_grid_resolution,
                                                                           rim_pixels=RIM_PIXELS)
        tile_coordinates_with_rim.shape = tile_shape_with_rim + (2,)
    else:
        angle_source_coordinates = None
        angle_data_mask = None
        tile_coordinates_with_rim = None

    if reprojection_type in ["nearest_neighbour", "nearest_neighbor", "nn"]:
        reprojected_data, is_empty = do_reproject(product_type, final_grid_resolution, creo_path, band_names,
                                                  source_coordinates, tile_coordinates, data_mask,
                                                  angle_source_coordinates, tile_coordinates_with_rim, angle_data_mask,
                                                  digital_numbers)
    elif reprojection_type == REPROJECTION_TYPE_BINNING:
        _binning_args = binning_args or {}
        super_sampling = _binning_args.get(KEY_SUPER_SAMPLING, DEFAULT_SUPER_SAMPLING)
        flag_band = _binning_args.get(KEY_FLAG_BAND, None)
        flag_bitmask = _binning_args.get(KEY_FLAG_BITMASK, DEFAULT_FLAG_BITMASK)

        reprojected_data = do_binning(
            product_type,
            final_grid_resolution,
            creo_path,
            band_names,
            source_coordinates,
            tile_coordinates,
            data_mask,
            None,
            None,
            None,
            digital_numbers,
            super_sampling=super_sampling,
            flag_band=flag_band,
            flag_bitmask=flag_bitmask,
        )
    else:
        raise ValueError(f"reprojection_type must be either 'nearest_neighbor' or 'binning', found '{reprojection_type}'")
    return reprojected_data


def create_final_grid(final_bbox, resolution, rim_pixels=0 ):
    """
    Create a grid for a given bounding box and resolution.

    Optionally, a number of pixels can be given to create an extra rim on each
    side of the bounding box.

    :param final_bbox: List containing [final_xmin, final_ymin, final_xmax, final_ymax]
    :param resolution: The resolution of the grid
    :param rim_pixels: The number of pixels to create an extra rim in each side of the array, default=0
    :return: Tuple of (target_coordinates, target_shape) where target_coordinates is a 2D-array
        containing the final coordinates of pixel centers and target_shape is a tuple containing
        the number of rows/columns
    """

    final_xmin, final_ymin, final_xmax, final_ymax = final_bbox
    #the boundingbox of the tile seems to missing the last pixels, that is why we add 1 resolution to the second argument
    # target coordinates have to be computed as pixel centers
    grid_x, grid_y = np.meshgrid(
        np.arange(final_xmin + 0.5 * resolution - (rim_pixels * resolution),
                  final_xmax - 0.5 * resolution + resolution + (rim_pixels * resolution), resolution),
        np.arange(final_ymax - 0.5 * resolution + (rim_pixels * resolution),
                  final_ymin + 0.5 * resolution - resolution - (rim_pixels * resolution),
                  -resolution)  # without lat mirrored
        # np.arange(ref_ymin, ref_ymax, self.final_grid_resolution)   #with lat mirrored
    )
    target_shape = grid_x.shape
    target_coordinates = np.column_stack((grid_x.ravel(), grid_y.ravel()))
    return target_coordinates, target_shape

def do_binning(
        product_type,
        final_grid_resolution,
        creo_path: pathlib.Path,
        band_names,
        source_coordinates,
        target_coordinates,
        data_mask,
        angle_source_coordinates,
        target_coordinates_with_rim,
        angle_data_mask,
        digital_numbers=True,
        super_sampling: int=DEFAULT_SUPER_SAMPLING,
        flag_band: str=None,
        flag_bitmask: np.uint16=DEFAULT_FLAG_BITMASK,
    ):
    """
    Perform binning of Sentinel-3 data to a regular grid.

    Assumes that we have a lat/lon grid. Does not support angles.

    :param product_type: Sentinel-3 product type
    :param final_grid_resolution: Resolution of the target grid
    :param creo_path: Path to the Sentinel-3 product
    :param band_names: List of band names to process
    :param source_coordinates: Source coordinate grid
    :param target_coordinates: Target coordinate grid
    :param data_mask: Mask for valid data
    :param angle_source_coordinates: Source coordinates for angle bands (not supported)
    :param target_coordinates_with_rim: Target coordinates including rim pixels
    :param angle_data_mask: Mask for angle data
    :param digital_numbers: If True, keep digital numbers; if False, apply scaling
    :param super_sampling: Super sampling applied to the coordinate variables (linear super sampling)
        and band values (repeating). When binning values on a grid to finer or similarly scaled grid,
        not all output pixels may be assigned a value if no super sampling is applied. This leads to
        missing values in the output. Super sampling interpolates the input grid to fill these gaps.
        The larger the relative difference between pixel sizes, the higher the value must be set to
        avoid missing values. Must be larger or equal to one (default is 1, implying no super sampling)
    :param flag_band: If provided, interpret this band as flags to be compared with flag_bitmask.
        Pixels in all bands except flag_band will be set to their fill value (NaN by default) if
        the corresponding flag_band pixel has any bit in common with flag_bitmask, i.e.
        pixel_value & flag_bitmask > 0. The band specified as flag_band will not be included
        in the output (default is None, no band will be used for masking)
    :param flag_bitmask: Bitmask to identify invalid pixels in flag_band. Pixels are considered invalid
        if their flag_band value has any bits set that are also set in flag_bitmask. This means that
        a flag value of zero cannot be masked with this mechanism. Ignored if flag_bands is not set
        (default is 0xff)
    :return: Array of binned band data
    """
    logger.info(f"Binning {product_type} with super sampling '{super_sampling}' flag band '{flag_band}' and flag bitmask '{flag_bitmask}'")
    if angle_source_coordinates is not None:
        raise NotImplementedError("Binning for angle coordinates is not supported.")

    data_vars = {}
    dims = ("latitude", "longitude")
    attrs = {}
    coords = {
        "lon": (("y", "x"), source_coordinates[0, :]),
        "lat": (("y", "x"), source_coordinates[1, :]),
    }
    band_names_parsed = []

    for band_name in band_names:
        logger.info(" Reprojecting %s" % band_name)
        base_name = band_name
        if ":" in band_name:
            base_name, band_name = band_name.split(":")

        in_file = creo_path /  (base_name + '.nc')
        if product_type == SYNERGY_PRODUCT_TYPE and not band_name.endswith("_flags"):
                band_name = f"SDR_{band_name.split('_')[1]}"

        band_names_parsed.append(band_name)
        band_data, band_settings = read_band(in_file, in_band=band_name, data_mask=data_mask)

        # We are rescaling before the binning in order to correctly handle fill values (not identifiable after binning)
        if '_FillValue' in band_settings and not digital_numbers:
            band_data[band_data == band_settings['_FillValue']] = np.nan
        if 'add_offset' in band_settings and 'scale_factor' in band_settings and not digital_numbers:
            band_data = band_data.astype("float32") * band_settings['scale_factor'] + band_settings['add_offset']

        data_vars[band_name] = (dims, band_data)
        attrs[band_name] = band_settings

    if flag_band is not None:
        logger.info(f"Interpreting band '{flag_band}' as flag, with bitmask '{flag_bitmask}'")
        if flag_band not in band_names_parsed:
            raise ValueError(f"Specified flag_band '{flag_band}', which was not found in specified bands '{band_names_parsed}'")
        data_band_names = set(band_names_parsed) - {flag_band}

        flags = data_vars[flag_band][1]
        for band_name in data_band_names:
            fill_value = np.nan
            band_dims, band_data = data_vars[band_name]
            band_dtype = attrs[band_name]["dtype"]
            masked_band_data = xr.where(np.isnan(flags) | (flags.astype(band_dtype) & flag_bitmask) > 0, fill_value, band_data)
            data_vars[band_name] = (band_dims, masked_band_data)

    ds_in = xr.Dataset(
        data_vars, coords, attrs
    )

    #  second dimension is redundant...
    target_lon_centers = target_coordinates[0, :, 0]
    # binning expects lat to be increasing
    target_lat_centers = target_coordinates[::-1, 0, 1]
    target_lon_edges = binning.compute_edges_from_pixel_centers(target_lon_centers, final_grid_resolution)
    target_lat_edges = binning.compute_edges_from_pixel_centers(target_lat_centers, final_grid_resolution)
    binned = binning.bin_to_grid(ds_in, bands=data_vars.keys(), lat_edges=target_lat_edges, lon_edges=target_lon_edges, super_sampling=super_sampling)

    # because lat was flipped, we need to the values it back
    binned_data_vars = np.array([np.flip(var, axis=0) for var in binned.data_vars.values()])
    return binned_data_vars

def do_reproject(product_type, final_grid_resolution, creo_path, band_names,
                 source_coordinates, target_coordinates, data_mask,
                 angle_source_coordinates, target_coordinates_with_rim, angle_data_mask,
                 digital_numbers=True):
    """
    Create LUT for reprojecting, and reproject all specified bands.

    :param product_type: Sentinel-3 product type
    :param final_grid_resolution: Resolution of the target grid
    :param creo_path: Path to the Sentinel-3 product
    :param band_names: List of band names to reproject
    :param source_coordinates: Source coordinate grid
    :param target_coordinates: 2D numpy array representing the complete grid for the tile
    :param data_mask: Mask for valid data
    :param angle_source_coordinates: Source coordinates for angle bands
    :param target_coordinates_with_rim: Target coordinates including rim pixels
    :param angle_data_mask: Mask for angle data
    :param digital_numbers: If True, keep digital numbers; if False, apply scaling
    :return: Tuple of (reprojected_data, is_empty) where reprojected_data is an array of
        reprojected band data and is_empty is True if no valid data was found
    """

    is_empty = False
    logger.info(f"Reprojecting {product_type}")
    ### create LUT for radiances
    _, LUT = create_index_LUT(source_coordinates,
                                     target_coordinates,
                                     2 * final_grid_resolution)  # indices will have the same size as flattend grid_xy referring to the index from the latlon a data arrays

    ### create LUT for angle files
    if angle_source_coordinates is not None:
        _, LUT_angles = create_index_LUT(angle_source_coordinates,
                                         target_coordinates_with_rim,
                                         2 * final_grid_resolution)
    else:
        LUT_angles = None

    varOut = []

    for band_name in band_names:
        logger.info(" Reprojecting %s" % band_name)

        if ":" in band_name:
            base_name, band_name = band_name.split(":")
        else:
            base_name = band_name

        in_file = os.path.join(creo_path, base_name + '.nc')

        def variable_name(band_name: str):  # actually, the file without the .nc extension
            if product_type == OLCI_PRODUCT_TYPE:
                return band_name
            if product_type == SYNERGY_PRODUCT_TYPE:
                if band_name.endswith("_flags"):
                    return band_name
                else:
                    return f"SDR_{band_name.split('_')[1]}"
            if product_type == SLSTR_PRODUCT_TYPE:
                return band_name
            if product_type in ["OL_2_LFR___", "OL_2_WFR___", "SY_2_AOD___"]:
                return band_name
            raise ValueError(band_name)

        def readAndReproject(data_mask_,LUT):
            band_data, band_settings = read_band(in_file, in_band=variable_name(band_name), data_mask=data_mask_)
            flat = band_data.flatten()
            #inconvenient approach for compatibility with old xarray version
            flat_mask = data_mask_.where(data_mask_, drop=True).data.flatten()
            flat_mask[np.isnan(flat_mask)] = 0
            filtered = flat[flat_mask.astype(np.bool_)]
            return band_settings,apply_LUT_on_band(filtered, LUT, band_settings.get('_FillValue', None))

        if product_type == SLSTR_PRODUCT_TYPE and band_name in ["solar_azimuth_tn", "solar_zenith_tn", "sat_azimuth_tn", "sat_zenith_tn",]:
              # result is an numpy array with reprojected data
            band_settings, reprojected_data = readAndReproject(angle_data_mask,LUT_angles)
            interpolated = _linearNDinterpolate(reprojected_data)
            reprojected_data = interpolated[RIM_PIXELS:-RIM_PIXELS, RIM_PIXELS:-RIM_PIXELS]
        else:
            band_settings, reprojected_data = readAndReproject(data_mask, LUT)


        if '_FillValue' in band_settings and not digital_numbers:
            reprojected_data[reprojected_data == band_settings['_FillValue']] = np.nan
        if 'add_offset' in band_settings and 'scale_factor' in band_settings and not digital_numbers:
            reprojected_data = reprojected_data.astype("float32") * band_settings['scale_factor'] + band_settings['add_offset']

        varOut.append(reprojected_data)

    logger.info(f"Done reprojecting {product_type}")
    return np.array(varOut), is_empty


def _linearNDinterpolate(in_array):
    """
    Interpolate low resolution bands to fill the target grid completely.

    Some low resolution bands only contain values in diagonal lines. They need to be
    interpolated to fill the target grid completely. NaN values will be interpolated.
    If nothing was interpolated, the in_array is returned.

    :param in_array: The array containing NaNs that needs to be interpolated
    :return: 2D numpy array containing interpolated data
    """
    from scipy.interpolate import LinearNDInterpolator
    logger.info("  Start interpolation...")
    rownrs = np.arange(in_array.shape[0])  # Column indices (longitude)
    columnnrs = np.arange(in_array.shape[1])  # Row indices (latitude)

    X, Y = np.meshgrid(rownrs, columnnrs)  # 2D grid for interpolation
    ok = ~np.isnan(in_array)  # ok= all valid data =True
    xp = ok.nonzero()[0]  # get the index of all valid pixels
    if len(xp) < 10:
        logger.warning("Angle files are empty here, skip the interpolation. The not interpolated result(array_in) will be reused")
        return in_array
    yp = ok.nonzero()[1]
    fp = in_array[~np.isnan(in_array)]  # get the value of all valid pixels
    # interp = RegularGridInterpolator((xp, fp), in_array)
    coordinates = list(zip(xp, yp))
    interp = LinearNDInterpolator(coordinates, fp, fill_value=np.nan)
    # X = np.linspace(0, in_array.shape[0], num=1)
    # Y = np.linspace(0, in_array.shape[1], num=1)
    # X, Y = np.meshgrid(in_array)
    Z = interp(X, Y)
    return Z.T  # we should transpose this array


def _read_latlonfile(bbox, latlon_file, lat_band="latitude", lon_band="longitude", interpolation_margin=0, flatten=True):
    """
    Read latitude/longitude data from a NetCDF file.

    :param bbox: Bounding box to filter data
    :param latlon_file: The file containing the latitudes and longitudes
    :param lat_band: The band containing the latitudes (default='latitude')
    :param lon_band: The band containing the longitudes (default='longitude')
    :param interpolation_margin: Margin to add around the bounding box (default=0)
    :param flatten: If True, flatten coordinates; if False, keep 2D structure (default=True)
    :return: Tuple of (bbox_original, source_coordinates, data_mask) where bbox_original is
        the surrounding bounding box of the lat/lon [x_min, y_min, x_max, y_max],
        source_coordinates is a 2D-numpy array [[lon, lat], ..., [lon, lat]],
        and data_mask is the mask for valid data
    """
    # getting  geo information
    logger.debug("Reading lat/lon from file %s" % latlon_file)
    potential_variables = ["elevation_in", "elevation_orphan_in",
                           "latitude_in", "latitude_orphan_in",
                           "longitude_in", "longitude_orphan_in",
                           "latitude_tx", "longitude_tx",
                           ]
    to_drop = [x for x in potential_variables if x != lat_band and x != lon_band]  # saves memory
    # `open_dataarray` could allow for lazy loading, but is more complex and saves the same amount of memory
    # decode_cf=True forces lat/lon bands from int32 to float64 by filling nans, scaling, and offsetting. This causes OOM errors.
    # Instead, we do the scaling ourselves. Filling nans is not required for coordinates.
    # Note that float32 causes us to lose precision, so we stick to int32.
    lat_lon_ds = xr.open_dataset(latlon_file, drop_variables=to_drop, decode_cf=False)
    lat_scale = lat_lon_ds[lat_band].attrs.get('scale_factor', 1.0)
    lat_offset = lat_lon_ds[lat_band].attrs.get('add_offset', 0.0)
    lon_scale = lat_lon_ds[lon_band].attrs.get('scale_factor', 1.0)
    lon_offset = lat_lon_ds[lon_band].attrs.get('add_offset', 0.0)

    # lat_lon_ds is scaled, so we need to inverse scale the bbox as well.
    xmin_s, ymin_s, xmax_s, ymax_s = (bbox[0]/lon_scale)-lon_offset, (bbox[1]/lat_scale)-lat_offset, (bbox[2]/lon_scale)-lon_offset, (bbox[3]/lat_scale)-lat_offset
    interpolation_margin_lat = (interpolation_margin/lat_scale)-lat_offset
    interpolation_margin_lon = (interpolation_margin/lon_scale)-lon_offset

    lat_mask = xr.apply_ufunc(lambda lat: (lat >= ymin_s - interpolation_margin_lat) & (lat <= ymax_s + interpolation_margin_lat), lat_lon_ds[lat_band])
    lon_mask = xr.apply_ufunc(lambda lon: (lon >= xmin_s - interpolation_margin_lon) & (lon <= xmax_s + interpolation_margin_lon), lat_lon_ds[lon_band])
    data_mask = lat_mask & lon_mask


    # Create the coordinate arrays for latitude and longitude
    ## Coordinated referring to the CENTER of the pixel
    # We can rescale to float64 from this point since we are working with smaller arrays.
    lat_orig = lat_lon_ds[lat_band].where(data_mask, drop=True).values*lat_scale+lat_offset
    lon_orig = lat_lon_ds[lon_band].where(data_mask, drop=True).values*lon_scale+lon_offset
    lat_lon_ds.close()

    if lat_orig.size == 0 or lon_orig.size == 0:
        logger.warning("No valid data found in lat/lon file")
        return None, None, None

    extreme_right_lon = lon_orig[0,-1] # negative degrees (-170)
    extreme_left_lon = lon_orig[-1,0]  # possitive degrees (169)
    if extreme_right_lon < extreme_left_lon:
        passing_date_line = True
        # self.lon_orig=np.where(self.lon_orig<0, self.lon_orig+360, self.lon_orig) #change grid from -180->180 to 0->360

    x_min, x_max = np.nanmin(lon_orig), np.nanmax(lon_orig)
    y_min, y_max = np.nanmin(lat_orig), np.nanmax(lat_orig)
    bbox_original = [x_min, y_min, x_max, y_max]
    if not flatten:
        source_coordinates = np.stack((lon_orig, lat_orig), axis=0)
        return bbox_original, source_coordinates, data_mask
    lon_flat = lon_orig.flatten()
    lat_flat = lat_orig.flatten()
    lon_flat = lon_flat[~np.isnan(lon_flat)]
    lat_flat = lat_flat[~np.isnan(lat_flat)]
    source_coordinates = np.column_stack((lon_flat, lat_flat))
    return bbox_original, source_coordinates, data_mask

def create_index_LUT(coordinates, target_coordinates, max_distance):
    """
    Create a LUT (lookup table) containing the reprojection indexes.

    Find all the indices of the nearest neighbors within the maximum distance.

    :param coordinates: 2D-numpy array [[lon, lat], ..., [lon, lat]] from the source
    :param target_coordinates: 2D-numpy array [[lon, lat], ..., [lon, lat]] from the
        complete gridded target tile
    :param max_distance: The maximum distance to reproject pixels
    :return: Tuple of (distances, lut_indices) where distances is a 2D-numpy array containing
        the distance between source and target location, and lut_indices is a numpy array
        containing the source index for each target location. If the index is not available
        in the source array (value=len(source_array)), it means that no valid pixel was found
    """
    logger.info("Creating LUT with shape " + str(target_coordinates.shape))
    # Create a KDTree from the input points
    tree = cKDTree(coordinates)

    # Find ALL the indices of the nearest neighbors within the maximum distance
    distances, lut_indices = tree.query(target_coordinates, k=1,
                                        distance_upper_bound=max_distance)  # empty results will have an index=len(indices)
    # lut_indices refer to the index of the "coordinates" and has the length of target_coordinates.

    return distances, lut_indices


def read_band(in_file, in_band, data_mask, get_data_array=True):
    """
    Get array and settings (metadata) from the input file.

    :param in_file: The input file that needs to be read
    :param in_band: The band name of the NetCDF that needs to be read
    :param data_mask: Mask for valid data
    :param get_data_array: If True, return input_array and settings;
        if False, return only the settings (metadata)
    :return: Tuple of (data_array, settings) where data_array is the numpy array from
        the NetCDF band and settings is a dict containing the band's metadata
        (_FillValue, name, dtype, units, etc.)
    """
    # can be used to get only the band settings or together with the data
    logger.debug(f"Reading {in_band} from file {in_file}")
    dataset = xr.open_dataset(in_file, mask_and_scale=False, cache=False)  # disable autoconvert digital values
    settings = dataset[in_band].attrs
    settings['dtype'] = dataset[in_band].dtype.name
    settings['name'] = in_band

    for key, value in settings.items():
        if isinstance(value, str) or isinstance(value, np.ndarray):
            continue
        if value.dtype.kind in ['i', 'u']:
            settings[key] = int(value)
        elif value.dtype.kind in ['f']:
            settings[key] = float(value)

    # overwrite initial band settings by their final values
    band = in_band
    if band[-9:] == '_radiance':  # OLCI Radiance
        settings["_FillValue"] = 65535
        settings['name'] = band
        settings['standard_name'] = band
    elif 'radiance_unc' in band:  # OLCI Radiance uncertainties
        settings["_FillValue"] = 255
        settings['name'] = band
        settings['standard_name'] = band
    elif 'radiance_an' in band:  # SLSTR radiance
        settings["_FillValue"] = -32768
        settings['name'] = band
        settings['standard_name'] = band
    elif band == 'quality_flags':
        settings["_FillValue"] = np.array(4294967295, dtype=settings['dtype'])  # 2147483647 in NRT_production, 4294967295 in TOA-TDS
    elif band == 'pixel_classif_flags':
        settings["_FillValue"] = -1
    elif band in ['OAA', 'OZA', 'SAA', 'SZA', 'sea_level_pressure', 'total_columnar_water_vapour']:
        settings["_FillValue"] = None
    elif band == 'cloud_an':
        settings["_FillValue"] = 65535
    elif band in ['sat_zenith_tn', 'solar_azimuth_tn', 'sat_azimuth_tn', 'solar_zenith_tn']:
        settings["_FillValue"] = None
        settings["dtype"] = 'float32'

    if get_data_array:
        data_array = dataset[in_band].where(data_mask, drop=True).data
    else:
        data_array = None
    dataset.close()

    return data_array, settings


def apply_LUT_on_band(in_data, LUT, nodata=None):
    """
    Apply reprojection LUT on an array.

    The LUT contains the index of the source array.

    :param in_data: 1D-numpy array containing all the source (frame) values
    :param LUT: 2D-numpy array with the size of a tile, containing the index of the
        source data (the index in in_data)
    :param nodata: The nodata value to use when a target coordinate has no corresponding
        input value
    :return: 2D-numpy array with the size of a tile containing reprojected values
    """


    # if nodata is empty, we will just use the max possible value
    if nodata is None:
        if in_data.dtype.kind in ['i', 'u']:
            nodata = np.nan
            # nodata = np.iinfo(in_data.dtype).max
        elif in_data.dtype.kind in ['f']:
            nodata = np.nan
            # nodata = np.finfo(in_data.dtype).max

    data_flat = np.append(in_data, nodata)
    #TODO: this avoids getting IndexOutOfBounds, but may hide the actual issue because array sizes don't match
    LUT_invalid_index = LUT >= len(data_flat)
    LUT[LUT_invalid_index] = 0
    grid_values = data_flat[ LUT ]
    #ncols = self.target_shape[1]
    #grid_values.shape = (grid_values.size // ncols, ncols)  # convert flattend array to 2D
    grid_values[LUT_invalid_index] = nodata
    return grid_values


if __name__ == '__main__':
    logging.basicConfig(level="INFO")

    lat_lon_bbox = [2.535352308127358, 50.57415247573394, 5.713651867060349, 51.718230797191836]
    lat_lon_bbox = [9.944991786580573, 45.99238819027832, 12.146700668591137, 47.27025711819684]
    lat_lon_bbox = [129.4502384682777, -18.161201081701407, 138.1236995771855, -10.287173762653026]
    # lat_lon_bbox = [0.0, 50.0, 5.0, 55.0]
    from_date = "2024-01-01T00:00:00Z"
    to_date = "2024-01-01T02:00:00Z"
    band_names = ["NTC_AOD:Surface_reflectance_440"]

    product_type = "SY_2_AOD___"
    native_resolution = 0.040178571
    bbox = BoundingBox.from_wsen_tuple(lat_lon_bbox, crs=4326).reproject(32753)

    main(product_type, native_resolution, bbox, from_date, to_date, band_names)
