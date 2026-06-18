""" Load Sentinel-5P satellite data from a NetCDF file.

This code provides a functionality to read and filter different gases
data from Sentinel-5P NetCDF files based on specified spatial and temporal
extents, as well as quality filtering.

Filtering:
   The default is set as per Sentinel-5P documentation. It differs per gas
   If the user provides a different value which exists, it can be used as filter.
   According to documentation, the quality values are between 0-1.
   For more details, please refer to the PRF-**** documents on this page
   https://sentiwiki.copernicus.eu/web/s5p-products#S5PProducts-L2S5P-Products-L2


Attributes:
     (dict): A dictionary mapping band names to their respective
                      paths in the NetCDF file for Sentinel-5P CO level-2 data.

Everything should happen in EPSG: 4326 (lat-lon) as Sentinel-5P data is in lat-lon grid.

Algorithm:
    1. Check inputs
        - if the file exists
        - If bands and filter value are provided, load default bands if not.
        - Check if spatial extent is provided.
        - Check if temporal extent is provided and is made of datetime objects.
        - Resampling parameters.
    2. Load data from netCDF file where
        - Validate temporal extent from delta_time field and
        - Validate spatial extents from latitude and longitude fields and
            get pixel indices representing the spatial extents.
        - Load the required bands from the pixels indices.
    3. Resample data if required.
    4. Apply quality filtering based on qa_value_mask band.

"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np

from .sentinel5p_functions import (
    adapt_coordinates,
    apply_quality_filter,
    get_gas_variables,
    interpolate,
    load_data_from_file,
    resample_data,
)

logger = logging.getLogger(__name__)


def load_level2_data(params):
    """Load Sentinel-5P level-2 data from a NetCDF file.

    Args:
        params (dict): A dict of parameters containing following
            filename: str, path to the NetCDF file.
            spatial_extent: tuple, (min_lon, min_lat, max_lon, max_lat)
            temporal_extent: tuple, (start_time, end_time) as datetime objects.
            bands: list of str, list of band names to load.
            filter_value: float, filter_value, quality filtering value between 0-1.
            resample_factor: [Bool, Float, method], Resample data, resampling resolution and method
                              to downsample the data.
    Returns:
        data: dict, dictionary containing loaded data arrays for the specified bands.

    Raises:
        Exception: If the file does not exist or if the temporal extent does not intersect or
                   if the spatial extent is invalid.

    """
    # filename, spatial_extent, temporal_extent, bands, filter_value
    # check if the file exists
    file_path = Path(params.get("filename", ""))

    if not file_path.exists():
        raise Exception(f"read_product: path {file_path} does not exist.")
    # get the gas
    file_gas = file_path.name.split("_")[4]
    VARIABLE_LOC_IN_FILE, DEFAULT_BANDS, DEFAULT_FILTER_VALUE = get_gas_variables(file_gas)

    # check the spatial extent and temporal extent keys
    spatial_extent = params.get("spatial_extent", None)
    temporal_extent = params.get("temporal_extent", None)
    # check if temporal_extent is made of datetime objects
    if temporal_extent is not None:
        if not all(isinstance(x, datetime) for x in temporal_extent):
            raise Exception("temporal_extent should be made of datetime objects.")
    # check the band names and filter_value
    bands = params.get("bands", [])
    # if bands are not defined then load the default bands
    if not bands:
        bands = DEFAULT_BANDS
    # filter value
    filter_value = params.get("filter_value", DEFAULT_FILTER_VALUE)
    # this should be on the client side
    if not ((filter_value >= 0.0) and (filter_value <= 1.0)):
        raise IOError(
            f"Warning: filter_value {filter_value} is not standard as per Sentinel-5P documentation."
            " It should be between 0.0-1.0."
        )
    # resampling parameters
    resample_params = params.get("resample_factor", [False, 0.025, "nearest"])

    # Check if file is temporally valid and if valid, then load data from the file
    # temporally_valid = is_temporal_extent_valid(file_path.name, temporal_extent)
    # if not temporally_valid:
    #     raise Exception(
    #         f"Input temporal extent doesn't intersect with the temporal extent of the file {file_path.name}."
    #     )
    # else:


    # Load raw data from the file based on spatial and temporal extents and resampling
    # This function can raise a lot of exceptions which will be propagated.
    # TODO No idea if we should catch and re-raise them with more context here.
    data = load_data_from_file(
        file_path,
        spatial_extent,
        temporal_extent,
        bands,
        VARIABLE_LOC_IN_FILE,
        filter_value,
    )

    # resample data
    if resample_params[0]:  # if resampling is required
        data = resample_data(data, bands, spatial_extent, resample_params[1], resample_params[2])

    # apply quality filtering
    final_data = apply_quality_filter(data, bands, "qa_value_mask")
    return final_data


def _instant_ms_to_minute(instant: int) -> datetime:
    """Convert a Unix millisecond timestamp to a datetime rounded down to the minute.

    Matches the convention used in the Sentinel-3 loader so that time-series joins work correctly.
    """
    return datetime(*(datetime.utcfromtimestamp(instant // 1000).timetuple()[:5]))


def read_product(
    product,
    band_names: List[str],
    tile_size: int,
    resolution: float,
) -> List[Tuple]:
    """Read Sentinel-5P data from a NetCDF file and return GeoTrellis tiles.

    Follows the same interface as :func:`openeogeotrellis.collections.sentinel3.read_product`
    so that the two collections can be handled uniformly in the openEO pipeline.

    Args:
        product: Tuple of (creo_path, features) where *creo_path* is the path to the
            NetCDF file and *features* is a list of tile feature dicts (each containing
            ``key``, ``key_extent`` and ``key_epsg`` as produced by
            ``FileRDDFactory.loadSpatialFeatureJsonRDD``).
        band_names: Band names to load (e.g. ``["carbonmonoxide_total_column_corrected"]``).
            When empty the gas-specific defaults are used.
        tile_size: Number of pixels per tile edge.
        resolution: Pixel size in degrees (EPSG:4326).

    Returns:
        List of ``(SpaceTimeKey, Tile)`` tuples ready for a GeoTrellis
        ``TiledRasterLayer``.  Returns an empty list when no valid data falls
        within the requested extent.
    """
    import geopyspark
    from openeogeotrellis.collections.s1backscatter_orfeo import get_total_extent
    from openeogeotrellis.utils import ensure_executor_logging

    creo_path, features = product
    creo_path = Path(creo_path)

    if not creo_path.exists():
        raise Exception(f"read_product: path {creo_path} does not exist.")

    file_gas = creo_path.name.split("_")[4]
    variable_loc_in_file, default_bands, default_filter_value = get_gas_variables(file_gas)

    col_min = min(f["key"]["col"] for f in features)
    col_max = max(f["key"]["col"] for f in features)
    row_min = min(f["key"]["row"] for f in features)
    row_max = max(f["key"]["row"] for f in features)
    cols = col_max - col_min + 1
    rows = row_max - row_min + 1

    instants = set(f["key"]["instant"] for f in features)
    assert len(instants) == 1, f"Expected a single instant, got: {instants}"
    instant = instants.pop()

    layout_extent = get_total_extent(features)
    xmin = layout_extent["xmin"]
    ymin = layout_extent["ymin"]
    xmax = layout_extent["xmax"]
    ymax = layout_extent["ymax"]
    spatial_extent = [xmin, ymin, xmax, ymax]

    bands_to_load = band_names if band_names else default_bands

    try:
        # temporal_extent=None loads all scanlines in the file; spatial filtering narrows to
        # the tile layout extent
        raw_data = load_data_from_file(
            creo_path, spatial_extent, None, bands_to_load, variable_loc_in_file, default_filter_value
        )
    except Exception as exc:
        if any(
            msg in str(exc)
            for msg in [
                "No data is available",
                "Input spatial extent is not in the file",
            ]
        ):
            logger.debug(f"No S5P data for {creo_path.name} in extent {spatial_extent}: {exc}")
            return []
        raise

    # Build a regular grid that exactly matches the tile layout
    # (cols * tile_size) pixels wide, (rows * tile_size) pixels tall
    n_x = cols * tile_size
    n_y = rows * tile_size
    xx = np.linspace(xmin + resolution / 2, xmax - resolution / 2, n_x)
    yy = np.linspace(ymax - resolution / 2, ymin + resolution / 2, n_y)
    grid_x, grid_y = np.meshgrid(xx, yy)

    source_lon = raw_data["longitude"].ravel()
    source_lat = raw_data["latitude"].ravel()
    source_coords = np.stack((source_lon, source_lat), axis=-1)
    target_coords = np.stack((grid_x.ravel(), grid_y.ravel()), axis=-1)

    if xmin > xmax:  # anti-meridian crossing
        source_coords, target_coords = adapt_coordinates(source_coords, target_coords)

    # Resample quality mask with "nearest" (preserves boolean semantics)
    qa_flat = raw_data["qa_value_mask"].ravel().astype(np.float64)
    qa_grid = interpolate(source_coords, qa_flat, target_coords, method="nearest").reshape(n_y, n_x).astype(bool)

    # Resample each band and apply quality mask
    band_grids = []
    for band in bands_to_load:
        if band not in raw_data:
            continue
        grid = (
            interpolate(source_coords, raw_data[band].ravel(), target_coords, method="nearest")
            .reshape(n_y, n_x)
            .astype(np.float32)
        )
        grid = np.where(qa_grid, grid, np.nan)
        band_grids.append(grid)

    if not band_grids:
        return []

    combined = np.stack(band_grids, axis=0)  # (n_bands, n_y, n_x)

    if np.isnan(combined).all():
        return []

    tiles = []
    cell_type = geopyspark.CellType.FLOAT32
    nodata = np.nan

    for f in features:
        col = f["key"]["col"]
        row = f["key"]["row"]
        c = col - col_min
        r = row - row_min

        tile_data = combined[
            :,
            r * tile_size : (r + 1) * tile_size,
            c * tile_size : (c + 1) * tile_size,
        ]

        if np.isnan(tile_data).all():
            continue

        key = geopyspark.SpaceTimeKey(col=col, row=row, instant=_instant_ms_to_minute(instant))
        tile = geopyspark.Tile(tile_data, cell_type, no_data_value=nodata)
        tiles.append((key, tile))

    logger.info(f"read_product: {creo_path.name} produced {len(tiles)} tile(s)")
    return tiles


def pyramid(
    metadata_properties,
    projected_polygons_native_crs,
    from_date,
    to_date,
    band_names: List[str],
    data_cube_parameters,
    native_cell_size,
    feature_flags: Dict,
    jvm,
    spatial_extent=None,
    use_stac_client: bool = False,
) -> Dict[int, "geopyspark.TiledRasterLayer"]:
    """Build a GeoTrellis pyramid from Sentinel-5P level-2 NetCDF files.

    Mirrors :func:`openeogeotrellis.collections.sentinel3.pyramid` so that
    Sentinel-5P can be loaded via the ``file-s5p`` layer source type in the
    layer catalog.
    """
    import json
    from functools import partial

    import geopyspark
    import pyspark
    import pyspark.serializers

    from openeogeotrellis.collections import convert_scala_metadata
    from openeogeotrellis.collections.sentinel3 import (
        _build_legacy_opensearch_client,
        _instant_ms_to_minute as s3_instant,
    )
    from openeogeotrellis.utils import get_jvm

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

    collection_id = "Sentinel5P"
    correlation_id = ""

    file_rdd_factory = jvm.org.openeo.geotrellis.file.FileRDDFactory(
        _build_legacy_opensearch_client(jvm),
        collection_id,
        metadata_properties,
        correlation_id,
        native_cell_size,
    )

    zoom = 0
    tile_size = data_cube_parameters.tileSize()

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

    def process_feature(feature: dict):
        creo_path = feature["feature"]["id"]
        return creo_path, {
            "key": feature["key"],
            "key_extent": feature["key_extent"],
            "bbox": feature["feature"]["bbox"],
            "key_epsg": feature["metadata"]["crs_epsg"],
        }

    per_product = pyrdd.map(process_feature).groupByKey().mapValues(list)
    creo_paths = per_product.keys().collect()

    assert native_cell_size.width() == native_cell_size.height()
    resolution = native_cell_size.width()

    tile_rdd = per_product.partitionBy(numPartitions=len(creo_paths), partitionFunc=creo_paths.index).flatMap(
        partial(read_product, band_names=band_names, tile_size=tile_size, resolution=resolution)
    )

    logger.info(f"Constructing Sentinel-5P TiledRasterLayer with metadata {layer_metadata_py!r}")

    tile_layer = geopyspark.TiledRasterLayer.from_numpy_rdd(
        layer_type=geopyspark.LayerType.SPACETIME, numpy_rdd=tile_rdd, metadata=layer_metadata_py
    )

    context_rdd = jvm.org.openeo.geotrellis.OpenEOProcesses().mergeTiles(tile_layer.srdd.rdd())
    temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
    srdd = temporal_tiled_raster_layer.apply(jvm.scala.Option.apply(zoom), context_rdd)
    merged_tile_layer = geopyspark.TiledRasterLayer(geopyspark.LayerType.SPACETIME, srdd)

    return {zoom: merged_tile_layer}
