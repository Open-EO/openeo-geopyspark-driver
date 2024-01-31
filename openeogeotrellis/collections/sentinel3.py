import json
import logging
import os
import pathlib
import sys
from datetime import datetime
from functools import partial
from glob import glob
from typing import Tuple

import numpy as np
import pyspark
from pyspark import SparkContext, find_spark_home
from scipy.spatial import cKDTree  # used for tuning the griddata interpolation settings
import xarray as xr

from openeogeotrellis.utils import get_jvm, ensure_executor_logging, set_max_memory

OLCI_PRODUCT_TYPE = "OL_1_EFR___"
SYNERGY_PRODUCT_TYPE = "SY_2_SYN___"
SLSTR_PRODUCT_TYPE = "SL_2_LST___"

logger = logging.getLogger(__name__)


def main(product_type, lat_lon_bbox, from_date, to_date, band_names):
    spark_python = os.path.join(find_spark_home._find_spark_home(), 'python')
    py4j = glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
    sys.path[:0] = [spark_python, py4j]

    import geopyspark as gps

    conf = gps.geopyspark_conf(master="local[*]", appName=__file__)
    conf.set('spark.kryo.registrator', 'geotrellis.spark.store.kryo.KryoRegistrator')
    conf.set('spark.driver.extraJavaOptions', "-Dlog4j2.debug=false -Dlog4j2.configurationFile=file:/home/bossie/PycharmProjects/openeo/openeo-python-driver/log4j2.xml")

    with SparkContext(conf=conf):
        jvm = get_jvm()

        extent = jvm.geotrellis.vector.Extent(*lat_lon_bbox)
        crs = "EPSG:4326"
        projected_polygons_native_crs = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(extent, crs)

        data_cube_parameters = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
        getattr(data_cube_parameters, "tileSize_$eq")(256)
        getattr(data_cube_parameters, "layoutScheme_$eq")("FloatingLayoutScheme")
        data_cube_parameters.setGlobalExtent(*lat_lon_bbox, crs)
        cell_size = jvm.geotrellis.raster.CellSize(0.008928571428571, 0.008928571428571)

        layer = pyramid(product_type, projected_polygons_native_crs, from_date, to_date, band_names,
                        data_cube_parameters, cell_size, jvm)[0]
        layer.to_spatial_layer().save_stitched(f"/tmp/{product_type}_{from_date}_{to_date}.tif",
                                               crop_bounds=gps.geotrellis.Extent(*lat_lon_bbox))


def pyramid(metadata_properties, projected_polygons_native_crs, from_date, to_date, band_names, data_cube_parameters,
            cell_size, feature_flags, jvm):
    from openeogeotrellis.collections.s1backscatter_orfeo import S1BackscatterOrfeoV2
    import geopyspark as gps

    limit_executor_python_memory = feature_flags.get("limit_executor_python_memory", False)

    opensearch_client = jvm.org.openeo.opensearch.OpenSearchClient.apply(
        "https://catalogue.dataspace.copernicus.eu/resto", False, "", [], "",
    )

    collection_id = "Sentinel3"
    product_type = metadata_properties["productType"]
    correlation_id = ""

    file_rdd_factory = jvm.org.openeo.geotrellis.file.FileRDDFactory(
        opensearch_client, collection_id, metadata_properties, correlation_id, cell_size
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
    pyrdd = gps.create_python_rdd(j2p_rdd, serializer=serializer)
    pyrdd = pyrdd.map(json.loads)

    layer_metadata_py = S1BackscatterOrfeoV2(jvm)._convert_scala_metadata(metadata_sc)

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

    tile_rdd = (per_product
                .partitionBy(numPartitions=len(creo_paths), partitionFunc=creo_paths.index)
                .flatMap(partial(read_product, product_type=product_type, band_names=band_names, tile_size=tile_size,
                                 limit_python_memory=limit_executor_python_memory)))

    logger.info("Constructing TiledRasterLayer from numpy rdd, with metadata {m!r}".format(m=layer_metadata_py))

    tile_layer = gps.TiledRasterLayer.from_numpy_rdd(
        layer_type=gps.LayerType.SPACETIME,
        numpy_rdd=tile_rdd,
        metadata=layer_metadata_py
    )
    # Merge any keys that have more than one tile.
    contextRDD = jvm.org.openeo.geotrellis.OpenEOProcesses().mergeTiles(tile_layer.srdd.rdd())
    temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
    srdd = temporal_tiled_raster_layer.apply(jvm.scala.Option.apply(zoom), contextRDD)
    merged_tile_layer = gps.TiledRasterLayer(gps.LayerType.SPACETIME, srdd)

    return {zoom: merged_tile_layer}

def _instant_ms_to_hour(instant: int) -> datetime:
    """
    Convert Geotrellis SpaceTimeKey instant (Scala Long, millisecond resolution) to Python datetime object,
    rounded down to hour resolution (UTC time 00:00:00), a convention used in other places
    of our openEO backend implementation and necessary to follow, for example
    to ensure that timeseries related data joins work properly.

    Sentinel-3 can have many observations per day, warranting the choice of hourly rather than daily aggregation
    """
    return datetime(*(datetime.utcfromtimestamp(instant // 1000).timetuple()[:4]))

@ensure_executor_logging
def read_product(product, product_type, band_names, tile_size, limit_python_memory):
    from openeogeotrellis.collections.s1backscatter_orfeo import get_total_extent
    import geopyspark

    if limit_python_memory:
        max_total_memory_in_bytes = os.environ.get('PYTHON_MAX_MEMORY')
        if max_total_memory_in_bytes:
            set_max_memory(int(max_total_memory_in_bytes))

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
    instants = set(f["key"]["instant"] for f in features)
    assert len(instants) == 1, f"Not single instant: {instants}"
    instant = instants.pop()
    logger.info(
        f"{log_prefix} Layout key extent: col[{col_min}:{col_max}] row[{row_min}:{row_max}]"
        f" ({cols}x{rows}={cols * rows} tiles) instant[{instant}]."
    )

    layout_extent = get_total_extent(features)

    key_epsgs = set(f["key_epsg"] for f in features)
    assert len(key_epsgs) == 1, f"Multiple key CRSs {key_epsgs}"
    layout_epsg = key_epsgs.pop()

    logger.info(
        f"{log_prefix} Layout extent {layout_extent} EPSG {layout_epsg}:"
    )

    orfeo_bands = create_s3_toa(product_type, creo_path, band_names,
                                [layout_extent['xmin'], layout_extent['ymin'], layout_extent['xmax'],
                                 layout_extent['ymax']])

    debug_mode = True

    # Split orfeo output in tiles
    logger.info(f"{log_prefix} Split {orfeo_bands.shape} in tiles of {tile_size}")
    if product_type == OLCI_PRODUCT_TYPE:
        nodata = 65535
    elif product_type == SYNERGY_PRODUCT_TYPE:
        nodata = -10000
    elif product_type == SLSTR_PRODUCT_TYPE:
        nodata = -32768
    else:
        raise ValueError(product_type)
    cell_type = geopyspark.CellType.create_user_defined_celltype("int32", nodata)  # TODO: check if right
    tiles = []
    for c in range(col_max - col_min + 1):
        for r in range(row_max - row_min + 1):
            col = col_min + c
            row = row_min + r
            key = geopyspark.SpaceTimeKey(col=col, row=row, instant=_instant_ms_to_hour(instant))
            tile = orfeo_bands[:, r * tile_size:(r + 1) * tile_size, c * tile_size:(c + 1) * tile_size]
            if not (tile == nodata).all():
                if debug_mode:
                    logger.info(f"{log_prefix} Create Tile for key {key} from {tile.shape}")
                tile = geopyspark.Tile(tile, cell_type, no_data_value=nodata)
                tiles.append((key, tile))

    logger.info(f"{log_prefix} Layout extent split in {len(tiles)} tiles")

    return tiles


def create_s3_toa(product_type, creo_path, band_names, bbox_tile):
    if product_type == OLCI_PRODUCT_TYPE:
        geofile = 'geo_coordinates.nc'
        lat_band = 'latitude'
        lon_band = 'longitude'
        final_grid_resolution = 1 / 112 / 3
    elif product_type == SYNERGY_PRODUCT_TYPE:
        geofile = 'geolocation.nc'
        lat_band = 'lat'
        lon_band = 'lon'
        final_grid_resolution = 1 / 112 / 3
    elif product_type == SLSTR_PRODUCT_TYPE:
        geofile = 'geodetic_in.nc'
        lat_band = 'latitude_in'
        lon_band = 'longitude_in'
        final_grid_resolution = 1 / 112
    else:
        raise ValueError(product_type)

    tile_coordinates, tile_shape = create_final_grid(bbox_tile, resolution=final_grid_resolution)
    tile_coordinates.shape = tile_shape + (2,)  # target =((4352, 5114, 2)) before=22256128,2

    geofile = os.path.join(creo_path, geofile)

    bbox_original, source_coordinates = _read_latlonfile(geofile, lat_band, lon_band)
    logger.info(f"{bbox_original=} {source_coordinates=}")

    reprojected_data, is_empty = do_reproject(product_type, final_grid_resolution, creo_path, band_names,
                                              source_coordinates, tile_coordinates)

    return reprojected_data


def create_final_grid(final_bbox, resolution, rim_pixels=0 ):
    """this function will create a grid for a given boundingbox and resolution, optionnally,
    a number of pixels can be given to create an extra rim on each side of the boundingbox

    Parameters
    ----------
    final_bbox : float
        a list containing the following info [final_xmin, final_ymin, final_xmax, final_ymax]
    resolution : float
        the resolution of the grid
    rim_pixels : int
        the number of pixels to create an extra rim in each side of the array, default=0

    Returns
    -------
    target_coordinates: float
        A 2D-array containing the final coordinates
    target_shape : (int,int)
        A tuple containing the number of rows/columns
    """

    final_xmin, final_ymin, final_xmax, final_ymax = final_bbox
    #the boundingbox of the tile seems to missing the last pixels, that is why we add 1 resolution to the second argument
    grid_x, grid_y = np.meshgrid(
        np.arange(final_xmin - (rim_pixels * resolution), final_xmax + resolution +(rim_pixels * resolution), resolution),
        np.arange(final_ymax + (rim_pixels * resolution), final_ymin - resolution - (rim_pixels * resolution), -resolution)  # without lat mirrored
        # np.arange(ref_ymin, ref_ymax, self.final_grid_resolution)   #with lat mirrored
    )
    target_shape = grid_x.shape
    target_coordinates = np.column_stack((grid_x.ravel(), grid_y.ravel()))
    return target_coordinates, target_shape


def do_reproject(product_type, final_grid_resolution, creo_path, band_names, source_coordinates, target_coordinates):
    """Create LUT for reprojecting, and reproject all possible(hard-coded) bands

    Parameters
    ----------
    out_file : str
        the output file used for writing the data. The file should already exist and initialised correctly
    target_coordinates : float
        a 2D numpy array representing the complete grid for the tile.

    Returns
    -------
    out_file: str
        the name of the output file
    is_empty: bool
        True in case no valid data was found in the reprojected data. This test is based on layer Oa02_radiance
    """

    is_empty = False
    logger.info(f"Reprojecting {product_type}")
    ### create LUT for radiances
    distance, LUT = create_index_LUT(source_coordinates,
                                     target_coordinates,
                                     2 * final_grid_resolution)  # indices will have the same size as flattend grid_xy referring to the index from the latlon a data arrays

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
                return f"SDR_{band_name.split('_')[1]}"
            if product_type == SLSTR_PRODUCT_TYPE:
                return band_name
            raise ValueError(band_name)

        band_data, band_settings = read_band(in_file, in_band=variable_name(band_name))

        reprojected_data = apply_LUT_on_band(band_data, LUT, band_settings.get('_FillValue', None))  # result is an numpy array with reprojected data

        varOut.append(reprojected_data)

    logger.info(f"Done reprojecting {product_type}")
    return np.array(varOut), is_empty


def _read_latlonfile(latlon_file, lat_band="latitude", lon_band="longitude"):
    """Read latlon data from this netcdf file

    Parameters
    ----------
    latlon_file : str
        the file containing the latitudes and longitudes
    lat_band : str
        the band containing the latitudes (default=latitude)
    lon_band : float
        the band containing the longitudes (default=longitude)

    Returns
    -------
    bbox_original: [float, float, float, float]
        the surrounding boundingbox of the lat/lon : x_min, y_min, x_max, y_max
    source_coordinates: a list of coordinates
        2D-numpy array [[lon, lat], ..., [lon, lat]]
    """
    # getting  geo information
    logger.debug("Reading lat/lon from file %s" % latlon_file)
    lat_lon_ds = xr.open_dataset(latlon_file).astype("float32")
    # Create the coordinate arrays for latitude and longitude
    ## Coordinated referring to the CENTER of the pixel
    lat_orig = lat_lon_ds[lat_band].values
    lon_orig = lat_lon_ds[lon_band].values
    lat_lon_ds.close()

    extreme_right_lon = lon_orig[0,-1] # negative degrees (-170)
    extreme_left_lon = lon_orig[-1,0]  # possitive degrees (169)
    if extreme_right_lon < extreme_left_lon:
        passing_date_line = True
        # self.lon_orig=np.where(self.lon_orig<0, self.lon_orig+360, self.lon_orig) #change grid from -180->180 to 0->360

    x_min, x_max = np.min(lon_orig), np.max(lon_orig)
    y_min, y_max = np.min(lat_orig), np.max(lat_orig)
    bbox_original = [x_min, y_min, x_max, y_max]
    source_coordinates = np.column_stack((lon_orig.flatten(), lat_orig.flatten()))
    return bbox_original, source_coordinates

def create_index_LUT(coordinates, target_coordinates, max_distance):
    """Create A LUT containing the reprojection indexes. Find ALL the indices of the nearest neighbors within the maximum distance.

    Parameters
    ----------
    coordinates : float
        2D-numpy array [[lon, lat], ..., [lon, lat]] from the source
    target_coordinates : float
        2D-numpy array [[lon, lat], ..., [lon, lat]] from the complete gridded target tile
    max_distance : float
        The maximum distance to reproject pixels

    Returns
    -------
    distances: numpy array
        2D-numpy array [[lon, lat], ..., [lon, lat]] containing the distance between source and target location
    lut_indices: numpy arry
        This numpy array contains the source index for each target location.
        If the index is not available in the source array(value=len(source_array)), it means that no valid pixel was found=> nodata
    """
    logger.info("Creating LUT with shape " + str(target_coordinates.shape))
    # Create a KDTree from the input points
    tree = cKDTree(coordinates)

    # Find ALL the indices of the nearest neighbors within the maximum distance
    distances, lut_indices = tree.query(target_coordinates, k=1,
                                        distance_upper_bound=max_distance)  # empty results will have an index=len(indices)
    # lut_indices refer to the index of the "coordinates" and has the length of target_coordinates.

    return distances, lut_indices


def read_band(in_file, in_band, get_data_array=True):
    """get array and settings(metadata) out of the in_file

    Parameters
    ----------
    in_file : str
        The input file that needs to be read
    in_band : str
        The band name of the netcdf that needs to be read
    get_data_array : bool
        True : return input_array and settings
        False : return only the settings(metadata)

    Returns
    -------
    data_array: numpy array
        The array from the netcdf band
    settings: dict
        A dict containing the bands metadata (_FillValue, name, dtype, units,...)
    """
    # can be used to get only the band settings or together with the data
    dataset = xr.open_dataset(in_file, mask_and_scale=False)  # disable autoconvert digital values
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
        data_array = dataset[in_band].data
    else:
        data_array = None
    dataset.close()

    return data_array, settings


def apply_LUT_on_band(in_data, LUT, nodata=None):
    """Apply reprojection LUT on an array. The LUT contains the index of the source array

    Parameters
    ----------
    in_data : numpy array
        A 2D-numpy array containing all the source(frame) values
    LUT : numpy array
        A 2D-numpy array : LUT has the size of a tile, containing the index of the source data
    nodata : float
        The nodata value to use when a target coordinate has no corresponding input value.

    Returns
    -------
    grid_values: numpy array
        2D-numpy array with the size of a tile containing reprojected values
    """
    data_flat = in_data.flatten()

    # if nodata is empty, we will just use the max possible value
    if nodata is None:
        if in_data.dtype.kind in ['i', 'u']:
            nodata = np.nan
            # nodata = np.iinfo(in_data.dtype).max
        elif in_data.dtype.kind in ['f']:
            nodata = np.nan
            # nodata = np.finfo(in_data.dtype).max

    data_flat = np.append(data_flat, nodata)
    grid_values = data_flat[LUT]
    #ncols = self.target_shape[1]
    #grid_values.shape = (grid_values.size // ncols, ncols)  # convert flattend array to 2D
    return grid_values


if __name__ == '__main__':
    logging.basicConfig(level="INFO")

    lat_lon_bbox = [2.535352308127358, 50.57415247573394, 5.713651867060349, 51.718230797191836]
    lat_lon_bbox = [9.944991786580573, 45.99238819027832, 12.146700668591137, 47.27025711819684]
    lat_lon_bbox = [-128.4272367635350349, 49.7476186207236424, -126.9726189291401113, 50.8176823149911527]
    #lat_lon_bbox = [0.0, 50.0, 5.0, 55.0]
    from_date = "2018-03-12T00:00:00Z"
    to_date = from_date
    band_names = ["LST_in:LST"]

    main(SLSTR_PRODUCT_TYPE, lat_lon_bbox, from_date, to_date, band_names)
