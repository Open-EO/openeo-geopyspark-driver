import copy
import datetime

import numpy as np
import pytest
import pytz

from openeogeotrellis.service_registry import InMemoryServiceRegistry

TILE_SIZE = 16

matrix_of_one = np.zeros((1, TILE_SIZE, TILE_SIZE))
matrix_of_one.fill(1)

matrix_of_two = np.zeros((1, TILE_SIZE, TILE_SIZE))
matrix_of_two.fill(2)

matrix_of_nodata = np.zeros((1, TILE_SIZE, TILE_SIZE))
matrix_of_nodata.fill(-1)

extent = {"xmin": 0.0, "ymin": 0.0, "xmax": 4.0, "ymax": 4.0}
extent_webmerc = {"xmin": 0.0, "ymin": 0.0, "xmax": 445277.96317309426, "ymax": 445640.1096560266}
layout = {"layoutCols": 1, "layoutRows": 1, "tileCols": TILE_SIZE, "tileRows": TILE_SIZE}


openeo_metadata = {
    "cube:dimensions": {
        "x": {"type": "spatial", "axis": "x"},
        "y": {"type": "spatial", "axis": "y"},
        "bands": {"type": "bands", "values": ["red", "nir"]},
        "t": {"type": "temporal"}
    },
    "bands": [

        {
            "band_id": "red",
            "name": "red",
            "offset": 0,
            "res_m": 10,
            "scale": 0.0001,
            "type": "int16",
            "unit": "1",
            "wavelength_nm": 664.5
        },
        {
            "band_id": "nir",
            "name": "nir",
            "offset": 0,
            "res_m": 10,
            "scale": 0.0001,
            "type": "int16",
            "unit": "1",
            "wavelength_nm": 835.1
        }
    ],
    "_vito": {"accumulo_data_id": "CGS_SENTINEL2_RADIOMETRY_V101"},
    "description": "Sentinel 2 Level-2: Bottom-of-atmosphere reflectances in cartographic geometry",
    "extent": {
        "bottom": 39,
        "crs": "EPSG:4326",
        "left": -34,
        "right": 35,
        "top": 71
    },
    "product_id": "CGS_SENTINEL2_RADIOMETRY_V101",
    "time": {
        "from": "2016-01-01",
        "to": "2019-10-01"
    }
}


def openeo_metadata_spatial():
    metadata = copy.deepcopy(openeo_metadata)
    metadata["cube:dimensions"].pop("t")
    metadata.pop("time")
    return metadata


def double_size_2d_array_repeat(arr, repeats=1):
    for _ in range(repeats):
        arr = double_size_2d_array(arr)
    return arr


def double_size_2d_array(input_array):
    original_shape = input_array.shape
    new_shape = (original_shape[0] * 2, original_shape[1] * 2)
    new_array = np.zeros(new_shape, dtype=input_array.dtype)
    new_array[::2, ::2] = input_array
    new_array[1::2, ::2] = input_array
    new_array[::2, 1::2] = input_array
    new_array[1::2, 1::2] = input_array
    return new_array


def layer_with_two_bands_and_one_date():
    from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
    from geopyspark.geotrellis.constants import LayerType
    from geopyspark.geotrellis.layer import TiledRasterLayer
    from pyspark import SparkContext

    two_band_one_two = np.array([matrix_of_one, matrix_of_two], dtype='int')
    tile = Tile.from_numpy_array(two_band_one_two, -1)

    date1 = datetime.datetime.strptime("2017-09-25T11:37:00Z", '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)

    layer = [(SpaceTimeKey(0, 0, date1), tile),
             (SpaceTimeKey(1, 0, date1), tile),
             (SpaceTimeKey(0, 1, date1), tile),
             (SpaceTimeKey(1, 1, date1), tile)]

    rdd = SparkContext.getOrCreate().parallelize(layer)

    metadata = {
        'cellType': 'int32ud-1',
        'extent': extent,
        'crs': '+proj=longlat +datum=WGS84 +no_defs ',
        'bounds': {
            'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(date1)},
            'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(date1)}
        },
        'layoutDefinition': {
            'extent': extent,
            'tileLayout': layout
        }
    }

    return TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata) \
        .convert_data_type('int32', no_data_value=-1)



@pytest.fixture
def imagecollection_with_two_bands_and_one_date(request):
    import geopyspark as gps
    from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata
    geopyspark_layer = layer_with_two_bands_and_one_date()
    datacube = GeopysparkDataCube(pyramid=gps.Pyramid({0: geopyspark_layer}), metadata=GeopysparkCubeMetadata(openeo_metadata))

    if request.instance:
        request.instance.imagecollection_with_two_bands_and_one_date = datacube
    return datacube


@pytest.fixture
def imagecollection_with_two_bands_and_three_dates(request):
    from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
    from geopyspark.geotrellis.constants import LayerType
    from geopyspark.geotrellis.layer import TiledRasterLayer
    import geopyspark as gps

    from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata

    date1, date3, rdd = numpy_rdd_two_bands_and_three_dates()

    metadata = {'cellType': 'int32ud-1',
                'extent': extent,
                'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                'bounds': {
                    'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(date1)},
                    'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(date3)}
                },
                'layoutDefinition': {
                    'extent': extent,
                    'tileLayout': layout
                }
                }

    geopyspark_layer = TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata).convert_data_type('int32',no_data_value=-1)

    import copy
    openeo_metadata_copy = copy.deepcopy(openeo_metadata)
    openeo_metadata_copy["cube:dimensions"]["t"]["extent"] = [date1,date3]

    datacube = GeopysparkDataCube(pyramid=gps.Pyramid({0: geopyspark_layer}), metadata=GeopysparkCubeMetadata(openeo_metadata_copy))
    if request.instance:
        request.instance.imagecollection_with_two_bands_and_three_dates = datacube
    return datacube


def numpy_rdd_two_bands_and_three_dates():
    from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
    from pyspark import SparkContext

    two_band_one_two = np.array([matrix_of_one, matrix_of_two], dtype='int')
    first_tile = Tile.from_numpy_array(two_band_one_two, -1)
    second_tile = Tile.from_numpy_array(np.array([matrix_of_two, matrix_of_one], dtype='int'), -1)
    nodata_tile = Tile.from_numpy_array(np.array([matrix_of_nodata, matrix_of_nodata], dtype='int'), -1)
    date1 = datetime.datetime.strptime("2017-09-25T11:37:00Z", '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
    date2 = datetime.datetime.strptime("2017-09-30T00:37:00Z", '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
    date3 = datetime.datetime.strptime("2017-10-25T11:37:00Z", '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
    layer = [(SpaceTimeKey(0, 0, date1), first_tile),
             (SpaceTimeKey(1, 0, date1), first_tile),
             (SpaceTimeKey(0, 1, date1), first_tile),
             (SpaceTimeKey(1, 1, date1), first_tile),
             (SpaceTimeKey(0, 0, date2), nodata_tile),
             (SpaceTimeKey(1, 0, date2), nodata_tile),
             (SpaceTimeKey(0, 1, date2), nodata_tile),
             (SpaceTimeKey(1, 1, date2), nodata_tile),
             (SpaceTimeKey(0, 0, date3), second_tile),
             (SpaceTimeKey(1, 0, date3), second_tile),
             (SpaceTimeKey(0, 1, date3), second_tile),
             (SpaceTimeKey(1, 1, date3), second_tile)
             ]
    rdd = SparkContext.getOrCreate().parallelize(layer)
    return date1, date3, rdd


@pytest.fixture
def imagecollection_with_two_bands_and_three_dates_webmerc(request):
    from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
    from geopyspark.geotrellis.constants import LayerType
    from geopyspark.geotrellis.layer import TiledRasterLayer
    import geopyspark as gps

    from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube,GeopysparkCubeMetadata

    date1, date3, rdd = numpy_rdd_two_bands_and_three_dates()

    metadata = {'cellType': 'int32ud-1',
                'extent': extent_webmerc,
                'crs': '+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +no_defs ',
                'bounds': {
                    'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(date1)},
                    'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(date3)}
                },
                'layoutDefinition': {
                    'extent': extent_webmerc,
                    'tileLayout': layout
                }
                }

    geopyspark_layer = TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)

    datacube = GeopysparkDataCube(pyramid=gps.Pyramid({0: geopyspark_layer}), metadata=GeopysparkCubeMetadata(openeo_metadata))
    if request.instance:
        request.instance.imagecollection_with_two_bands_and_three_dates = datacube
    return datacube


@pytest.fixture
def imagecollection_with_two_bands_spatial_only(request):
    import geopyspark as gps
    from geopyspark.geotrellis import (SpatialKey, Tile)
    from geopyspark.geotrellis.constants import LayerType
    from geopyspark.geotrellis.layer import TiledRasterLayer
    from pyspark import SparkContext
    from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata

    two_band_one_two = np.array([matrix_of_one, matrix_of_two], dtype='int')
    tile = Tile.from_numpy_array(two_band_one_two, -1)

    layer = [(SpatialKey(0, 0), tile),
             (SpatialKey(1, 0), tile),
             (SpatialKey(0, 1), tile),
             (SpatialKey(1, 1), tile)]

    rdd = SparkContext.getOrCreate().parallelize(layer)

    metadata = {
        'cellType': 'float64ud-1.0',
        'extent': extent,
        'crs': '+proj=longlat +datum=WGS84 +no_defs ',
        'bounds': {
            'minKey': {'col': 0, 'row': 0},
            'maxKey': {'col': 1, 'row': 1}
        },
        'layoutDefinition': {
            'extent': extent,
            'tileLayout': layout
        }
    }

    input_layer = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, metadata)
    datacube = GeopysparkDataCube(
        pyramid=gps.Pyramid({0: input_layer}),
        metadata=GeopysparkCubeMetadata(openeo_metadata_spatial())
    )

    if request.instance:
        request.instance.imagecollection_with_two_bands_and_three_dates = datacube
    return datacube


def layer_with_two_bands_and_one_date_multiple_values():
    from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
    from geopyspark.geotrellis.constants import LayerType
    from geopyspark.geotrellis.layer import TiledRasterLayer
    from pyspark import SparkContext

    tilesize = 256
    matrix1 = np.zeros((1, tilesize, tilesize))
    for i in range(32):
        matrix1[0, i, :] = i
    matrix2 = np.zeros((1, tilesize, tilesize))
    for i in range(tilesize):
        matrix2[0, i, :] = i + tilesize

    bands = np.array([matrix1, matrix2], dtype='int')
    tile = Tile.from_numpy_array(bands, -1)

    date1 = datetime.datetime.strptime("2017-09-25T11:37:00Z", '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)

    layer = [(SpaceTimeKey(0, 0, date1), tile),
             (SpaceTimeKey(1, 0, date1), tile),
             (SpaceTimeKey(0, 1, date1), tile),
             (SpaceTimeKey(1, 1, date1), tile)]

    rdd = SparkContext.getOrCreate().parallelize(layer)

    metadata = {
        'cellType': 'int32ud-1',
        'extent': extent,
        'crs': '+proj=longlat +datum=WGS84 +no_defs ',
        'bounds': {
            'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(date1)},
            'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(date1)}
        },
        'layoutDefinition': {
            'extent': extent,
            'tileLayout': {'layoutCols': 1, 'layoutRows': 1, 'tileCols': tilesize, 'tileRows': tilesize}
        }
    }

    return TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata) \
        .convert_data_type('int32', no_data_value=-1)


def layer_with_one_band_and_three_dates():
    from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
    from geopyspark.geotrellis.constants import LayerType
    from geopyspark.geotrellis.layer import TiledRasterLayer
    from pyspark import SparkContext

    band1 = np.array([
        [-1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0]])

    band2 = np.array([
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, -1.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0]])

    band1 = double_size_2d_array_repeat(band1, 5)
    band2 = double_size_2d_array_repeat(band2, 5)

    tile1 = Tile.from_numpy_array(band1, no_data_value=-1.0)
    tile2 = Tile.from_numpy_array(band2, no_data_value=-1.0)
    time_1 = datetime.datetime.strptime("2016-08-24T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
    time_2 = datetime.datetime.strptime("2017-08-24T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
    time_3 = datetime.datetime.strptime("2017-10-17T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')

    layer = [(SpaceTimeKey(0, 0, time_1), tile1),
             (SpaceTimeKey(1, 0, time_1), tile2),
             (SpaceTimeKey(0, 1, time_1), tile1),
             (SpaceTimeKey(1, 1, time_1), tile1),
             (SpaceTimeKey(0, 0, time_2), tile2),
             (SpaceTimeKey(1, 0, time_2), tile2),
             (SpaceTimeKey(0, 1, time_2), tile2),
             (SpaceTimeKey(1, 1, time_2), tile2),
             (SpaceTimeKey(0, 0, time_3), tile1),
             (SpaceTimeKey(1, 0, time_3), tile2),
             (SpaceTimeKey(0, 1, time_3), tile1),
             (SpaceTimeKey(1, 1, time_3), tile1)
             ]

    rdd = SparkContext.getOrCreate().parallelize(layer)

    extent = {'xmin': 1.0, 'ymin': 1.0, 'xmax': 5.0, 'ymax': 5.0}
    layout = {'layoutCols': 2, 'layoutRows': 2, 'tileCols': len(band1[0]), 'tileRows': len(band1)}
    metadata = {'cellType': 'float32ud-1.0',
                'extent': extent,
                'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                'bounds': {
                    'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(time_1)},
                    'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(time_3)}},
                'layoutDefinition': {
                    'extent': extent,
                    'tileLayout': layout.copy()}}

    return TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)


@pytest.fixture
def imagecollection_with_two_bands_and_one_date_multiple_values(request):
    import geopyspark as gps
    from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata
    geopyspark_layer = layer_with_two_bands_and_one_date_multiple_values()
    datacube = GeopysparkDataCube(pyramid=gps.Pyramid({0: geopyspark_layer}), metadata=openeo_metadata)

    if request.instance:
        request.instance.imagecollection_with_two_bands_and_one_date_multiple_values = datacube
    return datacube
