import datetime

import numpy as np
import pytest
import pytz

from openeogeotrellis.service_registry import InMemoryServiceRegistry

matrix_of_one = np.zeros((1, 4, 4))
matrix_of_one.fill(1)

matrix_of_two = np.zeros((1, 4, 4))
matrix_of_two.fill(2)

matrix_of_nodata = np.zeros((1, 4, 4))
matrix_of_nodata.fill(-1)

extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 4.0, 'ymax': 4.0}
extent_webmerc = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 445277.96317309426, 'ymax': 445640.1096560266}
layout = {'layoutCols': 1, 'layoutRows': 1, 'tileCols': 4, 'tileRows': 4}



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


@pytest.fixture
def imagecollection_with_two_bands_and_one_date(request):
    import geopyspark as gps
    from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
    from geopyspark.geotrellis.constants import LayerType
    from geopyspark.geotrellis.layer import TiledRasterLayer
    from pyspark import SparkContext

    from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube

    print(request)
    two_band_one_two = np.array([matrix_of_one, matrix_of_two], dtype='int')
    tile = Tile.from_numpy_array(two_band_one_two, -1)

    date1 = datetime.datetime.strptime("2017-09-25T11:37:00Z", '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)


    layer = [(SpaceTimeKey(0, 0, date1), tile),
             (SpaceTimeKey(1, 0, date1), tile),
             (SpaceTimeKey(0, 1, date1), tile),
             (SpaceTimeKey(1, 1, date1), tile)]

    rdd = SparkContext.getOrCreate().parallelize(layer)

    metadata = {'cellType': 'int32ud-1',
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

    geopyspark_layer = TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)

    datacube = GeopysparkDataCube(pyramid=gps.Pyramid({0: geopyspark_layer}), metadata=openeo_metadata)

    request.instance.imagecollection_with_two_bands_and_one_date = datacube
    return datacube


@pytest.fixture
def imagecollection_with_two_bands_and_three_dates(request):
    from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
    from geopyspark.geotrellis.constants import LayerType
    from geopyspark.geotrellis.layer import TiledRasterLayer
    import geopyspark as gps

    from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube

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

    geopyspark_layer = TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)

    datacube = GeopysparkDataCube(pyramid=gps.Pyramid({0: geopyspark_layer}), metadata=openeo_metadata)
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

    from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube

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

    datacube = GeopysparkDataCube(pyramid=gps.Pyramid({0: geopyspark_layer}), metadata=openeo_metadata)
    if request.instance:
        request.instance.imagecollection_with_two_bands_and_three_dates = datacube
    return datacube
