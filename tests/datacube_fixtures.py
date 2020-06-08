import datetime

import geopyspark as gps
import numpy as np
import pytest
import pytz
from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from pyspark import SparkContext

from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from openeogeotrellis.service_registry import InMemoryServiceRegistry

matrix_of_one = np.zeros((1, 4, 4))
matrix_of_one.fill(1)

matrix_of_two = np.zeros((1, 4, 4))
matrix_of_two.fill(2)

matrix_of_zero = np.zeros((1, 4, 4))

extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 4.0, 'ymax': 4.0}
layout = {'layoutCols': 1, 'layoutRows': 1, 'tileCols': 4, 'tileRows': 4}



openeo_metadata = {
    "cube:dimensions": {
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

    datacube = GeotrellisTimeSeriesImageCollection(gps.Pyramid({0: geopyspark_layer}), InMemoryServiceRegistry(), openeo_metadata)

    request.instance.imagecollection_with_two_bands_and_one_date = datacube
    return datacube


@pytest.fixture
def imagecollection_with_two_bands_and_two_dates():
    two_band_one_two = np.array([matrix_of_one, matrix_of_two], dtype='int')
    first_tile = Tile.from_numpy_array(two_band_one_two, -1)
    second_tile = Tile.from_numpy_array(np.array([matrix_of_two, matrix_of_one], dtype='int'), -1)

    layer = [(SpaceTimeKey(0, 0, now), first_tile),
             (SpaceTimeKey(1, 0, now), first_tile),
             (SpaceTimeKey(0, 1, now), first_tile),
             (SpaceTimeKey(1, 1, now), first_tile)]

    rdd = SparkContext.getOrCreate().parallelize(layer)

    metadata = {'cellType': 'int32ud-1',
                'extent': extent,
                'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                'bounds': {
                    'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(now)},
                    'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(now)}
                },
                'layoutDefinition': {
                    'extent': extent,
                    'tileLayout': layout
                }
                }

    geopyspark_layer = TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)

    return GeotrellisTimeSeriesImageCollection(gps.Pyramid({0: geopyspark_layer}), InMemoryServiceRegistry(), openeo_metadata)


