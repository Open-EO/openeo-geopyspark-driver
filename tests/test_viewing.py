import datetime
from unittest import TestCase

import geopyspark as gps
import numpy as np
import requests
from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from pyspark import SparkContext
from shapely.geometry import Point

from openeogeotrellis import GeotrellisTimeSeriesImageCollection
from openeogeotrellis.service_registry import InMemoryServiceRegistry


class TestViewing(TestCase):

    first = np.zeros((1, 4, 4))
    first.fill(1)

    second = np.zeros((1, 4, 4))
    second.fill(2)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 4.0, 'ymax': 4.0}
    layout = {'layoutCols': 1, 'layoutRows': 1, 'tileCols': 4, 'tileRows': 4}

    now = datetime.datetime.strptime("2017-09-25T11:37:00Z", '%Y-%m-%dT%H:%M:%SZ')

    points = [
        Point(1.0, -3.0),
        Point(2.0, 4.0),
        Point(3.0, 3.0),
        Point(1.0, -2.0),
        Point(-10.0, 15.0)
    ]

    labeled_points = {
        'A': points[0],
        'B': points[1],
        'C': points[2],
        'D': points[3],
        'E': points[4]
    }

    expected_spatial_points_list = [
        (Point(1.0, -3.0), [1, 2]),
        (Point(2.0, 4.0), [1, 2]),
        (Point(3.0, 3.0), [1, 2]),
        (Point(1.0, -2.0), [1, 2]),
        (Point(-10.0, 15.0), None)
    ]

    expected_spacetime_points_list = [
        (Point(1.0, -3.0), now, [1, 2]),
        (Point(2.0, 4.0), now, [1, 2]),
        (Point(3.0, 3.0), now, [1, 2]),
        (Point(1.0, -2.0), now, [1, 2]),
        (Point(-10.0, 15.0), None, None)
    ]


    def create_spacetime_layer(self) -> TiledRasterLayer:
        cells = np.array([self.first, self.second], dtype='int')
        tile = Tile.from_numpy_array(cells, -1)

        layer = [(SpaceTimeKey(0, 0, self.now), tile),
                 (SpaceTimeKey(1, 0, self.now), tile),
                 (SpaceTimeKey(0, 1, self.now), tile),
                 (SpaceTimeKey(1, 1, self.now), tile)]

        rdd = SparkContext.getOrCreate().parallelize(layer)

        metadata = {'cellType': 'int32ud-1',
                    'extent': self.extent,
                    'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                    'bounds': {
                        'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(self.now)},
                        'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(self.now)}
                    },
                    'layoutDefinition': {
                        'extent': self.extent,
                        'tileLayout': self.layout
                    }
                    }

        return TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)


    def test_viewing(self):
        geotrellis_layer = self.create_spacetime_layer()
        imagecollection = GeotrellisTimeSeriesImageCollection(gps.Pyramid({0: geotrellis_layer}), InMemoryServiceRegistry())
        metadata = imagecollection.tiled_viewing_service(type="TMS", process_graph={})
        print(metadata)
        self.assertEqual('TMS',metadata['type'])
        self.assertIsNotNone(metadata['bounds'])
        tileresponse = requests.get(metadata['url'].format(x=0, y=0, z=0), timeout=2)
        self.assertEqual(200,tileresponse.status_code)
        tileresponse = requests.get(metadata['url'].format(x=1, y=1, z=0), timeout=2)
        self.assertEqual(200,tileresponse.status_code)
