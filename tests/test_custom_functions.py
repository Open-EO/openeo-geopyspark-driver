import datetime

import numpy as np
import geopyspark as gps
from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from shapely.geometry import Point, Polygon
import pytz

from .base_test_class import BaseTestClass


class TestCustomFunctions(BaseTestClass):

    first = np.zeros((1, 4, 4))
    first.fill(1)

    second = np.zeros((1, 4, 4))
    second.fill(2)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 4.0, 'ymax': 4.0}
    layout = {'layoutCols': 1, 'layoutRows': 1, 'tileCols': 4, 'tileRows': 4}

    now = datetime.datetime.strptime("2017-09-25T11:37:00Z", '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)

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
        (Point(1.0, -3.0), now, [3]),
        (Point(2.0, 4.0), now, [3]),
        (Point(3.0, 3.0), now, [3]),
        (Point(1.0, -2.0), now, [3]),
        (Point(-10.0, 15.0), None, None)
    ]


    def create_spacetime_layer(self):
        cells = np.array([self.first, self.second], dtype='int')
        tile = Tile.from_numpy_array(cells, -1)

        layer = [(SpaceTimeKey(0, 0, self.now), tile),
                 (SpaceTimeKey(1, 0, self.now), tile),
                 (SpaceTimeKey(0, 1, self.now), tile),
                 (SpaceTimeKey(1, 1, self.now), tile)]

        rdd = BaseTestClass.pysc.parallelize(layer)

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




    def test_apply_to_tile(self):
        def custom_function(cells:np.ndarray,nd):
            return cells[0]+cells[1]
        cells = np.array([self.first, self.second], dtype='int')
        tile = Tile.from_numpy_array(cells, -1)
        custom_function(tile.cells,0)


    def test_point_series(self):

        def custom_function(cells:np.ndarray,nd):
            return cells[0]+cells[1]

        input = self.create_spacetime_layer()

        imagecollection = GeotrellisTimeSeriesImageCollection(gps.Pyramid({0: input}))
        transformed_collection = imagecollection.apply_pixel([0, 1], custom_function)

        for p in self.points[0:3]:
            result = transformed_collection.timeseries(p.x, p.y)
            print(result)
            value = result.popitem()
            self.assertEqual(3.0,value[1][0])

    def test_polygon_series(self):
        input = self.create_spacetime_layer()

        polygon = Polygon([(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)])

        imagecollection = GeotrellisTimeSeriesImageCollection(gps.Pyramid({0: input}))

        means = imagecollection.polygonal_mean_timeseries(polygon)
        assert len(means) == 1
        assert [item[1] for item in means.items()][0] == [1.0, 2.0]
