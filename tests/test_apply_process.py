import datetime
from unittest import TestCase

from .base_test_class import BaseTestClass
BaseTestClass.setup_local_spark()
import numpy as np
import geopyspark as gps
from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from openeogeotrellis.service_registry import InMemoryServiceRegistry
from shapely.geometry import Point, Polygon
import pytz

import openeo_udf.functions
from pyspark import SparkContext


class TestCustomFunctions(TestCase):

    first = np.zeros((1, 4, 4))
    first.fill(10)

    second = np.zeros((1, 4, 4))
    second.fill(5)

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

    openeo_metadata = {
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
        "data_id": "CGS_SENTINEL2_RADIOMETRY_V101",
        "description": "Sentinel 2 Level-2: Bottom-of-atmosphere reflectances in cartographic geometry",
        "extent": {
            "bottom": 39,
            "crs": "EPSG:4326",
            "left": -34,
            "right": 35,
            "top": 71
        },
        "product_id": "CGS_SENTINEL2_RADIOMETRY_V101",
        "source": "Terrascope - VITO",
        "time": {
            "from": "2016-01-01",
            "to": "2019-10-01"
        }
    }

    def create_spacetime_layer(self):
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

    def create_spacetime_layer_singleband(self):
        cells = np.array([self.first], dtype='int')
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



    def test_point_series(self):

        input = self.create_spacetime_layer()

        imagecollection = GeotrellisTimeSeriesImageCollection(gps.Pyramid({0: input}), InMemoryServiceRegistry())
        transformed_collection = imagecollection.apply("cos")
        import math
        for p in self.points[0:3]:
            result = transformed_collection.timeseries(p.x, p.y)
            print(result)
            value = result.popitem()

            self.assertEqual(math.cos(10),value[1][0])
            self.assertEqual(math.cos(5), value[1][1])


    def test_reduce_bands(self):
        input = self.create_spacetime_layer()
        input = gps.Pyramid({0: input})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())

        from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
        visitor = GeotrellisTileProcessGraphVisitor()
        graph = {
            "sum": {
                "arguments": {
                    "data": {
                        "from_argument": "dimension_data"
                    }
                },
                "process_id": "sum"
            },
            "subtract": {
                "arguments": {
                    "data": {
                        "from_argument": "dimension_data"
                    }
                },
                "process_id": "subtract"
            },
            "divide": {
                "arguments": {
                    "data":[ {
                        "from_node": "sum"
                    },
                    {
                        "from_node": "subtract"
                    }
                    ]
                },
                "process_id": "divide",
                "result": True
            }
        }
        visitor.accept_process_graph(graph)
        stitched = imagecollection.reduce_bands(visitor).pyramid.levels[0].to_spatial_layer().stitch()
        print(stitched)
        self.assertEqual(3.0, stitched.cells[0][0][0])

    def test_reduce_bands_logical_ops(self):
        input = self.create_spacetime_layer_singleband()
        input = gps.Pyramid({0: input})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())

        from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
        visitor = GeotrellisTileProcessGraphVisitor()
        graph = {
            "eq": {
                "arguments": {
                    "x": {
                        "from_argument": "data"
                    },
                    "y": 10
                },
                "process_id": "eq",
            },
            "not": {
                "arguments": {
                    "expression": {
                        "from_node": "eq"
                    }
                },
                "process_id": "not",
                "result": True
            }
        }
        visitor.accept_process_graph(graph)
        stitched = imagecollection.reduce_bands(visitor).pyramid.levels[0].to_spatial_layer().stitch()
        print(stitched)
        self.assertEqual(0, stitched.cells[0][0][0])

    def test_reduce_bands_arrayelement(self):
        input = self.create_spacetime_layer()
        input = gps.Pyramid({0: input})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())

        from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
        visitor = GeotrellisTileProcessGraphVisitor()
        graph = {
            "sum": {
                "arguments": {
                    "data": {
                        "from_argument": "dimension_data"
                    }
                },
                "process_id": "sum"
            },
            "array_element": {
                "arguments": {
                    "data": {
                        "from_argument": "dimension_data"
                    },
                    "index": 1
                },
                "process_id": "array_element"
            },
            "divide": {
                "arguments": {
                    "data":[ {
                        "from_node": "sum"
                    },
                    {
                        "from_node": "array_element"
                    }
                    ]
                },
                "process_id": "divide",
                "result": True
            }
        }
        visitor.accept_process_graph(graph)
        stitched = imagecollection.reduce_bands(visitor).pyramid.levels[0].to_spatial_layer().stitch()
        print(stitched)
        self.assertEqual(3.0, stitched.cells[0][0][0])
