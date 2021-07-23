import datetime
import math
from typing import List
from unittest import TestCase
import pytest

import geopyspark as gps
import numpy as np
import pytz
from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from openeo_driver.errors import OpenEOApiException
from pyspark import SparkContext
from shapely.geometry import Point

from openeo_driver.utils import EvalEnv
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata
from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
from openeogeotrellis.service_registry import InMemoryServiceRegistry


def _build_metadata(bands: List[str] = ["B01", "B02"]) -> GeopysparkCubeMetadata:
    """Helper to build metadata instance"""
    return GeopysparkCubeMetadata({
        "cube:dimensions": {
            "bands": {"type": "bands", "values": bands}
        },
        "summaries": {
            "eo:bands": [{"name": b, "common_name": "common" + b} for b in bands]
        }
    })


class TestApplyProcess(TestCase):

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

    def _create_spacetime_layer(self, cells: np.ndarray = None) -> TiledRasterLayer:
        # TODO all these "create_spacetime_layer" functions are duplicated across all tests
        #       and better should be moved to some kind of general factory or test fixture
        assert len(cells.shape) == 4
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

    def create_spacetime_layer(self) -> TiledRasterLayer:
        cells = np.array([self.first, self.second], dtype='int')
        return self._create_spacetime_layer(cells)

    def create_spacetime_layer_singleband(self) -> TiledRasterLayer:
        cells = np.array([self.first], dtype='int')
        return self._create_spacetime_layer(cells)


    def test_point_series(self):

        input = self.create_spacetime_layer()

        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
        transformed_collection = imagecollection.apply("cos")
        for p in self.points[0:3]:
            result = transformed_collection.timeseries(p.x, p.y)
            print(result)
            value = result.popitem()

            self.assertEqual(math.cos(10),value[1][0])
            self.assertEqual(math.cos(5), value[1][1])

    def test_apply_cos(self):
        input = self.create_spacetime_layer()
        cube = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
        res = cube.apply("cos")
        data = res.pyramid.levels[0].to_spatial_layer().stitch().cells
        np.testing.assert_array_almost_equal(data[0, 2:6, 2:6], np.cos(self.first[0]))
        np.testing.assert_array_almost_equal(data[1, 2:6, 2:6], np.cos(self.second[0]))

    def test_apply_complex_graph(self):
        graph = {
            "sin": {
                "arguments": {
                    "x": {
                        "from_argument": "data"
                    }
                },
                "process_id": "sin",
                "result": False
            },
            "multiply": {
                "arguments": {
                    "x": {
                        "from_node": "sin"
                    },
                    "y": 5.0
                },
                "process_id": "multiply",
                "result": True
            }
        }

        input = self.create_spacetime_layer()
        cube = GeopysparkDataCube(gps.Pyramid({0: input}), InMemoryServiceRegistry())
        res = cube.apply(graph)
        data = res.pyramid.levels[0].to_spatial_layer().stitch().cells
        np.testing.assert_array_almost_equal(data[0, 2:6, 2:6], 5.0*np.sin(self.first[0]))
        np.testing.assert_array_almost_equal(data[1, 2:6, 2:6], 5.0*np.sin(self.second[0]))

    def test_reduce_bands(self):
        input = self.create_spacetime_layer()
        input = gps.Pyramid({0: input})
        collection_metadata = GeopysparkCubeMetadata({
            "cube:dimensions": {
                "my_bands": {"type": "bands", "values": ["B04", "B08"]},
            }
        })
        imagecollection = GeopysparkDataCube(pyramid=input, metadata=collection_metadata)

        visitor = GeotrellisTileProcessGraphVisitor()
        graph = {
            "sum": {
                "arguments": {
                    "data": {
                        "from_argument": "dimension_data"
                    },
                    "ignore_nodata":True
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
        stitched = imagecollection.reduce_dimension(dimension='my_bands', reducer=visitor, env=EvalEnv()).pyramid.levels[0].to_spatial_layer().stitch()
        print(stitched)
        self.assertEqual(3.0, stitched.cells[0][0][0])

    def test_reduce_bands_logical_ops(self):
        input = self.create_spacetime_layer_singleband()
        input = gps.Pyramid({0: input})

        imagecollection = GeopysparkDataCube(pyramid=input)

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

    def test_apply_if(self):
        input = self.create_spacetime_layer_singleband()
        input = gps.Pyramid({0: input})

        imagecollection = GeopysparkDataCube(pyramid=input)

        graph = {
            "6": {
              "arguments": {
                "reject": {"from_parameter":"x"},
                "value": {
                  "from_node": "10"
                },
                "accept": 2.0

              },
              "process_id": "if",
              "result": True
            },
            "10": {
              "process_id": "gt",
              "arguments": {
                "x": {
                  "from_parameter": "x"
                },
                "y": 7.0
              }
            }
          }

        stitched = imagecollection.apply(graph).pyramid.levels[0].to_spatial_layer().stitch()
        print(stitched)
        self.assertEqual(2.0, stitched.cells[0][0][0])

    def test_reduce_bands_comparison_ops(self):
        input = self.create_spacetime_layer_singleband()
        input = gps.Pyramid({0: input})

        imagecollection = GeopysparkDataCube(pyramid=input)

        visitor = GeotrellisTileProcessGraphVisitor()
        graph = {
            "gt": {
                "arguments": {
                    "x": {
                        "from_argument": "data"
                    },
                    "y": 6.0
                },
                "process_id": "gt",
                "result": True
            }
        }
        visitor.accept_process_graph(graph)
        stitched = imagecollection.reduce_bands(visitor).pyramid.levels[0].to_spatial_layer().stitch()
        print(stitched)
        self.assertEqual(1, stitched.cells[0][0][0])

    def test_reduce_bands_arrayelement(self):
        input = self.create_spacetime_layer()
        input = gps.Pyramid({0: input})

        imagecollection = GeopysparkDataCube(pyramid=input)

        visitor = GeotrellisTileProcessGraphVisitor()
        graph ={
                    "arrayelement3": {
                        "process_id": "array_element",
                        "result": False,
                        "arguments": {
                            "data": {
                                "from_argument": "data"
                            },
                            "index": 0
                        }
                    },
                    "subtract1": {
                        "process_id": "subtract",
                        "result": False,
                        "arguments": {
                            "data": [
                                {
                                    "from_node": "arrayelement1"
                                },
                                {
                                    "from_node": "arrayelement2"
                                }
                            ]
                        }
                    },
                    "arrayelement4": {
                        "process_id": "array_element",
                        "result": False,
                        "arguments": {
                            "data": {
                                "from_argument": "data"
                            },
                            "index": 1
                        }
                    },
                    "arrayelement1": {
                        "process_id": "array_element",
                        "result": False,
                        "arguments": {
                            "data": {
                                "from_argument": "data"
                            },
                            "index": 0
                        }
                    },
                    "divide1": {
                        "process_id": "divide",
                        "result": True,
                        "arguments": {
                            "data": [
                                {
                                    "from_node": "sum1"
                                },
                                {
                                    "from_node": "subtract1"
                                }
                            ]
                        }
                    },
                    "sum1": {
                        "process_id": "sum",
                        "result": False,
                        "arguments": {
                            "data": [
                                {
                                    "from_node": "arrayelement3"
                                },
                                {
                                    "from_node": "arrayelement4"
                                }
                            ]
                        }
                    },
                    "arrayelement2": {
                        "process_id": "array_element",
                        "result": False,
                        "arguments": {
                            "data": {
                                "from_argument": "data"
                            },
                            "index": 1
                        }
                    }
                }
        visitor.accept_process_graph(graph)
        stitched = imagecollection.reduce_bands(visitor).pyramid.levels[0].to_spatial_layer().stitch()
        print(stitched)
        self.assertEqual(3.0, stitched.cells[0][0][0])

    def test_ndvi(self):
        imagecollection = self.create_red_nir_layer()

        stitched = imagecollection.ndvi().pyramid.levels[0].to_spatial_layer().stitch()
        cells = stitched.cells[0, 0:4, 0:4]
        expected = np.array([
            [np.nan, 1 / 1, 2 / 2, 3 / 3],
            [-1 / 1, 0 / 2, 1 / 3, 2 / 4],
            [-2 / 2, -1 / 3, 0 / 4, 1 / 5],
            [-3 / 3, -2 / 4, -1 / 5, 0 / 6]
        ])
        np.testing.assert_array_almost_equal(cells, expected)

    def create_red_nir_layer(self):
        red_ramp, nir_ramp = np.mgrid[0:4, 0:4]
        layer = self._create_spacetime_layer(cells=np.array([[red_ramp], [nir_ramp]]))
        pyramid = gps.Pyramid({0: layer})
        metadata = GeopysparkCubeMetadata({
            "cube:dimensions": {
                # TODO: also specify other dimensions?
                "bands": {"type": "bands", "values": ["B04", "B08"]}
            },
            "summaries": {
                "eo:bands": [
                    {"name": "B04", "common_name": "red"},
                    {"name": "B08", "common_name": "nir"},
                ]
            }
        })
        imagecollection = GeopysparkDataCube(pyramid=pyramid, metadata=metadata)
        return imagecollection

    def test_linear_scale_range(self):
        imagecollection = self.create_red_nir_layer()

        stitched = imagecollection.ndvi().linear_scale_range(-1, 1, 0, 100).pyramid.levels[0].to_spatial_layer().stitch()
        cells = stitched.cells[0, 0:4, 0:4]
        expected =50.0*  (1.0 +np.array([
            [np.nan, 1 / 1, 2 / 2, 3 / 3],
            [-1 / 1, 0 / 2, 1 / 3, 2 / 4],
            [-2 / 2, -1 / 3, 0 / 4, 1 / 5],
            [-3 / 3, -2 / 4, -1 / 5, 0 / 6]
        ]))
        expected[0][0]=255.0
        np.testing.assert_array_almost_equal(cells, expected.astype(np.uint8))


    def test_linear_scale_range_reduce(self):
        imagecollection = self.create_red_nir_layer()

        visitor = GeotrellisTileProcessGraphVisitor()
        graph = {
            "scale": {
                "process_id": "linear_scale_range",
                "result": True,
                "arguments": {
                    "x": {
                        "from_argument": "data"
                    },
                    "inputMin": -1,
                    "inputMax": 1,
                    "outputMin": 0,
                    "outputMax": 100,
                }
            }

        }
        visitor.accept_process_graph(graph)

        scaled_layer = imagecollection.ndvi().reduce_bands(visitor).pyramid.levels[0].to_spatial_layer()
        assert scaled_layer.layer_metadata.cell_type == 'uint8ud255'
        stitched = scaled_layer.stitch()
        cells = stitched.cells[0, 0:4, 0:4]
        expected =50.0*  (1.0 +np.array([
            [np.nan, 1 / 1, 2 / 2, 3 / 3],
            [-1 / 1, 0 / 2, 1 / 3, 2 / 4],
            [-2 / 2, -1 / 3, 0 / 4, 1 / 5],
            [-3 / 3, -2 / 4, -1 / 5, 0 / 6]
        ]))
        expected[0][0]=255.0
        np.testing.assert_array_almost_equal(cells, expected.astype(np.uint8))

    def _test_merge_cubes_subtract_spatial(self, left_spatial=False, right_spatial=False):
        # TODO: this would be cleaner with @pytest.mark.parameterize but that's not supported on TestCase methods
        red_ramp, nir_ramp = np.mgrid[0:4, 0:4]
        layer1 = self._create_spacetime_layer(cells=np.array([[red_ramp]]))
        if left_spatial:
            layer1 = layer1.to_spatial_layer()
        layer2 = self._create_spacetime_layer(cells=np.array([[nir_ramp]]))
        if right_spatial:
            layer2 = layer2.to_spatial_layer()
        metadata = _build_metadata()
        cube1 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer1}), metadata=metadata)
        cube2 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer2}), metadata=metadata)

        res = cube1.merge_cubes(cube2, 'subtract')
        layer = res.pyramid.levels[0]
        if layer.layer_type != LayerType.SPATIAL:
            layer = layer.to_spatial_layer()
        actual = layer.stitch().cells[0, 0:4, 0:4]
        expected = red_ramp - nir_ramp
        np.testing.assert_array_equal(expected, actual)

    def test_merge_cubes_subtract_spatial_0_0(self):
        self._test_merge_cubes_subtract_spatial(False, False)

    def test_merge_cubes_subtract_spatial_0_1(self):
        self._test_merge_cubes_subtract_spatial(False, True)

    def test_merge_cubes_subtract_spatial_1_0(self):
        self.skipTest("TODO EP-3635 still Causes exception in geotrellis extension")
        self._test_merge_cubes_subtract_spatial(True, False)

    def test_merge_cubes_subtract_spatial_1_1(self):
        self._test_merge_cubes_subtract_spatial(True, True)

    def test_merge_cubes_into_single_band(self):
        red_ramp, nir_ramp = np.mgrid[0:4, 0:4]
        layer1 = self._create_spacetime_layer(cells=np.array([[red_ramp]]))
        layer2 = self._create_spacetime_layer(cells=np.array([[nir_ramp]]))
        metadata = _build_metadata(bands=["the_band"])
        cube1 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer1}), metadata=metadata)
        cube2 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer2}), metadata=metadata)
        res = cube1.merge_cubes(cube2, 'sum')
        stitched = res.pyramid.levels[0].to_spatial_layer().stitch()
        assert stitched.cells.shape[0] == 1
        np.testing.assert_array_equal(red_ramp + nir_ramp, stitched.cells[0, 0:4, 0:4])

    def test_merge_cubes_exception_if_levels_do_not_match(self):
        red_ramp, nir_ramp = np.mgrid[0:4, 0:4]
        layer1 = self._create_spacetime_layer(cells=np.array([[red_ramp]]))
        layer2 = self._create_spacetime_layer(cells=np.array([[nir_ramp]]))
        metadata = _build_metadata(bands=["the_band"])
        cube1 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer1}), metadata=metadata)
        cube2 = GeopysparkDataCube(pyramid=gps.Pyramid({14: layer2}), metadata=metadata)
        with pytest.raises(OpenEOApiException) as excinfo:
            res = cube1.merge_cubes(cube2, 'sum')


        
    def test_merge_cubes_into_separate_bands(self):
        red_ramp, nir_ramp = np.mgrid[0:4, 0:4]
        layer1 = self._create_spacetime_layer(cells=np.array([[red_ramp]]))
        layer2 = self._create_spacetime_layer(cells=np.array([[nir_ramp]]))

        metadata1 = _build_metadata(bands=["the_band_1"])
        metadata2 = _build_metadata(bands=["the_band_2"])

        cube1 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer1}), metadata=metadata1)
        cube2 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer2}), metadata=metadata2)
        res = cube1.merge_cubes(cube2)
        stitched = res.pyramid.levels[0].to_spatial_layer().stitch()
        assert stitched.cells.shape[0] == 2
        np.testing.assert_array_equal(red_ramp, stitched.cells[0, 0:4, 0:4])        
        np.testing.assert_array_equal(nir_ramp, stitched.cells[1, 0:4, 0:4])        
        