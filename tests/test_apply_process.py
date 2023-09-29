import datetime
from typing import List

import geopyspark as gps
import math
import numpy as np
import pytest
import pytz
from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time, Metadata, Extent, LayoutDefinition,
                                   TileLayout, Bounds)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from pyspark import SparkContext
from shapely.geometry import Point

from .data import get_test_data_file
from openeo_driver.errors import OpenEOApiException
from openeo_driver.utils import EvalEnv
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata
from openeogeotrellis.processgraphvisiting import GeotrellisTileProcessGraphVisitor
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

def _create_spacetime_layer( cells: np.ndarray = None) -> TiledRasterLayer:
    # TODO all these "create_spacetime_layer" functions are duplicated across all tests
    #       and better should be moved to some kind of general factory or test fixture
    assert len(cells.shape) == 4
    tile = Tile.from_numpy_array(cells, -1)

    layer = [(SpaceTimeKey(0, 0, now), tile),
             (SpaceTimeKey(1, 0, now), tile),
             (SpaceTimeKey(0, 1, now), tile),
             (SpaceTimeKey(1, 1, now), tile)]

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

    return TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)

def create_spacetime_layer() -> TiledRasterLayer:
    cells = np.array([first, second], dtype='int')
    return _create_spacetime_layer(cells)

def create_spacetime_layer_singleband() -> TiledRasterLayer:
    cells = np.array([first], dtype='int')
    return _create_spacetime_layer(cells)


def test_point_series():
    input = create_spacetime_layer()
    cube = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
    transformed_collection = cube.apply(
        process={"cos": {"process_id": "cos", "arguments": {"x": {"from_parameter": "x"}}, "result": True}},
        env=EvalEnv(),
    )
    for p in points[0:3]:
        # TODO #421 drop old unsued "point timeseries" feature
        result = transformed_collection.timeseries(p.x, p.y)
        assert result == {"2017-09-25T11:37:00+00:00": [pytest.approx(math.cos(10)), pytest.approx(math.cos(5))]}


def test_apply_cos():
    input = create_spacetime_layer()
    cube = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
    res = cube.apply(
        process={"cos": {"process_id": "cos", "arguments": {"x": {"from_parameter": "x"}}, "result": True}},
        env=EvalEnv(),
    )
    data = res.pyramid.levels[0].to_spatial_layer().stitch().cells
    np.testing.assert_array_almost_equal(data[0, 2:6, 2:6], np.cos(first[0]))
    np.testing.assert_array_almost_equal(data[1, 2:6, 2:6], np.cos(second[0]))


def test_apply_complex_graph():
    graph = {
        "sin": {
            "arguments": {
                "x": {
                    "from_parameter": "x"
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

    input = create_spacetime_layer()
    cube = GeopysparkDataCube(gps.Pyramid({0: input}), InMemoryServiceRegistry())
    res = cube.apply(process=graph, env=EvalEnv())
    data = res.pyramid.levels[0].to_spatial_layer().stitch().cells
    np.testing.assert_array_almost_equal(data[0, 2:6, 2:6], 5.0*np.sin(first[0]))
    np.testing.assert_array_almost_equal(data[1, 2:6, 2:6], 5.0*np.sin(second[0]))


def test_reduce_bands():
    input = create_spacetime_layer()
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
                    "from_parameter": "data"
                },
                "ignore_nodata":True
            },
            "process_id": "sum"
        },
        # TODO: this does not make sense: `subtract` process on an array
        "subtract": {
            "arguments": {
                "data": {
                    "from_parameter": "data"
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
    assert 3.0 == stitched.cells[0][0][0]

def test_reduce_bands_logical_ops():
    input = create_spacetime_layer_singleband()
    input = gps.Pyramid({0: input})

    imagecollection = GeopysparkDataCube(pyramid=input)

    visitor = GeotrellisTileProcessGraphVisitor()
    graph = {
        "eq": {
            "arguments": {
                "x": {
                    "from_parameter": "x"
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
    assert 0 == stitched.cells[0][0][0]

def test_apply_if():
    input = create_spacetime_layer_singleband()
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

    stitched = imagecollection.apply(process=graph, env=EvalEnv()).pyramid.levels[0].to_spatial_layer().stitch()
    print(stitched)
    assert 2.0 == stitched.cells[0][0][0]

def test_reduce_bands_comparison_ops():
    input = create_spacetime_layer_singleband()
    input = gps.Pyramid({0: input})

    imagecollection = GeopysparkDataCube(pyramid=input)

    visitor = GeotrellisTileProcessGraphVisitor()
    graph = {
        "gt": {
            "arguments": {
                "x": {
                    "from_parameter": "x"
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
    assert 1 == stitched.cells[0][0][0]

def test_reduce_bands_arrayelement():
    input = create_spacetime_layer()
    input = gps.Pyramid({0: input})

    imagecollection = GeopysparkDataCube(pyramid=input)

    visitor = GeotrellisTileProcessGraphVisitor()
    graph ={
                "arrayelement3": {
                    "process_id": "array_element",
                    "result": False,
                    "arguments": {
                        "data": {
                            "from_parameter": "data"
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
                            "from_parameter": "data"
                        },
                        "index": 1
                    }
                },
                "arrayelement1": {
                    "process_id": "array_element",
                    "result": False,
                    "arguments": {
                        "data": {
                            "from_parameter": "data"
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
                            "from_parameter": "data"
                        },
                        "index": 1
                    }
                }
            }
    visitor.accept_process_graph(graph)
    stitched = imagecollection.reduce_bands(visitor).pyramid.levels[0].to_spatial_layer().stitch()
    print(stitched)
    assert 3.0 == stitched.cells[0][0][0]

@pytest.mark.parametrize("target_band", [None,"ndvi"])
def test_ndvi(target_band):
    imagecollection = create_red_nir_layer()

    stitched = imagecollection.ndvi(target_band=target_band).pyramid.levels[0].to_spatial_layer().stitch()
    index = 0
    if(target_band is not None):
        index = 2
    cells = stitched.cells[index, 0:4, 0:4]
    expected = np.array([
        [np.nan, 1 / 1, 2 / 2, 3 / 3],
        [-1 / 1, 0 / 2, 1 / 3, 2 / 4],
        [-2 / 2, -1 / 3, 0 / 4, 1 / 5],
        [-3 / 3, -2 / 4, -1 / 5, 0 / 6]
    ])
    np.testing.assert_array_almost_equal(cells, expected)

def create_red_nir_layer():
    red_ramp, nir_ramp = np.mgrid[0:4, 0:4]
    layer = _create_spacetime_layer(cells=np.array([[red_ramp], [nir_ramp]]))
    pyramid = gps.Pyramid({0: layer})
    metadata = GeopysparkCubeMetadata({
        "cube:dimensions": {
            "x": {"type": "spatial", "axis": "x"},
            "y": {"type": "spatial", "axis": "y"},
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

def test_linear_scale_range():
    imagecollection = create_red_nir_layer()

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


def test_linear_scale_range_reduce():
    imagecollection = create_red_nir_layer()

    visitor = GeotrellisTileProcessGraphVisitor()
    graph = {
        "scale": {
            "process_id": "linear_scale_range",
            "result": True,
            "arguments": {
                "x": {
                    "from_parameter": "x"
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

def _test_merge_cubes_subtract_spatial( left_spatial=False, right_spatial=False):
    # TODO: this would be cleaner with @pytest.mark.parameterize but that's not supported on TestCase methods
    red_ramp, nir_ramp = np.mgrid[0:4, 0:4]
    layer1 = _create_spacetime_layer(cells=np.array([[red_ramp]]))
    if left_spatial:
        layer1 = layer1.to_spatial_layer()
    layer2 = _create_spacetime_layer(cells=np.array([[nir_ramp]]))
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

def test_merge_cubes_subtract_spatial_0_0():
    _test_merge_cubes_subtract_spatial(False, False)

def test_merge_cubes_subtract_spatial_0_1():
    _test_merge_cubes_subtract_spatial(False, True)

@pytest.mark.skipif( reason="TODO EP-3635 still Causes exception in geotrellis extension")
def test_merge_cubes_subtract_spatial_1_0():
    _test_merge_cubes_subtract_spatial(True, False)

def test_merge_cubes_subtract_spatial_1_1():
    _test_merge_cubes_subtract_spatial(True, True)

def test_merge_cubes_into_single_band():
    red_ramp, nir_ramp = np.mgrid[0:4, 0:4]
    layer1 = _create_spacetime_layer(cells=np.array([[red_ramp]]))
    layer2 = _create_spacetime_layer(cells=np.array([[nir_ramp]]))
    metadata = _build_metadata(bands=["the_band"])
    cube1 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer1}), metadata=metadata)
    cube2 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer2}), metadata=metadata)
    res = cube1.merge_cubes(cube2, 'sum')
    stitched = res.pyramid.levels[0].to_spatial_layer().stitch()
    assert stitched.cells.shape[0] == 1
    np.testing.assert_array_equal(red_ramp + nir_ramp, stitched.cells[0, 0:4, 0:4])

def test_merge_cubes_exception_if_levels_do_not_match():
    red_ramp, nir_ramp = np.mgrid[0:4, 0:4]
    layer1 = _create_spacetime_layer(cells=np.array([[red_ramp]]))
    layer2 = _create_spacetime_layer(cells=np.array([[nir_ramp]]))
    metadata = _build_metadata(bands=["the_band"])
    cube1 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer1}), metadata=metadata)
    cube2 = GeopysparkDataCube(pyramid=gps.Pyramid({14: layer2}), metadata=metadata)
    with pytest.raises(OpenEOApiException) as excinfo:
        res = cube1.merge_cubes(cube2, 'sum')



def test_merge_cubes_into_separate_bands():
    red_ramp, nir_ramp = np.mgrid[0:4, 0:4]
    layer1 = _create_spacetime_layer(cells=np.array([[red_ramp]]))
    layer2 = _create_spacetime_layer(cells=np.array([[nir_ramp]]))

    metadata1 = _build_metadata(bands=["the_band_1"])
    metadata2 = _build_metadata(bands=["the_band_2"])

    cube1 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer1}), metadata=metadata1)
    cube2 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer2}), metadata=metadata2)
    res = cube1.merge_cubes(cube2)
    stitched = res.pyramid.levels[0].to_spatial_layer().stitch()
    assert stitched.cells.shape[0] == 2
    np.testing.assert_array_equal(red_ramp, stitched.cells[0, 0:4, 0:4])
    np.testing.assert_array_equal(nir_ramp, stitched.cells[1, 0:4, 0:4])


def test_merge_cubes_error():
    with pytest.raises(OpenEOApiException):
        red_ramp, nir_ramp = np.mgrid[0:4, 0:4]
        layer1 = _create_spacetime_layer(cells=np.array([[red_ramp]]))
        layer2 = _create_spacetime_layer(cells=np.array([[nir_ramp]]))

        metadata1 = _build_metadata(bands=["band1", "band2"])
        metadata2 = _build_metadata(bands=["band2", "band3"])

        cube1 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer1}), metadata=metadata1)
        cube2 = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer2}), metadata=metadata2)
        cube1.merge_cubes(cube2)


def test_apply_increase_resultion(imagecollection_with_two_bands_and_three_dates_large):
    input_cube = imagecollection_with_two_bands_and_three_dates_large

    file_name = get_test_data_file("udf_increase_resolution.py")
    with open(file_name, "r") as f:
        udf_code = f.read()
    udf = {
        "udf_process": {
            "process_id": "run_udf",
            "arguments": {
                "data": {
                    "from_parameter": "data"
                },
                "udf": udf_code,
                "runtime": "Python-Jep"
            },
            "result": True
        },
    }

    result = input_cube.apply(process = udf, context = {}, env = EvalEnv())

    input_metadata = imagecollection_with_two_bands_and_three_dates_large.get_max_level().layer_metadata
    result_level = result.get_max_level()
    result_metadata = result_level.layer_metadata

    # Check extents.
    input_extent: Extent = input_metadata.extent
    input_layout_definition: LayoutDefinition = input_metadata.layout_definition
    assert input_extent == result_metadata.extent
    assert input_layout_definition.extent == result_metadata.layout_definition.extent

    # Check tile_layout.
    input_tile_layout: TileLayout = input_metadata.tile_layout
    assert input_tile_layout.layoutCols * 2 == result_metadata.tile_layout.layoutCols
    assert input_tile_layout.layoutRows * 2 == result_metadata.tile_layout.layoutRows
    assert input_tile_layout.tileCols == result_metadata.tile_layout.tileCols
    assert input_tile_layout.tileRows == result_metadata.tile_layout.tileRows
    assert result_metadata.layout_definition.tileLayout == result_metadata.tile_layout

    # Check bounds.
    # They should change from (0,0)->(1,1) to (0,0)->(3,3) (4 to 16 multiband tiles per date)
    input_bounds: Bounds = input_metadata.bounds
    assert input_bounds.minKey.col == result_metadata.bounds.minKey.col
    assert input_bounds.minKey.row == result_metadata.bounds.minKey.row
    assert input_bounds.maxKey.col * 4 - 1 == result_metadata.bounds.maxKey.col  # (1,1) => (3,3)
    assert input_bounds.maxKey.row * 4 - 1 == result_metadata.bounds.maxKey.row
    assert input_bounds.minKey.instant == result_metadata.bounds.minKey.instant
    assert input_bounds.maxKey.instant == result_metadata.bounds.maxKey.instant

    result_numpy: List = result_level.to_numpy_rdd().collect()
    space_time_keys: List[SpaceTimeKey] = [x[0] for x in result_numpy]
    nr_dates = 3
    # Every tile transforms into 4 tiles because the resolution doubled. (128x128 => 256x256)
    assert len(space_time_keys) == 4 * nr_dates * input_tile_layout.layoutRows * input_tile_layout.layoutCols
    tiles: List[Tile] = [x[1] for x in result_numpy]
    # The tile sizes should not change when the resolution changes. Only the number of tiles changes.
    # This is important because we'll never receive an error when tile sizes are too large,
    # netCDFWriter just crops them.
    shapes = [x.cells.shape for x in tiles]
    for shape in shapes:
        assert shape == (2, input_tile_layout.tileRows, input_tile_layout.tileCols)

# TODO: test_apply_increase_resultion_and_change_extent(imagecollection_with_two_bands_and_three_dates_large)
