import contextlib
import logging
import textwrap

import numpy as np
import pytest
from numpy.testing import assert_equal

from openeo_driver.testing import TEST_USER
from openeogeotrellis.testing import random_name
from openeogeotrellis.utils import get_jvm, UtcNowClock

_log = logging.getLogger(__name__)


@contextlib.contextmanager
def set_jvm_system_properties(properties: dict):
    """Context manager to temporary set jvm System properties."""
    jvm_system = get_jvm().System
    orig_properties = {k: jvm_system.getProperty(k) for k in properties.keys()}

    def set_all(properties: dict):
        for k, v in properties.items():
            if v:
                jvm_system.setProperty(k, str(v))
            else:
                jvm_system.clearProperty(k)

    set_all(properties)
    yield
    set_all(orig_properties)


def test_execute_math_basic(api100):
    res = api100.check_result({"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}})
    assert res.json == 8


def test_load_collection_json_basic(api100):
    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-01-10"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                "bands": ["Flat:1", "TileRow", "Longitude", "Day"]
            },
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "lc"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == ["t", "bands", "x", "y"]
    data = result["data"]
    assert_equal(data, [[
        np.ones((4, 4)),
        np.zeros((4, 4)),
        [[0, 0, 0, 0], [0.25, 0.25, 0.25, 0.25], [0.5, 0.5, 0.5, 0.5], [0.75, 0.75, 0.75, 0.75]],
        np.full((4, 4), fill_value=5)
    ]])


def test_udp_simple_temporal_reduce(api100, user_defined_process_registry):
    """Test calling a UDP with simple temporal reduce operation"""
    udp_id = random_name("udp")
    udp_spec = {
        "id": udp_id,
        "parameters": [
            {"name": "data", "schema": {"type": "object", "subtype": "raster-cube"}}
        ],
        "process_graph": {
            "reduce": {
                "process_id": "reduce_dimension",
                "arguments": {
                    "data": {"from_parameter": "data"},
                    "dimension": "t",
                    "reducer": {"process_graph": {"max": {
                        "process_id": "max", "arguments": {"data": {"from_parameter": "data"}}, "result": True
                    }}}
                },
                "result": True
            }
        }
    }
    user_defined_process_registry.save(user_id=TEST_USER, process_id=udp_id, spec=udp_spec)

    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-02-01"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                "bands": ["Longitude", "Day"]
            },
        },
        "udp": {
            "process_id": udp_id, "arguments": {"data": {"from_node": "lc"}}
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "udp"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == ["bands", "x", "y"]
    data = result["data"]
    assert_equal(data, np.array([
        np.array([[0, .25, .5, .75]] * 4).T,
        np.full((4, 4), fill_value=25)
    ]))


@pytest.mark.parametrize("udf_code", [
    """
        from openeo_udf.api.datacube import DataCube  # Old style openeo_udf API
        def apply_datacube(cube: DataCube, context: dict) -> DataCube:
            return DataCube(cube.get_array().max("t"))
    """,
    """
        from openeo.udf import XarrayDataCube
        def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
            return XarrayDataCube(cube.get_array().max("t"))
    """,
])
def test_udp_udf_reduce_temporal(api100, user_defined_process_registry, udf_code):
    """Test calling a UDP with a UDF based reduce operation"""
    udf_code = textwrap.dedent(udf_code)
    udp_id = random_name("udp")
    udp_spec = {
        "id": udp_id,
        "parameters": [
            {"name": "data", "schema": {"type": "object", "subtype": "raster-cube"}},
        ],
        "process_graph": {
            "reduce": {
                "process_id": "reduce_dimension",
                "arguments": {
                    "data": {"from_parameter": "data"},
                    "dimension": "t",
                    "reducer": {"process_graph": {"udf": {
                        "process_id": "run_udf",
                        "arguments": {
                            "data": {"from_parameter": "data"},
                            "udf": udf_code,
                            "runtime": "Python",
                        },
                        "result": True
                    }}}
                },
                "result": True
            }
        }
    }
    user_defined_process_registry.save(user_id=TEST_USER, process_id=udp_id, spec=udp_spec)

    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-02-01"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Longitude", "Day"]
            },
        },
        "udp": {
            "process_id": udp_id, "arguments": {"data": {"from_node": "lc"}}
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "udp"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == ["bands", "x", "y"]
    data = result["data"]
    assert_equal(data, np.array([
        np.array([[0, .25, .5, .75]] * 8).T,
        np.full((4, 8), fill_value=25)
    ]))


@pytest.mark.parametrize("set_offset", [False, True])
@pytest.mark.parametrize("udf_code", [
    """
        from openeo_udf.api.datacube import DataCube  # Old style openeo_udf API
        def apply_datacube(cube: DataCube, context: dict) -> DataCube:
            offset = context.get("offset", 34)
            return DataCube(cube.get_array().max("t") + offset) 
    """,
    """
        from openeo.udf import XarrayDataCube
        def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
            offset = context.get("offset", 34)
            return XarrayDataCube(cube.get_array().max("t") + offset) 
    """,
])
def test_udp_udf_reduce_temporal_with_parameter(api100, user_defined_process_registry, set_offset, udf_code):
    """Test calling a UDP with a UDF based reduce operation and fetching a UDP parameter value (EP-3781)"""
    udf_code = textwrap.dedent(udf_code)
    udp_id = random_name("udp")
    udp_spec = {
        "id": udp_id,
        "parameters": [
            {"name": "data", "schema": {"type": "object", "subtype": "raster-cube"}},
            {"name": "offset", "default": 12, "optional": True, "schema": {"type": "number"}},
        ],
        "process_graph": {
            "reduce": {
                "process_id": "reduce_dimension",
                "arguments": {
                    "data": {"from_parameter": "data"},
                    "dimension": "t",
                    "reducer": {"process_graph": {"udf": {
                        "process_id": "run_udf",
                        "arguments": {
                            "data": {"from_parameter": "data"},
                            "udf": udf_code,
                            "runtime": "Python",
                            "context": {"offset": {"from_parameter": "offset"}}
                        },
                        "result": True
                    }}}
                },
                "result": True
            }
        }
    }
    user_defined_process_registry.save(user_id=TEST_USER, process_id=udp_id, spec=udp_spec)

    udp_args = {"data": {"from_node": "lc"}}
    if set_offset:
        udp_args["offset"] = 56
    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-02-01"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                "bands": ["Longitude", "Day"]
            },
        },
        "udp": {"process_id": udp_id, "arguments": udp_args},
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "udp"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == ["bands", "x", "y"]
    data = result["data"]
    expected_offset = 56 if set_offset else 12
    assert_equal(data, expected_offset + np.array([
        np.array([[0, .25, .5, .75]] * 4).T,
        np.full((4, 4), fill_value=25)
    ]))


@pytest.mark.parametrize("set_parameters", [False, True])
@pytest.mark.parametrize("udf_code", [
    """
        from openeo_udf.api.datacube import DataCube  # Old style openeo_udf API
        def apply_datacube(cube: DataCube, context: dict) -> DataCube:
            l_scale = context.get("l_scale", 100)
            d_scale = context.get("d_scale", 1)
            array = cube.get_array()
            res = l_scale * array.sel(bands="Longitude") + d_scale * array.sel(bands="Day") 
            return DataCube(res) 
    """,
    """
        from openeo.udf import XarrayDataCube
        def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
            l_scale = context.get("l_scale", 100)
            d_scale = context.get("d_scale", 1)
            array = cube.get_array()
            res = l_scale * array.sel(bands="Longitude") + d_scale * array.sel(bands="Day") 
            return XarrayDataCube(res) 
    """,
])
def test_udp_udf_reduce_bands_with_parameter(api100, user_defined_process_registry, set_parameters, udf_code):
    """Test calling a UDP with a UDF based reduce operation and fetching a UDP parameter value (EP-3781)"""
    udf_code = textwrap.dedent(udf_code)
    udp_id = random_name("udp")
    udp_spec = {
        "id": udp_id,
        "parameters": [
            {"name": "data", "schema": {"type": "object", "subtype": "raster-cube"}},
            {"name": "l_scale", "default": 1000, "optional": True, "schema": {"type": "number"}},
            {"name": "d_scale", "default": 2, "optional": True, "schema": {"type": "number"}},
        ],
        "process_graph": {
            "reduce": {
                "process_id": "reduce_dimension",
                "arguments": {
                    "data": {"from_parameter": "data"},
                    "dimension": "bands",
                    "reducer": {"process_graph": {"udf": {
                        "process_id": "run_udf",
                        "arguments": {
                            "data": {"from_parameter": "data"},
                            "udf": udf_code,
                            "runtime": "Python",
                            "context": {
                                "l_scale": {"from_parameter": "l_scale"},
                                "d_scale": {"from_parameter": "d_scale"}
                            }
                        },
                        "result": True
                    }}}
                },
                "result": True
            }
        }
    }
    user_defined_process_registry.save(user_id=TEST_USER, process_id=udp_id, spec=udp_spec)

    udp_args = {"data": {"from_node": "lc"}}
    if set_parameters:
        udp_args["l_scale"] = 100000
        udp_args["d_scale"] = 3

    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-02-01"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                "bands": ["Longitude", "Day"]
            },
        },
        "udp": {"process_id": udp_id, "arguments": udp_args},
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "udp"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == ["t", "x", "y"]
    data = result["data"]

    if set_parameters:
        expected = np.array([
            np.array([[15, 25015, 50015, 75015]] * 4).T,
            np.array([[45, 25045, 50045, 75045]] * 4).T,
            np.array([[75, 25075, 50075, 75075]] * 4).T,
        ])
    else:
        expected = np.array([
            np.array([[10, 10 + 250, 10 + 500, 10 + 750]] * 4).T,
            np.array([[30, 30 + 250, 30 + 500, 30 + 750]] * 4).T,
            np.array([[50, 50 + 250, 50 + 500, 50 + 750]] * 4).T,
        ])

    assert_equal(data, expected)


def test_apply_square_pixels(api100):
    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-02-01"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                "bands": ["Longitude", "Day"]
            },
        },
        "apply": {
            "process_id": "apply",
            "arguments": {
                "data": {"from_node": "lc"},
                "process": {"process_graph": {"udf": {
                    "process_id": "multiply",
                    "arguments": {"x": {"from_parameter": "x"}, "y": {"from_parameter": "x"}},
                    "result": True
                }}}
            }
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "apply"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == ["t", "bands", "x", "y"]
    data = result["data"]
    expected = np.array([
        [np.array([[0, 0.25 ** 2, 0.5 ** 2, 0.75 ** 2]] * 4).T, np.full((4, 4), fill_value=5 ** 2)],
        [np.array([[0, 0.25 ** 2, 0.5 ** 2, 0.75 ** 2]] * 4).T, np.full((4, 4), fill_value=15 ** 2)],
        [np.array([[0, 0.25 ** 2, 0.5 ** 2, 0.75 ** 2]] * 4).T, np.full((4, 4), fill_value=25 ** 2)],
    ])
    assert_equal(data, expected)


@pytest.mark.parametrize("udf_code", [
    """
        from openeo_udf.api.datacube import DataCube  # Old style openeo_udf API
        def apply_datacube(cube: DataCube, context: dict) -> DataCube:
            array = cube.get_array()
            return DataCube(array * array)  
    """,
    """
        from openeo.udf import XarrayDataCube
        def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
            array = cube.get_array()
            return XarrayDataCube(array * array)  
    """,
])
def test_apply_udf_square_pixels(api100, udf_code):
    udf_code = textwrap.dedent(udf_code)

    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-02-01"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                "bands": ["Longitude", "Day"]
            },
        },
        "apply": {
            "process_id": "apply",
            "arguments": {
                "data": {"from_node": "lc"},
                "process": {"process_graph": {"udf": {
                    "process_id": "run_udf",
                    "arguments": {
                        "data": {"from_parameter": "data"},
                        "udf": udf_code,
                        "runtime": "Python",
                    },
                    "result": True
                }}}
            }
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "apply"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == ["t", "bands", "x", "y"]
    data = result["data"]
    expected = np.array([
        [np.array([[0, 0.25 ** 2, 0.5 ** 2, 0.75 ** 2]] * 4).T, np.full((4, 4), fill_value=5 ** 2)],
        [np.array([[0, 0.25 ** 2, 0.5 ** 2, 0.75 ** 2]] * 4).T, np.full((4, 4), fill_value=15 ** 2)],
        [np.array([[0, 0.25 ** 2, 0.5 ** 2, 0.75 ** 2]] * 4).T, np.full((4, 4), fill_value=25 ** 2)],
    ])
    assert_equal(data, expected)


@pytest.mark.parametrize("set_parameters", [False, True])
@pytest.mark.parametrize("udf_code", [
    """
        from openeo_udf.api.datacube import DataCube  # Old style openeo_udf API
        def apply_datacube(cube: DataCube, context: dict) -> DataCube:
            offset = context.get("offset", 100)
            return DataCube(cube.get_array() + offset) 
    """,
    """
        from openeo.udf import XarrayDataCube
        def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
            offset = context.get("offset", 100)
            return XarrayDataCube(cube.get_array() + offset) 
    """,
])
def test_udp_udf_apply_neirghborhood_with_parameter(api100, user_defined_process_registry, set_parameters, udf_code):
    """Test calling a UDP with a UDF based reduce operation and fetching a UDP parameter value (EP-3781)"""
    udf_code = textwrap.dedent(udf_code)
    udp_id = random_name("udp")
    udp_spec = {
        "id": udp_id,
        "parameters": [
            {"name": "data", "schema": {"type": "object", "subtype": "raster-cube"}},
            {"name": "offset", "default": 10, "optional": True, "schema": {"type": "number"}},
        ],
        "process_graph": {
            "apply_neighborhood": {
                "process_id": "apply_neighborhood",
                "arguments": {
                    "data": {"from_parameter": "data"},
                    "process": {"process_graph": {"udf": {
                        "process_id": "run_udf",
                        "arguments": {
                            "data": {"from_parameter": "data"},
                            "udf": udf_code,
                            "runtime": "Python",
                            "context": {
                                "offset": {"from_parameter": "offset"},
                            }
                        },
                        "result": True
                    }}},
                    "size": [{'dimension': 'x', 'unit': 'px', 'value': 32},
                             {'dimension': 'y', 'unit': 'px', 'value': 32}],
                    "overlap": [{'dimension': 'x', 'unit': 'px', 'value': 8},
                                {'dimension': 'y', 'unit': 'px', 'value': 8}],
                },
                "result": True
            }
        }
    }
    user_defined_process_registry.save(user_id=TEST_USER, process_id=udp_id, spec=udp_spec)

    udp_args = {"data": {"from_node": "lc"}}
    if set_parameters:
        udp_args["offset"] = 20

    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-02-01"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                "bands": ["Longitude", "Day"]
            },
        },
        "udp": {"process_id": udp_id, "arguments": udp_args},
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "udp"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == ["t", "bands", "x", "y"]
    data = result["data"]

    expected = np.array([
        [[[.0] * 4, [.25] * 4, [.5] * 4, [.75] * 4], [[5] * 4] * 4],
        [[[.0] * 4, [.25] * 4, [.5] * 4, [.75] * 4], [[15] * 4] * 4],
        [[[.0] * 4, [.25] * 4, [.5] * 4, [.75] * 4], [[25] * 4] * 4],
    ]) + (20 if set_parameters else 10)

    assert_equal(data, expected)


@pytest.mark.parametrize("geometries", [
    {"type": "Polygon", "coordinates": [[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]},
    {"type": "MultiPolygon", "coordinates": [[[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]]},
    {
        "type": "GeometryCollection",
        "geometries": [{"type": "Polygon", "coordinates": [[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]}],
    },
    {
        "type": "Feature",
        "geometry": {"type": "Polygon", "coordinates": [[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]},
    },
    {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]},
        }]
    }
])
@pytest.mark.parametrize("pixels_threshold", [0, 10000])
@pytest.mark.parametrize("reducer", ["mean", "median"])
def test_ep3718_aggregate_spatial_geometries(api100, geometries, pixels_threshold, reducer):
    """EP-3718: different results when doing aggregate_spatial with Polygon or GeometryCollection"""

    with set_jvm_system_properties({"pixels.treshold": pixels_threshold}):
        response = api100.check_result({
            "lc": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "TestCollection-LonLat4x4",
                    "temporal_extent": ["2021-01-01", "2021-02-20"],
                    "spatial_extent": {"west": 0.0, "south": 0.0, "east": 2.0, "north": 2.0},
                    "bands": ["Flat:1", "Month", "Day"]
                },
            },
            "aggregate": {
                "process_id": "aggregate_spatial",
                "arguments": {
                    "data": {"from_node": "lc"},
                    "geometries": geometries,
                    "reducer": {"process_graph": {
                        reducer: {
                            "process_id": reducer, "arguments": {"data": {"from_parameter": "data"}}, "result": True
                        }
                    }}
                }
            },
            "save": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "aggregate"}, "format": "json"},
                "result": True,
            }
        })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    # Strip out empty entries
    result = {k: v for (k, v) in result.items() if v != [[]]}

    expected = {
        "2021-01-05T00:00:00Z": [[1.0, 1.0, 5.0]],
        "2021-01-15T00:00:00Z": [[1.0, 1.0, 15.0]],
        "2021-01-25T00:00:00Z": [[1.0, 1.0, 25.0]],
        "2021-02-05T00:00:00Z": [[1.0, 2.0, 5.0]],
        "2021-02-15T00:00:00Z": [[1.0, 2.0, 15.0]],
    }
    assert result == expected


@pytest.mark.parametrize("geometries", [
    {"type": "Polygon", "coordinates": [[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]},
    {"type": "MultiPolygon", "coordinates": [[[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]]},
    {
        "type": "GeometryCollection",
        "geometries": [{"type": "Polygon", "coordinates": [[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]}],
    },
    {
        "type": "Feature",
        "geometry": {"type": "Polygon", "coordinates": [[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]},
    },
    {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]},
        }]
    }
])
def test_ep3887_mask_polygon(api100, geometries):
    """EP-3887: mask_polygon with GeometryCollection/FeatureCollection gives empty result"""

    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-04", "2021-01-06"],
                "bands": ["Flat:2"]
            },
        },
        "maskpolygon1": {
            "process_id": "mask_polygon",
            "arguments": {
                "data": {"from_node": "lc"},
                "mask": geometries,
            }
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "maskpolygon1"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == ["t", "bands", "x", "y"]
    assert result["coords"]["x"]["data"] == [0.125, 0.375, 0.625, 0.875, 1.125, 1.375, 1.625, 1.875]
    assert result["coords"]["y"]["data"] == [0.125, 0.375, 0.625, 0.875, 1.125, 1.375, 1.625, 1.875]
    assert result["data"] == [[[
        [2, 0, 0, 0, 0, 0, 0, 0],
        [2, 2, 0, 0, 0, 0, 0, 0],
        [2, 2, 2, 2, 0, 0, 0, 0],
        [2, 2, 2, 2, 2, 2, 0, 0],
        [2, 2, 2, 2, 2, 2, 2, 0],
        [2, 2, 2, 2, 2, 0, 0, 0],
        [2, 2, 0, 0, 0, 0, 0, 0],
        [0, 0, 0, 0, 0, 0, 0, 0],
    ]]]


def test_apply_dimension_array_concat(api100):
    """EP-3775 apply_dimension with array_concat"""
    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-01-10"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                "bands": ["Flat:1", "TileRow", "Longitude", "Day"]
            },
        },
        "ad": {
            "process_id": "apply_dimension",
            "arguments": {
                "data": {"from_node": "lc"},
                "dimension": "bands",
                "process": {"process_graph": {
                    "one": {
                        "process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 0}
                    },
                    "lon": {
                        "process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 2}
                    },
                    "day": {
                        "process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 3}
                    },
                    "lon x day": {
                        "process_id": "multiply", "arguments": {"x": {"from_node": "lon"}, "y": {"from_node": "day"}}
                    },
                    "array_concat": {
                        "process_id": "array_concat",
                        "arguments": {
                            "array1": {"from_parameter": "data"},
                            "array2": [{"from_node": "one"}, {"from_node": "lon x day"}, ]
                        },
                        "result": True,
                    }
                }},
            },
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "ad"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == ["t", "bands", "x", "y"]
    data = result["data"]
    assert_equal(data, [[
        np.ones((4, 4)),
        np.zeros((4, 4)),
        [[0, 0, 0, 0], [0.25, 0.25, 0.25, 0.25], [0.5, 0.5, 0.5, 0.5], [0.75, 0.75, 0.75, 0.75]],
        np.full((4, 4), fill_value=5),
        np.ones((4, 4)),
        [[0, 0, 0, 0], [1.25, 1.25, 1.25, 1.25], [2.5, 2.5, 2.5, 2.5], [3.75, 3.75, 3.75, 3.75]],
    ]])

def test_apply_dimension_reduce_time(api100):
    """EP-3775 apply_dimension with array_concat"""
    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-01-30"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                "bands": ["Flat:1", "TileRow", "Longitude", "Day"]
            },
        },
        "ad": {
            "process_id": "apply_dimension",
            "arguments": {
                "data": {"from_node": "lc"},
                "dimension": "t",
                "target_dimension": "bands",
                "process": {"process_graph": {
                  "arrayconcat1": {
                    "arguments": {
                      "array1": {
                        "from_node": "quantiles1"
                      },
                      "array2": [
                        {
                          "from_node": "sd1"
                        },
                        {
                          "from_node": "mean1"
                        }
                      ]
                    },
                    "process_id": "array_concat"

                  },
                  "mean1": {
                    "arguments": {
                      "data": {
                        "from_parameter": "data"
                      }
                    },
                    "process_id": "mean"
                  },
                  "quantiles1": {
                    "arguments": {
                      "data": {
                        "from_parameter": "data"
                      },
                      "probabilities": [
                        0.25,
                        0.5,
                        0.75
                      ]
                    },

                    "process_id": "quantiles"
                  },
                  "sd1": {
                    "arguments": {
                      "data": {
                        "from_parameter": "data"
                      }
                    },
                      "result": True,
                    "process_id": "sd"
                  }
                }},
            },
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "ad"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == [ "bands", "x", "y"]
    data = result["data"]
    #TODO EP-3916: this result is probably wrong, maybe compute expected result with numpy, also user more complex callback graph, to test feature engineering
    assert_equal(data, [
        np.zeros((4, 4)),
        np.zeros((4, 4)),
        np.zeros((4, 4)),
        np.full((4, 4), fill_value=10.0),
    ])


@pytest.mark.parametrize("repeat", [1, 3])
def test_apply_dimension_array_create(api100, repeat):
    """EP-3775 apply_dimension with array_create"""
    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-01-10"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                "bands": ["Flat:1", "Day"]
            },
        },
        "ad": {
            "process_id": "apply_dimension",
            "arguments": {
                "data": {"from_node": "lc"},
                "dimension": "bands",
                "process": {"process_graph": {
                    "one": {
                        "process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 0}
                    },
                    "day": {
                        "process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 1}
                    },
                    "array_create": {
                        "process_id": "array_create",
                        "arguments": {
                            "data": [{"from_node": "one"}, {"from_node": "day"}, ],
                            "repeat": repeat
                        },
                        "result": True,
                    }
                }},
            },
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "ad"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == ["t", "bands", "x", "y"]
    data = result["data"]
    assert_equal(data, [[np.ones((4, 4)), np.full((4, 4), fill_value=5), ] * repeat])


def test_reduce_dimension_array_create_array_concat(api100):
    """EP-3775 reduce_dimension with array_create and array_concat"""
    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-01-10"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                "bands": ["Flat:1", "Flat:2"]
            },
        },
        "reduce": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "lc"},
                "dimension": "bands",
                "reducer": {"process_graph": {
                    "one": {
                        "process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 0}
                    },
                    "two": {
                        "process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 1}
                    },
                    "two ones": {
                        "process_id": "array_create",
                        "arguments": {"data": [{"from_node": "one"}, {"from_node": "one"}]}
                    },
                    "three twos": {
                        "process_id": "array_create",
                        "arguments": {"data": [{"from_node": "two"}, {"from_node": "two"}, {"from_node": "two"}]}
                    },
                    "array_concat": {
                        "process_id": "array_concat",
                        "arguments": {
                            "array1": {"from_node": "two ones"},
                            "array2": {"from_node": "three twos"},
                        },
                    },
                    "sum": {
                        "process_id": "sum",
                        "arguments": {"data": {"from_node": "array_concat"}},
                        "result": True
                    }
                }},
            },
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "reduce"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == ["t", "x", "y"]
    data = result["data"]
    assert_equal(data, np.full((1, 4, 4), fill_value=8))


@pytest.mark.parametrize("udf_code", [
    """
        from openeo_udf.api.datacube import DataCube  # Old style openeo_udf API
        def apply_datacube(cube: DataCube, context: dict) -> DataCube:
            return DataCube(cube.get_array().max("t"))
    """,
    """
        from openeo.udf import XarrayDataCube
        def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
            return XarrayDataCube(cube.get_array().max("t"))
    """,
    """
        from openeo.udf import XarrayDataCube
        def apply_hypercube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
            return XarrayDataCube(cube.get_array().max("t"))
    """,
])
def test_udf_basic_reduce_temporal(api100, user_defined_process_registry, udf_code):
    """Test doing UDF based temporal reduce"""
    udf_code = textwrap.dedent(udf_code)

    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-02-01"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Longitude", "Day"]
            },
        },
        "reduce": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "lc"},
                "dimension": "t",
                "reducer": {"process_graph": {"udf": {
                    "process_id": "run_udf",
                    "arguments": {
                        "data": {"from_parameter": "data"},
                        "udf": udf_code,
                        "runtime": "Python",
                    },
                    "result": True
                }}}
            },
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "reduce"}, "format": "json"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json
    _log.info(repr(result))

    assert result["dims"] == ["bands", "x", "y"]
    data = result["data"]
    assert_equal(data, np.array([
        np.array([[0, .25, .5, .75]] * 8).T,
        np.full((4, 8), fill_value=25)
    ]))


@pytest.mark.parametrize("udf_code", [
    """
        def hello(name: str):
            return f"hello {name}"
    """,
    """
        def apply_datacube(cube, context: dict):
            return DataCube(cube.get_array().max("t"))
    """,
    """
        def apply_hypercube(cube, context: dict):
            return DataCube(cube.get_array().max("t"))
    """,
    """
        from openeo.udf import XarrayDataCube
        def apply_datacube(cube: XarrayDataCube) -> XarrayDataCube:
            return XarrayDataCube(cube.get_array().max("t"))
    """
])
def test_udf_invalid_signature(api100, user_defined_process_registry, udf_code):
    """Test doing UDF with invalid signature: should raise error"""
    udf_code = textwrap.dedent(udf_code)

    response = api100.result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-02-01"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Longitude", "Day"]
            },
        },
        "reduce": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "lc"},
                "dimension": "t",
                "reducer": {"process_graph": {"udf": {
                    "process_id": "run_udf",
                    "arguments": {
                        "data": {"from_parameter": "data"},
                        "udf": udf_code,
                        "runtime": "Python",
                    },
                    "result": True
                }}}
            },
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "reduce"}, "format": "json"},
            "result": True,
        }
    })

    # TODO: improve status code, error code and message
    response.assert_error(status_code=500, error_code=None, message="No UDF found")


def test_extra_validation_creo(api100, requests_mock):
    pg = {"lc": {
        "process_id": "load_collection",
        "arguments": {
            "id": "SENTINEL2_L2A_CREO",
            "temporal_extent": ["2020-03-01", "2020-03-10"],
            "spatial_extent": {"west": -87, "south": 67, "east": -86, "north": 68},
            "properties": {"eo:cloud_cover": {"process_graph": {"lte1": {"process_id": "lte", "arguments": {"x": {"from_parameter": "value"}, "y": 50}, "result": True}}}}
        },
        "result": True
    }}

    requests_mock.get(
        "https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json?productType=L2A&startDate=2020-03-01T00%3A00%3A00&cloudCover=%5B0%2C50%5D&page=1&maxRecords=100&sortParam=startDate&sortOrder=ascending&status=all&dataset=ESA-DATASET&completionDate=2020-03-10T23%3A59%3A59.999999&geometry=POLYGON+%28%28-87+68%2C+-86+68%2C+-86+67%2C+-87+67%2C+-87+68%29%29",
        json={
            "type": "FeatureCollection",
            "properties": {"totalResults": 28, "itemsPerPage": 28},
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "status": 0,
                        "productIdentifier": "/eodata/Sentinel-2/MSI/L2A/2020/03/01/S2A_MSIL2A_20200301T173231_N0214_R055_T16WEV_20200301T220803.SAFE",
                        "title": "S2A_MSIL2A_20200301T173231_N0214_R055_T16WEV_20200301T220803.SAFE",
                    }
                },
                {
                    "type": "Feature",
                    "properties": {
                        "status": 31,
                        "productIdentifier": "/eodata/Sentinel-2/MSI/L2A/2020/03/01/S2A_MSIL2A_20200301T173231_N0214_R055_T16WDA_20200301T220803.SAFE",
                        "title": "S2A_MSIL2A_20200301T173231_N0214_R055_T16WDA_20200301T220803.SAFE",
                    }
                },
            ],
        }
    )
    requests_mock.get(
        "https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json?productType=L2A&startDate=2020-03-01T00%3A00%3A00&cloudCover=%5B0%2C50%5D&page=2&maxRecords=100&sortParam=startDate&sortOrder=ascending&status=all&dataset=ESA-DATASET&completionDate=2020-03-10T23%3A59%3A59.999999&geometry=POLYGON+%28%28-87+68%2C+-86+68%2C+-86+67%2C+-87+67%2C+-87+68%29%29",
        json={
            "type": "FeatureCollection",
            "properties": {"totalResults": 28, "itemsPerPage": 28},
            "features": [],
        }
    )

    response = api100.validation(pg)
    assert response.json == {'errors': [
        {'code': 'MissingProduct', 'message': "Tile 'S2A_MSIL2A_20200301T173231_N0214_R055_T16WDA_20200301T220803' in collection 'SENTINEL2_L2A_CREO' is not available."}
    ]}


def test_extra_validation_terrascope(api100, requests_mock):
    pg = {"lc": {
        "process_id": "load_collection",
        "arguments": {
            "id": "TERRASCOPE_S2_TOC_V2",
            "temporal_extent": ["2020-03-01", "2020-03-10"],
            "spatial_extent": {"west": -87, "south": 67, "east": -86, "north": 68},
            "properties": {"eo:cloud_cover": {"process_graph": {
                "lte1": {"process_id": "lte", "arguments": {"x": {"from_parameter": "value"}, "y": 50},
                         "result": True}}}}
        },
        "result": True
    }}

    requests_mock.get(
        "https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json?processingLevel=LEVEL1C&startDate=2020-03-01T00%3A00%3A00&cloudCover=%5B0%2C50%5D&page=1&maxRecords=100&sortParam=startDate&sortOrder=ascending&status=all&dataset=ESA-DATASET&completionDate=2020-03-10T23%3A59%3A59.999999&geometry=POLYGON+%28%28-87+68%2C+-86+68%2C+-86+67%2C+-87+67%2C+-87+68%29%29",
        json={
            "type": "FeatureCollection",
            "properties": {"totalResults": 28, "itemsPerPage": 28},
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "status": 0,
                        "productIdentifier": "/eodata/Sentinel-2/MSI/L1C/2020/03/01/S2A_MSIL1C_20200301T173231_N0209_R055_T16WEA_20200301T210331.SAFE",
                        "title": "S2A_MSIL1C_20200301T173231_N0209_R055_T16WEA_20200301T210331",
                    }
                },
                {
                    "type": "Feature",
                    "properties": {
                        "status": 0,
                        "productIdentifier": "/eodata/Sentinel-2/MSI/L1C/2020/03/01/S2A_MSIL1C_20200301T173231_N0209_R055_T16WDA_20200301T210331.SAFE",
                        "title": "S2A_MSIL1C_20200301T1732…DA_20200301T210331.SAFE",
                    }
                },
            ],
        }
    )
    requests_mock.get(
        "https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json?processingLevel=LEVEL1C&startDate=2020-03-01T00%3A00%3A00&cloudCover=%5B0%2C50%5D&page=2&maxRecords=100&sortParam=startDate&sortOrder=ascending&status=all&dataset=ESA-DATASET&completionDate=2020-03-10T23%3A59%3A59.999999&geometry=POLYGON+%28%28-87+68%2C+-86+68%2C+-86+67%2C+-87+67%2C+-87+68%29%29",
        json={
            "type": "FeatureCollection",
            "properties": {"totalResults": 28, "itemsPerPage": 28},
            "features": [],
        }
    )
    requests_mock.get(
        "https://services.terrascope.be/catalogue/products?collection=urn%3Aeop%3AVITO%3ATERRASCOPE_S2_TOC_V2&bbox=-87%2C67%2C-86%2C68&sortKeys=title&startIndex=1&start=2020-03-01T00%3A00%3A00&end=2020-03-10T23%3A59%3A59.999999&cloudCover=[0,50]",
        json={
            "type": "FeatureCollection",
            "properties": {},
            "features": [
                {"type": "Feature", "properties": {
                    "title": "S2A_20200301T173231_16WEA_TOC_V200",
                    "identifier": "urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2A_20200301T173231_16WEA_TOC_V200",
                }}
            ]
        }
    )
    requests_mock.get(
        "https://services.terrascope.be/catalogue/products?collection=urn%3Aeop%3AVITO%3ATERRASCOPE_S2_TOC_V2&bbox=-87%2C67%2C-86%2C68&sortKeys=title&startIndex=2&start=2020-03-01T00%3A00%3A00&end=2020-03-10T23%3A59%3A59.999999&cloudCover=[0,50]",
        json={
            "type": "FeatureCollection",
            "properties": {},
            "features": []
        }
    )

    response = api100.validation(pg)
    assert response.json == {'errors': [
        {'code': 'MissingProduct', 'message': "Tile '16WDA' in collection 'TERRASCOPE_S2_TOC_V2' is not available."}
    ]}


@pytest.mark.parametrize(["lc_args", "expected"], [
    (
            {"id": "TERRASCOPE_S2_TOC_V2"},
            ["No temporal extent given.", "No spatial extent given."]
    ),
    (
            {"id": "TERRASCOPE_S2_TOC_V2", "temporal_extent": ["2020-03-01", "2020-03-10"]},
            ["No spatial extent given."]
    ),
    (
            {"id": "TERRASCOPE_S2_TOC_V2", "spatial_extent": {"west": -87, "south": 67, "east": -86, "north": 68}},
            ["No temporal extent given."]
    ),
])
def test_extra_validation_unlimited_extent(api100, lc_args, expected):
    pg = {"lc": {"process_id": "load_collection", "arguments": lc_args, "result": True}}
    response = api100.validation(pg)
    assert response.json == {'errors': [{'code': 'UnlimitedExtent', 'message': m} for m in expected]}


@pytest.mark.parametrize(["temporal_extent", "expected"], [
    (("2020-01-01", None), ("2020-01-05 00:00:00", "2020-02-15 00:00:00")),
    ((None, "2019-01-10"), ("2000-01-05 00:00:00", "2019-01-05 00:00:00")),
])
def test_load_collection_open_temporal_extent(api100, temporal_extent, expected):
    with UtcNowClock.mock(now="2020-02-20"):
        response = api100.check_result({
            "lc": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "TestCollection-LonLat4x4",
                    "temporal_extent": temporal_extent,
                    "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                    "bands": ["Day"]
                },
            },
            "save": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "lc"}, "format": "json"},
                "result": True,
            }
        })

    result = response.assert_status_code(200).json
    # _log.info(repr(result))
    assert result["dims"] == ["t", "bands", "x", "y"]
    dates = result["coords"]["t"]["data"]
    assert (min(dates), max(dates)) == expected
