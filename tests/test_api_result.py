import contextlib
import datetime as dt
import json
import logging
import os
import shutil
import textwrap
import urllib.parse
import urllib.request
from pathlib import Path
from typing import List, Sequence, Union

import mock
import numpy as np
import openeo
import openeo.processes
import pytest
import rasterio
import xarray
from mock import MagicMock
from numpy.testing import assert_equal
from openeo_driver.backend import UserDefinedProcesses
from openeo_driver.jobregistry import JOB_STATUS
from openeo_driver.testing import (
    TEST_USER,
    ApiResponse,
    ApiTester,
    DictSubSet,
    IgnoreOrder,
    ListSubSet,
    RegexMatcher,
    UrllibMocker,
    load_json,
)
from openeo_driver.util.auth import ClientCredentials
from openeo_driver.util.geometry import as_geojson_feature, as_geojson_feature_collection
from pystac import Asset, Catalog, Collection, Extent, Item, SpatialExtent, TemporalExtent
from shapely.geometry import GeometryCollection, Point, Polygon, box, mapping

from openeogeotrellis.backend import JOB_METADATA_FILENAME
from openeogeotrellis.job_registry import ZkJobRegistry
from openeogeotrellis.testing import KazooClientMock, config_overrides, random_name
from openeogeotrellis.utils import UtcNowClock, drop_empty_from_aggregate_polygon_result, get_jvm, is_package_available

from .data import get_test_data_file

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
    try:
        yield
    finally:
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


def _load_xarray_dataset_from_netcdf_response(response: ApiResponse, tmp_path) -> xarray.Dataset:
    """Load NetCDF API response as xarray Dataset"""
    response.assert_status_code(200)
    assert response.headers["Content-Type"] == "application/x-netcdf"
    path = tmp_path / f"netcd_response_{id(response)}.nc"
    _log.info(f"Saving NetCDF response to and loading from: {path}")
    with path.open(mode="wb") as f:
        f.write(response.data)
    return xarray.load_dataset(path)


def test_load_collection_netcdf_basic(api100, tmp_path):
    response = api100.check_result(
        {
            "lc": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "TestCollection-LonLat4x4",
                    "temporal_extent": ["2021-01-01", "2021-01-10"],
                    "spatial_extent": {
                        "west": 0.0,
                        "south": 0.0,
                        "east": 1.0,
                        "north": 1.0,
                    },
                    "bands": ["Flat:1", "TileRow", "Longitude", "Latitude", "Day"],
                },
            },
            "save": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "lc"}, "format": "netcdf"},
                "result": True,
            },
        }
    )
    ds = _load_xarray_dataset_from_netcdf_response(response, tmp_path=tmp_path)

    assert ds.sizes == {"t": 1, "x": 4, "y": 4}
    assert_equal(ds.coords["t"].values, [np.datetime64("2021-01-05")])
    assert_equal(ds.coords["x"].values, [0.125, 0.375, 0.625, 0.875])
    assert_equal(ds.coords["y"].values, [0.875, 0.625, 0.375, 0.125])
    assert_equal(ds["Flat:1"].values, np.ones((1, 4, 4)))
    assert_equal(ds["TileRow"].values, np.zeros((1, 4, 4)))
    assert_equal(
        ds["Longitude"].values,
        [[[0.00, 0.25, 0.50, 0.75]] * 4],
    )
    assert_equal(
        ds["Latitude"].values,
        [
            [
                [0.75, 0.75, 0.75, 0.75],
                [0.5, 0.5, 0.5, 0.5],
                [0.25, 0.25, 0.25, 0.25],
                [0.0, 0.0, 0.0, 0.0],
            ]
        ],
    )
    assert_equal(ds["Day"].values, np.full((1, 4, 4), fill_value=5))


def test_load_collection_netcdf_extent(api100, tmp_path):
    response = api100.check_result(
        {
            "lc": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "TestCollection-LonLat4x4",
                    "temporal_extent": ["2021-01-01", "2021-01-10"],
                    "spatial_extent": {
                        "west": 0.5,
                        "south": 1.2,
                        "east": 3.3,
                        "north": 2.7,
                    },
                    "bands": ["Flat:1", "TileRow", "Longitude", "Latitude", "Day"],
                },
            },
            "save": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "lc"}, "format": "netcdf"},
                "result": True,
            },
        }
    )
    ds = _load_xarray_dataset_from_netcdf_response(response, tmp_path=tmp_path)

    assert ds.sizes == {"t": 1, "x": 12, "y": 7}
    assert_equal(ds.coords["t"].values, [np.datetime64("2021-01-05")])
    assert_equal(
        ds.coords["x"].values,
        [0.625, 0.875]
        + [1.125, 1.375, 1.625, 1.875]
        + [2.125, 2.375, 2.625, 2.875]
        + [3.125, 3.375],
    )
    assert_equal(
        ds.coords["y"].values, [2.625, 2.375, 2.125, 1.875, 1.625, 1.375, 1.125]
    )
    assert_equal(ds["Flat:1"].values, np.ones((1, 7, 12)))
    assert_equal(
        ds["TileRow"].values,
        [
            [[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]] * 3
            + [[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]] * 4
        ],
    )
    assert_equal(
        ds["Longitude"].values,
        [[[0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.25]] * 7],
    )
    assert_equal(
        ds["Latitude"].values,
        [
            [
                [2.5] * 12,
                [2.25] * 12,
                [2.0] * 12,
                [1.75] * 12,
                [1.5] * 12,
                [1.25] * 12,
                [1.0] * 12,
            ]
        ],
    )
    assert_equal(ds["Day"].values, np.full((1, 7, 12), fill_value=5))


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
@pytest.mark.parametrize(
    "udf_code",
    [
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
@pytest.mark.parametrize(
    "udf_code",
    [
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


@pytest.mark.parametrize(
    "udf_code",
    [
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
    expected = np.array(
        [
            [np.array([[0, 0.25**2, 0.5**2, 0.75**2]] * 4).T, np.full((4, 4), fill_value=5**2)],
            [np.array([[0, 0.25**2, 0.5**2, 0.75**2]] * 4).T, np.full((4, 4), fill_value=15**2)],
            [np.array([[0, 0.25**2, 0.5**2, 0.75**2]] * 4).T, np.full((4, 4), fill_value=25**2)],
        ]
    )
    assert_equal(data, expected)


class TestApplyRunUDFWithContext:
    """
    Tests for handling contexts when doing apply/apply_dimension/reduce_dimension
    with a UDF that expects configuration from context
    """

    UDF_APPLY_MULTIPLY_FACTOR = textwrap.dedent(
        """
        from openeo.udf import XarrayDataCube
        def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
            factor = context["factor"]
            array = cube.get_array()
            return XarrayDataCube(factor * array)
        """
    )

    UDF_REDUCE_SUM_AXIS0 = textwrap.dedent(
        """
        from openeo.udf import XarrayDataCube
        def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
            factor = context["factor"]
            array = cube.get_array().sum(axis=0)
            return XarrayDataCube(factor * array)
        """
    )

    LONGITUDE4x4 = np.array([[0, 0.25, 0.5, 0.75]] * 4).T

    def _build_process_graph(self, apply_something: dict) -> dict:
        """
        Simple helper to build a process graph by squeezing
        an apply/apply_dimension between load_collection and save_result
        """
        pg = {
            "lc": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "TestCollection-LonLat4x4",
                    "temporal_extent": ["2021-01-01", "2021-02-01"],
                    "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                    "bands": ["Longitude"],
                },
            },
            "apply_something": apply_something,
            "save": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "apply_something"}, "format": "json"},
                "result": True,
            },
        }
        _log.info(f"{self=}._build_process_graph -> {pg=}")
        return pg

    def _skip_when_no_jep_and(self, condition: bool = True):
        """Helper to skip tests that require jep package to be installed."""
        if condition and not is_package_available("jep"):
            pytest.skip(reason="No 'jep' package available.")

    @pytest.mark.parametrize(
        ["parent", "extra_args"],
        [
            ("apply", {}),
            ("apply_dimension", {"dimension": "t"}),
            ("apply_dimension", {"dimension": "bands"}),
            # ("apply_dimension", {"dimension": "x"}),  # TODO?
            (
                "apply_neighborhood",
                {
                    "size": [
                        {"dimension": "x", "unit": "px", "value": 32},
                        {"dimension": "y", "unit": "px", "value": 32},
                    ],
                    "overlap": [
                        {"dimension": "x", "unit": "px", "value": 8},
                        {"dimension": "y", "unit": "px", "value": 8},
                    ],
                },
            ),
            (
                # TODO: replace `chunk_polygon` with `apply_polygon`
                "chunk_polygon",
                {"chunks": {"type": "Polygon", "coordinates": [[[0, 0], [2, 0], [2, 3], [0, 3], [0, 0]]]}},
            ),
        ],
    )
    def test_apply_run_udf_with_direct_context(self, api100, parent, extra_args):
        """context directly defined in `run_udf` node."""
        self._skip_when_no_jep_and(parent == "chunk_polygon")

        pg = self._build_process_graph(
            {
                "process_id": parent,
                "arguments": {
                    **{
                        "data": {"from_node": "lc"},
                        "process": {
                            "process_graph": {
                                "udf": {
                                    "process_id": "run_udf",
                                    "arguments": {
                                        "data": {"from_parameter": "data"},
                                        "udf": self.UDF_APPLY_MULTIPLY_FACTOR,
                                        "runtime": "Python",
                                        "context": {"factor": 123},
                                    },
                                    "result": True,
                                }
                            }
                        },
                    },
                    **extra_args,
                },
            }
        )

        response = api100.check_result(pg)
        result = response.assert_status_code(200).json
        _log.info(repr(result))

        assert result["dims"] == ["t", "bands", "x", "y"]
        expected = 123 * np.array([[self.LONGITUDE4x4]] * 3)
        assert_equal(result["data"], expected)

    @pytest.mark.parametrize(
        ["parent", "extra_args"],
        [
            ("apply", {}),
            ("apply_dimension", {"dimension": "t"}),
            ("apply_dimension", {"dimension": "bands"}),
            # ("apply_dimension", {"dimension": "x"}),  # TODO?
            (
                "apply_neighborhood",
                {
                    "size": [
                        {"dimension": "x", "unit": "px", "value": 32},
                        {"dimension": "y", "unit": "px", "value": 32},
                    ],
                    "overlap": [
                        {"dimension": "x", "unit": "px", "value": 8},
                        {"dimension": "y", "unit": "px", "value": 8},
                    ],
                },
            ),
            (
                # TODO: replace `chunk_polygon` with `apply_polygon`
                "chunk_polygon",
                {"chunks": {"type": "Polygon", "coordinates": [[[0, 0], [2, 0], [2, 3], [0, 3], [0, 0]]]}},
            ),
        ],
    )
    def test_apply_run_udf_with_apply_context(self, api100, parent, extra_args):
        """Context defined by parent `apply`"""
        self._skip_when_no_jep_and(parent == "chunk_polygon")

        pg = self._build_process_graph(
            {
                "process_id": parent,
                "arguments": {
                    **{
                        "data": {"from_node": "lc"},
                        "process": {
                            "process_graph": {
                                "udf": {
                                    "process_id": "run_udf",
                                    "arguments": {
                                        "data": {"from_parameter": "data"},
                                        "udf": self.UDF_APPLY_MULTIPLY_FACTOR,
                                        "runtime": "Python",
                                        "context": {"from_parameter": "context"},
                                    },
                                    "result": True,
                                }
                            }
                        },
                        "context": {"factor": 123},
                    },
                    **extra_args,
                },
            }
        )

        response = api100.check_result(pg)
        result = response.assert_status_code(200).json
        _log.info(repr(result))

        assert result["dims"] == ["t", "bands", "x", "y"]
        expected = 123 * np.array([[self.LONGITUDE4x4]] * 3)
        assert_equal(result["data"], expected)

    @pytest.mark.parametrize("dimension", ["t", "bands"])
    def test_reduce_dimension_run_udf_with_direct_context(self, api100, dimension):
        """context directly defined in `run_udf` node."""
        pg = self._build_process_graph(
            {
                "process_id": "reduce_dimension",
                "arguments": {
                    "data": {"from_node": "lc"},
                    "dimension": dimension,
                    "reducer": {
                        "process_graph": {
                            "udf": {
                                "process_id": "run_udf",
                                "arguments": {
                                    "data": {"from_parameter": "data"},
                                    "udf": self.UDF_REDUCE_SUM_AXIS0,
                                    "runtime": "Python",
                                    "context": {"factor": 123},
                                },
                                "result": True,
                            }
                        }
                    },
                },
            }
        )

        response = api100.check_result(pg)
        result = response.assert_status_code(200).json
        _log.info(repr(result))

        if dimension == "t":
            assert result["dims"] == ["bands", "x", "y"]
            expected = 3 * 123 * np.array([self.LONGITUDE4x4])
        elif dimension == "bands":
            assert result["dims"] == ["t", "x", "y"]
            expected = 123 * np.array([self.LONGITUDE4x4] * 3)
        else:
            raise ValueError(dimension)

        assert_equal(result["data"], expected)

    def _register_udp(
        self, *, user_defined_process_registry: UserDefinedProcesses, process_graph: dict, parameters: List[dict]
    ) -> str:
        udp_id = random_name("udp")
        udp_spec = {
            "id": udp_id,
            "parameters": parameters,
            "process_graph": process_graph,
        }
        _log.info(f"{self=}._register_udp -> {udp_spec=}")
        user_defined_process_registry.save(user_id=TEST_USER, process_id=udp_id, spec=udp_spec)
        return udp_id

    @pytest.mark.parametrize(
        ["parent", "extra_parent_args"],
        [
            ("apply", {}),
            ("apply_dimension", {"dimension": "t"}),
            ("apply_dimension", {"dimension": "bands"}),
            # ("apply_dimension", {"dimension": "x"}),  # TODO?
            (
                "apply_neighborhood",
                {
                    "size": [
                        {"dimension": "x", "unit": "px", "value": 32},
                        {"dimension": "y", "unit": "px", "value": 32},
                    ],
                    "overlap": [
                        {"dimension": "x", "unit": "px", "value": 8},
                        {"dimension": "y", "unit": "px", "value": 8},
                    ],
                },
            ),
            (
                # TODO: replace `chunk_polygon` with `apply_polygon`
                "chunk_polygon",
                {"chunks": {"type": "Polygon", "coordinates": [[[0, 0], [2, 0], [2, 3], [0, 3], [0, 0]]]}},
            ),
        ],
    )
    @pytest.mark.parametrize(
        ["udp_parameter", "apply_context", "run_udf_context", "udp_arguments", "expected_factor"],
        [
            (
                {"name": "factor", "default": 1000, "optional": True, "schema": {"type": "number"}},
                {"factor": {"from_parameter": "factor"}},
                {"from_parameter": "context"},
                {"factor": 123},
                123,
            ),
            (
                {"name": "factor", "default": 1000, "optional": True, "schema": {"type": "number"}},
                {"factor": {"from_parameter": "factor"}},
                {"from_parameter": "context"},
                {},
                1000,
            ),
            (
                {"name": "multiplier", "default": 1000, "optional": True, "schema": {"type": "number"}},
                {"factor": {"from_parameter": "multiplier"}},
                {"from_parameter": "context"},
                {"multiplier": 123},
                123,
            ),
            (
                {"name": "factor", "default": 1000, "optional": True, "schema": {"type": "number"}},
                None,
                {"factor": {"from_parameter": "factor"}},
                {"factor": 123},
                123,
            ),
            (
                {"name": "multiplier", "default": 1000, "optional": True, "schema": {"type": "number"}},
                None,
                {"factor": {"from_parameter": "multiplier"}},
                {"multiplier": 123},
                123,
            ),
            (
                {"name": "multiplier", "default": 1000, "optional": True, "schema": {"type": "number"}},
                None,
                {"factor": {"from_parameter": "multiplier"}},
                {},
                1000,
            ),
        ],
    )
    def test_udp_parameter_to_apply_run_udf_context(
        self,
        api100,
        user_defined_process_registry,
        parent,
        extra_parent_args,
        udp_parameter,
        apply_context,
        run_udf_context,
        udp_arguments,
        expected_factor,
    ):
        self._skip_when_no_jep_and(parent == "chunk_polygon")

        udp_id = self._register_udp(
            user_defined_process_registry=user_defined_process_registry,
            parameters=[udp_parameter],
            process_graph={
                "apply": {
                    "process_id": parent,
                    "arguments": {
                        **{
                            "data": {"from_parameter": "data"},
                            "process": {
                                "process_graph": {
                                    "udf": {
                                        "process_id": "run_udf",
                                        "arguments": {
                                            "data": {"from_parameter": "data"},
                                            "udf": self.UDF_APPLY_MULTIPLY_FACTOR,
                                            "runtime": "Python",
                                            "context": run_udf_context,
                                        },
                                        "result": True,
                                    }
                                }
                            },
                            "context": apply_context,
                        },
                        **extra_parent_args,
                    },
                    "result": True,
                },
            },
        )

        response = api100.check_result(
            process_graph=self._build_process_graph(
                apply_something={
                    "process_id": udp_id,
                    "arguments": {**{"data": {"from_node": "lc"}}, **udp_arguments},
                }
            )
        )
        result = response.assert_status_code(200).json
        _log.info(repr(result))

        assert result["dims"] == ["t", "bands", "x", "y"]
        expected = expected_factor * np.array([[self.LONGITUDE4x4]] * 3)
        assert_equal(result["data"], expected)

    @pytest.mark.parametrize(
        "dimension",
        ["t", "bands"],
    )
    @pytest.mark.parametrize(
        ["udp_parameter", "apply_context", "run_udf_context", "udp_arguments", "expected_factor"],
        [
            (
                {"name": "factor", "default": 1000, "optional": True, "schema": {"type": "number"}},
                {"factor": {"from_parameter": "factor"}},
                {"from_parameter": "context"},
                {"factor": 123},
                123,
            ),
            (
                {"name": "factor", "default": 1000, "optional": True, "schema": {"type": "number"}},
                {"factor": {"from_parameter": "factor"}},
                {"from_parameter": "context"},
                {},
                1000,
            ),
            (
                {"name": "multiplier", "default": 1000, "optional": True, "schema": {"type": "number"}},
                {"factor": {"from_parameter": "multiplier"}},
                {"from_parameter": "context"},
                {"multiplier": 123},
                123,
            ),
            (
                {"name": "factor", "default": 1000, "optional": True, "schema": {"type": "number"}},
                None,
                {"factor": {"from_parameter": "factor"}},
                {"factor": 123},
                123,
            ),
            (
                {"name": "multiplier", "default": 1000, "optional": True, "schema": {"type": "number"}},
                None,
                {"factor": {"from_parameter": "multiplier"}},
                {"multiplier": 123},
                123,
            ),
            (
                {"name": "multiplier", "default": 1000, "optional": True, "schema": {"type": "number"}},
                None,
                {"factor": {"from_parameter": "multiplier"}},
                {},
                1000,
            ),
        ],
    )
    def test_udp_parameter_to_reduce_dimension_run_udf_context(
        self,
        api100,
        user_defined_process_registry,
        dimension,
        udp_parameter,
        apply_context,
        run_udf_context,
        udp_arguments,
        expected_factor,
    ):
        udp_id = self._register_udp(
            user_defined_process_registry=user_defined_process_registry,
            parameters=[udp_parameter],
            process_graph={
                "apply": {
                    "process_id": "reduce_dimension",
                    "arguments": {
                        "data": {"from_parameter": "data"},
                        "dimension": dimension,
                        "reducer": {
                            "process_graph": {
                                "udf": {
                                    "process_id": "run_udf",
                                    "arguments": {
                                        "data": {"from_parameter": "data"},
                                        "udf": self.UDF_REDUCE_SUM_AXIS0,
                                        "runtime": "Python",
                                        "context": run_udf_context,
                                    },
                                    "result": True,
                                }
                            }
                        },
                        "context": apply_context,
                    },
                    "result": True,
                },
            },
        )

        response = api100.check_result(
            process_graph=self._build_process_graph(
                apply_something={
                    "process_id": udp_id,
                    "arguments": {**{"data": {"from_node": "lc"}}, **udp_arguments},
                }
            )
        )
        result = response.assert_status_code(200).json
        _log.info(repr(result))

        if dimension == "t":
            assert result["dims"] == ["bands", "x", "y"]
            expected = 3 * expected_factor * np.array([self.LONGITUDE4x4])
        elif dimension == "bands":
            assert result["dims"] == ["t", "x", "y"]
            expected = expected_factor * np.array([self.LONGITUDE4x4] * 3)
        else:
            raise ValueError(dimension)

        assert_equal(result["data"], expected)

    def test_usecase_279_1(self, api100):
        """Example 1 from https://github.com/openEOPlatform/architecture-docs/issues/279#issuecomment-1302725540"""
        udf = textwrap.dedent(
            """
            from openeo.udf import XarrayDataCube
            def apply_datacube(cube: XarrayDataCube, context) -> XarrayDataCube:
                array = cube.get_array()
                array.values = context['length'] * array.values
                return cube
        """
        )
        pg = self._build_process_graph(
            apply_something={
                "process_id": "apply",
                "arguments": {
                    "data": {"from_node": "lc"},
                    "process": {
                        "process_graph": {
                            "run1": {
                                "process_id": "run_udf",
                                "arguments": {
                                    "data": {"from_parameter": "data"},
                                    "runtime": "Python",
                                    "udf": udf,
                                    "context": {"length": {"from_parameter": "context"}},
                                },
                                "result": True,
                            }
                        }
                    },
                    "context": -123,
                },
            }
        )
        response = api100.check_result(pg)
        result = response.assert_status_code(200).json
        _log.info(repr(result))

        assert result["dims"] == ["t", "bands", "x", "y"]
        expected = -123 * np.array([[self.LONGITUDE4x4]] * 3)
        assert_equal(result["data"], expected)

    def test_usecase_279_2(self, api100):
        """Example 2 from https://github.com/openEOPlatform/architecture-docs/issues/279#issuecomment-1302725540"""
        udf = textwrap.dedent(
            """
            from openeo.udf import XarrayDataCube
            def apply_datacube(cube: XarrayDataCube, context) -> XarrayDataCube:
                array = cube.get_array()
                array.values = context * array.values
                return cube
        """
        )
        pg = self._build_process_graph(
            apply_something={
                "process_id": "apply",
                "arguments": {
                    "data": {"from_node": "lc"},
                    "process": {
                        "process_graph": {
                            "run1": {
                                "process_id": "run_udf",
                                "arguments": {
                                    "data": {"from_parameter": "data"},
                                    "runtime": "Python",
                                    "udf": udf,
                                    "context": {"from_parameter": "context"},
                                },
                                "result": True,
                            }
                        }
                    },
                    "context": -123,
                },
            }
        )
        response = api100.check_result(pg)
        result = response.assert_status_code(200).json
        _log.info(repr(result))

        assert result["dims"] == ["t", "bands", "x", "y"]
        expected = -123 * np.array([[self.LONGITUDE4x4]] * 3)
        assert_equal(result["data"], expected)


@pytest.mark.parametrize("set_parameters", [False, True])
@pytest.mark.parametrize(
    "udf_code",
    [
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
    ],
)
def test_udp_udf_apply_neighborhood_with_parameter(api100, user_defined_process_registry, set_parameters, udf_code):
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
        "properties":{},
        "geometry": {"type": "Polygon", "coordinates": [[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]},
    },
    {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "properties":{},
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
    result = drop_empty_from_aggregate_polygon_result(result)

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
                        "process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "label": "Longitude"}
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
    response.assert_error(status_code=500, error_code="Internal", message="No UDF found")


class CreoApiMocker:
    """Helper to build fake Creodias finder API catalog responses"""

    @classmethod
    def feature(cls, tile_id="T16WEA", title=None, status=0, date: str = "20200301") -> dict:
        title = title or f"S2A_MSIL2A_{date}T173231_N0209_R055_T{tile_id}_{date}T210331.SAFE"
        pid = f"/eodata/Sentinel-2/MSI/L1C/{date[0:4]}/{date[4:6]}/{date[6:8]}/{title}"
        return {
            "type": "Feature",
            "properties": {
                "status": status,
                "productIdentifier": pid,
                "title": title,
            }
        }

    @classmethod
    def feature_collection(cls, features: List[dict]) -> dict:
        features = [
            f if f.get("type") == "Feature" else cls.feature(**f)
            for f in features
        ]
        return {
            "type": "FeatureCollection",
            "properties": {"totalResults": len(features), "itemsPerPage": max(10, len(features))},
            "features": features,
        }


class TerrascopeApiMocker:
    """Helper to build fake Terrascope catalog responses"""

    @classmethod
    def feature(cls, tile_id="T16WEA", title=None, date: str = "20200301") -> dict:
        title = title or f"S2A_{date}T173231_{tile_id}_TOC_V200"
        pid = f"urn:eop:VITO:TERRASCOPE_S2_TOC_V2:{title}"
        return {
            "type": "Feature",
            "properties": {
                "title": title,
                "identifier": pid,
            }
        }

    @classmethod
    def feature_collection(cls, features: List[dict]) -> dict:
        features = [
            f if f.get("type") == "Feature" else cls.feature(**f)
            for f in features
        ]
        return {
            "type": "FeatureCollection",
            "properties": {},
            "features": features,
        }


@pytest.fixture
def jvm_mock():
    with mock.patch('openeogeotrellis.layercatalog.get_jvm') as get_jvm:
        jvm_mock = get_jvm.return_value
        raster_layer = MagicMock()
        jvm_mock.geopyspark.geotrellis.TemporalTiledRasterLayer.return_value = raster_layer
        raster_layer.layerMetadata.return_value = """{
            "crs": "EPSG:4326",
            "cellType": "uint8",
            "bounds": {"minKey": {"col":0, "row":0}, "maxKey": {"col": 1, "row": 1}},
            "extent": {"xmin": 0,"ymin": 0, "xmax": 1,"ymax": 1},
            "layoutDefinition": {
                "extent": {"xmin": 0, "ymin": 0,"xmax": 1,"ymax": 1},
                "tileLayout": {"layoutCols": 1, "layoutRows": 1, "tileCols": 256, "tileRows": 256}
            }
        }"""
        yield jvm_mock


@pytest.fixture
def urllib_mock() -> UrllibMocker:
    with UrllibMocker().patch() as mocker:
        yield mocker


@pytest.fixture
def zk_client() -> KazooClientMock:
    zk_client = KazooClientMock()
    with mock.patch(
        "openeogeotrellis.job_registry.KazooClient", return_value=zk_client
    ):
        yield zk_client


@pytest.fixture
def zk_job_registry(zk_client) -> ZkJobRegistry:
    return ZkJobRegistry(zk_client=zk_client)


def test_extra_validation_terrascope(jvm_mock, api100):
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

    simple_layer = jvm_mock.geopyspark.geotrellis.TemporalTiledRasterLayer()
    jvm_mock.org.openeo.geotrellis.file.PyramidFactory.datacube_seq.return_value = simple_layer
    jvm_mock.org.openeo.geotrellis.file.PyramidFactory.pyramid_seq.return_value = simple_layer

    def mock_query_jvm_opensearch_client(open_search_client, collection_id, _query_kwargs, processing_level=""):
        if "CreodiasClient" in str(open_search_client):
            mock_collection = [{"tile_id": "16WEA", "date": '20200301'}, {"tile_id": "16WDA", "date": '20200301'}]
        elif "OscarsClient" in str(open_search_client) or "OpenSearchClient" in str(open_search_client):
            mock_collection = [{"tile_id": "16WEA", "date": '20200301'}]
        else:
            raise Exception("Unknown open_search_client: " + str(open_search_client))
        return {
            (p["tile_id"], p["date"])
            for p in mock_collection
        }

    with mock.patch("openeogeotrellis.layercatalog.query_jvm_opensearch_client", new=mock_query_jvm_opensearch_client):

        response = api100.validation(pg)
        expected = {'errors': [
            {'code': 'MissingProduct',
             'message': "Tile ('16WDA', '20200301') in collection 'TERRASCOPE_S2_TOC_V2' is not available."}
        ]}
        assert list(sorted(map(str, response.json["errors"]))) == list(sorted(map(str, expected["errors"])))


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


def test_apply_neighborhood_filter_spatial(api100, tmp_path):
    """
    https://github.com/Open-EO/openeo-geopyspark-driver/issues/147
    @param api100:
    @param tmp_path:
    @return:
    """
    graph = {
        "abs": {
            "arguments": {
                "p": {
                    "from_parameter": "data"
                },
                "base": 2
            },
            "process_id": "power",
            "result": True
        }
    }

    response = api100.check_result({
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2020-03-01", "2020-03-10"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 32.0, "north": 32.0},
                "bands": ["Longitude", "Day"]
            },
        },
        "apply_neighborhood": {
            "process_id": "apply_neighborhood",
            "arguments": {
                "data": {"from_node": "lc"},
                "process": {"process_graph": graph},
                "size": [{'dimension': 'x', 'unit': 'px', 'value': 32},
                         {'dimension': 'y', 'unit': 'px', 'value': 32},
                         {'dimension': 't', 'value': 'P1D'}],
                "overlap": [{'dimension': 'x', 'unit': 'px', 'value': 1},
                            {'dimension': 'y', 'unit': 'px', 'value': 1}],
            },
            "result": False
        },
        "filter": {
            "process_id": "filter_spatial",
            "arguments": {"data": {"from_node": "apply_neighborhood"},
                          "geometries": mapping(box(10,10,11,11))
                          },
            "result": False,
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "filter"}, "format": "GTiff", "options":{"strict_cropping":True}},
            "result": True,
        }
    })
    with open(tmp_path / "apply_neighborhood.tif","wb") as f:
        f.write(response.data)

    with rasterio.open(tmp_path / "apply_neighborhood.tif") as ds:
        print(ds.bounds)
        assert ds.bounds.right == 11
        assert ds.width == 4


def test_aggregate_spatial_netcdf_feature_names(api100, tmp_path):
    response = api100.check_result({
        'loadcollection1': {
            'process_id': 'load_collection',
            'arguments': {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-02-20"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 2.0, "north": 2.0},
                "bands": ["Flat:1", "Month", "Day"]
            }
        },
        'loaduploadedfiles1': {
            'process_id': 'load_uploaded_files',
            'arguments': {
                'format': 'GeoJSON',
                'paths': [str(get_test_data_file("geometries/FeatureCollection.geojson"))]
            }
        },
        'aggregatespatial1': {
            'process_id': 'aggregate_spatial',
            'arguments': {
                'data': {'from_node': 'loadcollection1'},
                'geometries': {'from_node': 'loaduploadedfiles1'},
                'reducer': {
                    'process_graph': {
                        'mean1': {
                            'process_id': 'mean',
                            'arguments': {
                                'data': {'from_parameter': 'data'}
                            },
                            'result': True
                        }
                    }
                }
            }
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "aggregatespatial1"},
                "format": "netCDF"
            },
            "result": True
        }
    })

    ds = _load_xarray_dataset_from_netcdf_response(response, tmp_path=tmp_path)
    assert ds["Flat:1"].sel(t='2021-02-05').values.tolist() == [1.0, 1.0]
    assert ds["Month"].sel(t='2021-02-05').values.tolist() == [2.0, 2.0]
    assert ds["Day"].sel(t='2021-02-05').values.tolist() == [5.0, 5.0]
    assert ds.coords["feature_names"].values.tolist() == ["apples", "oranges"]


def test_load_collection_is_cached(api100):
    # unflattening this process graph will result in two calls to load_collection, unless it is cached

    process_graph = {
        'loadcollection1': {
            'process_id': 'load_collection',
            'arguments': {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-02-20"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 2.0, "north": 2.0},
                "bands": ["Flat:1", "Month"]
            }
        },
        'filterbands1': {
            'process_id': 'filter_bands',
            'arguments': {
                'bands': ['Flat:1'],
                'data': {'from_node': 'loadcollection1'}
            }
        },
        'filterbands2': {
            'process_id': 'filter_bands',
            'arguments': {
                'bands': ['Month'],
                'data': {'from_node': 'loadcollection1'}
            }
        },
        'mergecubes1': {
            'process_id': 'merge_cubes',
            'arguments': {
                'cube1': {'from_node': 'filterbands1'},
                'cube2': {'from_node': 'filterbands2'}
            }
        },
        'loaduploadedfiles1': {
            'process_id': 'load_uploaded_files',
            'arguments': {
                'format': 'GeoJSON',
                'paths': [str(get_test_data_file("geometries/FeatureCollection.geojson"))]
            }
        },
        'aggregatespatial1': {
            'process_id': 'aggregate_spatial',
            'arguments': {
                'data': {'from_node': 'mergecubes1'},
                'geometries': {'from_node': 'loaduploadedfiles1'},
                'reducer': {
                    'process_graph': {
                        'mean1': {
                            'process_id': 'mean',
                            'arguments': {
                                'data': {'from_parameter': 'data'}
                            },
                            'result': True}
                    }
                }
            }, 'result': True
        }
    }

    with mock.patch('openeogeotrellis.layercatalog.logger') as logger:
        result = api100.check_result(process_graph).json

        assert result == {
            "2021-01-05T00:00:00Z": [[1.0, 1.0], [1.0, 1.0]],
            "2021-01-15T00:00:00Z": [[1.0, 1.0], [1.0, 1.0]],
            "2021-01-25T00:00:00Z": [[1.0, 1.0], [1.0, 1.0]],
            "2021-02-05T00:00:00Z": [[1.0, 2.0], [1.0, 2.0]],
            "2021-02-15T00:00:00Z": [[1.0, 2.0], [1.0, 2.0]],
        }

        # TODO: is there an easier way to count the calls to lru_cache-decorated function load_collection?
        creating_layer_calls = list(filter(lambda call: call.args[0].startswith("Creating layer for TestCollection-LonLat4x4"),
                                           logger.info.call_args_list))

        n_load_collection_calls = len(creating_layer_calls)
        assert n_load_collection_calls == 1


class TestAggregateSpatial:
    """
    Various tests for aggregate_spatial (e.g. with Point geometries),
    originally defined in openeo-geopyspark-integrationtests test function `test_point_timeseries`
    """

    def _load_cube(
        self, spatial_extent: Union[dict, str, None] = "default"
    ) -> openeo.DataCube:
        """Load initial dummy data cube"""
        if spatial_extent == "default":
            spatial_extent = {"west": 0, "south": 0, "east": 8, "north": 5}
        return openeo.DataCube.load_collection(
            "TestCollection-LonLat4x4",
            temporal_extent=["2021-01-01", "2021-02-01"],
            spatial_extent=spatial_extent,
            bands=["Day", "Longitude", "Latitude"],
            fetch_metadata=False,
        )

    @pytest.fixture
    def cube(self) -> openeo.DataCube:
        return self._load_cube()

    @pytest.mark.parametrize(
        ["geometry", "expected_lon_lat_agg"],
        [
            (Point(2.2, 2.2), [2.0, 2.0]),
            (Point(5.5, 3.3), [5.5, 3.25]),
            (Point(3.9, 4.6), [3.75, 4.5]),
            (Polygon.from_bounds(3.1, 1.2, 4.9, 2.8), [3.875, 1.875]),
            (Polygon.from_bounds(0.4, 3.2, 2.8, 4.8), [1.5, 3.875]),
            (Polygon.from_bounds(5.6, 0.2, 7.4, 3.8), [6.375, 1.875]),
        ],
    )
    @pytest.mark.parametrize("load_collection_spatial_extent", ["default", None])
    def test_aggregate_single_geometry(
        self, api100, geometry, expected_lon_lat_agg, load_collection_spatial_extent
    ):
        cube = self._load_cube(spatial_extent=load_collection_spatial_extent)
        cube = cube.aggregate_spatial(geometry, "mean")
        result = api100.check_result(cube).json
        result = drop_empty_from_aggregate_polygon_result(result)

        assert result == {
            "2021-01-05T00:00:00Z": [[5.0] + expected_lon_lat_agg],
            "2021-01-15T00:00:00Z": [[15.0] + expected_lon_lat_agg],
            "2021-01-25T00:00:00Z": [[25.0] + expected_lon_lat_agg],
        }

    @pytest.mark.parametrize(
        ["geometry", "expected"],
        [
            (Point(1.2, 2.3), (1, 1, 2.25)),
            (Point(2.7, 4.9), (1, 2.5, 4.75)),
            (Polygon.from_bounds(3.1, 1.2, 4.9, 2.8), (48, 3.0, 1.25)),
            (Polygon.from_bounds(5.6, 0.2, 7.4, 3.8), (112, 5.5, 0.25)),
        ],
    )
    def test_aggregate_single_geometry_multiple_aggregations(
        self, cube, api100, geometry, expected
    ):
        from openeo.processes import array_create, count, min

        cube = cube.aggregate_spatial(
            geometry, lambda data: array_create([min(data), count(data)])
        )
        result = api100.check_result(cube).json
        c, o, a = expected
        assert result == {
            "2021-01-05T00:00:00Z": [[5.0, c, o, c, a, c]],
            "2021-01-15T00:00:00Z": [[15.0, c, o, c, a, c]],
            "2021-01-25T00:00:00Z": [[25.0, c, o, c, a, c]],
        }

    def test_aggregate_heterogeneous_geometry_collection(self, cube, api100):
        # TODO #71 GeometryCollection usage is deprecated usage pattern
        geometry = GeometryCollection(
            [
                Point(1.2, 2.3),
                Point(3.7, 4.2),
                Polygon.from_bounds(3.1, 1.2, 4.9, 2.8),
                Point(4.5, 3.8),
                Polygon.from_bounds(5.6, 0.2, 7.4, 3.8),
            ]
        )
        cube = cube.aggregate_spatial(geometry, "mean")
        result = api100.check_result(cube).json
        assert result == {
            "2021-01-05T00:00:00Z": [
                [5.0, 1.0, 2.25],
                [5.0, 3.5, 4.0],
                [5.0, 3.875, 1.875],
                [5.0, 4.5, 3.75],
                [5.0, 6.375, 1.875],
            ],
            "2021-01-15T00:00:00Z": [
                [15.0, 1.0, 2.25],
                [15.0, 3.5, 4.0],
                [15.0, 3.875, 1.875],
                [15.0, 4.5, 3.75],
                [15.0, 6.375, 1.875],
            ],
            "2021-01-25T00:00:00Z": [
                [25.0, 1.0, 2.25],
                [25.0, 3.5, 4.0],
                [25.0, 3.875, 1.875],
                [25.0, 4.5, 3.75],
                [25.0, 6.375, 1.875],
            ],
        }

    @pytest.mark.parametrize(
        ["geometry", "expected_lon_lat_agg"],
        [
            (Point(1.2, 2.3), [1.0, 2.25]),
            (Polygon.from_bounds(3.1, 1.2, 4.9, 2.8), [3.875, 1.875]),
        ],
    )
    @pytest.mark.parametrize("load_collection_spatial_extent", ["default", None])
    def test_aggregate_feature_with_single_geometry(
        self, api100, geometry, expected_lon_lat_agg, load_collection_spatial_extent
    ):
        cube = self._load_cube(spatial_extent=load_collection_spatial_extent)

        geometry = as_geojson_feature(geometry)
        cube = cube.aggregate_spatial(geometry, "mean")
        result = api100.check_result(cube).json
        result = drop_empty_from_aggregate_polygon_result(result)
        assert result == {
            "2021-01-05T00:00:00Z": [[5.0] + expected_lon_lat_agg],
            "2021-01-15T00:00:00Z": [[15.0] + expected_lon_lat_agg],
            "2021-01-25T00:00:00Z": [[25.0] + expected_lon_lat_agg],
        }

    @pytest.mark.parametrize("load_collection_spatial_extent", ["default", None])
    def test_aggregate_feature_collection_of_points(
        self, api100, load_collection_spatial_extent
    ):
        cube = self._load_cube(spatial_extent=load_collection_spatial_extent)
        geometry = as_geojson_feature_collection(
            Point(1.2, 2.3),
            Point(3.7, 4.2),
            Point(4.5, 3.8),
        )
        cube = cube.aggregate_spatial(geometry, "mean")
        result = api100.check_result(cube).json
        assert result == {
            "2021-01-05T00:00:00Z": [
                [5.0, 1.0, 2.25],
                [5.0, 3.5, 4.0],
                [5.0, 4.5, 3.75],
            ],
            "2021-01-15T00:00:00Z": [
                [15.0, 1.0, 2.25],
                [15.0, 3.5, 4.0],
                [15.0, 4.5, 3.75],
            ],
            "2021-01-25T00:00:00Z": [
                [25.0, 1.0, 2.25],
                [25.0, 3.5, 4.0],
                [25.0, 4.5, 3.75],
            ],
        }

    def test_aggregate_feature_collection_of_polygons(self, cube, api100):
        geometry = as_geojson_feature_collection(
            Polygon.from_bounds(3.1, 1.2, 4.9, 2.8),
            Polygon.from_bounds(0.4, 3.2, 2.8, 4.8),
            Polygon.from_bounds(5.6, 0.2, 7.4, 3.8),
        )
        cube = cube.aggregate_spatial(geometry, "mean")
        result = api100.check_result(cube).json
        result = drop_empty_from_aggregate_polygon_result(result)
        assert result == {
            "2021-01-05T00:00:00Z": [
                [5.0, 3.875, 1.875],
                [5.0, 1.5, 3.875],
                [5.0, 6.375, 1.875],
            ],
            "2021-01-15T00:00:00Z": [
                [15.0, 3.875, 1.875],
                [15.0, 1.5, 3.875],
                [15.0, 6.375, 1.875],
            ],
            "2021-01-25T00:00:00Z": [
                [25.0, 3.875, 1.875],
                [25.0, 1.5, 3.875],
                [25.0, 6.375, 1.875],
            ],
        }

    @pytest.mark.parametrize("load_collection_spatial_extent", ["default", None])
    def test_aggregate_feature_collection_heterogeneous(
        self, api100, load_collection_spatial_extent
    ):
        cube = self._load_cube(spatial_extent=load_collection_spatial_extent)

        geometry = as_geojson_feature_collection(
            Point(1.2, 2.3),
            Point(3.7, 4.2),
            Polygon.from_bounds(3.1, 1.2, 4.9, 2.8),
            Point(4.5, 3.8),
            Polygon.from_bounds(5.6, 0.2, 7.4, 3.8),
        )
        cube = cube.aggregate_spatial(geometry, "mean")
        result = api100.check_result(cube).json
        assert result == {
            "2021-01-05T00:00:00Z": [
                [5.0, 1.0, 2.25],
                [5.0, 3.5, 4.0],
                [5.0, 3.875, 1.875],
                [5.0, 4.5, 3.75],
                [5.0, 6.375, 1.875],
            ],
            "2021-01-15T00:00:00Z": [
                [15.0, 1.0, 2.25],
                [15.0, 3.5, 4.0],
                [15.0, 3.875, 1.875],
                [15.0, 4.5, 3.75],
                [15.0, 6.375, 1.875],
            ],
            "2021-01-25T00:00:00Z": [
                [25.0, 1.0, 2.25],
                [25.0, 3.5, 4.0],
                [25.0, 3.875, 1.875],
                [25.0, 4.5, 3.75],
                [25.0, 6.375, 1.875],
            ],
        }

    def test_aggregate_feature_collection_heterogeneous_multiple_aggregations(
        self, cube, api100
    ):
        from openeo.processes import array_create, count, min

        geometry = as_geojson_feature_collection(
            Point(1.2, 2.3),
            Point(3.7, 4.2),
            Polygon.from_bounds(3.1, 1.2, 4.9, 2.8),
            Point(4.5, 3.8),
            Polygon.from_bounds(5.6, 0.2, 7.4, 3.8),
        )
        cube = cube.aggregate_spatial(
            geometry, lambda data: array_create([min(data), count(data)])
        )
        result = api100.check_result(cube).json
        assert result == {
            "2021-01-05T00:00:00Z": [
                [5.0, 1.0, 1.0, 1.0, 2.25, 1.0],
                [5.0, 1.0, 3.5, 1.0, 4.0, 1.0],
                [5.0, 48.0, 3.0, 48.0, 1.25, 48.0],
                [5.0, 1.0, 4.5, 1.0, 3.75, 1.0],
                [5.0, 112.0, 5.5, 112.0, 0.25, 112.0],
            ],
            "2021-01-15T00:00:00Z": [
                [15.0, 1.0, 1.0, 1.0, 2.25, 1.0],
                [15.0, 1.0, 3.5, 1.0, 4.0, 1.0],
                [15.0, 48.0, 3.0, 48.0, 1.25, 48.0],
                [15.0, 1.0, 4.5, 1.0, 3.75, 1.0],
                [15.0, 112.0, 5.5, 112.0, 0.25, 112.0],
            ],
            "2021-01-25T00:00:00Z": [
                [25.0, 1.0, 1.0, 1.0, 2.25, 1.0],
                [25.0, 1.0, 3.5, 1.0, 4.0, 1.0],
                [25.0, 48.0, 3.0, 48.0, 1.25, 48.0],
                [25.0, 1.0, 4.5, 1.0, 3.75, 1.0],
                [25.0, 112.0, 5.5, 112.0, 0.25, 112.0],
            ],
        }

    @pytest.mark.parametrize("load_collection_spatial_extent", ["default", None])
    def test_aggregate_geometry_from_file(self, api100, load_collection_spatial_extent):
        cube = self._load_cube(spatial_extent=load_collection_spatial_extent)
        cube = cube.aggregate_spatial(
            get_test_data_file("geometries/FeatureCollection.geojson"), "mean"
        )
        result = api100.check_result(cube).json
        result = drop_empty_from_aggregate_polygon_result(result)
        assert result == {
            "2021-01-05T00:00:00Z": [[5.0, 0.375, 0.25], [5.0, 1.625, 1.625]],
            "2021-01-15T00:00:00Z": [[15.0, 0.375, 0.25], [15.0, 1.625, 1.625]],
            "2021-01-25T00:00:00Z": [[25.0, 0.375, 0.25], [25.0, 1.625, 1.625]],
        }


class TestVectorCubeRunUdf:
    """
    Tests about running `run_udf` on a vector cube (e.g. output of aggregate_spatial)
    in scalable way (parallelized)

    ref: https://github.com/Open-EO/openeo-geopyspark-driver/issues/251
    """

    def _load_cube(
        self,
        temporal_extent: Sequence[str] = ("2021-01-01", "2021-02-01"),
        spatial_extent: Union[str, dict, None] = "default",
    ) -> openeo.DataCube:
        if spatial_extent == "default":
            spatial_extent = {"west": 0, "south": 0, "east": 8, "north": 8}
        cube = openeo.DataCube.load_collection(
            "TestCollection-LonLat4x4",
            temporal_extent=temporal_extent,
            spatial_extent=spatial_extent,
            bands=["Day", "Longitude", "Latitude"],
            fetch_metadata=False,
        )
        return cube

    def test_legacy_simple(self, api100):
        """Legacy run_udf on vector cube (non-parallelized)."""
        cube = self._load_cube()
        geometries = get_test_data_file("geometries/FeatureCollection03.json")
        aggregates = cube.aggregate_spatial(geometries, "min")
        udf = textwrap.dedent(
            """
            from openeo.udf import UdfData, StructuredData
            def apply_udf_data(data: UdfData):
                data.set_feature_collection_list(None)
                data.set_structured_data_list([
                    StructuredData([1, 1, 2, 3, 5, 8]),
                ])
            """
        )
        processed = openeo.processes.run_udf(aggregates, udf=udf, runtime="Python")
        result = api100.check_result(processed).json
        assert result == [1, 1, 2, 3, 5, 8]

    def test_legacy_run_in_executor(self, api100):
        """
        Legacy run_udf implementation should not run in driver process
        https://github.com/Open-EO/openeo-geopyspark-driver/issues/404
        """
        cube = self._load_cube()
        geometries = get_test_data_file("geometries/FeatureCollection03.json")
        aggregates = cube.aggregate_spatial(geometries, "min")
        udf = textwrap.dedent(
            """
            import os, sys
            from openeo.udf import UdfData, StructuredData
            def apply_udf_data(data: UdfData):
                data.set_feature_collection_list(None)
                data.set_structured_data_list([
                    StructuredData([
                        "Greetings from apply_udf_data",
                        os.getpid(),
                        sys.executable,
                        sys.argv,
                    ])
                ])
        """
        )
        processed = openeo.processes.run_udf(aggregates, udf=udf, runtime="Python")

        result = api100.check_result(processed).json
        print(result)
        assert isinstance(result, list)
        assert result[0] == "Greetings from apply_udf_data"
        # Check that UDF ran in executor process instead of driver process
        assert result[1] != os.getpid()
        assert "pyspark/daemon.py" in result[3][0]

    def test_udf_apply_udf_data_scalar(self, api100):
        # TODO: influence of tight spatial_extent that excludes some geometries? e.g.:
        cube = self._load_cube()
        geometries = get_test_data_file("geometries/FeatureCollection03.json")
        aggregates = cube.aggregate_spatial(geometries, "min")
        udf = textwrap.dedent(
            """
            from openeo.udf import UdfData

            def udf_apply_udf_data(udf_data: UdfData) -> float:
                data = udf_data.get_structured_data_list()[0].data
                # Data's structure: {datetime: [[float for each band] for each polygon]}
                assert isinstance(data, dict)
                # Convert to single scalar value
                ((_, lon, lat),) = data["2021-01-05T00:00:00Z"]
                return 1000 * lon + lat
        """
        )
        processed = openeo.processes.run_udf(aggregates, udf=udf, runtime="Python")

        result = api100.check_result(processed).json
        result = drop_empty_from_aggregate_polygon_result(result)
        assert result == DictSubSet(
            columns=["feature_index", "0"],
            data=IgnoreOrder(
                [
                    [0, 1001.0],
                    [1, 4002.0],
                    [2, 2004.0],
                    [3, 5000.0],
                ]
            ),
        )

    def test_udf_apply_udf_data_reduce_bands(self, api100):
        cube = self._load_cube(temporal_extent=["2021-01-01", "2021-01-20"])
        geometries = get_test_data_file("geometries/FeatureCollection03.json")
        aggregates = cube.aggregate_spatial(geometries, "min")
        udf = textwrap.dedent(
            """
            from openeo.udf import UdfData, StructuredData

            def udf_apply_udf_data(udf_data: UdfData) -> UdfData:
                data = udf_data.get_structured_data_list()[0].data
                # Data's structure: {datetime: [[float for each band] for each polygon]}
                # Convert to {datetime: [float for each polygon]}
                data = {date: [sum(bands) for bands in geometry_data] for date, geometry_data in data.items()}
                return UdfData(structured_data_list=[StructuredData(data)])
        """
        )
        processed = openeo.processes.run_udf(aggregates, udf=udf, runtime="Python")

        result = api100.check_result(processed).json
        result = drop_empty_from_aggregate_polygon_result(result)
        assert result == DictSubSet(
            columns=["feature_index", "level_1", "0"],
            data=IgnoreOrder(
                [
                    [0, "2021-01-05T00:00:00Z", 5 + 1 + 1],
                    [0, "2021-01-15T00:00:00Z", 15 + 1 + 1],
                    [1, "2021-01-05T00:00:00Z", 5 + 4 + 2],
                    [1, "2021-01-15T00:00:00Z", 15 + 4 + 2],
                    [2, "2021-01-05T00:00:00Z", 5 + 2 + 4],
                    [2, "2021-01-15T00:00:00Z", 15 + 2 + 4],
                    [3, "2021-01-05T00:00:00Z", 5 + 5 + 0],
                    [3, "2021-01-15T00:00:00Z", 15 + 5 + 0],
                ]
            ),
        )

    def test_udf_apply_udf_data_reduce_time(self, api100):
        cube = self._load_cube()
        geometries = get_test_data_file("geometries/FeatureCollection03.json")
        aggregates = cube.aggregate_spatial(geometries, "min")
        udf = textwrap.dedent(
            """
            from openeo.udf import UdfData, StructuredData
            import functools

            def udf_apply_udf_data(udf_data: UdfData) -> UdfData:
                data = udf_data.get_structured_data_list()[0].data
                # Data's structure: {datetime: [[float for each band] for each polygon]}
                # Convert to [[float for each band] for each polygon]}
                data = functools.reduce(
                    lambda d1, d2: [[b1 + b2 for (b1, b2) in zip(d1[0], d2[0])]],
                    data.values()
                )
                return UdfData(structured_data_list=[StructuredData(data)])
        """
        )
        processed = openeo.processes.run_udf(aggregates, udf=udf, runtime="Python")

        result = api100.check_result(processed).json
        result = drop_empty_from_aggregate_polygon_result(result)
        assert result == DictSubSet(
            columns=["feature_index", "level_1", "0", "1", "2"],
            data=IgnoreOrder(
                [
                    [0, 0, 5 + 15 + 25, 3 * 1, 3 * 1],
                    [1, 0, 5 + 15 + 25, 3 * 4, 3 * 2],
                    [2, 0, 5 + 15 + 25, 3 * 2, 3 * 4],
                    [3, 0, 5 + 15 + 25, 3 * 5, 3 * 0],
                ]
            ),
        )

    def test_udf_apply_udf_data_return_series(self, api100):
        cube = self._load_cube()
        geometries = get_test_data_file("geometries/FeatureCollection03.json")
        aggregates = cube.aggregate_spatial(geometries, "min")
        udf = textwrap.dedent(
            """
            import pandas
            from openeo.udf import UdfData, StructuredData

            def udf_apply_udf_data(udf_data: UdfData) -> UdfData:
                data = udf_data.get_structured_data_list()[0].data
                # Data's structure: {datetime: [[float for each band] for each polygon]}
                # Convert to series {"start": sum(bands), "end": sum(bands)}
                series = pandas.Series({
                    "start": sum(min(data.items())[1][0]),
                    "end": sum(max(data.items())[1][0]),
                })
                return series
        """
        )
        processed = openeo.processes.run_udf(aggregates, udf=udf, runtime="Python")

        result = api100.check_result(processed).json
        result = drop_empty_from_aggregate_polygon_result(result)
        assert result == DictSubSet(
            columns=["feature_index", "start", "end"],
            data=IgnoreOrder(
                [
                    [0, 5 + 1 + 1, 25 + 1 + 1],
                    [1, 5 + 4 + 2, 25 + 4 + 2],
                    [2, 5 + 2 + 4, 25 + 2 + 4],
                    [3, 5 + 5 + 0, 25 + 5 + 0],
                ]
            ),
        )

    def test_udf_apply_udf_data_return_dataframe(self, api100):
        cube = self._load_cube()
        geometries = get_test_data_file("geometries/FeatureCollection03.json")
        aggregates = cube.aggregate_spatial(geometries, "min")
        udf = textwrap.dedent(
            """
            import pandas
            from openeo.udf import UdfData, StructuredData

            def udf_apply_udf_data(udf_data: UdfData) -> UdfData:
                data = udf_data.get_structured_data_list()[0].data
                # Data's structure: {datetime: [[float for each band] for each polygon]}
                # Convert to series {"start": [floats], "end": [floats]}
                start_values = min(data.items())[1][0]
                end_values = max(data.items())[1][0]
                df = pandas.DataFrame(
                    {
                        "start": [min(start_values), max(start_values)],
                        "end": [min(end_values), max(end_values)],
                    },
                    index = pandas.Index(["min", "max"], name="band_range")
                )
                return df
        """
        )
        processed = openeo.processes.run_udf(aggregates, udf=udf, runtime="Python")

        result = api100.check_result(processed).json
        result = drop_empty_from_aggregate_polygon_result(result)
        assert result == DictSubSet(
            columns=["feature_index", "band_range", "start", "end"],
            data=IgnoreOrder(
                [
                    [0, "max", 5, 25],
                    [0, "min", 1, 1],
                    [1, "max", 5, 25],
                    [1, "min", 2, 2],
                    [2, "max", 5, 25],
                    [2, "min", 2, 2],
                    [3, "max", 5, 25],
                    [3, "min", 0, 0],
                ]
            ),
        )

    def test_udf_apply_feature_dataframe_basic(self, api100):
        cube = self._load_cube()
        geometries = get_test_data_file("geometries/FeatureCollection02.json")
        aggregates = cube.aggregate_spatial(geometries, "max")
        udf = textwrap.dedent(
            """
            import pandas as pd
            def udf_apply_feature_dataframe(df: pd.DataFrame) -> pd.DataFrame:
                return df + 1000
        """
        )
        processed = openeo.processes.run_udf(aggregates, udf=udf, runtime="Python")

        result = api100.check_result(processed).json
        result = drop_empty_from_aggregate_polygon_result(result)
        assert result == DictSubSet(
            columns=[
                "feature_index",
                "date",
                "max(band_0)",
                "max(band_1)",
                "max(band_2)",
            ],
            data=IgnoreOrder(
                [
                    [0, "2021-01-05T00:00:00.000Z", 1005.0, 1002.75, 1002.5],
                    [0, "2021-01-15T00:00:00.000Z", 1015.0, 1002.75, 1002.5],
                    [0, "2021-01-25T00:00:00.000Z", 1025.0, 1002.75, 1002.5],
                    [1, "2021-01-05T00:00:00.000Z", 1005.0, 1004.75, 1003.75],
                    [1, "2021-01-15T00:00:00.000Z", 1015.0, 1004.75, 1003.75],
                    [1, "2021-01-25T00:00:00.000Z", 1025.0, 1004.75, 1003.75],
                ],
            ),
        )

    def test_udf_apply_feature_dataframe_reduce_time(self, api100):
        cube = self._load_cube()
        geometries = get_test_data_file("geometries/FeatureCollection03.json")
        aggregates = cube.aggregate_spatial(geometries, "max")
        udf = textwrap.dedent(
            """
            import pandas as pd
            def udf_apply_feature_dataframe(df: pd.DataFrame) -> pd.Series:
                return df.sum(axis=0)
        """
        )
        processed = openeo.processes.run_udf(aggregates, udf=udf, runtime="Python")

        result = api100.check_result(processed).json
        result = drop_empty_from_aggregate_polygon_result(result)
        assert result == DictSubSet(
            columns=[
                "feature_index",
                "max(band_0)",
                "max(band_1)",
                "max(band_2)",
            ],
            data=IgnoreOrder(
                [
                    [0, 5 + 15 + 25, 3 * 2.75, 3 * 2.75],
                    [1, 5 + 15 + 25, 3 * 6.75, 3 * 3.75],
                    [2, 5 + 15 + 25, 3 * 2.75, 3 * 7.75],
                    [3, 5 + 15 + 25, 3 * 5.75, 3 * 0.75],
                ]
            ),
        )

    def test_udf_apply_feature_dataframe_reduce_bands(self, api100):
        cube = self._load_cube()
        geometries = get_test_data_file("geometries/FeatureCollection03.json")
        aggregates = cube.aggregate_spatial(geometries, "max")
        udf = textwrap.dedent(
            """
            import pandas as pd
            def udf_apply_feature_dataframe(df: pd.DataFrame) -> pd.Series:
                series = df.sum(axis=1)
                series.index = series.index.strftime("%Y-%m-%d")
                return series
        """
        )
        processed = openeo.processes.run_udf(aggregates, udf=udf, runtime="Python")

        result = api100.check_result(processed).json
        result = drop_empty_from_aggregate_polygon_result(result)
        assert result == DictSubSet(
            columns=["feature_index", "2021-01-05", "2021-01-15", "2021-01-25"],
            data=IgnoreOrder(
                [
                    [0, 5 + 2.75 + 2.75, 15 + 2.75 + 2.75, 25 + 2.75 + 2.75],
                    [1, 5 + 6.75 + 3.75, 15 + 6.75 + 3.75, 25 + 6.75 + 3.75],
                    [2, 5 + 2.75 + 7.75, 15 + 2.75 + 7.75, 25 + 2.75 + 7.75],
                    [3, 5 + 5.75 + 0.75, 15 + 5.75 + 0.75, 25 + 5.75 + 0.75],
                ]
            ),
        )

    def test_udf_apply_feature_dataframe_reduce_both_to_float(self, api100):
        cube = self._load_cube()
        geometries = get_test_data_file("geometries/FeatureCollection03.json")
        aggregates = cube.aggregate_spatial(geometries, "max")
        udf = textwrap.dedent(
            """
            import pandas as pd
            def udf_apply_feature_dataframe(df: pd.DataFrame) -> float:
                return df.sum().sum()
        """
        )
        processed = openeo.processes.run_udf(aggregates, udf=udf, runtime="Python")

        result = api100.check_result(processed).json
        result = drop_empty_from_aggregate_polygon_result(result)
        assert result == DictSubSet(
            columns=["feature_index", "0"],
            data=IgnoreOrder(
                [
                    [0, 5 + 15 + 25 + 3 * (2.75 + 2.75)],
                    [1, 5 + 15 + 25 + 3 * (6.75 + 3.75)],
                    [2, 5 + 15 + 25 + 3 * (2.75 + 7.75)],
                    [3, 5 + 15 + 25 + 3 * (5.75 + 0.75)],
                ]
            ),
        )

    # TODO test that df index is timestamp
    # TODO: test that creates new series/dataframe with band stats


def _setup_existing_job(
    *,
    job_id: str,
    api: ApiTester,
    batch_job_output_root: Path,
    zk_job_registry: ZkJobRegistry,
    user_id=TEST_USER,
) -> Path:
    """
    Set up an exiting job, with a geopyspark-driver-style job result folder,
    (based on result files from a given job id from the test data folder),
    and metadata in job registry.
    """
    source_result_dir = api.data_path(f"binary/jobs/{job_id}")
    result_dir = batch_job_output_root / job_id
    _log.info(f"Copy {source_result_dir=} to {result_dir=}")
    shutil.copytree(src=source_result_dir, dst=result_dir)

    # Rewrite paths in job_metadata.json
    job_metadata_file = result_dir / JOB_METADATA_FILENAME
    _log.info(f"Rewriting asset paths in {job_metadata_file=}")
    job_metadata_file.write_text(
        job_metadata_file.read_text(encoding="utf-8").replace(
            "/data/projects/OpenEO", str(batch_job_output_root)
        )
    )

    # Register metadata in job registry too
    zk_job_registry.register(
        job_id=job_id,
        user_id=user_id,
        api_version="1.1.0",
        specification=load_json(result_dir / "process_graph.json"),
    )
    job_metadata = load_json(job_metadata_file)
    zk_job_registry.patch(
        job_id=job_id,
        user_id=user_id,
        **{k: job_metadata[k] for k in ["bbox", "epsg"]},
    )
    zk_job_registry.set_status(
        job_id=job_id, user_id=user_id, status=JOB_STATUS.FINISHED
    )

    return result_dir


def _setup_metadata_request_mocking(
    job_id: str,
    api: ApiTester,
    results_dir: Path,
    results_url: str,
    urllib_mock: UrllibMocker,
):
    # Use ApiTester to easily build responses for the metadata request we have to mock
    api.set_auth_bearer_token()
    results_metadata = (
        api.get(f"jobs/{job_id}/results").assert_status_code(200).json
    )
    urllib_mock.get(results_url, data=json.dumps(results_metadata))
    # Mock each collection item metadata request too
    for link in results_metadata["links"]:
        if link["rel"] == "item":
            path = link["href"].partition(api.url_root)[-1]
            item_metadata = api.get(path).assert_status_code(200).json
            # Change asset urls to local paths so the data can easily be read (without URL mocking in scala) by
            # org.openeo.geotrellis.geotiff.PyramidFactory.from_uris()
            for k in item_metadata["assets"]:
                item_metadata["assets"][k]["href"] = str(results_dir / k)
            urllib_mock.get(link["href"], json.dumps(item_metadata))


class TestLoadResult:
    def test_load_result_job_id_basic(
        self, api110, zk_client, zk_job_registry, batch_job_output_root
    ):
        job_id = "j-ec5d3e778ba5423d8d88a50b08cb9f63"

        _setup_existing_job(
            job_id=job_id,
            api=api110,
            batch_job_output_root=batch_job_output_root,
            zk_job_registry=zk_job_registry,
        )

        process_graph = {
            "lc": {
                "process_id": "load_result",
                "arguments": {"id": job_id},
            },
            "save": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "lc"}, "format": "json"},
                "result": True,
            },
        }
        response = api110.check_result(process_graph)
        result = response.assert_status_code(200).json

        assert result == DictSubSet(
            {
                "dims": ["t", "bands", "x", "y"],
                "attrs": DictSubSet(
                    {
                        "shape": [3, 2, 73, 92],
                        "crs": RegexMatcher(".*utm.*zone=31"),
                    }
                ),
                "coords": DictSubSet(
                    {
                        "t": {
                            "dims": ["t"],
                            "attrs": {"dtype": "datetime64[ns]", "shape": [3]},
                            "data": [
                                "2022-09-07 00:00:00",
                                "2022-09-12 00:00:00",
                                "2022-09-19 00:00:00",
                            ],
                        },
                        "bands": {
                            "attrs": {"dtype": "<U3", "shape": [2]},
                            "data": ["B02", "B03"],
                            "dims": ["bands"],
                        },
                        "x": {
                            "dims": ["x"],
                            "attrs": {"dtype": "float64", "shape": [73]},
                            "data": ListSubSet(
                                [644765.0, 644775.0, 644785.0, 645485.0]
                            ),
                        },
                        "y": {
                            "dims": ["y"],
                            "attrs": {"dtype": "float64", "shape": [92]},
                            "data": ListSubSet(
                                [5675445.0, 5675455.0, 5675465.0, 5676355.0]
                            ),
                        },
                    }
                ),
            }
        )
        data = np.array(result["data"])
        assert data.shape == (3, 2, 73, 92)
        assert_equal(
            data[:1, :2, :3, :4],
            [
                [
                    [[584, 587, 592, 579], [580, 604, 604, 560], [610, 592, 611, 592]],
                    [[588, 593, 572, 560], [565, 574, 580, 566], [576, 572, 590, 568]],
                ]
            ],
        )

    @pytest.mark.parametrize(
        ["load_result_kwargs", "expected"],
        [
            (
                {
                    "spatial_extent": {
                        "west": 5.077,
                        "south": 51.215,
                        "east": 5.080,
                        "north": 51.219,
                    }
                },
                {
                    "dims": ["t", "bands", "x", "y"],
                    "shape": (3, 2, 24, 46),
                    "ts": [
                        "2022-09-07 00:00:00",
                        "2022-09-12 00:00:00",
                        "2022-09-19 00:00:00",
                    ],
                    "bands": ["B02", "B03"],
                    "xs": [645045.0, 645055.0, 645275.0],
                    "ys": [5675785.0, 5675795.0, 5676235.0],
                },
            ),
            (
                {"temporal_extent": ["2022-09-10", "2022-09-15"]},
                {
                    "dims": ["t", "bands", "x", "y"],
                    "shape": (1, 2, 73, 92),
                    "ts": [
                        "2022-09-12 00:00:00",
                    ],
                    "bands": ["B02", "B03"],
                    "xs": [644765.0, 644775.0, 645485.0],
                    "ys": [5675445.0, 5675455.0, 5676355.0],
                },
            ),
            (
                {"bands": ["B03"]},
                {
                    "dims": ["t", "bands", "x", "y"],
                    "shape": (3, 1, 73, 92),
                    "ts": [
                        "2022-09-07 00:00:00",
                        "2022-09-12 00:00:00",
                        "2022-09-19 00:00:00",
                    ],
                    "bands": ["B03"],
                    "xs": [644765.0, 644775.0, 645485.0],
                    "ys": [5675445.0, 5675455.0, 5676355.0],
                },
            ),
        ],
    )
    def test_load_result_job_id_filtering(
        self,
        api110,
        zk_client,
        zk_job_registry,
        batch_job_output_root,
        load_result_kwargs,
        expected,
    ):
        job_id = "j-ec5d3e778ba5423d8d88a50b08cb9f63"

        _setup_existing_job(
            job_id=job_id,
            api=api110,
            batch_job_output_root=batch_job_output_root,
            zk_job_registry=zk_job_registry,
        )

        process_graph = {
            "lc": {
                "process_id": "load_result",
                "arguments": {"id": job_id, **load_result_kwargs},
            },
            "save": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "lc"}, "format": "json"},
                "result": True,
            },
        }
        response = api110.check_result(process_graph)
        result = response.assert_status_code(200).json

        assert result["dims"] == expected["dims"]
        assert result["attrs"]["shape"] == list(expected["shape"])
        assert result["coords"]["t"]["data"] == expected["ts"]
        assert result["coords"]["bands"]["data"] == expected["bands"]
        assert result["coords"]["x"]["data"] == ListSubSet(expected["xs"])
        assert result["coords"]["y"]["data"] == ListSubSet(expected["ys"])
        data = np.array(result["data"])
        assert data.shape == expected["shape"]

    def test_load_result_url_basic(
        self,
        api110,
        zk_client,
        zk_job_registry,
        batch_job_output_root,
        urllib_mock,
    ):
        job_id = "j-ec5d3e778ba5423d8d88a50b08cb9f63"
        results_url = f"https://foobar.test/job/{job_id}/results"

        results_dir = _setup_existing_job(
            job_id=job_id,
            api=api110,
            batch_job_output_root=batch_job_output_root,
            zk_job_registry=zk_job_registry,
        )
        _setup_metadata_request_mocking(
            job_id=job_id,
            api=api110,
            results_dir=results_dir,
            results_url=results_url,
            urllib_mock=urllib_mock,
        )

        process_graph = {
            "lc": {
                "process_id": "load_result",
                "arguments": {"id": results_url},
            },
            "save": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "lc"}, "format": "json"},
                "result": True,
            },
        }
        response = api110.check_result(process_graph)
        result = response.assert_status_code(200).json

        assert result == DictSubSet(
            {
                "dims": ["t", "bands", "x", "y"],
                "attrs": DictSubSet(
                    {
                        "shape": [3, 2, 73, 92],
                        "crs": RegexMatcher(".*utm.*zone=31"),
                    }
                ),
                "coords": DictSubSet(
                    {
                        "t": {
                            "dims": ["t"],
                            "attrs": {"dtype": "datetime64[ns]", "shape": [3]},
                            "data": [
                                "2022-09-07 00:00:00",
                                "2022-09-12 00:00:00",
                                "2022-09-19 00:00:00",
                            ],
                        },
                        "bands": {
                            "attrs": {"dtype": "<U3", "shape": [2]},
                            "data": ["B02", "B03"],
                            "dims": ["bands"],
                        },
                        "x": {
                            "dims": ["x"],
                            "attrs": {"dtype": "float64", "shape": [73]},
                            "data": ListSubSet(
                                [644765.0, 644775.0, 644785.0, 645485.0]
                            ),
                        },
                        "y": {
                            "dims": ["y"],
                            "attrs": {"dtype": "float64", "shape": [92]},
                            "data": ListSubSet(
                                [5675445.0, 5675455.0, 5675465.0, 5676355.0]
                            ),
                        },
                    }
                ),
            }
        )
        data = np.array(result["data"])
        assert data.shape == (3, 2, 73, 92)
        assert_equal(
            data[:1, :2, :3, :4],
            [
                [
                    [[584, 587, 592, 579], [580, 604, 604, 560], [610, 592, 611, 592]],
                    [[588, 593, 572, 560], [565, 574, 580, 566], [576, 572, 590, 568]],
                ]
            ],
        )

    @pytest.mark.parametrize(
        ["load_result_kwargs", "expected"],
        [
            (
                {
                    "spatial_extent": {
                        "west": 5.077,
                        "south": 51.215,
                        "east": 5.080,
                        "north": 51.219,
                    }
                },
                {
                    "dims": ["t", "bands", "x", "y"],
                    "shape": (3, 2, 24, 46),
                    "ts": [
                        "2022-09-07 00:00:00",
                        "2022-09-12 00:00:00",
                        "2022-09-19 00:00:00",
                    ],
                    "bands": ["B02", "B03"],
                    "xs": [645045.0, 645055.0, 645275.0],
                    "ys": [5675785.0, 5675795.0, 5676235.0],
                },
            ),
            (
                {"temporal_extent": ["2022-09-10", "2022-09-15"]},
                {
                    "dims": ["t", "bands", "x", "y"],
                    "shape": (1, 2, 73, 92),
                    "ts": [
                        "2022-09-12 00:00:00",
                    ],
                    "bands": ["B02", "B03"],
                    "xs": [644765.0, 644775.0, 645485.0],
                    "ys": [5675445.0, 5675455.0, 5676355.0],
                },
            ),
            (
                {"bands": ["B03"]},
                {
                    "dims": ["t", "bands", "x", "y"],
                    "shape": (3, 1, 73, 92),
                    "ts": [
                        "2022-09-07 00:00:00",
                        "2022-09-12 00:00:00",
                        "2022-09-19 00:00:00",
                    ],
                    "bands": ["B03"],
                    "xs": [644765.0, 644775.0, 645485.0],
                    "ys": [5675445.0, 5675455.0, 5676355.0],
                },
            ),
        ],
    )
    def test_load_result_url_filtering(
        self,
        api110,
        zk_client,
        zk_job_registry,
        batch_job_output_root,
        urllib_mock,
        load_result_kwargs,
        expected,
    ):
        job_id = "j-ec5d3e778ba5423d8d88a50b08cb9f63"
        results_url = f"https://foobar.test/job/{job_id}/results"

        results_dir = _setup_existing_job(
            job_id=job_id,
            api=api110,
            batch_job_output_root=batch_job_output_root,
            zk_job_registry=zk_job_registry,
        )
        _setup_metadata_request_mocking(
            job_id=job_id,
            api=api110,
            results_dir=results_dir,
            results_url=results_url,
            urllib_mock=urllib_mock,
        )

        process_graph = {
            "lc": {
                "process_id": "load_result",
                "arguments": {"id": results_url, **load_result_kwargs},
            },
            "save": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "lc"}, "format": "json"},
                "result": True,
            },
        }
        response = api110.check_result(process_graph)
        result = response.assert_status_code(200).json

        assert result["dims"] == expected["dims"]
        assert result["attrs"]["shape"] == list(expected["shape"])
        assert result["coords"]["t"]["data"] == expected["ts"]
        assert result["coords"]["bands"]["data"] == expected["bands"]
        assert result["coords"]["x"]["data"] == ListSubSet(expected["xs"])
        assert result["coords"]["y"]["data"] == ListSubSet(expected["ys"])
        data = np.array(result["data"])
        assert data.shape == expected["shape"]


class TestLoadStac:
    def test_stac_api_item_search_bbox_is_epsg_4326(self, api110):
        process_graph = {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {
                    "url": "https://somestacapi/collections/BVL_v1",
                    "spatial_extent": {"west": 4309100, "south": 3014100,
                                       "east": 4309900, "north": 3014900,
                                       "crs": "EPSG:3035"}
                }
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "loadstac1"}, "format": "GTiff"},
                "result": True
            }
        }

        mock_stac_client = MagicMock()
        mock_item_search = mock_stac_client.search.return_value
        mock_item_search.items.return_value = [
            Item(
                id="item",
                geometry={"type": "Polygon", "coordinates": [
                    [[9.831776345947729, 50.23804708435827], [9.831776345947729, 50.24705765771707],
                     [9.845824108087408, 50.24705765771707], [9.845824108087408, 50.23804708435827],
                     [9.831776345947729, 50.23804708435827]]]},
                bbox=[9.831776345947729, 50.23804708435827, 9.845824108087408, 50.24705765771707],
                datetime=dt.datetime(2021, 1, 1, 0, 0, 0, 0, tzinfo=dt.timezone.utc),
                properties={"datetime": "2021-01-01T00:00:00Z",
                            "proj:epsg": 3035,
                            "proj:bbox": [4309000, 3014000, 4310000, 3015000],
                            "proj:shape": [100, 100]},
                assets={"result": Asset(href=f"file:{get_test_data_file('binary/load_stac/BVL_v1/BVL_v1_2021.tif')}",
                                        extra_fields={"eo:bands": [{"name": "class"}]})})]

        with mock.patch("pystac.read_file", return_value=self._mock_stac_api_collection()), mock.patch(
                "pystac_client.Client.open", return_value=mock_stac_client):
            api110.result(process_graph).assert_status_code(200)

        requested_bbox = mock_stac_client.search.call_args.kwargs["bbox"]
        assert requested_bbox == pytest.approx((9.83318136095339, 50.23894821967924,
                                                9.844419570631366, 50.246156678379016))

    def test_stac_collection_multiple_items_no_spatial_extent_specified(self, api110, zk_job_registry,
                                                                        batch_job_output_root, urllib_mock):
        job_id = "j-ec5d3e778ba5423d8d88a50b08cb9f63"
        results_url = f"https://foobar.test/job/{job_id}/results"

        results_dir = _setup_existing_job(
            job_id=job_id,
            api=api110,
            batch_job_output_root=batch_job_output_root,
            zk_job_registry=zk_job_registry,
        )
        _setup_metadata_request_mocking(
            job_id=job_id,
            api=api110,
            results_dir=results_dir,
            results_url=results_url,
            urllib_mock=urllib_mock,
        )

        # sanity check: multiple items
        results = json.loads(urllib.request.urlopen(results_url).read())
        item_links = [link for link in results["links"] if link["rel"] == "item"]
        assert len(item_links) > 1

        process_graph = {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {
                    "url": results_url,
                }
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "loadstac1"}, "format": "netCDF"},
                "result": True
            }
        }

        api110.result(process_graph).assert_status_code(200)

    def test_stac_api_no_spatial_extent_specified(self, api110):
        process_graph = {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {
                    "url": "https://somestacapi/collections/BVL_v1",
                }
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "loadstac1"}, "format": "GTiff"},
                "result": True
            }
        }

        with mock.patch("pystac.read_file", return_value=self._mock_stac_api_collection()), mock.patch(
                "pystac_client.Client.open") as mock_pystac_client_open:
            api110.result(process_graph).assert_error(400, error_code="NoDataAvailable")

        mock_stac_client = mock_pystac_client_open.return_value
        mock_stac_client.search.assert_called_once()

    @staticmethod
    def _mock_stac_api_collection() -> Collection:
        collection = Collection(
            id="BVL_v1",
            description="BVL_v1",
            extent=Extent(spatial=SpatialExtent([[-180, -90, 180, 90]]), temporal=TemporalExtent([[None, None]])),
        )

        root_catalog = Catalog(id="somestacapi", description="somestacapi")
        root_catalog.extra_fields["conformsTo"] = ["https://api.stacspec.org/v1.0.0-rc.1/item-search"]
        collection.set_root(root_catalog)

        return collection


class TestEtlApiReporting:
    @pytest.fixture(autouse=True)
    def _config_overrides(self):
        with config_overrides(
            use_etl_api_on_sync_processing=True,
        ):
            yield

    @pytest.fixture(autouse=True)
    def mock_metadata_tracker(self):
        """
        Fixture to set up mock of metadata tracker in geotrellis extension
        """
        with mock.patch("openeogeotrellis.backend.get_jvm") as get_jvm:
            tracker = get_jvm.return_value.org.openeo.geotrelliscommon.ScopedMetadataTracker.apply.return_value
            tracker.sentinelHubProcessingUnits.return_value = 123
            yield tracker

    @pytest.fixture
    def etl_client_credentials(self) -> ClientCredentials:
        return ClientCredentials(
            oidc_issuer="https://oidc.test",
            client_id="etl-client-123",
            client_secret="etl-secret-abc",
        )

    _ETL_API_ACCESS_TOKEN = "access-token-123"

    @pytest.fixture(autouse=True)
    def etl_api_oidc_issuer(self, requests_mock, etl_client_credentials):
        """Fixture to set up OIDC auth to get access token for ETL API"""
        requests_mock.get(
            "https://oidc.test/.well-known/openid-configuration",
            json={
                "issuer": "https://oidc.test",
                "token_endpoint": "https://oidc.test/token",
            },
        )

        def post_token(request, context):
            params = urllib.parse.parse_qs(request.text)
            assert params == {
                "client_id": [etl_client_credentials.client_id],
                "client_secret": [etl_client_credentials.client_secret],
                "grant_type": ["client_credentials"],
                "scope": ["openid"],
            }
            return {"access_token": self._ETL_API_ACCESS_TOKEN}

        requests_mock.post("https://oidc.test/token", json=post_token)

    @pytest.fixture
    def etl_creds_from_env_triplet(self, etl_client_credentials, monkeypatch):
        """Fixture to set up getting ETL API creds from env vars (triplet style)."""
        monkeypatch.setenv("OPENEO_ETL_API_OIDC_ISSUER", etl_client_credentials.oidc_issuer)
        monkeypatch.setenv("OPENEO_ETL_OIDC_CLIENT_ID", etl_client_credentials.client_id)
        monkeypatch.setenv("OPENEO_ETL_OIDC_CLIENT_SECRET", etl_client_credentials.client_secret)
        yield

    @pytest.fixture
    def etl_creds_from_env_compact(self, etl_client_credentials, monkeypatch):
        """Fixture to set up getting ETL API creds from env var (simple style)."""
        monkeypatch.setenv(
            "OPENEO_ETL_OIDC_CLIENT_CREDENTIALS",
            f"{etl_client_credentials.client_id}:{etl_client_credentials.client_secret}@{etl_client_credentials.oidc_issuer}",
        )
        yield

    @pytest.fixture(autouse=True)
    def mock_etl_api_resources(self, requests_mock):
        """Setup up request mock for ETL API `/resources` endpoint"""
        def post_resources(request, context):
            assert request.headers["Authorization"] == f"Bearer {self._ETL_API_ACCESS_TOKEN}"
            assert request.json() == DictSubSet(
                {
                    "userId": TEST_USER,
                    "metrics": {"processing": {"unit": "shpu", "value": 123}},
                }
            )
            return [{"cost": 33}, {"cost": 55}]

        requests_mock.post("https://etl-api.test/resources", json=post_resources)

    def test_sync_processing_etl_reporting_credentials_env_triplet(self, api100, etl_creds_from_env_triplet):
        """
        Do sync processing with ETL reporting, using env vars code path to get ETL API creds (triplet style)
        """
        res = api100.check_result({"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}})
        assert res.json == 8
        assert res.headers["OpenEO-Costs-experimental"] == "88"

    def test_sync_processing_etl_reporting_credentials_env_compact(self, api100, etl_creds_from_env_compact):
        """
        Do sync processing with ETL reporting, using env vars code path to get ETL API cred (compact style)
        """
        res = api100.check_result({"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}})
        assert res.json == 8
        assert res.headers["OpenEO-Costs-experimental"] == "88"
