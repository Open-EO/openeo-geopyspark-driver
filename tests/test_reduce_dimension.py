import datetime as dt
from typing import Dict

import numpy as np
import pytest
from geopyspark import Tile, TiledRasterLayer
from numpy.testing import assert_array_almost_equal

from openeo_driver.utils import EvalEnv
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.processgraphvisiting import GeotrellisTileProcessGraphVisitor


def _simple_reducer(operation: str) -> GeotrellisTileProcessGraphVisitor:
    graph = {
        "sum": {
            "process_id": operation,
            "arguments": {
                "data": {"from_parameter": "data"}
            },
            "result": True
        }
    }
    visitor = GeotrellisTileProcessGraphVisitor()
    return visitor.accept_process_graph(graph)


def _stitch(cube: GeopysparkDataCube) -> Tile:
    return cube.pyramid.levels[0].stitch()


def _timeseries_stitch(cube: GeopysparkDataCube) -> Dict[dt.datetime, Tile]:
    """Stitch each instant of the SPACETIME TiledRasterLayer of given cube."""
    layer: TiledRasterLayer = cube.pyramid.levels[0]
    keys = layer.collect_keys()
    instants = sorted(set(k.instant for k in keys))
    return {t: layer.to_spatial_layer(target_time=t).stitch() for t in instants}


def test_reduce_bands(imagecollection_with_two_bands_and_three_dates):
    cube = imagecollection_with_two_bands_and_three_dates
    ts = _timeseries_stitch(cube)
    assert len(ts) == 3
    assert set(t.cells.shape for t in ts.values()) == {(2, 8, 8)}

    reducer = _simple_reducer("sum")
    env = EvalEnv()
    cube = cube.reduce_dimension(dimension="bands", reducer=reducer, env=env)
    ts = _timeseries_stitch(cube)
    assert len(ts) == 3
    assert_array_almost_equal(ts[dt.datetime(2017, 9, 25, 11, 37, 0)].cells, np.full((1, 8, 8), 3.0))
    assert_array_almost_equal(ts[dt.datetime(2017, 9, 30, 0, 37, 0)].cells, np.full((1, 8, 8), np.nan))
    assert_array_almost_equal(ts[dt.datetime(2017, 9, 25, 11, 37, 0)].cells, np.full((1, 8, 8), 3.0))


@pytest.mark.parametrize("udf", [("udf_noop"), ("udf_noop_jep")])
def test_reduce_bands_reduce_time(imagecollection_with_two_bands_and_three_dates, udf, request):
    udf = request.getfixturevalue(udf)
    cube = imagecollection_with_two_bands_and_three_dates
    ts = _timeseries_stitch(cube)
    assert len(ts) == 3
    assert set(t.cells.shape for t in ts.values()) == {(2, 8, 8)}

    reducer = _simple_reducer("sum")
    env = EvalEnv()
    cube = cube.reduce_dimension(dimension="bands", reducer=reducer, env=env)
    ts = _timeseries_stitch(cube)
    assert len(ts) == 3
    assert set(t.cells.shape for t in ts.values()) == {(1, 8, 8)}

    cube = cube.reduce_dimension(dimension='t', reducer=udf, env=env)
    stiched = _stitch(cube)
    assert stiched.cells.shape == (1, 8, 8)
    expected = np.full((1, 8, 8), 3.0)
    assert_array_almost_equal(stiched.cells, expected)


@pytest.mark.parametrize("udf", [("udf_noop"), ("udf_noop_jep")])
def test_reduce_bands_udf(imagecollection_with_two_bands_and_three_dates, udf, request):
    udf = request.getfixturevalue(udf)
    the_date = dt.datetime(2017, 9, 25, 11, 37)
    cube = imagecollection_with_two_bands_and_three_dates
    input = imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    result = imagecollection_with_two_bands_and_three_dates.reduce_dimension(
        reducer=udf, dimension="bands", env=EvalEnv()
    )
    input_xarray = imagecollection_with_two_bands_and_three_dates._to_xarray()
    result_xarray = result._to_xarray()
    assert(len(input_xarray.shape) == 4)
    assert(len(result_xarray.shape) == 3)

    result_array = result.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    subresult = result_array[:input.shape[0], :input.shape[1], :input.shape[2]]
    assert_array_almost_equal(input, subresult)
