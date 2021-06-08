import datetime
from typing import Dict

import numpy as np
from geopyspark import Tile, TiledRasterLayer
from numpy.testing import assert_array_almost_equal

from openeo_driver.utils import EvalEnv
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor


def _simple_reducer(operation: str) -> GeotrellisTileProcessGraphVisitor:
    graph = {
        "sum": {
            "process_id": operation,
            "arguments": {
                "data": {"from_argument": "dimension_data"}
            },
            "result": True
        }
    }
    visitor = GeotrellisTileProcessGraphVisitor()
    return visitor.accept_process_graph(graph)


def _stitch(cube: GeopysparkDataCube) -> Tile:
    return cube.pyramid.levels[0].stitch()


def _timeseries_stitch(cube: GeopysparkDataCube) -> Dict[datetime.datetime, Tile]:
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
    assert len(ts) == 2
    expected = np.full((1, 8, 8), 3.0)
    for t, tile in ts.items():
        assert_array_almost_equal(tile.cells, expected)


def test_reduce_bands_reduce_time(imagecollection_with_two_bands_and_three_dates, udf_noop):
    cube = imagecollection_with_two_bands_and_three_dates
    ts = _timeseries_stitch(cube)
    assert len(ts) == 3
    assert set(t.cells.shape for t in ts.values()) == {(2, 8, 8)}

    reducer = _simple_reducer("sum")
    env = EvalEnv()
    cube = cube.reduce_dimension(dimension="bands", reducer=reducer, env=env)
    ts = _timeseries_stitch(cube)
    assert len(ts) == 2
    assert set(t.cells.shape for t in ts.values()) == {(1, 8, 8)}

    cube = cube.reduce_dimension(dimension='t', reducer=udf_noop, env=env)
    stiched = _stitch(cube)
    assert stiched.cells.shape == (1, 8, 8)
    expected = np.full((1, 8, 8), 3.0)
    assert_array_almost_equal(stiched.cells, expected)
