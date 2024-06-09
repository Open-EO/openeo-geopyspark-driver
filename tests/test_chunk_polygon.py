import datetime
from typing import List, Union
from geopyspark import Tile, TiledRasterLayer, Extent, LayerType
from openeo_driver.utils import EvalEnv
import numpy as np
from shapely.geometry import Polygon, MultiPolygon

from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeo_driver.datacube import DriverVectorCube
from .data import get_test_data_file, TEST_DATA_ROOT


# Note: Ensure that the python environment has all the required modules installed.
# Numpy should be installed before Jep for off-heap memory tiles to work!
#
# Note: In order to run these tests you need to set several environment variables.
# If you use the virtual environment venv (with JEP and Numpy installed):
# 1. LD_LIBRARY_PATH = .../venv/lib/python3.8/site-packages/jep
#   This will look for the shared library 'jep.so'. This is the compiled C code that binds Java and Python objects.

def test_chunk_polygon(imagecollection_with_two_bands_and_three_dates):
    file_name = get_test_data_file("udf_add_to_bands.py")
    with open(file_name, "r") as f:
        udf_code = f.read()
    udf_add_to_bands = {
        "udf_process": {
            "process_id": "run_udf",
            "arguments": {
                "data": {
                    "from_parameter": "data"
                },
                "udf": udf_code
            },
            "result": True
        },
    }
    env = EvalEnv()

    polygon1 = Extent(0.0, 0.0, 4.0, 4.0).to_polygon
    chunks = DriverVectorCube.from_geometry(polygon1)
    cube: GeopysparkDataCube = imagecollection_with_two_bands_and_three_dates
    result_cube: GeopysparkDataCube = cube.chunk_polygon(udf_add_to_bands, chunks=chunks, mask_value=None, env=env)
    result_layer: TiledRasterLayer = result_cube.pyramid.levels[0]

    assert result_layer.layer_type == LayerType.SPACETIME

    results_numpy = result_layer.to_numpy_rdd().collect()
    band0_month10 = np.zeros((4, 4))
    band1_month10 = np.zeros((4, 4))
    band0_month10.fill(1012)
    band1_month10.fill(1101)
    for key_and_tile in results_numpy:
        instant: datetime.datetime = key_and_tile[0].instant
        tile: Tile = key_and_tile[1]
        cells: np.ndarray = tile.cells
        assert cells.shape == (2, 4, 4)
        assert tile.cell_type == 'FLOAT'
        if instant.month == 10:
            np.testing.assert_array_equal(cells, np.array([band0_month10, band1_month10]))
        elif instant.month == 9 and instant.day == 25:
            np.testing.assert_array_equal(cells, np.array([band0_month10 - 1, band1_month10 + 1]))
