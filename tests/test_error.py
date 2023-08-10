from geopyspark import TiledRasterLayer, Extent
from openeo_driver.utils import EvalEnv
from shapely.geometry import MultiPolygon

from openeogeotrellis.backend import GeoPySparkBackendImplementation
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube


# Note: Ensure that the python environment has all the required modules installed.
# Numpy should be installed before Jep for off-heap memory tiles to work!
#
# Note: In order to run these tests you need to set several environment variables.
# If you use the virtual environment venv (with JEP and Numpy installed):
# 1. LD_LIBRARY_PATH = .../venv/lib/python3.6/site-packages/jep
#   This will look for the shared library 'jep.so'. This is the compiled C code that binds Java and Python objects.

def test_chunk_polygon_exception(imagecollection_with_two_bands_and_three_dates):
    udf_code = """
import xarray
from openeo.udf import XarrayDataCube

def function_in_root():
    raise Exception("This error message should be visible to user")

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    function_in_root()
    array = cube.get_array()
    return XarrayDataCube(array)
"""
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
    chunks = MultiPolygon([polygon1])
    cube: GeopysparkDataCube = imagecollection_with_two_bands_and_three_dates
    try:
        result_cube: GeopysparkDataCube = cube.chunk_polygon(udf_add_to_bands, chunks=chunks, mask_value=None, env=env)
        result_layer: TiledRasterLayer = result_cube.pyramid.levels[0]
        result_layer.to_numpy_rdd().collect()
    except Exception as e:
        error_summary = GeoPySparkBackendImplementation.summarize_exception_static(e)
        print(error_summary.summary)
        assert "This error message should be visible to user" in error_summary.summary
    else:
        raise Exception("There should have been an exception raised in the try clause.")
