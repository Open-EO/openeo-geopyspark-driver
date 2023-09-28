from geopyspark import TiledRasterLayer, Extent
from openeo_driver.utils import EvalEnv
from py4j.protocol import Py4JJavaError
from shapely.geometry import MultiPolygon

from openeogeotrellis.backend import GeoPySparkBackendImplementation
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.utils import get_jvm


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


def test_summarize_sentinel1_band_not_present_exception(caplog):
    caplog.set_level("DEBUG")

    jvm = get_jvm()

    http_request = jvm.scalaj.http.Http.apply("https://sentinel-hub.example.org/process")
    response_headers = getattr(getattr(jvm.scala.collection.immutable, "Map$"), "MODULE$").empty()
    sentinel1_band_not_present_exception = jvm.org.openeo.geotrellissentinelhub.Sentinel1BandNotPresentException(
        http_request, None, 400, response_headers, """{"error":{"status":400,"reason":"Bad Request","message":"Requested band 'HH' is not present in Sentinel 1 tile 'S1A_IW_GRDH_1SDV_20170301T050935_20170301T051000_015494_019728_3A01' returned by criteria specified in `dataFilter` parameter.","code":"RENDERER_S1_MISSING_POLARIZATION"}}""")
    spark_exception = jvm.org.apache.spark.SparkException(
        "Job aborted due to stage failure ...", sentinel1_band_not_present_exception)
    py4j_error: Exception = Py4JJavaError(
        msg="An error occurred while calling z:org.openeo.geotrellis.geotiff.package.saveRDD.",
        java_exception=spark_exception)

    error_summary = GeoPySparkBackendImplementation.summarize_exception_static(py4j_error)

    assert error_summary.is_client_error
    assert (error_summary.summary ==
            f"Requested band 'HH' is not present in Sentinel 1 tile;"
            f' try specifying a "polarization" property filter according to the table at'
            f' https://docs.sentinel-hub.com/api/latest/data/sentinel-1-grd/#polarization.')

    assert ("exception chain classes: org.apache.spark.SparkException "
            "caused by org.openeo.geotrellissentinelhub.Sentinel1BandNotPresentException" in caplog.messages)


def test_summarize_sentinel1_band_not_present_exception_workaround_for_root_cause_missing(caplog):
    caplog.set_level("DEBUG")

    jvm = get_jvm()

    # does not have a root cause attached
    spark_exception = jvm.org.apache.spark.SparkException('Job aborted due to stage failure: \nAborting TaskSet 16.0 because task 2 (partition 2)\ncannot run anywhere due to node and executor excludeOnFailure.\nMost recent failure:\nLost task 0.2 in stage 16.0 (TID 2580) (epod049.vgt.vito.be executor 57): org.openeo.geotrellissentinelhub.Sentinel1BandNotPresentException: Sentinel Hub returned an error\nresponse: HTTP/1.1 400 Bad Request with body: {"error":{"status":400,"reason":"Bad Request","message":"Requested band \'HH\' is not present in Sentinel 1 tile \'S1B_IW_GRDH_1SDV_20170302T050053_20170302T050118_004525_007E0D_CBC9\' returned by criteria specified in `dataFilter` parameter.","code":"RENDERER_S1_MISSING_POLARIZATION"}}\nrequest: POST https://services.sentinel-hub.com/api/v1/process with (possibly abbreviated) body: { ...')
    py4j_error: Exception = Py4JJavaError(
        msg="An error occurred while calling z:org.openeo.geotrellis.geotiff.package.saveRDD.",
        java_exception=spark_exception)

    error_summary = GeoPySparkBackendImplementation.summarize_exception_static(py4j_error)

    assert error_summary.is_client_error
    assert (error_summary.summary ==
            f"Requested band 'HH' is not present in Sentinel 1 tile;"
            f' try specifying a "polarization" property filter according to the table at'
            f' https://docs.sentinel-hub.com/api/latest/data/sentinel-1-grd/#polarization.')

    assert ("exception chain classes: org.apache.spark.SparkException" in caplog.messages)
