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

def test_summarize_big_error(caplog):
    caplog.set_level("DEBUG")

    jvm = get_jvm()

    # does not have a root cause attached
    spark_exception = jvm.org.apache.spark.SparkException("""
          Traceback (most recent call last):
  File "/opt/openeo/lib64/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 1375, in <module>
    main(sys.argv)
  File "/opt/openeo/lib64/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 1040, in main
    run_driver()
  File "/opt/openeo/lib64/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 1011, in run_driver
    run_job(
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/utils.py", line 56, in memory_logging_wrapper
    return function(*args, **kwargs)
  File "/opt/openeo/lib64/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 1146, in run_job
    the_assets_metadata = result.write_assets(str(output_file))
  File "/opt/openeo/lib/python3.8/site-packages/openeo_driver/save_result.py", line 150, in write_assets
    return self.cube.write_assets(filename=directory, format=self.format, format_options=self.options)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/geopysparkdatacube.py", line 1823, in write_assets
    outputPaths = get_jvm().org.openeo.geotrellis.geotiff.package.saveRDD(max_level.srdd.rdd(),band_count,str(save_filename),zlevel,get_jvm().scala.Option.apply(crop_extent),gtiff_options)
  File "/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/java_gateway.py", line 1322, in __call__
    return_value = get_return_value(
  File "/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/protocol.py", line 326, in get_return_value
    raise Py4JJavaError(
py4j.protocol.Py4JJavaError: An error occurred while calling z:org.openeo.geotrellis.geotiff.package.saveRDD.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 14.2 failed 4 times, most recent failure: Lost task 0.3 in stage 14.2 (TID 1744) (10.42.141.21 executor 119): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 830, in main
    process()
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 822, in process
    serializer.dump_stream(out_iter, outfile)
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/serializers.py", line 146, in dump_stream
    for obj in iterator:
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/util.py", line 81, in wrapper
    return f(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/utils.py", line 56, in memory_logging_wrapper
    return function(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 44, in wrapper
    return _FUNCTION_POINTERS[key](*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 37, in first_time
    return f(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/geopysparkdatacube.py", line 517, in tile_function
    result_data = run_udf_code(code=udf_code, data=data)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/udf.py", line 20, in run_udf_code
    return openeo.udf.run_udf_code(code=code, data=data)
  File "/opt/openeo/lib/python3.8/site-packages/openeo/udf/run_code.py", line 180, in run_udf_code
    result_cube = func(cube=data.get_datacube_list()[0], context=data.user_context)
  File "<string>", line 282, in apply_datacube
  File "<string>", line 235, in delineate
  File "tmp/venv_model/fielddelineation/utils/delineation.py", line 59, in _apply_delineation
    preds = run_prediction(
  File "tmp/venv_model/vito_lot_delineation/inference/main.py", line 33, in main
    semantic = model.forward_process(inp)
  File "tmp/venv_model/vito_lot_delineation/models/MultiHeadResUnet3D/main.py", line 99, in forward_process
    return self.model(x)
  File "tmp/venv_static/torch/nn/modules/module.py", line 1130, in _call_impl
    return forward_call(*input, **kwargs)
  File "tmp/venv_model/vito_lot_delineation/models/MultiHeadResUnet3D/model/main.py", line 205, in forward
    memory.append(layer(memory[-1]))
  File "tmp/venv_static/torch/nn/modules/module.py", line 1130, in _call_impl
    return forward_call(*input, **kwargs)
  File "tmp/venv_model/vito_lot_delineation/models/MultiHeadResUnet3D/model/layers.py", line 105, in forward
    x = self.down(x)
  File "tmp/venv_static/torch/nn/modules/module.py", line 1130, in _call_impl
    return forward_call(*input, **kwargs)
  File "tmp/venv_model/vito_lot_delineation/models/MultiHeadResUnet3D/model/modules.py", line 367, in forward
    return self.f(x)
  File "tmp/venv_static/torch/nn/modules/module.py", line 1130, in _call_impl
    return forward_call(*input, **kwargs)
  File "tmp/venv_model/vito_lot_delineation/models/MultiHeadResUnet3D/model/modules.py", line 82, in forward
    x = self.conv(x)  # Perform the convolution
  File "tmp/venv_static/torch/nn/modules/module.py", line 1130, in _call_impl
    return forward_call(*input, **kwargs)
  File "tmp/venv_static/torch/nn/modules/conv.py", line 607, in forward
    return self._conv_forward(input, self.weight, self.bias)
  File "tmp/venv_static/torch/nn/modules/conv.py", line 602, in _conv_forward
    return F.conv3d(
RuntimeError: Calculated padded input size per channel: (3 x 66 x 66). Kernel size: (4 x 4 x 4). Kernel size can't be greater than actual input size
""")
    py4j_error: Exception = Py4JJavaError(
        msg="",
        java_exception=spark_exception)

    error_summary = GeoPySparkBackendImplementation.summarize_exception_static(py4j_error)

    assert ("Kernel size can't be greater than actual input size" in error_summary.summary)

def test_summarize_big_error_syntetic(caplog):
    caplog.set_level("DEBUG")

    jvm = get_jvm()

    # does not have a root cause attached
    spark_exception = jvm.org.apache.spark.SparkException("""
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/geopysparkdatacube.py", line 517, in tile_function
    result_data = run_udf_code(code=udf_code, data=data)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/udf.py", line 20, in run_udf_code
    return openeo.udf.run_udf_code(code=code, data=data)
  File "/opt/openeo/lib/python3.8/site-packages/openeo/udf/run_code.py", line 180, in run_udf_code
  """ + ("A" * 3000) + """
  File "<string>", line 235, in delineate
  File "tmp/venv_model/fielddelineation/utils/delineation.py", line 59, in _apply_delineation
    preds = run_prediction(
  File "tmp/venv_model/vito_lot_delineation/inference/main.py", line 33, in main
    semantic = model.forward_process(inp)
  File "tmp/venv_model/vito_lot_delineation/models/MultiHeadResUnet3D/main.py", line 99, in forward_process
    return self.model(x)
  File "tmp/venv_static/torch/nn/modules/module.py", line 1130, in _call_impl
    return forward_call(*input, **kwargs)
  File "tmp/venv_model/vito_lot_delineation/models/MultiHeadResUnet3D/model/main.py", line 205, in forward
    memory.append(layer(memory[-1]))
  File "tmp/venv_static/torch/nn/modules/module.py", line 1130, in _call_impl
    return forward_call(*input, **kwargs)
  File "tmp/venv_model/vito_lot_delineation/models/MultiHeadResUnet3D/model/layers.py", line 105, in forward
    x = self.down(x)
  File "tmp/venv_static/torch/nn/modules/module.py", line 1130, in _call_impl
    return forward_call(*input, **kwargs)
  File "tmp/venv_model/vito_lot_delineation/models/MultiHeadResUnet3D/model/modules.py", line 367, in forward
    return self.f(x)
  File "tmp/venv_static/torch/nn/modules/module.py", line 1130, in _call_impl
    return forward_call(*input, **kwargs)
  File "tmp/venv_model/vito_lot_delineation/models/MultiHeadResUnet3D/model/modules.py", line 82, in forward
    x = self.conv(x)  # Perform the convolution
  File "tmp/venv_static/torch/nn/modules/module.py", line 1130, in _call_impl
    return forward_call(*input, **kwargs)
  File "tmp/venv_static/torch/nn/modules/conv.py", line 607, in forward
    return self._conv_forward(input, self.weight, self.bias)
  File "tmp/venv_static/torch/nn/modules/conv.py", line 602, in _conv_forward
    return F.conv3d(
RuntimeError: Calculated padded input size per channel: (3 x 66 x 66). Kernel size: (4 x 4 x 4). Kernel size can't be greater than actual input size
""")
    py4j_error: Exception = Py4JJavaError(
        msg="",
        java_exception=spark_exception)

    error_summary = GeoPySparkBackendImplementation.summarize_exception_static(py4j_error)

    assert ("Kernel size can't be greater than actual input size" in error_summary.summary)
