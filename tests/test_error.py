import textwrap
import urllib
from unittest import mock, skip

import pytest
from geopyspark import Extent, TiledRasterLayer
from py4j.protocol import Py4JJavaError
from shapely.geometry import MultiPolygon

import openeo
from openeo.internal.graph_building import PGNode
from openeo_driver.testing import ApiException
from openeo_driver.utils import EvalEnv
from openeogeotrellis.backend import GeoPySparkBackendImplementation
from openeogeotrellis.configparams import ConfigParams
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
    return cube
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
        # Will run in Jep:
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


def test_summarize_eofexception():
    jvm = get_jvm()
    # This exception has no message. Typically it means OOM
    java_exception = jvm.java.io.EOFException()
    spark_exception = jvm.org.apache.spark.SparkException("Job aborted due to stage failure ...", java_exception)
    py4j_error: Exception = Py4JJavaError(msg="An error occur...", java_exception=spark_exception)
    error_summary = GeoPySparkBackendImplementation.summarize_exception_static(py4j_error).summary

    assert "null" not in error_summary
    assert "None" not in error_summary
    assert "try submitting again" in error_summary


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


@mock.patch.object(ConfigParams, "is_kube_deploy", new_callable=mock.PropertyMock)
def test_summarize_kubernetes_client_exceptions_ApiException(mock_config_use_object_storage):
    import kubernetes.client.exceptions

    mock_config_use_object_storage.return_value = True
    exception = kubernetes.client.exceptions.ApiException(status=401, reason="Unauthorized")
    error_summary = GeoPySparkBackendImplementation.summarize_exception_static(exception)

    assert "Unauthorized" in error_summary.summary


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


def test_summarize_bad_alloc(caplog):
    caplog.set_level("DEBUG")

    jvm = get_jvm()

    # does not have a root cause attached
    spark_exception = jvm.org.apache.spark.SparkException(
        """Traceback (most recent call last):
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 790, in start_main
    main(sys.argv)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 246, in main
    run_driver()
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 207, in run_driver
    run_job(
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/utils.py", line 64, in memory_logging_wrapper
    return function(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 395, in run_job
    assets_metadata = list(map(result_write_assets, results))
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 391, in result_write_assets
    return result_arg.write_assets(str(output_file))
  File "/opt/openeo/lib/python3.8/site-packages/openeo_driver/save_result.py", line 170, in write_assets
    return self.cube.write_assets(filename=directory, format=self.format, format_options=self.options)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/geopysparkdatacube.py", line 2195, in write_assets
    asset_paths = get_jvm().org.openeo.geotrellis.netcdf.NetCDFRDDWriter.writeRasters(
  File "/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/java_gateway.py", line 1322, in __call__
    return_value = get_return_value(
  File "/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/protocol.py", line 326, in get_return_value
    raise Py4JJavaError(
py4j.protocol.Py4JJavaError: An error occurred while calling z:org.openeo.geotrellis.netcdf.NetCDFRDDWriter.writeRasters.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 64 in stage 16.0 failed 4 times, most recent failure: Lost task 64.3 in stage 16.0 (TID 391) (10.42.120.241 executor 4): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 1247, in main
    process()
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 1239, in process
    serializer.dump_stream(out_iter, outfile)
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/serializers.py", line 146, in dump_stream
    for obj in iterator:
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/util.py", line 83, in wrapper
    return f(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/utils.py", line 64, in memory_logging_wrapper
    return function(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 44, in wrapper
    return _FUNCTION_POINTERS[key](*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 37, in first_time
    return f(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/geopysparkdatacube.py", line 571, in tile_function
    result_data = run_udf_code(code=udf_code, data=data)
  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 44, in wrapper
    return _FUNCTION_POINTERS[key](*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 37, in first_time
    return f(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/udf.py", line 67, in run_udf_code
    return openeo.udf.run_udf_code(code=code, data=data)
  File "/opt/openeo/lib/python3.8/site-packages/openeo/udf/run_code.py", line 195, in run_udf_code
    result_cube: xarray.DataArray = func(cube=data.get_datacube_list()[0].get_array(), context=data.user_context)
  File "<string>", line 160, in apply_datacube
  File "<string>", line 54, in process_window_onnx
  File "<string>", line 29, in load_ort_sessions
  File "<string>", line 30, in <listcomp>
  File "onnx_deps/onnxruntime/capi/onnxruntime_inference_collection.py", line 360, in __init__
    self._create_inference_session(providers, provider_options, disabled_optimizers)
  File "onnx_deps/onnxruntime/capi/onnxruntime_inference_collection.py", line 408, in _create_inference_session
    sess.initialize_session(providers, provider_options, disabled_optimizers)
onnxruntime.capi.onnxruntime_pybind11_state.RuntimeException: [ONNXRuntimeError] : 6 : RUNTIME_EXCEPTION : Exception during initialization: std::bad_alloc
"""
    )
    py4j_error: Exception = Py4JJavaError(msg="", java_exception=spark_exception)

    error_summary = GeoPySparkBackendImplementation.summarize_exception_static(py4j_error)

    assert "python-memory" in error_summary.summary  # Check if instructions are displayed


def test_summarize_big_error_syntetic(caplog):
    caplog.set_level("DEBUG")

    jvm = get_jvm()

    # does not have a root cause attached
    spark_exception = jvm.org.apache.spark.SparkException(
        """
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/geopysparkdatacube.py", line 517, in tile_function
    result_data = run_udf_code(code=udf_code, data=data)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/udf.py", line 20, in run_udf_code
    return openeo.udf.run_udf_code(code=code, data=data)
  File "/opt/openeo/lib/python3.8/site-packages/openeo/udf/run_code.py", line 180, in run_udf_code
  File "<string>", line 235, in delineate
  """
        + ("AAA" * 1000)
        + """
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
"""
    )
    py4j_error: Exception = Py4JJavaError(msg="", java_exception=spark_exception)

    error_summary = GeoPySparkBackendImplementation.summarize_exception_static(py4j_error)

    assert "AAAAAAAAAAAAA..." in error_summary.summary


def test_extract_udf_stacktrace_standard_exception():
    summarized = GeoPySparkBackendImplementation.extract_udf_stacktrace(
        """
    Traceback (most recent call last):
 File "/opt/spark3_2_0/python/lib/pyspark.zip/pyspark/worker.py", line 619, in main
 process()
 File "/opt/spark3_2_0/python/lib/pyspark.zip/pyspark/worker.py", line 611, in process
 serializer.dump_stream(out_iter, outfile)
 File "/opt/spark3_2_0/python/lib/pyspark.zip/pyspark/serializers.py", line 132, in dump_stream
 for obj in iterator:
 File "/opt/spark3_2_0/python/lib/pyspark.zip/pyspark/util.py", line 74, in wrapper
 return f(*args, **kwargs)
 File "/opt/venv/lib64/python3.8/site-packages/openeogeotrellis/utils.py", line 52, in memory_logging_wrapper
 return function(*args, **kwargs)
 File "/opt/venv/lib64/python3.8/site-packages/epsel.py", line 44, in wrapper
 return _FUNCTION_POINTERS[key](*args, **kwargs)
 File "/opt/venv/lib64/python3.8/site-packages/epsel.py", line 37, in first_time
 return f(*args, **kwargs)
 File "/opt/venv/lib64/python3.8/site-packages/openeogeotrellis/geopysparkdatacube.py", line 701, in tile_function
 result_data = run_udf_code(code=udf_code, data=data)
 File "/opt/venv/lib64/python3.8/site-packages/openeo/udf/run_code.py", line 180, in run_udf_code
 func(data)
 File "<string>", line 8, in transform
 File "<string>", line 7, in function_in_transform
 File "<string>", line 4, in function_in_root
Exception: This error message should be visible to user
"""
    )
    assert (
        summarized
        == """ File "<string>", line 8, in transform
 File "<string>", line 7, in function_in_transform
 File "<string>", line 4, in function_in_root
Exception: This error message should be visible to user"""
    )


def test_extract_udf_stacktrace_excludeonfailure():
    summarized = GeoPySparkBackendImplementation.extract_udf_stacktrace(
        """
Traceback (most recent call last):
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 805, in start_main
    main(sys.argv)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 246, in main
    run_driver()
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 207, in run_driver
    run_job(
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/utils.py", line 66, in memory_logging_wrapper
    return function(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 396, in run_job
    assets_metadata = list(map(result_write_assets, results))
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py", line 392, in result_write_assets
    return result_arg.write_assets(str(output_file))
  File "/opt/openeo/lib/python3.8/site-packages/openeo_driver/save_result.py", line 170, in write_assets
    return self.cube.write_assets(filename=directory, format=self.format, format_options=self.options)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/geopysparkdatacube.py", line 2115, in write_assets
    get_jvm().org.openeo.geotrellis.geotiff.package.saveRDDTemporalAllowAssetPerBand(
  File "/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/java_gateway.py", line 1322, in __call__
    return_value = get_return_value(
  File "/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/protocol.py", line 326, in get_return_value
    raise Py4JJavaError(
py4j.protocol.Py4JJavaError: An error occurred while calling z:org.openeo.geotrellis.geotiff.package.saveRDDTemporalAllowAssetPerBand.
: org.apache.spark.SparkException: Job aborted due to stage failure: 
Aborting TaskSet 5.0 because task 0 (partition 0)
cannot run anywhere due to node and executor excludeOnFailure.
Most recent failure:
Lost task 1.2 in stage 5.0 (TID 13) (10.42.158.11 executor 3): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 1247, in main
    process()
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 1239, in process
    serializer.dump_stream(out_iter, outfile)
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/serializers.py", line 146, in dump_stream
    for obj in iterator:
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/util.py", line 83, in wrapper
    return f(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/utils.py", line 66, in memory_logging_wrapper
    return function(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 44, in wrapper
    return _FUNCTION_POINTERS[key](*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 37, in first_time
    return f(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/geopysparkdatacube.py", line 805, in tile_function
    result_data = run_udf_code(code=udf_code, data=data)
  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 44, in wrapper
    return _FUNCTION_POINTERS[key](*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 37, in first_time
    return f(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/udf.py", line 67, in run_udf_code
    return openeo.udf.run_udf_code(code=code, data=data)
  File "/opt/openeo/lib/python3.8/site-packages/openeo/udf/run_code.py", line 180, in run_udf_code
    result_cube = func(cube=data.get_datacube_list()[0], context=data.user_context)
  File "<string>", line 3, in apply_datacube
  File "<string>", line 3, in <listcomp>
MemoryError

	at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:572)
	at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:784)
	at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:766)
	at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:525)
	at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
	at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
	at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:513)
	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:491)
	at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:491)
	at org.apache.spark.storage.memory.MemoryStore.putIterator(MemoryStore.scala:223)
	at org.apache.spark.storage.memory.MemoryStore.putIteratorAsValues(MemoryStore.scala:302)
	at org.apache.spark.storage.BlockManager.$anonfun$doPutIterator$1(BlockManager.scala:1597)
	at org.apache.spark.storage.BlockManager.org$apache$spark$storage$BlockManager$$doPut(BlockManager.scala:1524)
	at org.apache.spark.storage.BlockManager.doPutIterator(BlockManager.scala:1588)
	at org.apache.spark.storage.BlockManager.getOrElseUpdate(BlockManager.scala:1389)
	at org.apache.spark.storage.BlockManager.getOrElseUpdateRDDBlock(BlockManager.scala:1343)
	at org.apache.spark.rdd.RDD.getOrCompute(RDD.scala:379)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
	at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:104)
	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:54)
	at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
	at org.apache.spark.scheduler.Task.run(Task.scala:141)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
	at java.base/java.lang.Thread.run(Thread.java:829)


ExcludeOnFailure behavior can be configured via spark.excludeOnFailure.*.

	at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2856)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2792)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2791)
	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2791)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1247)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1247)
	at scala.Option.foreach(Option.scala:407)
	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1247)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:3060)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2994)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2983)
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:989)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2393)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2414)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2433)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2458)
	at org.apache.spark.rdd.RDD.$anonfun$collect$1(RDD.scala:1049)
	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
	at org.apache.spark.rdd.RDD.withScope(RDD.scala:410)
	at org.apache.spark.rdd.RDD.collect(RDD.scala:1048)
	at org.openeo.geotrellis.geotiff.package$.saveRDDTemporalAllowAssetPerBand(package.scala:254)
	at org.openeo.geotrellis.geotiff.package.saveRDDTemporalAllowAssetPerBand(package.scala)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.base/java.lang.reflect.Method.invoke(Method.java:566)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
	at py4j.Gateway.invoke(Gateway.java:282)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
	at py4j.ClientServerConnection.run(ClientServerConnection.java:106)
	at java.base/java.lang.Thread.run(Thread.java:829)
"""
    )
    assert (
            summarized
            == """  File "<string>", line 3, in apply_datacube
  File "<string>", line 3, in <listcomp>
MemoryError"""
    )


def test_extract_udf_stacktrace_inspect():
    summarized = GeoPySparkBackendImplementation.extract_udf_stacktrace(
        """Traceback (most recent call last):
  File "/opt/spark3_2_0/python/lib/pyspark.zip/pyspark/worker.py", line 619, in main
    process()
  File "/opt/spark3_2_0/python/lib/pyspark.zip/pyspark/worker.py", line 611, in process
    serializer.dump_stream(out_iter, outfile)
  File "/opt/spark3_2_0/python/lib/pyspark.zip/pyspark/serializers.py", line 132, in dump_stream
    for obj in iterator:
  File "/opt/spark3_2_0/python/lib/pyspark.zip/pyspark/util.py", line 74, in wrapper
    return f(*args, **kwargs)
  File "/opt/venv/lib64/python3.8/site-packages/openeogeotrellis/utils.py", line 49, in memory_logging_wrapper
    return function(*args, **kwargs)
  File "/opt/venv/lib64/python3.8/site-packages/epsel.py", line 44, in wrapper
    return _FUNCTION_POINTERS[key](*args, **kwargs)
  File "/opt/venv/lib64/python3.8/site-packages/epsel.py", line 37, in first_time
    return f(*args, **kwargs)
  File "/opt/venv/lib64/python3.8/site-packages/openeogeotrellis/geopysparkdatacube.py", line 519, in tile_function
    result_data = run_udf_code(code=udf_code, data=data)
  File "/opt/venv/lib64/python3.8/site-packages/openeo/udf/run_code.py", line 175, in run_udf_code
    result_cube = func(data.get_datacube_list()[0], data.user_context)
  File "<string>", line 156, in apply_datacube
TypeError: inspect() got multiple values for argument 'data'
"""
    )
    assert (
        summarized
        == """  File "<string>", line 156, in apply_datacube
TypeError: inspect() got multiple values for argument 'data'"""
    )


def test_extract_udf_stacktrace_standard_exception_api100():
    message = """Traceback (most recent call last):
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 1247, in main
    process()
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 1239, in process
    serializer.dump_stream(out_iter, outfile)
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 146, in dump_stream
    for obj in iterator:
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/pyspark/python/lib/pyspark.zip/pyspark/util.py", line 83, in wrapper
    return f(*args, **kwargs)
  File "/home/***/openeo/openeo-geopyspark-driver/openeogeotrellis/utils.py", line 64, in memory_logging_wrapper
    return function(*args, **kwargs)
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/epsel.py", line 44, in wrapper
    return _FUNCTION_POINTERS[key](*args, **kwargs)
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/epsel.py", line 37, in first_time
    return f(*args, **kwargs)
  File "/home/***/openeo/openeo-geopyspark-driver/openeogeotrellis/geopysparkdatacube.py", line 789, in tile_function
    result_data = run_udf_code(code=udf_code, data=data)
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/epsel.py", line 44, in wrapper
    return _FUNCTION_POINTERS[key](*args, **kwargs)
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/epsel.py", line 37, in first_time
    return f(*args, **kwargs)
  File "/home/***/openeo/openeo-geopyspark-driver/openeogeotrellis/udf.py", line 65, in run_udf_code
    return openeo.udf.run_udf_code(code=code, data=data)
  File "/home/***/openeo/openeo-python-client/openeo/udf/run_code.py", line 180, in run_udf_code
    result_cube = func(cube=data.get_datacube_list()[0], context=data.user_context)
  File "<string>", line 11, in apply_datacube
Exception: Test exception

"""
    udf_stacktrace = GeoPySparkBackendImplementation.extract_udf_stacktrace(message)
    assert (
        udf_stacktrace
        == """  File "<string>", line 11, in apply_datacube
Exception: Test exception"""
    )


def test_extract_udf_stacktrace_without_user_trace():
    stacktrace = """Traceback (most recent call last):
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 1247, in main
    process()
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 1239, in process
    serializer.dump_stream(out_iter, outfile)
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 146, in dump_stream
    for obj in iterator:
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/pyspark/python/lib/pyspark.zip/pyspark/util.py", line 83, in wrapper
    return f(*args, **kwargs)
  File "/home/***/openeo/openeo-geopyspark-driver/openeogeotrellis/utils.py", line 64, in memory_logging_wrapper
    return function(*args, **kwargs)
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/epsel.py", line 44, in wrapper
    return _FUNCTION_POINTERS[key](*args, **kwargs)
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/epsel.py", line 37, in first_time
    return f(*args, **kwargs)
  File "/home/***/openeo/openeo-geopyspark-driver/openeogeotrellis/geopysparkdatacube.py", line 789, in tile_function
    result_data = run_udf_code(code=udf_code, data=data)
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/epsel.py", line 44, in wrapper
    return _FUNCTION_POINTERS[key](*args, **kwargs)
  File "/home/***/openeo/venv_python3_8/lib/python3.8/site-packages/epsel.py", line 37, in first_time
    return f(*args, **kwargs)
  File "/home/***/openeo/openeo-geopyspark-driver/openeogeotrellis/udf.py", line 65, in run_udf_code
    return openeo.udf.run_udf_code(code=code, data=data)
  File "/home/***/openeo/openeo-python-client/openeo/udf/run_code.py", line 235, in run_udf_code
    raise OpenEoUdfException(
openeo.udf.OpenEoUdfException: No UDF found.
Multiline test.
"""
    assert GeoPySparkBackendImplementation.extract_udf_stacktrace(stacktrace) is None
    assert (
        GeoPySparkBackendImplementation.extract_python_error(stacktrace)
        == """openeo.udf.OpenEoUdfException: No UDF found.
Multiline test."""
    )


def test_extract_udf_stacktrace_no_udf():
    stacktrace = """Traceback (most recent call last):
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 619, in main
    process()
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 611, in process
    serializer.dump_stream(out_iter, outfile)
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/serializers.py", line 132, in dump_stream
    for obj in iterator:
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/util.py", line 74, in wrapper
    return f(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 44, in wrapper
    return _FUNCTION_POINTERS[key](*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 37, in first_time
    return f(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/openeo/util.py", line 362, in wrapper
    return f(*args, **kwargs)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/collections/s1backscatter_orfeo.py", line 794, in process_product
    dem_dir_context = S1BackscatterOrfeo._get_dem_dir_context(
  File "/opt/openeo/lib64/python3.8/site-packages/openeogeotrellis/collections/s1backscatter_orfeo.py", line 258, in _get_dem_dir_context
    dem_dir_context = S1BackscatterOrfeo._creodias_dem_subset_srtm_hgt_unzip(
  File "/opt/openeo/lib64/python3.8/site-packages/openeogeotrellis/collections/s1backscatter_orfeo.py", line 664, in _creodias_dem_subset_srtm_hgt_unzip
    with zipfile.ZipFile(zip_filename, 'r') as z:
  File "/usr/lib64/python3.8/zipfile.py", line 1251, in __init__
    self.fp = io.open(file, filemode)
FileNotFoundError: [Errno 2] No such file or directory: '/eodata/auxdata/SRTMGL1/dem/N64E024.SRTMGL1.hgt.zip'
"""
    assert GeoPySparkBackendImplementation.extract_udf_stacktrace(stacktrace) is None
    python_error_str = GeoPySparkBackendImplementation.extract_python_error(stacktrace)
    assert (
        python_error_str
        == "FileNotFoundError: [Errno 2] No such file or directory: '/eodata/auxdata/SRTMGL1/dem/N64E024.SRTMGL1.hgt.zip'"
    )


def test_extract_udf_stacktrace_so():
    # Was launched with "python-memory": "600m" and was OOM
    stacktrace = """Traceback (most recent call last):
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 1227, in main
    func, profiler, deserializer, serializer = read_command(pickleSer, infile)
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 90, in read_command
    command = serializer._read_with_length(file)
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/serializers.py", line 174, in _read_with_length
    return self.loads(obj)
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/serializers.py", line 472, in loads
    return cloudpickle.loads(obj, encoding=encoding)
  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/collections/sentinel3.py", line 19, in <module>
    from scipy.spatial import cKDTree  # used for tuning the griddata interpolation settings
  File "/opt/openeo/lib/python3.8/site-packages/scipy/spatial/__init__.py", line 104, in <module>
    from ._qhull import *
ImportError: libopenblasp-r0-8b9e111f.3.17.so: failed to map segment from shared object
"""
    assert GeoPySparkBackendImplementation.extract_udf_stacktrace(stacktrace) is None
    python_error_str = GeoPySparkBackendImplementation.extract_python_error(stacktrace)
    assert python_error_str == "ImportError: libopenblasp-r0-8b9e111f.3.17.so: failed to map segment from shared object"


def test_extract_udf_stacktrace_tensorflow_oom():
    stacktrace = """Traceback (most recent call last):
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/tensorflow/python/pywrap_tensorflow.py", line 60, in <module>
    from tensorflow.python._pywrap_tensorflow_internal import *
ImportError: /home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/tensorflow/python/_pywrap_tensorflow_internal.so: failed to map segment from shared object

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 1247, in main
    process()
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 1239, in process
    serializer.dump_stream(out_iter, outfile)
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/pyspark/python/lib/pyspark.zip/pyspark/serializers.py", line 146, in dump_stream
    for obj in iterator:
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/pyspark/python/lib/pyspark.zip/pyspark/util.py", line 83, in wrapper
    return f(*args, **kwargs)
  File "/home/pakske-friet/openeo/openeo-geopyspark-driver/openeogeotrellis/utils.py", line 64, in memory_logging_wrapper
    return function(*args, **kwargs)
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/epsel.py", line 44, in wrapper
    return _FUNCTION_POINTERS[key](*args, **kwargs)
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/epsel.py", line 37, in first_time
    return f(*args, **kwargs)
  File "/home/pakske-friet/openeo/openeo-geopyspark-driver/openeogeotrellis/geopysparkdatacube.py", line 789, in tile_function
    result_data = run_udf_code(code=udf_code, data=data)
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/epsel.py", line 44, in wrapper
    return _FUNCTION_POINTERS[key](*args, **kwargs)
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/epsel.py", line 37, in first_time
    return f(*args, **kwargs)
  File "/home/pakske-friet/openeo/openeo-geopyspark-driver/openeogeotrellis/udf.py", line 65, in run_udf_code
    return openeo.udf.run_udf_code(code=code, data=data)
  File "/home/pakske-friet/openeo/openeo-python-client/openeo/udf/run_code.py", line 149, in run_udf_code
    module = load_module_from_string(code)
  File "/home/pakske-friet/openeo/openeo-python-client/openeo/udf/run_code.py", line 61, in load_module_from_string
    exec(code, globals)
  File "<string>", line 7, in <module>
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/tensorflow/__init__.py", line 37, in <module>
    from tensorflow.python.tools import module_util as _module_util
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/tensorflow/python/__init__.py", line 36, in <module>
    from tensorflow.python import pywrap_tensorflow as _pywrap_tensorflow
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/tensorflow/python/pywrap_tensorflow.py", line 75, in <module>
    raise ImportError(
ImportError: Traceback (most recent call last):
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/tensorflow/python/pywrap_tensorflow.py", line 60, in <module>
    from tensorflow.python._pywrap_tensorflow_internal import *
ImportError: /home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/tensorflow/python/_pywrap_tensorflow_internal.so: failed to map segment from shared object


Failed to load the native TensorFlow runtime.
See https://www.tensorflow.org/install/errors for some common causes and solutions.
If you need help, create an issue at https://github.com/tensorflow/tensorflow/issues and include the entire stack trace above this error message.
"""

    udf_stacktrace = GeoPySparkBackendImplementation.extract_udf_stacktrace(stacktrace)
    assert (
        udf_stacktrace
        == """  File "<string>", line 7, in <module>
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/tensorflow/__init__.py", line 37, in <module>
    from tensorflow.python.tools import module_util as _module_util
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/tensorflow/python/__init__.py", line 36, in <module>
    from tensorflow.python import pywrap_tensorflow as _pywrap_tensorflow
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/tensorflow/python/pywrap_tensorflow.py", line 75, in <module>
    raise ImportError(
ImportError: Traceback (most recent call last):
  File "/home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/tensorflow/python/pywrap_tensorflow.py", line 60, in <module>
    from tensorflow.python._pywrap_tensorflow_internal import *
ImportError: /home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/tensorflow/python/_pywrap_tensorflow_internal.so: failed to map segment from shared object


Failed to load the native TensorFlow runtime.
See https://www.tensorflow.org/install/errors for some common causes and solutions.
If you need help, create an issue at https://github.com/tensorflow/tensorflow/issues and include the entire stack trace above this error message."""
    )
    python_error_str = GeoPySparkBackendImplementation.extract_python_error(stacktrace)
    assert (
        python_error_str
        == "ImportError: /home/pakske-friet/openeo/venv_python3_8/lib/python3.8/site-packages/tensorflow/python/_pywrap_tensorflow_internal.so: failed to map segment from shared object"
    )


def test_empty_assert_message():
    with pytest.raises(AssertionError) as e_info:
        from openeogeotrellis.collections.testing import load_test_collection
        from openeogeotrellis.geopysparkcubemetadata import GeopysparkCubeMetadata

        # triggering an assert error directly here gives a default error message.
        # This nested assert gives an empty message:
        load_test_collection(4, GeopysparkCubeMetadata({}), None, "EPSG:INVALID", "", "")

    msg = GeoPySparkBackendImplementation.summarize_exception_static(e_info.value).summary
    assert 'srs == "EPSG:4326"' in str(msg)  # assert message should be in the summary


def test_HTTPError_404(urllib_mock):
    fake_url = "http://a.test/404"
    urllib_mock.get(fake_url, data="404", code=404)
    from urllib.error import HTTPError

    with pytest.raises(HTTPError) as e_info:
        urllib.request.urlopen(fake_url)
    msg = GeoPySparkBackendImplementation.summarize_exception_static(e_info.value).summary
    assert fake_url in str(msg)


@pytest.fixture
def datacube() -> openeo.DataCube:
    return openeo.DataCube.load_collection(
        "TestCollection-LonLat4x4",
        temporal_extent=["2021-01-05", "2021-01-06"],
        spatial_extent={"west": 0, "south": 0, "east": 8, "north": 5},
        bands=["Longitude"],
        fetch_metadata=False,
    )


@skip("Disabled because it is flaky on CI")
def test_udf_with_oom(datacube, api100):
    udf_code = textwrap.dedent(
        """
        def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
            [' ' * 999999999 for x in range(9999999999)] # trigger OOM
            return cube
        """
    ).rstrip()
    datacube = datacube.apply(
        PGNode(
            process_id="run_udf",
            data={"from_parameter": "data"},
            udf=udf_code,
            runtime="Python",
        ),
    )
    with pytest.raises(ApiException) as e_info:
        api100.check_result(datacube)
    msg = e_info.value.args[0]
    assert "in <listcomp>" in str(msg)  # this OOM should have a stacktrace.
    assert "out of memory" in str(msg)
    assert "python-memory" in str(msg)  # Check if instructions are displayed
