from openeo_driver.datacube import DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.save_result import JSONResult, NullResult, VectorCubeResult

from openeogeotrellis.job_results.wrapping import wrap_evaluation_result
from tests.data import get_test_data_file


def test_wrap_plain_dict_result():
    results = wrap_evaluation_result({"a": 1}, job_specification={})
    assert len(results) == 1
    assert isinstance(results[0], JSONResult)


def test_wrap_save_result_passthrough():
    save_result = NullResult()
    results = wrap_evaluation_result(save_result, job_specification={})
    assert results == [save_result]


def test_wrap_list_of_results():
    results = wrap_evaluation_result([NullResult(), NullResult()], job_specification={})
    assert len(results) == 2
    assert all(isinstance(r, NullResult) for r in results)


def test_wrap_driver_vector_cube_sets_batch_mode():
    vector_cube = DriverVectorCube.from_fiona([str(get_test_data_file("geometries/FeatureCollection.geojson"))])
    job_specification = {"output": {"format": "GTiff"}}

    results = wrap_evaluation_result(vector_cube, job_specification=job_specification)

    assert len(results) == 1
    assert isinstance(results[0], VectorCubeResult)
    assert results[0].options["batch_mode"] is True


def test_wrap_delayed_vector():
    delayed_vector = DelayedVector(str(get_test_data_file("geometries/FeatureCollection.geojson")))

    results = wrap_evaluation_result(delayed_vector, job_specification={})

    assert len(results) == 1
    assert isinstance(results[0], JSONResult)
