from dataclasses import fields

from openeogeotrellis.job_options import JobOptions, K8SOptions
from openeogeotrellis.config import get_backend_config


def test_initialization_with_defaults():
    job_options = JobOptions()
    backend_config = get_backend_config()
    assert job_options.driver_memory == backend_config.default_driver_memory
    assert job_options.executor_memory == backend_config.default_executor_memory
    assert job_options.executor_cores == backend_config.default_executor_cores
    assert job_options.udf_dependency_archives == []


def test_from_dict():
    data = {
        "driver-memory": "16G",
        "executor-memory": "8G",
        "executor-cores": "4",
        "udf-dependency-archives": ["http://example.com/archive1.zip"]
    }
    job_options = JobOptions.from_dict(data)
    assert job_options.driver_memory == "16G"
    assert job_options.executor_memory == "8G"
    assert job_options.executor_cores == "4"
    assert job_options.udf_dependency_archives == ["http://example.com/archive1.zip"]


def test_from_dict_with_missing_values():
    data = {
        "driver-memory": "16G"
    }
    backend_config = get_backend_config()
    job_options = JobOptions.from_dict(data)
    assert job_options.driver_memory == "16G"
    assert job_options.executor_memory == backend_config.default_executor_memory
    assert job_options.executor_cores == backend_config.default_executor_cores


def test_list_options():
    options = JobOptions.list_options(public_only=False)
    assert_listing_shared_options(options)


def assert_listing_shared_options(options):
    assert isinstance(options, list)
    assert any(option["name"] == "driver-memory" for option in options)
    assert any(option["name"] == "executor-memory" for option in options)
    assert any(option["name"] == "udf-dependency-archives" for option in options)
    print(options)
    for opt in options:
        assert "name" in opt
        assert "schema" in opt
        assert "description" in opt
        assert "default" in opt
        if "udf-dependency-archives" == opt["name"]:
            assert opt["schema"] == {"type": "array", "items": {"type": "string"}}


def test_list_options_k8s():
    options = K8SOptions.list_options(public_only=False)
    assert_listing_shared_options(options)
    assert any(option["name"] == "executor-request-cores" for option in options)



def test_list_options_with_public_only():
    options = JobOptions.list_options(public_only=True)
    assert isinstance(options, list)
    # Ensure no private options are included
    assert all(option.get("public", True) for option in options)