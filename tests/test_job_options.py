from dataclasses import fields

import pytest

from openeo.util import dict_no_none
from openeogeotrellis.constants import JOB_OPTION_LOGGING_THRESHOLD
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
        "udf-dependency-archives": ["http://example.com/archive1.zip"],
        "log_level": "warning"
    }
    job_options = JobOptions.from_dict(data)
    assert job_options.driver_memory == "16G"
    assert job_options.executor_memory == "8G"
    assert job_options.executor_cores == "4"
    assert job_options.log_level == "WARN"
    assert job_options.udf_dependency_archives == ["http://example.com/archive1.zip"]


@pytest.mark.parametrize(
    "in_python, in_overhead, expected_python, expected_overhead",
    [
        (None, None, -1, "3G"),  # on yarn, no python-memory set => stick to using memoryOverhead?
        ("2G", None, 2147483648, "128m"),  # Custom Python memory
        (None, "1G", -1, "1G"), #respect overhead on yarn
        ("2G", "1G", 2147483648, "1G")      # Custom both
    ])
def test_from_dict_python_memory(in_python, in_overhead, expected_python, expected_overhead):
    data = {
        "python-memory": in_python,
        "executor-memoryOverhead": in_overhead,

    }
    job_options = JobOptions.from_dict(dict_no_none(**data))
    assert job_options.python_memory == expected_python
    assert job_options.executor_memory_overhead == expected_overhead

@pytest.mark.parametrize(
    "in_python, in_overhead, expected_python, expected_overhead",
    [
        (None, None, -1, "3G"),  # on yarn, no python-memory set => stick to using memoryOverhead?
        ("2G", None, 2147483648, "128m"),  # Custom Python memory
        (None, "1G", 939524096, "1G"), # automatically convert memOverhead to python memory on kubernetes
        ("2G", "1G", 2147483648, "1G")      # Custom both
    ])
def test_from_dict_python_memory_kube(in_python, in_overhead, expected_python, expected_overhead):
    data = {
        "python-memory": in_python,
        "executor-memoryOverhead": in_overhead,

    }
    job_options = K8SOptions.from_dict(dict_no_none(**data))
    assert job_options.python_memory == expected_python
    assert job_options.executor_memory_overhead == expected_overhead


def test_from_dict_with_missing_values():
    data = {
        "driver-memory": "16G",
        JOB_OPTION_LOGGING_THRESHOLD: "DEBUG"
    }
    backend_config = get_backend_config()
    job_options = JobOptions.from_dict(data)
    assert job_options.driver_memory == "16G"
    assert job_options.log_level == "DEBUG"
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