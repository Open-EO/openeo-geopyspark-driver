import os

import pytest
from mock import mock
from openeo_driver.constants import JOB_STATUS

from openeo_driver.users import User

from openeogeotrellis.backend import GeoPySparkBackendImplementation
from openeogeotrellis.job_registry import InMemoryJobRegistry
from openeogeotrellis.testing import gps_config_overrides


@pytest.fixture()
def job_registry() -> InMemoryJobRegistry:
    return InMemoryJobRegistry()  # TODO: replace with EagerlyK8sTrackingJobRegistry


@pytest.fixture
def backend_implementation(job_registry) -> GeoPySparkBackendImplementation:
    return GeoPySparkBackendImplementation(
        use_zookeeper=False,
        elastic_job_registry=job_registry,
    )


@pytest.fixture
def kube_no_zk(monkeypatch):
    with gps_config_overrides(setup_kerberos_auth=False, use_zk_job_registry=False):
        monkeypatch.setenv("KUBE", "TRUE")
        yield


@mock.patch("kubernetes.config.load_incluster_config", return_value=mock.MagicMock())
@mock.patch("kubernetes.client.CoreV1Api.read_namespaced_pod", return_value=mock.MagicMock())
@mock.patch("kubernetes.client.CustomObjectsApi.create_namespaced_custom_object", return_value=mock.MagicMock())
@mock.patch(
    "kubernetes.client.CustomObjectsApi.get_namespaced_custom_object", return_value={"status": "does not matter"}
)
def test_basic(
    mock_get_spark_pod_status,  # mock arguments in reverse order of patch decorators, as per the docs
    mock_create_spark_pod,
    mock_get_pod_image,
    mock_k8s_config,
    kube_no_zk,
    backend_implementation,
    job_registry,
    mock_s3_bucket,
    fast_sleep,
):
    user = User(user_id="test_user", internal_auth_data={"access_token": "4cc3ss_t0k3n"})

    # 1: create job
    backend_implementation.batch_jobs.create_job(
        user=user,
        process={
            "process_graph": {
                "loadcollection1": {
                    "process_id": "load_collection",
                    "arguments": {
                        "id": "TestCollection-LonLat16x16",
                        "spatial_extent": {"west": 5.0, "south": 50.0, "east": 6.0, "north": 51.0, "crs": "EPSG:4326"},
                        "temporal_extent": ["2020-01-01", "2020-01-31"],
                    },
                },
                "saveresult1": {
                    "process_id": "save_result",
                    "arguments": {
                        "data": {"from_node": "loadcollection1"},
                        "format": "GTiff",
                    },
                    "result": True,
                },
            }
        },
        api_version="1.2",
        metadata={},
        job_options={"log_level": "debug"},
    )

    job_id, job = next(iter(job_registry.db.items()))
    assert job["status"] == JOB_STATUS.CREATED

    # 2: start job
    backend_implementation.batch_jobs.start_job(job_id, user)
    assert mock_k8s_config.called
    assert mock_create_spark_pod.called
    assert mock_get_spark_pod_status.called

    # TODO: 3: poll job
    # TODO: 4: download job results
