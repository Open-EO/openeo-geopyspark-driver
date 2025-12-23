import json

import pytest
from mock import mock
from openeo_driver.constants import JOB_STATUS
from openeo_driver.errors import JobNotFinishedException
from openeo_driver.testing import DictSubSet

from openeo_driver.users import User

from openeogeotrellis.backend import GeoPySparkBackendImplementation
from openeogeotrellis.integrations.kubernetes import K8S_SPARK_APP_STATE, kube_client
from openeogeotrellis.job_registry import EagerlyK8sTrackingInMemoryJobRegistry
from openeogeotrellis.testing import gps_config_overrides


@pytest.fixture
def kubernetes_api():
    # TODO: avoid duplicate load_incluster_config mock (see below)?
    with mock.patch("kubernetes.config.load_incluster_config", return_value=mock.MagicMock()):
        with mock.patch("kubernetes.config.load_kube_config", return_value=mock.MagicMock()):
            return kube_client("CustomObject")


@pytest.fixture
def tracking_job_registry(kubernetes_api) -> EagerlyK8sTrackingInMemoryJobRegistry:
    return EagerlyK8sTrackingInMemoryJobRegistry(kubernetes_api=kubernetes_api)


@pytest.fixture
def backend_implementation(tracking_job_registry) -> GeoPySparkBackendImplementation:
    return GeoPySparkBackendImplementation(
        use_zookeeper=False,
        elastic_job_registry=tracking_job_registry,
    )


@pytest.fixture
def kube_no_zk(monkeypatch):
    with gps_config_overrides(setup_kerberos_auth=False, use_zk_job_registry=False, yunikorn_user_specific_queues=True):
        monkeypatch.setenv("KUBE", "TRUE")
        yield


@mock.patch("kubernetes.config.load_kube_config", return_value=mock.MagicMock())
@mock.patch("kubernetes.config.load_incluster_config", return_value=mock.MagicMock())
@mock.patch("kubernetes.client.CoreV1Api.read_namespaced_pod", return_value=mock.MagicMock())
@mock.patch("kubernetes.client.CustomObjectsApi.create_namespaced_custom_object", return_value=mock.MagicMock())
@mock.patch(
    "kubernetes.client.CustomObjectsApi.get_namespaced_custom_object",
    side_effect=[
        {"status": {"applicationState": {"state": K8S_SPARK_APP_STATE.SUBMITTED}}},  # start job
        {"status": {"applicationState": {"state": K8S_SPARK_APP_STATE.RUNNING}}},  # poll job
        {"status": {"applicationState": {"state": K8S_SPARK_APP_STATE.RUNNING}}},  # get job results (unfinished)
        {"status": {"applicationState": {"state": K8S_SPARK_APP_STATE.COMPLETED}}},  # get job results (finished)
    ],
)
def test_basic(
    mock_get_spark_pod_status,  # mock arguments in reverse order of patch decorators, as per the docs
    mock_create_spark_pod,
    mock_get_pod_image,
    mock_k8s_config,
    kube_no_zk,
    backend_implementation,
    tracking_job_registry,
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

    job_id, job = next(iter(tracking_job_registry.db.items()))
    assert job["status"] == JOB_STATUS.CREATED
    assert job.get("application_id") is None

    # 2: start job
    backend_implementation.batch_jobs.start_job(job_id, user)
    assert mock_k8s_config.called
    assert mock_create_spark_pod.called
    assert mock_get_spark_pod_status.called

    job_id, job = next(iter(tracking_job_registry.db.items()))
    assert job["status"] == JOB_STATUS.CREATED
    assert job["application_id"].startswith("a-")

    # 3: poll job
    job_metadata = backend_implementation.batch_jobs.get_job_info(job_id, user.user_id)
    assert job_metadata.id == job_id
    assert job_metadata.status == JOB_STATUS.RUNNING

    # 4: get job results (unfinished)
    with pytest.raises(JobNotFinishedException):
        backend_implementation.batch_jobs.get_result_assets(job_id, user.user_id)

    mock_s3_bucket.put_object(
        Key=f"batch_jobs/{job_id}/job_metadata.json",
        Body=json.dumps(
            {
                "items": [
                    {
                        "id": "b9510f20-92a0-4947-a4b6-a6a934a0015e",
                        "assets": {"openEO": {"href": "s3://bucket/path/to/openEO.tif"}},
                    },
                ],
                "assets": {"openEO": {"href": "s3://bucket/path/to/openEO.tif"}},
            }
        ).encode("utf-8"),
    )

    # 5: get job results (finished)
    asset_key, asset = next(
        iter(
            asset
            for item in backend_implementation.batch_jobs.get_result_metadata(job_id, user.user_id).items.values()
            for asset in item["assets"].items()
        )
    )
    assert asset_key == "openEO"
    assert asset == DictSubSet({"href": "s3://bucket/path/to/openEO.tif"})
