import contextlib
import datetime as dt
import json
import logging
import re
import sys
import textwrap
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from unittest import mock

import kubernetes
import pytest
import re_assert
import requests_mock
from openeo.util import rfc3339, url_join
from openeo_driver.testing import DictSubSet
from openeo_driver.utils import generate_unique_id

from openeogeotrellis.integrations.kubernetes import K8S_SPARK_APP_STATE, k8s_job_name
from openeogeotrellis.integrations.yarn import YARN_FINAL_STATUS, YARN_STATE
from openeogeotrellis.job_costs_calculator import CostsDetails
from openeogeotrellis.job_registry import ZkJobRegistry, InMemoryJobRegistry
from openeogeotrellis.job_tracker_v2 import (
    JobCostsCalculator,
    JobTracker,
    K8sStatusGetter,
    YarnAppReportParseException,
    YarnStatusGetter,
)
from openeogeotrellis.testing import KazooClientMock
from openeogeotrellis.utils import json_write
from openeogeotrellis.configparams import ConfigParams

# TODO: move YARN related mocks to openeogeotrellis.testing


@dataclass
class YarnAppInfo:
    """Dummy YARN app metadata."""

    app_id: str
    user_id: str = "johndoe"
    queue: str = "default"
    start_time: int = 0
    finish_time: int = 0
    progress: int = 0
    state: str = YARN_STATE.SUBMITTED
    final_state: str = YARN_FINAL_STATUS.UNDEFINED
    memory_seconds: int = 1234
    vcore_seconds: int = 32
    diagnostics: str = ""

    def set_state(self, state: str, final_state: str) -> "YarnAppInfo":
        self.state = state
        self.final_state = final_state
        return self

    def set_submitted(self):
        return self.set_state(YARN_STATE.SUBMITTED, YARN_FINAL_STATUS.UNDEFINED)

    def set_accepted(self):
        return self.set_state(YARN_STATE.ACCEPTED, YARN_FINAL_STATUS.UNDEFINED)

    def set_running(self):
        if not self.start_time:
            self.start_time = int(1000 * time.time())
        return self.set_state(YARN_STATE.RUNNING, YARN_FINAL_STATUS.UNDEFINED)

    def set_finished(self, final_state: str = YARN_FINAL_STATUS.SUCCEEDED):
        assert final_state in {YARN_FINAL_STATUS.SUCCEEDED, YARN_FINAL_STATUS.FAILED}
        if not self.finish_time:
            self.finish_time = int(1000 * time.time())
        # TODO: what is the meaning actually of state=FINISHED + final-state=FAILED?
        return self.set_state(YARN_STATE.FINISHED, final_state)

    def set_failed(self):
        return self.set_state(YARN_STATE.FAILED, YARN_FINAL_STATUS.FAILED)

    def set_killed(self):
        return self.set_state(YARN_STATE.KILLED, YARN_FINAL_STATUS.KILLED)

    def set_launch_failed(self):
        """Simulate that yarn launch failed to launch the application.

        Often the cause is that the container could not be launched / could not be found.
        The 'diagnostics' field in the response should contain more information about what went wrong.
        """
        self.diagnostics = textwrap.dedent(
            f"""
            Application {self.app_id} failed 1 times (global limit =2; local limit is =1) due to AM Container for appattempt_1670152552564_21143_000001 exited with  exitCode: 7
            Failing this attempt.Diagnostics: [2022-12-14 10:27:49.976]Exception from container-launch.
            Container id: container_e5070_1670152552564_21143_01_000001
            Exit code: 7
            Exception message: Launch container failed
            Shell error output: Unable to find image 'test-repository/test-image:1234' locally
            docker: Error response from daemon: <some error response here, left it out>
            See 'docker run --help'.
            """
        )
        return self.set_failed()

    def status_rest_response(self) -> dict:
        return fake_yarn_rest_response_json(
            app_id=self.app_id,
            user=self.user_id,
            state=self.state,
            final_status=self.final_state,
            queue=self.queue,
            progress=self.progress,
            started_time=self.start_time,
            finished_time=self.finish_time,
            memory_seconds=self.memory_seconds,
            vcore_seconds=self.vcore_seconds,
            diagnostics=self.diagnostics,
        )


def fake_yarn_rest_response_json(
    app_id: str,
    user: str = "jenkins",
    state: str = "FINISHED",
    final_status: str = "SUCCEEDED",
    queue: str = "default",
    progress: int = 0.0,
    started_time: int = 0,
    finished_time: int = 0,
    memory_seconds: int = 0,
    vcore_seconds: int = 0,
    diagnostics: str = "",
):
    """Helper function to create JSON for YARN REST responses in mocks."""
    return {
        "app": {
            "id": app_id,
            "user": user,
            "name": "openEO batch_None_j-a697fe72ef44449e9c5b7e11f0c2b89a_user a1a40dc638599c56313417ac216e814b8ff5eb124b896569360abe95b8b9cb28@egi.eu",
            "queue": queue,
            "state": state,
            "finalStatus": final_status,
            "progress": progress,
            "trackingUI": "History",
            "trackingUrl": "https://openeo.test",
            "diagnostics": diagnostics,
            "clusterId": 1674538064532,
            "applicationType": "SPARK",
            "applicationTags": "openeo",
            "priority": 0,
            "startedTime": started_time,
            "finishedTime": finished_time,
            "elapsedTime": 299033,
            "amContainerLogs": "https://openeo.test/somewhere",
            "amHostHttpAddress": "fake.test:11111",
            "amRPCAddress": "fake.test:22222",
            "masterNodeId": "fake.test:33333",
            "allocatedMB": -1,
            "allocatedVCores": -1,
            "reservedMB": -1,
            "reservedVCores": -1,
            "runningContainers": -1,
            "memorySeconds": memory_seconds,
            "vcoreSeconds": vcore_seconds,
            "queueUsagePercentage": 0.0,
            "clusterUsagePercentage": 0.0,
            "resourceSecondsMap": {
                "entry": {
                    "key": "memory-mb",
                    "value": f'"{memory_seconds}"',  # In the yarn response this is a string, not an int.
                },
                "entry": {
                    "key": "vcores",
                    "value": f'"{vcore_seconds}"',  # In the yarn response this is a string, not an int.
                },
            },
            "preemptedResourceMB": 0,
            "preemptedResourceVCores": 0,
            "numNonAMContainerPreempted": 0,
            "numAMContainerPreempted": 0,
            "preemptedMemorySeconds": 0,
            "preemptedVcoreSeconds": 0,
            "preemptedResourceSecondsMap": {},
            "logAggregationStatus": "TIME_OUT",
            "unmanagedApplication": False,
            "amNodeLabelExpression": "",
            "timeouts": {
                "timeout": [
                    {
                        "type": "LIFETIME",
                        "expiryTime": "UNLIMITED",
                        "remainingTimeInSeconds": -1,
                    }
                ]
            },
        }
    }

class YarnMock:
    """YARN cluster mock"""

    def __init__(self):
        self.apps: Dict[str, YarnAppInfo] = {}
        self.corrupt_app_ids = set()

        # Simulates app IDs for which yarn could not launch a container.
        self.failed_yarn_launch_app_ids = set()

    def submit(self, app_id: Optional[str] = None, **kwargs) -> YarnAppInfo:
        """Create a new (dummy) YARN app"""
        if app_id is None:
            app_id = generate_unique_id(prefix="app")
        self.apps[app_id] = app = YarnAppInfo(app_id=app_id, **kwargs)
        return app

    @contextlib.contextmanager
    def patch(self):
        with requests_mock.Mocker() as requests_mocker:
            # Mock the requests to the REST API of YARN. The configuration tells us what the base URL is.
            # To direct the request to a fake URL, mock the environment variable YARN_REST_API_BASE_URL.
            base_url = ConfigParams().yarn_rest_api_base_url
            url_matcher = re.compile(f"{base_url}/ws/v1/cluster/apps/")

            def response_call_back(request, context):
                context.status_code = 200
                app_id = str(request.url).split("/")[-1]
                if app_id in self.corrupt_app_ids:
                    return f'"C0rRup7! {app_id}"'
                elif app_id in self.apps:
                    return json.dumps(self.apps[app_id].status_rest_response())
                elif app_id in self.failed_yarn_launch_app_ids:
                    return json.dumps(self.apps[app_id].status_rest_response())
                else:
                    context.status_code = 404
                    return json.dumps(
                        {
                            "RemoteException": {
                                "exception": "BadRequestException",
                                "message": f"java.lang.IllegalArgumentException: Invalid ApplicationId: {app_id}",
                                "javaClassName": "org.apache.hadoop.yarn.webapp.BadRequestException",
                            }
                        }
                    )

            requests_mocker.get(
                url_matcher,
                text=response_call_back,
            )

            yield

@dataclass
class KubernetesAppInfo:
    """Dummy Kubernetes app metadata."""

    app_id: str
    state: str = K8S_SPARK_APP_STATE.NEW
    start_time: Union[str, None] = None
    finish_time: Union[str, None] = None

    def set_submitted(self):
        if not self.start_time:
            self.start_time = rfc3339.datetime(dt.datetime.utcnow())
        self.state = K8S_SPARK_APP_STATE.SUBMITTED

    def set_running(self):
        self.state = K8S_SPARK_APP_STATE.RUNNING

    def set_completed(self):
        if not self.finish_time:
            self.finish_time = rfc3339.datetime(dt.datetime.utcnow())
        self.state = K8S_SPARK_APP_STATE.COMPLETED

    def set_failed(self):
        if not self.finish_time:
            self.finish_time = rfc3339.datetime(dt.datetime.utcnow())
        self.state = K8S_SPARK_APP_STATE.FAILED


class KubernetesMock:
    """Kubernetes cluster mock."""

    def __init__(self, kubecost_url: str = "https://kubecost.test"):
        self.apps: Dict[str, KubernetesAppInfo] = {}
        self.kubecost_url = kubecost_url
        self.corrupt_app_ids = set()

    @contextlib.contextmanager
    def patch(self):
        def get_namespaced_custom_object(name: str, **kwargs) -> dict:
            if name in self.corrupt_app_ids:
                raise kubernetes.client.exceptions.ApiException(
                    status=500, reason="Internal Server Error"
                )
            if name not in self.apps:
                raise kubernetes.client.exceptions.ApiException(
                    status=404, reason="Not Found"
                )
            app = self.apps[name]
            if app.state == K8S_SPARK_APP_STATE.NEW:
                # TODO: is this the actual behavior for "new" apps: no "status" in response?
                return {}
            return {
                "status": {
                    "applicationState": {"state": app.state},
                    "lastSubmissionAttemptTime": app.start_time,
                    "terminationTime": app.finish_time,
                }
            }

        def get_model_allocation(
            request: requests_mock.request._RequestObjectProxy, context
        ) -> dict:
            namespace = request.qs["filternamespaces"][0]
            return {
                "code": 200,
                "data": [
                    {
                        namespace: {
                            "cpuCoreHours": 2.34,
                            "ramByteHours": 5.678 * 1024 * 1024,
                        }
                    }
                ],
            }

        with mock.patch(
            "openeogeotrellis.job_tracker_v2.kube_client"
        ) as kube_client, requests_mock.Mocker() as requests_mocker:
            # Mock Kubernetes interaction
            api_instance = kube_client.return_value
            api_instance.get_namespaced_custom_object = get_namespaced_custom_object

            # Mock kubernetes usage API
            requests_mocker.get(
                url_join(self.kubecost_url, "/model/allocation"),
                json=get_model_allocation,
            )

            yield

    def submit(
        self, app_id: str, state: str = K8S_SPARK_APP_STATE.SUBMITTED, **kwargs
    ) -> KubernetesAppInfo:
        """Create a new (dummy) Kubernetes app"""
        assert app_id not in self.apps
        self.apps[app_id] = app = KubernetesAppInfo(
            app_id=app_id, state=state, **kwargs
        )
        return app


@pytest.fixture
def zk_client() -> KazooClientMock:
    return KazooClientMock()


@pytest.fixture
def zk_job_registry(zk_client) -> ZkJobRegistry:
    return ZkJobRegistry(zk_client=zk_client)


@pytest.fixture
def yarn_mock() -> YarnMock:
    yarn = YarnMock()
    with yarn.patch():
        yield yarn



@pytest.fixture
def elastic_job_registry() -> InMemoryJobRegistry:
    # TODO: stop calling this fixture *elastic* job registry?
    return InMemoryJobRegistry()


@pytest.fixture
def job_costs_calculator() -> JobCostsCalculator:
    calculator_mock = mock.Mock(JobCostsCalculator)
    calculator_mock.calculate_costs.return_value = 129.95
    return calculator_mock


DUMMY_PG_1 = {
    "add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}
}
DUMMY_PROCESS_1 = {"process_graph": DUMMY_PG_1}


def _extract_update_statuses_stats(caplog) -> List[dict]:
    return [
        json.loads(msg.split(":", 1)[1])
        for msg in caplog.messages
        if msg.startswith("JobTracker.update_statuses stats:")
    ]

class TestYarnJobTracker:
    @pytest.fixture
    def job_tracker(
        self, zk_job_registry, elastic_job_registry, batch_job_output_root, job_costs_calculator
    ) -> JobTracker:
        principal = "john@EXAMPLE.TEST"
        keytab = "test/openeo.keytab"
        job_tracker = JobTracker(
            app_state_getter=YarnStatusGetter(ConfigParams().yarn_rest_api_base_url),
            zk_job_registry=zk_job_registry,
            principal=principal,
            keytab=keytab,
            job_costs_calculator=job_costs_calculator,
            output_root_dir=batch_job_output_root,
            elastic_job_registry=elastic_job_registry,
        )
        return job_tracker

    def test_yarn_zookeeper_basic(
        self,
        zk_job_registry,
        yarn_mock,
        job_tracker,
        elastic_job_registry,
        caplog,
        time_machine,
        job_costs_calculator,
    ):
        caplog.set_level(logging.WARNING)
        time_machine.move_to("2022-12-14T12:00:00Z", tick=False)

        # Create openeo batch job (not started yet)
        user_id = "john"
        job_id = "job-123"
        zk_job_registry.register(
            job_id=job_id,
            user_id=user_id,
            api_version="1.2.3",
            specification=DUMMY_PROCESS_1,
        )
        elastic_job_registry.create_job(
            job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1
        )

        def zk_job_info() -> dict:
            return zk_job_registry.get_job(job_id=job_id, user_id=user_id)

        # Check initial status in registry
        assert zk_job_info() == DictSubSet(
            {
                "job_id": job_id,
                "user_id": user_id,
                "status": "created",
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:00:00Z",  # TODO: get this working?
            }
        )
        assert elastic_job_registry.db == {
            "job-123": DictSubSet(
                {
                    "job_id": "job-123",
                    "user_id": "john",
                    "status": "created",
                    "process": DUMMY_PROCESS_1,
                    "created": "2022-12-14T12:00:00Z",
                    "updated": "2022-12-14T12:00:00Z",
                }
            )
        }

        # Start job: submit app to yarn
        time_machine.coordinates.shift(70)
        yarn_app = yarn_mock.submit(app_id="app-123")
        yarn_app.set_submitted()
        zk_job_registry.set_application_id(
            job_id=job_id, user_id=user_id, application_id=yarn_app.app_id
        )

        # Trigger `update_statuses`
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "queued",
                "created": "2022-12-14T12:00:00Z",
                "application_id": yarn_app.app_id,
                # "updated": "2022-12-14T12:01:10Z",  # TODO: get this working?
            }
        )
        assert elastic_job_registry.db[job_id] == DictSubSet(
            {
                "status": "queued",
                "created": "2022-12-14T12:00:00Z",
                "updated": "2022-12-14T12:01:10Z",
                # "application_id": yarn_app.app_id,  # TODO: get this working?
            }
        )

        # Set ACCEPTED in Yarn
        time_machine.coordinates.shift(70)
        yarn_app.set_accepted()
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "queued",
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:02:20Z",  # TODO: get this working?
            }
        )
        assert elastic_job_registry.db[job_id] == DictSubSet(
            {
                "status": "queued",
                "created": "2022-12-14T12:00:00Z",
                "updated": "2022-12-14T12:02:20Z",
            }
        )

        # Set RUNNING in Yarn
        time_machine.coordinates.shift(70)
        yarn_app.set_running()
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "running",
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:03:30Z",  # TODO: get this working?
            }
        )
        assert elastic_job_registry.db[job_id] == DictSubSet(
            {
                "status": "running",
                "created": "2022-12-14T12:00:00Z",
                "updated": "2022-12-14T12:03:30Z",
                "started": "2022-12-14T12:03:30Z",
            }
        )

        # Set FINISHED in Yarn
        time_machine.coordinates.shift(70)
        yarn_app.set_finished()
        json_write(
            path=job_tracker._batch_jobs.get_results_metadata_path(job_id=job_id),
            data={"foo": "bar"},
        )
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "finished",
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:04:40Z",  # TODO: get this working?,
                "usage": {
                    "cpu": {"unit": "cpu-seconds", "value": 32},
                    "memory": {"unit": "mb-seconds", "value": 1234},
                },
                "costs": 129.95
            }
        )
        assert elastic_job_registry.db[job_id] == DictSubSet(
            {
                "status": "finished",
                "created": "2022-12-14T12:00:00Z",
                "updated": "2022-12-14T12:04:40Z",
                "started": "2022-12-14T12:03:30Z",
                "finished": "2022-12-14T12:04:40Z",
            }
        )

        calculate_costs_calls = job_costs_calculator.calculate_costs.call_args_list
        assert len(calculate_costs_calls) == 1
        costs_details: CostsDetails
        (costs_details,), _ = calculate_costs_calls[0]

        assert costs_details == CostsDetails(
            job_id=job_id,
            user_id=user_id,
            execution_id=yarn_app.app_id,
            app_state='FINISHED',
            area_square_meters=None,
            job_title=None,
            start_time=dt.datetime(2022, 12, 14, 12, 3, 30),
            finish_time=dt.datetime(2022, 12, 14, 12, 4, 40),
            cpu_seconds=32,
            mb_seconds=1234,
            sentinelhub_processing_units=0.0,
            unique_process_ids=[]
        )

        assert caplog.record_tuples == []

    def test_yarn_zookeeper_lost_yarn_app(
        self,
        zk_job_registry,
        yarn_mock,
        job_tracker,
        elastic_job_registry,
        caplog,
    ):
        """
        Check that JobTracker.update_statuses() keeps working if there is no YARN app for a given job
        """
        caplog.set_level(logging.WARNING)
        for j in [1, 2, 3]:
            job_id = f"job-{j}"
            user_id = f"user{j}"
            app_id = f"app-{j}"
            zk_job_registry.register(
                job_id=job_id,
                user_id=user_id,
                api_version="1.2.3",
                specification=DUMMY_PROCESS_1,
            )
            zk_job_registry.set_application_id(
                job_id=job_id, user_id=user_id, application_id=app_id
            )
            elastic_job_registry.create_job(
                job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1
            )
            # YARN apps 1 and 3 are running but app 2 is lost/missing
            if j != 2:
                yarn_mock.submit(app_id=app_id, state=YARN_STATE.RUNNING)

        # Let job tracker do status updates
        job_tracker.update_statuses()

        assert zk_job_registry.get_job(job_id="job-1", user_id="user1") == DictSubSet(
            {"status": "running"}
        )
        assert zk_job_registry.get_job(job_id="job-2", user_id="user2") == DictSubSet(
            {"status": "error"}
        )
        assert zk_job_registry.get_job(job_id="job-3", user_id="user3") == DictSubSet(
            {"status": "running"}
        )

        assert elastic_job_registry.db == {
            "job-1": DictSubSet(status="running"),
            "job-2": DictSubSet(status="error"),
            "job-3": DictSubSet(status="running"),
        }

        assert caplog.record_tuples == [
            (
                "openeogeotrellis.job_tracker_v2",
                logging.WARNING,
                "App not found: job_id='job-2' application_id='app-2'",
            )
        ]

    def test_yarn_zookeeper_unexpected_yarn_error(
        self,
        zk_job_registry,
        yarn_mock,
        job_tracker,
        elastic_job_registry,
        caplog,
    ):
        """
        Check that JobTracker.update_statuses() keeps working if there is unexpected error while checking YARN state.
        """
        caplog.set_level(logging.WARNING)
        for j in [1, 2, 3]:
            zk_job_registry.register(
                job_id=f"job-{j}",
                user_id=f"user{j}",
                api_version="1.2.3",
                specification=DUMMY_PROCESS_1,
            )
            zk_job_registry.set_application_id(
                job_id=f"job-{j}", user_id=f"user{j}", application_id=f"app-{j}"
            )
            elastic_job_registry.create_job(
                job_id=f"job-{j}", user_id=f"user{j}", process=DUMMY_PROCESS_1
            )
            # YARN apps 1 and 3 are running but app 2 is lost/missing
            if j != 2:
                yarn_mock.submit(app_id=f"app-{j}", state=YARN_STATE.RUNNING)
            else:
                yarn_mock.corrupt_app_ids.add(f"app-{j}")

        # Let job tracker do status updates
        job_tracker.update_statuses()

        assert zk_job_registry.get_job(job_id="job-1", user_id="user1") == DictSubSet(
            {"status": "running"}
        )
        # job-2 is currently stuck in state "created"
        assert zk_job_registry.get_job(job_id="job-2", user_id="user2") == DictSubSet(
            {"status": "created"}
        )
        assert zk_job_registry.get_job(job_id="job-3", user_id="user3") == DictSubSet(
            {"status": "running"}
        )

        assert elastic_job_registry.db == {
            "job-1": DictSubSet(status="running"),
            # job-2 is currently stuck in state "created"
            "job-2": DictSubSet(status="created"),
            "job-3": DictSubSet(status="running"),
        }
        assert caplog.record_tuples == [
            (
                "openeogeotrellis.job_tracker_v2",
                logging.ERROR,
                (
                    "Failed status sync for job_id='job-2': "
                    + "unexpected YarnAppReportParseException: "
                    + "Cannot parse response body: expecting a JSON dict but body contains "
                    + "a value of type <class 'str'>, value='C0rRup7! app-2' Response body='\"C0rRup7! app-2\"'"
                ),
            )
        ]

    def test_yarn_zookeeper_no_app_id(
        self,
        zk_job_registry,
        yarn_mock,
        job_tracker,
        elastic_job_registry,
        caplog,
        time_machine,
    ):
        caplog.set_level(logging.INFO)

        time_machine.move_to("2022-12-14T12:00:00Z", tick=False)

        # Job without app id (not started yet)
        user_id = "john"
        job_id = "job-123"
        zk_job_registry.register(
            job_id=job_id,
            user_id=user_id,
            api_version="1.2.3",
            specification=DUMMY_PROCESS_1,
        )
        elastic_job_registry.create_job(
            job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1
        )

        # Another job that has an app id (already running)
        zk_job_registry.register(
            job_id=job_id + "-other",
            user_id=user_id,
            api_version="1.2.3",
            specification=DUMMY_PROCESS_1,
        )
        elastic_job_registry.create_job(
            job_id=job_id + "-other", user_id=user_id, process=DUMMY_PROCESS_1
        )
        app_other = yarn_mock.submit(app_id="app-123-other").set_running()
        zk_job_registry.set_application_id(
            job_id=job_id + "-other", user_id=user_id, application_id=app_other.app_id
        )

        def zk_job_info() -> dict:
            return zk_job_registry.get_job(job_id=job_id, user_id=user_id)

        # Trigger `update_statuses` a bit later
        time_machine.move_to("2022-12-14T12:30:00Z", tick=False)
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "created",
                "created": "2022-12-14T12:00:00Z",
            }
        )
        assert elastic_job_registry.db[job_id] == DictSubSet(
            {
                "status": "created",
                "created": "2022-12-14T12:00:00Z",
                "updated": "2022-12-14T12:00:00Z",
            }
        )

        assert "ERROR" not in caplog.text
        assert caplog.text == re_assert.Matches(
            ".*Skipping job without application_id: job_id='job-123'.*age.*seconds=1800.*status='created'",
            flags=re.DOTALL,
        )

        [stats] = _extract_update_statuses_stats(caplog)
        assert stats == {
            "collected jobs": 2,
            "skip due to no application_id (status='created')": 1,
            "get metadata attempt": 1,
            "job with previous_status='created'": 1,
            "new metadata": 1,
            "status change": 1,
            "status change 'created' -> 'running'": 1,
        }

    def test_yarn_zookeeper_yarn_failed_to_launch_container(
        self,
        zk_job_registry,
        yarn_mock,
        job_tracker,
        elastic_job_registry,
        caplog,
        time_machine,
    ):
        caplog.set_level(logging.WARNING)
        time_machine.move_to("2022-12-14T12:00:00Z", tick=False)

        # Create openeo batch job (not started yet)
        user_id = "john"
        job_id = "job-123"
        zk_job_registry.register(
            job_id=job_id,
            user_id=user_id,
            api_version="1.2.3",
            specification=DUMMY_PROCESS_1,
        )
        elastic_job_registry.create_job(
            job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1
        )

        def zk_job_info() -> dict:
            return zk_job_registry.get_job(job_id=job_id, user_id=user_id)

        # Check initial status in registry
        assert zk_job_info() == DictSubSet(
            {
                "job_id": job_id,
                "user_id": user_id,
                "status": "created",
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:00:00Z",  # TODO: get this working?
            }
        )
        assert elastic_job_registry.db == {
            "job-123": DictSubSet(
                {
                    "job_id": "job-123",
                    "user_id": "john",
                    "status": "created",
                    "process": DUMMY_PROCESS_1,
                    "created": "2022-12-14T12:00:00Z",
                    "updated": "2022-12-14T12:00:00Z",
                }
            )
        }

        # Start job: submit app to yarn
        time_machine.coordinates.shift(70)
        yarn_app = yarn_mock.submit(app_id="app-123")
        # Simulate that the yarn launch failed.
        yarn_app.set_launch_failed()
        zk_job_registry.set_application_id(
            job_id=job_id, user_id=user_id, application_id=yarn_app.app_id
        )

        # Trigger `update_statuses`
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "error",
                "created": "2022-12-14T12:00:00Z",
                "application_id": yarn_app.app_id,
                # "updated": "2022-12-14T12:01:10Z",  # TODO: get this working?
            }
        )
        assert elastic_job_registry.db[job_id] == DictSubSet(
            {
                "status": "error",
                "created": "2022-12-14T12:00:00Z",
                "updated": "2022-12-14T12:01:10Z",
                # "application_id": yarn_app.app_id,  # TODO: get this working?
            }
        )

        # When yarn could not launch the application, then we want to see the diagnostics in the logs.
        assert yarn_app.diagnostics in caplog.text

    def test_yarn_zookeeper_stats(
        self,
        zk_job_registry,
        yarn_mock,
        job_tracker,
        caplog,
    ):
        """
        Check stats reporting functionality
        """
        caplog.set_level(logging.INFO)
        for j in [1, 2, 3]:
            job_id = f"job-{j}"
            user_id = f"user{j}"
            app_id = f"app-{j}"
            zk_job_registry.register(
                job_id=job_id,
                user_id=user_id,
                api_version="1.2.3",
                specification=DUMMY_PROCESS_1,
            )
            zk_job_registry.set_application_id(
                job_id=job_id, user_id=user_id, application_id=app_id
            )
            # YARN apps 1 and 3 are running but app 2 is lost/missing
            if j != 2:
                yarn_mock.submit(app_id=app_id, state=YARN_STATE.RUNNING)

        # Let job tracker do status updates
        job_tracker.update_statuses()
        [stats] = _extract_update_statuses_stats(caplog)
        assert stats == {
            "collected jobs": 3,
            "job with previous_status='created'": 3,
            "app not found": 1,
            "get metadata attempt": 3,
            "new metadata": 2,
            "status change": 2,
            "status change 'created' -> 'running'": 2,
        }

        # Do it again
        caplog.clear()
        job_tracker.update_statuses()
        [stats] = _extract_update_statuses_stats(caplog)
        assert stats == {
            "collected jobs": 2,
            "job with previous_status='running'": 2,
            "get metadata attempt": 2,
            "new metadata": 2,
            "status same": 2,
            "status same 'running'": 2,
        }


class TestYarnStatusGetter:
    def test_parse_application_response_basic(self):
        response = fake_yarn_rest_response_json(
            app_id="application_1671092799310_26739",
            state="FINISHED",
            final_status="SUCCEEDED",
            started_time=1673021672793,
            finished_time=1673021943245,
            memory_seconds=5116996,
            vcore_seconds=2265,
        )

        job_metadata = YarnStatusGetter.parse_application_response(data=response)
        assert job_metadata.status == "finished"
        assert job_metadata.start_time == dt.datetime(2023, 1, 6, 16, 14, 32, 793000)
        assert job_metadata.finish_time == dt.datetime(2023, 1, 6, 16, 19, 3, 245000)
        assert job_metadata.usage.cpu_seconds == 2265
        assert job_metadata.usage.mb_seconds == 5116996

    def test_parse_application_response_running(self):
        response = fake_yarn_rest_response_json(
            app_id="application_1671092799310_26739",
            state="RUNNING",
            final_status="UNDEFINED",
            started_time=1673021672793,
            finished_time=0,
            progress=50.0,
            memory_seconds=96183879,
            vcore_seconds=46964,
            diagnostics="",
        )

        job_metadata = YarnStatusGetter.parse_application_response(data=response)
        assert job_metadata.status == "running"
        assert job_metadata.start_time == dt.datetime(2023, 1, 6, 16, 14, 32, 793000)
        assert job_metadata.finish_time is None
        assert job_metadata.usage.cpu_seconds == 46964
        assert job_metadata.usage.mb_seconds == 96183879

    def test_parse_application_response_empty(self):
        with pytest.raises(YarnAppReportParseException):
            YarnStatusGetter.parse_application_response(data={})

    def test_response_is_not_valid_json(self, requests_mock):
        status_getter = YarnStatusGetter(ConfigParams().yarn_rest_api_base_url)
        app_id = "app_123"
        app_url = status_getter.get_application_url(app_id)
        m_get = requests_mock.get(
            app_url, text="Bad response, not valid JSON without quotes"
        )

        with pytest.raises(json.JSONDecodeError):
            status_getter.get_job_metadata(None, None, app_id=app_id)

        assert m_get.called

    def test_response_is_not_dict(self, requests_mock):
        status_getter = YarnStatusGetter(ConfigParams().yarn_rest_api_base_url)
        app_id = "app_123"
        app_url = status_getter.get_application_url(app_id)
        m_get = requests_mock.get(
            app_url, text='"Bad response: is JSON but not a dict"'
        )

        with pytest.raises(YarnAppReportParseException):
            status_getter.get_job_metadata(None, None, app_id=app_id)

        assert m_get.called


class TestK8sJobTracker:
    @pytest.fixture
    def kubecost_url(self):
        return "https://kubecost.test/"

    @pytest.fixture
    def k8s_mock(self, kubecost_url) -> KubernetesMock:
        kube = KubernetesMock(kubecost_url=kubecost_url)
        with kube.patch():
            yield kube

    @pytest.fixture
    def job_tracker(
        self,
        zk_job_registry,
        elastic_job_registry,
        batch_job_output_root,
        k8s_mock,
        kubecost_url,
        job_costs_calculator
    ) -> JobTracker:
        principal = "john@EXAMPLE.TEST"
        keytab = "test/openeo.keytab"
        job_tracker = JobTracker(
            app_state_getter=K8sStatusGetter(kubecost_url=kubecost_url),
            zk_job_registry=zk_job_registry,
            principal=principal,
            keytab=keytab,
            job_costs_calculator=job_costs_calculator,
            output_root_dir=batch_job_output_root,
            elastic_job_registry=elastic_job_registry,
        )
        return job_tracker

    def test_k8s_zookeeper_basic(
        self,
        zk_job_registry,
        job_tracker,
        elastic_job_registry,
        caplog,
        time_machine,
        k8s_mock,
        job_costs_calculator,
    ):
        caplog.set_level(logging.WARNING)
        time_machine.move_to("2022-12-14T12:00:00Z", tick=False)

        user_id = "john"
        job_id = "job-123"
        zk_job_registry.register(
            job_id=job_id,
            user_id=user_id,
            api_version="1.2.3",
            specification=DUMMY_PROCESS_1,
        )
        elastic_job_registry.create_job(
            job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1
        )

        def zk_job_info() -> dict:
            return zk_job_registry.get_job(job_id=job_id, user_id=user_id)

        # Check initial status in registry
        assert zk_job_info() == DictSubSet(
            {
                "job_id": job_id,
                "user_id": user_id,
                "status": "created",
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:00:00Z",  # TODO: get this working?
            }
        )
        assert elastic_job_registry.db == {
            "job-123": DictSubSet(
                {
                    "job_id": "job-123",
                    "user_id": "john",
                    "status": "created",
                    "process": DUMMY_PROCESS_1,
                    "created": "2022-12-14T12:00:00Z",
                    "updated": "2022-12-14T12:00:00Z",
                }
            )
        }

        # Submit Kubernetes app
        time_machine.coordinates.shift(70)
        app_id = k8s_job_name(job_id=job_id, user_id=user_id)
        kube_app = k8s_mock.submit(app_id=app_id)
        kube_app.set_submitted()
        zk_job_registry.set_application_id(
            job_id=job_id, user_id=user_id, application_id=app_id
        )

        # Trigger `update_statuses`
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "queued",
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:02:20Z",  # TODO: get this working?
            }
        )
        assert elastic_job_registry.db[job_id] == DictSubSet(
            {
                "status": "queued",
                "created": "2022-12-14T12:00:00Z",
                "updated": "2022-12-14T12:01:10Z",
            }
        )

        # Set RUNNING IN Kubernetes
        time_machine.coordinates.shift(70)
        kube_app.set_running()
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "running",
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:03:30Z",  # TODO: get this working?
            }
        )
        assert elastic_job_registry.db[job_id] == DictSubSet(
            {
                "status": "running",
                "created": "2022-12-14T12:00:00Z",
                "updated": "2022-12-14T12:02:20Z",
                "started": "2022-12-14T12:01:10Z",
            }
        )

        # Set COMPLETED IN Kubernetes
        time_machine.coordinates.shift(70)
        kube_app.set_completed()
        json_write(
            path=job_tracker._batch_jobs.get_results_metadata_path(job_id=job_id),
            data={"foo": "bar"},
        )
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "finished",
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:04:40Z",  # TODO: get this working?
                "usage": {
                    "cpu": {"unit": "cpu-seconds", "value": pytest.approx(2.34 * 3600, rel=0.001)},
                    "memory": {"unit": "mb-seconds", "value": pytest.approx(5.678 * 3600, rel=0.001)},
                },
                "costs": 129.95
            }
        )
        assert elastic_job_registry.db[job_id] == DictSubSet(
            {
                "status": "finished",
                "created": "2022-12-14T12:00:00Z",
                "updated": "2022-12-14T12:03:30Z",
                "started": "2022-12-14T12:01:10Z",
                "finished": "2022-12-14T12:03:30Z",
                # TODO: usage tracking (cpu, memory)
            }
        )

        calculate_costs_calls = job_costs_calculator.calculate_costs.call_args_list
        assert len(calculate_costs_calls) == 1
        costs_details: CostsDetails
        (costs_details,), _ = calculate_costs_calls[0]

        assert costs_details == CostsDetails(
            job_id=job_id,
            user_id=user_id,
            execution_id=kube_app.app_id,
            app_state='COMPLETED',
            area_square_meters=None,
            job_title=None,
            start_time=dt.datetime(2022, 12, 14, 12, 1, 10),
            finish_time=dt.datetime(2022, 12, 14, 12, 3, 30),
            cpu_seconds=pytest.approx(2.34 * 3600, rel=0.001),
            mb_seconds=pytest.approx(5.678 * 3600, rel=0.001),
            sentinelhub_processing_units=0.0,
            unique_process_ids=[]
        )

        assert caplog.record_tuples == []

    def test_k8s_zookeeper_new_app(
        self,
        zk_job_registry,
        job_tracker,
        elastic_job_registry,
        caplog,
        time_machine,
        k8s_mock,
    ):
        caplog.set_level(logging.WARNING)
        time_machine.move_to("2022-12-14T12:00:00Z", tick=False)
        # TODO: avoid setting private property
        job_tracker._kube_mode = True

        user_id = "john"
        job_id = "job-123"
        zk_job_registry.register(
            job_id=job_id,
            user_id=user_id,
            api_version="1.2.3",
            specification=DUMMY_PROCESS_1,
        )
        elastic_job_registry.create_job(
            job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1
        )

        def zk_job_info() -> dict:
            return zk_job_registry.get_job(job_id=job_id, user_id=user_id)

        # Submit Kubernetes app
        time_machine.coordinates.shift(70)
        app_id = k8s_job_name(job_id=job_id, user_id=user_id)
        kube_app = k8s_mock.submit(app_id=app_id, state=K8S_SPARK_APP_STATE.NEW)
        zk_job_registry.set_application_id(
            job_id=job_id, user_id=user_id, application_id=app_id
        )

        # Trigger `update_statuses`
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "queued",
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:02:20Z",  # TODO: get this working?
            }
        )
        assert elastic_job_registry.db[job_id] == DictSubSet(
            {
                "status": "queued",
                "created": "2022-12-14T12:00:00Z",
                "updated": "2022-12-14T12:01:10Z",
            }
        )
        assert caplog.record_tuples == [
            (
                "openeogeotrellis.job_tracker_v2",
                logging.WARNING,
                "No K8s app status found, assuming new app",
            )
        ]

    def test_k8s_zookeeper_lost_app(
        self,
        zk_job_registry,
        job_tracker,
        elastic_job_registry,
        caplog,
        time_machine,
        k8s_mock,
    ):
        """
        Check that JobTracker.update_statuses() keeps working if there is no K8s app for a given job
        """
        caplog.set_level(logging.WARNING)

        for j in [1, 2, 3]:
            job_id = f"job-{j}"
            user_id = f"user{j}"
            app_id = k8s_job_name(job_id=job_id, user_id=user_id)

            zk_job_registry.register(
                job_id=job_id,
                user_id=user_id,
                api_version="1.2.3",
                specification=DUMMY_PROCESS_1,
            )
            zk_job_registry.set_application_id(
                job_id=job_id, user_id=user_id, application_id=app_id
            )
            elastic_job_registry.create_job(
                job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1
            )
            # K8s apps 1 and 3 are running but app 2 is lost/missing
            if j != 2:
                k8s_mock.submit(app_id=app_id, state=YARN_STATE.RUNNING)

        # Let job tracker do status updates
        job_tracker.update_statuses()

        assert zk_job_registry.get_job(job_id="job-1", user_id="user1") == DictSubSet(
            {"status": "running"}
        )
        assert zk_job_registry.get_job(job_id="job-2", user_id="user2") == DictSubSet(
            {"status": "error"}
        )
        assert zk_job_registry.get_job(job_id="job-3", user_id="user3") == DictSubSet(
            {"status": "running"}
        )

        assert elastic_job_registry.db == {
            "job-1": DictSubSet(status="running"),
            "job-2": DictSubSet(status="error"),
            "job-3": DictSubSet(status="running"),
        }

        assert caplog.record_tuples == [
            (
                "openeogeotrellis.job_tracker_v2",
                logging.WARNING,
                "App not found: job_id='job-2' application_id='job-job-2-user2'",
            )
        ]

    def test_k8s_zookeeper_unexpected_k8s_error(
        self,
        zk_job_registry,
        job_tracker,
        elastic_job_registry,
        caplog,
        time_machine,
        k8s_mock,
    ):
        """
        Check that JobTracker.update_statuses() keeps working if there is no K8s app for a given job
        """
        caplog.set_level(logging.WARNING)

        for j in [1, 2, 3]:
            job_id = f"job-{j}"
            user_id = f"user{j}"
            app_id = k8s_job_name(job_id=job_id, user_id=user_id)

            zk_job_registry.register(
                job_id=job_id,
                user_id=user_id,
                api_version="1.2.3",
                specification=DUMMY_PROCESS_1,
            )
            zk_job_registry.set_application_id(
                job_id=job_id, user_id=user_id, application_id=app_id
            )
            elastic_job_registry.create_job(
                job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1
            )
            # K8s apps 1 and 3 are running but app 2 is lost/missing
            if j != 2:
                k8s_mock.submit(app_id=app_id, state=YARN_STATE.RUNNING)
            else:
                k8s_mock.corrupt_app_ids.add(app_id)

        # Let job tracker do status updates
        job_tracker.update_statuses()

        assert zk_job_registry.get_job(job_id="job-1", user_id="user1") == DictSubSet(
            {"status": "running"}
        )
        assert zk_job_registry.get_job(job_id="job-2", user_id="user2") == DictSubSet(
            # job-2 is currently stuck in state "created"
            {"status": "created"}
        )
        assert zk_job_registry.get_job(job_id="job-3", user_id="user3") == DictSubSet(
            {"status": "running"}
        )

        assert elastic_job_registry.db == {
            "job-1": DictSubSet(status="running"),
            # job-2 is currently stuck in state "created"
            "job-2": DictSubSet(status="created"),
            "job-3": DictSubSet(status="running"),
        }

        assert caplog.record_tuples == [
            (
                "openeogeotrellis.job_tracker_v2",
                40,
                "Failed status sync for job_id='job-2': unexpected ApiException: (500)\nReason: Internal Server Error\n",
            )
        ]


class TestCliApp:
    def test_run_basic_help(self, pytester):
        command = [sys.executable, "-m", "openeogeotrellis.job_tracker_v2", "-h"]
        run_result = pytester.run(*command)
        assert run_result.ret == 0
        assert "JobTracker" in run_result.stdout.str()
        assert "--app-cluster" in run_result.stdout.str()
        assert run_result.errlines == []

    def test_run_basic_fail(self, pytester):
        command = [sys.executable, "-m", "openeogeotrellis.job_tracker_v2", "--foobar"]
        run_result = pytester.run(*command)
        assert run_result.ret == pytest.ExitCode.INTERRUPTED
        assert run_result.outlines == []
        assert "unrecognized arguments: --foobar" in run_result.stderr.str()
