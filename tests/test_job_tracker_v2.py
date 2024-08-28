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
import requests_mock
from openeo.util import rfc3339, url_join
from openeo_driver.testing import DictSubSet
from openeo_driver.utils import generate_unique_id

from openeogeotrellis.integrations.kubernetes import K8S_SPARK_APP_STATE, k8s_job_name
from openeogeotrellis.integrations.prometheus import Prometheus
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
from openeogeotrellis.testing import KazooClientMock, gps_config_overrides
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
        self.set_finish_time()
        # TODO: what is the meaning actually of state=FINISHED + final-state=FAILED?
        return self.set_state(YARN_STATE.FINISHED, final_state)

    def set_finish_time(self, force: bool = False):
        if not self.finish_time or force:
            self.finish_time = int(1000 * time.time())

    def set_failed(self):
        self.set_finish_time()
        return self.set_state(YARN_STATE.FAILED, YARN_FINAL_STATUS.FAILED)

    def set_killed(self):
        self.set_finish_time()
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

    def set_state(self, state: str):
        self.state = state

    def set_submitted(self):
        if not self.start_time:
            self.start_time = rfc3339.datetime(dt.datetime.utcnow())
        self.state = K8S_SPARK_APP_STATE.SUBMITTED

    def set_running(self):
        self.state = K8S_SPARK_APP_STATE.RUNNING

    def set_completed(self):
        self.set_finish_time()
        self.state = K8S_SPARK_APP_STATE.COMPLETED

    def set_failed(self):
        self.set_finish_time()
        self.state = K8S_SPARK_APP_STATE.FAILED

    def set_finish_time(self, force: bool = False):
        if not self.finish_time or force:
            self.finish_time = rfc3339.datetime(dt.datetime.utcnow())


class KubernetesMock:
    """Kubernetes cluster mock."""

    def __init__(self):
        self.apps: Dict[str, KubernetesAppInfo] = {}
        self.corrupt_app_ids = set()

    def get_namespaced_custom_object(self, name: str, **kwargs) -> dict:
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
DUMMY_JOB_OPTIONS = {"speed": "fast"}


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
        job_tracker = JobTracker(
            app_state_getter=YarnStatusGetter(ConfigParams().yarn_rest_api_base_url),
            zk_job_registry=zk_job_registry,
            principal="john@EXAMPLE.TEST",
            keytab="test/openeo.keytab",
            job_costs_calculator=job_costs_calculator,
            output_root_dir=batch_job_output_root,
            elastic_job_registry=elastic_job_registry,
        )
        return job_tracker

    @pytest.mark.parametrize("job_options", [None, DUMMY_JOB_OPTIONS])
    def test_yarn_zookeeper_basic(
        self,
        zk_job_registry,
        yarn_mock,
        job_tracker,
        elastic_job_registry,
        caplog,
        time_machine,
        job_costs_calculator,
        job_options,
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
            specification=ZkJobRegistry.build_specification_dict(process_graph=DUMMY_PG_1, job_options=job_options),
        )
        elastic_job_registry.create_job(
            job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1, job_options=job_options
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
            data={
                "foo": "bar",
                "usage": {"input_pixel": {"unit": "mega-pixel", "value": 1.125}}
            },
        )
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "finished",
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:04:40Z",  # TODO: get this working?,
                "foo": "bar",
                "usage": {
                    "input_pixel": {"unit": "mega-pixel", "value": 1.125},
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
                "usage": {
                    "input_pixel": {"unit": "mega-pixel", "value": 1.125},
                    "cpu": {"unit": "cpu-seconds", "value": 32},
                    "memory": {"unit": "mb-seconds", "value": 1234},
                },
                "costs": 129.95
            }
        )

        calculate_costs_calls = job_costs_calculator.calculate_costs.call_args_list
        assert len(calculate_costs_calls) == 1
        (costs_details,), _ = calculate_costs_calls[0]

        assert costs_details == CostsDetails(
            job_id=job_id,
            user_id=user_id,
            execution_id=yarn_app.app_id,
            app_state_etl_api_deprecated="FINISHED",
            job_status="finished",
            area_square_meters=None,
            job_title=None,
            start_time=dt.datetime(2022, 12, 14, 12, 3, 30),
            finish_time=dt.datetime(2022, 12, 14, 12, 4, 40),
            cpu_seconds=32,
            mb_seconds=1234,
            sentinelhub_processing_units=None,
            unique_process_ids=[],
            job_options=job_options,
            additional_credits_cost=None,
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
                specification=ZkJobRegistry.build_specification_dict(
                    process_graph=DUMMY_PG_1, job_options=DUMMY_JOB_OPTIONS
                ),
            )
            zk_job_registry.set_application_id(
                job_id=job_id, user_id=user_id, application_id=app_id
            )
            elastic_job_registry.create_job(
                job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1, job_options=DUMMY_JOB_OPTIONS
            )
            # YARN apps 1 and 3 are running but app 2 is lost/missing
            if j != 2:
                yarn_mock.submit(app_id=app_id, state=YARN_STATE.RUNNING)

        # Let job tracker do status updates
        job_tracker.update_statuses()

        assert zk_job_registry.get_job(job_id="job-1", user_id="user1") == DictSubSet({"status": "running"})
        assert zk_job_registry.get_job(job_id="job-2", user_id="user2") == DictSubSet({"status": "created"})
        assert zk_job_registry.get_job(job_id="job-3", user_id="user3") == DictSubSet({"status": "running"})

        assert elastic_job_registry.db == {
            "job-1": DictSubSet(status="running"),
            "job-2": DictSubSet(status="created"),
            "job-3": DictSubSet(status="running"),
        }

        assert caplog.record_tuples == [
            (
                "openeogeotrellis.job_tracker_v2",
                logging.ERROR,
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
                specification=ZkJobRegistry.build_specification_dict(
                    process_graph=DUMMY_PG_1, job_options=DUMMY_JOB_OPTIONS
                ),
            )
            zk_job_registry.set_application_id(
                job_id=f"job-{j}", user_id=f"user{j}", application_id=f"app-{j}"
            )
            elastic_job_registry.create_job(
                job_id=f"job-{j}", user_id=f"user{j}", process=DUMMY_PROCESS_1, job_options=DUMMY_JOB_OPTIONS
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
            specification=ZkJobRegistry.build_specification_dict(
                process_graph=DUMMY_PG_1, job_options=DUMMY_JOB_OPTIONS
            ),
        )
        elastic_job_registry.create_job(
            job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1, job_options=DUMMY_JOB_OPTIONS
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

        diagnostics_log_records = [record for record in caplog.records if yarn_app.diagnostics in record.msg]
        assert len(diagnostics_log_records) > 0
        assert all(r.levelname == "ERROR" and r.job_id == "job-123" and r.user_id == "john"
                   for r in diagnostics_log_records)

    def test_yarn_zookeeper_stats(
        self,
        zk_job_registry,
        elastic_job_registry,
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
                specification=ZkJobRegistry.build_specification_dict(
                    process_graph=DUMMY_PG_1, job_options=DUMMY_JOB_OPTIONS
                ),
            )
            zk_job_registry.set_application_id(
                job_id=job_id, user_id=user_id, application_id=app_id
            )
            elastic_job_registry.create_job(
                job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1, job_options=DUMMY_JOB_OPTIONS
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
            "skip: app not found": 1,
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
            "collected jobs": 3,
            "job with previous_status='created'": 1,
            "job with previous_status='running'": 2,
            "skip: app not found": 1,
            "get metadata attempt": 3,
            "new metadata": 2,
            "status same": 2,
            "status same 'running'": 2,
        }

    @pytest.mark.parametrize("job_options", [None, DUMMY_JOB_OPTIONS])
    @pytest.mark.parametrize(
        ["yarn_state", "yarn_final_state", "expected_etl_state", "expected_job_status"],
        [
            (YARN_STATE.FINISHED, YARN_FINAL_STATUS.SUCCEEDED, "FINISHED", "finished"),
            (
                YARN_STATE.FINISHED,
                YARN_FINAL_STATUS.FAILED,
                "FINISHED",  # TODO #565 should be "etl state" "FAILED"
                "error",
            ),
            (
                YARN_STATE.FINISHED,
                YARN_FINAL_STATUS.KILLED,
                "FINISHED",  # TODO #565 should be "etl state" "KILLED"?
                "canceled",
            ),
            (YARN_STATE.KILLED, YARN_FINAL_STATUS.KILLED, "KILLED", "canceled"),
            (YARN_STATE.FAILED, YARN_FINAL_STATUS.FAILED, "FAILED", "error"),
        ],
    )
    def test_yarn_zookeeper_job_cost(
        self,
        zk_job_registry,
        yarn_mock,
        job_tracker,
        elastic_job_registry,
        caplog,
        time_machine,
        job_costs_calculator,
        job_options,
        yarn_state,
        yarn_final_state,
        expected_etl_state,
        expected_job_status,
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
            specification=ZkJobRegistry.build_specification_dict(process_graph=DUMMY_PG_1, job_options=job_options),
        )
        elastic_job_registry.create_job(
            job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1, job_options=job_options
        )

        # Start job: submit app to YARN and set running
        time_machine.coordinates.shift(70)
        yarn_app = yarn_mock.submit(app_id="app-123")
        zk_job_registry.set_application_id(job_id=job_id, user_id=user_id, application_id=yarn_app.app_id)
        yarn_app.set_running()
        job_tracker.update_statuses()

        # Set end state in YARN
        time_machine.coordinates.shift(70)
        yarn_app.set_state(yarn_state, yarn_final_state)
        yarn_app.set_finish_time()
        json_write(
            path=job_tracker._batch_jobs.get_results_metadata_path(job_id=job_id),
            data={"foo": "bar", "usage": {"input_pixel": {"unit": "mega-pixel", "value": 1.125}}},
        )
        job_tracker.update_statuses()

        calculate_costs_calls = job_costs_calculator.calculate_costs.call_args_list
        assert len(calculate_costs_calls) == 1
        (costs_details,), _ = calculate_costs_calls[0]

        assert costs_details == CostsDetails(
            job_id=job_id,
            user_id=user_id,
            execution_id=yarn_app.app_id,
            app_state_etl_api_deprecated=expected_etl_state,
            job_status=expected_job_status,
            area_square_meters=None,
            job_title=None,
            start_time=dt.datetime(2022, 12, 14, 12, 1, 10),
            finish_time=dt.datetime(2022, 12, 14, 12, 2, 20),
            cpu_seconds=32,
            mb_seconds=1234,
            sentinelhub_processing_units=None,
            unique_process_ids=[],
            job_options=job_options,
        )

        assert caplog.record_tuples == []


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

        job_metadata = YarnStatusGetter.parse_application_response(data=response, job_id="j-abc123", user_id="johndoe")
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

        job_metadata = YarnStatusGetter.parse_application_response(data=response, job_id="j-abc123", user_id="johndoe")
        assert job_metadata.status == "running"
        assert job_metadata.start_time == dt.datetime(2023, 1, 6, 16, 14, 32, 793000)
        assert job_metadata.finish_time is None
        assert job_metadata.usage.cpu_seconds == 46964
        assert job_metadata.usage.mb_seconds == 96183879

    def test_parse_application_response_empty(self):
        with pytest.raises(YarnAppReportParseException):
            YarnStatusGetter.parse_application_response(data={}, job_id="j-abc123", user_id="johndoe")

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
    def k8s_mock(self) -> KubernetesMock:
        return KubernetesMock()

    @pytest.fixture
    def prometheus_mock(self):
        prometheus_mock = mock.Mock()
        prometheus_mock.endpoint = "https://prometheus.test/api/v1"
        prometheus_mock.get_cpu_usage.return_value = 2.34 * 3600
        prometheus_mock.get_network_received_usage.return_value = 370841160371254.75
        prometheus_mock.get_memory_usage.return_value = 5.678 * 1024 * 1024 * 3600
        prometheus_mock.get_max_executor_memory_usage.return_value = 3.5

        return prometheus_mock

    @pytest.fixture
    def job_tracker(
        self,
        zk_job_registry,
        elastic_job_registry,
        batch_job_output_root,
        k8s_mock,
        prometheus_mock,
        job_costs_calculator
    ) -> JobTracker:
        job_tracker = JobTracker(
            app_state_getter=K8sStatusGetter(k8s_mock, prometheus_mock),
            zk_job_registry=zk_job_registry,
            principal="john@EXAMPLE.TEST",
            keytab="test/openeo.keytab",
            job_costs_calculator=job_costs_calculator,
            output_root_dir=batch_job_output_root,
            elastic_job_registry=elastic_job_registry,
        )
        return job_tracker

    @pytest.mark.parametrize("job_options", [None, DUMMY_JOB_OPTIONS])
    def test_k8s_zookeeper_basic(
        self,
        zk_job_registry,
        job_tracker,
        elastic_job_registry,
        caplog,
        time_machine,
        k8s_mock,
        job_costs_calculator,
        job_options,
    ):
        caplog.set_level(logging.WARNING)
        time_machine.move_to("2022-12-14T12:00:00Z", tick=False)

        user_id = "john"
        job_id = "job-123"
        zk_job_registry.register(
            job_id=job_id,
            user_id=user_id,
            api_version="1.2.3",
            specification=ZkJobRegistry.build_specification_dict(process_graph=DUMMY_PG_1, job_options=job_options),
        )
        elastic_job_registry.create_job(
            job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1, job_options=job_options
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
        app_id = k8s_job_name()
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
            data={
                "foo": "bar",
                "usage": {"input_pixel": {"unit": "mega-pixel", "value": 1.125},
                          "sentinelhub": {"unit": "sentinelhub_processing_unit", "value": 1.25},}
            },
        )
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "finished",
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:04:40Z",  # TODO: get this working?
                "foo": "bar",
                "usage": {
                    "input_pixel": {"unit": "mega-pixel", "value": 1.125},
                    "max_executor_memory": {"unit": "gb", "value": 3.5},
                    "cpu": {"unit": "cpu-seconds", "value": pytest.approx(2.34 * 3600, rel=0.001)},
                    "memory": {"unit": "mb-seconds", "value": pytest.approx(5.678 * 3600, rel=0.001)},
                    "network_received": {"unit": "b", "value": pytest.approx(370841160371254.75, rel=0.001)},
                    "sentinelhub": {"unit": "sentinelhub_processing_unit", "value": 1.25},
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
                "usage": {
                    "input_pixel": {"unit": "mega-pixel", "value": 1.125},
                    "max_executor_memory": {"unit": "gb", "value": 3.5},
                    "cpu": {"unit": "cpu-seconds", "value": pytest.approx(2.34 * 3600, rel=0.001)},
                    "memory": {"unit": "mb-seconds", "value": pytest.approx(5.678 * 3600, rel=0.001)},
                    "network_received": {"unit": "b", "value": pytest.approx(370841160371254.75, rel=0.001)},
                    "sentinelhub": {"unit": "sentinelhub_processing_unit", "value": 1.25},
                },
                "costs": 129.95
            }
        )

        calculate_costs_calls = job_costs_calculator.calculate_costs.call_args_list
        assert len(calculate_costs_calls) == 1
        (costs_details,), _ = calculate_costs_calls[0]

        assert costs_details == CostsDetails(
            job_id=job_id,
            user_id=user_id,
            execution_id=kube_app.app_id,
            app_state_etl_api_deprecated="FINISHED",
            job_status="finished",
            area_square_meters=None,
            job_title=None,
            start_time=dt.datetime(2022, 12, 14, 12, 1, 10),
            finish_time=dt.datetime(2022, 12, 14, 12, 3, 30),
            cpu_seconds=pytest.approx(2.34 * 3600, rel=0.001),
            mb_seconds=pytest.approx(5.678 * 3600, rel=0.001),
            sentinelhub_processing_units=1.25,
            unique_process_ids=[],
            job_options=job_options,
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
            specification=ZkJobRegistry.build_specification_dict(
                process_graph=DUMMY_PG_1, job_options=DUMMY_JOB_OPTIONS
            ),
        )
        elastic_job_registry.create_job(
            job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1, job_options=DUMMY_JOB_OPTIONS
        )

        def zk_job_info() -> dict:
            return zk_job_registry.get_job(job_id=job_id, user_id=user_id)

        # Submit Kubernetes app
        time_machine.coordinates.shift(70)
        app_id = k8s_job_name()
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

        app_ids = {}

        for j in [1, 2, 3]:
            job_id = f"job-{j}"
            user_id = f"user{j}"
            app_ids[j] = app_id = k8s_job_name()

            zk_job_registry.register(
                job_id=job_id,
                user_id=user_id,
                api_version="1.2.3",
                specification=ZkJobRegistry.build_specification_dict(
                    process_graph=DUMMY_PG_1, job_options=DUMMY_JOB_OPTIONS
                ),
            )
            zk_job_registry.set_application_id(
                job_id=job_id, user_id=user_id, application_id=app_id
            )
            elastic_job_registry.create_job(
                job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1, job_options=DUMMY_JOB_OPTIONS
            )
            # K8s apps 1 and 3 are running but app 2 is lost/missing
            if j != 2:
                k8s_mock.submit(app_id=app_id, state=YARN_STATE.RUNNING)

        # Let job tracker do status updates
        job_tracker.update_statuses()

        assert zk_job_registry.get_job(job_id="job-1", user_id="user1") == DictSubSet({"status": "running"})
        assert zk_job_registry.get_job(job_id="job-2", user_id="user2") == DictSubSet({"status": "created"})
        assert zk_job_registry.get_job(job_id="job-3", user_id="user3") == DictSubSet({"status": "running"})

        assert elastic_job_registry.db == {
            "job-1": DictSubSet(status="running"),
            "job-2": DictSubSet(status="created"),
            "job-3": DictSubSet(status="running"),
        }

        assert caplog.record_tuples == [
            (
                "openeogeotrellis.job_tracker_v2",
                logging.ERROR,
                f"App not found: job_id='job-2' application_id='{app_ids[2]}'",
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
            app_id = k8s_job_name()

            zk_job_registry.register(
                job_id=job_id,
                user_id=user_id,
                api_version="1.2.3",
                specification=ZkJobRegistry.build_specification_dict(
                    process_graph=DUMMY_PG_1, job_options=DUMMY_JOB_OPTIONS
                ),
            )
            zk_job_registry.set_application_id(
                job_id=job_id, user_id=user_id, application_id=app_id
            )
            elastic_job_registry.create_job(
                job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1, job_options=DUMMY_JOB_OPTIONS
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

    @pytest.mark.parametrize("job_options", [None, DUMMY_JOB_OPTIONS])
    @pytest.mark.parametrize(
        ["k8s_app_state", "expected_etl_state", "expected_job_status"],
        [
            (K8S_SPARK_APP_STATE.COMPLETED, "FINISHED", "finished"),
            (K8S_SPARK_APP_STATE.FAILED, "FAILED", "error"),
            (K8S_SPARK_APP_STATE.SUBMISSION_FAILED, "FAILED", "error"),
            (K8S_SPARK_APP_STATE.FAILING, "FAILED", "error"),
        ],
    )
    @pytest.mark.parametrize("report_usage_sentinelhub_pus", [True, False], ids=["include_shpu", "exclude_shpu"])
    @pytest.mark.parametrize("batch_job_base_fee_credits", [None, 1.23])
    def test_k8s_zookeeper_job_cost(
        self,
        zk_job_registry,
        job_tracker,
        elastic_job_registry,
        caplog,
        time_machine,
        k8s_mock,
        job_costs_calculator,
        job_options,
        k8s_app_state,
        expected_etl_state,
        expected_job_status,
        report_usage_sentinelhub_pus,
        batch_job_base_fee_credits,
    ):
        with gps_config_overrides(
            report_usage_sentinelhub_pus=report_usage_sentinelhub_pus,
            batch_job_base_fee_credits=batch_job_base_fee_credits,
        ):
            caplog.set_level(logging.WARNING)
            time_machine.move_to("2022-12-14T12:00:00Z", tick=False)

            user_id = "john"
            job_id = "job-123"
            zk_job_registry.register(
                job_id=job_id,
                user_id=user_id,
                api_version="1.2.3",
                specification=ZkJobRegistry.build_specification_dict(process_graph=DUMMY_PG_1, job_options=job_options),
            )
            elastic_job_registry.create_job(
                job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1, job_options=job_options
            )

            # Submit Kubernetes app and set running
            time_machine.coordinates.shift(70)
            app_id = k8s_job_name()
            kube_app = k8s_mock.submit(app_id=app_id)
            zk_job_registry.set_application_id(job_id=job_id, user_id=user_id, application_id=app_id)
            kube_app.set_submitted()
            kube_app.set_running()
            job_tracker.update_statuses()

            # Set COMPLETED IN Kubernetes
            time_machine.coordinates.shift(70)
            kube_app.set_state(k8s_app_state)
            kube_app.set_finish_time()
            json_write(
                path=job_tracker._batch_jobs.get_results_metadata_path(job_id=job_id),
                data={
                    "usage": {
                        "input_pixel": {"unit": "mega-pixel", "value": 1.125},
                        "sentinelhub": {"unit": "sentinelhub_processing_unit", "value": 1.25},
                    }
                },
            )
            job_tracker.update_statuses()

            calculate_costs_calls = job_costs_calculator.calculate_costs.call_args_list
            assert len(calculate_costs_calls) == 1
            (costs_details,), _ = calculate_costs_calls[0]

            assert costs_details == CostsDetails(
                job_id=job_id,
                user_id=user_id,
                execution_id=kube_app.app_id,
                app_state_etl_api_deprecated=expected_etl_state,
                job_status=expected_job_status,
                area_square_meters=None,
                job_title=None,
                start_time=dt.datetime(2022, 12, 14, 12, 1, 10),
                finish_time=dt.datetime(2022, 12, 14, 12, 2, 20),
                cpu_seconds=pytest.approx(2.34 * 3600, rel=0.001),
                mb_seconds=pytest.approx(5.678 * 3600, rel=0.001),
                sentinelhub_processing_units=1.25 if report_usage_sentinelhub_pus else None,
                additional_credits_cost=batch_job_base_fee_credits,
                unique_process_ids=[],
                job_options=job_options,
            )

            assert caplog.record_tuples == []

    def test_k8s_no_zookeeper(self, k8s_mock, prometheus_mock, job_costs_calculator, batch_job_output_root,
                              elastic_job_registry, caplog, time_machine):
        job_tracker = JobTracker(
            app_state_getter=K8sStatusGetter(k8s_mock, prometheus_mock),
            zk_job_registry=None,
            principal="john@EXAMPLE.TEST",
            keytab="test/openeo.keytab",
            job_costs_calculator=job_costs_calculator,
            output_root_dir=batch_job_output_root,
            elastic_job_registry=elastic_job_registry,
        )

        caplog.set_level(logging.WARNING)
        time_machine.move_to("2022-12-14T12:00:00Z", tick=False)

        user_id = "john"
        job_id = "job-123"
        elastic_job_registry.create_job(
            job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1,
        )

        # Check initial status in registry
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
        app_id = k8s_job_name()
        kube_app = k8s_mock.submit(app_id=app_id)
        kube_app.set_submitted()
        elastic_job_registry.set_application_id(job_id=job_id, application_id=app_id)

        # Trigger `update_statuses`
        job_tracker.update_statuses()
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
            data={
                "foo": "bar",
                "usage": {"input_pixel": {"unit": "mega-pixel", "value": 1.125},
                          "sentinelhub": {"unit": "sentinelhub_processing_unit", "value": 1.25},
                          },
                "b0rked": [24, ["a", {"b": [float('nan'), None]}], {"c": [float('inf'), 1.23]}],
            },
        )
        job_tracker.update_statuses()
        assert elastic_job_registry.db[job_id] == DictSubSet(
            {
                "status": "finished",
                "created": "2022-12-14T12:00:00Z",
                "updated": "2022-12-14T12:03:30Z",
                "started": "2022-12-14T12:01:10Z",
                "finished": "2022-12-14T12:03:30Z",
                "usage": {
                    "input_pixel": {"unit": "mega-pixel", "value": 1.125},
                    "cpu": {"unit": "cpu-seconds", "value": pytest.approx(2.34 * 3600, rel=0.001)},
                    "memory": {"unit": "mb-seconds", "value": pytest.approx(5.678 * 3600, rel=0.001)},
                    "network_received": {"unit": "b", "value": pytest.approx(370841160371254.75, rel=0.001)},
                    "sentinelhub": {"unit": "sentinelhub_processing_unit", "value": 1.25},
                    "max_executor_memory": {"unit": "gb", "value": 3.5},
                },
                "costs": 129.95,
                "results_metadata": {
                    "foo": "bar",
                    "usage": {"input_pixel": {"unit": "mega-pixel", "value": 1.125},
                              "sentinelhub": {"unit": "sentinelhub_processing_unit", "value": 1.25},
                              },
                    "b0rked": [24, ["a", {"b": ['nan', None]}], {"c": ['inf', 1.23]}],
                },
            }
        )

        json.dumps(elastic_job_registry.db[job_id], allow_nan=False)

        assert caplog.record_tuples == []


class TestK8sStatusGetter:
    def test_cpu_and_memory_usage_not_in_prometheus(self, caplog):
        caplog.set_level(logging.WARNING)

        prometheus_mock = mock.Mock(Prometheus)
        prometheus_mock.get_cpu_usage.return_value = None
        prometheus_mock.get_memory_usage.return_value = 0.0
        prometheus_mock.endpoint = "https://prometheus.test/api/v1"

        k8s_mock = mock.Mock()
        k8s_mock.get_namespaced_custom_object.return_value = {
            "status": {
                "applicationState": {"state": K8S_SPARK_APP_STATE.COMPLETED},
                "lastSubmissionAttemptTime": rfc3339.datetime(dt.datetime.utcnow()),
                "terminationTime": rfc3339.datetime(dt.datetime.utcnow()),
            }
        }

        k8s_status_getter = K8sStatusGetter(k8s_mock, prometheus_mock)

        user_id = "john"
        job_id = "job-123"
        app_id = k8s_job_name()
        job_metadata = k8s_status_getter.get_job_metadata(job_id=job_id, user_id=user_id, app_id=app_id)

        assert (
                   "openeogeotrellis.job_tracker_v2",
                   logging.WARNING,
                   f"App {app_id} took 0.0s but no CPU or memory usage was recorded: "
                   f"cpu_seconds=None and byte_seconds=0.0",
               ) in caplog.record_tuples

        assert job_metadata.usage.cpu_seconds == 1.5 * 3600
        assert job_metadata.usage.mb_seconds == 3 * 3600 * 1024


class TestCliApp:
    def test_run_basic_help(self, pytester):
        command = [sys.executable, "-m", "openeogeotrellis.job_tracker_v2", "-h"]
        run_result = pytester.run(*command)
        assert run_result.ret == 0
        assert "JobTracker" in run_result.stdout.str()
        assert "--app-cluster" in run_result.stdout.str()
        assert "--run-id" in run_result.stdout.str()
        assert run_result.errlines == []

    def test_run_basic_fail(self, pytester):
        command = [sys.executable, "-m", "openeogeotrellis.job_tracker_v2", "--foobar"]
        run_result = pytester.run(*command)
        assert run_result.ret == pytest.ExitCode.INTERRUPTED
        assert run_result.outlines == []
        assert "unrecognized arguments: --foobar" in run_result.stderr.str()

    def test_run_rotating_log(self, pytester, tmp_path):
        rotating_log = tmp_path / "tracker.log"
        assert not rotating_log.exists()
        command = [
            sys.executable,
            "-m",
            "openeogeotrellis.job_tracker_v2",
            "--app-cluster",
            "broken-dummy",
            "--rotating-log",
            str(rotating_log),
            "--run-id",
            "run Forrest run",
        ]
        run_result = pytester.run(*command)
        assert run_result.ret == 1
        assert rotating_log.exists()
        # Some basic checks that we got logs in JSON Lines format
        logs = [n for n in rotating_log.read_text().split("\n") if n.strip()]
        assert len(logs) > 5
        for log in logs:
            log = json.loads(log)
            assert {"levelname", "name", "message", "run_id"}.issubset(log.keys())
            assert log["run_id"] == "run Forrest run"

    @pytest.mark.parametrize("run_id", [None, "run Forrest run"])
    def test_run_id(self, pytester, run_id):
        command = [
            sys.executable,
            "-m",
            "openeogeotrellis.job_tracker_v2",
            "--app-cluster",
            "broken-dummy",
        ]

        if run_id:
            command.extend([
                "--run-id",
                "run Forrest run",
            ])

        run_result = pytester.run(*command)
        assert run_result.ret == 1

        logs = run_result.errlines
        for log in logs:
            log = json.loads(log)
            assert log.get("run_id") == run_id
