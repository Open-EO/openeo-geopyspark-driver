import contextlib
import datetime as dt
import json
import logging
import re
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union
from unittest import mock

import kubernetes
import pytest
import re_assert
import requests_mock
from openeo.util import rfc3339, url_join
from openeo_driver.jobregistry import JOB_STATUS
from openeo_driver.testing import DictSubSet
from openeo_driver.utils import generate_unique_id

from openeogeotrellis.integrations.kubernetes import K8S_SPARK_APP_STATE, k8s_job_name
from openeogeotrellis.integrations.yarn import YARN_FINAL_STATUS, YARN_STATE
from openeogeotrellis.job_registry import ZkJobRegistry, InMemoryJobRegistry
from openeogeotrellis.job_tracker_v2 import (
    JobTracker,
    K8sStatusGetter,
    YarnAppReportParseException,
    YarnStatusGetter,
)
from openeogeotrellis.testing import KazooClientMock
from openeogeotrellis.utils import json_write

# TODO: move YARN related mocks to openeogeotrellis.testing


@dataclass
class YarnAppInfo:
    """Dummy YARN app metadata."""

    app_id: str
    user_id: str = "johndoe"
    queue: str = "default"
    start_time: int = 0
    finish_time: int = 0
    progress: str = "0%"
    state: str = YARN_STATE.SUBMITTED
    final_state: str = YARN_FINAL_STATUS.UNDEFINED
    aggregate_resource_allocation: Tuple[int, int] = (1234, 32)

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

    def status_report(self) -> str:
        fields = [
            f"\t{k} : {v}"
            for k, v in [
                ("Application-Id", self.app_id),
                ("User", self.user_id),
                ("Queue", self.queue),
                ("Start-Time", self.start_time),
                ("Finish-Time", self.finish_time),
                ("Progress", self.progress),
                ("State", self.state),
                ("Final-State", self.final_state),
                (
                    "Aggregate Resource Allocation",
                    "{} MB-seconds, {} vcore-seconds".format(
                        *self.aggregate_resource_allocation
                    ),
                ),
            ]
        ]
        return "\n".join(["Application Report : "] + fields + [""])


class YarnMock:
    """YARN cluster mock"""

    def __init__(self):
        self.apps: Dict[str, YarnAppInfo] = {}
        self.corrupt_app_ids = set()

    def submit(self, app_id: Optional[str] = None, **kwargs) -> YarnAppInfo:
        """Create a new (dummy) YARN app"""
        if app_id is None:
            app_id = generate_unique_id(prefix="app")
        self.apps[app_id] = app = YarnAppInfo(app_id=app_id, **kwargs)
        return app

    def _check_output(self, popenargs: List[str], **kwargs):
        """Mock for subprocess.check_output(["yarn", ...])"""
        if len(popenargs) == 4 and popenargs[:3] == ["yarn", "application", "-status"]:
            app_id = popenargs[3]
            if app_id in self.corrupt_app_ids:
                raise subprocess.CalledProcessError(
                    returncode=255,
                    cmd=popenargs,
                    output=f"C0rRup7! {app_id}'".encode("utf-8"),
                )
            elif app_id in self.apps:
                return self.apps[app_id].status_report().encode("utf-8")
            else:
                raise subprocess.CalledProcessError(
                    returncode=255,
                    cmd=popenargs,
                    output=f"Application with id '{app_id}' doesn't exist in RM or Timeline Server.".encode(
                        "utf-8"
                    ),
                )

        raise RuntimeError(f"Unsupported check_output({popenargs!r})")

    @contextlib.contextmanager
    def patch(self):
        with mock.patch(
            "subprocess.check_output", new=self._check_output
        ) as check_output:
            yield check_output


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
        self, zk_job_registry, elastic_job_registry, batch_job_output_root
    ) -> JobTracker:
        principal = "john@EXAMPLE.TEST"
        keytab = "test/openeo.keytab"
        job_tracker = JobTracker(
            app_state_getter=YarnStatusGetter(),
            zk_job_registry=zk_job_registry,
            principal=principal,
            keytab=keytab,
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
                # "updated": "2022-12-14T12:04:40Z",  # TODO: get this working?
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
                "Failed status sync for job_id='job-2': unexpected CalledProcessError: Command '['yarn', 'application', '-status', 'app-2']' returned non-zero exit status 255.",
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
    def test_parse_application_report_basic(self):
        report = textwrap.dedent(
            """
            Application Report :
            \tApplication-Id : application_1671092799310_26739
            \tApplication-Name : openEO batch_test_random_forest_train_and_load_from_jobid-user jenkins
            \tApplication-Type : SPARK
            \tUser : jenkins
            \tQueue : default
            \tApplication Priority : 0
            \tStart-Time : 1673021672793
            \tFinish-Time : 1673021943245
            \tProgress : 100%
            \tState : FINISHED
            \tFinal-State : SUCCEEDED
            \tAM Host : epod0123.test
            \tAggregate Resource Allocation : 5116996 MB-seconds, 2265 vcore-seconds
            \tAggregate Resource Preempted : 0 MB-seconds, 0 vcore-seconds
            \tTimeoutType : LIFETIME	ExpiryTime : UNLIMITED	RemainingTime : -1seconds
        """
        )
        job_metadata = YarnStatusGetter().parse_application_report(report=report)
        assert job_metadata.status == "finished"
        assert job_metadata.start_time == "2023-01-06T16:14:32Z"
        assert job_metadata.finish_time == "2023-01-06T16:19:03Z"
        assert job_metadata.usage == {
            "cpu": {"unit": "cpu-seconds", "value": 2265},
            "memory": {"unit": "mb-seconds", "value": 5116996},
        }

    def test_parse_application_report_running(self):
        report = textwrap.dedent(
            """
            Application Report :
            \tApplication-Id : application_1671092799310_26739
            \tStart-Time : 1673021672793
            \tFinish-Time : 0
            \tState : RUNNING
            \tFinal-State : UNDEFINED
            \tAM Host : epod0123.test
            \tAggregate Resource Allocation : 96183879 MB-seconds, 46964 vcore-seconds
            \tAggregate Resource Preempted : 0 MB-seconds, 0 vcore-seconds
        """
        )
        job_metadata = YarnStatusGetter().parse_application_report(report=report)
        assert job_metadata.status == "running"
        assert job_metadata.start_time == "2023-01-06T16:14:32Z"
        assert job_metadata.finish_time is None
        assert job_metadata.usage == {
            "cpu": {"unit": "cpu-seconds", "value": 46964},
            "memory": {"unit": "mb-seconds", "value": 96183879},
        }

    def test_parse_application_report_empty(self):
        with pytest.raises(YarnAppReportParseException):
            _ = YarnStatusGetter().parse_application_report(report="")


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
    ) -> JobTracker:
        principal = "john@EXAMPLE.TEST"
        keytab = "test/openeo.keytab"
        job_tracker = JobTracker(
            app_state_getter=K8sStatusGetter(kubecost_url=kubecost_url),
            zk_job_registry=zk_job_registry,
            principal=principal,
            keytab=keytab,
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
                    "cpu": {"unit": "cpu-hours", "value": 2.34},
                    "memory": {"unit": "mb-hours", "value": 5.678},
                },
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
