import contextlib
import datetime as dt
import logging
import subprocess
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union
from unittest import mock

import kubernetes
import pytest
import requests_mock

from openeo.util import rfc3339, url_join
from openeo_driver.jobregistry import JOB_STATUS
from openeo_driver.testing import DictSubSet
from openeo_driver.utils import generate_unique_id
from openeogeotrellis.integrations.kubernetes import k8s_job_name
from openeogeotrellis.job_registry import ZkJobRegistry
from openeogeotrellis.job_tracker import JobTracker
from openeogeotrellis.testing import KazooClientMock
from openeogeotrellis.utils import json_write


# TODO: move YARN related mocks to openeogeotrellis.testing


class YARN_STATE:
    # From https://hadoop.apache.org/docs/r3.1.1/api/org/apache/hadoop/yarn/api/records/YarnApplicationState.html
    # TODO: move this to openeogeotrellis.job_tracker
    ACCEPTED = "ACCEPTED"
    FAILED = "FAILED"
    FINISHED = "FINISHED"
    KILLED = "KILLED"
    NEW = "NEW"
    NEW_SAVING = "NEW_SAVING"
    RUNNING = "RUNNING"
    SUBMITTED = "SUBMITTED"


class YARN_FINAL_STATUS:
    # From https://hadoop.apache.org/docs/r3.1.1/api/org/apache/hadoop/yarn/api/records/FinalApplicationStatus.html
    # TODO: move this to openeogeotrellis.job_tracker
    ENDED = "ENDED"
    FAILED = "FAILED"
    KILLED = "KILLED"
    SUCCEEDED = "SUCCEEDED"
    UNDEFINED = "UNDEFINED"


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
        self._corrupt = set()

    def submit(self, app_id: Optional[str] = None, **kwargs) -> YarnAppInfo:
        """Create a new (dummy) YARN app"""
        if app_id is None:
            app_id = generate_unique_id(prefix="app")
        self.apps[app_id] = app = YarnAppInfo(app_id=app_id, **kwargs)
        return app

    def make_corrupt(self, app_id: str):
        self._corrupt.add(app_id)

    def _check_output(self, args: List[str]):
        """Mock for subprocess.check_output(["yarn", ...])"""
        if len(args) == 4 and args[:3] == ["yarn", "application", "-status"]:
            app_id = args[3]
            if app_id in self._corrupt:
                raise subprocess.CalledProcessError(
                    returncode=255,
                    cmd=args,
                    output=f"C0rRup7! {app_id}'".encode("utf-8"),
                )
            elif app_id in self.apps:
                return self.apps[app_id].status_report().encode("utf-8")
            else:
                raise subprocess.CalledProcessError(
                    returncode=255,
                    cmd=args,
                    output=f"Application with id '{app_id}' doesn't exist in RM or Timeline Server.".encode(
                        "utf-8"
                    ),
                )

        raise RuntimeError(f"Unsupported check_output({args!r})")

    @contextlib.contextmanager
    def patch(self):
        with mock.patch(
            "subprocess.check_output", new=self._check_output
        ) as check_output:
            yield check_output


class KUBERNETES_SPARK_APP_STATE:
    # Job states as returned by spark-on-k8s-operator (sparkoperator.k8s.io)
    # Based on https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/22cd4a2c6990df90ab1cb6b0ffbd9d8b76646790/pkg/apis/sparkoperator.k8s.io/v1beta2/types.go#L328-L344
    # TODO: move this to openeogeotrellis.job_tracker?
    NEW = ""
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    # TODO: cover more states?

    # TODO: "PENDING" is used by current `_kube_status_parser` implementation, but is this a valid state?
    PENDING = "PENDING"


@dataclass
class KubernetesAppInfo:
    """Dummy Kubernetes app metadata."""

    app_id: str
    state: str = KUBERNETES_SPARK_APP_STATE.NEW
    start_time: Union[str, None] = None
    finish_time: Union[str, None] = None

    def set_submitted(self):
        if not self.start_time:
            self.start_time = rfc3339.datetime(dt.datetime.utcnow())
        self.state = KUBERNETES_SPARK_APP_STATE.SUBMITTED

    def set_running(self):
        self.state = KUBERNETES_SPARK_APP_STATE.RUNNING

    def set_completed(self):
        if not self.finish_time:
            self.finish_time = rfc3339.datetime(dt.datetime.utcnow())
        self.state = KUBERNETES_SPARK_APP_STATE.COMPLETED

    def set_failed(self):
        if not self.finish_time:
            self.finish_time = rfc3339.datetime(dt.datetime.utcnow())
        self.state = KUBERNETES_SPARK_APP_STATE.FAILED


class KubernetesMock:
    """Kubernetes cluster mock."""

    def __init__(self):
        self.apps: Dict[str, KubernetesAppInfo] = {}

    @contextlib.contextmanager
    def patch(self):
        def get_namespaced_custom_object(name: str, **kwargs) -> dict:
            if name not in self.apps:
                raise kubernetes.client.exceptions.ApiException(
                    status=404, reason="Not Found"
                )
            app = self.apps[name]
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
            "openeogeotrellis.job_tracker.kube_client"
        ) as kube_client, requests_mock.Mocker() as requests_mocker:
            # Mock Kubernetes interaction
            api_instance = kube_client.return_value
            api_instance.get_namespaced_custom_object = get_namespaced_custom_object

            # Mock kubernetes usage API
            requests_mocker.get(
                url_join(JobTracker._KUBECOST_URL, "/model/allocation"),
                json=get_model_allocation,
            )

            yield

    def submit(self, app_id: Optional[str] = None, **kwargs) -> KubernetesAppInfo:
        """Create a new (dummy) Kubernetes app"""
        if app_id is None:
            app_id = generate_unique_id(prefix="app")
        assert app_id not in self.apps
        state = kwargs.pop("state", KUBERNETES_SPARK_APP_STATE.SUBMITTED)
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
def yarn() -> YarnMock:
    yarn = YarnMock()
    with yarn.patch():
        yield yarn


@pytest.fixture
def kubernetes() -> KubernetesMock:
    kube = KubernetesMock()
    with kube.patch():
        yield kube


class InMemoryJobRegistry:
    # TODO: common interface with ElasticJobRegistry
    # TODO move it to openeo_python_driver
    def __init__(self):
        self.db: Dict[str, dict] = {}

    def health_check(self, *args, **kwargs):
        pass

    def create_job(
        self,
        process: dict,
        user_id: str,
        job_id: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        api_version: Optional[str] = None,
        job_options: Optional[dict] = None,
    ):
        assert job_id not in self.db
        created = rfc3339.datetime(dt.datetime.utcnow())
        self.db[job_id] = {
            "job_id": job_id,
            "user_id": user_id,
            "process": process,
            "title": title,
            "description": description,
            "status": JOB_STATUS.CREATED,
            "created": created,
            "updated": created,
            "api_version": api_version,
            "job_options": job_options,
        }

    def set_status(
        self,
        job_id: str,
        status: str,
        *,
        updated: Optional[str] = None,
        started: Optional[str] = None,
        finished: Optional[str] = None,
    ):
        assert job_id in self.db
        data = {
            "status": status,
            "updated": rfc3339.datetime(updated or dt.datetime.utcnow()),
        }
        if started:
            data["started"] = rfc3339.datetime(started)
        if finished:
            data["finished"] = rfc3339.datetime(finished)

        self.db[job_id].update(data)


@pytest.fixture
def elastic_job_registry() -> InMemoryJobRegistry:
    # TODO: still call this fixture "elastic_job_registry"?
    return InMemoryJobRegistry()


DUMMY_PG_1 = {
    "add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}
}
DUMMY_PROCESS_1 = {"process_graph": DUMMY_PG_1}


class TestJobTracker:
    @pytest.fixture
    def job_tracker(
        self, zk_job_registry, elastic_job_registry, batch_job_output_root
    ) -> JobTracker:
        principal = "john@EXAMPLE.TEST"
        keytab = "test/openeo.keytab"
        job_tracker = JobTracker(
            job_registry=lambda: zk_job_registry,
            principal=principal,
            keytab=keytab,
            output_root_dir=batch_job_output_root,
            elastic_job_registry=elastic_job_registry,
        )
        return job_tracker

    def test_yarn_zookeeper_basic(
        self,
        zk_job_registry,
        zk_client,
        yarn,
        job_tracker,
        elastic_job_registry,
        caplog,
        time_machine,
    ):
        caplog.set_level(logging.WARNING)
        time_machine.move_to("2022-12-14T12:00:00Z", tick=False)

        user_id = "john"
        job_id = "job-123"
        # Register new job in zookeeper and yarn
        yarn_app = yarn.submit(app_id="app-123")
        zk_job_registry.register(
            job_id=job_id,
            user_id=user_id,
            api_version="1.2.3",
            specification=DUMMY_PROCESS_1,
        )
        zk_job_registry.set_application_id(
            job_id=job_id, user_id=user_id, application_id=yarn_app.app_id
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
                "application_id": yarn_app.app_id,
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

        # Set SUBMITTED in Yarn
        time_machine.coordinates.shift(70)
        yarn_app.set_submitted()
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "created",  # TODO: once job is submitted to YARN/k8s, status should be at least "queued"
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:01:10Z",  # TODO: get this working?
            }
        )
        assert elastic_job_registry.db[job_id] == DictSubSet(
            {
                "status": "created",  # TODO: once job is submitted to YARN/k8s, status should be at least "queued"
                "created": "2022-12-14T12:00:00Z",
                "updated": "2022-12-14T12:01:10Z",
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

    def test_yarn_zookeeper_lost_yarn_job(
        self,
        zk_job_registry,
        zk_client,
        yarn,
        job_tracker,
        elastic_job_registry,
        caplog,
    ):
        """
        Check that JobTracker.update_statuses() keeps working if there is no YARN app for a given job
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
                yarn.submit(app_id=f"app-{j}", state=YARN_STATE.RUNNING)

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
                "openeogeotrellis.job_tracker",
                logging.WARNING,
                "Failed status update of job_id='job-2': UnknownYarnApplicationException(\"Application with id 'app-2' doesn't exist in RM or Timeline Server.\")",
            )
        ]

    def test_yarn_zookeeper_unexpected_yarn_error(
        self,
        zk_job_registry,
        zk_client,
        yarn,
        job_tracker,
        elastic_job_registry,
        caplog,
    ):
        """
        Check that JobTracker.update_statuses() keeps working if there is unexpected error while checking YARN state.
        """
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
                yarn.submit(app_id=f"app-{j}", state=YARN_STATE.RUNNING)
            else:
                yarn.make_corrupt(f"app-{j}")

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
                "openeogeotrellis.job_tracker",
                logging.WARNING,
                "Failed status update of job_id='job-2': CalledProcessError()",
            )
        ]

    def test_kube_zookeeper_basic(
        self,
        zk_job_registry,
        zk_client,
        job_tracker,
        elastic_job_registry,
        caplog,
        time_machine,
        kubernetes,
    ):
        caplog.set_level(logging.WARNING)
        time_machine.move_to("2022-12-14T12:00:00Z", tick=False)
        # TODO: avoid setting private property
        job_tracker._kube_mode = True

        user_id = "john"
        job_id = "job-123"
        app_id = k8s_job_name(job_id=job_id, user_id=user_id)
        # Register new job in zookeeper and kube
        kube_app = kubernetes.submit(app_id=app_id)
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

        # Set SUBMITTED IN Kubernetes
        time_machine.coordinates.shift(70)
        kube_app.set_submitted()
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet(
            {
                "status": "created",  # TODO: once job is submitted to YARN/k8s, status should be at least "queued"
                "created": "2022-12-14T12:00:00Z",
                # "updated": "2022-12-14T12:02:20Z",  # TODO: get this working?
            }
        )
        assert elastic_job_registry.db[job_id] == DictSubSet(
            {
                "status": "created",  # TODO: once job is submitted to YARN/k8s, status should be at least "queued"
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
