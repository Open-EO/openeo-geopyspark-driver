import contextlib
import logging
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from unittest import mock

import pytest
from openeo_driver.testing import DictSubSet
from openeo_driver.utils import generate_unique_id

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
        return self.set_state(YARN_STATE.RUNNING, YARN_FINAL_STATUS.UNDEFINED)

    def set_finished(self, final_state: str = YARN_FINAL_STATUS.SUCCEEDED):
        assert final_state in {YARN_FINAL_STATUS.SUCCEEDED, YARN_FINAL_STATUS.FAILED}
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
def elastic_job_registry():
    # TODO use a real in-memory job registry instead of a mock?
    return mock.Mock()


DUMMY_PG_1 = {
    "add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}
}
DUMMY_SPEC_1 = {"process_graph": DUMMY_PG_1}


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
    ):
        user_id = "john"
        job_id = "job-123"
        # Register new job in zookeeper and yarn
        yarn_app = yarn.submit(app_id="app-123")
        zk_job_registry.register(
            job_id=job_id,
            user_id=user_id,
            api_version="1.2.3",
            specification=DUMMY_SPEC_1,
        )
        zk_job_registry.set_application_id(
            job_id=job_id, user_id=user_id, application_id=yarn_app.app_id
        )

        def zk_job_info() -> dict:
            return zk_job_registry.get_job(job_id=job_id, user_id=user_id)

        # Check initial status in registry
        assert zk_job_info() == DictSubSet(
            {
                "job_id": job_id,
                "user_id": user_id,
                "application_id": yarn_app.app_id,
                "status": "created",
            }
        )

        # Set SUBMITTED in Yarn
        yarn_app.set_submitted()
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet({"status": "created"})
        elastic_job_registry.set_status.assert_called_with(job_id, "created")

        # Set ACCEPTED in Yarn
        yarn_app.set_accepted()
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet({"status": "queued"})
        elastic_job_registry.set_status.assert_called_with(job_id, "queued")

        # Set RUNNING in Yarn
        yarn_app.set_running()
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet({"status": "running"})
        elastic_job_registry.set_status.assert_called_with(job_id, "running")

        # Set FINISHED in Yarn
        yarn_app.set_finished()
        json_write(
            path=job_tracker._batch_jobs.get_results_metadata_path(job_id=job_id),
            data={"foo": "bar"},
        )
        job_tracker.update_statuses()
        assert zk_job_info() == DictSubSet({"status": "finished", "foo": "bar"})
        elastic_job_registry.set_status.assert_called_with(job_id, "finished")

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
        for j in [1, 2, 3]:
            zk_job_registry.register(
                job_id=f"job-{j}",
                user_id=f"user{j}",
                api_version="1.2.3",
                specification=DUMMY_SPEC_1,
            )
            zk_job_registry.set_application_id(
                job_id=f"job-{j}", user_id=f"user{j}", application_id=f"app-{j}"
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

        # TODO get status from an in-memory registry, instead of mock.assert_any_call
        elastic_job_registry.set_status.assert_any_call("job-1", "running")
        elastic_job_registry.set_status.assert_any_call("job-2", "error")
        elastic_job_registry.set_status.assert_any_call("job-3", "running")

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
                specification=DUMMY_SPEC_1,
            )
            zk_job_registry.set_application_id(
                job_id=f"job-{j}", user_id=f"user{j}", application_id=f"app-{j}"
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

        # TODO get status from an in-memory registry, instead of mock.assert_any_call
        elastic_job_registry.set_status.assert_any_call("job-1", "running")
        elastic_job_registry.set_status.assert_any_call("job-2", "error")
        elastic_job_registry.set_status.assert_any_call("job-3", "running")

        assert caplog.record_tuples == [
            (
                "openeogeotrellis.job_tracker",
                logging.WARNING,
                "Failed status update of job_id='job-2': CalledProcessError()",
            )
        ]
