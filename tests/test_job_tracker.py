from unittest import mock

import contextlib
import pytest
import subprocess
from dataclasses import dataclass
from typing import Tuple, List, Optional, Dict

from openeo_driver.utils import generate_unique_id
from openeogeotrellis.job_registry import ZkJobRegistry
from openeogeotrellis.job_tracker import JobTracker
from openeogeotrellis.testing import KazooClientMock

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

    def submit(self, app_id: Optional[str] = None, **kwargs) -> YarnAppInfo:
        """Create a new (dummy) YARN app"""
        if app_id is None:
            app_id = generate_unique_id(prefix="app")
        self.apps[app_id] = app = YarnAppInfo(app_id=app_id, **kwargs)
        return app

    def _check_output(self, args: List[str]):
        """Mock for subprocess.check_output(["yarn", ...])"""
        if len(args) == 4 and args[:3] == ["yarn", "application", "-status"]:
            app_id = args[3]
            if app_id in self.apps:
                return self.apps[app_id].status_report().encode("utf-8")
            else:
                raise subprocess.CalledProcessError(
                    returncode=255,
                    cmd=args,
                    output=f"Application with id '{app_id}' doesn't exist in RM or Timeline Server.",
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


class TestJobTracker:
    @pytest.fixture
    def job_tracker(self, zk_job_registry) -> JobTracker:
        principal = "john@EXAMPLE.TEST"
        keytab = "test/openeo.keytab"
        job_tracker = JobTracker(
            job_registry=lambda: zk_job_registry,
            principal=principal,
            keytab=keytab,
        )
        return job_tracker

    def test_basic_yarn_zookeeper(self, zk_job_registry, yarn, job_tracker):
        user_id = "john"
        job_id = "job-123"
        # Register new job in zookeeper and yarn
        yarn_app = yarn.submit(app_id="app-123")
        zk_job_registry.register(
            job_id=job_id, user_id=user_id, api_version="1.2.3", specification={}
        )
        zk_job_registry.set_application_id(
            job_id=job_id, user_id=user_id, application_id=yarn_app.app_id
        )

        def zk_status() -> str:
            return zk_job_registry.get_status(job_id=job_id, user_id=user_id)

        # Check initial status in registry
        assert zk_status() == "created"

        # Set SUBMITTED in Yarn
        yarn_app.set_submitted()
        job_tracker.update_statuses()
        assert zk_status() == "created"

        # Set ACCEPTED in Yarn
        yarn_app.set_accepted()
        job_tracker.update_statuses()
        assert zk_status() == "queued"

        # Set RUNNING in Yarn
        yarn_app.set_running()
        job_tracker.update_statuses()
        assert zk_status() == "running"

        # Set FINISHED in Yarn
        yarn_app.set_finished()
        job_tracker.update_statuses()
        assert zk_status() == "finished"

        # TODO: assert for warnings
