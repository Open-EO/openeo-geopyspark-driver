import logging
import threading
import time

import pytest
from openeo_driver.jobregistry import JOB_STATUS

from openeogeotrellis import fast_job
from openeogeotrellis.fast_job import (
    FastJobLogHandler,
    fast_job_application_id,
    fast_job_thread_name,
    is_fast_execution_requested,
    is_fast_job_application_id,
    is_thread_alive,
    read_fast_job_logs,
    run_fast_job,
)


@pytest.mark.parametrize(
    ["job_options", "expected"],
    [
        (None, False),
        ({}, False),
        ({"allow_fast_execution": False}, False),
        ({"allow_fast_execution": "false"}, False),
        ({"allow_fast_execution": True}, True),
        ({"allow_fast_execution": "true"}, True),
    ],
)
def test_is_fast_execution_requested(job_options, expected):
    assert is_fast_execution_requested(job_options) == expected


def test_fast_job_application_id(monkeypatch):
    monkeypatch.setenv("POD_NAME", "openeo-webapp-123")
    application_id = fast_job_application_id("j-123")
    assert application_id == "openeo-webapp-123/fast-job-j-123"
    assert is_fast_job_application_id(application_id)
    assert not is_fast_job_application_id("application_1234_5678")
    assert not is_fast_job_application_id(None)


def test_is_thread_alive():
    done = threading.Event()
    thread = threading.Thread(name=fast_job_thread_name("j-123"), target=done.wait)
    thread.start()
    try:
        assert is_thread_alive("fast-job-j-123")
        assert not is_thread_alive("fast-job-j-456")
    finally:
        done.set()
        thread.join()
    assert not is_thread_alive("fast-job-j-123")


def test_log_handler_and_reader(tmp_path):
    log_file = tmp_path / "log.jsonl"
    logger = logging.getLogger("openeogeotrellis.test_fast_job")
    logger.setLevel(logging.DEBUG)
    handler = FastJobLogHandler(job_id="j-123", log_file=log_file)
    logger.addHandler(handler)

    def work():
        logger.debug("Hello debug")
        logger.info("Hello info", extra={"audience": "user"})
        logger.error("Oops", extra={"audience": "developer"})

    try:
        thread = threading.Thread(name=fast_job_thread_name("j-123"), target=work)
        thread.start()
        thread.join()
        # Not from the fast job thread: only included when tagged with the job_id
        logger.info("Other thread, untagged")
        logger.info("Other thread, tagged", extra={"job_id": "j-123"})
        logger.info("Other job", extra={"job_id": "j-456"})
    finally:
        logger.removeHandler(handler)
        handler.close()

    logs = list(read_fast_job_logs(log_file))
    assert [(l["id"], l["level"], l["message"]) for l in logs] == [
        ("0", "debug", "Hello debug"),
        ("1", "info", "Hello info"),
        ("2", "error", "Oops"),
        ("3", "info", "Other thread, tagged"),
    ]
    assert all(l["time"].endswith("Z") for l in logs)

    assert [l["message"] for l in read_fast_job_logs(log_file, level="info")] == [
        "Hello info",
        "Oops",
        "Other thread, tagged",
    ]
    assert [l["message"] for l in read_fast_job_logs(log_file, offset="1")] == ["Oops", "Other thread, tagged"]
    assert [l["message"] for l in read_fast_job_logs(log_file, audience="user")] == [
        "Hello debug",
        "Hello info",
        "Other thread, tagged",
    ]
    assert list(read_fast_job_logs(tmp_path / "nope.jsonl")) == []


class TestRunFastJob:
    @pytest.fixture
    def statuses(self):
        return []

    def _run(self, tmp_path, statuses, run, current_status=JOB_STATUS.RUNNING):
        thread = threading.Thread(
            name=fast_job_thread_name("j-123"),
            target=run_fast_job,
            kwargs=dict(
                job_id="j-123",
                user_id="john",
                job_dir=tmp_path,
                set_status=lambda status, **kwargs: statuses.append(status),
                get_status=lambda: current_status,
                run=run,
            ),
        )
        thread.start()
        thread.join()

    def test_success(self, tmp_path, statuses):
        self._run(tmp_path, statuses, run=lambda: logging.getLogger("openeogeotrellis.foo").info("Doing work"))
        assert statuses == [JOB_STATUS.RUNNING, JOB_STATUS.FINISHED]
        messages = [l["message"] for l in read_fast_job_logs(tmp_path / fast_job.FAST_JOB_LOG_FILENAME)]
        assert "Doing work" in messages

    def test_failure(self, tmp_path, statuses):
        def run():
            raise RuntimeError("Nope")

        self._run(tmp_path, statuses, run=run)
        assert statuses == [JOB_STATUS.RUNNING, JOB_STATUS.ERROR]
        logs = list(read_fast_job_logs(tmp_path / fast_job.FAST_JOB_LOG_FILENAME, level="error"))
        assert any("RuntimeError('Nope')" in l["message"] for l in logs)

    def test_canceled(self, tmp_path, statuses):
        self._run(tmp_path, statuses, run=lambda: None, current_status=JOB_STATUS.CANCELED)
        assert statuses == [JOB_STATUS.RUNNING]


class TestFastJobBackend:
    PROCESS_GRAPH = {
        "add": {"process_id": "add", "arguments": {"x": 3, "y": 5}},
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "add"}, "format": "JSON"},
            "result": True,
        },
    }

    def _create_and_start(self, api100, job_options, process_graph=None):
        api100.set_auth_bearer_token()
        res = api100.post(
            "/jobs",
            json={"process": {"process_graph": process_graph or self.PROCESS_GRAPH}, "job_options": job_options},
        ).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]
        api100.post(f"/jobs/{job_id}/results").assert_status_code(202)
        return job_id

    def _wait_for_final_status(self, job_registry, job_id, timeout=120) -> str:
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = job_registry.db[job_id]["status"]
            if status in {JOB_STATUS.FINISHED, JOB_STATUS.ERROR} and not is_thread_alive(fast_job_thread_name(job_id)):
                return status
            time.sleep(0.1)
        raise TimeoutError(f"Fast job {job_id} did not finish in time")

    def test_fast_job(self, api100, job_registry, batch_job_output_root, monkeypatch):
        statuses = []
        orig_set_status = job_registry.set_status

        def set_status(job_id, *, status, **kwargs):
            statuses.append(status)
            return orig_set_status(job_id, status=status, **kwargs)

        monkeypatch.setattr(job_registry, "set_status", set_status)

        job_id = self._create_and_start(api100, job_options={"allow_fast_execution": True})
        assert self._wait_for_final_status(job_registry, job_id) == JOB_STATUS.FINISHED
        assert statuses == [JOB_STATUS.QUEUED, JOB_STATUS.RUNNING, JOB_STATUS.FINISHED]
        job = job_registry.db[job_id]
        assert job["application_id"].endswith(f"/fast-job-{job_id}")
        assert job["results_metadata_uri"] == f"file://{batch_job_output_root / job_id / 'job_metadata.json'}"
        # Process graph in registry should not be mutated by the evaluation
        assert job["process"]["process_graph"] == self.PROCESS_GRAPH

        assert api100.get(f"/jobs/{job_id}").assert_status_code(200).json["status"] == "finished"

        results = api100.get(f"/jobs/{job_id}/results").assert_status_code(200).json
        assert len(results["assets"]) == 1
        (asset,) = results["assets"].values()
        assert asset["type"] == "application/json"
        href = api100.extract_path(asset["href"])
        assert api100.get(href).assert_status_code(200).json == 8

        logs = api100.get(f"/jobs/{job_id}/logs").assert_status_code(200).json["logs"]
        messages = [l["message"] for l in logs]
        assert f"Fast job {job_id} finished successfully" in messages

    def test_fast_job_error(self, api100, job_registry):
        process_graph = {"lc": {"process_id": "load_collection", "arguments": {"id": "NOPE"}, "result": True}}
        job_id = self._create_and_start(api100, job_options={"allow_fast_execution": True}, process_graph=process_graph)
        assert self._wait_for_final_status(job_registry, job_id) == JOB_STATUS.ERROR
        logs = api100.get(f"/jobs/{job_id}/logs", params={"level": "error"}).assert_status_code(200).json["logs"]
        assert any("CollectionNotFound" in l["message"] for l in logs)

    def test_no_fast_job_without_option(self, api100, job_registry, monkeypatch):
        """Without `allow_fast_execution`, the normal batch job path should be taken."""
        normal_path_called = []

        class DummyRunner:
            def __init__(self, *args, **kwargs):
                pass

            def set_default_sentinel_hub_credentials(self, *args):
                pass

            def run_job(self, *args, **kwargs):
                normal_path_called.append(True)
                return "application_123"

        monkeypatch.setattr("openeogeotrellis.backend.YARNBatchJobRunner", DummyRunner)
        start_fast_job_thread = []
        monkeypatch.setattr(fast_job, "start_fast_job_thread", lambda **kwargs: start_fast_job_thread.append(True))

        job_id = self._create_and_start(api100, job_options={})

        assert normal_path_called == [True]
        assert start_fast_job_thread == []
        assert job_registry.db[job_id]["application_id"] == "application_123"
        assert job_registry.db[job_id]["status"] == JOB_STATUS.QUEUED
