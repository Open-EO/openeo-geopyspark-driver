import copy
import datetime as dt
import logging
import threading
from unittest import mock

import kubernetes.client.exceptions
import pytest
from openeo_driver.jobregistry import JOB_STATUS
from openeo_driver.testing import DictSubSet

from openeogeotrellis.integrations.kubernetes import K8S_SPARK_APP_STATE
from openeogeotrellis.job_registry import InMemoryJobRegistry
from openeogeotrellis.job_tracker_daemon import (
    AppFinishedEvent,
    JobTrackerDaemon,
    SparkApplicationWatcher,
    UserOutcomeWindow,
    WorkQueue,
)
from openeogeotrellis.job_tracker_v2 import JobTracker, K8sStatusGetter

from .test_job_tracker_v2 import DUMMY_PROCESS_1, KubernetesMock


class TestWorkQueue:
    def test_fifo_and_dedup(self):
        q = WorkQueue()
        assert q.put("a", 1)
        assert q.put("b", 2)
        # Re-put: replaces value, keeps position
        assert not q.put("a", 111)
        assert len(q) == 2
        assert "a" in q
        assert q.pop() == 111
        assert q.pop() == 2
        assert q.pop() is None
        assert len(q) == 0

    def test_remove(self):
        q = WorkQueue()
        q.put("a", 1)
        q.put("b", 2)
        assert q.remove("a")
        assert not q.remove("zzz")
        assert q.pop() == 2


class TestUserOutcomeWindow:
    def test_summary(self):
        now = [1000.0]
        window = UserOutcomeWindow(window=dt.timedelta(minutes=30), clock=lambda: now[0])
        window.record("alice", JOB_STATUS.FINISHED)
        window.record("alice", JOB_STATUS.ERROR)
        window.record("bob", JOB_STATUS.ERROR)
        window.record("carol", JOB_STATUS.FINISHED)
        # alice 50% (not more than), bob 100%
        assert window.summary() == (3, 1)
        # Expire everything
        now[0] += 31 * 60
        assert window.summary() == (0, 0)


class _FakeClock:
    def __init__(self, start: float = 1_700_000_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def shift(self, seconds: float):
        self.now += seconds


class TestJobTrackerDaemon:
    @pytest.fixture
    def k8s_mock(self) -> KubernetesMock:
        return KubernetesMock()

    @pytest.fixture
    def prometheus_mock(self):
        prometheus_mock = mock.Mock()
        prometheus_mock.endpoint = "https://prometheus.test/api/v1"
        prometheus_mock.get_cpu_usage.return_value = 100.0
        prometheus_mock.get_billable_cpu_requested.return_value = 100.0
        prometheus_mock.get_network_received_usage.return_value = 1000.0
        prometheus_mock.get_memory_usage.return_value = 2000.0 * 1024 * 1024
        prometheus_mock.get_billable_memory_requested.return_value = 2000.0 * 1024 * 1024
        prometheus_mock.get_max_executor_memory_usage.return_value = 3.5
        return prometheus_mock

    @pytest.fixture
    def elastic_job_registry(self) -> InMemoryJobRegistry:
        return InMemoryJobRegistry()

    @pytest.fixture
    def job_tracker(self, elastic_job_registry, batch_job_output_root, k8s_mock, prometheus_mock) -> JobTracker:
        calculator = mock.Mock()
        calculator.calculate_costs.return_value = 1.5
        return JobTracker(
            app_state_getter=K8sStatusGetter(k8s_mock, prometheus_mock),
            principal="john@EXAMPLE.TEST",
            keytab="test/openeo.keytab",
            job_costs_calculator=calculator,
            output_root_dir=batch_job_output_root,
            elastic_job_registry=elastic_job_registry,
        )

    @pytest.fixture
    def clock(self) -> _FakeClock:
        return _FakeClock()

    @pytest.fixture
    def idle_waits(self) -> list:
        return []

    @pytest.fixture
    def daemon(self, job_tracker, tmp_path, clock, idle_waits) -> JobTrackerDaemon:
        return JobTrackerDaemon(
            job_tracker=job_tracker,
            scan_interval=60,
            heartbeat_file=tmp_path / "heartbeat",
            clock=clock,
            idle_wait=lambda timeout: idle_waits.append(timeout),
        )

    def _create_job(self, elastic_job_registry, k8s_mock, job_id, user_id="john", state=K8S_SPARK_APP_STATE.RUNNING):
        elastic_job_registry.create_job(job_id=job_id, user_id=user_id, process=DUMMY_PROCESS_1)
        app_id = f"app-{job_id}"
        app = k8s_mock.submit(app_id=app_id, state=state)
        app.start_time = "2024-01-01T10:00:00Z"
        elastic_job_registry.set_application_id(job_id=job_id, application_id=app_id)
        return app

    @staticmethod
    def _detach_queue_snapshots(daemon: JobTrackerDaemon):
        """InMemoryJobRegistry returns live dicts; make the queued job infos real (stale-able) snapshots."""
        items = []
        while (item := daemon.normal_queue.pop()) is not None:
            items.append(copy.deepcopy(item))
        for item in items:
            daemon.normal_queue.put(item["job_id"], item)

    def test_scan_and_process_normal_queue(self, daemon, elastic_job_registry, k8s_mock, tmp_path, idle_waits):
        self._create_job(elastic_job_registry, k8s_mock, "job-1")
        self._create_job(elastic_job_registry, k8s_mock, "job-2")

        # Iteration 1: scan + process job-1, iteration 2: process job-2, iteration 3: idle
        daemon.run(max_iterations=3, install_signal_handlers=False)

        assert elastic_job_registry.db["job-1"]["status"] == "running"
        assert elastic_job_registry.db["job-2"]["status"] == "running"
        assert len(daemon.normal_queue) == 0
        assert daemon.stats["scan"] == 1
        assert (tmp_path / "heartbeat").exists()
        assert idle_waits == [60]

    def test_rescan_only_adds_new_items(self, daemon, elastic_job_registry, k8s_mock, clock):
        self._create_job(elastic_job_registry, k8s_mock, "job-1")
        # Scan, but do not process anything yet
        daemon._scan()
        assert len(daemon.normal_queue) == 1
        self._create_job(elastic_job_registry, k8s_mock, "job-2")
        clock.shift(61)
        daemon._scan()
        assert len(daemon.normal_queue) == 2
        assert daemon.normal_queue.pop()["job_id"] == "job-1"
        assert daemon.normal_queue.pop()["job_id"] == "job-2"

    def test_no_rescan_before_interval(self, daemon, elastic_job_registry, k8s_mock, clock):
        self._create_job(elastic_job_registry, k8s_mock, "job-1")
        daemon.run(max_iterations=2, install_signal_handlers=False)
        assert daemon.stats["scan"] == 1
        clock.shift(30)
        daemon.run(max_iterations=1, install_signal_handlers=False)
        assert daemon.stats["scan"] == 1
        clock.shift(31)
        daemon.run(max_iterations=1, install_signal_handlers=False)
        assert daemon.stats["scan"] == 2

    def test_high_priority_first(self, daemon, elastic_job_registry, k8s_mock):
        self._create_job(elastic_job_registry, k8s_mock, "job-1")
        app2 = self._create_job(elastic_job_registry, k8s_mock, "job-2")
        app2.set_completed()
        daemon._scan()
        assert len(daemon.normal_queue) == 2

        daemon._on_app_finished(
            AppFinishedEvent(app_id=app2.app_id, job_id="job-2", user_id="john", app_state="COMPLETED")
        )
        assert len(daemon.high_queue) == 1

        daemon.run(max_iterations=1, install_signal_handlers=False)
        # job-2 was handled first and removed from the normal queue
        assert elastic_job_registry.db["job-2"] == DictSubSet({"status": "finished", "costs": 1.5})
        assert elastic_job_registry.db["job-1"]["status"] == "created"
        assert len(daemon.high_queue) == 0
        assert len(daemon.normal_queue) == 1
        assert daemon.normal_queue.pop()["job_id"] == "job-1"
        assert daemon.stats["reached final status finished"] == 1

    def test_high_priority_already_final_is_skipped(self, daemon, elastic_job_registry, k8s_mock):
        app = self._create_job(elastic_job_registry, k8s_mock, "job-1")
        app.set_completed()
        event = AppFinishedEvent(app_id=app.app_id, job_id="job-1", user_id="john", app_state="COMPLETED")
        daemon._on_app_finished(event)
        daemon.run(max_iterations=1, install_signal_handlers=False)
        assert elastic_job_registry.db["job-1"]["status"] == "finished"

        # Duplicate event (e.g. FAILING then FAILED, or app seen by relist): no second cost calculation
        daemon._on_app_finished(event)
        daemon.run(max_iterations=1, install_signal_handlers=False)
        assert daemon.stats["skip: already final"] == 1
        assert daemon.stats["reached final status finished"] == 1
        assert daemon._job_tracker._job_costs_calculator.calculate_costs.call_count == 1

    def test_high_priority_unknown_job_is_dropped(self, daemon, caplog):
        caplog.set_level(logging.WARNING)
        daemon._on_app_finished(
            AppFinishedEvent(app_id="app-x", job_id="job-unknown", user_id="john", app_state="COMPLETED")
        )
        daemon.run(max_iterations=1, install_signal_handlers=False)
        assert daemon.stats["high: job not found"] == 1
        assert "not found in registry" in caplog.text

    def test_high_priority_app_mismatch_is_dropped(self, daemon, elastic_job_registry, k8s_mock):
        self._create_job(elastic_job_registry, k8s_mock, "job-1")
        daemon._on_app_finished(
            AppFinishedEvent(app_id="app-other", job_id="job-1", user_id="john", app_state="COMPLETED")
        )
        daemon.run(max_iterations=1, install_signal_handlers=False)
        assert daemon.stats["high: app mismatch"] == 1
        assert elastic_job_registry.db["job-1"]["status"] == "created"

    def test_stale_normal_queue_snapshot_does_not_report_costs_twice(self, daemon, elastic_job_registry, k8s_mock):
        """Job finalized (e.g. by another tracker instance) while its snapshot was waiting in the normal queue."""
        app = self._create_job(elastic_job_registry, k8s_mock, "job-1")
        daemon._scan()
        assert len(daemon.normal_queue) == 1
        self._detach_queue_snapshots(daemon)
        # Meanwhile: app completes and someone else finalizes the job in the registry
        app.set_completed()
        elastic_job_registry.set_status(job_id="job-1", status=JOB_STATUS.FINISHED)
        elastic_job_registry.db["job-1"]["costs"] = 42

        daemon.run(max_iterations=1, install_signal_handlers=False)
        assert elastic_job_registry.db["job-1"] == DictSubSet({"status": "finished", "costs": 42})
        assert daemon.stats["skip: already final (refreshed)"] == 1
        assert daemon._job_tracker._job_costs_calculator.calculate_costs.call_count == 0

    def test_stale_snapshot_app_not_found_does_not_set_error(self, daemon, elastic_job_registry, k8s_mock, monkeypatch):
        """Job finalized and app cleaned up by another tracker instance while snapshot was queued."""
        from openeogeotrellis.testing import gps_config_overrides

        app = self._create_job(elastic_job_registry, k8s_mock, "job-1")
        daemon._scan()
        self._detach_queue_snapshots(daemon)
        del k8s_mock.apps[app.app_id]
        elastic_job_registry.set_status(job_id="job-1", status=JOB_STATUS.FINISHED)

        with gps_config_overrides(infra_id="infra-a"):
            daemon.run(max_iterations=1, install_signal_handlers=False)
        assert elastic_job_registry.db["job-1"]["status"] == "finished"
        assert daemon.stats["skip: already final (refreshed)"] == 1

    def test_iteration_error_does_not_stop_loop(self, daemon, caplog):
        caplog.set_level(logging.ERROR)
        with mock.patch.object(daemon, "_update_gauges", side_effect=RuntimeError("gauge boom")):
            daemon.run(max_iterations=2, install_signal_handlers=False)
        assert daemon.stats["iteration failed"] == 2
        assert "Unexpected error in daemon loop iteration" in caplog.text

    def test_scan_failure_does_not_stop_loop(self, daemon, job_tracker, caplog):
        caplog.set_level(logging.ERROR)
        with mock.patch.object(job_tracker, "list_jobs_to_track", side_effect=RuntimeError("EJR down")):
            daemon.run(max_iterations=2, install_signal_handlers=False)
        assert daemon.stats["scan failed"] == 1
        assert "Failed to list active jobs" in caplog.text

    def test_scan_failure_fail_fast(self, job_tracker, clock):
        daemon = JobTrackerDaemon(job_tracker=job_tracker, clock=clock, fail_fast=True, idle_wait=lambda t: None)
        with mock.patch.object(job_tracker, "list_jobs_to_track", side_effect=RuntimeError("EJR down")):
            with pytest.raises(RuntimeError, match="EJR down"):
                daemon.run(max_iterations=2, install_signal_handlers=False)

    def test_sync_failure_does_not_stop_loop(self, daemon, elastic_job_registry, k8s_mock):
        self._create_job(elastic_job_registry, k8s_mock, "job-1")
        self._create_job(elastic_job_registry, k8s_mock, "job-2")
        k8s_mock.corrupt_app_ids.add("app-job-1")
        daemon.run(max_iterations=3, install_signal_handlers=False)
        assert daemon.stats["failed sync"] == 1
        assert elastic_job_registry.db["job-1"]["status"] == "created"
        assert elastic_job_registry.db["job-2"]["status"] == "running"

    def test_stop(self, daemon, idle_waits):
        def stopper():
            daemon.stop()

        threading.Timer(0.2, stopper).start()
        # Without max_iterations: loop would run forever (idle_wait is non-blocking here), so make it blocking
        daemon._idle_wait = daemon._default_idle_wait
        daemon.run(install_signal_handlers=False)
        assert daemon._stop.is_set()

    def test_user_outcome_gauges(self, daemon, elastic_job_registry, k8s_mock):
        self._create_job(elastic_job_registry, k8s_mock, "job-1", user_id="alice").set_completed()
        self._create_job(elastic_job_registry, k8s_mock, "job-2", user_id="bob").set_failed()
        daemon.run(max_iterations=3, install_signal_handlers=False)
        assert daemon._user_window.summary() == (2, 1)
        assert daemon._metrics.users_with_final_jobs._value.get() == 2
        assert daemon._metrics.users_with_mostly_errors._value.get() == 1


class _WatchExhausted(Exception):
    """Raised by `_FakeWatch` when its scripted batches are used up."""


class _FakeWatch:
    """Fake of `kubernetes.watch.Watch` yielding scripted event batches (one batch per `stream` call)."""

    def __init__(self, batches):
        self.batches = list(batches)
        self.calls = []

    def stream(self, func, **kwargs):
        self.calls.append(kwargs)
        if not self.batches:
            raise _WatchExhausted
        batch = self.batches.pop(0)
        if isinstance(batch, Exception):
            raise batch
        return iter(batch)


def _spark_app(name, state, job_id="job-1", user="john", rv="1", termination_time="2024-01-01T10:30:00Z"):
    return {
        "metadata": {"name": name, "resourceVersion": rv, "labels": {"correlation_id": job_id, "user": user}},
        "status": {"applicationState": {"state": state}, "terminationTime": termination_time},
    }


class TestSparkApplicationWatcher:
    @pytest.fixture
    def k8s_api(self):
        api = mock.Mock()
        api.list_namespaced_custom_object.return_value = {
            "metadata": {"resourceVersion": "100"},
            "items": [
                _spark_app("app-done", "COMPLETED", job_id="job-done"),
                _spark_app("app-failed", "FAILED", job_id="job-failed"),
                _spark_app("app-running", "RUNNING", job_id="job-running"),
            ],
        }
        return api

    def _run_watcher(self, k8s_api, fake_watch, stop_after_events=None):
        watcher = SparkApplicationWatcher(k8s_api, namespace="spark-jobs")
        events = []

        def on_event(e):
            events.append(e)
            if stop_after_events and len(events) >= stop_after_events:
                watcher.stop()

        watcher._on_event = on_event
        with mock.patch("kubernetes.watch.Watch", return_value=fake_watch):
            try:
                watcher._list_then_watch()
            except _WatchExhausted:
                pass
        return watcher, events

    def test_initial_list_and_final_events(self, k8s_api):
        fake_watch = _FakeWatch(
            [
                [
                    {"type": "MODIFIED", "object": _spark_app("app-1", "RUNNING", rv="101")},
                    {"type": "MODIFIED", "object": _spark_app("app-1", "FAILED", rv="102")},
                    {"type": "ADDED", "object": _spark_app("app-2", "COMPLETED", job_id="job-2", rv="103")},
                    {"type": "DELETED", "object": _spark_app("app-2", "COMPLETED", job_id="job-2", rv="104")},
                ]
            ]
        )
        watcher, events = self._run_watcher(k8s_api, fake_watch)
        # Note: from the initial list, only apps in cleanup statuses (COMPLETED) are emitted
        assert [(e.app_id, e.job_id, e.app_state) for e in events] == [
            ("app-done", "job-done", "COMPLETED"),
            ("app-1", "job-1", "FAILED"),
            ("app-2", "job-2", "COMPLETED"),
        ]
        assert events[0].finish_time == dt.datetime(2024, 1, 1, 10, 30, tzinfo=dt.timezone.utc)
        assert events[0].user_id == "john"
        assert fake_watch.calls[0]["resource_version"] == "100"
        assert fake_watch.calls[0]["plural"] == "sparkapplications"

    def test_resource_version_progresses_between_streams(self, k8s_api):
        fake_watch = _FakeWatch(
            [
                [{"type": "MODIFIED", "object": _spark_app("app-1", "RUNNING", rv="150")}],
                [],
            ]
        )
        self._run_watcher(k8s_api, fake_watch)
        assert [c["resource_version"] for c in fake_watch.calls] == ["100", "150", "150"]

    def test_410_triggers_relist(self, k8s_api):
        fake_watch = _FakeWatch([kubernetes.client.exceptions.ApiException(status=410, reason="Gone")])
        watcher, events = self._run_watcher(k8s_api, fake_watch)
        assert watcher.stats["relist"] == 1
        assert not watcher.connected

    def test_error_event_raises(self, k8s_api):
        fake_watch = _FakeWatch([[{"type": "ERROR", "object": {"kind": "Status", "code": 410}}]])
        watcher = SparkApplicationWatcher(k8s_api, namespace="spark-jobs")
        watcher._on_event = lambda e: None
        with mock.patch("kubernetes.watch.Watch", return_value=fake_watch):
            with pytest.raises(RuntimeError, match="Watch error event"):
                watcher._list_then_watch()

    def test_missing_job_id_label_is_skipped(self, k8s_api):
        app = _spark_app("app-nolabel", "COMPLETED")
        del app["metadata"]["labels"]["correlation_id"]
        k8s_api.list_namespaced_custom_object.return_value = {"metadata": {"resourceVersion": "1"}, "items": []}
        watcher, events = self._run_watcher(k8s_api, _FakeWatch([[{"type": "MODIFIED", "object": app}]]))
        assert events == []
        assert watcher.stats["skip: no job_id label"] == 1

    def test_thread_reconnects_on_failure(self, k8s_api):
        listings = iter([RuntimeError("boom")])
        ok_listing = {"metadata": {"resourceVersion": "1"}, "items": []}

        def list_side_effect(**kwargs):
            result = next(listings, ok_listing)
            if isinstance(result, Exception):
                raise result
            return result

        k8s_api.list_namespaced_custom_object.side_effect = list_side_effect
        watcher = SparkApplicationWatcher(k8s_api, namespace="spark-jobs", max_backoff=0.01)
        events = []
        got_event = threading.Event()

        def on_event(e):
            events.append(e)
            got_event.set()

        fake_watch = _FakeWatch([[{"type": "MODIFIED", "object": _spark_app("app-1", "COMPLETED")}]])
        with mock.patch("kubernetes.watch.Watch", return_value=fake_watch):
            watcher.start(on_event=on_event)
            assert got_event.wait(timeout=5)
            watcher.stop()
            watcher._thread.join(timeout=5)
        assert watcher.restarts >= 1
        assert events[0].app_id == "app-1"
