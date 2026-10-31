"""
Daemon mode for the job tracker (see `openeogeotrellis.job_tracker_v2`).

Instead of a one-shot run (e.g. as a K8s CronJob), this keeps a long-lived process (e.g. a single replica K8s Deployment)
that continuously syncs job statuses from the app cluster (K8s) to the job registry (EJR):

- A "normal priority" queue is periodically (re)filled with all active jobs from the job registry
  (same listing as the one-shot mode).
  This is the source of truth for correctness: anything missed by the fast path is picked up here eventually.
- A "high priority" queue is filled by a watcher of SparkApplication changes:
  when an app reaches a final state, the corresponding job is synced as soon as possible.
  This is purely a latency optimization.

A single worker loop drains the high priority queue first, then the normal queue, and sleeps when both are empty.
"""

import collections
import datetime as dt
import logging
import signal
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, NamedTuple, Optional, Union

from openeo.util import TimingLogger
from openeo_driver.jobregistry import JOB_STATUS, JobNotFoundException
from openeo_driver.util.date_math import now_utc

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.integrations.kubernetes import K8S_SPARK_APP_STATE, k8s_state_to_openeo_job_status
from openeogeotrellis.job_tracker_v2 import FINAL_JOB_STATUSES, JobTracker

_log = logging.getLogger("openeogeotrellis.job_tracker_daemon")

#: SparkApplication states that correspond to a final openEO job status
K8S_FINAL_APP_STATES = frozenset(
    {
        K8S_SPARK_APP_STATE.COMPLETED,
        K8S_SPARK_APP_STATE.FAILED,
        K8S_SPARK_APP_STATE.SUBMISSION_FAILED,
        K8S_SPARK_APP_STATE.FAILING,
    }
)

SPARK_APP_GROUP = "sparkoperator.k8s.io"
SPARK_APP_VERSION = "v1beta2"
SPARK_APP_PLURAL = "sparkapplications"


class WorkQueue:
    """Thread-safe FIFO queue of work items, deduplicated by key."""

    def __init__(self):
        self._items: "collections.OrderedDict[str, object]" = collections.OrderedDict()
        self._lock = threading.Lock()

    def put(self, key: str, item) -> bool:
        """
        Add item. If its key is already present, the item is replaced (keeping its position in the queue)
        so that the freshest data is used. Returns True if the key was newly added.
        """
        with self._lock:
            added = key not in self._items
            self._items[key] = item
            return added

    def pop(self):
        """Pop the oldest item (or None if empty)."""
        with self._lock:
            if not self._items:
                return None
            _, item = self._items.popitem(last=False)
            return item

    def remove(self, key: str) -> bool:
        with self._lock:
            return self._items.pop(key, None) is not None

    def __contains__(self, key: str) -> bool:
        with self._lock:
            return key in self._items

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)


class AppFinishedEvent(NamedTuple):
    """Work item for the high priority queue: a SparkApplication reached a final state."""

    app_id: str
    job_id: str
    user_id: Optional[str]
    app_state: str
    # UTC datetime the app terminated, if known.
    finish_time: Optional[dt.datetime] = None


class SparkApplicationWatcher:
    """
    Watch SparkApplication resources (sparkoperator.k8s.io) in a namespace and
    emit `AppFinishedEvent`s whenever an app reaches a final state.

    Runs a "list then watch" loop in a daemon thread, reconnecting with backoff on errors.
    """

    def __init__(
        self,
        kubernetes_api,
        namespace: str,
        *,
        watch_timeout: float = 300,
        request_timeout: float = 330,
        max_backoff: float = 60,
    ):
        self._kubernetes_api = kubernetes_api
        self._namespace = namespace
        self._watch_timeout = watch_timeout
        self._request_timeout = request_timeout
        self._max_backoff = max_backoff
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self.connected = False
        self.restarts = 0
        self.stats = collections.Counter()
        self._on_event: Optional[Callable[[AppFinishedEvent], None]] = None

    def start(self, on_event: Callable[[AppFinishedEvent], None]) -> None:
        self._on_event = on_event
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="sparkapp-watcher", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        initial_backoff = min(1.0, self._max_backoff)
        backoff = initial_backoff
        while not self._stop.is_set():
            try:
                self._list_then_watch()
                backoff = initial_backoff
            except Exception as e:
                self.connected = False
                self.restarts += 1
                _log.warning(
                    f"SparkApplication watch failed ({type(e).__name__}: {e}), reconnecting in {backoff}s",
                    exc_info=True,
                )
                self._stop.wait(backoff)
                backoff = min(backoff * 2, self._max_backoff)

    def _list_then_watch(self) -> None:
        # Local import to avoid kubernetes dependency when not necessary
        import kubernetes.client.exceptions
        import kubernetes.watch

        initial = self._kubernetes_api.list_namespaced_custom_object(
            group=SPARK_APP_GROUP,
            version=SPARK_APP_VERSION,
            namespace=self._namespace,
            plural=SPARK_APP_PLURAL,
            _request_timeout=self._request_timeout,
        )
        # From the initial list, only emit apps that get cleaned up after a successful sync (e.g. COMPLETED):
        # these are either unprocessed finished jobs (sync them fast) or rare cleanup failures (one cheap lookup).
        # Other final apps (e.g. FAILED, kept around until their TTL) are left to the periodic scan,
        # otherwise every (re)list would cause a burst of registry lookups for them.
        cleanup_statuses = set(get_backend_config().job_tracker_cleanup_openeo_statuses)
        for obj in initial.get("items", []):
            state = obj.get("status", {}).get("applicationState", {}).get("state", K8S_SPARK_APP_STATE.NEW)
            if state in K8S_FINAL_APP_STATES and k8s_state_to_openeo_job_status(state) in cleanup_statuses:
                self._handle_object(obj)
        resource_version = initial.get("metadata", {}).get("resourceVersion")
        _log.info(f"Listed {len(initial.get('items', []))} SparkApplications, starting watch at {resource_version=}")

        watch = kubernetes.watch.Watch()
        while not self._stop.is_set():
            try:
                self.connected = True
                for event in watch.stream(
                    self._kubernetes_api.list_namespaced_custom_object,
                    group=SPARK_APP_GROUP,
                    version=SPARK_APP_VERSION,
                    namespace=self._namespace,
                    plural=SPARK_APP_PLURAL,
                    resource_version=resource_version,
                    timeout_seconds=self._watch_timeout,
                    _request_timeout=self._request_timeout,
                ):
                    if self._stop.is_set():
                        return
                    self.stats[f"event {event.get('type')}"] += 1
                    obj = event.get("object") or {}
                    if event.get("type") == "ERROR":
                        # e.g. {"type": "ERROR", "object": {"kind": "Status", "code": 410, ...}}
                        raise RuntimeError(f"Watch error event: {obj}")
                    resource_version = obj.get("metadata", {}).get("resourceVersion", resource_version)
                    if event.get("type") in {"ADDED", "MODIFIED"}:
                        self._handle_object(obj)
            except kubernetes.client.exceptions.ApiException as e:
                if e.status == 410:
                    # Resource version too old: relist from scratch
                    _log.info("SparkApplication watch resource version expired (410), relisting")
                    self.stats["relist"] += 1
                    return
                raise
            finally:
                self.connected = False

    def _handle_object(self, obj: dict) -> None:
        try:
            state = obj.get("status", {}).get("applicationState", {}).get("state", K8S_SPARK_APP_STATE.NEW)
            if state not in K8S_FINAL_APP_STATES:
                return
            metadata = obj.get("metadata", {})
            labels = metadata.get("labels", {})
            app_id = metadata.get("name")
            job_id = labels.get("correlation_id")
            if not app_id or not job_id:
                self.stats["skip: no job_id label"] += 1
                return
            finish_time = None
            termination_time = obj.get("status", {}).get("terminationTime")
            if termination_time:
                try:
                    finish_time = dt.datetime.fromisoformat(termination_time.replace("Z", "+00:00"))
                except ValueError:
                    pass
            self.stats[f"final app {state}"] += 1
            self._on_event(
                AppFinishedEvent(
                    app_id=app_id, job_id=job_id, user_id=labels.get("user"), app_state=state, finish_time=finish_time
                )
            )
        except Exception as e:
            _log.warning(f"Failed to handle SparkApplication event: {type(e).__name__}: {e}", exc_info=True)
            self.stats["handle error"] += 1


class _Metrics:
    """Prometheus metrics of the daemon (no-op if `prometheus_client` is not available)."""

    PREFIX = "openeo_job_tracker_"

    def __init__(self, port: int = 0):
        self.enabled = False
        try:
            import prometheus_client as pc
        except ImportError:
            if port:
                _log.warning("prometheus_client not available: metrics disabled")
            return
        # Dedicated registry: allows multiple instances (e.g. in tests) and avoids unrelated default collectors
        self.registry = pc.CollectorRegistry()
        r = self.registry
        p = self.PREFIX
        self.heartbeat = pc.Gauge(
            p + "loop_heartbeat_timestamp_seconds", "Unix time of last daemon loop iteration", registry=r
        )
        self.scan_last_success = pc.Gauge(
            p + "scan_last_success_timestamp_seconds", "Unix time of last successful active jobs listing", registry=r
        )
        self.scan_duration = pc.Summary(p + "scan_duration_seconds", "Duration of active jobs listing", registry=r)
        self.scan_jobs_collected = pc.Gauge(
            p + "scan_jobs_collected", "Number of active jobs in last listing", registry=r
        )
        self.scan_failures = pc.Counter(p + "scan_failures_total", "Failed active jobs listings", registry=r)
        self.queue_size = pc.Gauge(p + "queue_size", "Number of items in work queue", ["queue"], registry=r)
        self.watch_connected = pc.Gauge(
            p + "watch_connected", "Whether the SparkApplication watch is connected", registry=r
        )
        self.watch_restarts = pc.Gauge(p + "watch_restarts", "SparkApplication watch reconnects", registry=r)
        self.watch_events = pc.Counter(
            p + "watch_events_total", "Final state SparkApplication events", ["state"], registry=r
        )
        self.job_sync = pc.Counter(p + "job_sync_total", "Job status sync attempts", ["result", "priority"], registry=r)
        self.job_final_status = pc.Counter(
            p + "job_final_status_total", "Jobs that reached a final status", ["status"], registry=r
        )
        self.detection_latency = pc.Histogram(
            p + "status_detection_latency_seconds",
            "Delay between app termination and job registry update",
            buckets=[5, 15, 30, 60, 120, 300, 600, 1800, 3600, float("inf")],
            registry=r,
        )
        self.users_with_final_jobs = pc.Gauge(
            p + "users_with_final_jobs",
            "Distinct users with jobs reaching a final status in the recent window",
            registry=r,
        )
        self.users_with_mostly_errors = pc.Gauge(
            p + "users_with_more_than_50p_error_jobs",
            "Distinct users with >50% error jobs among their final jobs in the recent window",
            registry=r,
        )
        self.enabled = True
        if port:
            pc.start_http_server(port, registry=r)
            _log.info(f"Prometheus metrics exposed on port {port}")

    def __getattr__(self, name):
        # Metrics disabled: swallow all calls
        if name.startswith("_"):
            raise AttributeError(name)
        return _NoopMetric()


class _NoopMetric:
    def __getattr__(self, name):
        return self

    def __call__(self, *args, **kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class UserOutcomeWindow:
    """Sliding window of job outcomes per user, to distinguish user-specific from systemic failures."""

    def __init__(self, window: dt.timedelta = dt.timedelta(minutes=30), clock: Callable[[], float] = time.time):
        self._window = window.total_seconds()
        self._clock = clock
        self._events: "collections.deque[tuple[float, str, bool]]" = collections.deque()

    def record(self, user_id: str, status: str) -> None:
        self._events.append((self._clock(), user_id, status == JOB_STATUS.ERROR))
        self._expire()

    def _expire(self):
        cutoff = self._clock() - self._window
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    def summary(self) -> "tuple[int, int]":
        """Returns (users with final jobs, users with more than 50% error jobs)."""
        self._expire()
        per_user: Dict[str, List[int]] = collections.defaultdict(lambda: [0, 0])
        for _, user_id, is_error in self._events:
            per_user[user_id][0] += 1
            per_user[user_id][1] += int(is_error)
        mostly_errors = sum(1 for total, errors in per_user.values() if errors * 2 > total)
        return len(per_user), mostly_errors


class JobTrackerDaemon:
    def __init__(
        self,
        *,
        job_tracker: JobTracker,
        watcher: Optional[SparkApplicationWatcher] = None,
        scan_interval: float = 60,
        heartbeat_file: Optional[Union[str, Path]] = None,
        metrics_port: int = 0,
        fail_fast: bool = False,
        clock: Callable[[], float] = time.time,
        idle_wait: Optional[Callable[[float], None]] = None,
    ):
        """
        :param scan_interval: interval (seconds) to list active jobs from the job registry
        :param idle_wait: callable to wait for work when idle (for testing)
        """
        self._job_tracker = job_tracker
        self._watcher = watcher
        self._scan_interval = scan_interval
        self._heartbeat_file = Path(heartbeat_file) if heartbeat_file else None
        self._fail_fast = fail_fast
        self._clock = clock
        self._metrics = _Metrics(port=metrics_port)
        self._user_window = UserOutcomeWindow(clock=clock)

        self.normal_queue = WorkQueue()
        self.high_queue = WorkQueue()
        self._wakeup = threading.Condition()
        self._stop = threading.Event()
        self._idle_wait = idle_wait or self._default_idle_wait
        self._last_scan: Optional[float] = None
        self.stats = collections.Counter()

    # --- Lifecycle -------------------------------------------------------

    def stop(self) -> None:
        self._stop.set()
        with self._wakeup:
            self._wakeup.notify_all()

    def _install_signal_handlers(self) -> None:
        def handler(signum, frame):
            _log.info(f"Received signal {signum}, stopping after current item")
            self.stop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, handler)

    def run(self, *, max_iterations: Optional[int] = None, install_signal_handlers: bool = True) -> None:
        """Run the daemon loop until stopped (or `max_iterations` reached, for testing)."""
        if install_signal_handlers:
            self._install_signal_handlers()
        if self._watcher:
            self._watcher.start(on_event=self._on_app_finished)
        _log.info(f"Starting job tracker daemon loop (scan interval {self._scan_interval}s, watcher={self._watcher})")
        iterations = 0
        try:
            while not self._stop.is_set():
                try:
                    self._iteration()
                except Exception as e:
                    # Individual sync/scan failures are handled inside `_iteration`; this is a last line of defense
                    # to keep the daemon alive on unexpected errors.
                    _log.exception(f"Unexpected error in daemon loop iteration: {type(e).__name__}: {e}")
                    self.stats["iteration failed"] += 1
                    if self._fail_fast:
                        raise
                    self._stop.wait(1.0)
                iterations += 1
                if max_iterations is not None and iterations >= max_iterations:
                    break
        finally:
            if self._watcher:
                self._watcher.stop()
            _log.info(f"Job tracker daemon loop stopped. stats: {dict(self.stats)}")

    # --- Loop ------------------------------------------------------------

    def _iteration(self) -> None:
        now = self._clock()
        self._touch_heartbeat()
        self._update_gauges()

        if self._watcher and not self._watcher.is_alive() and not self._stop.is_set():
            _log.error("SparkApplication watcher thread died, restarting it")
            self.stats["watcher restart"] += 1
            self._watcher.start(on_event=self._on_app_finished)

        if self._last_scan is None or now - self._last_scan >= self._scan_interval:
            self._scan()

        event = self.high_queue.pop()
        if event is not None:
            self._process_high_priority(event)
            return
        item = self.normal_queue.pop()
        if item is not None:
            self._process_normal(item)
            return

        next_scan_in = max(0.0, (self._last_scan or now) + self._scan_interval - now)
        self._idle_wait(next_scan_in)

    def _default_idle_wait(self, timeout: float) -> None:
        """Wait for a notification on self._wakeup and wake up if a stop is requested or there are events on
        the high priority queue."""
        with self._wakeup:
            # Predicate based wait: no lost wakeups between the queue check in `_iteration` and this wait.
            self._wakeup.wait_for(lambda: self._stop.is_set() or len(self.high_queue) > 0, timeout=timeout)

    def _touch_heartbeat(self) -> None:
        self._metrics.heartbeat.set(self._clock())
        if self._heartbeat_file:
            try:
                self._heartbeat_file.touch()
            except OSError as e:
                _log.warning(f"Failed to touch heartbeat file {self._heartbeat_file}: {e}")

    def _update_gauges(self) -> None:
        self._metrics.queue_size.labels(queue="normal").set(len(self.normal_queue))
        self._metrics.queue_size.labels(queue="high").set(len(self.high_queue))
        if self._watcher:
            self._metrics.watch_connected.set(int(self._watcher.connected))
            self._metrics.watch_restarts.set(self._watcher.restarts)
        users_total, users_mostly_errors = self._user_window.summary()
        self._metrics.users_with_final_jobs.set(users_total)
        self._metrics.users_with_mostly_errors.set(users_mostly_errors)

    def _scan(self) -> None:
        self._last_scan = self._clock()
        try:
            with TimingLogger("JobTrackerDaemon.scan", logger=_log.debug), self._metrics.scan_duration.time():
                jobs = self._job_tracker.list_jobs_to_track()
            added = 0
            for job_info in jobs:
                job_id = job_info.get("job_id") if isinstance(job_info, dict) else None
                if job_id and self.normal_queue.put(job_id, job_info):
                    added += 1
            _log.info(
                f"Scan collected {len(jobs)} active jobs, added {added} to normal queue (size {len(self.normal_queue)})"
            )
            self.stats["scan"] += 1
            self._metrics.scan_last_success.set(self._clock())
            self._metrics.scan_jobs_collected.set(len(jobs))
        except Exception as e:
            _log.exception(f"Failed to list active jobs: {type(e).__name__}: {e}")
            self.stats["scan failed"] += 1
            self._metrics.scan_failures.inc()
            if self._fail_fast:
                raise

    def _on_app_finished(self, event: AppFinishedEvent) -> None:
        """Callback from watcher thread"""
        self._metrics.watch_events.labels(state=event.app_state).inc()
        with self._wakeup:
            if self.high_queue.put(event.job_id, event):
                self._wakeup.notify_all()

    def _process_high_priority(self, event: AppFinishedEvent) -> None:
        log = logging.LoggerAdapter(_log, extra={"job_id": event.job_id, "user_id": event.user_id})
        try:
            job_info = self._job_tracker.get_job_info(job_id=event.job_id, user_id=event.user_id)
        except JobNotFoundException:
            # E.g. race: this should not happen but defensive programming so just warn and the periodic scan will pick it up.
            log.warning(f"High priority: job {event.job_id} not found in registry, dropping (app {event.app_id})")
            self.stats["high: job not found"] += 1
            self._metrics.job_sync.labels(result="dropped", priority="high").inc()
            return
        except Exception as e:
            log.exception(f"High priority: failed to get job {event.job_id}: {type(e).__name__}: {e}")
            self.stats["high: get job failed"] += 1
            self._metrics.job_sync.labels(result="failed", priority="high").inc()
            if self._fail_fast:
                raise
            return
        if job_info.get("application_id") != event.app_id:
            log.warning(
                f"High priority: app {event.app_id} does not match registry {job_info.get('application_id')!r}, dropping"
            )
            self.stats["high: app mismatch"] += 1
            self._metrics.job_sync.labels(result="dropped", priority="high").inc()
            return
        # Avoid processing it twice
        self.normal_queue.remove(event.job_id)
        self._sync(job_info, priority="high", finish_time=event.finish_time)

    def _process_normal(self, job_info: dict) -> None:
        self._sync(job_info, priority="normal")

    def _sync(self, job_info: dict, *, priority: str, finish_time: Optional[dt.datetime] = None) -> None:
        stats = collections.Counter()
        previous_status = job_info.get("status")
        ok = self._job_tracker.sync_job(job_info, stats=stats, fail_fast=self._fail_fast)
        for k, v in stats.items():
            self.stats[k] += v
        if ok:
            result = "ok"
        elif stats.get("failed sync"):
            result = "failed"
        else:
            result = "skipped"
        self._metrics.job_sync.labels(result=result, priority=priority).inc()
        if ok:
            new_status = self._final_status_from_stats(stats)
            if new_status:
                self._metrics.job_final_status.labels(status=new_status).inc()
                self._user_window.record(job_info["user_id"], new_status)
                if finish_time:
                    latency = (now_utc() - finish_time.astimezone(dt.timezone.utc)).total_seconds()
                    self._metrics.detection_latency.observe(max(0.0, latency))
                _log.info(
                    f"Job {job_info['job_id']} reached final status {new_status} (was {previous_status}, {priority=})",
                    extra={"job_id": job_info["job_id"], "user_id": job_info["user_id"]},
                )

    @staticmethod
    def _final_status_from_stats(stats: collections.Counter) -> Optional[str]:
        for status in FINAL_JOB_STATUSES:
            if stats.get(f"reached final status {status}"):
                return status
        return None
