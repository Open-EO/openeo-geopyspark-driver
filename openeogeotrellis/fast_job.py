"""
Fast execution of small batch jobs (EOEPCA/roadmap#566).

Instead of spinning up a dedicated batch job driver, a small batch job is run
"behind the scenes" in a dedicated thread ("fast job thread") of the web app driver,
reusing its (already running) Spark context.
The fast job thread updates the job status in the job registry and writes
results and STAC metadata in the same layout as a normal batch job.
"""

import datetime
import json
import logging
import os
import socket
import threading
from pathlib import Path
from typing import Callable, Iterable, Optional, Union

from openeo.util import rfc3339
from openeo_driver.jobregistry import JOB_STATUS

from openeogeotrellis.util.audience_logging import AUDIENCE_ALL

try:
    from openeo.rest.models.logs import normalize_log_level
except ImportError:
    from openeo.api.logs import normalize_log_level

_log = logging.getLogger(__name__)

# Job option to mark a job as "small" so that it is executed in the web app driver.
# TODO: this is a testing switch: replace with actual "is this job small?" detection.
JOB_OPTION_ALLOW_FAST_EXECUTION = "allow_fast_execution"

FAST_JOB_THREAD_NAME_PREFIX = "fast-job-"
FAST_JOB_LOG_FILENAME = "fast_job_log.jsonl"

_LOG_LEVEL_NAMES = {
    "CRITICAL": "error",
    "ERROR": "error",
    "WARNING": "warning",
    "INFO": "info",
    "DEBUG": "debug",
}


def is_fast_execution_requested(job_options: Optional[dict]) -> bool:
    if not isinstance(job_options, dict):
        return False
    value = job_options.get(JOB_OPTION_ALLOW_FAST_EXECUTION, False)
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return value is True


def fast_job_thread_name(job_id: str) -> str:
    return f"{FAST_JOB_THREAD_NAME_PREFIX}{job_id}"


def get_webapp_dns_id() -> str:
    """Identifier of the web app driver (instance) running the fast job thread."""
    return os.environ.get("POD_NAME") or socket.gethostname()


def fast_job_application_id(job_id: str) -> str:
    """Application id to register in the job registry: `webapp_dns_id + thread_id`."""
    return f"{get_webapp_dns_id()}/{fast_job_thread_name(job_id)}"


def is_fast_job_application_id(application_id: Optional[str]) -> bool:
    return bool(application_id) and f"/{FAST_JOB_THREAD_NAME_PREFIX}" in application_id


def is_thread_alive(thread_name: str) -> bool:
    """Is there a running thread in this process with the given name (e.g. `fast-job-<job_id>`)?"""
    return any(t.name == thread_name and t.is_alive() for t in threading.enumerate())


def get_fast_job_log_file(job_dir: Path) -> Path:
    return Path(job_dir) / FAST_JOB_LOG_FILENAME


class FastJobLogHandler(logging.Handler):
    """
    Logging handler that writes the log records emitted from a fast job thread
    (or records explicitly tagged with its `job_id`) as JSON lines to a per-job log file.
    """

    def __init__(self, *, job_id: str, log_file: Path, thread_name: Optional[str] = None, level=logging.DEBUG):
        super().__init__(level=level)
        self.job_id = job_id
        self.thread_name = thread_name or fast_job_thread_name(job_id)
        self.log_file = Path(log_file)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.log_file.open("a", encoding="utf8")

    def filter(self, record: logging.LogRecord) -> bool:
        return (
            record.threadName == self.thread_name or getattr(record, "job_id", None) == self.job_id
        ) and super().filter(record)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
            if record.exc_info:
                message += "\n" + logging.Formatter().formatException(record.exc_info)
            entry = {
                "time": rfc3339.datetime(datetime.datetime.fromtimestamp(record.created, tz=datetime.timezone.utc)),
                "levelname": record.levelname,
                "name": record.name,
                "message": message,
                "job_id": self.job_id,
                "audience": getattr(record, "audience", None),
            }
            line = json.dumps(entry, default=str)
            with self.lock:
                self._fh.write(line + "\n")
                self._fh.flush()
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        with self.lock:
            if not self._fh.closed:
                self._fh.close()
        super().close()


def read_fast_job_logs(
    log_file: Path,
    *,
    offset: Optional[str] = None,
    level: Optional[str] = None,
    audience: Optional[str] = None,
) -> Iterable[dict]:
    """Read openEO API style log entries from a fast job log file (see `FastJobLogHandler`)."""
    log_file = Path(log_file)
    if not log_file.exists():
        return
    min_level = normalize_log_level(level) if level else logging.DEBUG
    start = int(offset) + 1 if offset not in (None, "") else 0
    with log_file.open("r", encoding="utf8") as f:
        for index, line in enumerate(f):
            if index < start:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            levelname = record.get("levelname")
            openeo_level = _LOG_LEVEL_NAMES.get(levelname)
            if openeo_level is None or logging.getLevelName(levelname) < min_level:
                continue
            if audience and audience != AUDIENCE_ALL and record.get("audience") not in (None, audience):
                continue
            yield {
                "id": str(index),
                "time": record.get("time"),
                "level": openeo_level,
                "message": record.get("message"),
            }


def run_fast_job(
    *,
    job_id: str,
    user_id: str,
    job_dir: Path,
    set_status: Callable[..., None],
    get_status: Callable[[], str],
    run: Callable[[], None],
) -> None:
    """
    Body of a fast job thread: run the job (`run`) and keep the job status in the job registry up to date.

    :param set_status: callback to set the job status in the job registry: `set_status(status=..., **kwargs)`
    :param get_status: callback to get the current job status (e.g. to detect cancellation)
    :param run: callback that does the actual job evaluation and result writing.
    """
    log = logging.LoggerAdapter(_log, extra={"job_id": job_id, "user_id": user_id})
    handler = FastJobLogHandler(job_id=job_id, log_file=get_fast_job_log_file(job_dir))
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    try:
        log.info(f"Fast job {job_id} started in thread {threading.current_thread().name!r}")
        set_status(status=JOB_STATUS.RUNNING, started=rfc3339.now_utc())
        try:
            run()
        except BaseException as e:
            log.exception(f"Fast job {job_id} failed: {e!r}")
            status = JOB_STATUS.ERROR
        else:
            log.info(f"Fast job {job_id} finished successfully")
            status = JOB_STATUS.FINISHED
        try:
            current = get_status()
        except Exception:
            log.warning("Failed to get current job status", exc_info=True)
            current = None
        if current == JOB_STATUS.CANCELED:
            log.info(f"Fast job {job_id} was canceled: not updating status to {status!r}")
        else:
            set_status(status=status, finished=rfc3339.now_utc())
    except Exception:
        log.exception(f"Unexpected failure in fast job thread of {job_id}")
    finally:
        root_logger.removeHandler(handler)
        handler.close()


def start_fast_job_thread(
    *,
    job_id: str,
    user_id: str,
    job_dir: Path,
    set_status: Callable[..., None],
    get_status: Callable[[], str],
    run: Callable[[], None],
) -> threading.Thread:
    thread = threading.Thread(
        name=fast_job_thread_name(job_id),
        target=run_fast_job,
        kwargs=dict(
            job_id=job_id,
            user_id=user_id,
            job_dir=job_dir,
            set_status=set_status,
            get_status=get_status,
            run=run,
        ),
        daemon=True,
    )
    # TODO: thread cleaner: kill/abandon fast job threads that run longer than 30 minutes (EOEPCA/roadmap#566).
    thread.start()
    return thread
