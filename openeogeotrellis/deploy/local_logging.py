"""
Minimal Elasticsearch log shipping for the local (single-machine) openEO dev deployment
(`openeogeotrellis.deploy.local`).

In "real" deployments (YARN/Kubernetes), batch job JSON log files get shipped to
Elasticsearch by an external log forwarder (e.g. Filebeat), matching the index
pattern/field layout that `openeogeotrellis.logs.elasticsearch_logs` queries
(a `job_id` term filter, an `openeo` tag, `levelname`, `message`, `@timestamp`, ...).

`local.py` runs batch jobs synchronously in-process (in a background thread) instead
of going through that log forwarding pipeline, so `GET /jobs/{id}/logs` would otherwise
always come back empty. This module provides a lightweight `logging.Handler` that
indexes matching log records (i.e. ones carrying a `job_id`, see
`openeo_driver.util.logging.ExtraLoggingFilter`) directly into Elasticsearch, so batch
job logs remain queryable locally, the same way they are in production.
"""

import datetime as dt
import itertools
import logging
import os
import threading
from typing import Optional

from openeo_driver.util.logging import ExtraLoggingFilter

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.logs import ES_TAGS

_log = logging.getLogger(__name__)


class ElasticsearchLoggingHandler(logging.Handler):
    """
    Logging handler that ships log records tagged with a `job_id`
    (e.g. through `ExtraLoggingFilter.with_extra_logging(job_id=...)`) straight into
    Elasticsearch, using the index pattern/field layout expected by
    `openeogeotrellis.logs.elasticsearch_logs`.
    """

    def __init__(self, level=logging.NOTSET):
        super().__init__(level=level)
        # `ExtraLoggingFilter.data` is a shared (class level) thread-local, so adding
        # our own instance as filter is enough to pick up e.g. `job_id`/`user_id`
        # set through `ExtraLoggingFilter.with_extra_logging(...)` in the current thread.
        self.addFilter(ExtraLoggingFilter())
        self._offset_counter = itertools.count()
        self._lock = threading.Lock()
        self._es = None
        self._broken = False
        # Re-entrancy guard (per thread): the Elasticsearch client itself logs its HTTP
        # requests (e.g. through the "elasticsearch"/"urllib3" loggers), and since that
        # happens on the same (job) thread, it would otherwise carry the same `job_id`
        # (set through `ExtraLoggingFilter.with_extra_logging`) and recurse right back
        # into this handler, indexing its own request log, indefinitely.
        self._emitting = threading.local()

    def _client(self):
        if self._es is None:
            from elasticsearch import Elasticsearch

            self._es = Elasticsearch(get_backend_config().logging_es_hosts)
        return self._es

    def _index_name(self) -> str:
        # e.g. index pattern "openeo-local-index-1m*" -> index "openeo-local-index-1m-2024.01.01"
        # (mimics the daily rolling indices a real log forwarder would create).
        pattern = get_backend_config().logging_es_index_pattern
        base = pattern.rstrip("*")
        if not base.endswith("-"):
            base += "-"
        return base + dt.datetime.utcnow().strftime("%Y.%m.%d")

    def emit(self, record: logging.LogRecord):
        if getattr(self._emitting, "active", False):
            # Re-entrant call (e.g. the ES client logging its own HTTP request): skip it,
            # or we would end up shipping (and recursing on) our own request logs forever.
            return
        job_id = getattr(record, "job_id", None)
        if not job_id:
            # Only ship batch job log records (the only ones queried through `/jobs/{id}/logs`).
            return
        self._emitting.active = True
        try:
            with self._lock:
                offset = next(self._offset_counter)
            document = {
                "@timestamp": dt.datetime.utcfromtimestamp(record.created).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
                + "Z",
                "levelname": record.levelname,
                "message": record.getMessage(),
                "name": record.name,
                "job_id": job_id,
                "tags": ES_TAGS,
                "log": {"offset": offset},
            }
            user_id = getattr(record, "user_id", None)
            if user_id:
                document["user_id"] = user_id
            self._client().index(index=self._index_name(), body=document)
        except Exception:
            if not self._broken:
                # Avoid spamming logs (and infinite recursion) if Elasticsearch is unreachable.
                self._broken = True
                _log.warning(
                    "Failed to ship log record to Elasticsearch (further failures will be suppressed)",
                    exc_info=True,
                )
        finally:
            self._emitting.active = False


def setup_es_log_shipping(level: int = logging.DEBUG) -> Optional[ElasticsearchLoggingHandler]:
    """
    Attach an `ElasticsearchLoggingHandler` to the root logger, so that batch job logs
    (run in-process by `local.py`) end up in Elasticsearch and remain retrievable through
    the regular `GET /jobs/{id}/logs` endpoint.
    """
    if "LOGGING_ES_HOSTS" not in os.environ:
        _log.info("LOGGING_ES_HOSTS is not set: skipping local Elasticsearch log shipping setup")
        return None
    handler = ElasticsearchLoggingHandler(level=level)
    logging.getLogger().addHandler(handler)
    _log.info(f"Attached {handler!r} to root logger (hosts={get_backend_config().logging_es_hosts!r})")
    return handler
