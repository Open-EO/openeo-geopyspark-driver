"""
Fetching and polling STAC objects for load_stac.

Fetches the STAC object (Item, Collection, or Catalog) at a given URL,
polling until the results of a (partial) batch job are complete.

Note: polling for the own-job dependency case (`extract_own_job_info` /
`_await_dependency_job`) lives in `openeogeotrellis.stac.own_job` instead,
as it is genuinely backend/job-registry-specific (needs `BatchJobs`),
unlike the generic STAC object fetching here which is portable as-is.
"""
from __future__ import annotations

import dataclasses
import logging
import random
import time
from typing import Optional

import pystac
import pystac.stac_io
import requests
import requests.adapters
from pystac import STACObject
from urllib3 import Retry

from openeo_driver.jobregistry import PARTIAL_JOB_STATUS

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.integrations.stac import ResilientStacIO

logger = logging.getLogger(__name__)

STAC_API_BACKOFF_FACTOR = 2
STAC_API_RETRY_TOTAL = 25
STAC_API_MINIMUM_BACKOFF_SECONDS = 1
STAC_API_MAXIMUM_BACKOFF_SECONDS = 240
REQUESTS_TIMEOUT_SECONDS = 60


@dataclasses.dataclass(frozen=True)
class PollingConfig:
    """How long to keep polling a not-yet-complete STAC source, and how often."""

    poll_interval_seconds: float
    max_poll_delay_seconds: float

    @classmethod
    def from_backend_config(cls) -> "PollingConfig":
        backend_config = get_backend_config()
        return cls(
            poll_interval_seconds=backend_config.job_dependencies_poll_interval_seconds,
            max_poll_delay_seconds=backend_config.job_dependencies_max_poll_delay_seconds,
        )

    def deadline(self) -> float:
        return time.time() + self.max_poll_delay_seconds


class _JitteredRetry(Retry):
    """Retry with jitter to avoid thundering herd on 429 responses.

    - No Retry-After header: full jitter (random in [0, base_backoff])
    - Retry-After header present: respects it as a minimum with full jitter
    """

    def get_backoff_time(self) -> float:
        base = min(super().get_backoff_time(), STAC_API_MAXIMUM_BACKOFF_SECONDS)
        return random.uniform(0, base)

    def sleep_for_retry(self, response=None) -> bool:
        retry_after = self.get_retry_after(response)
        if retry_after is not None:
            backoff_time = max(super().get_backoff_time(), STAC_API_MINIMUM_BACKOFF_SECONDS)
            backoff_time = min(backoff_time, STAC_API_MAXIMUM_BACKOFF_SECONDS)
            jitter = random.uniform(0, backoff_time)
            time.sleep(retry_after + jitter)
            return True
        return False


def _await_stac_object(
    url: str,
    *,
    poll_interval_seconds: float,
    max_poll_delay_seconds: float,
    max_poll_time: float,
    stac_io: Optional[pystac.stac_io.StacIO] = None,
) -> STACObject:
    if stac_io is None:
        retry = _JitteredRetry(
            total=STAC_API_RETRY_TOTAL,
            backoff_factor=STAC_API_BACKOFF_FACTOR,
            status_forcelist={429, 500, 502, 503, 504},
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        stac_io = ResilientStacIO(session)

    while True:
        stac_object = pystac.read_file(href=url, stac_io=stac_io)

        if isinstance(stac_object, pystac.Catalog):
            stac_object._stac_io = stac_io  # TODO: avoid accessing internals (fix pystac)

        partial_job_status = stac_object.to_dict(include_self_link=False, transform_hrefs=False).get("openeo:status")

        logger.debug(f"OpenEO batch job results status of {url}: {partial_job_status}")

        if partial_job_status in [PARTIAL_JOB_STATUS.ERROR, PARTIAL_JOB_STATUS.CANCELED]:
            logger.error(f"Failing because OpenEO batch job with results at {url} failed")
        elif partial_job_status in [None, PARTIAL_JOB_STATUS.FINISHED]:
            break  # not a partial job result or success: proceed

        # still running: continue polling
        if time.time() >= max_poll_time:
            raise Exception(
                f"OpenEO batch job results dependency at {url} was not satisfied after"
                f" {max_poll_delay_seconds} s, aborting"
            )

        time.sleep(poll_interval_seconds)

    return stac_object

