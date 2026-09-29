"""
Own-job dependency polling for load_stac.

Resolving a load_stac `url` that points at a sibling batch job of the current
user (rather than a live STAC object) requires backend/job-registry-specific
logic (needs `BatchJobs`), unlike the rest of the `stac` package which is
portable/engine-agnostic.
"""
from __future__ import annotations

import logging
import time
from typing import Optional
from urllib.parse import urlparse

import openeo_driver.backend
from openeo_driver.backend import BatchJobMetadata
from openeo_driver.errors import JobNotFoundException
from openeo_driver.jobregistry import PARTIAL_JOB_STATUS
from openeo_driver.users import User

logger = logging.getLogger(__name__)


def extract_own_job_info(
    url: str, user_id: str, batch_jobs: openeo_driver.backend.BatchJobs
) -> Optional[BatchJobMetadata]:
    path_segments = urlparse(url).path.split("/")

    if len(path_segments) < 3:
        return None

    jobs_position_segment, job_id, results_position_segment = path_segments[-3:]
    if jobs_position_segment != "jobs" or results_position_segment != "results":
        return None

    try:
        return batch_jobs.get_job_info(job_id=job_id, user_id=user_id)
    except JobNotFoundException:
        logger.debug(f"job {job_id} does not belong to current user {user_id}", exc_info=True)
        return None


def await_dependency_job(
    url: str,
    *,
    user: Optional[User] = None,
    batch_jobs: Optional[openeo_driver.backend.BatchJobs] = None,
    poll_interval_seconds: float,
    max_poll_delay_seconds: float,
    max_poll_time: float,
) -> Optional[BatchJobMetadata]:
    def get_dependency_job_info() -> Optional[BatchJobMetadata]:
        return extract_own_job_info(url, user.user_id, batch_jobs) if user and batch_jobs else None

    dependency_job_info = get_dependency_job_info()
    if not dependency_job_info:
        return None

    logger.info(f"load_stac of results of own job {dependency_job_info.id}")

    while True:
        partial_job_status = PARTIAL_JOB_STATUS.for_job_status(dependency_job_info.status)

        logger.debug(f"OpenEO batch job results status of own job {dependency_job_info.id}: {partial_job_status=}")

        if partial_job_status in [PARTIAL_JOB_STATUS.ERROR, PARTIAL_JOB_STATUS.CANCELED]:
            logger.error(f"Failing because own OpenEO batch job {dependency_job_info.id} failed")
        elif partial_job_status in [None, PARTIAL_JOB_STATUS.FINISHED]:
            break  # not a partial job result or success: proceed

        # still running: continue polling
        if time.time() >= max_poll_time:
            max_poll_delay_reached_error = (
                f"OpenEO batch job results dependency of"
                f" own job {dependency_job_info.id} was not satisfied after"
                f" {max_poll_delay_seconds}s, aborting"
            )
            raise Exception(max_poll_delay_reached_error)

        time.sleep(poll_interval_seconds)

        dependency_job_info = get_dependency_job_info()

    return dependency_job_info
