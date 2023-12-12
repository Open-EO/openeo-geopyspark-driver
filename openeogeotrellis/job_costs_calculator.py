import abc
import datetime as dt
import logging
from typing import NamedTuple, Optional, List

from openeogeotrellis.integrations.etl_api import EtlApi, ETL_API_STATUS

_log = logging.getLogger(__name__)


class CostsDetails(NamedTuple):  # for lack of a better name
    """
    Container for batch job details that are relevant for reporting resource usage and calculating costs.
    """
    job_id: str
    user_id: str
    execution_id: str
    # TODO #610 this is just part of a temporary migration path, to be cleaned up when just openEO-style `job_status` can be used
    app_state_etl_api_deprecated: Optional[str] = None
    job_status: Optional[str] = None  # (openEO style) job status #TODO #610
    area_square_meters: Optional[float] = None
    job_title: Optional[str] = None
    start_time: Optional[dt.datetime] = None
    finish_time: Optional[dt.datetime] = None
    cpu_seconds: Optional[float] = None
    mb_seconds: Optional[float] = None
    sentinelhub_processing_units: Optional[float] = None
    unique_process_ids: List[str] = []
    job_options: Optional[dict] = None


class JobCostsCalculator(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def calculate_costs(self, details: CostsDetails) -> float:
        raise NotImplementedError


class NoJobCostsCalculator(JobCostsCalculator):
    def calculate_costs(self, details: CostsDetails) -> float:
        return 0.0


noJobCostsCalculator = NoJobCostsCalculator()


class EtlApiJobCostsCalculator(JobCostsCalculator):
    """
    Base class for cost calculators based on resource reporting with ETL API.
    """
    def __init__(self, etl_api: EtlApi):
        self._etl_api = etl_api

    def calculate_costs(self, details: CostsDetails) -> float:
        started_ms = details.start_time.timestamp() * 1000 if details.start_time is not None else None
        finished_ms = details.finish_time.timestamp() * 1000 if details.finish_time is not None else None
        duration_ms = finished_ms - started_ms if finished_ms is not None and started_ms is not None else None

        resource_costs_in_credits = self._etl_api.log_resource_usage(
            batch_job_id=details.job_id,
            title=details.job_title,
            execution_id=details.execution_id,
            user_id=details.user_id,
            started_ms=started_ms,
            finished_ms=finished_ms,
            # TODO #610 replace state/status with generic openEO-style job status
            state=details.app_state_etl_api_deprecated,
            status=ETL_API_STATUS.UNDEFINED,  # TODO: map as well? it's just for reporting
            # TODO #610 already possible to send `details.job_status` in request?
            cpu_seconds=details.cpu_seconds,
            mb_seconds=details.mb_seconds,
            duration_ms=duration_ms,
            sentinel_hub_processing_units=details.sentinelhub_processing_units,
        )

        if details.area_square_meters is None:
            added_value_costs_in_credits = 0.0
            _log.debug("not logging added value because area is None")
        else:
            added_value_costs_in_credits = sum(self._etl_api.log_added_value(
                batch_job_id=details.job_id,
                title=details.job_title,
                execution_id=details.execution_id,
                user_id=details.user_id,
                started_ms=started_ms,
                finished_ms=finished_ms,
                process_id=process_id,
                square_meters=details.area_square_meters,
                ) for process_id in details.unique_process_ids)

        return resource_costs_in_credits + added_value_costs_in_credits
