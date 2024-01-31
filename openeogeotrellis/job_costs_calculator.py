import abc
import datetime as dt
import logging
from typing import List, NamedTuple, Optional

from openeo_driver.util.caching import TtlCache
from openeo_driver.util.http import requests_with_retry

from openeogeotrellis.integrations.etl_api import ETL_API_STATUS, EtlApi, get_etl_api

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
        super().__init__()
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


class DynamicEtlApiJobCostCalculator(JobCostsCalculator):
    """
    Like EtlApiJobCostsCalculator but with an ETL API endpoint that is determined based on user or job data.
    It basically moves determining the ETL API url (`get_etl_api()`) from constructor time
    to per-case `calculate_costs()` call time
    """

    def __init__(self, cache_ttl: int = 5 * 60):
        self._request_session = requests_with_retry(total=3, backoff_factor=2)
        # Cache of `EtlApi` instances, used in `get_etl_api()`
        self._etl_cache: Optional[TtlCache] = TtlCache(default_ttl=cache_ttl) if cache_ttl > 0 else None

    def calculate_costs(self, details: CostsDetails) -> float:
        job_options = details.job_options or {}
        etl_api = get_etl_api(
            job_options=job_options,
            allow_dynamic_etl_api=True,
            requests_session=self._request_session,
            etl_api_cache=self._etl_cache,
        )
        _log.debug(f"DynamicEtlApiJobCostCalculator.calculate_costs with {etl_api=}")
        # Reuse logic from EtlApiJobCostsCalculator
        return EtlApiJobCostsCalculator(etl_api=etl_api).calculate_costs(details=details)
