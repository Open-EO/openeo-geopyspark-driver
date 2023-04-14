import abc
import datetime as dt
import logging
from typing import NamedTuple, Optional, List

from openeogeotrellis.integrations.etl_api import EtlApi, ETL_API_STATE
from openeogeotrellis.integrations.kubernetes import K8S_SPARK_APP_STATE


_log = logging.getLogger(__name__)


class CostsDetails(NamedTuple):  # for lack of a better name
    job_id: str
    user_id: str
    execution_id: str
    app_state: str
    area_square_meters: Optional[float] = None
    job_title: Optional[str] = None
    start_time: Optional[dt.datetime] = None
    finish_time: Optional[dt.datetime] = None
    cpu_seconds: Optional[float] = None
    mb_seconds: Optional[float] = None
    sentinelhub_processing_units: Optional[float] = None
    unique_process_ids: List[str] = []


class JobCostsCalculator(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def calculate_costs(self, details: CostsDetails) -> float:
        raise NotImplementedError


class NoJobCostsCalculator(JobCostsCalculator):
    def calculate_costs(self, details: CostsDetails) -> float:
        return 0.0


noJobCostsCalculator = NoJobCostsCalculator()


class EtlApiJobCostsCalculator(JobCostsCalculator):
    def __init__(self, etl_api: EtlApi, etl_api_access_token: str):
        self._etl_api = etl_api
        self._etl_api_access_token = etl_api_access_token

    @abc.abstractmethod
    def etl_api_state(self, app_state: str) -> str:
        raise NotImplementedError

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
            state=self.etl_api_state(details.app_state),
            status='UNDEFINED',  # TODO: map as well? it's just for reporting
            cpu_seconds=details.cpu_seconds,
            mb_seconds=details.mb_seconds,
            duration_ms=duration_ms,
            sentinel_hub_processing_units=details.sentinelhub_processing_units,
            access_token=self._etl_api_access_token
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
                access_token=self._etl_api_access_token) for process_id in details.unique_process_ids)

        return resource_costs_in_credits + added_value_costs_in_credits


class YarnJobCostsCalculator(EtlApiJobCostsCalculator):
    def __init__(self, etl_api: EtlApi, etl_api_access_token: str):
        super().__init__(etl_api, etl_api_access_token)

    def etl_api_state(self, app_state: str) -> str:
        return app_state


class K8sJobCostsCalculator(EtlApiJobCostsCalculator):
    def __init__(self, etl_api: EtlApi, etl_api_access_token: str):
        super().__init__(etl_api, etl_api_access_token)

    def etl_api_state(self, app_state: str) -> str:
        if app_state in {K8S_SPARK_APP_STATE.NEW, K8S_SPARK_APP_STATE.SUBMITTED}:
            return ETL_API_STATE.ACCEPTED
        if app_state in {K8S_SPARK_APP_STATE.RUNNING, K8S_SPARK_APP_STATE.SUCCEEDING}:
            return ETL_API_STATE.RUNNING
        if app_state == K8S_SPARK_APP_STATE.COMPLETED:
            return ETL_API_STATE.FINISHED
        if app_state in {K8S_SPARK_APP_STATE.FAILED, K8S_SPARK_APP_STATE.SUBMISSION_FAILED,
                         K8S_SPARK_APP_STATE.FAILING}:
            return ETL_API_STATE.FAILED

        return ETL_API_STATE.ACCEPTED
