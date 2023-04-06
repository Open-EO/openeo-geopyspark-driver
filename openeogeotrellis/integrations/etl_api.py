import logging
from time import sleep
from typing import Optional

import requests
from requests.exceptions import RequestException

SOURCE_ID = "TerraScope/MEP"
ORCHESTRATOR = "openeo"

_log = logging.getLogger(__name__)


class ETL_API_STATE:
    # https://etl.terrascope.be/docs/#/resources/ResourcesController_upsertResource

    ACCEPTED = "ACCEPTED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    KILLED = "KILLED",
    FAILED = "FAILED",
    UNDEFINED = "UNDEFINED"


class EtlApi:
    def __init__(self, endpoint: str, requests_session: Optional[requests.Session] = None):
        self._endpoint = endpoint
        self._session = requests_session or requests.Session()

    def log_resource_usage(self, batch_job_id: str, title: Optional[str], execution_id: str, user_id: str,
                           started_ms: Optional[float], finished_ms: Optional[float], state: str, status: str,
                           cpu_seconds: Optional[float], mb_seconds: Optional[float], duration_ms: Optional[float],
                           sentinel_hub_processing_units: Optional[float], access_token: str) -> float:
        metrics = {}

        if cpu_seconds is not None:
            metrics['cpu'] = {'value': cpu_seconds, 'unit': 'cpu-seconds'}

        if mb_seconds is not None:
            metrics['memory'] = {'value': mb_seconds, 'unit': 'mb-seconds'}

        if duration_ms is not None:
            metrics['time'] = {'value': duration_ms, 'unit': 'milliseconds'}

        if sentinel_hub_processing_units is not None:
            metrics['processing'] = {'value': sentinel_hub_processing_units, 'unit': 'shpu'}

        data = {
            'jobId': batch_job_id,
            'jobName': title,
            'executionId': execution_id,
            'userId': user_id,
            'sourceId': SOURCE_ID,
            'orchestrator': ORCHESTRATOR,
            'jobStart': started_ms,
            'jobFinish': finished_ms,
            'state': state,
            'status': status,
            'metrics': metrics
        }

        def send_request():
            with self._session.post(f"{self._endpoint}/resources", headers={'Authorization': f"Bearer {access_token}"},
                                    json=data) as resp:
                if not resp.ok:
                    _log.warning(
                        f"{resp.request.method} {resp.request.url} {data} returned {resp.status_code}: {resp.text}",
                        extra={
                            'user_id': user_id,
                            'job_id': batch_job_id
                        })

                resp.raise_for_status()

                total_credits = sum(resource['cost'] for resource in resp.json())
                return total_credits

        return self._retry(send_request)

    def log_added_value(self, batch_job_id: str, title: Optional[str], execution_id: str, user_id: str,
                        started_ms: Optional[float], finished_ms: Optional[float], process_id: str,
                        square_meters: Optional[float], access_token: str) -> float:
        billable = process_id not in ["fahrenheit_to_celsius", "mask_polygon", "mask_scl_dilation", "filter_bbox",
                                      "mean", "aggregate_spatial", "discard_result", "filter_temporal",
                                      "load_collection", "reduce_dimension", "apply_dimension", "not", "max", "or",
                                      "and", "run_udf", "save_result", "mask", "array_element", "add_dimension",
                                      "multiply", "subtract", "divide", "filter_spatial", "merge_cubes", "median",
                                      "filter_bands"]

        if not billable:
            return 0.0

        data = {
            'jobId': batch_job_id,
            'jobName': title,
            'executionId': execution_id,
            'userId': user_id,
            'sourceId': SOURCE_ID,
            'orchestrator': ORCHESTRATOR,
            'jobStart': started_ms,
            'jobFinish': finished_ms,
            'service': process_id,
        }

        if square_meters is not None:
            data['area'] = {'value': square_meters, 'unit': 'square_meter'}

        def send_request():
            with self._session.post(f"{self._endpoint}/addedvalue", headers={'Authorization': f"Bearer {access_token}"},
                                    json=data) as resp:
                if not resp.ok:
                    _log.warning(
                        f"{resp.request.method} {resp.request.url} {data} returned {resp.status_code}: {resp.text}",
                        extra={
                            'user_id': user_id,
                            'job_id': batch_job_id
                        })

                resp.raise_for_status()

                total_credits = sum(resource['cost'] for resource in resp.json())
                return total_credits

        return self._retry(send_request)

    @staticmethod
    def _retry(func):
        attempt = 1

        while True:
            try:
                return func()
            except RequestException as e:
                if attempt >= 5:
                    raise e

                attempt += 1
                sleep(10)
