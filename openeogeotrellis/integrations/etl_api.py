import logging
from typing import Optional

import requests
from openeo.rest.auth.oidc import OidcProviderInfo, OidcClientInfo, OidcClientCredentialsAuthenticator

from openeogeotrellis.configparams import ConfigParams

SOURCE_ID = "TerraScope/MEP"
ORCHESTRATOR = "openeo"

_log = logging.getLogger(__name__)


class ETL_API_STATE:
    # https://etl.terrascope.be/docs/#/resources/ResourcesController_upsertResource

    ACCEPTED = "ACCEPTED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    KILLED = "KILLED"
    FAILED = "FAILED"
    UNDEFINED = "UNDEFINED"


class EtlApi:
    def __init__(self, endpoint: str, requests_session: Optional[requests.Session] = None):
        self._endpoint = endpoint
        self._session = requests_session or requests.Session()

    def assert_access_token_valid(self, access_token: str):
        # will work regardless of ability to log resources
        with self._session.get(f"{self._endpoint}/user/permissions",
                               headers={'Authorization': f"Bearer {access_token}"}) as resp:
            _log.debug(resp.text)
            resp.raise_for_status()  # value of "execution" is unrelated

    def assert_can_log_resources(self, access_token: str):
        # will also work if able to log resources
        with self._session.get(f"{self._endpoint}/validate/auth",
                               headers={'Authorization': f"Bearer {access_token}"}) as resp:
            _log.debug(resp.text)
            resp.raise_for_status()

    def log_resource_usage(self, batch_job_id: str, title: Optional[str], execution_id: str, user_id: str,
                           started_ms: Optional[float], finished_ms: Optional[float], state: str, status: str,
                           cpu_seconds: Optional[float], mb_seconds: Optional[float], duration_ms: Optional[float],
                           sentinel_hub_processing_units: Optional[float], access_token: str) -> float:
        log = logging.LoggerAdapter(_log, extra={"job_id": batch_job_id, "user_id": user_id})

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

        log.debug(f"logging resource usage {data}")

        with self._session.post(f"{self._endpoint}/resources", headers={'Authorization': f"Bearer {access_token}"},
                                json=data) as resp:
            if not resp.ok:
                log.warning(
                    f"{resp.request.method} {resp.request.url} {data} returned {resp.status_code}: {resp.text}",
                    extra={
                        'user_id': user_id,
                        'job_id': batch_job_id
                    })

            resp.raise_for_status()

            total_credits = sum(resource['cost'] for resource in resp.json())
            return total_credits

    def log_added_value(self, batch_job_id: str, title: Optional[str], execution_id: str, user_id: str,
                        started_ms: Optional[float], finished_ms: Optional[float], process_id: str,
                        square_meters: float, access_token: str) -> float:
        log = logging.LoggerAdapter(_log, extra={"job_id": batch_job_id, "user_id": user_id})

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
            'area': {'value': square_meters, 'unit': 'square_meter'}
        }

        log.debug(f"logging added value {data}")

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


def get_etl_api_access_token(client_id: str, client_secret: str, requests_session: requests.Session) -> str:
    oidc_provider = OidcProviderInfo(
        # TODO: get issuer from the secret as well? (~ openeo-job-registry-elastic-api)
        issuer=ConfigParams().etl_api_oidc_issuer,
        requests_session=requests_session,
    )
    client_info = OidcClientInfo(
        provider=oidc_provider,
        client_id=client_id,
        client_secret=client_secret,
    )

    authenticator = OidcClientCredentialsAuthenticator(
        client_info=client_info,
        requests_session=requests_session,
    )
    return authenticator.get_tokens().access_token


def assert_resource_logging_possible():
    import os

    logging.basicConfig(level="DEBUG")

    os.environ['OPENEO_ETL_API_OIDC_ISSUER'] = "https://cdasid.cloudferro.com/auth/realms/CDAS"
    client_id = 'openeo-job-tracker'
    client_secret = ...

    requests_session = requests.Session()

    access_token = get_etl_api_access_token(client_id, client_secret, requests_session)
    print(access_token)

    etl_api = EtlApi("https://marketplace-cost-api-stag-warsaw.dataspace.copernicus.eu", requests_session)

    etl_api.assert_access_token_valid(access_token)
    etl_api.assert_can_log_resources(access_token)


if __name__ == '__main__':
    assert_resource_logging_possible()
