import logging
import os
from typing import Optional

import requests

from openeo.rest.auth.oidc import OidcProviderInfo, OidcClientInfo, OidcClientCredentialsAuthenticator
from openeo_driver.config import get_backend_config
from openeo_driver.util.auth import ClientCredentials, ClientCredentialsAccessTokenHelper
from openeogeotrellis.vault import Vault

ORCHESTRATOR = "openeo"

_log = logging.getLogger(__name__)


class ETL_API_STATE:
    """
    Possible values for the "state" field in the ETL API
    (e.g. `POST /resources` endpoint https://etl.terrascope.be/docs/#/resources/ResourcesController_upsertResource).
    Note that this roughly corresponds to YARN app state (for legacy reasons), also see `YARN_STATE`.
    """
    ACCEPTED = "ACCEPTED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    KILLED = "KILLED"
    FAILED = "FAILED"
    UNDEFINED = "UNDEFINED"


class ETL_API_STATUS:
    """
    Possible values for the "status" field in the ETL API
    (e.g. `POST /resources` endpoint https://etl.terrascope.be/docs/#/resources/ResourcesController_upsertResource).
    Note that this roughly corresponds to YARN final status (for legacy reasons), also see `YARN_FINAL_STATUS`.
    """

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    KILLED = "KILLED"
    UNDEFINED = "UNDEFINED"


class EtlApi:
    """
    API for reporting resource usage and added value to the ETL (EOPlaza marketplace) API
    and deriving a cost estimate.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        source_id: Optional[str] = None,
        requests_session: Optional[requests.Session] = None,
        credentials: Optional[ClientCredentials] = None,
    ):
        self._endpoint = endpoint
        self._source_id = source_id or get_backend_config().etl_source_id
        self._session = requests_session or requests.Session()
        self._access_token_helper = ClientCredentialsAccessTokenHelper(session=self._session, credentials=credentials)

    def assert_access_token_valid(self, access_token: Optional[str] = None):
        # will work regardless of ability to log resources
        access_token = access_token or self._access_token_helper.get_access_token()
        with self._session.get(f"{self._endpoint}/user/permissions",
                               headers={'Authorization': f"Bearer {access_token}"}) as resp:
            _log.debug(resp.text)
            resp.raise_for_status()  # value of "execution" is unrelated

    def assert_can_log_resources(self, access_token: Optional[str] = None):
        # will also work if able to log resources
        access_token = access_token or self._access_token_helper.get_access_token()
        with self._session.get(f"{self._endpoint}/validate/auth",
                               headers={'Authorization': f"Bearer {access_token}"}) as resp:
            _log.debug(resp.text)
            resp.raise_for_status()

    def log_resource_usage(self, batch_job_id: str, title: Optional[str], execution_id: str, user_id: str,
                           started_ms: Optional[float], finished_ms: Optional[float], state: str, status: str,
                           cpu_seconds: Optional[float], mb_seconds: Optional[float], duration_ms: Optional[float],
                           sentinel_hub_processing_units: Optional[float]) -> float:
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
            'sourceId': self._source_id,
            'orchestrator': ORCHESTRATOR,
            'jobStart': started_ms,
            'jobFinish': finished_ms,
            'state': state,
            'status': status,
            'metrics': metrics
        }

        log.debug(f"logging resource usage {data} at {self._endpoint}")

        access_token = self._access_token_helper.get_access_token()
        with self._session.post(f"{self._endpoint}/resources", headers={'Authorization': f"Bearer {access_token}"},
                                json=data) as resp:
            if not resp.ok:
                log.warning(
                    f"{resp.request.method} {resp.request.url} {data} returned {resp.status_code}: {resp.text}")

            resp.raise_for_status()

            total_credits = sum(resource['cost'] for resource in resp.json())
            return total_credits

    def log_added_value(self, batch_job_id: str, title: Optional[str], execution_id: str, user_id: str,
                        started_ms: Optional[float], finished_ms: Optional[float], process_id: str,
                        square_meters: float) -> float:
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
            'sourceId': self._source_id,
            'orchestrator': ORCHESTRATOR,
            'jobStart': started_ms,
            'jobFinish': finished_ms,
            'service': process_id,
            'area': {'value': square_meters, 'unit': 'square_meter'}
        }

        log.debug(f"logging added value {data} at {self._endpoint}")

        access_token = self._access_token_helper.get_access_token()
        with self._session.post(f"{self._endpoint}/addedvalue", headers={'Authorization': f"Bearer {access_token}"},
                                json=data) as resp:
            if not resp.ok:
                log.warning(
                    f"{resp.request.method} {resp.request.url} {data} returned {resp.status_code}: {resp.text}")

            resp.raise_for_status()

            total_credits = sum(resource['cost'] for resource in resp.json())
            return total_credits


def get_etl_api_credentials(
    kerberos_principal: str,
    key_tab: str,
    requests_session: Optional[requests.Session] = None,
) -> ClientCredentials:
    # TODO: unify this with get_etl_api_access_token
    if os.environ.get("OPENEO_ETL_OIDC_CLIENT_CREDENTIALS"):
        _log.debug("Getting ETL credentials from env var (compact style)")
        return ClientCredentials.from_credentials_string(os.environ["OPENEO_ETL_OIDC_CLIENT_CREDENTIALS"], strict=True)
    elif all(
        v in os.environ
        for v in [
            # "OPENEO_ETL_API_OIDC_ISSUER",
            "OPENEO_ETL_OIDC_CLIENT_ID",
            "OPENEO_ETL_OIDC_CLIENT_SECRET",
        ]
    ):
        # TODO: deprecate this code path and just go for compact `ClientCredentials.from_credentials_string`
        _log.debug("Getting ETL credentials from env vars (triplet style)")
        return ClientCredentials(
            oidc_issuer=os.environ.get("OPENEO_ETL_API_OIDC_ISSUER") or get_backend_config().etl_api_oidc_issuer,
            client_id=os.environ["OPENEO_ETL_OIDC_CLIENT_ID"],
            client_secret=os.environ["OPENEO_ETL_OIDC_CLIENT_SECRET"],
        )
    else:
        _log.debug("Getting ETL credentials from vault")
        # Get credentials directly from vault
        # TODO: eliminate this code path? https://github.com/Open-EO/openeo-geopyspark-driver/issues/564
        vault = Vault(get_backend_config().vault_addr, requests_session)
        vault_token = vault.login_kerberos(kerberos_principal, key_tab)
        return vault.get_etl_api_credentials(vault_token)


def assert_resource_logging_possible():
    # TODO: still necessary to keep this function around, or was this just a temp debugging thing?

    logging.basicConfig(level="DEBUG")

    credentials = ClientCredentials(
        oidc_issuer=os.environ.get("OPENEO_ETL_API_OIDC_ISSUER", "https://cdasid.cloudferro.com/auth/realms/CDAS"),
        client_id=os.environ.get("OPENEO_ETL_OIDC_CLIENT_ID", "openeo-job-tracker"),
        client_secret=os.environ.get("OPENEO_ETL_OIDC_CLIENT_SECRET", "..."),
    )
    etl_url = os.environ.get("OPENEO_ETL_API", "https://marketplace-cost-api-stag-warsaw.dataspace.copernicus.eu")
    source_id = "cdse"

    etl_api = EtlApi(etl_url, source_id=source_id, credentials=credentials)

    etl_api.assert_access_token_valid()
    etl_api.assert_can_log_resources()


if __name__ == '__main__':
    assert_resource_logging_possible()
