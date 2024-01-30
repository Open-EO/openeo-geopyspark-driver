import logging
import os
from typing import Callable, Dict, Optional, Union

import requests
from openeo_driver.config import get_backend_config
from openeo_driver.users import User
from openeo_driver.util.auth import ClientCredentials, ClientCredentialsAccessTokenHelper
from openeo_driver.util.caching import TtlCache

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.config.config import EtlApiConfig

ORCHESTRATOR = "openeo"

_log = logging.getLogger(__name__)


class ETL_API_STATE:
    """
    Possible values for the "state" field in the ETL API
    (e.g. `POST /resources` endpoint https://etl.terrascope.be/docs/#/resources/ResourcesController_upsertResource).
    Note that this roughly corresponds to YARN app state (for legacy reasons), also see `YARN_STATE`.
    """

    # TODO #610 Simplify ETL-level state/status complexity to a single openEO-style job status
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

    # TODO #610 Simplify ETL-level state/status complexity to a single openEO-style job status
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    KILLED = "KILLED"
    UNDEFINED = "UNDEFINED"


class EtlApi:
    """
    API for reporting resource usage and added value to the ETL (EOPlaza marketplace) API
    and deriving a cost estimate.
    """

    __slots__ = ("_endpoint", "_source_id", "_session", "_access_token_helper")

    def __init__(
        self,
        endpoint: str,
        *,
        credentials: ClientCredentials,
        source_id: Optional[str] = None,
        requests_session: Optional[requests.Session] = None,
    ):
        self._endpoint = endpoint
        self._source_id = source_id or get_backend_config().etl_source_id
        _log.debug(f"EtlApi.__init__() with {self._endpoint=} {self._source_id=}")
        self._session = requests_session or requests.Session()
        self._access_token_helper = ClientCredentialsAccessTokenHelper(session=self._session, credentials=credentials)

    @property
    def root_url(self) -> str:
        return self._endpoint

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
            'idempotencyKey': execution_id,
            # TODO #610 simplify this state/status stuff to single openEO-style job status?
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
            'idempotencyKey': execution_id,
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


def get_etl_api_credentials_from_env() -> ClientCredentials:
    """Get ETL API OIDC client credentials from environment."""
    if os.environ.get("OPENEO_ETL_OIDC_CLIENT_CREDENTIALS"):
        _log.debug("Getting ETL credentials from env var (compact style)")
        return ClientCredentials.from_credentials_string(os.environ["OPENEO_ETL_OIDC_CLIENT_CREDENTIALS"], strict=True)
    elif all(
        v in os.environ
        for v in ["OPENEO_ETL_API_OIDC_ISSUER", "OPENEO_ETL_OIDC_CLIENT_ID", "OPENEO_ETL_OIDC_CLIENT_SECRET"]
    ):
        # TODO: deprecate this code path and just go for compact `ClientCredentials.from_credentials_string`
        _log.debug("Getting ETL credentials from env vars (triplet style)")
        return ClientCredentials(
            oidc_issuer=os.environ["OPENEO_ETL_API_OIDC_ISSUER"],
            client_id=os.environ["OPENEO_ETL_OIDC_CLIENT_ID"],
            client_secret=os.environ["OPENEO_ETL_OIDC_CLIENT_SECRET"],
        )
    else:
        raise RuntimeError("No ETL API credentials configured")


class SimpleEtlApiConfig(EtlApiConfig):
    """Simple EtlApiConfig: just a single ETL API endpoint."""

    __slots__ = ["_root_url", "_client_credentials"]

    def __init__(self, root_url: str, client_credentials: Optional[ClientCredentials] = None):
        super().__init__()
        self._root_url = root_url
        self._client_credentials = client_credentials

    def get_root_url(self, *, user: Optional[User] = None, job_options: Optional[dict] = None) -> str:
        return self._root_url

    def get_client_credentials(self, root_url: str) -> Optional[ClientCredentials]:
        assert root_url == self._root_url
        return self._client_credentials


class DynamicEtlApiConfig(EtlApiConfig):
    """Minimal base EtlApiConfig implementation supporting dynamic ETL API selection."""

    def __init__(self, urls_and_credentials: Dict[str, ClientCredentials]):
        self._urls_and_credentials = urls_and_credentials

    def get_root_url(self, *, user: Optional[User] = None, job_options: Optional[dict] = None) -> str:
        # TODO: possible to provide some generic logic here?
        raise NotImplementedError

    def get_client_credentials(self, root_url: str) -> Optional[ClientCredentials]:
        # TODO: return None on unknown root_url instead of raising KeyError?
        return self._urls_and_credentials[root_url]


def get_etl_api(
    *,
    root_url: Optional[str] = None,
    user: Optional[User] = None,
    job_options: Optional[dict] = None,
    requests_session: Optional[requests.Session] = None,
    # TODO #531 remove this temporary feature flag/toggle for dynamic ETL selection.
    allow_dynamic_etl_api: bool = False,
    etl_api_cache: Optional[TtlCache] = None,
) -> EtlApi:
    """
    Get EtlApi, possibly depending on additional data (pre-determined root_url, current user, ...).

    :param user: (optional) user to dynamically determine ETL API endpoint with `EtlApiConfig.get_root_url` API
    :param job_options: (optional) job options dict to dynamically determine ETL API endpoint with `EtlApiConfig.get_root_url` API
    :param requests_session: (optional) provide a `requests.Session` (e.g. with desired retry settings) to use for HTTP requests
    :param etl_api_cache: (optional) provide a `TtlCache` to fetch existing `EtlApi` instances from
        to avoid repeatedly setting up `EtlApi` instances each time
        (which can be costly due to client credentials related OIDC discovery requests).
        Note that the caller is cache owner and responsible for providing the same cache instance on each call
        and setting the default TTL (as desired).
    """
    # TODO #531 is there a practical need to expose `root_url` to the caller?
    backend_config = get_backend_config()
    etl_config: Optional[EtlApiConfig] = backend_config.etl_api_config
    _log.info(f"get_etl_api with {etl_config=}")

    def get_cached_or_build(cache_key: tuple, build: Callable[[], EtlApi]) -> EtlApi:
        """Helper to build an EtlApi object, with optional caching."""
        if etl_api_cache:
            return etl_api_cache.get_or_call(cache_key, build)
        else:
            return build()

    dynamic_etl_mode = allow_dynamic_etl_api and (etl_config is not None)
    if dynamic_etl_mode:
        # First get root URL as main ETL API identifier
        if root_url is None:
            root_url = etl_config.get_root_url(user=user, job_options=job_options)
        _log.debug(f"get_etl_api: dynamic EtlApiConfig based ETL API selection: {root_url=}")

        # Build EtlApi (or get from cache if possible)
        return get_cached_or_build(
            cache_key=("get_etl_api", root_url),
            build=lambda: EtlApi(
                endpoint=root_url,
                credentials=etl_config.get_client_credentials(root_url=root_url),
                requests_session=requests_session,
            ),
        )
    else:
        # TODO #531 eliminate this code path
        _log.debug("get_etl_api: legacy static EtlApi")
        return get_cached_or_build(
            cache_key=("get_etl_api", "__static_etl_api__"),
            build=lambda: EtlApi(
                endpoint=backend_config.etl_api,
                credentials=get_etl_api_credentials_from_env(),
                requests_session=requests_session,
            ),
        )


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
