import logging
import os
from typing import Callable, Dict, Optional, Union, Tuple, List

import requests
from openeo_driver.config import get_backend_config
from openeo_driver.users import User
from openeo_driver.util.auth import ClientCredentials, ClientCredentialsAccessTokenHelper
from openeo_driver.util.caching import TtlCache

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.config.config import EtlApiConfig

ORCHESTRATOR = "openeo"
REQUESTS_TIMEOUT_SECONDS = 60

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
                               headers={'Authorization': f"Bearer {access_token}"},
                               timeout=REQUESTS_TIMEOUT_SECONDS) as resp:
            _log.debug(resp.text)
            resp.raise_for_status()  # value of "execution" is unrelated

    def assert_can_log_resources(self, access_token: Optional[str] = None):
        # will also work if able to log resources
        access_token = access_token or self._access_token_helper.get_access_token()
        with self._session.get(f"{self._endpoint}/validate/auth",
                               headers={'Authorization': f"Bearer {access_token}"},
                               timeout=REQUESTS_TIMEOUT_SECONDS) as resp:
            _log.debug(resp.text)
            resp.raise_for_status()

    def log_resource_usage(
        self,
        batch_job_id: str,
        title: Optional[str],
        execution_id: str,
        user_id: str,
        started_ms: Optional[float],
        finished_ms: Optional[float],
        state: str,
        status: str,
        cpu_seconds: Optional[float],
        mb_seconds: Optional[float],
        duration_ms: Optional[float],
        sentinel_hub_processing_units: Optional[float],
        additional_credits_cost: Optional[float],
    ) -> float:
        """
        :param user_id: user identifier expected by ETL API: must be "sub" field from (verified) access token

        :return: costs (in credits)
        """
        # TODO: the given `batch_job_id` argument is not always a real batch job id
        #       (e.g. request id for sync processing), while this logger adapter seems to assume that
        log = logging.LoggerAdapter(_log, extra={"job_id": batch_job_id, "user_id": user_id})

        access_token = self._access_token_helper.get_access_token()

        def _log_metrics(metrics: dict) -> float:
            data = {
                "jobId": batch_job_id,
                "jobName": title,
                "executionId": execution_id,
                "userId": user_id,
                "sourceId": self._source_id,
                "orchestrator": ORCHESTRATOR,
                "jobStart": started_ms,
                "jobFinish": finished_ms,
                "idempotencyKey": execution_id,
                # TODO #610 simplify this state/status stuff to single openEO-style job status?
                "state": state,
                "status": status,
                "metrics": metrics,
            }

            log.debug(f"EtlApi.log_resource_usage: POST {data=} at {self._endpoint!r}")

            with self._session.post(
                f"{self._endpoint}/resources",
                headers={"Authorization": f"Bearer {access_token}"},
                json=data,
                timeout=REQUESTS_TIMEOUT_SECONDS,
            ) as resp:
                if not resp.ok:
                    # TODO: doing both `resp.ok` and `resp.raise_for_status` is redundant?
                    log.error(
                        f"EtlApi.log_resource_usage {resp.request.method} {resp.request.url} {data} failed with {resp.status_code}: {resp.text}"
                    )
                    resp.raise_for_status()
                else:
                    # TODO: is cost guaranteed to be in credits?
                    #       Or, vice versa, is it unnecessary to assume anything about the cost unit here?
                    resources = resp.json()
                    total_metrics_credits_cost = sum(resource["cost"] for resource in resources)
                    log.debug(f"EtlApi.log_resource_usage: got {total_metrics_credits_cost=} from {resources=}")
                    return total_metrics_credits_cost

        total_credits_cost = 0

        base_metrics = {}
        if cpu_seconds is not None:
            base_metrics["cpu"] = {"value": cpu_seconds, "unit": "cpu-seconds"}
        if mb_seconds is not None:
            base_metrics["memory"] = {"value": mb_seconds, "unit": "mb-seconds"}
        if duration_ms is not None:
            base_metrics["time"] = {"value": duration_ms, "unit": "milliseconds"}

        # ETL API doesn't allow reporting both PUs and credits in the same request
        if sentinel_hub_processing_units is not None and additional_credits_cost is not None:
            base_metrics["processing"] = {"value": sentinel_hub_processing_units, "unit": "shpu"}
            total_credits_cost += _log_metrics({"processing": {"value": additional_credits_cost, "unit": "credits"}})
        elif sentinel_hub_processing_units is not None:
            base_metrics["processing"] = {"value": sentinel_hub_processing_units, "unit": "shpu"}
        elif additional_credits_cost is not None:
            base_metrics["processing"] = {"value": additional_credits_cost, "unit": "credits"}

        total_credits_cost += _log_metrics(base_metrics)

        return total_credits_cost

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
                                json=data, timeout=REQUESTS_TIMEOUT_SECONDS) as resp:
            # TODO: doing both `resp.ok` and `resp.raise_for_status` is redundant?
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


class EtlApiConfigException(Exception):
    """Base exception for ETL API config and lookup related issues"""
    pass


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
        if root_url != self._root_url:
            _log.error(f"EtlApiConfig: given {root_url=} differs from expected {self._root_url!r}")
            raise EtlApiConfigException("Invalid ETL API root URL.")
        return self._client_credentials


class MultiEtlApiConfig(EtlApiConfig):
    """
    EtlApiConfig implementation that supports multiple ETL API endpoints:
    - one default
    - multiple other ones that can be selected based on a field in job options

    Minimal usage example, with one alternative ETL API

        etl_config = MultiEtlApiConfig(
            default_root_url="https://etl.test",
            other_etl_apis=[
                ("alt", "https://etl-alt.test", "OPENEO_ETL_OIDC_CLIENT_CREDENTIALS_ALT"),
            ]
        )

    By default, "https://etl.test" will be used as ETL API
    (with credentials extracted from the default env var `OPENEO_ETL_OIDC_CLIENT_CREDENTIALS`).
    If user has `"etl_api_id": "alt"` in job options,
    "https://etl-alt.test" will be used as ETL API
    (with credentials from env var `OPENEO_ETL_OIDC_CLIENT_CREDENTIALS_ALT`).

    """

    def __init__(
        self,
        *,
        default_root_url: str,
        default_credentials_env_var: str = "OPENEO_ETL_OIDC_CLIENT_CREDENTIALS",
        other_etl_apis: List[Tuple[str, str, str]],
        job_option_field: str = "etl_api_id",
        credential_extraction_log_level: int = logging.WARNING,
    ):
        """
        :param default_root_url: default ETL API root URL
        :param default_credentials_env_var: default env var to extract credentials from
        :param other_etl_apis: list of tuples (id, url, cred_var) defining other ETL API endpoints:
            - id: identifier for the ETL API endpoint
            - url: root URL of the ETL API endpoint
            - cred_var: env var to extract credentials from
        :param job_option_field: field in job options for user to select ETL API endpoint
        """
        super().__init__()
        self._default_root_url = default_root_url
        self._job_option_field = job_option_field

        # TODO: option to fail fast on missing env vars instead of ignoring?
        self._other_root_urls: Dict[str, str] = {}
        self._credentials: Dict[str, ClientCredentials] = {}
        try:
            self._credentials[default_root_url] = ClientCredentials.from_credentials_string(
                os.environ[default_credentials_env_var]
            )
        except Exception as e:
            _log.log(
                level=credential_extraction_log_level,
                msg=f"MultiEtlApiConfig: failed to get credentials for {default_root_url=} from {default_credentials_env_var=}: {e!r}",
            )
        for etl_id, etl_url, cred_var in other_etl_apis:
            try:
                creds = ClientCredentials.from_credentials_string(os.environ[cred_var])
            except Exception as e:
                _log.log(
                    level=credential_extraction_log_level,
                    msg=f"MultiEtlApiConfig: failed to get credentials for {etl_url=} ({etl_id=}) from {cred_var=}: {e!r}. Skipping.",
                )
            else:
                self._other_root_urls[etl_id] = etl_url
                self._credentials[etl_url] = creds

    def get_root_url(self, *, user: Optional[User] = None, job_options: Optional[dict] = None) -> str:
        # TODO: also support extraction from user?
        etl_api_id = job_options and job_options.get(self._job_option_field)
        if etl_api_id:
            if etl_api_id in self._other_root_urls:
                return self._other_root_urls[etl_api_id]
            else:
                _log.warning(f"MultiEtlApiConfig: Invalid {etl_api_id=}, using default.")
        return self._default_root_url

    def get_client_credentials(self, root_url: str) -> Union[ClientCredentials, None]:
        if root_url not in self._credentials:
            _log.error(f"MultiEtlApiConfig: invalid {root_url=}: not in {self._credentials.keys()!r}")
            raise EtlApiConfigException("Invalid ETL API root URL.")
        return self._credentials[root_url]


def get_etl_api(
    *,
    root_url: Optional[str] = None,
    user: Optional[User] = None,
    job_options: Optional[dict] = None,
    requests_session: Optional[requests.Session] = None,
    # TODO #531 remove this temporary feature flag/toggle for dynamic ETL selection. (True from all call locations now)
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
