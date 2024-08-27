import logging
from typing import Optional

import dirty_equals
import pytest

from openeo.rest.auth.testing import OidcMock
from openeo_driver.users import User
from openeo_driver.util.auth import ClientCredentials
from openeo_driver.util.caching import TtlCache
from openeogeotrellis.config.config import EtlApiConfig
from openeogeotrellis.integrations.etl_api import (
    ETL_API_STATE,
    EtlApi,
    get_etl_api,
    get_etl_api_credentials_from_env,
    SimpleEtlApiConfig,
    MultiEtlApiConfig,
    EtlApiConfigException,
)
from openeogeotrellis.testing import gps_config_overrides


def test_etl_api_state():
    constant_values = (value for key, value in vars(ETL_API_STATE).items() if not key.startswith("_"))
    for value in constant_values:
        assert isinstance(value, str)


def test_get_etl_api_credentials_from_env_compact(monkeypatch):
    monkeypatch.setenv("OPENEO_ETL_OIDC_CLIENT_CREDENTIALS", "foo:s3cr3t@https://oidc.test/")
    creds = get_etl_api_credentials_from_env()
    assert creds == ClientCredentials("https://oidc.test/", "foo", "s3cr3t")


def test_get_etl_api_credentials_from_env_legacy(monkeypatch):
    monkeypatch.setenv("OPENEO_ETL_API_OIDC_ISSUER", "https://oidc.test/")
    monkeypatch.setenv("OPENEO_ETL_OIDC_CLIENT_ID", "foo")
    monkeypatch.setenv("OPENEO_ETL_OIDC_CLIENT_SECRET", "s3cr3t")
    creds = get_etl_api_credentials_from_env()
    assert creds == ClientCredentials("https://oidc.test/", "foo", "s3cr3t")


def test_get_etl_api_credentials_from_env_default():
    with pytest.raises(RuntimeError):
        _ = get_etl_api_credentials_from_env()


@pytest.fixture
def etl_credentials() -> ClientCredentials:
    """Default client credentials for ETL API access"""
    return ClientCredentials(oidc_issuer="https://oidc.test", client_id="client123", client_secret="s3cr3t")


@pytest.fixture(autouse=True)
def oidc_mock(requests_mock, etl_credentials: ClientCredentials) -> OidcMock:
    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        oidc_issuer=etl_credentials.oidc_issuer,
        expected_grant_type="client_credentials",
        expected_client_id=etl_credentials.client_id,
        expected_fields={"client_secret": etl_credentials.client_secret, "scope": "openid"},
    )
    return oidc_mock


class TestGetEtlApi:
    @pytest.fixture
    def etl_credentials_in_env(self, etl_credentials, monkeypatch):
        """Set env var to get ETL OIDC client credentials from"""
        monkeypatch.setenv(
            "OPENEO_ETL_OIDC_CLIENT_CREDENTIALS",
            f"{etl_credentials.client_id}:{etl_credentials.client_secret}@{etl_credentials.oidc_issuer}",
        )

    def assert_etl_access_token(self, etl_api: EtlApi, requests_mock, oidc_mock):
        """Helper to assert that expected access token is passed in ETL API requests"""

        def get_etl_user_permissions(request, context):
            expected_access_token = oidc_mock.state["access_token"]
            assert request.headers["Authorization"] == f"Bearer {expected_access_token}"
            return {"execution": True}

        mock = requests_mock.get(f"{etl_api.root_url}/user/permissions", json=get_etl_user_permissions)
        etl_api.assert_access_token_valid()
        assert mock.called_once

    @pytest.fixture
    def custom_etl_api_config(self, etl_credentials) -> EtlApiConfig:
        ETL_ALT = "https://etl-alt.test"
        ETL_PLANB = "https://etl.planb.test"

        class CustomEtlConfig(EtlApiConfig):
            def get_root_url(self, *, user: Optional[User] = None, job_options: Optional[dict] = None) -> str:
                if user:
                    return {"a": ETL_ALT, "b": ETL_PLANB}[user.user_id[:1]]
                if job_options and "my_etl" in job_options:
                    id = job_options["my_etl"]
                    return {"alt": ETL_ALT, "planb": ETL_PLANB}[id]
                raise RuntimeError("Don't know which ETL API to use")

            def get_client_credentials(self, root_url: str) -> Optional[ClientCredentials]:
                return {
                    # Note using same credentials for all ETL API instances, to keep testing here simple
                    ETL_ALT: etl_credentials,
                    ETL_PLANB: etl_credentials,
                }[root_url]

        return CustomEtlConfig()

    def test_default_gives_legacy(self, etl_credentials_in_env, requests_mock, oidc_mock):
        etl_api = get_etl_api()
        assert isinstance(etl_api, EtlApi)
        assert etl_api.root_url == "https://etl-api.test"
        self.assert_etl_access_token(etl_api=etl_api, requests_mock=requests_mock, oidc_mock=oidc_mock)

    def test_default_with_etl_api_config_still_gives_legacy(
        self, etl_credentials_in_env, custom_etl_api_config, requests_mock, oidc_mock
    ):
        with gps_config_overrides(etl_api_config=custom_etl_api_config):
            etl_api = get_etl_api()
            assert isinstance(etl_api, EtlApi)
            assert etl_api.root_url == "https://etl-api.test"
            self.assert_etl_access_token(etl_api=etl_api, requests_mock=requests_mock, oidc_mock=oidc_mock)

    def test_allow_dynamic_etl_api_but_no_config(self, etl_credentials_in_env, requests_mock, oidc_mock):
        with gps_config_overrides(etl_api_config=None):
            etl_api = get_etl_api(allow_dynamic_etl_api=True)
            assert isinstance(etl_api, EtlApi)
            assert etl_api.root_url == "https://etl-api.test"
            self.assert_etl_access_token(etl_api=etl_api, requests_mock=requests_mock, oidc_mock=oidc_mock)

    def test_dynamic_etl_api_with_user(self, custom_etl_api_config, requests_mock, oidc_mock):
        """Dynamic ETL selection based on user id"""
        with gps_config_overrides(etl_api_config=custom_etl_api_config):
            etl_api = get_etl_api(user=User("alice"), allow_dynamic_etl_api=True)
            assert isinstance(etl_api, EtlApi)
            assert etl_api.root_url == "https://etl-alt.test"
            self.assert_etl_access_token(etl_api=etl_api, requests_mock=requests_mock, oidc_mock=oidc_mock)

            etl_api = get_etl_api(user=User("bob"), allow_dynamic_etl_api=True)
            assert isinstance(etl_api, EtlApi)
            assert etl_api.root_url == "https://etl.planb.test"
            self.assert_etl_access_token(etl_api=etl_api, requests_mock=requests_mock, oidc_mock=oidc_mock)

    def test_dynamic_etl_api_with_job_options(self, custom_etl_api_config, requests_mock, oidc_mock):
        """Dynamic ETL selection based on job options"""
        with gps_config_overrides(etl_api_config=custom_etl_api_config):
            etl_api = get_etl_api(job_options={"my_etl": "alt"}, allow_dynamic_etl_api=True)
            assert isinstance(etl_api, EtlApi)
            assert etl_api.root_url == "https://etl-alt.test"
            self.assert_etl_access_token(etl_api=etl_api, requests_mock=requests_mock, oidc_mock=oidc_mock)

            etl_api = get_etl_api(job_options={"my_etl": "planb"}, allow_dynamic_etl_api=True)
            assert isinstance(etl_api, EtlApi)
            assert etl_api.root_url == "https://etl.planb.test"
            self.assert_etl_access_token(etl_api=etl_api, requests_mock=requests_mock, oidc_mock=oidc_mock)

    def test_legacy_mode_with_caching(self, etl_credentials_in_env, time_machine, oidc_mock):
        assert oidc_mock.mocks["oidc_discovery"].call_count == 0

        time_machine.move_to("2023-04-05T12:00:00Z")
        etl_api_cache = TtlCache(default_ttl=60)
        etl_api1 = get_etl_api(etl_api_cache=etl_api_cache)
        assert isinstance(etl_api1, EtlApi)
        assert oidc_mock.mocks["oidc_discovery"].call_count == 1

        time_machine.move_to("2023-04-05T12:00:10Z")
        etl_api2 = get_etl_api(etl_api_cache=etl_api_cache)
        assert isinstance(etl_api2, EtlApi)
        assert etl_api2 is etl_api1
        assert oidc_mock.mocks["oidc_discovery"].call_count == 1

        time_machine.move_to("2023-04-05T12:30:00Z")
        etl_api3 = get_etl_api(etl_api_cache=etl_api_cache)
        assert isinstance(etl_api3, EtlApi)
        assert etl_api3 is not etl_api1
        assert etl_api3 is not etl_api2
        assert oidc_mock.mocks["oidc_discovery"].call_count == 2
        etl_api4 = get_etl_api(etl_api_cache=etl_api_cache)
        assert isinstance(etl_api4, EtlApi)
        assert etl_api4 is etl_api3
        assert oidc_mock.mocks["oidc_discovery"].call_count == 2

    def test_dynamic_mode_with_caching(self, custom_etl_api_config, time_machine, oidc_mock):
        with gps_config_overrides(etl_api_config=custom_etl_api_config):
            etl_api_cache = TtlCache(default_ttl=60)
            assert oidc_mock.mocks["oidc_discovery"].call_count == 0

            time_machine.move_to("2023-04-05T12:00:00Z")
            etl_api1 = get_etl_api(user=User("alice"), allow_dynamic_etl_api=True, etl_api_cache=etl_api_cache)
            assert isinstance(etl_api1, EtlApi)
            assert oidc_mock.mocks["oidc_discovery"].call_count == 1

            etl_api2 = get_etl_api(user=User("alphonse"), allow_dynamic_etl_api=True, etl_api_cache=etl_api_cache)
            assert isinstance(etl_api2, EtlApi)
            assert etl_api2 is etl_api1
            assert oidc_mock.mocks["oidc_discovery"].call_count == 1

            time_machine.move_to("2023-04-05T12:00:10Z")
            etl_api3 = get_etl_api(user=User("alice"), allow_dynamic_etl_api=True, etl_api_cache=etl_api_cache)
            assert isinstance(etl_api3, EtlApi)
            assert etl_api3 is etl_api1
            assert oidc_mock.mocks["oidc_discovery"].call_count == 1

            time_machine.move_to("2023-04-05T12:30:00Z")
            etl_api4 = get_etl_api(user=User("alice"), allow_dynamic_etl_api=True, etl_api_cache=etl_api_cache)
            assert isinstance(etl_api4, EtlApi)
            assert etl_api4 is not etl_api1
            assert etl_api4 is not etl_api2
            assert etl_api4 is not etl_api3
            assert oidc_mock.mocks["oidc_discovery"].call_count == 2

            etl_api5 = get_etl_api(user=User("alfred"), allow_dynamic_etl_api=True, etl_api_cache=etl_api_cache)
            assert isinstance(etl_api5, EtlApi)
            assert etl_api5 is etl_api4
            assert oidc_mock.mocks["oidc_discovery"].call_count == 2


class TestEtlApi:
    def test_log_resource_usage(self, requests_mock, etl_credentials):
        mock_endpoint = "https://etl-api.test"
        etl_api = EtlApi(mock_endpoint, credentials=etl_credentials, source_id="test")

        def verify_request(request, context):
            assert request.json() == dict(
                jobId="j-abc123",
                jobName="a test",
                executionId="application_1704961751000_456",
                userId="johndoe",
                sourceId="test",
                orchestrator="openeo",
                jobStart=1704961751000,
                jobFinish=1704961804000,
                idempotencyKey="application_1704961751000_456",
                state="FINISHED",
                status="SUCCEEDED",
                metrics={
                    "cpu": {"value": 53, "unit": "cpu-seconds"},
                    "memory": {"value": 6784, "unit": "mb-seconds"},
                    "time": {"value": 53000, "unit": "milliseconds"},
                    "processing": {"value": 4.0, "unit": "shpu"},
                }
            )

            context.status_code = 201
            return [{
                "jobId": "j-abc123",
                "cost": 9.87
            }]

        requests_mock.post(f"{mock_endpoint}/resources", json=verify_request)

        credits_cost = etl_api.log_resource_usage(
            batch_job_id="j-abc123",
            title="a test",
            execution_id="application_1704961751000_456",
            user_id="johndoe",
            started_ms=1704961751000,
            finished_ms=1704961804000,
            state="FINISHED",
            status="SUCCEEDED",
            cpu_seconds=53,
            mb_seconds=6784,
            duration_ms=53000,
            sentinel_hub_processing_units=4.0,
            additional_credits_cost=None,
        )

        assert credits_cost == 9.87

    def test_log_resource_usage_with_additional_credits_cost(self, requests_mock, etl_credentials):
        mock_endpoint = "https://etl-api.test"
        etl_api = EtlApi(mock_endpoint, credentials=etl_credentials, source_id="test")

        usage_credits_cost = 9.87
        additional_credits_cost = 1.23

        def verify_request(is_additional_credits_cost_request):
            def verify_request(request, context):
                expected_request_json = dict(
                    jobId="j-abc123",
                    jobName="a test",
                    executionId="application_1704961751000_456",
                    userId="johndoe",
                    sourceId="test",
                    orchestrator="openeo",
                    jobStart=1704961751000,
                    jobFinish=1704961804000,
                    idempotencyKey="application_1704961751000_456",
                    state="FINISHED",
                    status="SUCCEEDED",
                )

                if is_additional_credits_cost_request:
                    expected_request_json["metrics"] = {
                        "processing": {"value": additional_credits_cost, "unit": "credits"},
                    }
                else:
                    expected_request_json["metrics"] = {
                        "cpu": {"value": 53, "unit": "cpu-seconds"},
                        "memory": {"value": 6784, "unit": "mb-seconds"},
                        "time": {"value": 53000, "unit": "milliseconds"},
                        "processing": {"value": 4.0, "unit": "shpu"},
                    }

                assert request.json() == expected_request_json

                context.status_code = 201
                return [
                    {
                        "jobId": "j-abc123",
                        "cost": additional_credits_cost if is_additional_credits_cost_request else usage_credits_cost,
                    }
                ]

            return verify_request

        mock = requests_mock.post(
            f"{mock_endpoint}/resources",
            [
                {"json": verify_request(is_additional_credits_cost_request=False)},
                {"json": verify_request(is_additional_credits_cost_request=True)},
            ],
        )

        credits_cost = etl_api.log_resource_usage(
            batch_job_id="j-abc123",
            title="a test",
            execution_id="application_1704961751000_456",
            user_id="johndoe",
            started_ms=1704961751000,
            finished_ms=1704961804000,
            state="FINISHED",
            status="SUCCEEDED",
            cpu_seconds=53,
            mb_seconds=6784,
            duration_ms=53000,
            sentinel_hub_processing_units=4.0,
            additional_credits_cost=additional_credits_cost,
        )

        assert mock.call_count == 2
        assert credits_cost == usage_credits_cost + additional_credits_cost

    def test_log_added_value(self, requests_mock, etl_credentials):
        mock_endpoint = "https://etl-api.test"
        etl_api = EtlApi(mock_endpoint, credentials=etl_credentials, source_id="test")

        def verify_request(request, context):
            assert request.json() == dict(
                jobId="j-abc123",
                jobName="a test",
                executionId="application_1704961751000_456",
                userId="johndoe",
                sourceId="test",
                orchestrator="openeo",
                jobStart=1704961751000,
                jobFinish=1704961804000,
                idempotencyKey="application_1704961751000_456",
                service="load_stac",
                area={"value": 40.0, "unit": "square_meter"},
            )

            context.status_code = 201
            return [{
                "jobId": "j-abc123",
                "cost": 8.76
            }]

        requests_mock.post(f"{mock_endpoint}/addedvalue", json=verify_request)

        credits_cost = etl_api.log_added_value(batch_job_id="j-abc123", title="a test",
                                               execution_id="application_1704961751000_456", user_id="johndoe",
                                               started_ms=1704961751000, finished_ms=1704961804000,
                                               process_id="load_stac", square_meters=40.0)

        assert credits_cost == 8.76


class TestSimpleEtlApiConfig:
    def test_simple_config(self):
        client_credentials = ClientCredentials(
            oidc_issuer="https://oidc.test", client_id="client123", client_secret="s3cr3t"
        )
        config = SimpleEtlApiConfig(
            root_url="https://etl.test",
            client_credentials=client_credentials,
        )
        assert config.get_root_url() == "https://etl.test"
        assert config.get_client_credentials("https://etl.test") is client_credentials
        with pytest.raises(EtlApiConfigException, match="Invalid ETL API root URL."):
            _ = config.get_client_credentials("https://etl-alt.test")


class TestMultiEtlApiConfig:
    def test_basic(self, monkeypatch, caplog):
        caplog.set_level(logging.WARNING)
        monkeypatch.setenv("OPENEO_ETL_OIDC_CLIENT_CREDENTIALS", "john:pw6@https://oidc.test/")
        monkeypatch.setenv("OPENEO_ETL_OIDC_CLIENT_CREDENTIALS_ALT", "alt:6lt@https://oidc-alt.test/")
        monkeypatch.setenv("OPENEO_ETL_OIDC_CLIENT_CREDENTIALS_BETA", "bob:808@https://boidc.test/")

        config = MultiEtlApiConfig(
            default_root_url="https://etl.test/",
            other_etl_apis=[
                ("alt", "https://etl-alt.test/", "OPENEO_ETL_OIDC_CLIENT_CREDENTIALS_ALT"),
                ("beta", "https://etl-beta.test/", "OPENEO_ETL_OIDC_CLIENT_CREDENTIALS_BETA"),
            ],
        )

        assert config.get_root_url() == "https://etl.test/"
        assert config.get_root_url(job_options={}) == "https://etl.test/"
        assert config.get_root_url(job_options={"etl_api_id": "alt"}) == "https://etl-alt.test/"
        assert config.get_root_url(job_options={"etl_api_id": "beta"}) == "https://etl-beta.test/"
        assert caplog.messages == []

        assert config.get_root_url(job_options={"etl_api_id": "foobar"}) == "https://etl.test/"
        assert [f"{r.levelname}: {r.message}" for r in caplog.records] == dirty_equals.Contains(
            dirty_equals.IsStr(regex="WARNING:.*Invalid etl_api_id='foobar', using default.*")
        )

        assert config.get_client_credentials("https://etl.test/") == ClientCredentials(
            oidc_issuer="https://oidc.test/", client_id="john", client_secret="pw6"
        )
        assert config.get_client_credentials("https://etl-alt.test/") == ClientCredentials(
            oidc_issuer="https://oidc-alt.test/", client_id="alt", client_secret="6lt"
        )
        assert config.get_client_credentials("https://etl-beta.test/") == ClientCredentials(
            oidc_issuer="https://boidc.test/", client_id="bob", client_secret="808"
        )
        with pytest.raises(EtlApiConfigException, match="Invalid ETL API root URL."):
            _ = config.get_client_credentials("https://meh.test/")

    def test_missing_env_vars(self, monkeypatch, caplog):
        monkeypatch.setenv("OPENEO_ETL_OIDC_CLIENT_CREDENTIALS", "john:pw6@https://oidc.test/")
        # Missing OPENEO_ETL_OIDC_CLIENT_CREDENTIALS_ALT
        monkeypatch.setenv("OPENEO_ETL_OIDC_CLIENT_CREDENTIALS_BETA", "bob:808@https://boidc.test/")

        config = MultiEtlApiConfig(
            default_root_url="https://etl.test/",
            other_etl_apis=[
                ("alt", "https://etl-alt.test/", "OPENEO_ETL_OIDC_CLIENT_CREDENTIALS_ALT"),
                ("beta", "https://etl-beta.test/", "OPENEO_ETL_OIDC_CLIENT_CREDENTIALS_BETA"),
            ],
        )
        assert [f"{r.levelname}: {r.message}" for r in caplog.records] == dirty_equals.Contains(
            dirty_equals.IsStr(regex="WARNING:.*failed to get credentials for.*etl-alt.test.*Skipping.*")
        )

        # Test get_root_url behavior
        caplog.clear()

        assert config.get_root_url() == "https://etl.test/"
        assert config.get_root_url(job_options={}) == "https://etl.test/"
        # Missing env var, so falls back to default
        assert config.get_root_url(job_options={"etl_api_id": "alt"}) == "https://etl.test/"
        # Working env var: picks up the alternative root URL
        assert config.get_root_url(job_options={"etl_api_id": "beta"}) == "https://etl-beta.test/"
        assert [f"{r.levelname}: {r.message}" for r in caplog.records] == dirty_equals.Contains(
            dirty_equals.IsStr(regex="WARNING:.*Invalid etl_api_id='alt', using default.*")
        )

        # Test get_client_credentials behavior
        caplog.clear()

        assert config.get_client_credentials("https://etl.test/") == ClientCredentials(
            oidc_issuer="https://oidc.test/", client_id="john", client_secret="pw6"
        )
        # Missing env var -> failure to get credentials
        with pytest.raises(EtlApiConfigException, match="Invalid ETL API root URL."):
            _ = config.get_client_credentials("https://etl-alt.test/")
        # Working env var -> get alternative credentials
        assert config.get_client_credentials("https://etl-beta.test/") == ClientCredentials(
            oidc_issuer="https://boidc.test/", client_id="bob", client_secret="808"
        )

        assert [f"{r.levelname}: {r.message}" for r in caplog.records] == dirty_equals.Contains(
            dirty_equals.IsStr(
                regex="ERROR:.*invalid root_url.*https://etl-alt.test/.*not in.*https://etl.test/.*https://etl-beta.test/.*"
            )
        )
