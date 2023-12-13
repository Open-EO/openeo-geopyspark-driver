from typing import Optional

import pytest

from openeo.rest.auth.testing import OidcMock
from openeo_driver.users import User
from openeo_driver.util.auth import ClientCredentials
from openeogeotrellis.integrations.etl_api import (
    ETL_API_STATE,
    get_etl_api_credentials_from_env,
    get_etl_api,
    EtlApi,
    DynamicEtlApiConfig,
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


class TestGetEtlApi:
    @pytest.fixture
    def etl_credentials(self) -> ClientCredentials:
        """Default client credentials for ETL API access"""
        return ClientCredentials(oidc_issuer="https://oidc.test", client_id="client123", client_secret="s3cr3t")

    @pytest.fixture(autouse=True)
    def oidc_mock(self, requests_mock, etl_credentials: ClientCredentials) -> OidcMock:
        oidc_mock = OidcMock(
            requests_mock=requests_mock,
            oidc_issuer=etl_credentials.oidc_issuer,
            expected_grant_type="client_credentials",
            expected_client_id=etl_credentials.client_id,
            expected_fields={"client_secret": etl_credentials.client_secret, "scope": "openid"},
        )
        return oidc_mock

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
    def custom_etl_api_config(self, etl_credentials):
        ETL_ALT = "https://etl-alt.test"
        ETL_PLANB = "https://etl.planb.test"

        class CustomEtlConfig(DynamicEtlApiConfig):
            def get_root_url(self, *, user: Optional[User] = None, job_options: Optional[dict] = None) -> str:
                if user:
                    return {"a": ETL_ALT, "b": ETL_PLANB}[user.user_id[:1]]
                if job_options and "my_etl" in job_options:
                    id = job_options["my_etl"]
                    return {"alt": ETL_ALT, "planb": ETL_PLANB}[id]
                raise RuntimeError("Don't know which ETL API to use")

        return CustomEtlConfig(
            urls_and_credentials={
                # Note using same credentials for all ETL API instances, to keep testing here simple
                ETL_ALT: etl_credentials,
                ETL_PLANB: etl_credentials,
            }
        )

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
