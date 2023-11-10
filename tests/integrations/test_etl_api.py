import pytest

from openeo_driver.util.auth import ClientCredentials
from openeogeotrellis.integrations.etl_api import ETL_API_STATE, get_etl_api_credentials_from_env


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
