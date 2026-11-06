from unittest import mock

import pytest

from openeogeotrellis.integrations.job_storage import (
    AzureBlobJobStorage,
    S3JobStorage,
    azure_token_credential,
    get_job_storage,
)
from openeogeotrellis.testing import gps_config_overrides


class TestGetJobStorage:
    def test_s3(self):
        with gps_config_overrides(s3_bucket_name="default-bucket", azure_storage_account_name=None):
            storage = get_job_storage()
            assert isinstance(storage, S3JobStorage) and storage.bucket == "default-bucket"
            assert get_job_storage("other-bucket").bucket == "other-bucket"

    def test_azure(self):
        with gps_config_overrides(s3_bucket_name="default-container", azure_storage_account_name="acc"):
            storage = get_job_storage()
            assert isinstance(storage, AzureBlobJobStorage)
            assert storage.account_url == "https://acc.blob.core.windows.net"
            assert storage.container == "default-container"
            assert get_job_storage("other-container").container == "other-container"


def test_s3_round_trip(mock_s3_bucket, tmp_path):
    storage = S3JobStorage(bucket=mock_s3_bucket.name)
    storage.upload_bytes(b'{"process_graph": {}}', "data/jobs/j-123/job_specification.json")
    assert mock_s3_bucket.Object("data/jobs/j-123/job_specification.json").get()["Body"].read() == (
        b'{"process_graph": {}}'
    )
    local = tmp_path / "job_specification.json"
    storage.download_file("data/jobs/j-123/job_specification.json", local)
    assert local.read_bytes() == b'{"process_graph": {}}'


class TestAzureBlobJobStorage:
    @pytest.fixture
    def container_client(self):
        pytest.importorskip("azure.storage.blob")
        with mock.patch("azure.storage.blob.ContainerClient") as ContainerClient, mock.patch(
            "openeogeotrellis.integrations.job_storage.azure_token_credential", return_value="credential"
        ):
            client = ContainerClient.return_value.__enter__.return_value
            yield ContainerClient, client

    def test_upload_bytes(self, container_client):
        ContainerClient, client = container_client
        AzureBlobJobStorage(account_name="acc", container="cont").upload_bytes(b"{}", "data/job_specification.json")
        ContainerClient.assert_called_once_with(
            account_url="https://acc.blob.core.windows.net", container_name="cont", credential="credential"
        )
        client.upload_blob.assert_called_once_with(name="data/job_specification.json", data=b"{}", overwrite=True)

    def test_download_file(self, container_client, tmp_path):
        _, client = container_client
        client.download_blob.return_value.readinto.side_effect = lambda f: f.write(b"{}")
        local = tmp_path / "sub" / "job_specification.json"
        AzureBlobJobStorage(account_name="acc", container="cont").download_file("data/job_specification.json", local)
        client.download_blob.assert_called_once_with("data/job_specification.json")
        assert local.read_bytes() == b"{}"


def test_azure_token_credential_ignores_client_secret_in_env(monkeypatch):
    pytest.importorskip("azure.identity")
    from azure.identity import EnvironmentCredential

    monkeypatch.setenv("AZURE_TENANT_ID", "tenant")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret")
    credential = azure_token_credential()
    assert not any(isinstance(c, EnvironmentCredential) for c in credential.credentials)
