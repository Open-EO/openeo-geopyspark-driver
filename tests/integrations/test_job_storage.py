from unittest import mock

import pytest

from openeogeotrellis.integrations.job_storage import (
    AzureBlobJobStorage,
    LocalFileJobStorage,
    S3JobStorage,
    azure_token_credential,
    get_job_storage,
    get_results_metadata_key,
    parse_job_storage_uri,
)
from openeogeotrellis.testing import gps_config_overrides


class TestGetJobStorage:
    @pytest.fixture
    def use_object_storage(self):
        from openeogeotrellis.configparams import ConfigParams

        with mock.patch.object(
            ConfigParams, "use_object_storage", new_callable=mock.PropertyMock, return_value=True
        ) as m:
            yield m

    def test_unspecified_falls_back_to_s3_bucket_name(self, use_object_storage):
        with gps_config_overrides(
            s3_bucket_name="default-bucket",
            job_aws_s3_bucket_name="ignored",
            job_azure_storage_account_name="ignored",
        ):
            storage = get_job_storage()
            assert isinstance(storage, S3JobStorage) and storage.bucket == "default-bucket"
            assert get_job_storage(job_dict={"job_id": "j-123"}).bucket == "default-bucket"

    def test_unspecified_falls_back_to_local_file_without_object_storage(self, use_object_storage):
        use_object_storage.return_value = False
        with gps_config_overrides(s3_bucket_name="default-bucket"):
            storage = get_job_storage(job_dict={"job_id": "j-123"})
            assert isinstance(storage, LocalFileJobStorage) and storage.root.as_posix() == "/"

    def test_aws_s3(self):
        with gps_config_overrides(
            s3_bucket_name="ignored",
            job_storage_type="aws_s3",
            job_aws_s3_bucket_name="job-bucket",
            job_azure_storage_account_name="ignored",
        ):
            storage = get_job_storage()
            assert isinstance(storage, S3JobStorage) and storage.bucket == "job-bucket"

    def test_azure_blob(self):
        with gps_config_overrides(
            s3_bucket_name="ignored",
            job_storage_type="azure_blob",
            job_azure_storage_account_name="acc",
            job_azure_storage_container_name="job-container",
            job_aws_s3_bucket_name="ignored",
        ):
            storage = get_job_storage()
            assert isinstance(storage, AzureBlobJobStorage)
            assert storage.account_url == "https://acc.blob.core.windows.net"
            assert storage.container == "job-container"

    @pytest.mark.parametrize(
        ["overrides", "missing"],
        [
            ({"job_storage_type": "aws_s3"}, "job_aws_s3_bucket_name"),
            (
                {"job_storage_type": "azure_blob", "job_azure_storage_container_name": "c"},
                "job_azure_storage_account_name",
            ),
            (
                {"job_storage_type": "azure_blob", "job_azure_storage_account_name": "acc"},
                "job_azure_storage_container_name",
            ),
        ],
    )
    def test_missing_config(self, overrides, missing):
        with gps_config_overrides(**overrides):
            with pytest.raises(ValueError, match=f"Config '{missing}' is required"):
                get_job_storage()

    @pytest.mark.parametrize(
        ["uri", "expected_type", "expected_repr"],
        [
            ("s3://uri-bucket/data/jobs/j-123/job_metadata.json", S3JobStorage, "S3JobStorage(bucket='uri-bucket')"),
            ("file:///data/jobs/j-123/job_metadata.json", LocalFileJobStorage, "LocalFileJobStorage(root='/')"),
            (
                "https://acc2.blob.core.windows.net/uri-container/data/jobs/j-123/job_metadata.json",
                AzureBlobJobStorage,
                "AzureBlobJobStorage(account_url='https://acc2.blob.core.windows.net', container='uri-container')",
            ),
        ],
    )
    def test_from_job_dict_results_metadata_uri(self, uri, expected_type, expected_repr):
        with gps_config_overrides(
            job_storage_type="azure_blob",
            job_azure_storage_account_name="acc",
            job_azure_storage_container_name="config-container",
        ):
            storage = get_job_storage(job_dict={"job_id": "j-123", "results_metadata_uri": uri})
            assert isinstance(storage, expected_type)
            assert repr(storage) == expected_repr

    @pytest.mark.parametrize(
        "job_dict",
        [
            {"job_id": "j-123"},
            {"job_id": "j-123", "results_metadata_uri": None},
            {"job_id": "j-123", "results_metadata_uri": "ftp://host/data/jobs/j-123/job_metadata.json"},
        ],
    )
    def test_from_job_dict_fallback_to_config(self, job_dict):
        with gps_config_overrides(job_storage_type="aws_s3", job_aws_s3_bucket_name="job-bucket"):
            storage = get_job_storage(job_dict=job_dict)
            assert isinstance(storage, S3JobStorage) and storage.bucket == "job-bucket"

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="job_storage_type"):
            with gps_config_overrides(job_storage_type="ftp"):
                pass


@pytest.mark.parametrize(
    ["job_dict", "expected"],
    [
        (None, "default/job_metadata.json"),
        ({}, "default/job_metadata.json"),
        ({"results_metadata_uri": None}, "default/job_metadata.json"),
        ({"results_metadata_uri": "s3://b/uri/job_metadata.json"}, "uri/job_metadata.json"),
        ({"results_metadata_uri": "file:///uri/job_metadata.json"}, "uri/job_metadata.json"),
        ({"results_metadata_uri": "ftp://host/uri/job_metadata.json"}, "default/job_metadata.json"),
    ],
)
def test_get_results_metadata_key(job_dict, expected):
    assert get_results_metadata_key(job_dict, default="/default/job_metadata.json") == expected


@pytest.mark.parametrize(
    "storage",
    [
        S3JobStorage(bucket="b"),
        LocalFileJobStorage(),
        AzureBlobJobStorage(account_name="acc", container="c"),
    ],
)
def test_uri_round_trip(storage):
    uri = storage.uri("/data/jobs/j-123/job_metadata.json")
    parsed, key = parse_job_storage_uri(uri)
    assert repr(parsed) == repr(storage)
    assert key == "data/jobs/j-123/job_metadata.json"


def test_uri_formats():
    assert S3JobStorage(bucket="b").uri("data/x.json") == "s3://b/data/x.json"
    assert LocalFileJobStorage().uri("data/x.json") == "file:///data/x.json"
    assert LocalFileJobStorage().uri("/data/x.json") == "file:///data/x.json"
    assert (
        AzureBlobJobStorage(account_name="acc", container="c").uri("data/x.json")
        == "https://acc.blob.core.windows.net/c/data/x.json"
    )


@pytest.mark.parametrize(
    "uri",
    [
        "file://remote-host/data/x.json",
        "file:relative/x.json",
        "s3:///data/x.json",
        "https://example.com/c/data/x.json",
        "https://acc.blob.core.windows.net/c",
        "https://.blob.core.windows.net/c/data/x.json",
    ],
)
def test_parse_job_storage_uri_unsupported(uri):
    with pytest.raises(ValueError, match="Unsupported job storage URI"):
        parse_job_storage_uri(uri)


def test_s3_round_trip(mock_s3_bucket, tmp_path):
    storage = S3JobStorage(bucket=mock_s3_bucket.name)
    storage.upload_bytes(b'{"process_graph": {}}', "data/jobs/j-123/job_specification.json")
    assert mock_s3_bucket.Object("data/jobs/j-123/job_specification.json").get()["Body"].read() == (
        b'{"process_graph": {}}'
    )
    local = tmp_path / "job_specification.json"
    storage.download_file("data/jobs/j-123/job_specification.json", local)
    assert local.read_bytes() == b'{"process_graph": {}}'
    assert storage.download_bytes("data/jobs/j-123/job_specification.json") == b'{"process_graph": {}}'


def test_local_file_round_trip(tmp_path):
    storage = LocalFileJobStorage(root=tmp_path)
    storage.upload_bytes(b'{"process_graph": {}}', "data/jobs/j-123/job_specification.json")
    assert (tmp_path / "data/jobs/j-123/job_specification.json").read_bytes() == b'{"process_graph": {}}'
    local = tmp_path / "local" / "job_specification.json"
    storage.download_file("data/jobs/j-123/job_specification.json", local)
    assert local.read_bytes() == b'{"process_graph": {}}'
    # Downloading onto itself is a no-op
    storage.download_file("data/jobs/j-123/job_specification.json", tmp_path / "data/jobs/j-123/job_specification.json")
    assert storage.download_bytes("data/jobs/j-123/job_specification.json") == b'{"process_graph": {}}'


def test_local_file_from_uri(tmp_path):
    path = tmp_path / "jobs" / "j 123" / "job_metadata.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"{}")
    storage, key = parse_job_storage_uri(path.as_uri())
    assert storage.download_bytes(key) == b"{}"
    assert storage.uri(key) == path.as_uri()


class TestLocalFileJobStorageRetry:
    def test_default_no_retry(self, tmp_path):
        with mock.patch("time.sleep") as sleep:
            with pytest.raises(FileNotFoundError):
                LocalFileJobStorage(root=tmp_path).download_bytes("missing.json")
        sleep.assert_not_called()

    def test_retry_from_config(self, tmp_path):
        path = tmp_path / "late.json"
        calls = []

        def sleep(_):
            calls.append(_)
            path.write_bytes(b"{}")

        with gps_config_overrides(read_results_metadata_file_retry_settings=dict(tries=3, delay=7)), mock.patch(
            "time.sleep", side_effect=sleep
        ):
            assert LocalFileJobStorage(root=tmp_path).download_bytes("late.json") == b"{}"
        assert calls == [7]

    def test_retry_exhausted(self, tmp_path):
        storage = LocalFileJobStorage(root=tmp_path, retry_settings=dict(tries=2, delay=1))
        with mock.patch("time.sleep") as sleep:
            with pytest.raises(FileNotFoundError):
                storage.download_bytes("missing.json")
        assert sleep.call_count == 1


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

    def test_download_bytes(self, container_client):
        _, client = container_client
        client.download_blob.return_value.readall.return_value = b"{}"
        data = AzureBlobJobStorage(account_name="acc", container="cont").download_bytes("data/job_metadata.json")
        client.download_blob.assert_called_once_with("data/job_metadata.json")
        assert data == b"{}"


def test_azure_token_credential_ignores_client_secret_in_env(monkeypatch):
    pytest.importorskip("azure.identity")
    from azure.identity import EnvironmentCredential

    monkeypatch.setenv("AZURE_TENANT_ID", "tenant")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret")
    credential = azure_token_credential()
    assert not any(isinstance(c, EnvironmentCredential) for c in credential.credentials)
