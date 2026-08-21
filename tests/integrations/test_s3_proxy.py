from datetime import datetime, timezone
from unittest import mock

import pytest

from openeo_driver.errors import OpenEOApiException
from openeogeotrellis.integrations.s3proxy.asset_urls import PresignedS3AssetUrls
from openeogeotrellis.integrations.s3proxy.exceptions import S3ProxyDisabled
from openeogeotrellis.integrations.s3proxy.s3_shared_context import presign_s3_urls_for_internal_usage
from openeogeotrellis.integrations.s3proxy.s3_uris import get_bucket_key_from_uri, split_s3_uri_and_alias
from openeogeotrellis.integrations.s3proxy.sts import STSCredentials
from openeogeotrellis.config.s3_config import AWSConfig
from openeogeotrellis.testing import gps_config_overrides
from openeogeotrellis.workspace import ObjectStorageWorkspace


def test_get_bucket_key_from_uri():
    bucket, key = get_bucket_key_from_uri("s3://my-bucket/my/key")

    assert bucket == "my-bucket"
    assert key == "my/key"


class TestSplitS3UriAndAlias:
    def test_uri_with_alias(self):
        uri, alias = split_s3_uri_and_alias("s3://bucket/key#myalias")
        assert uri == "s3://bucket/key"
        assert alias == "myalias"

    def test_uri_without_alias(self):
        uri, alias = split_s3_uri_and_alias("s3://bucket/key")
        assert uri == "s3://bucket/key"
        assert alias is None

    def test_multiple_hashes_rejected(self):
        with pytest.raises(OpenEOApiException) as exc_info:
            split_s3_uri_and_alias("s3://bucket/key#alias#extra")
        assert exc_info.value.status_code == 400

    def test_empty_alias_rejected(self):
        with pytest.raises(OpenEOApiException) as exc_info:
            split_s3_uri_and_alias("s3://bucket/key#")
        assert exc_info.value.status_code == 400


class TestPresignS3UrlsForInternalUsage:
    """Tests for presign_s3_urls_for_internal_usage using moto fixtures."""

    @pytest.fixture
    def workspace_bucket(self) -> str:
        return "openeo-fake-bucketname"

    @pytest.fixture
    def mock_job_credentials(self, aws_credentials):
        """Mock get_job_aws_credentials_for_proxy to return the moto test credentials."""
        creds = STSCredentials(
            access_key_id="testing",
            secret_access_key="testing",
            session_token="testing",
            expiration=datetime(2099, 1, 1, tzinfo=timezone.utc),
        )
        with mock.patch(
            "openeogeotrellis.integrations.s3proxy.s3_shared_context.get_job_aws_credentials_for_proxy",
            return_value=creds,
        ):
            yield

    @pytest.fixture
    def s3_bucket_with_object(self, moto_server, mock_s3_client, mock_sts_client, workspace_bucket):
        """Create a bucket with a test object in moto."""
        mock_s3_client.create_bucket(
            Bucket=workspace_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
        mock_s3_client.put_object(Bucket=workspace_bucket, Key="deps/mylib.zip", Body=b"fake-content")
        return workspace_bucket

    def test_non_s3_uri_passthrough(self, moto_server, mock_s3_client, mock_sts_client, monkeypatch):
        """Non-S3 URIs should be returned unchanged."""
        monkeypatch.setenv(AWSConfig.S3PROXY_S3_ENDPOINT_URL, moto_server)
        result = presign_s3_urls_for_internal_usage(
            ["https://example.com/helper.py"],
            job_id="j-001",
            user_id="alice",
            expiration=3600,
        )
        assert result == ["https://example.com/helper.py"]

    def test_s3_uri_with_alias_becomes_presigned_url(
        self, s3_bucket_with_object, moto_server, monkeypatch, workspace_bucket, mock_job_credentials
    ):
        """An S3 URI with alias should produce a presigned URL with the alias fragment."""
        monkeypatch.setenv(AWSConfig.S3PROXY_S3_ENDPOINT_URL, moto_server)
        with gps_config_overrides(
            workspaces={"my_ws": ObjectStorageWorkspace(bucket=workspace_bucket, region="eu-central-1")}
        ):
            result = presign_s3_urls_for_internal_usage(
                [f"s3://{workspace_bucket}/deps/mylib.zip#mylib", "https://example.com/helper.py"],
                job_id="j-001",
                user_id="alice",
                expiration=3600,
            )

        assert len(result) == 2
        assert result[1] == "https://example.com/helper.py"
        presigned, fragment = result[0].rsplit("#", 1)
        assert fragment == "mylib"
        # Presigned URL should be a proper HTTP(S) URL
        assert presigned.startswith("http")

    def test_s3_uri_without_alias_becomes_presigned_url(
        self, s3_bucket_with_object, moto_server, monkeypatch, workspace_bucket, mock_job_credentials
    ):
        """An S3 URI without alias should produce a presigned URL without a fragment."""
        monkeypatch.setenv(AWSConfig.S3PROXY_S3_ENDPOINT_URL, moto_server)
        with gps_config_overrides(
            workspaces={"my_ws": ObjectStorageWorkspace(bucket=workspace_bucket, region="eu-central-1")}
        ):
            result = presign_s3_urls_for_internal_usage(
                [f"s3://{workspace_bucket}/deps/mylib.zip"],
                job_id="j-001",
                user_id="alice",
                expiration=3600,
            )

        assert len(result) == 1
        assert "#" not in result[0]
        assert result[0].startswith("http")

    def test_unknown_bucket_raises_s3_proxy_disabled(self, moto_server, mock_s3_client, mock_sts_client, monkeypatch):
        """Bucket with no matching workspace config (unknown region) should raise S3ProxyDisabled."""
        monkeypatch.setenv(AWSConfig.S3PROXY_S3_ENDPOINT_URL, moto_server)
        with gps_config_overrides(workspaces={}):
            with pytest.raises(S3ProxyDisabled, match="unknown region"):
                presign_s3_urls_for_internal_usage(
                    ["s3://some-unknown-bucket/key#alias"],
                    job_id="j-001",
                    user_id="alice",
                    expiration=3600,
                )
