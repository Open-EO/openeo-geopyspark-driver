from __future__ import annotations
import os
import json
import logging
import socket
from pathlib import Path
import boto3
from botocore.config import Config
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session
from botocore.exceptions import BotoCoreError, ClientError
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

logger = logging.getLogger(__name__)


def _get_token_file() -> Path:
    """
    When executing in a user context the runtime environment communicates the user identity via an OIDC token.
    This method returns that token.
    """
    return Path(os.environ.get("OPENEO_WEB_IDENTITY_TOKEN_FILE", "/opt/job_config/token"))


def _get_bucket_config_file() -> Path:
    """
    When the proxy is to be used a bucket file is present which communicates per bucket details for using the proxy.
    """
    return Path(os.environ.get("OPENEO_BUCKET_CONFIG_FILE", "/opt/job_config/bucket_config.json"))


def should_proxy_be_used() -> bool:
    return _get_bucket_config_file().is_file()


def build_proxy_s3_client(bucket_name: str) -> Optional[S3Client]:
    """
    Returns a boto3 S3 client configured to go through the S3 proxy,
    or None on any failure.
    """
    # --- token file ---
    token_file = _get_token_file()
    if not token_file.is_file() or not os.access(token_file, os.R_OK):
        logger.warning(
            "Cannot build proxy S3 client for bucket %s: " "web identity token file is not readable: %s",
            bucket_name,
            token_file,
        )
        return None
    # --- bucket config file ---
    bucket_config_file = _get_bucket_config_file()
    if not bucket_config_file.is_file() or not os.access(bucket_config_file, os.R_OK):
        logger.warning(
            "Cannot build proxy S3 client for bucket %s: " "bucket config file is not readable: %s",
            bucket_name,
            bucket_config_file,
        )
        return None
    # --- read bucket config ---
    bucket_config = _read_proxy_bucket_config(bucket_name, bucket_config_file)
    if bucket_config is None:
        return None
    # --- required endpoints ---
    sts_endpoint = _get_required_uri("S3PROXY_STS_ENDPOINT_URL")
    if sts_endpoint is None:
        return None
    s3_endpoint = _get_required_uri("S3PROXY_S3_ENDPOINT_URL")
    if s3_endpoint is None:
        return None

    sts_client = boto3.client(
        "sts",
        region_name=bucket_config["region"],
        endpoint_url=sts_endpoint,
        aws_access_key_id="anonymous",
        aws_secret_access_key="anonymous",
        config=Config(
            retries={
                "max_attempts": 5,  # total attempts (1 initial + 4 retries)
                "mode": "adaptive",  # "legacy", "standard", or "adaptive"
            },
            connect_timeout=2,
            read_timeout=5,
        ),
    )

    def refresh():
        """Called automatically by botocore when credentials are near expiry."""
        assumed = sts_client.assume_role_with_web_identity(
            RoleArn=bucket_config["role_arn"],
            RoleSessionName=_proxy_role_session_name(bucket_name),
            WebIdentityToken=token_file.read_text().strip(),
        )
        creds = assumed["Credentials"]
        return {
            "access_key": creds["AccessKeyId"],
            "secret_key": creds["SecretAccessKey"],
            "token": creds["SessionToken"],
            "expiry_time": creds["Expiration"].isoformat(),  # must be an ISO-8601 string
        }

    try:
        # Do the first credential fetch eagerly so we fail fast if something is wrong.
        initial = refresh()
    except Exception as e:
        logger.warning("Cannot build proxy S3 client for bucket %s: %s", bucket_name, e)
        return None

    refreshable_creds = RefreshableCredentials.create_from_metadata(
        metadata=initial,
        refresh_using=refresh,  # botocore calls this automatically before expiry
        method="sts-assume-role-with-web-identity",
    )

    try:
        # There is no public botocore API for injecting RefreshableCredentials into
        # a session; setting _credentials is the established workaround.
        botocore_session = get_session()
        botocore_session._credentials = refreshable_creds
        return boto3.Session(botocore_session=botocore_session).client(
            "s3",
            region_name=bucket_config["region"],
            endpoint_url=s3_endpoint,
        )
    except (BotoCoreError, ClientError, Exception) as e:
        logger.warning("Cannot build proxy S3 client for bucket %s: %s", bucket_name, e)
        return None


def _read_proxy_bucket_config(bucket_name: str, bucket_config_file: Path):
    try:
        config = json.loads(bucket_config_file.read_text())
        if not isinstance(config, dict):
            logger.warning(
                "Cannot build proxy S3 client for bucket %s: " "bucket config file does not contain a JSON object: %s",
                bucket_name,
                bucket_config_file,
            )
            return None
        bucket = config.get(bucket_name)
        if not isinstance(bucket, dict):
            logger.warning(
                "Cannot build proxy S3 client for bucket %s: "
                "bucket config file has no object entry for this bucket: %s",
                bucket_name,
                bucket_config_file,
            )
            return None
        region = bucket.get("region", "").strip() or None
        if region is None:
            logger.warning(
                "Cannot build proxy S3 client for bucket %s: " "bucket config entry has no region: %s",
                bucket_name,
                bucket_config_file,
            )
            return None
        role_arn = (bucket.get("role_arn") or "").strip() or None
        if role_arn is None:
            logger.warning(
                "Cannot build proxy S3 client for bucket %s: " "bucket config entry has no role_arn: %s",
                bucket_name,
                bucket_config_file,
            )
            return None
        return {"region": region, "role_arn": role_arn}
    except Exception as e:
        logger.warning(
            "Cannot read S3proxy config for bucket %s: " "failed to read bucket config file %s: %s",
            bucket_name,
            bucket_config_file,
            e,
        )
        return None


def _get_required_uri(env_name: str):
    value = os.environ.get(env_name, "").strip() or None
    if value is None:
        logger.warning("Required environment variable for S3proxy %s is not set.", env_name)
        return None
    return value


def _proxy_role_session_name(bucket_name: str) -> str:
    try:
        pod_name = socket.gethostname()
    except Exception as e:
        logger.warning("Could not get hostname: %s", e)
        pod_name = "unknown"
    return f"py-{pod_name}-{bucket_name}"[:64]
