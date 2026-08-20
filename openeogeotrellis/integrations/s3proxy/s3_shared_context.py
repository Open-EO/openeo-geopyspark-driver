from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, List, Optional, Tuple

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

from botocore.config import Config
import boto3
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.config.s3_config import AWSConfig
from openeogeotrellis.integrations.s3proxy.exceptions import S3ProxyDisabled, S3ProxyUnsupportedBucketType
from openeogeotrellis.integrations.s3proxy.sts import get_job_aws_credentials_for_proxy
from openeo_driver.errors import OpenEOApiException
from openeo_driver.integrations.s3.bucket_details import BucketDetails, is_workspace_bucket, _REGION_UNKNOWN
from openeo_driver.integrations.s3.presigned_url import create_presigned_url
from openeo_driver.util.caching import BoundedTtlCache

logger = logging.getLogger(__name__)


def _get_role_arn(bucket_details: BucketDetails) -> str:
    if is_workspace_bucket(bucket_details):
        assert bucket_details.type_id is not None
        return f"arn:openeows:iam:::role/{bucket_details.type_id}"
    raise S3ProxyUnsupportedBucketType


_client_cache = BoundedTtlCache(ttl=10 * 60, max_size=50)


def get_proxy_s3_client_for_job(bucket: str, job_id: str, user_id: str, internal: bool = True) -> S3Client:
    """
    Get an S3 proxy client that is scoped with job context. Clients are specific per bucket since the endpoint depends
    on the bucket. job_id and user_id are the context for which the client gets created. An internal client is a client
    that is usable only inside the OpenEO backend execution context.
    """
    return _client_cache.get_or_call(
        (bucket, job_id, user_id, str(internal)),
        lambda: _get_proxy_s3_client_for_job(bucket, job_id, user_id, internal),
    )


def _get_proxy_s3_client_for_job(bucket: str, job_id: str, user_id: str, internal: bool) -> S3Client:
    """
    A proxy S3 client gets a client which is configured for bucket access scoped to an execution context.

    It takes a bucket because a bucket is required to identify the region where the data resides.
    """
    bucket_details = BucketDetails.from_name(bucket)
    region_name = bucket_details.region
    if region_name == _REGION_UNKNOWN:
        raise S3ProxyDisabled(
            f"Bucket {bucket!r} has unknown region."
        )
    try:
        if internal:
            endpoint_url = os.environ[AWSConfig.S3PROXY_S3_ENDPOINT_URL]
        else:
            endpoint_url = get_backend_config().s3_region_proxy_endpoints[region_name]
        creds = get_job_aws_credentials_for_proxy(job_id, user_id, _get_role_arn(bucket_details))
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region_name,
            config=Config(signature_version="s3v4"),
            **creds.as_client_kwargs(),
        )
    except KeyError:
        raise S3ProxyDisabled(f"Region {region_name} is not supported by proxy.")


def _split_s3_uri_and_alias(s3_uri: str) -> Tuple[str, Optional[str]]:
    """Split an S3 URI into the URI without alias and an optional alias fragment.

    Returns (s3_uri_without_alias, alias) where alias may be None if no '#' is present.
    Raises OpenEOApiException if the URI has more than one '#' or an empty alias.
    """
    if s3_uri.count("#") > 1:
        raise OpenEOApiException(
            status_code=400,
            message=f"Invalid S3 URI {s3_uri!r}: expected at most one '#'.",
        )
    if "#" in s3_uri:
        s3_uri_without_alias, alias = s3_uri.split("#", 1)
        if not alias:
            raise OpenEOApiException(
                status_code=400,
                message=f"Invalid S3 URI {s3_uri!r}: expected non-empty alias after '#'.",
            )
        return s3_uri_without_alias, alias
    return s3_uri, None


def presign_s3_urls_for_internal_usage(
    s3_uris: List[str], *, job_id: str, user_id: str, expiration: int
) -> List[str]:
    """Resolve a list of URIs, replacing any S3 URIs with presigned URLs for internal access.

    "Internal" means the presigned URLs point at the cluster-internal proxy endpoint and are
    only resolvable from within the cluster (e.g. by Spark executors), not from the outside.

    Non-S3 URIs are passed through unchanged. S3 URIs may optionally carry a Spark alias
    fragment (``s3://bucket/key#alias``); if present it is re-appended after the presigned URL.
    """
    from urllib.parse import urlparse

    resolved = []
    for uri in s3_uris:
        if not uri.startswith("s3://"):
            resolved.append(uri)
            continue

        # Avoid circular import: asset_urls imports from this module
        from openeogeotrellis.integrations.s3proxy.asset_urls import PresignedS3AssetUrls

        s3_uri_without_alias, alias = _split_s3_uri_and_alias(uri)
        bucket, key = PresignedS3AssetUrls.get_bucket_key_from_uri(s3_uri_without_alias)
        s3_client = get_proxy_s3_client_for_job(bucket, job_id, user_id, internal=True)
        presigned_url = create_presigned_url(
            s3_client,
            bucket_name=bucket,
            object_name=key,
            expiration=expiration,
            parameters={"X-Proxy-Head-As-Get": "true"},
        )
        if presigned_url is None:
            raise OpenEOApiException(
                status_code=400,
                message=f"Could not create a presigned url for {s3_uri_without_alias} job_id={job_id} user={user_id}",
            )

        parsed = urlparse(presigned_url)
        if not (parsed.scheme and parsed.netloc and parsed.path):
            raise OpenEOApiException(
                status_code=400,
                message=f"Generated invalid presigned url {presigned_url!r} for {s3_uri_without_alias} job_id={job_id} user={user_id}",
            )

        resolved_uri = f"{presigned_url}#{alias}" if alias is not None else presigned_url
        logger.info(
            f"Resolved S3 URI {s3_uri_without_alias!r} to presigned URL: {resolved_uri!r}"
        )
        resolved.append(resolved_uri)
    return resolved
