from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, List
from urllib.parse import urlparse

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

from botocore.config import Config
import boto3
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.config.s3_config import AWSConfig
from openeogeotrellis.integrations.s3proxy.exceptions import S3ProxyDisabled, S3ProxyUnsupportedBucketType
from openeogeotrellis.integrations.s3proxy.s3_uris import get_bucket_key_from_uri, split_s3_uri_and_alias
from openeogeotrellis.integrations.s3proxy.sts import get_job_aws_credentials_for_proxy
from openeo_driver.errors import OpenEOApiException
from openeo_driver.integrations.s3.bucket_details import BucketDetails, is_workspace_bucket
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
    """Return a cached S3 proxy client scoped to a job execution context.

    The client endpoint is bucket-specific (the endpoint depends on the bucket's region).
    Set ``internal=True`` (the default) for a client that resolves to the cluster-internal
    proxy endpoint (usable only from within the cluster, e.g. by Spark executors).
    Set ``internal=False`` for a client that resolves to the externally accessible proxy endpoint.
    """
    return _client_cache.get_or_call(
        (bucket, job_id, user_id, str(internal)),
        lambda: _get_proxy_s3_client_for_job(bucket, job_id, user_id, internal),
    )


def _get_proxy_s3_client_for_job(bucket: str, job_id: str, user_id: str, internal: bool) -> S3Client:
    """Construct a boto3 S3 client pointed at the S3 proxy for the given bucket and job context.

    Raises S3ProxyUnsupportedBucketType if the bucket is not a known workspace bucket (no region available).
    Raises S3ProxyDisabled if no proxy endpoint is configured for the bucket's region.
    """
    bucket_details = BucketDetails.from_name(bucket)
    # Currently only workspace buckets are supported: they are the only bucket type for which
    # we can derive both a region (needed to pick the proxy endpoint) and a role ARN.
    if not is_workspace_bucket(bucket_details):
        raise S3ProxyUnsupportedBucketType(
            f"Bucket {bucket!r} is not a known workspace bucket; cannot determine region for S3 proxy."
        )
    region_name = bucket_details.region
    if internal:
        endpoint_url = os.environ.get(AWSConfig.S3PROXY_S3_ENDPOINT_URL)
        if not endpoint_url:
            raise S3ProxyDisabled(f"No internal S3 proxy endpoint configured ({AWSConfig.S3PROXY_S3_ENDPOINT_URL}).")
    else:
        endpoint_url = get_backend_config().s3_region_proxy_endpoints.get(region_name)
        if not endpoint_url:
            raise S3ProxyDisabled(f"No S3 proxy endpoint configured for region {region_name!r}.")
    creds = get_job_aws_credentials_for_proxy(job_id, user_id, _get_role_arn(bucket_details))
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region_name,
        config=Config(signature_version="s3v4"),
        **creds.as_client_kwargs(),
    )


def presign_s3_urls_for_internal_usage(
    s3_uris: List[str], *, job_id: str, user_id: str, expiration: int
) -> List[str]:
    """Resolve a list of URIs, replacing any S3 URIs with presigned URLs for internal access.

    "Internal" means the presigned URLs point at the cluster-internal proxy endpoint and are
    only resolvable from within the cluster (e.g. by Spark executors), not from the outside.

    Non-S3 URIs are passed through unchanged. S3 URIs may optionally carry a Spark alias
    fragment (``s3://bucket/key#alias``); if present it is re-appended after the presigned URL.

    Note that the effective lifetime of the presigned URLs is also capped by the lifetime
    of the STS credentials they are signed with.
    """
    resolved = []
    for uri in s3_uris:
        if not uri.startswith("s3://"):
            resolved.append(uri)
            continue

        s3_uri_without_alias, alias = split_s3_uri_and_alias(uri)
        bucket, key = get_bucket_key_from_uri(s3_uri_without_alias)
        if not bucket or not key:
            raise ValueError(f"Invalid S3 URI {s3_uri_without_alias!r}: expected form s3://<bucket>/<key>")
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
                status_code=500,
                message=f"Could not create a presigned url for {s3_uri_without_alias} job_id={job_id} user={user_id}",
            )

        parsed = urlparse(presigned_url)
        if not (parsed.scheme and parsed.netloc and parsed.path):
            raise OpenEOApiException(
                status_code=500,
                message=f"Generated invalid presigned url for {s3_uri_without_alias} job_id={job_id} user={user_id}",
            )

        resolved_uri = f"{presigned_url}#{alias}" if alias is not None else presigned_url
        # Don't log the presigned URL itself: its query string carries the signature and session token.
        logger.info(f"Resolved S3 URI {s3_uri_without_alias!r} to presigned URL on {parsed.netloc!r}")
        resolved.append(resolved_uri)
    return resolved
