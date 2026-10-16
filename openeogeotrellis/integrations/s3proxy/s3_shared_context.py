from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

from botocore.config import Config
import boto3
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.config.s3_config import AWSConfig
from openeogeotrellis.integrations.s3proxy.exceptions import S3ProxyDisabled, S3ProxyUnsupportedBucketType
from openeogeotrellis.integrations.s3proxy.sts import get_job_aws_credentials_for_proxy
from openeo_driver.integrations.s3.bucket_details import BucketDetails, is_workspace_bucket
from openeo_driver.util.caching import BoundedTtlCache


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
