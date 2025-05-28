from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

import boto3
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.integrations.s3proxy.exceptions import S3ProxyDisabled, S3ProxyUnsupportedBucketType
from openeogeotrellis.integrations.s3proxy.sts import get_job_aws_credentials_for_proxy
from openeo_driver.integrations.s3.bucket_details import BucketDetails, is_workspace_bucket


def _get_role_arn(bucket_details: BucketDetails) -> str:
    if is_workspace_bucket(bucket_details):
        assert bucket_details.type_id is not None
        return f"arn:openeows:iam:::role/{bucket_details.type_id}"
    raise S3ProxyUnsupportedBucketType


def get_proxy_s3_client_for_job(bucket: str, job_id: str, user_id) -> S3Client:
    """
    A proxy S3 client gets a client which is configured for bucket access scoped to an execution context.

    It takes a bucket because a bucket is required to identify the region where the data resides.
    """
    bucket_details = BucketDetails.from_name(bucket)
    region_name = bucket_details.region
    try:
        endpoint_url = get_backend_config().s3_region_proxy_endpoints[region_name]
        creds = get_job_aws_credentials_for_proxy(job_id, user_id, _get_role_arn(bucket_details))
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region_name,
            **creds.as_client_kwargs()
        )
    except KeyError:
        raise S3ProxyDisabled(f"Region {region_name} is not supported by proxy.")
