from __future__ import annotations
import boto3

from typing import TYPE_CHECKING
from openeogeotrellis.integrations.s3.endpoint import get_endpoint
from openeogeotrellis.integrations.s3.credentials import get_credentials


if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


def get_s3_client(region_name: str) -> S3Client:
    return boto3.client(
        "s3", region_name=region_name, endpoint_url=get_endpoint(region_name), **get_credentials(region_name)
    )
