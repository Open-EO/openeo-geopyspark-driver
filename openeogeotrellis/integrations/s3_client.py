"""
S3 client construction, kept in its own narrow module (no `pyspark`/`py4j`
imports, unlike `openeogeotrellis.utils` where this used to live) so that
non-Spark consumers of e.g. `openeogeotrellis.integrations.stac`
(which needs an S3 client for `s3://`-based STAC I/O) don't transitively pull
in `pyspark`/`py4j` just to get an S3 client.
"""
from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from openeo_driver.integrations.s3.client import S3ClientBuilder as PythonDriverS3ClientBuilder
from openeo_driver.util.caching import BoundedTtlCache

from openeogeotrellis.integrations.s3proxy.s3_user_context import build_proxy_s3_client, should_proxy_be_used

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

logger = logging.getLogger(__name__)


def eodata_s3_client():
    import boto3

    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    endpoint = os.environ.get("AWS_S3_ENDPOINT")
    https = "http" if os.environ.get("AWS_HTTPS", "yes").lower() == "no" else "https"
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=https + "://" + endpoint,
    )
    return s3_client


class S3ClientBuilder:
    _s3_client_cache = BoundedTtlCache(ttl=12 * 60 * 60, max_size=5)

    @classmethod
    def from_bucket(cls, bucket_name: str) -> S3Client:
        """
        Get an S3 client to allow for interaction with a certain bucket.
        """
        return cls._s3_client_cache.get_or_call(bucket_name, lambda: cls._get_s3_client(bucket_name))

    @classmethod
    def _get_s3_client(cls, bucket_name: str) -> S3Client:
        if should_proxy_be_used():
            client = build_proxy_s3_client(bucket_name)
            if client is None:
                raise RuntimeError(f"Failed to build proxy S3 client for bucket '{bucket_name}'; see prior warnings.")
            return client
        else:
            return cls._get_direct_s3_client(bucket_name)

    @classmethod
    def _get_direct_s3_client(cls, bucket_name: str) -> S3Client:
        """
        For clients that do not run in a context with OIDC access tokens. eodata is a special case.
        """
        if bucket_name.lower() == "eodata":
            logger.debug("Getting direct S3 client for eodata access")
            return eodata_s3_client()
        else:
            return PythonDriverS3ClientBuilder.from_bucket(bucket_name)
