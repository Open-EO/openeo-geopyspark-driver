import logging

from openeo_driver.asset_urls import AssetUrl
from openeogeotrellis.integrations.s3proxy.exceptions import ProxyException
from openeogeotrellis.integrations.s3proxy.s3_shared_context import get_proxy_s3_client_for_job
from openeogeotrellis.integrations.s3proxy.s3_uris import get_bucket_key_from_uri
from openeo_driver.integrations.s3.presigned_url import create_presigned_url

_log = logging.getLogger(__name__)


class PresignedS3AssetUrls(AssetUrl):
    def __init__(self, expiration: int = 24 * 3600):
        self._expiration = expiration

    def build_url(self, *, asset_metadata: dict, asset_name: str, job_id: str, user_id: str) -> str:
        href = asset_metadata.get("href")
        if isinstance(href, str) and href.startswith("s3://"):
            try:
                bucket, key = get_bucket_key_from_uri(href)
                return self._get_presigned_url_against_proxy(bucket, key, job_id, user_id, internal=False)
            except (ValueError, ProxyException) as e:
                logging.debug(f"Falling back to default asset getter because: {e}")
        return super().build_url(asset_metadata=asset_metadata, asset_name=asset_name, job_id=job_id, user_id=user_id)

    def _get_presigned_url_against_proxy(self, bucket: str, key: str, job_id: str, user_id: str, internal: bool) -> str:
        s3_client = get_proxy_s3_client_for_job(bucket, job_id, user_id, internal)
        url = create_presigned_url(
            s3_client,
            bucket_name=bucket,
            object_name=key,
            expiration=self._expiration,
            default=None,
            parameters={"X-Proxy-Head-As-Get": "true"},
        )
        if url is None:
            raise ValueError(f"Could not create a presigned url for s3://{bucket}/{key} job_id={job_id} user={user_id}")
        return url

    def get_presigned_url_against_internal_proxy(self, bucket: str, key: str, job_id: str, user_id: str) -> str:
        return self._get_presigned_url_against_proxy(
            bucket=bucket, key=key, job_id=job_id, user_id=user_id, internal=True
        )
