import logging
from typing import Tuple
from urllib.parse import urlparse

from openeo_driver.asset_urls import AssetUrl
from openeogeotrellis.integrations.s3proxy.exceptions import ProxyException
from openeogeotrellis.integrations.s3proxy.s3 import get_proxy_s3_client_for_job
from openeogeotrellis.integrations.s3 import create_presigned_url

_log = logging.getLogger(__name__)


class PresignedS3AssetUrls(AssetUrl):
    def __init__(self, expiration: int = 24 * 3600):
        self._expiration = expiration

    def get(self, asset_metadata: dict, asset_name: str, job_id: str, user_id: str) -> str:
        href = asset_metadata.get("href")
        if isinstance(href, str) and href.startswith("s3://"):
            try:
                bucket, key = self.get_bucket_key_from_uri(href)
                return self._get_presigned_url_against_proxy(bucket, key, job_id, user_id)
            except (ValueError, ProxyException) as e:
                logging.debug(f"Falling back to default asset getter because: {e}")
        return super().get(asset_metadata, asset_name, job_id, user_id)

    @staticmethod
    def get_bucket_key_from_uri(s3_uri: str) -> Tuple[str, str]:
        _parsed = urlparse(s3_uri, allow_fragments=False)
        if _parsed.scheme != "s3":
            raise ValueError(f"Input {s3_uri} is not a valid S3 URI should be of form s3://<bucket>/<key>")
        bucket = _parsed.netloc
        if _parsed.query:
            key = _parsed.path.lstrip('/') + '?' + _parsed.query
        else:
            key = _parsed.path.lstrip('/')
        return bucket, key

    def _get_presigned_url_against_proxy(self, bucket: str, key: str, job_id: str, user_id: str) -> str:
        s3_client = get_proxy_s3_client_for_job(bucket, job_id, user_id)
        return create_presigned_url(s3_client, bucket_name=bucket, object_name=key, expiration=self._expiration)
