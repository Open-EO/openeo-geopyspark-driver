import logging
from typing import TYPE_CHECKING, Any, Callable, Optional, Union
from urllib.parse import urlparse

import botocore.exceptions
from pystac import Link
from pystac.stac_io import DefaultStacIO

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

_log = logging.getLogger(__name__)


class CustomStacIO(DefaultStacIO):
    """Adds support for object storage."""

    def __init__(self, s3_client_factory: Callable[[str], "S3Client"], region: Optional[str] = None):
        super().__init__()
        self.region = region
        self._s3_client_factory = s3_client_factory

    def read_text(self, source: Union[str, Link], *args: Any, **kwargs: Any) -> str:
        parsed = urlparse(source)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path[1:]

            try:
                obj = self._s3_client_factory(bucket).get_object(Bucket=bucket, Key=key)
                return obj["Body"].read().decode("utf-8")
            except botocore.exceptions.ClientError as e:
                _log.warning(
                    f"could not get object at key {key}: [{e.response['Error']['Code']}] {e.response['Error']['Message']}",
                    exc_info=True,
                )
                raise
        else:
            return super().read_text(source, *args, **kwargs)

    def write_text(self, dest: Union[str, Link], txt: str, *args: Any, **kwargs: Any) -> None:
        parsed = urlparse(dest)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path[1:]

            try:
                self._s3_client_factory(bucket).put_object(
                    Bucket=bucket, Key=key, Body=txt.encode("utf-8"), ContentEncoding="utf-8"
                )
            except botocore.exceptions.ClientError as e:
                _log.warning(
                    f"could not put object at key {key}: [{e.response['Error']['Code']}] {e.response['Error']['Message']}",
                    exc_info=True,
                )
                raise
        else:
            super().write_text(dest, txt, *args, **kwargs)
