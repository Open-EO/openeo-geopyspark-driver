from typing import Union, Any
from urllib.parse import urlparse

from pystac import Link
from pystac.stac_io import DefaultStacIO

from openeogeotrellis.utils import s3_client


class CustomStacIO(DefaultStacIO):
    """Adds support for object storage."""

    @property
    def _s3(self):
        # otherwise there's an infinite recursion error upon Item.get_assets() wrt/ some boto3 reference
        return s3_client()

    def read_text(self, source: Union[str, Link], *args: Any, **kwargs: Any) -> str:
        parsed = urlparse(source)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path[1:]

            obj = self._s3.get_object(Bucket=bucket, Key=key)
            return obj["Body"].read().decode("utf-8")
        else:
            return super().read_text(source, *args, **kwargs)

    def write_text(self, dest: Union[str, Link], txt: str, *args: Any, **kwargs: Any) -> None:
        parsed = urlparse(dest)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path[1:]
            self._s3.put_object(Bucket=bucket, Key=key, Body=txt.encode("utf-8"), ContentEncoding="utf-8")
        else:
            super().write_text(dest, txt, *args, **kwargs)
