import functools
import abc
from typing import Optional, Union, List, Tuple
from urllib.parse import urlparse

import requests
import pystac
import pystac.stac_io
from pystac.stac_io import DefaultStacIO

from openeo_driver.integrations.s3.client import S3ClientBuilder


class ResilientStacIO(DefaultStacIO):
    """
    A STAC IO implementation that supports reading with timeout and retry.
    """

    def __init__(
        self,
        session: Optional[requests.Session] = None,
     ):
        super().__init__()
        self._session = session or requests.Session()

    def read_text_from_href(self, href: str) -> str:
        """Reads file as a UTF-8 string, with retry and timeout support.

        Args:
            href : The URI of the file to open.
        """
        is_url = urlparse(href).scheme != ""
        if is_url:
            try:
                with self._session.get(url=href) as resp:
                    resp.raise_for_status()
                    return resp.content.decode("utf-8")
            except requests.HTTPError as e:
                raise Exception(f"Could not read uri {href}") from e
        else:
            return super().read_text_from_href(href)


def ref_as_str(ref: Union[pystac.stac_io.HREF, pystac.Link]) -> str:
    """Helper to get the string representation of a STAC reference."""
    return ref.absolute_href if isinstance(ref, pystac.Link) else str(ref)


class ComposableStacIO(pystac.stac_io.StacIO, metaclass=abc.ABCMeta):
    """
    Interface for composable `StacIO` implementations,

    Allows to combine various `StacIO` implementations
    without having to work with multiple levels of inheritance.
    """

    @abc.abstractmethod
    def supports(self, ref: Union[pystac.stac_io.HREF, pystac.Link]) -> bool:
        return False


class CompositeStacIO(pystac.stac_io.StacIO):
    """
    A `StacIO` implementation that combines multiple `StacIO` implementations.
    """

    def __init__(self, stac_ios: List[ComposableStacIO], *, default: Optional[pystac.stac_io.StacIO] = None):
        super().__init__()
        self._stac_ios = stac_ios
        self._default_stac_io = default or DefaultStacIO()

    def _get_stac_io_for(self, ref: Union[str, pystac.Link]) -> pystac.stac_io.StacIO:
        for stac_io in self._stac_ios:
            if stac_io.supports(ref):
                return stac_io
        return self._default_stac_io

    def read_text(self, source: Union[str, pystac.Link], *args, **kwargs) -> str:
        stac_io = self._get_stac_io_for(source)
        return stac_io.read_text(source, *args, **kwargs)

    def write_text(self, dest: Union[str, pystac.Link], txt: str, *args, **kwargs) -> None:
        stac_io = self._get_stac_io_for(dest)
        return stac_io.write_text(dest, txt, *args, **kwargs)


class S3StacIO(ComposableStacIO):
    """
    Composable STAC IO implementation that supports S3 URIs using boto3.
    """

    def __init__(self, region: Optional[str] = None):
        super().__init__()
        self._region = region

    def supports(self, ref: Union[pystac.stac_io.HREF, pystac.Link]) -> bool:
        parsed = urlparse(ref)
        return parsed.scheme == "s3"

    @classmethod
    def _parse(cls, ref: Union[pystac.stac_io.HREF, pystac.Link]) -> Tuple[str, str]:
        ref = ref_as_str(ref)
        parsed = urlparse(ref)
        assert parsed.scheme == "s3", f"Only s3 scheme is supported, got: {parsed.scheme}"
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        return bucket, key

    @functools.lru_cache(maxsize=None)
    def _s3_client(self):
        return S3ClientBuilder.from_region(region_name=self._region)

    def read_text(self, source: Union[pystac.stac_io.HREF, pystac.Link], *args, **kwargs) -> str:
        bucket, key = self._parse(source)
        response = self._s3_client().get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    def write_text(self, dest: Union[pystac.stac_io.HREF, pystac.Link], txt: str, *args, **kwargs) -> None:
        bucket, key = self._parse(dest)
        self._s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=txt.encode("utf-8"),
            ContentEncoding="utf-8",
        )
