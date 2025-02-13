from typing import (
    Dict,
    Optional,
)
from urllib.error import HTTPError
from urllib.parse import urlparse

from pystac.stac_io import DefaultStacIO
from urllib3 import Retry, PoolManager


class StacApiIO(DefaultStacIO):
    """
    A STAC IO implementation that supports reading with timeout and retry.

    TODO: migrate to RetryStacIO when it has timeout support https://github.com/stac-utils/pystac/issues/1515
    """

    def __init__(
        self,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        retry: Optional[Retry] = None,
     ):
        super().__init__(headers=headers)
        self.timeout = timeout or 20
        self.retry = retry or Retry()

    def read_text_from_href(self, href: str) -> str:
        """Reads file as a UTF-8 string, with retry and timeout support.

        Args:
            href : The URI of the file to open.
        """
        is_url = urlparse(href).scheme != ""
        if is_url:
            http = PoolManager(retries=self.retry, timeout=self.timeout)
            try:
                response = http.request(
                    "GET", href
                )
                return response.data.decode("utf-8")
            except HTTPError as e:
                raise Exception("Could not read uri {}".format(href)) from e
        else:
            return super().read_text_from_href(href)
