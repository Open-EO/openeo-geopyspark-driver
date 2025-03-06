from typing import Optional
from urllib.parse import urlparse

import requests
from pystac.stac_io import DefaultStacIO


class StacApiIO(DefaultStacIO):
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
