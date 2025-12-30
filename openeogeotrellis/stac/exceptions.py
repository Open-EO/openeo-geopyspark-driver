from __future__ import annotations

from typing import Optional

from openeo_driver.errors import (
    OpenEOApiException,
)


class NoDataAvailableException(OpenEOApiException):
    status_code = 400
    code = "NoDataAvailable"
    message = "There is no data available for the given extents."


class LoadStacException(OpenEOApiException):
    """Generic/base exception for load_stac failures"""

    status_code = 500
    code = "LoadStacFailure"

    def __init__(
        self,
        *,
        url: str = "n/a",
        info: str = "n/a",
        message: Optional[str] = None,
        status_code: Optional[int] = None,
        code: Optional[str] = None,
    ):
        if not message:
            message = f"Error when constructing data cube from load_stac({url!r}): {info}"
        super().__init__(message=message, code=code, status_code=status_code)
        self.url = url
