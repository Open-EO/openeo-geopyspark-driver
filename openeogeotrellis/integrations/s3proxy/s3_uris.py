from __future__ import annotations

from typing import Optional, Tuple
from urllib.parse import urlparse

from openeo_driver.errors import OpenEOApiException


def get_bucket_key_from_uri(s3_uri: str) -> Tuple[str, str]:
    """Parse an S3 URI into (bucket, key).

    Raises ValueError if the URI is not a valid S3 URI.
    """
    parsed = urlparse(s3_uri, allow_fragments=False)
    if parsed.scheme != "s3":
        raise ValueError(f"Input {s3_uri!r} is not a valid S3 URI; expected form s3://<bucket>/<key>")
    bucket = parsed.netloc
    if parsed.query:
        key = parsed.path.lstrip("/") + "?" + parsed.query
    else:
        key = parsed.path.lstrip("/")
    return bucket, key


def split_s3_uri_and_alias(s3_uri: str) -> Tuple[str, Optional[str]]:
    """Split an S3 URI into the URI without alias and an optional alias fragment.

    Returns (s3_uri_without_alias, alias) where alias may be None if no '#' is present.
    Raises OpenEOApiException if the URI has more than one '#' or an empty alias.
    """
    if s3_uri.count("#") > 1:
        raise OpenEOApiException(
            status_code=400,
            message=f"Invalid S3 URI {s3_uri!r}: expected at most one '#'.",
        )
    if "#" in s3_uri:
        s3_uri_without_alias, alias = s3_uri.split("#", 1)
        if not alias:
            raise OpenEOApiException(
                status_code=400,
                message=f"Invalid S3 URI {s3_uri!r}: expected non-empty alias after '#'.",
            )
        return s3_uri_without_alias, alias
    return s3_uri, None
