import functools
import abc
import logging
import re
import threading
from collections import OrderedDict
from typing import Optional, Union, List, Tuple
from urllib.parse import urlparse

import requests
import pystac
import pystac.stac_io
from pystac.stac_io import DefaultStacIO
from pystac_client.stac_api_io import StacApiIO

from openeo_driver.integrations.s3.client import S3ClientBuilder

logger = logging.getLogger(__name__)


class _StacResponseCache:
    """Bounded, thread-safe LRU cache for STAC HTTP GET responses.

    Evicts least-recently-used entries when either the entry count exceeds
    ``maxsize`` or the total cached content exceeds ``max_bytes``.
    """

    def __init__(self, maxsize: int = 100, max_bytes: int = 30 * 1024 * 1024):
        self._maxsize = maxsize
        self._max_bytes = max_bytes
        self._cache: OrderedDict[str, str] = OrderedDict()
        self._total_bytes = 0
        self._lock = threading.Lock()

    @staticmethod
    def _normalize_url(url: str) -> str:
        return url.rstrip("/")

    @staticmethod
    def _byte_size(content: str) -> int:
        return len(content.encode("utf-8"))

    def get(self, url: str) -> Optional[str]:
        key = self._normalize_url(url)
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                logger.debug(f"STAC cache hit for {key}")
                return self._cache[key]
        logger.debug(f"STAC cache miss for {key}")
        return None

    def _evict_until_fits(self, needed_bytes: int) -> None:
        """Evict LRU entries until adding ``needed_bytes`` would stay within bounds.

        Must be called while holding ``self._lock``.
        """
        while self._cache and (
            len(self._cache) >= self._maxsize
            or self._total_bytes + needed_bytes > self._max_bytes
        ):
            evicted_key, evicted = self._cache.popitem(last=False)
            self._total_bytes -= self._byte_size(evicted)
            logger.debug(
                f"STAC cache evicting {evicted_key}"
                f" (entries={len(self._cache)}/{self._maxsize},"
                f" bytes={self._total_bytes}/{self._max_bytes})"
            )

    def put(self, url: str, content: str) -> None:
        key = self._normalize_url(url)
        content_bytes = self._byte_size(content)
        if content_bytes > self._max_bytes:
            logger.debug(
                f"STAC cache: skipping oversized entry {key}"
                f" ({content_bytes} bytes > {self._max_bytes} bytes limit)"
            )
            return
        with self._lock:
            if key in self._cache:
                old = self._cache[key]
                self._total_bytes -= self._byte_size(old)
                self._cache.move_to_end(key)
                self._cache[key] = content
                self._total_bytes += content_bytes
                logger.debug(
                    f"STAC cache updated {key}"
                    f" (entries={len(self._cache)}/{self._maxsize},"
                    f" bytes={self._total_bytes}/{self._max_bytes})"
                )
            else:
                self._evict_until_fits(content_bytes)
                self._cache[key] = content
                self._total_bytes += content_bytes
                logger.debug(
                    f"STAC cache added {key}"
                    f" (entries={len(self._cache)}/{self._maxsize},"
                    f" bytes={self._total_bytes}/{self._max_bytes})"
                )

    def clear(self) -> None:
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._total_bytes = 0
            logger.debug(f"STAC cache cleared ({count} entries removed)")


class _TieredStacResponseCache:
    """Two-tier STAC response cache with an explicit allowlist of cacheable URL types.

    Only three categories of URLs are cached:
    - API root and single-collection URLs (``/collections/<id>``) go into the
      ``_basic`` cache, isolated so that high-volume search traffic cannot
      evict them.
    - STAC search endpoint URLs (``/search``) go into the ``_search`` cache,
      keyed on the full URL including query parameters.

    All other URLs (items, job results, polling endpoints, …) are not cached.
    """

    # Matches paths like /collections/some-id (with optional trailing slash),
    # but NOT deeper sub-resources like /collections/some-id/items.
    _COLLECTION_RE = re.compile(r"/collections/[^/]+/?$")
    # Matches a single path segment that looks like a version prefix, e.g. /v1 or /v2.
    _VERSION_ROOT_RE = re.compile(r"^/v\d+/?$")
    # Matches a path ending in /search (with optional trailing slash).
    _SEARCH_RE = re.compile(r"/search/?$")

    def __init__(self, maxsize: int = 100, max_bytes: int = 30 * 1024 * 1024):
        self._basic = _StacResponseCache(maxsize=maxsize, max_bytes=max_bytes)
        self._search = _StacResponseCache(maxsize=maxsize, max_bytes=max_bytes)

    def _select(self, url: str) -> Optional[_StacResponseCache]:
        """Return the appropriate cache tier, or ``None`` if the URL should not be cached."""
        parsed = urlparse(url)
        path = parsed.path.rstrip("/")
        # API root: path is empty or a version prefix like /v1
        if not path or self._VERSION_ROOT_RE.match(path):
            return self._basic
        # Single collection: /…/collections/<id>
        if self._COLLECTION_RE.search(path):
            return self._basic
        # Search endpoint: /…/search — keyed on full URL including query params
        if self._SEARCH_RE.search(path):
            return self._search
        return None

    def get(self, url: str) -> Optional[str]:
        cache = self._select(url)
        if cache is None:
            return None
        return cache.get(url)

    def put(self, url: str, content: str) -> None:
        cache = self._select(url)
        if cache is None:
            return
        cache.put(url, content)

    def clear(self) -> None:
        self._basic.clear()
        self._search.clear()


_stac_response_cache = _TieredStacResponseCache(maxsize=100, max_bytes=30 * 1024 * 1024)


def _log_stac_response(response: requests.Response, *args, **kwargs) -> None:
    """requests response hook that logs every STAC HTTP request at DEBUG level."""
    elapsed = response.elapsed.total_seconds() if response.elapsed is not None else float("nan")
    logger.debug(
        f"STAC request: {response.request.method} {response.request.url}"
        f" -> {response.status_code} in {elapsed:.3f}s"
    )


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
        self._session.hooks["response"].append(_log_stac_response)

    def read_text_from_href(self, href: str) -> str:
        """Reads file as a UTF-8 string, with retry and timeout support.

        Args:
            href : The URI of the file to open.
        """
        is_url = urlparse(href).scheme != ""
        if is_url:
            cached = _stac_response_cache.get(href)
            if cached is not None:
                return cached
            try:
                with self._session.get(url=href) as resp:
                    resp.raise_for_status()
                    content = resp.content.decode("utf-8")
                    _stac_response_cache.put(href, content)
                    return content
            except requests.HTTPError as e:
                raise Exception(f"Could not read uri {href}") from e
        else:
            return super().read_text_from_href(href)


class LoggingStacApiIO(StacApiIO):
    """
    StacApiIO subclass that logs every HTTP request,
    including the method, URL, response status code, and elapsed time.

    Uses a requests response hook so every request made through the session
    is captured automatically — including the initial catalog fetch by
    Client.open() and each paginated search request.
    """

    # TODO: this class is called "Logging..." but is mostly about caching
    # TODO: both ResilientStacIO and LoggingStacApiIO do logging and caching now. Is that functional duplication (still) necessary?

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session.hooks["response"].append(_log_stac_response)

    def read_text_from_href(self, href: str) -> str:
        cached = _stac_response_cache.get(href)
        if cached is not None:
            return cached
        content = super().read_text_from_href(href)
        _stac_response_cache.put(href, content)
        return content

    def request(self, href, method=None, headers=None, parameters=None):
        # Only cache GET requests. POST requests (e.g. STAC search with a JSON
        # body) are not cached: building a reliable cache key from the request
        # body is non-trivial.
        is_get = method is None or method == "GET"
        cache_key = None
        if is_get:
            if parameters:
                prepped = self.session.prepare_request(
                    requests.Request("GET", href, params=parameters)
                )
                cache_key = prepped.url
            else:
                cache_key = href
            cached = _stac_response_cache.get(cache_key)
            if cached is not None:
                return cached
        content = super().request(href, method=method, headers=headers, parameters=parameters)
        if cache_key is not None:
            _stac_response_cache.put(cache_key, content)
        return content


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
