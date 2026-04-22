from pathlib import Path
import datetime
import boto3
import pystac
import pytest
import responses

from openeo.testing.stac import StacDummyBuilder
from openeogeotrellis.integrations.stac import (
    ResilientStacIO,
    S3StacIO,
    CompositeStacIO,
    ComposableStacIO,
    _TieredStacResponseCache,
    ref_as_str,
    CompactJsonStacIO,
)


class TestResilientStacIO:

    @responses.activate
    def test_basic(self):
        responses.get("https://example.test", json={"hello": "world"})

        stac_api_io = ResilientStacIO()
        assert stac_api_io.read_text_from_href("https://example.test") == '{"hello": "world"}'

    @responses.activate
    def test_http_error(self):
        responses.get("https://example.test", status=500)

        stac_api_io = ResilientStacIO()
        with pytest.raises(Exception, match="Could not read uri https://example.test"):
            stac_api_io.read_text_from_href("https://example.test")


def test_ref_as_str_simple_str():
    assert ref_as_str("https://stac.test/item.json") == "https://stac.test/item.json"
    assert ref_as_str("s3://bucket/key") == "s3://bucket/key"


def test_ref_as_str_link():
    link = pystac.Link(rel="self", target="s3://bucket/key")
    assert ref_as_str(link) == "s3://bucket/key"


def test_ref_as_str_link_with_owning_collection():
    link = pystac.Link(rel="item", target="item.json")
    collection = pystac.Collection.from_dict(
        StacDummyBuilder.collection(links=[{"rel": "self", "href": "s3://bucket/collection123"}])
    )
    link.set_owner(collection)
    assert ref_as_str(link) == "s3://bucket/item.json"


class _ReverseStacIO(ComposableStacIO):
    """Dummy STAC IO that reverses text content for files ending with .nosj"""

    def supports(self, ref):
        return ref_as_str(ref).endswith(".nosj")

    def read_text(self, source, *args, **kwargs):
        return Path(source).read_text()[::-1]

    def write_text(self, dest, txt, *args, **kwargs):
        txt = txt[::-1]
        return Path(dest).write_text(txt)


class TestCompositeStacIO:
    def test_read_text(self, tmp_path):
        item1_path = tmp_path / "item.json"
        item1_path.write_text("Hello world!")
        item2_path = tmp_path / "item.nosj"
        item2_path.write_text("!dlrow olleH")

        stac_io = CompositeStacIO(stac_ios=[_ReverseStacIO()])

        assert stac_io.read_text(str(item1_path)) == "Hello world!"
        assert stac_io.read_text(str(item2_path)) == "Hello world!"

    def test_write_text(self, tmp_path):
        item1_path = tmp_path / "item.json"
        item2_path = tmp_path / "item.nosj"

        stac_io = CompositeStacIO(stac_ios=[_ReverseStacIO()])

        stac_io.write_text(str(item1_path), "Hello world!")
        stac_io.write_text(str(item2_path), "Hello world!")

        assert item1_path.read_text() == "Hello world!"
        assert item2_path.read_text() == "!dlrow olleH"


class TestS3StacIO:
    @pytest.fixture(autouse=True)
    def s3(self, monkeypatch, moto_server):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")

        bucket = "thebucket"
        s3_client = boto3.client("s3", endpoint_url=moto_server)
        s3_client.create_bucket(Bucket=bucket)

        return s3_client

    @pytest.mark.parametrize(
        "ref_factory",
        [
            str,
            lambda s: pystac.Link(rel="self", target=s),
        ],
    )
    def test_read_text(self, s3, ref_factory):
        s3.put_object(Bucket="thebucket", Key="path/to/item.json", Body='{"type": "Feature", "id": "item123"}')

        stac_io = S3StacIO()
        source = ref_factory("s3://thebucket/path/to/item.json")
        assert stac_io.read_text(source) == '{"type": "Feature", "id": "item123"}'

    @pytest.mark.parametrize(
        "ref_factory",
        [
            str,
            lambda s: pystac.Link(rel="self", target=s),
        ],
    )
    def test_write_text(self, s3, ref_factory):
        stac_io = S3StacIO()
        dest = ref_factory("s3://thebucket/path/to/item.json")
        stac_io.write_text(dest, txt='{"type": "Feature", "id": "item123"}')

        response = s3.get_object(Bucket="thebucket", Key="path/to/item.json")
        body = response["Body"].read().decode("utf-8")
        assert body == '{"type": "Feature", "id": "item123"}'


class TestTieredStacResponseCache:
    @pytest.mark.parametrize(
        ["url", "expected_tier"],
        [
            # Basic tier: API root (no path)
            ("https://stac.test", "basic"),
            ("https://stac.test/", "basic"),
            ("https://stac.test/v1", "basic"),
            ("https://stac.test/v1/", "basic"),
            ("https://stac.test/?token=abc", "basic"),
            # Basic tier: single collection (various ID formats)
            ("https://stac.test/collections/sentinel-2-l2a", "basic"),
            ("https://stac.test/collections/sentinel-2-l2a/", "basic"),
            ("https://stac.test/v1/collections/my-collection", "basic"),
            ("https://stac.test/collections/cop.dem.glo30", "basic"),
            ("https://stac.test/collections/my_collection", "basic"),
            ("https://stac.test/collections/sentinel_2_l2a", "basic"),
            ("https://stac.test/v2/collections/my-collection", "basic"),
            ("https://stac.test/v10/collections/my-collection", "basic"),
            # Search tier
            ("https://stac.test/search", "search"),
            ("https://stac.test/v1/search", "search"),
            ("https://stac.test/v1/search?query=abc", "search"),
            ("https://stac.test/search?collections=sentinel-1-grd", "search"),
            (
                "https://stac.test/v1/search?collections=s2&bbox=1,2,3,4"
                "&datetime=2021-01-01T00:00:00Z/2021-02-01T00:00:00Z",
                "search",
            ),
            (
                "https://stac.dataspace.copernicus.eu/v1/search"
                "?limit=100&bbox=16.0%2C48.0%2C17.0%2C49.0"
                "&datetime=2021-01-01T00%3A00%3A00Z%2F2021-02-28T23%3A59%3A59.999000Z"
                "&collections=sentinel-2-l2a",
                "search",
            ),
            # Not cached (None): conformance, collections listing, items, queryables, etc.
            ("https://stac.test/conformance", None),
            ("https://stac.test/v1/conformance", None),
            ("https://stac.test/collections", None),
            ("https://stac.test/v1/collections", None),
            ("https://stac.test/collections/sentinel-2-l2a/items", None),
            ("https://stac.test/v1/collections/my-collection/items", None),
            ("https://stac.test/collections/sentinel-2-l2a/items/item-123", None),
            ("https://stac.test/v1/collections/my-collection/items/my-item", None),
            ("https://stac.test/queryables", None),
            ("https://stac.test/collections/sentinel-2-l2a/queryables", None),
            ("https://stac.test/jobs/job-123/results", None),
            ("https://stac.test/any/deep/path", None),
            ("https://stac.test/any", None),
            ("https://stac.test/api", None),
            ("https://stac.test/v1/api", None),
        ],
    )
    def test_select_routing(self, url, expected_tier):
        cache = _TieredStacResponseCache()
        tier = cache._select(url)
        if expected_tier == "basic":
            assert tier is cache._basic
        elif expected_tier == "search":
            assert tier is cache._search
        else:
            assert tier is None

    @pytest.mark.parametrize(
        ["url", "content"],
        [
            ("https://stac.test/collections/s2", "collection-content"),
            ("https://stac.test/search", "search-content"),
        ],
    )
    def test_get_put(self, url, content):
        cache = _TieredStacResponseCache()
        assert cache.get(url) is None
        cache.put(url, content)
        assert cache.get(url) == content

    def test_search_overflow_does_not_evict_basic(self):
        """Evictions in the search tier must not affect the basic tier."""
        cache = _TieredStacResponseCache(maxsize=2, max_bytes=10 * 1024 * 1024)
        cache.put("https://stac.test/collections/s2", "basic-content")
        for i in range(5):
            cache.put(f"https://stac.test/search?page={i}", f"search-result-{i}")
        assert cache.get("https://stac.test/collections/s2") == "basic-content"

    def test_basic_overflow_does_not_evict_search(self):
        """Evictions in the basic tier must not affect the search tier."""
        cache = _TieredStacResponseCache(maxsize=2, max_bytes=10 * 1024 * 1024)
        cache.put("https://stac.test/search?q=initial", "initial-search")
        for i in range(5):
            cache.put(f"https://stac.test/collections/collection-{i}", f"collection-{i}")
        assert cache.get("https://stac.test/search?q=initial") == "initial-search"

    def test_clear(self):
        cache = _TieredStacResponseCache()
        cache.put("https://stac.test/collections/s2", "basic-content")
        cache.put("https://stac.test/search", "search-content")
        cache.clear()
        assert cache.get("https://stac.test/collections/s2") is None
        assert cache.get("https://stac.test/search") is None

    def test_search_query_params_are_part_of_cache_key(self):
        """Different query parameters must produce independent cache entries."""
        cache = _TieredStacResponseCache()
        cache.put("https://stac.test/search?collections=sentinel-2-l2a", "result-s2")
        assert cache.get("https://stac.test/search?collections=sentinel-2-l2a") == "result-s2"
        assert cache.get("https://stac.test/search?collections=sentinel-1-grd") is None

    def test_uncached_url_put_is_noop(self):
        """Putting a non-cacheable URL must be silently ignored."""
        cache = _TieredStacResponseCache()
        cache.put("https://stac.test/collections/s2/items", "items-content")
        assert cache.get("https://stac.test/collections/s2/items") is None

    # --- Oversized-entry protection ---------------------------------------------

    def test_oversized_entry_not_stored_and_preserves_existing(self):
        """An entry larger than max_bytes must be silently dropped without evicting existing entries."""
        cache = _TieredStacResponseCache(maxsize=100, max_bytes=100)
        oversized = "x" * 101

        # Pre-populate both tiers
        cache.put("https://stac.test/search?q=a", "small-a")
        cache.put("https://stac.test/search?q=b", "small-b")
        cache.put("https://stac.test/collections/s2", "collection-s2")

        # Attempt oversized puts in both tiers
        cache.put("https://stac.test/search?q=big", oversized)
        cache.put("https://stac.test/collections/large", oversized)

        # Oversized entries must not be stored
        assert cache.get("https://stac.test/search?q=big") is None
        assert cache.get("https://stac.test/collections/large") is None
        # Existing entries in both tiers must survive
        assert cache.get("https://stac.test/search?q=a") == "small-a"
        assert cache.get("https://stac.test/search?q=b") == "small-b"
        assert cache.get("https://stac.test/collections/s2") == "collection-s2"


class TestCompactJsonStacIO:
    def test_basic(self, tmp_path):
        item = pystac.Item(
            id="item-123", geometry=None, bbox=None, datetime=datetime.datetime(2026, 5, 6), properties={}
        )
        path = tmp_path / "item.json"
        item.save_object(dest_href=str(path), stac_io=CompactJsonStacIO())
        serialized = path.read_text(encoding="utf-8")
        # No (unnecessary) whitespace
        assert len(serialized.split()) == 1
        # But still loadable
        _ = pystac.Item.from_file(path)
