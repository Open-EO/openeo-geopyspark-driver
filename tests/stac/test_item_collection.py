"""
Focused unit tests for `openeogeotrellis.stac.item_collection`, exercised
through its own public surface (no JVM/GeoPySpark dependency), independent of
`test_load_stac.py`'s end-to-end JVM-backed checks.
"""
import datetime
import json
import logging
from typing import Iterator

import dirty_equals
import pystac
import pystac.stac_io
import pytest
from openeo.testing.stac import StacDummyBuilder
from openeo_driver.users import User
from openeo_driver.util.geometry import BoundingBox

from openeogeotrellis.backend import GpsBatchJobs
from openeogeotrellis.job_registry import InMemoryJobRegistry
from openeogeotrellis.stac.extents import SpatioTemporalExtent
from openeogeotrellis.stac.item_collection import (
    STAC_API_PER_PAGE_LIMIT_DEFAULT,
    ItemCollection,
    LiveStacSourceResolver,
    StacResolution,
    _pystac_item_from_dict_lenient,
    _supports_item_search,
    construct_item_collection,
)
from openeogeotrellis.stac.item_deduplicator import ItemDeduplicator
from openeogeotrellis.stac.property_filter import PropertyFilter
from openeogeotrellis.stac.stac_object_fetching import PollingConfig
from openeogeotrellis.testing import DummyStacApiServer, gps_config_overrides
from openeogeotrellis.util.geometry import bbox_to_geojson


class _NeverCompletingStacIO(pystac.stac_io.StacIO):
    """Always reports a still-running partial batch job result."""

    def read_text(self, source, *args, **kwargs) -> str:
        return json.dumps(
            {
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": "never-done",
                "properties": {"datetime": "2024-01-01T00:00:00Z"},
                "geometry": None,
                "links": [],
                "assets": {},
                "openeo:status": "running",
            }
        )

    def write_text(self, dest, txt, *args, **kwargs) -> None:
        raise NotImplementedError


def test_live_stac_source_resolver_times_out_on_never_completing_source():
    resolver = LiveStacSourceResolver(
        stac_io=_NeverCompletingStacIO(),
        polling=PollingConfig(poll_interval_seconds=0.01, max_poll_delay_seconds=0.05),
    )
    with pytest.raises(Exception, match="was not satisfied after"):
        resolver.resolve("https://stac.test/never-done", spatiotemporal_extent=None)


def test_construct_item_collection_consults_source_resolvers_in_order():
    """A fake resolver passed via `source_resolvers=` is consulted first, ahead of the default resolvers."""
    item = pystac.Item.from_dict(StacDummyBuilder.item())
    fake_item_collection = ItemCollection(items=[item])

    class _FakeResolver:
        def __init__(self):
            self.calls = []

        def resolve(self, url, *, spatiotemporal_extent):
            self.calls.append(url)
            return StacResolution(item_collection=fake_item_collection)

    fake_resolver = _FakeResolver()
    stac_source = construct_item_collection(
        url="https://stac.test/never-fetched", source_resolvers=[fake_resolver]
    )

    assert fake_resolver.calls == ["https://stac.test/never-fetched"]
    assert stac_source.item_collection is fake_item_collection
    assert stac_source.collection_summary == {}
    assert stac_source.band_names == []


class TestItemCollection:
    def test_from_stac_item_basic(self):
        item = pystac.Item.from_dict(StacDummyBuilder.item())
        spatiotemporal_extent = SpatioTemporalExtent(bbox=None, from_date=None, to_date=None)
        item_collection = ItemCollection.from_stac_item(item, spatiotemporal_extent=spatiotemporal_extent)

        assert item_collection.items == [item]

    @pytest.mark.parametrize(
        ["bbox", "interval", "expected"],
        [
            ((20, 34, 26, 40), ["2025-09-01", "2025-10-01"], True),
            ((20, 34, 26, 40), ["2025-10-01", "2025-11-01"], False),
            ((30, 34, 36, 40), ["2025-09-01", "2025-10-01"], False),
        ],
    )
    def test_from_stac_item_with_filtering(self, bbox, interval, expected):
        item = pystac.Item.from_dict(StacDummyBuilder.item(datetime="2025-09-04", bbox=[20, 30, 25, 35]))

        from_date, to_date = interval
        spatiotemporal_extent = SpatioTemporalExtent(
            bbox=BoundingBox.from_wsen_tuple(bbox, crs=4326), from_date=from_date, to_date=to_date
        )
        item_collection = ItemCollection.from_stac_item(item, spatiotemporal_extent=spatiotemporal_extent)
        expected = [item] if expected else []
        assert item_collection.items == expected

    @pytest.mark.parametrize(
        ["bbox", "interval", "expected"],
        [
            # Full spatio-temporal overlap
            ((10, 20, 30, 40), ["2025-09-01", "2025-10-01"], [1, 2]),
            ((21, 31, 26, 36), ["2025-09-01", "2025-10-01"], [1, 2]),
            # Spatial constraints
            ((20, 30, 23, 33), ["2025-09-01", "2025-10-01"], [1]),
            ((26, 36, 27, 37), ["2025-09-01", "2025-10-01"], [2]),
            # Temporal constraints
            ((20, 34, 26, 40), ["2025-09-01", "2025-09-07"], [1]),
            ((20, 34, 26, 40), ["2025-09-05", "2025-09-10"], [2]),
            # No overlap
            ((10, 20, 30, 40), ["2025-10-01", "2025-11-01"], []),
            ((70, 70, 80, 80), ["2025-09-01", "2025-10-01"], []),
        ],
    )
    @gps_config_overrides()
    def test_from_own_job(self, bbox, interval, expected):
        from_date, to_date = interval
        spatiotemporal_extent = SpatioTemporalExtent(
            bbox=BoundingBox.from_wsen_tuple(bbox, crs=4326), from_date=from_date, to_date=to_date
        )

        user = User("john")
        job_registry = InMemoryJobRegistry()
        batch_jobs = GpsBatchJobs(catalog=None, jvm=None, elastic_job_registry=job_registry)
        job = batch_jobs.create_job(user=user, process={"foo": "bar"}, api_version="1.0.0", metadata={})
        job_registry.set_status(job_id=job.id, user_id=user.user_id, status="finished")
        job_registry.set_results_metadata(
            job_id=job.id,
            user_id=user.user_id,
            costs=0,
            usage={},
            results_metadata={
                "assets": {
                    "asset1": {
                        "bbox": [20, 30, 25, 35],
                        "geometry": bbox_to_geojson(20, 30, 25, 35),
                        "datetime": "2025-09-04T10:00:00Z",
                        "roles": ["data"],
                        "href": "https://data.test/asset1.tif",
                        "bands": [{"name": "red"}],
                    },
                    "asset2": {
                        "bbox": [24, 34, 28, 38],
                        "geometry": bbox_to_geojson(24, 34, 28, 38),
                        "datetime": "2025-09-08T10:00:00Z",
                        "roles": ["data"],
                        "href": "https://data.test/asset2.tif",
                        "bands": [{"name": "red"}],
                    },
                }
            },
        )

        item_collection = ItemCollection.from_own_job(
            job=job, spatiotemporal_extent=spatiotemporal_extent, batch_jobs=batch_jobs, user=user
        )

        expected_map = {
            1: dirty_equals.IsPartialDict(
                {
                    "type": "Feature",
                    "stac_version": dirty_equals.IsOneOf("1.0.0", "1.1.0"),
                    "id": "asset1",
                    "assets": {"asset1": {"eo:bands": [{"name": "red"}], "href": "https://data.test/asset1.tif"}},
                    "bbox": [20, 30, 25, 35],
                    "properties": {"datetime": "2025-09-04T10:00:00Z"},
                }
            ),
            2: dirty_equals.IsPartialDict(
                {
                    "type": "Feature",
                    "stac_version": dirty_equals.IsOneOf("1.0.0", "1.1.0"),
                    "id": "asset2",
                    "assets": {"asset2": {"eo:bands": [{"name": "red"}], "href": "https://data.test/asset2.tif"}},
                    "bbox": [24, 34, 28, 38],
                    "properties": {"datetime": "2025-09-08T10:00:00Z"},
                }
            ),
        }
        expected = [expected_map[e] for e in expected]
        assert [item.to_dict() for item in item_collection.items] == expected

    def test_from_stac_catalog_basic(self):
        collection = pystac.Collection(
            id="c123",
            description="C123",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent(bboxes=[[20, 30, 25, 35]]),
                temporal=pystac.TemporalExtent.from_dict({"interval": ["2025-07-01", "2025-08-31"]}),
            ),
        )
        item1 = pystac.Item.from_dict(StacDummyBuilder.item(datetime="2025-07-10", bbox=[21, 31, 25, 35]))
        item2 = pystac.Item.from_dict(StacDummyBuilder.item(datetime="2025-07-20", bbox=[22, 32, 25, 35]))
        collection.add_link(pystac.Link(rel=pystac.RelType.ITEM, target=item1))
        collection.add_link(pystac.Link(rel=pystac.RelType.ITEM, target=item2))

        spatiotemporal_extent = SpatioTemporalExtent(bbox=None, from_date=None, to_date=None)
        item_collection = ItemCollection.from_stac_catalog(collection, spatiotemporal_extent=spatiotemporal_extent)
        assert item_collection.items == [item1, item2]

    @pytest.mark.parametrize(
        ["bbox", "interval", "expected"],
        [
            (None, None, [1, 2]),
            ([20, 30, 25, 35], ["2025-06-01", "2025-09-30"], [1, 2]),
            ([20, 30, 21, 31], ["2025-06-01", "2025-09-30"], [1]),
            ([22.3, 32.3, 22.6, 32.6], ["2025-06-01", "2025-09-30"], [2]),
            ([20, 30, 25, 35], ["2025-07-05", "2025-07-15"], [1]),
            ([20, 30, 25, 35], ["2025-07-15", "2025-07-25"], [2]),
        ],
    )
    def test_from_stac_catalog_spatiotemporal_filtering(self, bbox, interval, expected):
        collection = pystac.Collection(
            id="c123",
            description="C123",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent(bboxes=[[20, 30, 25, 35]]),
                temporal=pystac.TemporalExtent.from_dict({"interval": ["2025-07-01", "2025-08-31"]}),
            ),
        )
        item1 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item1", datetime="2025-07-10", bbox=[21, 31, 21.5, 31.5])
        )
        item2 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item2", datetime="2025-07-20", bbox=[22, 32, 22.5, 32.5])
        )
        collection.add_link(pystac.Link(rel=pystac.RelType.ITEM, target=item1))
        collection.add_link(pystac.Link(rel=pystac.RelType.ITEM, target=item2))

        from_date, to_date = interval or (None, None)
        if bbox:
            bbox = BoundingBox.from_wsen_tuple(bbox, crs=4326)
        spatiotemporal_extent = SpatioTemporalExtent(bbox=bbox, from_date=from_date, to_date=to_date)
        item_collection = ItemCollection.from_stac_catalog(collection, spatiotemporal_extent=spatiotemporal_extent)

        expected = [{1: item1, 2: item2}[x] for x in expected]
        assert item_collection.items == expected

    @pytest.fixture
    def dummy_stac_api_server(self) -> DummyStacApiServer:
        dummy_server = DummyStacApiServer()

        dummy_server.define_collection(
            "custom-s2",
            extent={
                "spatial": {"bbox": [[3, 50, 5, 51]]},
                "temporal": {"interval": [["2024-02-01T00:00:00Z", "2024-12-01"]]},
            },
        )
        for m in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]:
            for x in [3, 4]:
                dummy_server.define_item(
                    collection_id="custom-s2",
                    item_id=f"item-{m}-{x}",
                    datetime=f"2024-{m:02d}-20T12:00:00Z",
                    bbox=[x + 0.1, 50.1, x + 0.9, 50.9],
                    properties={"flavor": {0: "apple", 1: "banana", 2: "coconut"}[(m + x) % 3]},
                )
        return dummy_server

    @pytest.fixture
    def dummy_stac_api(self, dummy_stac_api_server) -> Iterator[str]:
        with dummy_stac_api_server.serve() as root_url:
            yield root_url

    def test_from_stac_api_basic(self, dummy_stac_api):
        given_url = f"{dummy_stac_api}/collections/collection-123"
        collection: pystac.Collection = pystac.read_file(given_url)
        property_filter = PropertyFilter(properties={})
        spatiotemporal_extent = SpatioTemporalExtent(bbox=None, from_date="2024-01-01", to_date="2025-01-01")
        item_collection = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=property_filter,
            spatiotemporal_extent=spatiotemporal_extent,
        )
        assert [item.id for item in item_collection.items] == ["item-1", "item-2", "item-3"]

    @pytest.mark.parametrize(
        ["from_date", "to_date", "expected_items"],
        [
            ("2024-06-01", "2024-09-01", {"item-6-3", "item-6-4", "item-7-3", "item-7-4", "item-8-3", "item-8-4"}),
            (None, "2024-04-01", {"item-2-3", "item-2-4", "item-3-3", "item-3-4"}),
            ("2024-09-01", None, {"item-9-3", "item-9-4", "item-10-3", "item-10-4", "item-11-3", "item-11-4"}),
        ],
    )
    def test_from_stac_api_temporal_filter(self, dummy_stac_api, from_date, to_date, expected_items):
        given_url = f"{dummy_stac_api}/collections/custom-s2"
        collection: pystac.Collection = pystac.read_file(given_url)
        property_filter = PropertyFilter(properties={})
        spatiotemporal_extent = SpatioTemporalExtent(bbox=None, from_date=from_date, to_date=to_date)
        item_collection = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=property_filter,
            spatiotemporal_extent=spatiotemporal_extent,
        )
        assert set(item.id for item in item_collection.items) == expected_items

    @pytest.mark.parametrize(
        ["bbox", "expected_items"],
        [
            (
                BoundingBox(3, 50, 4, 51, crs=4326),
                {f"item-{x}-3" for x in range(2, 12)},
            ),
            (
                BoundingBox(4, 50, 5, 51, crs=4326),
                {f"item-{x}-4" for x in range(2, 12)},
            ),
        ],
    )
    def test_from_stac_api_spatial_filter(self, dummy_stac_api, bbox, expected_items):
        given_url = f"{dummy_stac_api}/collections/custom-s2"
        collection: pystac.Collection = pystac.read_file(given_url)
        property_filter = PropertyFilter(properties={})
        spatiotemporal_extent = SpatioTemporalExtent(bbox=bbox, from_date="2024-01-01", to_date="2025-01-01")
        item_collection = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=property_filter,
            spatiotemporal_extent=spatiotemporal_extent,
        )
        assert set(item.id for item in item_collection.items) == expected_items

    @pytest.mark.parametrize(
        ["use_filter_extension", "expected_search"],
        [
            (
                "cql2-json",
                {
                    "method": "POST",
                    "path": "/search",
                    "url_params": {},
                    "json": {
                        "collections": ["custom-s2"],
                        "datetime": "2024-01-01T00:00:00Z/2025-01-01T00:00:00Z",
                        "limit": STAC_API_PER_PAGE_LIMIT_DEFAULT,
                        "filter-lang": "cql2-json",
                        "filter": {"op": "=", "args": [{"property": "flavor"}, "banana"]},
                    },
                },
            ),
            (
                "cql2-text",
                {
                    "method": "GET",
                    "path": "/search",
                    "url_params": {
                        "collections": "custom-s2",
                        "datetime": "2024-01-01T00:00:00Z/2025-01-01T00:00:00Z",
                        "limit": str(STAC_API_PER_PAGE_LIMIT_DEFAULT),
                        "filter-lang": "cql2-text",
                        "filter": "\"flavor\" = 'banana'",
                    },
                    "json": None,
                },
            ),
            (
                # Auto mode: Prefer POST with cql2-json if supported by server
                True,
                {
                    "method": "POST",
                    "path": "/search",
                    "url_params": {},
                    "json": {
                        "collections": ["custom-s2"],
                        "datetime": "2024-01-01T00:00:00Z/2025-01-01T00:00:00Z",
                        "limit": STAC_API_PER_PAGE_LIMIT_DEFAULT,
                        "filter-lang": "cql2-json",
                        "filter": {"op": "=", "args": [{"property": "flavor"}, "banana"]},
                    },
                },
            ),
            (
                # No usage of filter extension
                False,
                {
                    "method": "GET",
                    "path": "/search",
                    "url_params": {
                        "collections": "custom-s2",
                        "datetime": "2024-01-01T00:00:00Z/2025-01-01T00:00:00Z",
                        "limit": str(STAC_API_PER_PAGE_LIMIT_DEFAULT),
                    },
                    "json": None,
                },
            ),
        ],
    )
    def test_from_stac_api_property_filter(
        self, dummy_stac_api, dummy_stac_api_server, use_filter_extension, expected_search
    ):
        given_url = f"{dummy_stac_api}/collections/custom-s2"
        collection: pystac.Collection = pystac.read_file(given_url)
        property_filter = PropertyFilter(
            properties={
                "flavor": {
                    "process_graph": {
                        "eq": {
                            "process_id": "eq",
                            "arguments": {"x": {"from_parameter": "value"}, "y": "banana"},
                            "result": True,
                        }
                    }
                }
            }
        )
        spatiotemporal_extent = SpatioTemporalExtent(bbox=None, from_date="2024-01-01", to_date="2025-01-01")
        item_collection = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=property_filter,
            spatiotemporal_extent=spatiotemporal_extent,
            use_filter_extension=use_filter_extension,
        )
        assert set(item.id for item in item_collection.items) == {
            "item-3-4",
            "item-4-3",
            "item-6-4",
            "item-7-3",
            "item-9-4",
            "item-10-3",
        }

        # Check search requests made to the STAC API server
        search_requests = [r for r in dummy_stac_api_server.request_history if r["path"] == "/search"]
        assert search_requests == [expected_search]

    def test_from_stac_api_antimeridian_handling(self, dummy_stac_api, dummy_stac_api_server):
        """Based on https://github.com/Open-EO/openeo-geopyspark-driver/issues/1568"""
        collection_id = "ogd-1568"
        dummy_stac_api_server.define_collection(collection_id)
        for x in [175, 176, 177, 178, 179, -180, -179, -178]:
            for y in [68, 69, 70, 71]:
                dummy_stac_api_server.define_item(
                    collection_id=collection_id,
                    item_id=f"item-{x}-{y}",
                    datetime=f"2025-09-01",
                    bbox=[x, y, x + 1, y + 1],
                )

        given_url = f"{dummy_stac_api}/collections/{collection_id}"
        collection: pystac.Collection = pystac.read_file(given_url)
        property_filter = PropertyFilter(properties={})
        bbox = BoundingBox(
            # Corresponds roughly in lon-lat to BoundingBox(west=177.9, south=69.2, east=-179.4, north=70.3)
            west=300000,
            south=7690200,
            east=409800,
            north=7800000,
            crs="EPSG:32601",
        )
        spatiotemporal_extent = SpatioTemporalExtent(bbox=bbox)
        item_collection = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=property_filter,
            spatiotemporal_extent=spatiotemporal_extent,
        )
        assert sorted(item.id for item in item_collection.items) == [
            "item--180-69",
            "item--180-70",
            "item-177-69",
            "item-177-70",
            "item-178-69",
            "item-178-70",
            "item-179-69",
            "item-179-70",
        ]

        search_requests = [r for r in dummy_stac_api_server.request_history if r["path"] == "/search"]
        assert search_requests == [
            dirty_equals.IsPartialDict(
                url_params={
                    "collections": collection_id,
                    "bbox": dirty_equals.IsStr(regex=r"177\.\d*,69\.\d*,180\.0,70\.\d+"),
                    "limit": "100",
                }
            ),
            dirty_equals.IsPartialDict(
                url_params={
                    "collections": collection_id,
                    "bbox": dirty_equals.IsStr(regex=r"-180\.0,69\.\d+,-179\.\d+,70\.\d+"),
                    "limit": "100",
                }
            ),
        ]

    @pytest.mark.parametrize(
        ["max_items", "expected_items"],
        [
            (1, ["item-1"]),
            (2, ["item-1", "item-2"]),
            (10, ["item-1", "item-2", "item-3"]),
        ],
    )
    def test_from_stac_api_bounded_iteration(self, dummy_stac_api, max_items, expected_items):
        given_url = f"{dummy_stac_api}/collections/collection-123"
        collection: pystac.Collection = pystac.read_file(given_url)
        item_collection = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=PropertyFilter(properties={}),
            spatiotemporal_extent=SpatioTemporalExtent(),
            max_items=max_items,
        )
        assert [item.id for item in item_collection.items] == expected_items

    def test_from_stac_api_asset_missing_href_is_skipped(self, requests_mock, caplog):
        """
        Regression test: STAC API responses where an asset has no 'href' field must not
        crash the item collection construction (KeyError: 'href' from pystac).
        The bad asset should be silently dropped (with a warning), while the rest of the
        item and all valid assets are kept.

        DummyStacApiServer cannot be used here because it also calls pystac.Item.from_dict()
        server-side and would crash the same way. Instead, the pystac Collection is built
        directly from dicts (bypassing urllib), and only the search endpoint is mocked via
        requests_mock so the bad JSON reaches the client code unchanged.
        """
        root_url = "https://stac.broken-test"
        collection_url = f"{root_url}/collections/broken-collection"

        catalog_dict = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "broken-test",
            "description": "broken test catalog",
            "links": [],
            "conformsTo": ["https://api.stacspec.org/v1.0.0-rc.1/item-search"],
        }
        requests_mock.get(root_url, json=catalog_dict)
        requests_mock.get(
            f"{root_url}/search",
            json={
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "stac_version": "1.0.0",
                        "id": "item-with-bad-asset",
                        "geometry": {"type": "Polygon", "coordinates": [[[3, 50], [5, 50], [5, 51], [3, 51], [3, 50]]]},
                        "bbox": [3, 50, 5, 51],
                        "properties": {"datetime": "2024-06-01T00:00:00Z"},
                        "links": [],
                        "assets": {
                            "good-asset": {"href": "/data/good.tif", "type": "image/tiff"},
                            "bad-asset": {"type": "image/tiff"},  # missing 'href' — triggers KeyError in pystac
                        },
                    }
                ],
                "links": [],
            },
        )

        # Build the pystac Collection directly (without making an HTTP call), because
        # pystac.read_file() uses urllib which is NOT intercepted by requests_mock.
        root_catalog = pystac.Catalog.from_dict(catalog_dict)
        root_catalog.set_self_href(root_url)
        collection: pystac.Collection = pystac.Collection.from_dict(
            {
                "type": "Collection",
                "stac_version": "1.0.0",
                "id": "broken-collection",
                "description": "collection with broken assets",
                "license": "unknown",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[None, None]]},
                },
                "links": [{"rel": "root", "href": root_url}],
            }
        )
        collection.set_self_href(collection_url)
        collection.set_root(root_catalog)
        with caplog.at_level(logging.WARNING, logger="openeogeotrellis"):
            item_collection = ItemCollection.from_stac_api(
                collection,
                original_url=collection_url,
                property_filter=PropertyFilter(properties={}),
                spatiotemporal_extent=SpatioTemporalExtent(),
            )

        # The item must be returned despite the bad asset
        assert len(item_collection.items) == 1
        assert item_collection.items[0].id == "item-with-bad-asset"

        # The bad asset must have been dropped, but the good one kept
        assets = item_collection.items[0].assets
        assert "good-asset" in assets
        assert "bad-asset" not in assets

        # A warning must have been logged mentioning the bad asset key
        assert any("bad-asset" in r.message for r in caplog.records)

    @pytest.mark.parametrize(
        ["stac_api_supports_property_filtering", "proj_epsg", "post_query_property_filtering", "expected"],
        [
            (True, 123, True, {"item-123"}),
            (True, 456, True, {"item-456"}),
            (False, 123, True, {"item-123"}),
            (True, 123, False, {"item-123"}),
            (False, 123, False, {"item-123", "item-456"}),
            (False, 123, ["something-else"], {"item-123", "item-456"}),
            (True, 123, ["something-else"], {"item-123"}),
            (False, 123, {"deny": "proj:epsg"}, {"item-123", "item-456"}),
            (True, 123, {"deny": "proj:epsg"}, {"item-123"}),
        ],
    )
    def test_from_stac_api_property_filter_api_vs_post_query_filtering(
        self,
        dummy_stac_api,
        dummy_stac_api_server,
        stac_api_supports_property_filtering,
        proj_epsg,
        post_query_property_filtering,
        expected,
    ):
        """
        Property filtering with properties that are possibly normalized by pystac (to newer version)
        """
        dummy_stac_api_server.support_property_filtering = stac_api_supports_property_filtering
        collection_id = "oldskool-s2"

        stac_extensions = [
            # We will use old style (v1.1) proj metadata: `"proj:epsg": 123` instead of proj-v2 style `"proj:code": "EPSG:123"`
            "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
        ]
        dummy_stac_api_server.define_collection(
            collection_id,
            extent={
                "spatial": {"bbox": [[3, 50, 5, 51]]},
                "temporal": {"interval": [["2024-02-01", "2024-03-01"]]},
            },
            stac_extensions=stac_extensions,
        )
        dummy_stac_api_server.define_item(
            collection_id=collection_id,
            item_id="item-123",
            datetime=f"2024-02-20T12:00:00Z",
            bbox=[3, 50, 5, 51],
            properties={"proj:epsg": 123},
            stac_extensions=stac_extensions,
        )
        dummy_stac_api_server.define_item(
            collection_id=collection_id,
            item_id="item-456",
            datetime=f"2024-02-20T12:00:00Z",
            bbox=[3, 50, 5, 51],
            properties={"proj:epsg": 456},
            stac_extensions=stac_extensions,
        )

        given_url = f"{dummy_stac_api}/collections/{collection_id}"
        collection: pystac.Collection = pystac.read_file(given_url)
        property_filter = PropertyFilter(
            properties={
                "proj:epsg": {
                    "process_graph": {
                        "eq": {
                            "process_id": "eq",
                            "arguments": {"x": {"from_parameter": "value"}, "y": proj_epsg},
                            "result": True,
                        }
                    }
                }
            }
        )
        spatiotemporal_extent = SpatioTemporalExtent(bbox=None, from_date="2024-01-01", to_date="2025-01-01")
        item_collection = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=property_filter,
            spatiotemporal_extent=spatiotemporal_extent,
            post_query_property_filtering=post_query_property_filtering,
        )
        assert set(item.id for item in item_collection.items) == expected

    def test_get_temporal_extent_empty(self):
        item_collection = ItemCollection(items=[])
        assert item_collection.get_temporal_extent() == (None, None)

    def test_get_temporal_extent_just_datetime(self):
        item_collection = ItemCollection(
            items=[
                pystac.Item.from_dict(StacDummyBuilder.item(datetime="2025-11-11T00:00:00Z")),
                pystac.Item.from_dict(StacDummyBuilder.item(datetime="2025-11-12T00:00:00Z")),
            ]
        )

        assert item_collection.get_temporal_extent() == (
            datetime.datetime(2025, 11, 11, tzinfo=datetime.timezone.utc),
            datetime.datetime(2025, 11, 12, tzinfo=datetime.timezone.utc),
        )

    def test_get_temporal_extent_start_and_end(self):
        item_collection = ItemCollection(
            items=[
                pystac.Item.from_dict(
                    StacDummyBuilder.item(
                        datetime="2025-11-11T00:00:00Z",
                        properties={
                            "start_datetime": "2025-11-10T10:00:00Z",
                            "end_datetime": "2025-11-12T12:00:00Z",
                        },
                    )
                ),
                pystac.Item.from_dict(
                    StacDummyBuilder.item(
                        datetime="2025-11-15T00:00:00Z",
                        properties={
                            "start_datetime": "2025-11-14T14:00:00Z",
                            "end_datetime": "2025-11-16T16:00:00Z",
                        },
                    )
                ),
            ]
        )

        assert item_collection.get_temporal_extent() == (
            datetime.datetime(2025, 11, 10, hour=10, tzinfo=datetime.timezone.utc),
            datetime.datetime(2025, 11, 16, hour=16, tzinfo=datetime.timezone.utc),
        )

    def test_deduplicated(self, dummy_stac_api_server, dummy_stac_api):
        collection_id = "with-dups"
        dummy_stac_api_server.define_collection(collection_id)
        for i in range(6):
            dummy_stac_api_server.define_item(
                collection_id=collection_id,
                item_id=f"item-{i}",
                datetime=f"2025-11-{(i // 2) + 1:02d}",
            )

        given_url = f"{dummy_stac_api}/collections/{collection_id}"
        collection: pystac.Collection = pystac.read_file(given_url)
        orig = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=PropertyFilter(properties={}),
            spatiotemporal_extent=SpatioTemporalExtent(bbox=None, from_date="2025-01-01", to_date=None),
        )
        assert set(item.id for item in orig.items) == {
            "item-0",
            "item-1",
            "item-2",
            "item-3",
            "item-4",
            "item-5",
        }

        deduplicator = ItemDeduplicator()
        deduped = orig.deduplicated(deduplicator=deduplicator)
        assert set(item.id for item in deduped.items) == {
            "item-1",
            "item-3",
            "item-5",
        }

    def test_serialization_basic(self, tmp_path):
        item = pystac.Item.from_dict(StacDummyBuilder.item())
        spatiotemporal_extent = SpatioTemporalExtent()
        orig = ItemCollection.from_stac_item(item, spatiotemporal_extent=spatiotemporal_extent)

        # Serialize to file
        dump_path = tmp_path / "item_collection.json"
        orig.to_file(dump_path)

        # Deserialize again
        loaded = ItemCollection.from_file(dump_path)
        assert [i.to_dict() for i in loaded.items] == [item.to_dict()]

    def test_serialization_with_stac_api(self, dummy_stac_api, tmp_path):
        given_url = f"{dummy_stac_api}/collections/collection-123"
        collection: pystac.Collection = pystac.read_file(given_url)
        orig = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=PropertyFilter(properties={}),
            spatiotemporal_extent=SpatioTemporalExtent(),
        )

        # Serialize to file
        dump_path = tmp_path / "item_collection.json"
        orig.to_file(dump_path)

        # Deserialize again
        loaded = ItemCollection.from_file(dump_path)
        assert [i.to_dict() for i in loaded.items] == [i.to_dict() for i in orig.items]


def test_construct_item_collection_minimal(dummy_stac_api):
    url = f"{dummy_stac_api}/collections/collection-123"
    stac_source = construct_item_collection(url=url)
    assert set(item.id for item in stac_source.item_collection.items) == {"item-1", "item-2", "item-3"}
    assert stac_source.band_names == []
    # TODO deeper tests that also involve various band metadata detection aspects


@pytest.mark.parametrize(
    ["assets", "expected"],
    [
        (
            {
                "asset1": {"href": "https://stac.test/asset1.tiff"},
                "asset2": {"href": "https://stac.test/asset2.tiff"},
            },
            {"asset1", "asset2"},
        ),
        (
            {
                "asset1": {"look ma": "no href"},
                "asset2": {"href": "https://stac.test/asset2.tiff"},
            },
            {"asset2"},
        ),
    ],
)
def test_pystac_item_from_dict_lenient(assets, expected):
    item = _pystac_item_from_dict_lenient(StacDummyBuilder.item(assets=assets))
    assert isinstance(item, pystac.Item)
    assert set(item.assets.keys()) == expected


@pytest.mark.parametrize(
    ["catalog", "expected"],
    [
        (None, False),
        (
            pystac.Catalog(
                id="catalog123",
                description="Test Catalog",
                extra_fields={"conformsTo": ["https://api.stacspec.org/v1.0.0/item-search"]},
            ),
            True,
        ),
    ],
)
def test_supports_item_search(tmp_path, catalog, expected):
    links = []
    if catalog:
        catalog_path = tmp_path / "catalog.json"
        pystac.write_file(catalog, dest_href=catalog_path)
        links.append({"rel": "root", "href": str(catalog_path)})

    collection = pystac.Collection.from_dict(StacDummyBuilder.collection(links=links))
    assert _supports_item_search(collection) == expected
