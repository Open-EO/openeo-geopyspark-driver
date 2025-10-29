import datetime

import dirty_equals
import pystac_client
import pystac_client.exceptions
import pytest
import requests
from kazoo.exceptions import BadVersionError, NoNodeError

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.testing import (
    DummyCubeBuilder,
    DummyStacApiServer,
    KazooClientMock,
    _ZNodeStat,
    gps_config_overrides,
)


def test_kazoo_mock_basic():
    client = KazooClientMock()
    assert client.dump() == {'/': b''}


def test_kazoo_mock_create_simple():
    client = KazooClientMock()
    client.create('/foo', b'd6t6')
    assert client.dump() == {
        '/': b'',
        '/foo': b'd6t6'
    }


def test_kazoo_mock_create_multiple():
    client = KazooClientMock()
    client.create('/foo', b'd6t6')
    client.create('/bar/baz', b'b6r', makepath=True)
    assert client.dump() == {
        '/': b'',
        '/foo': b'd6t6',
        '/bar': b'',
        '/bar/baz': b'b6r',
    }


def test_kazoo_mock_get():
    client = KazooClientMock()
    client.create('/foo', b'd6t6')
    client.create('/bar/baz', b'b6r', makepath=True)
    assert client.get('/foo') == (b'd6t6', _ZNodeStat(1))
    assert client.get('/bar') == (b'', _ZNodeStat(1))
    assert client.get('/bar/baz') == (b'b6r', _ZNodeStat(1))


def test_kazoo_mock_set():
    client = KazooClientMock()
    client.create('/bar/baz', b'b6r', makepath=True)
    assert client.get('/bar/baz') == (b'b6r', _ZNodeStat(1))
    client.set('/bar/baz', b'x3v')
    assert client.get('/bar/baz') == (b'x3v', _ZNodeStat(2))
    client.set('/bar/baz', b'l0l', version=2)
    assert client.get('/bar/baz') == (b'l0l', _ZNodeStat(3))
    with pytest.raises(BadVersionError):
        client.set('/bar/baz', b'l0l', version=2)


def test_kazoo_mock_delete():
    client = KazooClientMock()
    client.create('/bar/baz', b'b6r', makepath=True)
    assert client.get('/bar/baz') == (b'b6r', _ZNodeStat(1))
    client.delete('/bar/baz')
    with pytest.raises(NoNodeError):
        client.get('/bar/baz')


def test_kazoo_mock_children():
    client = KazooClientMock()
    client.create('/bar/baz', b'b6r', makepath=True)
    client.create('/bar/fii', b'f000', makepath=True)
    assert client.get_children('/') == ['bar']
    assert client.get_children('/bar') == ['baz', 'fii']
    assert client.get_children('/bar/fii') == []


class TestGpsConfigOverrides:
    def test_baseline(self):
        assert get_backend_config().id == "gps-test-dummy"

    def test_context(self):
        assert get_backend_config().id == "gps-test-dummy"
        with gps_config_overrides(id="hello-inline-context"):
            assert get_backend_config().id == "hello-inline-context"
        assert get_backend_config().id == "gps-test-dummy"

    def test_context_nesting(self):
        assert get_backend_config().id == "gps-test-dummy"
        with gps_config_overrides(id="hello-inline-context"):
            assert get_backend_config().id == "hello-inline-context"
            with gps_config_overrides(id="hello-again"):
                assert get_backend_config().id == "hello-again"
            assert get_backend_config().id == "hello-inline-context"
        assert get_backend_config().id == "gps-test-dummy"

    @pytest.fixture
    def special_stuff(self):
        with gps_config_overrides(id="hello-fixture"):
            yield

    def test_fixture(self, special_stuff):
        assert get_backend_config().id == "hello-fixture"

    def test_fixture_and_context(self, special_stuff):
        assert get_backend_config().id == "hello-fixture"
        with gps_config_overrides(id="hello-inline-context"):
            assert get_backend_config().id == "hello-inline-context"
        assert get_backend_config().id == "hello-fixture"

    @gps_config_overrides(id="hello-decorator")
    def test_decorator(self):
        assert get_backend_config().id == "hello-decorator"

    @gps_config_overrides(id="hello-decorator")
    def test_decorator_and_context(self):
        assert get_backend_config().id == "hello-decorator"
        with gps_config_overrides(id="hello-inline-context"):
            assert get_backend_config().id == "hello-inline-context"
        assert get_backend_config().id == "hello-decorator"

    @gps_config_overrides(id="hello-decorator")
    def test_decorator_vs_fixture(self, special_stuff):
        assert get_backend_config().id == "hello-decorator"


class TestDummyCubeBuilder:
    def test_to_datetime_list_int(self):
        builder = DummyCubeBuilder()
        assert builder.to_datetime_list(0) == []
        assert builder.to_datetime_list(1) == [
            datetime.datetime(2025, 9, 1, tzinfo=datetime.timezone.utc),
        ]
        assert builder.to_datetime_list(3) == [
            datetime.datetime(2025, 9, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime(2025, 9, 2, tzinfo=datetime.timezone.utc),
            datetime.datetime(2025, 9, 3, tzinfo=datetime.timezone.utc),
        ]

    def test_to_datetime_list_list(self):
        builder = DummyCubeBuilder()
        assert builder.to_datetime_list([]) == []
        assert builder.to_datetime_list(["2021-02-03", "2021-02-12"]) == [
            datetime.datetime(2021, 2, 3, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 2, 12, tzinfo=datetime.timezone.utc),
        ]
        assert builder.to_datetime_list(
            [
                datetime.datetime(2022, 3, 4),
                datetime.date(2022, 4, 5),
            ]
        ) == [
            datetime.datetime(2022, 3, 4, tzinfo=datetime.timezone.utc),
            datetime.datetime(2022, 4, 5, tzinfo=datetime.timezone.utc),
        ]


class TestDummyStacApiServer:
    def test_basic(self):
        with DummyStacApiServer().serve() as root_url:
            response = requests.get(root_url)
        assert response.status_code == 200
        assert response.json() == dirty_equals.IsPartialDict(
            {
                "id": "stac-api-123",
                "stac_version": "1.0.0",
                "type": "Catalog",
                "links": [
                    {"rel": "root", "href": f"{root_url}/"},
                    {"rel": "search", "href": f"{root_url}/search"},
                    {"rel": "self", "href": f"{root_url}/"},
                ],
            }
        )

    def test_get_collections_raw(self):
        with DummyStacApiServer().serve() as root_url:
            response = requests.get(f"{root_url}/collections")
        assert response.status_code == 200
        assert response.json() == dirty_equals.IsPartialDict(
            {
                "collections": [
                    dirty_equals.IsPartialDict(
                        {
                            "id": "collection-123",
                            "stac_version": "1.0.0",
                            "type": "Collection",
                            "links": [
                                {"rel": "root", "href": f"{root_url}/"},
                                {"rel": "self", "href": f"{root_url}/collections/collection-123"},
                            ],
                        }
                    ),
                ],
                "links": [
                    {"rel": "root", "href": f"{root_url}/"},
                    {"rel": "self", "href": f"{root_url}/collections"},
                ],
            }
        )

    def test_get_collections_pystac(self):
        with DummyStacApiServer().serve() as root_url:
            client = pystac_client.Client.open(root_url)
            collections = list(client.get_collections())
            assert [c.id for c in collections] == ["collection-123"]

    def test_get_collection_raw(self):
        with DummyStacApiServer().serve() as root_url:
            response = requests.get(f"{root_url}/collections/collection-123")
        assert response.status_code == 200
        assert response.json() == dirty_equals.IsPartialDict(
            {
                "id": "collection-123",
                "stac_version": "1.0.0",
                "type": "Collection",
                "links": [
                    {"rel": "root", "href": f"{root_url}/"},
                    {"rel": "self", "href": f"{root_url}/collections/collection-123"},
                ],
            }
        )

    def test_get_collection_pystac(self):
        with DummyStacApiServer().serve() as root_url:
            client = pystac_client.Client.open(root_url)
            collection = client.get_collection("collection-123")
            assert isinstance(collection, pystac_client.CollectionClient)
            assert collection.id == "collection-123"

    def test_get_collection_pystac_not_found(self):
        with DummyStacApiServer().serve() as root_url:
            client = pystac_client.Client.open(root_url)
            with pytest.raises(pystac_client.exceptions.APIError):
                _ = client.get_collection("collection-nope")

    @pytest.mark.parametrize("method", ["GET", "POST"])
    def test_item_search_basic(self, method):
        with DummyStacApiServer().serve() as root_url:
            client = pystac_client.Client.open(root_url)
            result = client.search(method=method, collections=["collection-123"])
            items = list(result.items())

        assert [item.id for item in items] == ["item-1", "item-2", "item-3"]

    @pytest.mark.parametrize("method", ["GET", "POST"])
    def test_item_search_item_metadata(self, method):
        with DummyStacApiServer().serve() as root_url:
            client = pystac_client.Client.open(root_url)
            result = client.search(method=method, collections=["collection-123"])
            items = list(result.items())

        assert [(item.id, item.datetime, item.bbox, item.properties) for item in items] == [
            (
                "item-1",
                datetime.datetime(2024, 5, 1, tzinfo=datetime.timezone.utc),
                [2, 49, 3, 50],
                {"datetime": "2024-05-01T00:00:00Z", "flavor": "apple"},
            ),
            (
                "item-2",
                datetime.datetime(2024, 6, 2, tzinfo=datetime.timezone.utc),
                [3, 50, 5, 51],
                {"datetime": "2024-06-02T00:00:00Z", "flavor": "banana"},
            ),
            (
                "item-3",
                datetime.datetime(2024, 7, 3, tzinfo=datetime.timezone.utc),
                [4, 51, 7, 52],
                {"datetime": "2024-07-03T00:00:00Z", "flavor": "coconut"},
            ),
        ]

    @pytest.mark.parametrize("method", ["GET", "POST"])
    @pytest.mark.parametrize(
        ["limit", "expected_items"],
        [
            (1, ["item-1"]),
            (2, ["item-1", "item-2"]),
            (3, ["item-1", "item-2", "item-3"]),
            (10, ["item-1", "item-2", "item-3"]),
        ],
    )
    def test_item_search_limit(self, method, limit, expected_items):
        with DummyStacApiServer().serve() as root_url:
            client = pystac_client.Client.open(root_url)
            result = client.search(method=method, collections=["collection-123"], limit=limit)
            items = list(result.items())

        assert [item.id for item in items] == expected_items

    @pytest.mark.parametrize("method", ["GET", "POST"])
    @pytest.mark.parametrize(
        ["search_datetime", "expected_items"],
        [
            (("2024-06-01", None), ["item-2", "item-3"]),
            ((None, "2024-07-01"), ["item-1", "item-2"]),
            (("2024-05-20", "2024-07-01"), ["item-2"]),
        ],
    )
    def test_item_search_datetime(self, method, search_datetime, expected_items):
        with DummyStacApiServer().serve() as root_url:
            client = pystac_client.Client.open(root_url)
            result = client.search(method=method, collections=["collection-123"], datetime=search_datetime)
            items = list(result.items())

        assert [item.id for item in items] == expected_items

    @pytest.mark.parametrize("method", ["GET", "POST"])
    @pytest.mark.parametrize(
        ["bbox", "expected_items"],
        [
            ([2.5, 48, 3.5, 49.5], ["item-1"]),
            ([4.5, 49.9, 4.6, 51.1], ["item-2", "item-3"]),
            ([77, 4, 88, 6], []),
        ],
    )
    def test_item_search_bbox(self, method, bbox, expected_items):
        with DummyStacApiServer().serve() as root_url:
            client = pystac_client.Client.open(root_url)
            result = client.search(method=method, collections=["collection-123"], bbox=bbox)
            items = list(result.items())

        assert [item.id for item in items] == expected_items

    @pytest.mark.parametrize(
        ["method", "filter", "expected_items"],
        [
            # GET with CQL2-text
            ("GET", "\"properties.flavor\" = 'banana'", ["item-2"]),
            ("GET", "\"properties.flavor\" = 'coconut'", ["item-3"]),
            ("GET", "\"properties.topping\" = 'chocolate'", []),
            # POST with CQL2-JSON
            ("POST", {"op": "=", "args": [{"property": "properties.flavor"}, "apple"]}, ["item-1"]),
            ("POST", {"op": "=", "args": [{"property": "properties.flavor"}, "banana"]}, ["item-2"]),
            ("POST", {"op": "=", "args": [{"property": "properties.topping"}, "chocolate"]}, []),
        ],
    )
    def test_item_search_filter_get_cql2_text(self, method, filter, expected_items):
        with DummyStacApiServer().serve() as root_url:
            client = pystac_client.Client.open(root_url)
            result = client.search(
                method=method,
                collections=["collection-123"],
                filter=filter,
            )
            items = list(result.items())

        assert [item.id for item in items] == expected_items
