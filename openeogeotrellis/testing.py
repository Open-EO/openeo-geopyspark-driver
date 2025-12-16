"""
Reusable helpers, functions, classes, fixtures for testing purposes
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import json
import rasterio
import re
import subprocess
import uuid
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Tuple, Union
from unittest import mock

import flask
import geopyspark.geotrellis
import geopyspark.geotrellis.constants
import geopyspark.geotrellis.layer
import kazoo
import kazoo.exceptions
import numpy
import openeo_driver.testing
import pyspark
import pystac
import pytest
import shapely
import werkzeug.exceptions

from openeo.testing.stac import StacDummyBuilder
from openeo_driver.testing import ephemeral_flask_server, ApiResponse
from openeo_driver.util.geometry import BoundingBox
from openeo_driver.util.utm import is_utm_crs

import openeogeotrellis
from openeogeotrellis.config import gps_config_getter
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.util.datetime import to_datetime_utc
from openeogeotrellis.util.geometry import to_geojson_io_url
from openeogeotrellis.util.runtime import is_package_available


def random_name(prefix: str = "") -> str:
    """Generate random name (with given prefix)"""
    return "{p}{r}".format(p=prefix, r=uuid.uuid4().hex[:8])


class _ZNodeStat:
    def __init__(self, version: int = 1):
        self._version = version

    @property
    def version(self):
        return self._version

    def bump_version(self):
        self._version += 1

    def __repr__(self):
        return "{c}({v})".format(c=self.__class__.__name__, v=self.version)

    def __eq__(self, other):
        return isinstance(other, _ZNodeStat) and (self.version,) == (other.version,)


class _ZNode:
    """Internal 'znode' container"""

    def __init__(self, value: bytes = b"", children: dict = None):
        self.value = value
        self.children = children or {}
        self.stat = _ZNodeStat(version=1)

    def assert_version(self, version=-1) -> "_ZNode":
        if version not in [-1, self.stat.version]:
            raise kazoo.exceptions.BadVersionError
        return self

    def dump(self, root=None) -> Iterator[Tuple[str, bytes]]:
        """Dump as iterator of (path, value) tuples for inspection"""
        root = Path(root or "/")
        yield str(root), self.value
        for name, child in self.children.items():
            yield from child.dump(root=root / name)


class KazooClientMock:
    """Simple mock for KazooClient that stores data in memory"""

    # TODO: unify (for better reuse/sharing) with DummyKazooClient from https://github.com/Open-EO/openeo-aggregator/blob/720fc26311c9a377cd45dfb2dc6b81616adb5850/src/openeo_aggregator/testing.py#L17

    def __init__(self, root: _ZNode = None):
        """Create client and optionally initialize state of root node (and its children)."""
        self.root = root or _ZNode()

    def start(self):
        pass

    def stop(self):
        pass

    def close(self):
        pass

    @staticmethod
    def _parse_path(path: Union[str, Path]) -> Tuple[str]:
        path = Path(path)
        assert path.is_absolute()
        return path.parts[1:]

    def _get(self, path: Union[str, Path]) -> _ZNode:
        cursor = self.root
        for part in self._parse_path(path):
            try:
                cursor = cursor.children[part]
            except KeyError:
                raise kazoo.exceptions.NoNodeError
        return cursor

    def ensure_path(self, path: Union[str, Path]) -> _ZNode:
        cursor = self.root
        for part in self._parse_path(path):
            if part not in cursor.children:
                cursor.children[part] = _ZNode()
            cursor = cursor.children[part]
        return cursor

    def get_children(self, path: Union[str, Path]) -> List[str]:
        return list(self._get(path).children.keys())

    def create(self, path: Union[str, Path], value: bytes, makepath=False):
        path = Path(path)
        if makepath:
            parent = self.ensure_path(path.parent)
        else:
            parent = self._get(path.parent)

        if path.name in parent.children:
            raise kazoo.exceptions.NodeExistsError
        parent.children[path.name] = _ZNode(value)

    def get(self, path: Union[str, Path]) -> Tuple[bytes, _ZNodeStat]:
        znode = self._get(path)
        return znode.value, znode.stat

    def set(self, path: Union[str, Path], value: bytes, version: int = -1):
        znode = self._get(path).assert_version(version)
        znode.value = value
        znode.stat.bump_version()

    def delete(self, path: Union[str, Path], version: int = -1):
        path = Path(path)
        znode = self._get(path).assert_version(version)
        if znode is self.root:
            # Special case: wipe everything, start over.
            self.root = _ZNode()
        else:
            parent = self._get(path.parent)
            del parent.children[path.name]

    def dump(self) -> Dict[str, bytes]:
        """Dump ZooKeeper data for inspection"""
        return dict(self.root.dump())

    def get_json_decoded(self, path: Union[str, Path]) -> dict:
        raw, _ = self.get(path=path)
        return json.loads(raw.decode("utf-8"))


def gps_config_overrides(**kwargs):
    """
    *Only to be used in unit tests*

    Override config fields in the config returned by `get_backend_config()` at run time

    Can be used as context manager

        >>> with gps_config_overrides(id="foobar"):
        ...     ...

    in a pytest fixture (as context manager):

        >>> @pytest.fixture
        ... def custom_setup()
        ...     with gps_config_overrides(id="foobar"):
        ...         yield

    or as test function decorator

        >>> @gps_config_overrides(id="foobar")
        ... def test_stuff():
    """
    return openeo_driver.testing.config_overrides(config_getter=gps_config_getter, **kwargs)


def skip_if_package_not_available(package: str):
    """Decorator to skip a test if given package is not available."""
    return pytest.mark.skipif(
        condition=not is_package_available(package),
        reason=f"This test depends on the presence of package {package!r}",
    )


class DummyCubeBuilder:
    """
    Helper to build simple GeopysparkDataCube instances for testing purposes.
    """

    def __init__(self, *, fallback_start_date: Union[str, datetime.datetime, None] = None):
        self.fallback_start_date = to_datetime_utc(fallback_start_date or "2025-09-01")
        # TODO: more fallback options

    def to_datetime_list(self, dates: Union[int, list]) -> List[datetime.datetime]:
        """Helper to construct date list from various inputs"""
        if isinstance(dates, int):
            dates = [self.fallback_start_date + datetime.timedelta(days=d) for d in range(dates)]
        elif isinstance(dates, tuple) and 2 <= len(dates) <= 3:
            start = to_datetime_utc(dates[0])
            end = to_datetime_utc(dates[1])
            step = dates[2] if len(dates) == 3 else 1
            dates = []
            current = start
            while current <= end:
                dates.append(current)
                current += datetime.timedelta(days=step)

        assert isinstance(dates, list)
        return [to_datetime_utc(d) for d in dates]

    def build_cube(
        self,
        *,
        layout_cols: int = 2,
        layout_rows: int = 2,
        tile_cols: int = 32,
        tile_rows: int = 32,
        dates: Union[int, List[Union[str, datetime.date, datetime.datetime]]] = 3,
        extent: Union[geopyspark.geotrellis.Extent, dict, None] = None,
    ) -> GeopysparkDataCube:
        dates: List[datetime.datetime] = self.to_datetime_list(dates)

        # TODO: option to customize tile building
        def build_tile(col: int, row: int, date: datetime.datetime) -> geopyspark.geotrellis.Tile:
            # TODO: first cols or rows in shape?
            numpy_array = numpy.ones(shape=(tile_cols, tile_rows), dtype="uint8")
            return geopyspark.geotrellis.Tile.from_numpy_array(numpy_array, no_data_value=255)

        rdd_data = [
            (
                geopyspark.geotrellis.SpaceTimeKey(col=c, row=r, instant=d),
                build_tile(col=c, row=r, date=d),
            )
            for c in range(0, layout_cols)
            for r in range(0, layout_rows)
            for d in dates
        ]
        rdd = pyspark.SparkContext.getOrCreate().parallelize(rdd_data)

        # TODO: customize extent?
        extent = extent or {"xmin": 1.0, "xmax": 2.0, "ymin": 3.0, "ymax": 4.0}
        if isinstance(extent, dict):
            extent = geopyspark.geotrellis.Extent(**extent)

        metadata = geopyspark.geotrellis.Metadata(
            bounds=geopyspark.geotrellis.Bounds(
                minKey=geopyspark.geotrellis.SpaceTimeKey(col=0, row=0, instant=min(dates)),
                maxKey=geopyspark.geotrellis.SpaceTimeKey(col=layout_cols - 1, row=layout_rows - 1, instant=max(dates)),
            ),
            # TODO: customize crs?
            crs="+proj=longlat +datum=WGS84 +no_defs ",
            # TODO: customize cell_type? Or autodetect from tile building?
            cell_type="float32ud-1.0",
            extent=extent,
            layout_definition=geopyspark.geotrellis.LayoutDefinition(
                extent=extent,
                tileLayout=geopyspark.geotrellis.TileLayout(
                    layoutCols=layout_cols, layoutRows=layout_rows, tileCols=tile_cols, tileRows=tile_cols
                ),
            ),
        )

        # TODO: support multiple pyramid levels?
        pyramid = geopyspark.geotrellis.layer.Pyramid(
            {
                0: geopyspark.geotrellis.layer.TiledRasterLayer.from_numpy_rdd(
                    layer_type=geopyspark.geotrellis.constants.LayerType.SPACETIME,
                    numpy_rdd=rdd,
                    metadata=metadata,
                )
            }
        )
        # TODO: customize GeopysparkCubeMetadata
        return GeopysparkDataCube(pyramid=pyramid)


class YarnMocker:
    """Test utility to mock YARN submit and related"""

    _YARN_SUBMIT_STDOUT_BASIC = """
    20/04/20 16:48:33 INFO YarnClientImpl: Submitted application application_1587387643572_0842
    20/04/20 16:48:33 INFO Client: Application report for application_1587387643572_0842 (state: ACCEPTED)
    """

    @dataclasses.dataclass
    class _SubprocessRunArgs:
        command: Union[List[str], None] = None
        kwargs: Optional[dict] = None
        env: Optional[dict] = None

    @contextlib.contextmanager
    def mock_yarn_submit_job(self, yarn_submit_stdout: str = _YARN_SUBMIT_STDOUT_BASIC) -> Iterator[_SubprocessRunArgs]:
        """
        Test utility to mock and inspect the `subprocess.run` call used to submit a YARN job.

        Usage:
            with yarn_mocker.mock_yarn_submit_job() as yarn_submit_call:
                runner.run_job(...)

            # after the block, you can inspect:
            yarn_submit_call.command  # List[str] - the "submit_batch_job_spark3.sh" command used
            yarn_submit_call.kwargs   # dict - other kwargs passed to subprocess.run
            yarn_submit_call.env      # dict - the environment variables used
        """
        with mock.patch("subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=yarn_submit_stdout, stderr="")
            result = self._SubprocessRunArgs()
            yield result
            run.assert_called_once()
            result.command = run.call_args.args[0]
            result.kwargs = run.call_args.kwargs
            result.env = run.call_args.kwargs["env"]


class DummyStacApiServer:
    """
    Dummy STAC API server for testing purposes.

    Powered by a Flask app serving a minimal STAC API implementation,
    that can be run as an ephemeral server in a thread for the duration of a test.
    This allows making real HTTP requests
    and eliminates the need to mock HTTP calls,
    which is complicated as pystac and pystac_client build on different HTTP libraries.

    Usage example:

        dummy_stac_api = DummyStacApiServer()
        # Optionally define collections and items
        dummy_stac_api.define_collection(id="my-collection")
        dummy_stac_api.define_item(collection_id="my-collection", item_id="item-1")

        with dummy_stac_api.serve() as root_url:
            response = requests.get(f"{root_url}/...")
            # Or use pystac_client to access the dummy STAC API
            client = pystac_client.Client.open(root_url)
            ...
    """

    DEFAULT_BBOX = [2, 49, 7, 52]
    DEFAULT_TEMPORAL_INTERVAL = ["2024-02-01", "2024-11-30"]

    @dataclasses.dataclass
    class _CollectionData:
        collection: dict
        items: List[dict]

    def __init__(self):
        # Mapping of collection id -> (collection dict, list of item dicts)
        self._collections: Dict[str, DummyStacApiServer._CollectionData] = {}

        self.request_history: List[dict] = []

        # TODO: don't do this here, but move to a fixture?
        self.default_setup()

    def default_setup(self):
        """Predefine a default collection with items"""
        # TODO: use real HTTP for asset hrefs intead of local file paths

        # Set up collection-123:
        # see coverage at https://geojson.io/#data=data:application/json,%7B%22type%22%3A%20%22FeatureCollection%22%2C%20%22features%22%3A%20%5B%7B%22type%22%3A%20%22Feature%22%2C%20%22properties%22%3A%20%7B%7D%2C%20%22geometry%22%3A%20%7B%22type%22%3A%20%22Polygon%22%2C%20%22coordinates%22%3A%20%5B%5B%5B3.0%2C%2049.0%5D%2C%20%5B3.0%2C%2050.0%5D%2C%20%5B2.0%2C%2050.0%5D%2C%20%5B2.0%2C%2049.0%5D%2C%20%5B3.0%2C%2049.0%5D%5D%5D%7D%7D%2C%20%7B%22type%22%3A%20%22Feature%22%2C%20%22properties%22%3A%20%7B%7D%2C%20%22geometry%22%3A%20%7B%22type%22%3A%20%22Polygon%22%2C%20%22coordinates%22%3A%20%5B%5B%5B5.0%2C%2050.0%5D%2C%20%5B5.0%2C%2051.0%5D%2C%20%5B3.0%2C%2051.0%5D%2C%20%5B3.0%2C%2050.0%5D%2C%20%5B5.0%2C%2050.0%5D%5D%5D%7D%7D%2C%20%7B%22type%22%3A%20%22Feature%22%2C%20%22properties%22%3A%20%7B%7D%2C%20%22geometry%22%3A%20%7B%22type%22%3A%20%22Polygon%22%2C%20%22coordinates%22%3A%20%5B%5B%5B7.0%2C%2051.0%5D%2C%20%5B7.0%2C%2052.0%5D%2C%20%5B4.0%2C%2052.0%5D%2C%20%5B4.0%2C%2051.0%5D%2C%20%5B7.0%2C%2051.0%5D%5D%5D%7D%7D%5D%7D
        # (generated with `openeogeotrellis.testing.DummyStacApiServer().collection_as_geojson_io_url("collection-123")`)
        asset_root = Path(openeogeotrellis.__file__).parent.parent / "tests" / "data" / "dummy-stac" / "collection-123"
        self.define_collection(id="collection-123")
        self.define_item(
            collection_id="collection-123",
            item_id="item-1",
            datetime="2024-05-01",
            bbox=[2, 49, 3, 50],
            properties={"flavor": "apple"},
            assets={
                "asset-1": StacDummyBuilder.asset(
                    href=str(asset_root / "asset-1.tiff"),
                    roles=["data"],
                    proj_code="EPSG:4326",
                    proj_bbox=[2, 49, 3, 50],
                    proj_shape=[32, 1 * 32],
                )
            },
        )
        self.define_item(
            collection_id="collection-123",
            item_id="item-2",
            datetime="2024-06-02",
            bbox=[3, 50, 5, 51],
            properties={"flavor": "banana"},
            assets={
                "asset-2": StacDummyBuilder.asset(
                    href=str(asset_root / "asset-2.tiff"),
                    roles=["data"],
                    proj_code="EPSG:4326",
                    proj_bbox=[3, 50, 5, 51],
                    proj_shape=[32, 2 * 32],
                )
            },
        )
        self.define_item(
            collection_id="collection-123",
            item_id="item-3",
            datetime="2024-07-03",
            bbox=[4, 51, 7, 52],
            properties={"flavor": "coconut"},
            assets={
                "asset-2": StacDummyBuilder.asset(
                    href=str(asset_root / "asset-3.tiff"),
                    roles=["data"],
                    proj_code="EPSG:4326",
                    proj_bbox=[4, 51, 7, 52],
                    proj_shape=[32, 3 * 32],
                )
            },
        )

    def define_collection(self, id: str, *, extent: Optional[dict] = None, **kwargs):
        """Define a new collection in the dummy STAC API server."""
        assert id not in self._collections
        if extent is None:
            extent = {
                "spatial": {"bbox": [self.DEFAULT_BBOX]},
                "temporal": {"interval": [self.DEFAULT_TEMPORAL_INTERVAL]},
            }
        self._collections[id] = DummyStacApiServer._CollectionData(
            collection=StacDummyBuilder.collection(id=id, extent=extent, **kwargs),
            items=[],
        )

    def define_item(self, collection_id: str, item_id: str, **kwargs):
        assert collection_id in self._collections
        item = StacDummyBuilder.item(id=item_id, **kwargs)
        self._collections[collection_id].items.append(item)

    def _build_flask_app(self) -> flask.Flask:
        app = flask.Flask(__name__)

        @app.before_request
        def track_request():
            self.request_history.append(
                {
                    "method": flask.request.method,
                    "path": flask.request.path,
                    "url_params": flask.request.args.to_dict(),
                    "json": flask.request.get_json(silent=True),
                }
            )

        # Use JSON formatted errors to eliminate the unnecessary verbosity of default HTML error pages
        @app.errorhandler(Exception)
        def error(e):
            app.logger.exception(f"Exception on {flask.request.method} {flask.request.path}: {e!r}")
            body = {"exception": repr(e), "url": flask.request.url}
            http_code = 500
            if isinstance(e, werkzeug.exceptions.HTTPException):
                http_code = e.code
                body["description"] = e.description
            return flask.jsonify(body), http_code

        def link(rel: str, endpoint: str, *, method: Optional[str] = None, **kwargs) -> dict:
            """Helper to build a link dict"""
            link = {
                "rel": rel,
                "href": flask.url_for(
                    endpoint,
                    **kwargs,
                    _external=True,
                ),
            }
            if method:
                link["method"] = method
            return link

        def with_links(data: dict, *, links: List[dict]) -> dict:
            """Helper to add links to the root of a dict"""
            data = data.copy()
            if "links" not in data:
                data["links"] = []
            data["links"].extend(links)
            # And sort to make outcome predictable
            data["links"] = sorted(data["links"], key=lambda link: link.get("rel", ""))
            return data

        @app.route("/", methods=["GET"])
        def get_index():
            root_catalog = StacDummyBuilder.catalog(id="stac-api-123", stac_version="1.0.0")
            root_catalog["conformsTo"] = [
                "https://api.stacspec.org/v1.0.0/collections",
                "https://api.stacspec.org/v1.0.0/item-search",
                "https://api.stacspec.org/v1.0.0/item-search#filter",
                "http://www.opengis.net/spec/cql2/1.0/conf/basic-cql2",
                "http://www.opengis.net/spec/cql2/1.0/conf/cql2-json",
                "http://www.opengis.net/spec/cql2/1.0/conf/cql2-text",
            ]
            root_catalog = with_links(
                root_catalog,
                links=[
                    link(rel="root", endpoint="get_index"),
                    link(rel="self", endpoint="get_index"),
                    link(rel="search", endpoint="get_search", method="GET"),
                    link(rel="search", endpoint="get_search", method="POST"),
                ],
            )
            return flask.jsonify(root_catalog)

        @app.route("/collections", methods=["GET"])
        def get_collections():
            response = {
                "collections": [
                    with_links(
                        data.collection,
                        links=[
                            link(rel="root", endpoint="get_index"),
                            link(rel="self", endpoint="get_collection", collection_id=cid),
                        ],
                    )
                    for cid, data in self._collections.items()
                ],
                "links": [
                    link(rel="root", endpoint="get_index"),
                    link(rel="self", endpoint="get_collections"),
                ],
            }
            return flask.jsonify(response)

        @app.route("/collections/<collection_id>", methods=["GET"])
        def get_collection(collection_id):
            if collection_id not in self._collections:
                flask.abort(404, description=f"Collection {collection_id!r} not found")
            response = with_links(
                self._collections.get(collection_id).collection,
                links=[
                    link(rel="root", endpoint="get_index"),
                    link(rel="self", endpoint="get_collection", collection_id=collection_id),
                ],
            )
            return flask.jsonify(response)

        @app.route("/search", methods=["GET"])
        def get_search():
            collections = flask.request.args.get("collections")
            collections = collections.split(",") if collections else []
            bbox = flask.request.args.get("bbox")
            bbox = [float(x) for x in bbox.split(",")] if bbox else None
            item_collection = _search(
                collections=collections,
                date_range=flask.request.args.get("datetime"),
                bbox=bbox,
                filter=flask.request.args.get("filter"),
                filter_language=flask.request.args.get("filter-lang"),
                limit=flask.request.args.get("limit", type=int),
            )
            return flask.jsonify(item_collection.to_dict())

        @app.route("/search", methods=["POST"])
        def post_search():
            body = flask.request.get_json()
            item_collection = _search(
                collections=body.get("collections", []),
                date_range=body.get("datetime"),
                bbox=body.get("bbox"),
                filter=body.get("filter"),
                filter_language=body.get("filter-lang"),
                limit=body.get("limit"),
            )
            return flask.jsonify(item_collection.to_dict())

        def _search(
            collections: List[str],
            date_range: Optional[str] = None,
            bbox: Optional[List[float]] = None,
            filter: Optional[Union[str, dict]] = None,
            filter_language: Optional[str] = None,
            limit: Optional[int] = None,
        ) -> pystac.ItemCollection:
            collection_ids = [cid for cid in collections if cid in self._collections]

            items = [pystac.Item.from_dict(item) for cid in collection_ids for item in self._collections[cid].items]

            if date_range:
                if "/" in date_range:
                    start_dt, end_dt = [to_datetime_utc(d) if d != ".." else None for d in date_range.split("/", 1)]
                    items = [
                        item
                        for item in items
                        if (start_dt is None or start_dt <= to_datetime_utc(item.datetime))
                        and (end_dt is None or to_datetime_utc(item.datetime) <= end_dt)
                    ]
                else:
                    target_dt = to_datetime_utc(date_range)
                    items = [item for item in items if item.datetime == target_dt]

            if bbox:
                assert len(bbox) == 4
                bbox = shapely.box(*bbox)
                # TODO: also support item geometry fields (not just bbox)
                items = [item for item in items if item.bbox and bbox.intersects(shapely.box(*item.bbox))]

            if filter:
                filter = self._build_property_filter(filter=filter, filter_language=filter_language)
                items = [item for item in items if filter(item)]

            if limit:
                items = items[: int(limit)]

            return pystac.ItemCollection(items)

        return app

    def _build_property_filter(self, filter: Union[str, dict], filter_language: str) -> Callable[[pystac.Item], bool]:
        if filter_language == "cql2-text":
            # Just very basic filter support here: single property equality check, e.g.:
            #     "properties.flavor" = 'banana'
            if m := re.fullmatch(r"['\"]properties.(?P<property>\w+)['\"]\s*=\s*'(?P<value>[^']+)'", filter):
                prop = m.group("property")
                value = m.group("value")
                return lambda item: item.properties.get(prop) == value
        elif filter_language == "cql2-json":
            # Just very basic filter support here: single property equality check, e.g.:
            #     {"op": "=", "args": [{"property": "properties.flavor"}, "banana"]}
            if (
                isinstance(filter, dict)
                and filter.get("op") == "="
                and isinstance(filter.get("args"), list)
                and len(filter["args"]) == 2
            ):
                arg1, arg2 = filter["args"]
                if not isinstance(arg1, dict):
                    arg1, arg2 = arg2, arg1
                if isinstance(arg1, dict) and isinstance(arg2, str) and arg1.get("property").startswith("properties."):
                    prop = arg1["property"].split(".", 1)[-1]
                    value = arg2
                    return lambda item: item.properties.get(prop) == value
        raise ValueError(f"Unsupported CQL filter: {filter_language=} {filter=}")

    def serve(self) -> contextlib.AbstractContextManager[str]:
        """
        Run dummy STAC API as ephemeral server.
        To be used as context manager, that generates the root url of the server.

        Usage example:

            with dummy_stac_api.serve() as root_url:
                response = requests.get(f"{root_url}/...")
        """
        app = self._build_flask_app()
        return ephemeral_flask_server(app)

    def history_has(self, *, method: Optional[str] = None, path: Optional[str] = None) -> List[dict]:
        """Assert helper to find requests in the request history by method and/or path substring."""
        return [
            entry
            for entry in self.request_history
            if (method is None or entry["method"] == method) and (path is None or path in entry["path"])
        ]

    def collection_as_geojson_io_url(self, collection_id: str) -> str:
        """Generate a geojson.io URL to quickly visualize the items of a given collection."""
        assert collection_id in self._collections
        items = [BoundingBox.from_wsen_tuple(item["bbox"]) for item in self._collections[collection_id].items]
        return to_geojson_io_url(items)


def create_dummy_geotiff(
    path: Union[Path, str], bbox: BoundingBox, shape: Tuple[int, int] = (32, 32), scale: Optional[float] = None
):
    """
    Create a dummy GeoTIFF file with synthetic data for testing purposes,
    georeferenced according to given bounding box.
    """
    # TODO: support for multiple bands
    xmin, ymin, xmax, ymax = bbox.as_wsen_tuple()
    xn, yn = shape
    xstep = (xmax - xmin) / xn
    ystep = (ymax - ymin) / yn
    y, x = numpy.ogrid[ymax - 0.5 * ystep : ymin : -ystep, xmin + 0.5 * xstep : xmax : xstep]
    # Some multi-scale sine waves
    if not scale:
        scale = 0.001 if is_utm_crs(bbox.crs) else 1
    data = (
        (numpy.sin(scale * x) + numpy.sin(scale * y))
        + 0.5 * numpy.sin(10 * scale * x) * numpy.sin(10 * scale * y)
        + 0.25 * (numpy.sin(100 * scale * x) + numpy.sin(100 * scale * y))
    )
    data = 100 + 30 * data
    data = data.astype(numpy.uint8)

    transform = rasterio.transform.from_bounds(west=xmin, south=ymin, east=xmax, north=ymax, width=xn, height=yn)
    with rasterio.open(
        path,
        mode="w",
        driver="GTiff",
        height=yn,
        width=xn,
        count=1,
        dtype=data.dtype,
        crs=bbox.crs,
        transform=transform,
        compress="deflate",
    ) as writer:
        writer.write(data, 1)
    return path


def rasterio_metadata_dump(source: Union[str, Path, bytes, ApiResponse]) -> dict:
    """
    Extract basic raster metadata using rasterio from a file path, raw bytes, or ApiResponse object.
    """
    # TODO: put this in a more central place for better reuse
    if isinstance(source, ApiResponse):
        source = source.data
    if isinstance(source, bytes):
        source_context = rasterio.io.MemoryFile(source)
    else:
        source_context = contextlib.nullcontext(source)

    with source_context as fp:
        with rasterio.open(fp) as ds:
            return {
                "epsg": ds.crs.to_epsg(),
                "bounds": ds.bounds,
                "shape": ds.shape,
            }
