"""
Reusable helpers, functions, classes, fixtures for testing purposes
"""

import contextlib
import dataclasses
import datetime
import json
import subprocess
import uuid
from pathlib import Path
from typing import Dict, Iterator, List, Tuple, Union, Optional
from unittest import mock

import geopyspark.geotrellis
import geopyspark.geotrellis.constants
import geopyspark.geotrellis.layer
import kazoo
import kazoo.exceptions
import numpy
import openeo_driver.testing
import pyspark
import pytest

from openeogeotrellis.config import gps_config_getter
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.util.datetime import to_datetime_utc
from openeogeotrellis.util.runtime import is_package_available


def random_name(prefix: str = "") -> str:
    """Generate random name (with given prefix)"""
    return '{p}{r}'.format(p=prefix, r=uuid.uuid4().hex[:8])


class _ZNodeStat:
    def __init__(self, version: int = 1):
        self._version = version

    @property
    def version(self):
        return self._version

    def bump_version(self):
        self._version += 1

    def __repr__(self):
        return '{c}({v})'.format(c=self.__class__.__name__, v=self.version)

    def __eq__(self, other):
        return isinstance(other, _ZNodeStat) and (self.version,) == (other.version,)


class _ZNode:
    """Internal 'znode' container"""

    def __init__(self, value: bytes = b"", children: dict = None):
        self.value = value
        self.children = children or {}
        self.stat = _ZNodeStat(version=1)

    def assert_version(self, version=-1) -> '_ZNode':
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
