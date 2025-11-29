import mock
import pytest

import openeo_driver.ProcessGraphDeserializer
from openeo_driver.ProcessGraphDeserializer import ENV_DRY_RUN_TRACER
from openeo_driver.dry_run import SourceConstraint, DryRunDataTracer
from openeo_driver.dummy.dummy_backend import DummyBackendImplementation
from openeo_driver.util.geometry import BoundingBox
from openeo_driver.utils import EvalEnv
from openeogeotrellis._backend.post_dry_run import (
    _align_extent,
    _snap_bbox,
    _GridInfo,
    _buffer_extent,
    _extract_target_extent_from_constraint,
)


class TestGridInfo:

    def test_minimal(self):
        grid = _GridInfo(crs="EPSG:4326")
        assert grid.crs_raw == "EPSG:4326"
        assert grid.crs_epsg == 4326
        assert grid.resolution is None

    def test_more(self):
        grid = _GridInfo(
            crs="EPSG:32631",
            resolution=(10, 20),
            extent_x=(-1000, 1000),
            extent_y=(2000, 3000),
        )
        assert grid.crs_raw == "EPSG:32631"
        assert grid.crs_epsg == 32631
        assert grid.resolution == (10, 20)
        assert grid.extent_x == (-1000, 1000)
        assert grid.extent_y == (2000, 3000)

    def test_from_datacube_metadata(self):
        grid = _GridInfo.from_datacube_metadata(
            {
                "cube:dimensions": {
                    "t": {"type": "temporal", "extent": ["2020-01-01T00:00:00Z", "2020-12-31T23:59:59Z"]},
                    "x": {
                        "type": "spatial",
                        "axis": "x",
                        "reference_system": 32631,
                        "extent": [-20000, 30000],
                        "step": 10,
                    },
                    "y": {
                        "type": "spatial",
                        "axis": "y",
                        "reference_system": 32631,
                        "extent": [10000, 40000],
                        "step": 20,
                    },
                }
            }
        )
        assert grid.crs_raw == 32631
        assert grid.crs_epsg == 32631
        assert grid.resolution == (10, 20)
        assert grid.extent_x == (-20000, 30000)
        assert grid.extent_y == (10000, 40000)


def test_snap_bbox():
    bbox = BoundingBox(1.222, 3.444, 5.666, 7.888)

    assert _snap_bbox(
        bbox,
        resolution=(1, 1),
        extent_x=(0, 10),
        extent_y=(0, 10),
    ) == BoundingBox(1, 3, 6, 8)

    assert _snap_bbox(
        bbox,
        resolution=(2, 3),
        extent_x=(0, 10),
        extent_y=(0, 10),
    ) == BoundingBox(0, 3, 6, 9)

    assert _snap_bbox(
        bbox,
        resolution=(0.3, 0.7),
        extent_x=(0, 10),
        extent_y=(0, 10),
    ) == BoundingBox(1.2, 2.8, 5.7, pytest.approx(8.4))

    assert _snap_bbox(
        bbox,
        resolution=(0.3, 0.7),
        extent_x=(1.1, 10),
        extent_y=(2.1, 5),
    ) == BoundingBox(1.1, 2.8, 5.9, 5)


def test_align_extent_4326_basic():
    extent = BoundingBox(1.222, 3.444, 5.666, 7.888, crs="EPSG:4326")
    source = _GridInfo(crs=4326, extent_x=(0, 10), extent_y=(0, 10), resolution=(0.2, 0.3))
    target = _GridInfo(crs=4326, resolution=(0.2, 0.3))
    aligned = _align_extent(extent=extent, source=source, target=target)
    assert aligned == BoundingBox(pytest.approx(1.2), 3.3, pytest.approx(5.8), 8.1, crs="EPSG:4326")


def test_align_extent_4326_no_target_resolution():
    extent = BoundingBox(1.222, 3.444, 5.666, 7.888, crs="EPSG:4326")
    source = _GridInfo(crs=4326, extent_x=(0, 10), extent_y=(0, 10), resolution=(0.2, 0.3))
    target = _GridInfo(crs=4326, resolution=None)
    aligned = _align_extent(extent=extent, source=source, target=target)
    assert aligned == BoundingBox(pytest.approx(1.2), 3.3, pytest.approx(5.8), 8.1, crs="EPSG:4326")


def test_align_extent_auto_utm():
    extent = BoundingBox(3, 51, 3.1, 51.1, crs="EPSG:4326")
    source = _GridInfo(crs="AUTO:42001", resolution=(10, 10))
    target = _GridInfo(crs="EPSG:32631", resolution=(10, 10))
    aligned = _align_extent(extent=extent, source=source, target=target)
    assert aligned == BoundingBox(west=500000, south=5649820, east=507020, north=5660960, crs="EPSG:32631")


def test_buffer_extent_stay_in_4326():
    bbox = BoundingBox(3, 51, 3.1, 51.1, crs="EPSG:4326")

    assert _buffer_extent(
        bbox, buffer=(10, 10), sampling=_GridInfo(crs="EPSG:4326", resolution=(0.01, 0.01))
    ) == BoundingBox(2.9, 50.9, 3.2, 51.2, crs="EPSG:4326")

    assert _buffer_extent(
        bbox, buffer=(3, 7), sampling=_GridInfo(crs="EPSG:4326", resolution=(0.01, 0.02))
    ) == BoundingBox(2.97, 50.86, 3.13, 51.24, crs="EPSG:4326")


def test_buffer_extent_4326_to_utm():
    bbox = BoundingBox(3, 51, 3.1, 51.1, crs="EPSG:4326")

    assert _buffer_extent(
        bbox, buffer=(3, 3), sampling=_GridInfo(crs="EPSG:32631", resolution=(10, 10))
    ) == BoundingBox(
        pytest.approx(500000 - 30, abs=1),
        pytest.approx(5649824 - 30, abs=1),
        pytest.approx(507016 + 30, abs=1),
        pytest.approx(5660950 + 30, abs=1),
        crs="EPSG:32631",
    )


def _get_source_constraints(pg: dict) -> list[SourceConstraint]:
    dry_run_tracer = DryRunDataTracer()
    env = EvalEnv({ENV_DRY_RUN_TRACER: dry_run_tracer, "backend_implementation": DummyBackendImplementation()})
    openeo_driver.ProcessGraphDeserializer.evaluate(pg, env=env)

    source_constraints = dry_run_tracer.get_source_constraints()
    return source_constraints


def test_extract_target_extent_from_constraint_minimal():
    # TODO
    catalog = mock.Mock()
    catalog.get_collection_metadata.return_value = {
        # TODO: helper for cube:dimensions metadata?
        "cube:dimensions": {
            "t": {"type": "temporal", "extent": ["2020-01-01T00:00:00Z", "2020-12-31T23:59:59Z"]},
            "x": {
                "type": "spatial",
                "axis": "x",
                "reference_system": 32631,
                "extent": [-20000, 30000],
                "step": 10,
            },
            "y": {
                "type": "spatial",
                "axis": "y",
                "reference_system": 32631,
                "extent": [10000, 40000],
                "step": 20,
            },
        }
    }

    # TODO: more helpers for constructing source constraints?
    source_constraints = _get_source_constraints(
        {
            "load_collection": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "S2_FOOBAR",
                    "spatial_extent": {"west": 1, "south": 2, "east": 3, "north": 4},
                },
                "result": True,
            },
        }
    )
    extent = _extract_target_extent_from_constraint(source_constraint=source_constraints[0], catalog=catalog)
    assert extent == BoundingBox(1, 2, 3, 4, crs=4326)
