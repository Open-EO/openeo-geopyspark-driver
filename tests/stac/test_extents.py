"""
Focused unit tests for `openeogeotrellis.stac.extents`, exercised through its own
public surface (no JVM/GeoPySpark dependency), independent of `test_load_stac.py`'s
end-to-end JVM-backed checks.
"""
import datetime

import pytest
from openeo_driver.util.geometry import BoundingBox

from openeogeotrellis.stac.extents import (
    SpatialFilteringGeometries,
    SpatioTemporalExtent,
    TemporalExtent,
    spatiotemporal_extent_from_load_params,
)


class TestTemporalExtent:
    def test_from_load_param_extent_basic(self):
        extent = TemporalExtent.from_load_param_extent(("2024-01-01", "2024-02-01"))
        assert extent.from_date == datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        assert extent.to_date == datetime.datetime(2024, 1, 31, 23, 59, 59, 999000, tzinfo=datetime.timezone.utc)

    def test_from_load_param_extent_open_ended(self):
        extent = TemporalExtent.from_load_param_extent((None, None))
        assert extent.is_unbounded()

    def test_from_load_param_extent_single_day_legacy(self):
        extent = TemporalExtent.from_load_param_extent(("2024-01-01", "2024-01-01"))
        assert extent.from_date == datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        assert extent.to_date == datetime.datetime(2024, 1, 1, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc)

    def test_intersects(self):
        extent = TemporalExtent.from_load_param_extent(("2024-01-01", "2024-02-01"))
        assert extent.intersects(nominal="2024-01-15")
        assert not extent.intersects(nominal="2024-03-01")

    def test_intersects_start_end(self):
        extent = TemporalExtent.from_load_param_extent(("2024-01-01", "2024-02-01"))
        assert extent.intersects(start_datetime="2023-12-01", end_datetime="2024-01-10")
        assert not extent.intersects(start_datetime="2024-03-01", end_datetime="2024-04-01")

    def test_equality(self):
        a = TemporalExtent(from_date="2024-01-01", to_date="2024-02-01")
        b = TemporalExtent(from_date="2024-01-01", to_date="2024-02-01")
        c = TemporalExtent(from_date="2024-01-01", to_date="2024-03-01")
        assert a == b
        assert a != c


class TestSpatioTemporalExtent:
    def test_item_intersects(self):
        bbox = BoundingBox(west=3, south=51, east=4, north=52, crs=4326)
        extent = SpatioTemporalExtent(bbox=bbox, from_date="2024-01-01", to_date="2024-02-01")

        class DummyItem:
            datetime = datetime.datetime(2024, 1, 15, tzinfo=datetime.timezone.utc)
            properties = {}
            bbox = [3.1, 51.1, 3.9, 51.9]

        assert extent.item_intersects(DummyItem())

    def test_item_does_not_intersect_outside_bbox(self):
        bbox = BoundingBox(west=3, south=51, east=4, north=52, crs=4326)
        extent = SpatioTemporalExtent(bbox=bbox, from_date="2024-01-01", to_date="2024-02-01")

        class DummyItem:
            datetime = datetime.datetime(2024, 1, 15, tzinfo=datetime.timezone.utc)
            properties = {}
            bbox = [10, 10, 11, 11]

        assert not extent.item_intersects(DummyItem())

    def test_spatiotemporal_extent_from_load_params(self):
        extent = spatiotemporal_extent_from_load_params(
            spatial_extent={"west": 3, "south": 51, "east": 4, "north": 52},
            temporal_extent=("2024-01-01", "2024-02-01"),
        )
        assert extent.spatial_extent.as_bbox().as_wsen_tuple() == (3, 51, 4, 52)
        assert not extent.temporal_extent.is_unbounded()


class TestSpatialFilteringGeometries:
    def test_none_geometries(self):
        geometries = SpatialFilteringGeometries(geometries=None)
        assert geometries.get_simplified_geojson() is None
