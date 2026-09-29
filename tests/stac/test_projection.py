"""
Focused unit tests for `openeogeotrellis.stac.projection`, exercised through its
own public surface (no JVM/GeoPySpark dependency), independent of
`test_load_stac.py`'s end-to-end JVM-backed checks.
"""
import datetime as dt

import pystac
import pytest

from openeogeotrellis.stac.projection import ProjectionMetadata, compute_cellsize, get_asset_property, get_proj_metadata

SOME_DATETIME = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)


class TestProjectionMetadata:
    def test_from_item_proj_code(self):
        item = pystac.Item(
            id="item1",
            geometry=None,
            bbox=None,
            datetime=SOME_DATETIME,
            properties={
                "proj:code": "EPSG:32631",
                "proj:bbox": [500000, 5600000, 510000, 5610000],
                "proj:shape": [1000, 1000],
            },
        )
        metadata = ProjectionMetadata.from_item(item)
        assert metadata.code == "EPSG:32631"
        assert metadata.epsg == 32631
        assert metadata.bbox == (500000, 5600000, 510000, 5610000)
        assert metadata.shape == (1000, 1000)
        assert metadata.resolution() == (10.0, 10.0)

    def test_from_item_legacy_proj_epsg(self):
        item = pystac.Item(
            id="item1", geometry=None, bbox=None, datetime=SOME_DATETIME, properties={"proj:epsg": 4326}
        )
        metadata = ProjectionMetadata.from_item(item)
        assert metadata.code == "EPSG:4326"
        assert metadata.epsg == 4326

    def test_bbox_from_transform(self):
        metadata = ProjectionMetadata(shape=(10, 20), transform=[1, 0, 500000, 0, -1, 5600000])
        assert metadata.bbox == (500000, 5600000 - 10, 500000 + 20, 5600000)

    def test_resolution_fail_on_miss(self):
        metadata = ProjectionMetadata()
        with pytest.raises(ValueError):
            metadata.resolution()
        assert metadata.resolution(fail_on_miss=False) is None

    def test_equality(self):
        a = ProjectionMetadata(code="EPSG:4326", bbox=(0, 0, 1, 1), shape=(1, 1))
        b = ProjectionMetadata(code="EPSG:4326", bbox=(0, 0, 1, 1), shape=(1, 1))
        c = ProjectionMetadata(code="EPSG:32631", bbox=(0, 0, 1, 1), shape=(1, 1))
        assert a == b
        assert a != c


class TestGetAssetProperty:
    def test_direct_field(self):
        asset = pystac.Asset(href="asset.tif", extra_fields={"proj:epsg": 32631})
        assert get_asset_property(asset, "proj:epsg") == 32631

    def test_from_consistent_bands(self):
        asset = pystac.Asset(
            href="asset.tif",
            extra_fields={"bands": [{"nodata": -9999}, {"nodata": -9999}]},
        )
        assert get_asset_property(asset, "nodata") == -9999

    def test_missing(self):
        asset = pystac.Asset(href="asset.tif")
        assert get_asset_property(asset, "nodata") is None


def test_compute_cellsize():
    assert compute_cellsize((0, 0, 100, 200), (20, 10)) == (10.0, 10.0)


def test_get_proj_metadata():
    item = pystac.Item(id="item1", geometry=None, bbox=None, datetime=SOME_DATETIME, properties={})
    asset = pystac.Asset(
        href="asset.tif",
        extra_fields={"proj:code": "EPSG:32631", "proj:bbox": [0, 0, 100, 200], "proj:shape": [20, 10]},
    )
    epsg, bbox, shape = get_proj_metadata(asset=asset, item=item)
    assert epsg == 32631
    assert bbox == (0, 0, 100, 200)
    assert shape == (20, 10)
