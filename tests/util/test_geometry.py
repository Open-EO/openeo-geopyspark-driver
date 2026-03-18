import itertools
import mock
import pytest

from openeo_driver.util.geometry import BoundingBox

from openeogeotrellis.util.geometry import BoundingBoxMerger, GridSnapper, bbox_to_geojson


class TestBoundingBoxMerger:
    def test_empty(self):
        merger = BoundingBoxMerger()
        assert merger.get() is None

    def test_single(self):
        merger = BoundingBoxMerger()
        merger.add(BoundingBox(1, 2, 3, 4))
        assert merger.get() == BoundingBox(1, 2, 3, 4)

    def test_multiple(self):
        merger = BoundingBoxMerger()
        merger.add(BoundingBox(1, 2, 3, 4))
        merger.add(BoundingBox(8, 4, 9, 6))
        merger.add(BoundingBox(4, 10, 5, 12))
        assert merger.get() == BoundingBox(1, 2, 9, 12)

    def test_target_crs(self):
        merger = BoundingBoxMerger(crs="EPSG:32631")
        merger.add(BoundingBox(3.1, 51.1, 3.2, 51.2, crs="EPSG:4326"))
        assert merger.get() == BoundingBox(506986, 5660950, 514003, 5672085, crs="EPSG:32631").approx(abs=1)

    @pytest.mark.parametrize("input_crs", ["EPSG:4326", None])
    def test_no_target_crs_but_input_with_single_crs(self, input_crs):
        merger = BoundingBoxMerger()
        merger.add(BoundingBox(3.1, 51.1, 3.2, 51.2, crs=input_crs))
        assert merger.get() == BoundingBox(3.1, 51.1, 3.2, 51.2, crs=input_crs)

    def test_no_target_crs_multiple_input_crses(self):
        merger = BoundingBoxMerger()
        merger.add(BoundingBox(3.1, 51.1, 3.2, 51.2, crs="EPSG:4326"))
        merger.add(BoundingBox(506980, 5660950, 514000, 5672080, crs="EPSG:32631"))
        with pytest.raises(ValueError, match="Undefined.*merging.*no target.*multiple CRSes.*EPSG:32631.*EPSG:4326"):
            _ = merger.get()

    def test_crs_grouped_reprojections(self):
        with mock.patch.object(
            BoundingBox, "reproject", wraps=BoundingBox.reproject, autospec=True
        ) as wrapped_reproject:
            merger = BoundingBoxMerger(crs="EPSG:32631")
            for i, j in itertools.product(range(10), range(10)):
                west = 3 + 0.1 * i
                south = 51 + 0.1 * j
                merger.add(BoundingBox(west=west, south=south, east=west + 0.1, north=south + 0.1, crs="EPSG:4326"))
            merged = merger.get()

        assert merged == BoundingBox(3, 51, 4, 52, crs="EPSG:4326").reproject("EPSG:32631").approx(abs=1)
        assert wrapped_reproject.call_count == 1


class TestGridSnapper:
    def test_simple(self):
        snapper = GridSnapper(origin=0, resolution=5)

        def down_round_up(x):
            return snapper.down(x), snapper.round(x), snapper.up(x)

        assert down_round_up(0) == (0, 0, 0)
        assert down_round_up(4.3) == (0, 5, 5)
        assert down_round_up(16.3) == (15, 15, 20)
        assert down_round_up(-4.3) == (-5, -5, 0)
        assert down_round_up(-16.3) == (-20, -15, -15)

    def test_orig_and_fractional_resolution(self):
        snapper = GridSnapper(origin=1.125, resolution=0.25)

        def down_round_up(x):
            return snapper.down(x), snapper.round(x), snapper.up(x)

        assert down_round_up(0) == (-0.125, 0.125, 0.125)
        assert down_round_up(1.12) == (0.875, 1.125, 1.125)
        assert down_round_up(1.13) == (1.125, 1.125, 1.375)
        assert down_round_up(4.3) == (4.125, 4.375, 4.375)
        assert down_round_up(100) == (99.875, 100.125, 100.125)
        assert down_round_up(-100) == (-100.125, -99.875, -99.875)


def test_bbox_to_geojson():
    expected = {
        "type": "Polygon",
        "coordinates": (((3, 2), (3, 4), (1, 4), (1, 2), (3, 2)),),
    }
    assert bbox_to_geojson(1, 2, 3, 4) == expected
    assert bbox_to_geojson((1, 2, 3, 4)) == expected


def test_bbox_to_geojson_antimeridian():
    expected = {
        "type": "MultiPolygon",
        "coordinates": [
            (((180, 2), (180, 4), (179, 4), (179, 2), (180, 2)),),
            (((-178, 2), (-178, 4), (-180, 4), (-180, 2), (-178, 2)),),
        ],
    }
    assert bbox_to_geojson(179, 2, -178, 4) == expected
    assert bbox_to_geojson((179, 2, -178, 4)) == expected
