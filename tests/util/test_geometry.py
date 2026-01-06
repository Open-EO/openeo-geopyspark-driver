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
