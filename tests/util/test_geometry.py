import math
import itertools
import logging

import geopandas
import mock
import pytest
import shapely.geometry

from openeo_driver.util.geometry import BoundingBox

from openeogeotrellis.util.geometry import BoundingBoxMerger, GeometrySimplifier, GridSnapper, bbox_to_geojson
from ..data import get_test_data_file

logger = logging.getLogger(__name__)


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


class TestGeometrySimplifier:
    def test_simplify_box_shape(self):
        box = shapely.geometry.box(1, 2, 3, 4)
        simplified = GeometrySimplifier().simplify(geometry=box)
        logger.info(f"{simplified=}")
        assert isinstance(simplified, shapely.geometry.Polygon)
        assert shapely.hausdorff_distance(simplified, box) == 0

    def test_simplify_box_df(self):
        box = shapely.geometry.box(1, 2, 3, 4)
        gdf = geopandas.GeoSeries([box])
        simplified = GeometrySimplifier().simplify(geometry=gdf)
        logger.info(f"{simplified=}")
        assert isinstance(simplified, shapely.geometry.Polygon)
        assert shapely.hausdorff_distance(simplified, box) == 0

    def test_box_to_simplified_geojson(self):
        box = shapely.geometry.box(1, 2, 3, 4)
        geojson = GeometrySimplifier().to_simplified_geojson(geometry=box)
        assert geojson == '{"type":"Polygon","coordinates":[[[3.0,2.0],[3.0,4.0],[1.0,4.0],[1.0,2.0],[3.0,2.0]]]}'

    def test_simplify_box_with_crs(self):
        box32631 = shapely.geometry.box(506986, 5660950, 514003, 5672085)
        gdf = geopandas.GeoSeries([box32631], crs="EPSG:32631")

        simplified = GeometrySimplifier().simplify(geometry=gdf)
        logger.info(f"{simplified=}")
        assert isinstance(simplified, shapely.geometry.Polygon)
        assert shapely.hausdorff_distance(simplified, box32631) == 0

        geojson = GeometrySimplifier().to_simplified_geojson(geometry=gdf)
        assert geojson.startswith('{"type":"Polygon","coordinates":[[[3.')
        expected = shapely.from_geojson(
            '{"type": "Polygon", "coordinates": [[[3.1, 51.1], [3.1, 51.2], [3.2, 51.2], [3.2, 51.1], [3.1, 51.1]]]}'
        )
        assert shapely.hausdorff_distance(shapely.from_geojson(geojson), expected) == pytest.approx(0, abs=0.001)

    @pytest.mark.parametrize(
        ["vertex_threshold", "expected"],
        [
            (
                # Threshold 32: plenty of room to preserve geometry
                32,
                "MULTIPOLYGON (((4 1, 4 2, 5 2, 5 1, 4 1)), ((2 0, 2 1, 3 1, 3 0, 2 0)), ((0 1, 0 2, 1 2, 1 1, 0 1)), ((2 2, 2 3, 3 3, 3 2, 2 2)))",
            ),
            (
                # Threshold 12: trigger simplification to hull
                12,
                "POLYGON ((2 0, 0 1, 0 2, 2 3, 3 3, 5 2, 5 1, 3 0, 2 0))",
            ),
            (
                # Threshold 6: trigger envelop (bounding box)
                6,
                "POLYGON ((0 0, 5 0, 5 3, 0 3, 0 0))",
            ),
        ],
    )
    def test_simplify_four_boxes_in_cross_arragement(self, vertex_threshold, expected):
        """
             ┌─┐12
        ┌─┐9 └─┘  ┌─┐3
        └─┘  ┌─┐6 └─┘
             └─┘
        """
        box3 = shapely.geometry.box(4, 1, 5, 2)
        box6 = shapely.geometry.box(2, 0, 3, 1)
        box9 = shapely.geometry.box(0, 1, 1, 2)
        box12 = shapely.geometry.box(2, 2, 3, 3)
        geometry = geopandas.GeoSeries([box3, box6, box9, box12])

        simplified = GeometrySimplifier().simplify(geometry=geometry, vertex_threshold=vertex_threshold)
        expected = shapely.from_wkt(expected)
        logger.info(f"{simplified=} {expected=}")
        assert isinstance(simplified, type(expected))
        assert shapely.hausdorff_distance(simplified, expected) == 0

    @pytest.mark.parametrize(
        ["vertex_threshold", "expected"],
        [
            (
                # Threshold 12: trigger union of envelopes
                12,
                "MULTIPOLYGON (((-1 4, 3 4, 3 0, -1 0, -1 4)), ((4 5, 8 5, 8 1, 4 1, 4 5)))",
            ),
            (
                # Threshold 8: trigger simplification to hull of envelopes
                8,
                "POLYGON ((-1 0, -1 4, 4 5, 8 5, 8 1, 3 0, -1 0))",
            ),
            (
                # Threshold 6: trigger overall envelop (bounding box)
                6,
                "POLYGON ((-1 0, 8 0, 8 5, -1 5, -1 0))",
            ),
        ],
    )
    def test_two_stars(self, vertex_threshold, expected):
        base_star = [(1, 0), (2, 1), (1, 1), (0, 2), (-1, 1), (-2, 1)]
        base_star = base_star + [(-x, -y) for (x, y) in base_star]
        star12 = shapely.geometry.Polygon((1 + x, 2 + y) for (x, y) in base_star)
        star63 = shapely.geometry.Polygon((6 + x, 3 + y) for (x, y) in base_star)
        geometry = geopandas.GeoSeries([star12, star63])

        simplified = GeometrySimplifier().simplify(geometry=geometry, vertex_threshold=vertex_threshold)
        expected = shapely.from_wkt(expected)
        logger.info(f"{simplified=} {expected=}")
        assert isinstance(simplified, type(expected))
        assert shapely.hausdorff_distance(simplified, expected) == 0

    def test_to_simplified_geojson_decimal_places(self):
        box = shapely.geometry.box(1.23456789, math.e, math.pi, 4)
        geojson = GeometrySimplifier().to_simplified_geojson(geometry=box)
        assert (
            geojson
            == '{"type":"Polygon","coordinates":[[[3.1416,2.7183],[3.1416,4.0],[1.2346,4.0],[1.2346,2.7183],[3.1416,2.7183]]]}'
        )

        geojson = GeometrySimplifier().to_simplified_geojson(geometry=box, round_decimals=2)
        assert (
            geojson == '{"type":"Polygon","coordinates":[[[3.14,2.72],[3.14,4.0],[1.23,4.0],[1.23,2.72],[3.14,2.72]]]}'
        )

    def test_to_simplified_geojson_wc_34TFM_100patches(self):
        """
        More complex, real world use case
        with 100 patches distributed in an S2 tile
        """
        path = get_test_data_file("geometries/wc-34TFM-100patches.geoparquet")
        geometry = geopandas.read_parquet(path).geometry
        geometry_4326 = geometry.to_crs("EPSG:4326")

        # GeoJSON representation
        orig_geojson = shapely.to_geojson(
            geometry_4326.union_all(method="coverage")
            if hasattr(geometry_4326, "union_all")
            # TODO: remove this fallback for geopandas<1.0.0 once Python 3.8 support is dropped
            else geometry_4326.unary_union
        )
        logger.info(f"{len(orig_geojson)=} {orig_geojson=}")
        simplified_geojson = GeometrySimplifier().to_simplified_geojson(geometry, round_decimals=3)
        logger.info(f"{len(simplified_geojson)=} {simplified_geojson=}")
        assert len(orig_geojson) > 15000 and len(simplified_geojson) < 300

        # Check coverage from sampling original geometry
        simplified = shapely.from_geojson(simplified_geojson)
        inside_samples = list(geometry_4326.centroid.iloc[::10])
        logger.info(f"{len(inside_samples)=} {inside_samples=}")
        assert all([simplified.contains(s) for s in inside_samples])

        # Corners of overall bounds should be outside
        outside_samples = [
            shapely.Point(x, y) for (x, y) in shapely.geometry.box(*geometry_4326.total_bounds).exterior.coords[:-1]
        ]
        logger.info(f"{len(outside_samples)=} {outside_samples=}")
        assert all([not simplified.contains(s) for s in outside_samples])
