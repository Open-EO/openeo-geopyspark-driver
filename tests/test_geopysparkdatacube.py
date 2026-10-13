import datetime
from unittest import mock

import geopyspark as gps
import geopandas as gpd
import numpy as np
import pytest
from pyproj import CRS
from shapely.geometry import Point, box
import shapely

from openeogeotrellis.geopysparkdatacube import GeopysparkCubeMetadata, GeopysparkDataCube
from openeogeotrellis.testing import DummyCubeBuilder
from openeogeotrellis.util.datetime import to_datetime_naive

from tests.data import get_test_data_file


def _build_metadata():
    return GeopysparkCubeMetadata(
        {
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B01"]}},
            "summaries": {"eo:bands": [{"name": "B01", "common_name": "commonB01"}]},
        }
    )


def _mock_cube(layer_type, metadata):
    cube = object.__new__(GeopysparkDataCube)
    cube.metadata = metadata
    cube.pyramid = mock.Mock(
        layer_type=layer_type,
        max_zoom=0,
        levels={0: mock.Mock(layer_type=layer_type, srdd=mock.Mock(rdd=mock.Mock()))},
    )
    return cube


class TestGeopysparkDataCube:
    @pytest.mark.parametrize(
        ["start", "end", "expected"],
        [
            (None, None, ("2020-09-24", "2020-10-04")),
            ("2020-09-01", "2020-10-30", ("2020-09-24", "2020-10-04")),
            ("2020-09-01", "2020-10-01", ("2020-09-24", "2020-09-30")),
            ("2020-09-26", "2020-10-04", ("2020-09-26", "2020-10-01")),
            ("2020-09-26", "2020-11-01", ("2020-09-26", "2020-10-04")),
        ],
    )
    def test_filter_temporal_half_open(self, start, end, expected):
        cube_dates = [
            "2020-09-24",
            "2020-09-26",
            "2020-09-30",
            "2020-10-01",
            "2020-10-04",
        ]
        cube = DummyCubeBuilder().build_cube(dates=cube_dates)
        result = cube.filter_temporal(start=start, end=end)

        layer = result.get_max_level()
        instants = set(layer.to_numpy_rdd().map(lambda kv: kv[0].instant).collect())
        expected = tuple(to_datetime_naive(e) for e in expected)
        assert (min(instants), max(instants)) == expected

    def test_filter_temporal_identical_start_and_end(self, caplog):
        cube_dates = ["2020-09-24", "2020-09-26", "2020-09-30"]
        cube = DummyCubeBuilder().build_cube(dates=cube_dates)
        result = cube.filter_temporal(start="2020-09-26", end="2020-09-26")
        layer = result.get_max_level()
        instants = set(layer.to_numpy_rdd().map(lambda kv: kv[0].instant).collect())
        assert instants == {datetime.datetime(2020, 9, 26, 0, 0)}
        assert "filter_temporal with invalid extent" in caplog.text

    def test_mask_polygon_clips_to_buffered_raster_footprint_before_reprojecting(self):
        cube = object.__new__(GeopysparkDataCube)
        cube.get_max_level = mock.Mock(
            return_value=mock.Mock(
                layer_metadata=mock.Mock(
                    crs="EPSG:32631",
                    extent=mock.Mock(xmin=640000, ymin=5675000, xmax=650000, ymax=5685000),
                )
            )
        )
        cube.apply_to_levels = mock.Mock(return_value="masked-cube")

        mask = box(-180, -90, 180, 90)
        raster_footprint_in_mask_crs = box(4.0, 50.0, 5.0, 51.0)
        expected_clipped_mask = mask.intersection(raster_footprint_in_mask_crs.buffer(1e-6))
        reprojected_polygon = box(644000, 5676000, 649000, 5684000)
        rasterizer_options = object()

        with mock.patch("openeogeotrellis.geopysparkdatacube.reproject_geometry") as reproject_geometry, mock.patch(
            "openeogeotrellis.geopysparkdatacube.gps.RasterizerOptions", return_value=rasterizer_options
        ), mock.patch("openeogeotrellis.geopysparkdatacube.gps.get_spark_context"):
            reproject_geometry.side_effect = [raster_footprint_in_mask_crs, reprojected_polygon]

            result = cube.mask_polygon(mask=mask, srs="EPSG:4326")

        assert result == "masked-cube"

        first_call = reproject_geometry.call_args_list[0]
        assert first_call.kwargs["src_crs"] == "EPSG:32631"
        assert first_call.kwargs["dst_crs"] == CRS.from_user_input("EPSG:4326")
        assert first_call.args[0].equals(box(640000, 5675000, 650000, 5685000))

        second_call = reproject_geometry.call_args_list[1]
        assert second_call.kwargs["src_crs"] == CRS.from_user_input("EPSG:4326")
        assert second_call.kwargs["dst_crs"] == "EPSG:32631"
        assert second_call.args[0].equals(expected_clipped_mask)

        apply_function = cube.apply_to_levels.call_args.args[0]
        rdd = mock.Mock()
        apply_function(rdd)
        rdd.mask.assert_called_once_with(
            reprojected_polygon,
            partition_strategy=None,
            options=rasterizer_options,
        )

    @pytest.mark.parametrize(
        "extent",
        [
            # make_valid returns GeometryCollection(MultiPolygon, LineString)
            {"xmin": 4880000.0, "ymin": 2840000.0, "xmax": 4900000.0, "ymax": 2860000.0},
            # make_valid returns Polygon
            {"xmin": 4900000.0, "ymin": 2840000.0, "xmax": 4920000.0, "ymax": 2860000.0},
            {"xmin": 4860000.0, "ymin": 2860000.0, "xmax": 4880000.0, "ymax": 2880000.0},
            # make_valid returns GeometryCollection(Polygon, LineString)
            {"xmin": 4840000.0, "ymin": 2800000.0, "xmax": 4860000.0, "ymax": 2820000.0},
            # make_valid returns GeometryCollection(Polygon, MultiLineString)
            {"xmin": 4860000.0, "ymin": 2820000.0, "xmax": 4880000.0, "ymax": 2840000.0},
        ],
    )
    def test_mask_polygon_make_valid(self, caplog, extent):
        from geopyspark.geotrellis import SpaceTimeKey, Tile, _convert_to_unix_time
        from geopyspark.geotrellis.constants import LayerType
        from geopyspark.geotrellis.layer import TiledRasterLayer
        from pyspark import SparkContext

        # Build a real (small) GeoTrellis layer, backed by a real Spark/JVM context, instead of
        # mocking `rdd.mask()`: the JTS bug this test reproduces only manifests inside the real
        # GeoTrellis/JTS `intersectionSafe` fallback (see MaskRDD.scala), not in mocked Python code.
        tile_size = 16
        tile = Tile.from_numpy_array(np.ones((1, tile_size, tile_size), dtype="int"), -1)
        date1 = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
        layer_data = [
            (SpaceTimeKey(0, 0, date1), tile),
            (SpaceTimeKey(1, 0, date1), tile),
            (SpaceTimeKey(0, 1, date1), tile),
            (SpaceTimeKey(1, 1, date1), tile),
        ]
        rdd = SparkContext.getOrCreate().parallelize(layer_data)

        layout = {"layoutCols": 1, "layoutRows": 1, "tileCols": tile_size, "tileRows": tile_size}
        metadata = {
            "cellType": "int32ud-1",
            "extent": extent,
            # GeoPySpark/GeoTrellis needs a proj4 string here (EPSG:3035 equivalent).
            "crs": "+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m +no_defs",
            "bounds": {
                "minKey": {"col": 0, "row": 0, "instant": _convert_to_unix_time(date1)},
                "maxKey": {"col": 1, "row": 1, "instant": _convert_to_unix_time(date1)},
            },
            "layoutDefinition": {"extent": extent, "tileLayout": layout},
        }
        gps_layer = TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)
        cube = GeopysparkDataCube(pyramid=gps.Pyramid({0: gps_layer}))

        polygon_path = get_test_data_file("geometries/model-valid-geometry_EUNIS2021plus_panEU_v311_2024_ALP.parquet")
        mask = gpd.read_parquet(polygon_path).union_all()
        with mock.patch.object(TiledRasterLayer, "mask", autospec=True, side_effect=TiledRasterLayer.mask) as mask_spy:
            result = cube.mask_polygon(mask=mask, srs="EPSG:4326")

        # Thanks to shapely.validation.make_valid, this should not throw this error:
        # `java.lang.IllegalArgumentException: Reduction failed, possible invalid input`
        # https://github.com/Open-EO/openeo-geopyspark-driver/issues/1850
        tiles = result.get_max_level().to_numpy_rdd().collect()
        assert "apply make_valid on reprojected polygon" in caplog.text
        assert len(tiles) > 0

        mask_spy.assert_called_once()
        output_polygon = mask_spy.call_args.args[1]
        # Debug dump
        # import pathlib
        # debug_dir = pathlib.Path("/tmp/openeo")
        # debug_dir.mkdir(parents=True, exist_ok=True)
        # gpd.GeoSeries([output_polygon], crs="EPSG:3035").to_file(
        #     debug_dir / f"test_mask_polygon_make_valid_output_polygon_{int(extent['xmin'])}_{int(extent['ymin'])}.geojson",
        #     driver="GeoJSON",
        # )
        # Scala side silently drops anything that is not a (Multi)Polygon, resulting in an empty cube
        assert isinstance(output_polygon, (shapely.Polygon, shapely.MultiPolygon))
        assert output_polygon.is_valid
        assert not output_polygon.is_empty

    def test_mask_polygon_uses_minimum_buffer_for_degenerate_reprojected_footprint(self):
        cube = object.__new__(GeopysparkDataCube)
        cube.get_max_level = mock.Mock(
            return_value=mock.Mock(
                layer_metadata=mock.Mock(
                    crs="EPSG:32631",
                    extent=mock.Mock(xmin=640000, ymin=5675000, xmax=650000, ymax=5685000),
                )
            )
        )
        cube.apply_to_levels = mock.Mock(return_value="masked-cube")

        mask = box(3.9, 49.9, 4.1, 50.1)
        collapsed_footprint_in_mask_crs = Point(4.0, 50.0)
        expected_clipped_mask = mask.intersection(collapsed_footprint_in_mask_crs.buffer(1e-12))
        reprojected_polygon = box(644000, 5676000, 649000, 5684000)

        with mock.patch("openeogeotrellis.geopysparkdatacube.reproject_geometry") as reproject_geometry, mock.patch(
            "openeogeotrellis.geopysparkdatacube.gps.get_spark_context"
        ):
            reproject_geometry.side_effect = [collapsed_footprint_in_mask_crs, reprojected_polygon]

            cube.mask_polygon(mask=mask, srs="EPSG:4326")

        assert reproject_geometry.call_args_list[1].args[0].equals(expected_clipped_mask)

    def test_merge_cubes_spatial_spacetime_adds_temporal_metadata(self):
        spatial = _mock_cube(layer_type=gps.LayerType.SPATIAL, metadata=_build_metadata())
        spacetime = _mock_cube(
            layer_type=gps.LayerType.SPACETIME,
            metadata=_build_metadata().with_temporal_extent(
                ("2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z"), allow_adding_dimension=True
            ),
        )
        merged = _mock_cube(layer_type=gps.LayerType.SPACETIME, metadata=_build_metadata())

        spatial._apply_to_levels_geotrellis_rdd = mock.Mock(return_value=merged)

        with mock.patch("openeogeotrellis.geopysparkdatacube.gps.get_spark_context", return_value=mock.Mock()):
            result = spatial.merge_cubes(spacetime, overlaps_resolver="subtract")

        assert result.metadata.has_temporal_dimension()
        assert result.metadata.temporal_dimension.extent == ("2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z")
