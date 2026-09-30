import datetime
import io
from unittest import mock

import geopandas as gpd
import geopyspark as gps
import pytest
import requests
from pyproj import CRS
from shapely.geometry import Point, box

from openeogeotrellis.geopysparkdatacube import GeopysparkCubeMetadata, GeopysparkDataCube
from openeogeotrellis.testing import DummyCubeBuilder
from openeogeotrellis.util.datetime import to_datetime_naive


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
                    crs="EPSG:3035",
                    extent=mock.Mock(xmin=4900000, ymin=2840000, xmax=4920000, ymax=2860000),
                )
            )
        )
        cube.apply_to_levels = mock.Mock(return_value="masked-cube")

        polygon_url = "https://s3.waw4-1.cloudferro.com/model-waw4-1-0qm0pt98q2fsihpm0duqjw4ell41oeiauvp4cy6edrl1kklfad/EUNIS2021plus/panEU/v311/2024/ALP/model-valid-geometry_EUNIS2021plus_panEU_v311_2024_ALP.parquet"
        response = requests.get(polygon_url, stream=True)
        response.raise_for_status()
        response.raw.decode_content = True
        mask = gpd.read_parquet(io.BytesIO(response.raw.read()))
        from pathlib import Path

        Path("/tmp/openeo").mkdir(exist_ok=True)
        mask.to_file("/tmp/openeo/mask.geojson", driver="GeoJSON")
        # global_extent: {'west': 4899610.0, 'south': 2839610.0, 'east': 4920390.0, 'north': 2860390.0, 'crs': 'EPSG:3035'}
        raster_footprint_in_mask_crs = box(4899610.0, 2839610.0, 4920390.0, 2860390.0)
        expected_clipped_mask = mask.intersection(raster_footprint_in_mask_crs.buffer(1e-6))
        reprojected_polygon = box(4908000, 2842000, 4918000, 2858000)
        rasterizer_options = object()

        with mock.patch("openeogeotrellis.geopysparkdatacube.reproject_geometry") as reproject_geometry, mock.patch(
            "openeogeotrellis.geopysparkdatacube.gps.RasterizerOptions", return_value=rasterizer_options
        ), mock.patch("openeogeotrellis.geopysparkdatacube.gps.get_spark_context"):
            reproject_geometry.side_effect = [raster_footprint_in_mask_crs, reprojected_polygon]

            result = cube.mask_polygon(mask=mask, srs="EPSG:4326")

        assert result == "masked-cube"

        first_call = reproject_geometry.call_args_list[0]
        assert first_call.kwargs["src_crs"] == "EPSG:3035"
        assert first_call.kwargs["dst_crs"] == CRS.from_user_input("EPSG:4326")
        assert first_call.args[0].equals(box(4900000, 2840000, 4920000, 2860000))

        second_call = reproject_geometry.call_args_list[1]
        assert second_call.kwargs["src_crs"] == CRS.from_user_input("EPSG:4326")
        assert second_call.kwargs["dst_crs"] == "EPSG:3035"
        assert second_call.args[0].equals(expected_clipped_mask)

        apply_function = cube.apply_to_levels.call_args.args[0]
        rdd = mock.Mock()
        apply_function(rdd)
        rdd.mask.assert_called_once_with(
            reprojected_polygon,
            partition_strategy=None,
            options=rasterizer_options,
        )
        # intersect "/tmp/openeo/mask.geojson" and "/tmp/openeo/reprojected_polygon.geojson"
        gpd.GeoDataFrame(geometry=[reprojected_polygon], crs="EPSG:3035").to_file(
            "/tmp/openeo/reprojected_polygon.geojson", driver="GeoJSON"
        )

        mask_gdf = gpd.read_file("/tmp/openeo/mask.geojson").to_crs("EPSG:3035")
        reprojected_polygon_gdf = gpd.read_file("/tmp/openeo/reprojected_polygon.geojson")
        intersection = gpd.overlay(mask_gdf, reprojected_polygon_gdf, how="intersection")
        assert intersection.is_valid.all(), "intersection should be valid"
        print(f"intersection empty: {intersection.empty}, area: {intersection.area.sum()}")
        intersection.to_file("/tmp/openeo/intersection.geojson", driver="GeoJSON")
        assert not intersection.empty, "mask and reprojected polygon do not intersect"


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
