import datetime
from unittest import mock

import pytest
from pyproj import CRS
from shapely.geometry import box

from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.testing import DummyCubeBuilder
from openeogeotrellis.util.datetime import to_datetime_naive


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
