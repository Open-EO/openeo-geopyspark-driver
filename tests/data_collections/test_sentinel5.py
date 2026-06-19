import os.path
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import pytest
import rasterio

if __name__ == "__main__":
    import openeogeotrellis.deploy.local

    # Allow to run from commandline:
    # /usr/bin/time -v python3 tests/data_collections/test_sentinel3.py
    # python3 -m memory_profiler tests/data_collections/test_sentinel3.py
    # The SparkContext is only needed to make imports work, but is actually not used for the tests
    openeogeotrellis.deploy.local.setup_environment()

from openeogeotrellis.collections.load_sentinel5p import load_level2_data, read_product

from tests.data import get_test_data_file

if not os.path.exists("/eodata") or not os.listdir("/eodata"):
    pytest.skip(reason="requires mounting /eodata.", allow_module_level=True)

# Small synthetic NetCDF created at tests/data/binary/Sentinel-5P/
# Covers lon 4.0–4.9 °E, lat 50.2–51.1 °N, 2024-09-02 10:00–10:19 UTC
SYNTHETIC_CO_FILE = get_test_data_file(
    "binary/Sentinel-5P/S5P_OFFL_L2__CO_____20240902T094132_20240902T112301_00001_03_020600_20240903T232407.nc"
)


# ---------------------------------------------------------------------------
# Tests using synthetic test data (no eodata mount required)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Tests that require a real eodata mount
# ---------------------------------------------------------------------------


class TestSentinel5:
    def setup_method(self):
        test_data_path = Path("/tmp/Sentinel5data/")
        test_data_path.mkdir(exist_ok=True)

        # important to get these files locally for testing
        self.filename = (
            test_data_path / "S5P_OFFL_L2__CO_____20240902T094132_20240902T112301_35696_03_020600_20240903T232407.nc"
        )
        if not os.path.exists(self.filename):
            shutil.copyfile(
                "/eodata/Sentinel-5P/TROPOMI/L2__CO____/2024/09/02/S5P_OFFL_L2__CO_____20240902T094132_20240902T112301_35696_03_020600_20240903T232407.nc",
                self.filename,
            )
        self.filename_anti = (
            test_data_path / "S5P_RPRO_L2__CO_____20180430T001950_20180430T020120_02818_03_020400_20220901T170054.nc"
        )
        if not os.path.exists(self.filename_anti):
            shutil.copyfile(
                "/eodata/Sentinel-5P/TROPOMI/L2__CO____/2018/04/30/S5P_RPRO_L2__CO_____20180430T001950_20180430T020120_02818_03_020400_20220901T170054.nc",
                self.filename_anti,
            )
        self.temporal_extent_anti = [datetime(2018, 4, 30, 0, 50, 0), datetime(2018, 4, 30, 1, 30, 0)]
        self.spatial_extent_anti = [179.5, 22, -179.5, 23]  # min_lon, min_lat, max_lon, max_lat

        self.temporal_extent_valid = [datetime(2024, 9, 2, 10, 30, 0), datetime(2024, 9, 2, 11, 0, 0)]
        self.temporal_extent_invalid = [datetime(2024, 9, 2, 11, 30, 0), datetime(2024, 9, 2, 11, 35, 0)]
        self.spatial_extent_normal = [30.0, 25.0, 30.05, 25.05]  # min_lon, min_lat, max_lon, max_lat
        self.spatial_extent_invalid = [22.0, 24.0, 24.0, 26.0]  # min_lon, min_lat, max_lon, max_lat

        self.filename_no2 = (
            test_data_path / "S5P_RPRO_L2__NO2____20220614T095228_20220614T113358_24190_03_020400_20230202T231229.nc"
        )
        if not os.path.exists(self.filename_no2):
            shutil.copyfile(
                "/eodata/Sentinel-5P/TROPOMI/L2__NO2___/2022/06/14/S5P_RPRO_L2__NO2____20220614T095228_20220614T113358_24190_03_020400_20230202T231229.nc",
                self.filename_no2,
            )
        self.spatial_extent_no2 = [10.0, 50.0, 10.05, 50.05]
        self.temporal_extent_no2 = [datetime(2022, 6, 14, 10, 30, 0), datetime(2022, 6, 14, 11, 0, 0)]

    def test_read_product_returns_tiles(self):
        """read_product produces at least one SpaceTimeKey+Tile pair for a valid extent."""
        import calendar

        instant_ms = calendar.timegm(datetime(2024, 9, 2, 10, 5).timetuple()) * 1000
        features = [
            {
                "key": {"col": 0, "row": 0, "instant": instant_ms},
                "key_extent": {"xmin": 4.0, "ymin": 50.5, "xmax": 4.9, "ymax": 51.1},
                "key_epsg": 4326,
            }
        ]
        result = read_product(
            (SYNTHETIC_CO_FILE, features),
            band_names=["carbonmonoxide_total_column_corrected", "carbonmonoxide_total_column"],
            tile_size=4,
            resolution=0.1,
        )
        assert len(result) > 0, "Expected at least one tile"
        key, tile = result[0]
        assert tile.cells.shape[0] == 2, "Expected 2 bands"
        assert tile.cells.shape[1] == 4, "Expected tile_size rows"
        assert tile.cells.shape[2] == 4, "Expected tile_size cols"

    def test_read_product_default_bands(self):
        """read_product uses default CO band when band_names is empty."""
        import calendar

        instant_ms = calendar.timegm(datetime(2024, 9, 2, 10, 5).timetuple()) * 1000
        features = [
            {
                "key": {"col": 0, "row": 0, "instant": instant_ms},
                "key_extent": {"xmin": 4.0, "ymin": 50.5, "xmax": 4.9, "ymax": 51.1},
                "key_epsg": 4326,
            }
        ]
        result = read_product(
            (SYNTHETIC_CO_FILE, features),
            band_names=[],
            tile_size=4,
            resolution=0.1,
        )
        assert len(result) > 0
        _key, tile = result[0]
        assert tile.cells.shape[0] == 1, "Expected 1 default band"

    def test_read_product_no_data_outside_extent(self):
        """read_product returns empty list when spatial extent has no data."""
        import calendar

        instant_ms = calendar.timegm(datetime(2024, 9, 2, 10, 5).timetuple()) * 1000
        features = [
            {
                "key": {"col": 0, "row": 0, "instant": instant_ms},
                "key_extent": {"xmin": 10.0, "ymin": 10.0, "xmax": 11.0, "ymax": 11.0},
                "key_epsg": 4326,
            }
        ]
        result = read_product(
            (SYNTHETIC_CO_FILE, features),
            band_names=["carbonmonoxide_total_column_corrected"],
            tile_size=4,
            resolution=0.1,
        )
        assert result == [], "Expected empty list for extent with no data"

    def test_read_product_spacetimekey_instant(self):
        """SpaceTimeKey instant is rounded down to the minute."""
        import calendar

        # Use calendar.timegm to create a UTC-based Unix timestamp (10:05:30 UTC)
        # 10:05:30 UTC → should round to 10:05:00
        instant_ms = calendar.timegm(datetime(2024, 9, 2, 10, 5, 30).timetuple()) * 1000
        features = [
            {
                "key": {"col": 0, "row": 0, "instant": instant_ms},
                "key_extent": {"xmin": 4.0, "ymin": 50.5, "xmax": 4.9, "ymax": 51.1},
                "key_epsg": 4326,
            }
        ]
        result = read_product(
            (SYNTHETIC_CO_FILE, features),
            band_names=["carbonmonoxide_total_column_corrected"],
            tile_size=4,
            resolution=0.1,
        )
        assert len(result) > 0
        key, _ = result[0]
        assert key.instant == datetime(2024, 9, 2, 10, 5, 0), f"Expected rounded instant, got {key.instant}"

    def test_sentinel5p_l2_co(self, api110, tmp_path) -> None:
        process_graph = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "SENTINEL5P_L2_CO",
                    "spatial_extent": {"west": 4, "south": 50, "east": 11, "north": 55},
                    "temporal_extent": ("2024-09-02T00:00:00Z", "2024-09-02T23:59:59Z"),
                    "bands": [
                        "carbonmonoxide_total_column",
                        "carbonmonoxide_total_column_corrected",
                        "qa_value",
                    ],
                },
                "result": True,
            },
        }
        response = api110.check_result(process_graph)

        output_file = tmp_path / "test_sentinel5p_l2_co.tif"
        with output_file.open(mode="wb") as f:
            f.write(response.data)

        with rasterio.open(output_file) as ds:
            print(ds.bounds)
            assert ds.bounds.right == 11.0

    def test_invalid_time_exception(self):
        params = {
            "filename": self.filename,
            "temporal_extent": self.temporal_extent_invalid,
        }
        with pytest.raises(Exception) as excinfo:
            _ = load_level2_data(params)
        assert ["Input temporal extent is not in the file" in str(excinfo.value)]

    def test_invalid_spatial_extent_exception(self):
        params = {
            "filename": self.filename,
            "spatial_extent": self.spatial_extent_invalid,
            "temporal_extent": None,
        }
        with pytest.raises(Exception) as excinfo:
            _ = load_level2_data(params)
        assert "Input spatial extent is not in the file" in str(excinfo.value)

    def test_data_availability_exception(self):
        """Valid temporal and spatial extents in the file but when combined there is no data."""
        params = {
            "filename": self.filename,
            "spatial_extent": self.spatial_extent_normal,
            "temporal_extent": [datetime(2024, 9, 2, 10, 5, 0), datetime(2024, 9, 2, 10, 10, 0)],
        }
        with pytest.raises(Exception) as excinfo:
            _ = load_level2_data(params)
        assert "No data is available for given spatial and temporal extent" in str(excinfo.value)

    def test_data_availability_based_on_filter_exception(self):
        """No data based on filter_value."""
        params = {
            "filename": self.filename,
            "spatial_extent": [45, 11, 46, 12],
            "temporal_extent": self.temporal_extent_valid,
            "filter_value": 0.5,
        }
        with pytest.raises(Exception) as excinfo:
            _ = load_level2_data(params)
        assert "No data is available after applying quality filter" in str(excinfo.value)

    def test_data_loading_co(self):
        """Test if it loads all bands, data and shape of bands."""
        params = {
            "filename": self.filename,
            "spatial_extent": [35, 24, 35.05, 24.05],
            "temporal_extent": self.temporal_extent_valid,
            "bands": ["carbonmonoxide_total_column_corrected", "carbonmonoxide_total_column", "qa_value"],
            "filter_value": 0.5,
            "resample_factor": [False, 0.025, "nearest"],
        }
        co_corr = np.array(
            [[np.nan, 0.03327221, 0.03151973], [0.03266068, 0.03381333, np.nan], [np.nan, 0.03140356, np.nan]]
        )
        data = load_level2_data(params)
        assert "carbonmonoxide_total_column_corrected" in data
        assert "carbonmonoxide_total_column" in data
        assert "qa_value" in data
        assert np.allclose(data["carbonmonoxide_total_column_corrected"], co_corr, equal_nan=True)
        assert data["carbonmonoxide_total_column"].shape == (3, 3)
        assert data["qa_value"].shape == (3, 3)

    def test_data_loading_with_resampling(self):
        params = {
            "filename": self.filename,
            "spatial_extent": [35, 24, 35.05, 24.05],
            "temporal_extent": self.temporal_extent_valid,
            "bands": ["carbonmonoxide_total_column_corrected", "carbonmonoxide_total_column", "qa_value"],
            "filter_value": 0.5,
            "resample_factor": [True, 0.025, "nearest"],
        }
        co = np.array([0.03099886, 0.03340864, 0.03340864, 0.03340864])
        co_corr = np.array([0.03140356, 0.03381333, 0.03381333, 0.03381333])
        lat = np.array([24.0375, 24.0375, 24.0125, 24.0125])
        lon = np.array([35.0125, 35.0375, 35.0125, 35.0375])
        data = load_level2_data(params)
        assert "carbonmonoxide_total_column_corrected" in data
        assert "carbonmonoxide_total_column" in data
        assert "qa_value" in data
        assert np.allclose(data["carbonmonoxide_total_column_corrected"].ravel(), co_corr, equal_nan=True)
        assert np.allclose(data["carbonmonoxide_total_column"].ravel(), co, equal_nan=True)
        assert np.allclose(data["latitude"].ravel(), lat, equal_nan=True)
        assert np.allclose(data["longitude"].ravel(), lon, equal_nan=True)

    def test_data_loading_with_antimeridian_crossing(self):
        """Test loading data that crosses the antimeridian."""
        params = {
            "filename": str(self.filename_anti),
            "spatial_extent": self.spatial_extent_anti,
            "temporal_extent": self.temporal_extent_anti,
        }
        data = load_level2_data(params)
        params1 = {
            "filename": str(self.filename_anti),
            "spatial_extent": [179.5, 22, 179.99, 23],
            "temporal_extent": self.temporal_extent_anti,
        }
        data1 = load_level2_data(params1)
        params2 = {
            "filename": str(self.filename_anti),
            "spatial_extent": [-179.99, 22, -179.5, 23],
            "temporal_extent": self.temporal_extent_anti,
        }
        data2 = load_level2_data(params2)
        # assert first 5 lines of data1 lon and data lon match
        assert np.allclose(data["longitude"][2:, :5], data1["longitude"][:, :5])
        # assert last 5 lines of data1 lon and data lon match
        assert np.allclose(data["longitude"][:-2, -5:], data2["longitude"][:, -5:])
        # assert first 5 lines of data1 carbonmonoxide_total_column_corrected and
        # data carbonmonoxide_total_column_corrected match
        assert np.allclose(
            data["carbonmonoxide_total_column_corrected"][2:, :5],
            data1["carbonmonoxide_total_column_corrected"][:, :5],
            equal_nan=True,
        )
        # assert last 5 lines of data2 carbonmonoxide_total_column_corrected and
        # data carbonmonoxide_total_column_corrected match
        assert np.allclose(
            data["carbonmonoxide_total_column_corrected"][:-2, -5:],
            data2["carbonmonoxide_total_column_corrected"][:, -5:],
            equal_nan=True,
        )

    def test_data_loading_no2(self):
        """Test if it loads all bands, data and shape of bands."""
        params = {
            "filename": self.filename_no2,
            "spatial_extent": self.spatial_extent_no2,
            "temporal_extent": self.temporal_extent_no2,
            "bands": ["nitrogendioxide_tropospheric_column", "qa_value"],
            "filter_value": 0.75,
            "resample_factor": [False, 0.025, "nearest"],
        }
        data = load_level2_data(params)
        no2_act = np.array([[np.nan, 3.6674388e-05], [2.5847688e-05, 3.4489720e-05], [2.9435478e-05, 1.3215038e-05]])
        assert "nitrogendioxide_tropospheric_column" in data
        assert "qa_value" in data
        assert np.allclose(data["nitrogendioxide_tropospheric_column"], no2_act, equal_nan=True)
        assert data["nitrogendioxide_tropospheric_column"].shape == data["qa_value"].shape
