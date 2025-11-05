import pytest
from pathlib import Path
from datetime import datetime
from netCDF4 import Dataset, num2date
import numpy as np

import rasterio

if __name__ == "__main__":
    import openeogeotrellis.deploy.local

    # Allow to run from commandline:
    # /usr/bin/time -v python3 tests/data_collections/test_sentinel3.py
    # python3 -m memory_profiler tests/data_collections/test_sentinel3.py
    # The SparkContext is only needed to make imports work, but is actually not used for the tests
    openeogeotrellis.deploy.local.setup_environment()

from openeogeotrellis.collections.sentinel5p_CO import *
from numpy.testing import assert_allclose

# important to get these files locally for testing
filename = "Sentinel5data/S5P_OFFL_L2__CO_____20240902T094132_20240902T112301_35696_03_020600_20240903T232407.nc"
filename_anti = "Sentinel5data/S5P_RPRO_L2__CO_____20180430T001950_20180430T020120_02818_03_020400_20220901T170054.nc"
temporal_extent_anti = [datetime(2018, 4, 30, 0, 50, 0), datetime(2018, 4, 30, 1, 30, 0)]
spatial_extent_anti = [179.5, 22, -179.5, 23]  # min_lon, min_lat, max_lon, max_lat

temporal_extent_valid = [datetime(2024, 9, 2, 10, 30, 0), datetime(2024, 9, 2, 11, 0, 0)]
temporal_extent_invalid = [datetime(2024, 9, 2, 11, 30, 0), datetime(2024, 9, 2, 11, 35, 0)]
spatial_extent = [30.0, 25.0, 30.05, 25.05]  # min_lon, min_lat, max_lon, max_lat
spatial_extent_invalid = [22.0, 24.0, 24.0, 26.0]  # min_lon, min_lat, max_lon, max_lat


def test_co_invalid_time_exception():
    params = {
        "filename": filename,
        "temporal_extent": temporal_extent_invalid,
    }
    with pytest.raises(Exception) as excinfo:
        _ = load_carbonmonoxide(params)
    assert ["Input temporal extent is not in the file" in str(excinfo.value)]


def test_co_invalid_spatial_extent_exception():
    params = {
        "filename": filename,
        "spatial_extent": spatial_extent_invalid,
        "temporal_extent": None,
    }
    with pytest.raises(Exception) as excinfo:
        _ = load_carbonmonoxide(params)
    assert "Input spatial extent is not in the file" in str(excinfo.value)


def test_co_data_availability_exception():
    """Valid temporal and spatial extents in the file but when combined there is no data."""
    params = {
        "filename": filename,
        "spatial_extent": spatial_extent,
        "temporal_extent": [datetime(2024, 9, 2, 10, 5, 0), datetime(2024, 9, 2, 10, 10, 0)],
    }
    with pytest.raises(Exception) as excinfo:
        _ = load_carbonmonoxide(params)
    assert "No data is available for given spatial and temporal extent" in str(excinfo.value)


def test_co_data_availability_based_on_filter_exception():
    """No data based on filter_value."""
    params = {
        "filename": filename,
        "spatial_extent": [45, 11, 46, 12],
        "temporal_extent": temporal_extent_valid,
        "filter_value": 0.5,
    }
    with pytest.raises(Exception) as excinfo:
        _ = load_carbonmonoxide(params)
    assert "No data is available after applying quality filter" in str(excinfo.value)


def test_co_data_loading():
    """Test if it loads all bands, data and shape of bands."""
    params = {
        "filename": filename,
        "spatial_extent": [35, 24, 35.05, 24.05],
        "temporal_extent": temporal_extent_valid,
        "bands": ["co_corrected", "co", "qa_value"],
        "filter_value": 0.5,
        "resample_factor": [False, 0.025, "nearest"],
    }
    co_corr = np.array(
        [[np.nan, 0.03327221, 0.03151973], [0.03266068, 0.03381333, np.nan], [np.nan, 0.03140356, np.nan]]
    )
    data = load_carbonmonoxide(params)
    assert "co_corrected" in data
    assert "co" in data
    assert "qa_value" in data
    assert np.allclose(data["co_corrected"], co_corr, equal_nan=True)
    assert data["co"].shape == (3, 3)
    assert data["qa_value"].shape == (3, 3)


def test_co_data_loading_with_resampling():
    params = {
        "filename": filename,
        "spatial_extent": [35, 24, 35.05, 24.05],
        "temporal_extent": temporal_extent_valid,
        "bands": ["co_corrected", "co", "qa_value"],
        "filter_value": 0.5,
        "resample_factor": [True, 0.025, "nearest"],
    }
    co = np.array([0.03099886, 0.03340864, 0.03340864, 0.03340864])
    co_corr = np.array([0.03140356, 0.03381333, 0.03381333, 0.03381333])
    lat = np.array([24.0375, 24.0375, 24.0125, 24.0125])
    lon = np.array([35.0125, 35.0375, 35.0125, 35.0375])
    data = load_carbonmonoxide(params)
    assert "co_corrected" in data
    assert "co" in data
    assert "qa_value" in data
    assert np.allclose(data["co_corrected"].ravel(), co_corr, equal_nan=True)
    assert np.allclose(data["co"].ravel(), co, equal_nan=True)
    assert np.allclose(data["lat"].ravel(), lat, equal_nan=True)
    assert np.allclose(data["lon"].ravel(), lon, equal_nan=True)


def test_co_data_loading_with_antimeridian_crossing():
    """Test loading data that crosses the antimeridian."""
    filename_anti = "/home/manu/Documents/git/OpenEO/Sentinel5data/S5P_RPRO_L2__CO_____20180430T001950_20180430T020120_02818_03_020400_20220901T170054.nc"
    temporal_extent = [datetime(2018, 4, 30, 0, 50, 0), datetime(2018, 4, 30, 1, 30, 0)]
    spatial_extent = [179.5, 22, -179.5, 23]  # min_lon, min_lat, max_lon, max_lat
    params = {
        "filename": str(filename_anti),
        "spatial_extent": spatial_extent,
        "temporal_extent": temporal_extent,
    }
    data = load_carbonmonoxide(params)
    params1 = {
        "filename": str(filename_anti),
        "spatial_extent": [179.5, 22, 179.99, 23],
        "temporal_extent": temporal_extent,
    }
    data1 = load_carbonmonoxide(params1)
    params2 = {
        "filename": str(filename_anti),
        "spatial_extent": [-179.99, 22, -179.5, 23],
        "temporal_extent": temporal_extent,
    }
    data2 = load_carbonmonoxide(params2)
    # assert first 5 lines of data1 lon and data lon match
    assert np.allclose(data["lon"][2:, :5], data1["lon"][:,:5])
    # assert last 5 lines of data1 lon and data lon match
    assert np.allclose(data["lon"][:-2, -5:], data2["lon"][:,-5:])
    # assert first 5 lines of data1 co_corrected and data co_corrected match
    assert np.allclose(data["co_corrected"][2:, :5], data1["co_corrected"][:,:5], equal_nan=True)
    # assert last 5 lines of data2 co_corrected and data co_corrected match
    assert np.allclose(data["co_corrected"][:-2, -5:], data2["co_corrected"][:,-5:], equal_nan=True)

