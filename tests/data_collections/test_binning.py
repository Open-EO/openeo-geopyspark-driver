import pytest
import numpy as np
import xarray as xr

from openeogeotrellis.collections import binning

EPS = 1e-5

@pytest.fixture
def num_rows_global():
    return 1800

@pytest.fixture
def aoi_size_degrees(num_rows_global):
    return 1

@pytest.fixture
def input_grid_edges(num_rows_global, aoi_size_degrees):
    bbox = {
        "south": 1,
        "north": 1 + aoi_size_degrees,
        "west": 1,
        "east": 1 + aoi_size_degrees,
    }
    lat_edges, lon_edges = binning._compute_grid_bin_edges(bbox, num_rows_global)
    return lat_edges, lon_edges


@pytest.fixture
def test_input_ds(input_grid_edges, num_rows_global, aoi_size_degrees):
    lat_edges, lon_edges = input_grid_edges
    pixel_size = lat_edges[1] - lat_edges[0]
    lat_centers, lon_centers = lat_edges[:-1] + 0.5 * pixel_size, lon_edges[:-1] + 0.5 * pixel_size
    pixels_per_dim = aoi_size_degrees * num_rows_global  // 180

    assert len(lat_centers) == pixels_per_dim
    assert len(lon_centers) == pixels_per_dim

    band_1 = np.arange(pixels_per_dim * pixels_per_dim).astype(float).reshape(pixels_per_dim, pixels_per_dim)
    band_2 = np.linspace(0, stop=pixels_per_dim * pixels_per_dim, num=pixels_per_dim * pixels_per_dim, dtype=float).reshape(pixels_per_dim, pixels_per_dim)
    band_3 = np.ones_like(band_1)

    lat = lat_centers[:, np.newaxis].repeat(pixels_per_dim, axis=1)
    lon = lon_centers[np.newaxis].repeat(pixels_per_dim, axis=0)
    dims = ("lat", "lon")
    # TODO change to meaningful band names
    ds = xr.Dataset(
        data_vars={
            "band1": (dims, band_1),
            "band2": (dims, band_2),
            "band3": (dims, band_3),
        },
        coords={"lat": (("y", "x"),  lat), "lon": (("y", "x"), lon)},
    )
    return ds

def test_bin_to_same_grid(test_input_ds, input_grid_edges):
    lat_edges, lon_edges = input_grid_edges
    binned = binning.bin_to_grid(test_input_ds, ["band1", "band2"], lat_edges, lon_edges)

    assert np.allclose(test_input_ds.band1, binned.band1.values, 1e-3)
    assert np.allclose(test_input_ds.band2, binned.band2.values, 1e-3)


def test_bin_to_five_times_coarser_grid(test_input_ds, input_grid_edges, aoi_size_degrees, num_rows_global):
    lat_edges, lon_edges = input_grid_edges

    block = 10 * np.arange(5)[:, np.newaxis] + np.arange(5)
    expected = [
        [np.mean(block), np.mean(block + 5)],
        [np.mean(block + 50), np.mean(block + 55)],
    ]

    binned = binning.bin_to_grid(test_input_ds, ["band1"], lat_edges[::5], lon_edges[::5])

    assert np.allclose(binned.band1.values, expected, 1e-3)


def test_bin_to_five_times_finer_grid(test_input_ds, input_grid_edges, aoi_size_degrees, num_rows_global):
    lat_edges, lon_edges = input_grid_edges
    expected = None

    binned = binning.bin_to_grid(test_input_ds, ["band1"], binning._super_sample_1d(lat_edges, 5), binning._super_sample_1d(lon_edges, 5), super_sampling=2)

    assert np.allclose(binned.band1.values, expected, 1e-3)


@pytest.mark.parametrize("factor", [2, 3, 4])
def test_super_sample_1d_linear(factor):
    x = np.arange(10)
    expected = (np.arange(10 * factor) / factor)[:-(factor-1)]

    super_sampled = binning._super_sample_1d(x, factor, kind="linear")

    assert np.allclose(super_sampled, expected, EPS)

def test_super_sample_2d_linear():
    factor = 2
    x = np.arange(25).reshape(5, 5) * 2
    expected = np.arange(9)[np.newaxis, :].repeat(9, axis=0) + np.arange(0, 45, 5)[:, np.newaxis]

    super_sampled = binning._super_sample_2d(x, factor, kind="linear")

    assert np.allclose(super_sampled, expected, EPS)