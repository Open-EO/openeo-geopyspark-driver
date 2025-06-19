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


def test_bin_to_five_times_finer_grid_no_supersampling(test_input_ds, input_grid_edges, aoi_size_degrees, num_rows_global):
    lat_edges, lon_edges = input_grid_edges
    expected = test_input_ds.band1.values
    finite_values_mask = np.zeros((test_input_ds["band1"].shape[0] * 5, test_input_ds["band1"].shape[1] * 5), dtype=bool)
    finite_values_mask[2::5, 2::5] = 1

    binned = binning.bin_to_grid(test_input_ds, ["band1"], binning._super_sample_1d(lat_edges, 5, kind="linear")[2:-2], binning._super_sample_1d(lon_edges, 5, kind="linear")[2:-2], super_sampling=1)

    assert np.allclose(binned.band1.values[finite_values_mask], expected.ravel(), 1e-3)
    assert np.isnan(binned.band1.values[~finite_values_mask]).all()


def test_bin_to_five_times_finer_grid_with_supersampling(test_input_ds, input_grid_edges, aoi_size_degrees, num_rows_global):
    lat_edges, lon_edges = input_grid_edges
    original_values_mask = np.zeros((test_input_ds["band1"].shape[0] * 5, test_input_ds["band1"].shape[1] * 5), dtype=bool)
    original_values_mask[2::5, 2::5] = 1
    expected = test_input_ds.band1.values.ravel()

    binned = binning.bin_to_grid(test_input_ds, ["band1"], binning._super_sample_1d(lat_edges, 5, kind="linear")[2:-2], binning._super_sample_1d(lon_edges, 5, kind="linear")[2:-2], super_sampling=5)

    assert np.allclose(binned.band1.values[original_values_mask], expected, 1e-3)
    assert np.logical_not(np.isnan(binned.band1.values).any())


@pytest.mark.parametrize("factor", [2, 3, 4, 5])
def test_super_sample_1d_linear(factor):
    x = np.arange(10)
    if factor & 1: # odd
        shift = 1 / factor * (factor // 2)
    else:
        shift = 1 / factor * ((factor // 2) - 0.5)
    expected = np.linspace(x[0]-shift, x[-1]+shift, factor * x.shape[0])

    super_sampled = binning._super_sample_1d(x, factor, kind="linear")

    assert super_sampled.shape[0] == factor * x.shape[0]
    assert np.allclose(super_sampled, expected, EPS)

@pytest.mark.parametrize("factor", [2, 3])
def test_super_sample_2d_linear(factor):
    x = np.arange(9).reshape(3, 3) * factor
    expected_y = binning._super_sample_1d(x[:, 0], factor, kind="linear")
    expected_x = binning._super_sample_1d(x[0, :], factor, kind="linear")
    expected = expected_y[:, np.newaxis] + expected_x[np.newaxis, :]

    super_sampled = binning._super_sample_2d(x, factor, kind="linear")

    assert np.allclose(super_sampled, expected, EPS)

def test_extrapolate_edges_2d():
    arr = np.array([
        [1, 2, 3],
        [5, 6, 7],
    ], dtype=np.float64)

    expected_1 = np.array([
        [-4, -3, -2, -1,  0],
        [ 0,  1,  2,  3,  4],
        [ 4,  5,  6,  7,  8],
        [ 8,  9, 10, 11, 12]
    ], dtype=np.float64)

    expected_2 = np.array([
        [-9, -8, -7, -6, -5, -4, -3],
        [-5, -4, -3, -2, -1,  0,  1],
        [-1,  0,  1,  2,  3,  4,  5],
        [ 3,  4,  5,  6,  7,  8,  9],
        [ 7,  8,  9, 10, 11, 12, 13],
        [11, 12, 13, 14, 15, 16, 17]
    ], dtype=np.float64)

    inp_1 = np.pad(arr, pad_width=1, mode="constant", constant_values=np.nan)
    inp_2 = np.pad(arr, pad_width=2, mode="constant", constant_values=np.nan)

    res_1 = binning._extrapolate_edges_2d(inp_1.copy(), 1)
    res_2 = binning._extrapolate_edges_2d(inp_2.copy(), 2)

    inp_1_2 = np.pad(res_1, pad_width=1, mode="constant", constant_values=np.nan)

    res_1_2 = binning._extrapolate_edges_2d(inp_1_2.copy(), 1)

    assert np.allclose(res_1, expected_1)
    assert np.allclose(res_2, expected_2)
    assert np.allclose(res_1_2, expected_2)
