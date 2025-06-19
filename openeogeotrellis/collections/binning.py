""" This module implements spatial binning that can be used to avoid issues with nearest neighbor resampling
while preserving physical spectra"""
from collections.abc import Iterable, Mapping
from typing import Tuple

from scipy.interpolate import griddata, make_interp_spline
from numpy import floating
from numpy.typing import NDArray
import xarray as xr
import numpy as np

# TODO find out which type ``grid`` should be
def bin_to_grid(ds: xr.Dataset, bands: Iterable[str], lat_edges, lon_edges, *, aggregation="mean", super_sampling: int=1) -> xr.Dataset:
    if aggregation == "mean":
        return  _bin_with_mean_statistic(ds, bands, lat_edges, lon_edges, super_sampling=super_sampling)
    else:
        raise ValueError("Only mean aggregation is supported")


def _bin_generic():
    """
    Binning implementation that supports multiple aggregators via scipy ``binned_statistic``.
    """
    pass


def _bin_with_mean_statistic(ds: xr.Dataset, bands: Iterable[str], lat_edges, lon_edges, *, super_sampling: int=1) -> xr.Dataset:
    """
    Optimized implementation of mean binning using ``np.histogram`` to sum the input values in each bin.
    """
    # TODO describe what properties ds should have

    assert super_sampling > 0, "Super sampling must be greater than 0 (1 means no super sampling)"
    lat2d = ds["lat"].data
    lon2d = ds["lon"].data

    lat = _super_sample_2d(lat2d, super_sampling, kind="linear").ravel()
    lon = _super_sample_2d(lon2d, super_sampling, kind="linear").ravel()

    width = lon_edges.shape[0] - 1
    height = lat_edges.shape[0] - 1

    pixel_size = (lon_edges[-1] - lon_edges[0]) / width

    # Reuse for outputs
    bin_idx = _compute_bin_index(lat, lon, (lat_edges[0], lon_edges[0]), pixel_size, output_grid_height=height, output_grid_width=width)

    # counts = Number of source pixels combined in one target pixel
    counts, _ = np.histogram(bin_idx, width * height, range=(0, width*height))

    binned = {}
    means = None
    # TODO can pre-allocate here for all bands
    sampled_data = None

    for band in bands:
        data = ds[band].data
        if super_sampling != 1:
            sampled_data = _super_sample_2d(data, super_sampling)
        else:
            sampled_data = data

        # Promote datatype to avoid overflows
        if isinstance(sampled_data.dtype, np.integer):
            if sampled_data.dtype.itemsize < 4:
                sampled_data = sampled_data.astype(np.int32)
            elif sampled_data.dtype.itemsize < 8:
                sampled_data = sampled_data.astype(np.int64)
        hist, _ = np.histogram(bin_idx, range(width * height + 1), weights=sampled_data.reshape(-1), range=(0, width*height))
        # TODO what if counts is zero?
        means = np.divide(hist, counts, out=means)
        # It is important to make a copy here, event if not scaling.
        # Otherwise, means will be overwritten
        scaled_means = means.reshape(height, width) * 1.0
        binned[band] = scaled_means

    lat_centers = _compute_pixel_centers_from_edges(lat_edges, pixel_size)
    lon_centers = _compute_pixel_centers_from_edges(lon_edges, pixel_size)

    binned = xr.Dataset(
        data_vars={
            band: (("lat", "lon"), binned[band]) for band in bands
        },
        coords={
            "lat": lat_centers,
            "lon": lon_centers,
        },
    )
    return binned


def _compute_bin_index(input_pixel_centers_lat: NDArray[float], input_pixel_centers_lon: NDArray[float], output_grid_edges: Tuple[float, float], output_grid_pixel_size: float, output_grid_height: int, output_grid_width: int) -> NDArray[int]:
    """
    Compute the bin index in lat and lon for each pixel i with centers defined by ``input_pixel_centers_lat[i]``
    and ``input_pixel_centers_lon[i]`` on the output grid defined by ``output_grid_edges``, ``output_grid_width``
    and ``output_grid_width``.

    :param input_pixel_centers_lat: Array of pixel **center** latitudes to be binned to the target grid.
    :param input_pixel_centers_lon: Array of pixel **center** longitudes to be binned to the target grid.
    :param output_grid_edges: Pixel **edges** (lat, lon) of the lowest value in the target grid.
    :param output_grid_pixel_size: Pixel size of the target grid.
    :param output_grid_height: Number of latitude values of the target grid.
    :param output_grid_width: Number of longitude values of the target grid.

    :returns: For each pixel in ``input_pixel_center`` the linear index of its grid cell in the flattened target grid.
    """
    bin_idx_row =  np.divide(input_pixel_centers_lat - output_grid_edges[0], output_grid_pixel_size).astype(int)
    bin_idx_col =  np.divide(input_pixel_centers_lon - output_grid_edges[1], output_grid_pixel_size).astype(int)
    bin_idx = bin_idx_row * output_grid_width + bin_idx_col
    bin_idx[(bin_idx_row < 0) | (bin_idx_row > output_grid_height) | (bin_idx_col < 0) | (bin_idx_col > output_grid_width)] = -1
    return bin_idx

def _super_sample_2d(arr, factor, *, kind="nearest"):
    """
    Create a super sampled version of the input array ``arr``. Each pixel is repeated ``factor`` times in two dimensions.

    :param arr: Input array for super-sampling.
    :param factor: Super sampling factor.
    :returns: Super sampled array of shape ``[d * factor for d in arr.shape]``.
    """
    if factor == 1:
        return arr
    # more efficient implementation (requires cv2):
    # out = np.zeros_like(arr, shape=(arr.shape[0] * factor, arr.shape[1] * factor)
    # cv2.resize(arr, dst=out, dsize=out.shape[::-1], fx=factor, fy=factor, interpolation=cv2.INTER_NEAREST)
    # return out
    if kind == "nearest":
        return arr.repeat(factor, axis=1).repeat(factor, axis=0)
    elif kind == "linear":
        # TODO check order of xi
        cols = np.arange(arr.shape[1])
        rows = np.arange(arr.shape[0])
        xp_x, xp_y = np.meshgrid(cols, rows)
        x_x, x_y = np.meshgrid(_super_sample_1d(cols, factor, kind=kind), _super_sample_1d(rows, factor, kind=kind))
        interpolated = griddata((xp_y.ravel(), xp_x.ravel()), arr.reshape(-1), (x_y.ravel(), x_x.ravel()), method=kind).reshape(arr.shape[0] * factor, -1)
        extrapolated = _extrapolate_edges_2d(interpolated, factor // 2)
        return extrapolated
    else:
        raise ValueError(f"Super sampling kind must be 'nearest' or 'linear', found {kind}")

def _super_sample_1d(arr, factor, *, kind="nearest"):
    """

    """
    # TODO docstring
    if factor == 1:
        return arr
    if kind == "nearest":
        return arr.repeat(factor)
    elif kind == "linear":
        xp = np.arange(0, len(arr))
        if factor & 1: # odd
            shift = 1 / factor * (factor // 2)
        else:
            shift = 1 / factor * ((factor // 2) - 0.5)
        x = np.linspace(xp[0]-shift, xp[-1]+shift, factor * xp.shape[0])
        spl = make_interp_spline(xp, arr, k=1) # linear
        return spl(x)

        #return np.interp(x, xp, arr)
    else:
        raise ValueError(f"Unknown kind: {kind}")

def _compute_grid_bin_edges(bbox: Mapping[str, float], num_rows: int) -> Tuple[NDArray[floating], NDArray[floating]]:
    """
    Create an array each of latitude and longitude bin edges inside the provided bounding box for a plate carrée
    projection that divides latitude into ``num_rows`` equally spaced rows.

    :param bbox: The bounding box in lat/lon
    :param num_rows: Number of rows in the grid
    :returns: Arrays of the bin edges for latitude/longitude. For the entire earth, the entries have
        ``num_rows`` + 1 entries.
    """

    lat = np.linspace(0, 180, num=num_rows + 1, endpoint=True) - 90
    lon = np.linspace(0, 360, num=num_rows * 2 + 1, endpoint=True) - 180

    # TODO can be computed directly more efficiently (lat is sorted)
    lat_idx_min = np.argmax(lat > bbox["south"]) - 1
    lat_idx_max = np.argmax(lat >= bbox["north"])
    lon_idx_min = np.argmax(lon > bbox["west"]) - 1
    lon_idx_max = np.argmax(lon >= bbox["east"])

    res = lat[lat_idx_min:lat_idx_max + 1], lon[lon_idx_min:lon_idx_max + 1]
    return res

def _compute_pixel_centers_from_edges(edges, pixel_size):
    return edges[:-1] + 0.5 * pixel_size


def _extrapolate_edges_2d(arr: NDArray, border: int) -> NDArray:
    """
    In-place extrapolation of interpolated results

    arr should already contain the border with values that will be overwritten
    border is an int, used for all dimensions
    """
    # TODO doc string
    assert (arr.shape[0] > border * 2) and (arr.shape[1] > border * 2), (
        "The border must be fully contained in the array (no overlap). Found "
        f"shape {arr.shape} and border size {border}."
        )

    for i in range(border, 0, -1):
        # border = 1: i = 1
        # top
        #  [0, :] =    [1,   :] -    [2, :]
        arr[i-1, :] = 2 * arr[i, :] - arr[i+1, :]
        # bottom
        #  [-1, :] =        [-2,   :] -    [-3, :]
        arr[-i, :] = 2 * arr[-(i+1), :] - arr[-(i+2), :]
        # left
        arr[:, i-1] = 2 * arr[:, i] - arr[:, i+1]
        # right
        arr[:, -i] = 2 * arr[:, -(i+1)] - arr[:, -(i+2)]

    return arr