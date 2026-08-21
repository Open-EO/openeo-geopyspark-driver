"""Module to load Sentinel-5P satellite data from a NetCDF file.

This module provides functionality to read and filter different level-2
data from Sentinel-5P NetCDF files based on specified spatial and temporal
extents, as well as quality filtering.

Everything should happen in EPSG: 4326 (lat-lon) as Sentinel-5P data is in lat-lon grid.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Sequence

import numpy as np
from netCDF4 import Dataset, num2date

from openeogeotrellis.utils import typechecked

############# DO NOT CHANGE THE VARIABLE NAMES BELOW #############
# The following variables are defined to specify the paths
# to various data fields within the NetCDF file. These are also
# possible bands
COMMON_VARIABLES_IN_FILE = {
    "time": "PRODUCT/time",
    "delta_time": "PRODUCT/delta_time",
    "latitude": "PRODUCT/latitude",
    "longitude": "PRODUCT/longitude",
    "qa_value": "PRODUCT/qa_value",
}

all_gases: dict[str, dict[str, Any]] = {
    # CO gas variables
    "gas_co": {
        "VARIABLE_LOC_IN_FILE": {
            "carbonmonoxide_total_column": "PRODUCT/carbonmonoxide_total_column",  # raw data
            "carbonmonoxide_total_column_corrected": "PRODUCT/carbonmonoxide_total_column_corrected",
        },
        "DEFAULT_BANDS": ["carbonmonoxide_total_column_corrected"],
        "FILTER_VALUE": 0.5,  # default filter value for CO as per documentation
    },
    # NO2 gas variables
    "gas_no2": {
        "VARIABLE_LOC_IN_FILE": {
            "nitrogendioxide_tropospheric_column": "PRODUCT/nitrogendioxide_tropospheric_column",  # raw data
        },
        "DEFAULT_BANDS": ["nitrogendioxide_tropospheric_column"],
        "FILTER_VALUE": 0.75,  # default filter value for NO2 as per documentation
    },
    # CH4 gas variables
    "gas_ch4": {
        "VARIABLE_LOC_IN_FILE": {
            "methane_mixing_ratio": "PRODUCT/methane_mixing_ratio",
            "methane_mixing_ratio_bias_corrected": "PRODUCT/methane_mixing_ratio_bias_corrected",
        },
        "DEFAULT_BANDS": ["methane_mixing_ratio_bias_corrected"],
        "FILTER_VALUE": 0.5,  # default filter value for CH4 as per documentation
    },
    # SO2 gas variables
    "gas_so2": {
        "VARIABLE_LOC_IN_FILE": {
            "sulfurdioxide_total_vertical_column": "PRODUCT/sulfurdioxide_total_vertical_column",
        },
        "DEFAULT_BANDS": ["sulfurdioxide_total_vertical_column"],
        "FILTER_VALUE": 0.5,  # default filter value for SO2 as per documentation
    },
    # HCHO gas variables
    "gas_hcho": {
        "VARIABLE_LOC_IN_FILE": {
            "formaldehyde_tropospheric_vertical_column": "PRODUCT/formaldehyde_tropospheric_vertical_column",
        },
        "DEFAULT_BANDS": ["formaldehyde_tropospheric_vertical_column"],
        "FILTER_VALUE": 0.5,  # default filter value for HCHO as per documentation
    },
    # O3 gas variables
    "gas_o3": {
        "VARIABLE_LOC_IN_FILE": {
            "ozone_total_vertical_column": "PRODUCT/ozone_total_vertical_column",
        },
        "DEFAULT_BANDS": ["ozone_total_vertical_column"],
        "FILTER_VALUE": 0.5,  # default filter value for O3 as per documentation
    },
    # AER_AI (UV Aerosol Index) gas variables: both the 340/380 nm and the
    # 354/388 nm wavelength pairs are present in the same PRODUCT group.
    "gas_aer_ai": {
        "VARIABLE_LOC_IN_FILE": {
            "aerosol_index_340_380": "PRODUCT/aerosol_index_340_380",
            "aerosol_index_354_388": "PRODUCT/aerosol_index_354_388",
        },
        "DEFAULT_BANDS": ["aerosol_index_340_380"],
        "FILTER_VALUE": 0.8,  # default filter value for AER_AI as per documentation
    },
    # CLOUD gas/product variables
    "gas_cloud": {
        "VARIABLE_LOC_IN_FILE": {
            "cloud_fraction": "PRODUCT/cloud_fraction",
            "cloud_top_pressure": "PRODUCT/cloud_top_pressure",
            "cloud_base_pressure": "PRODUCT/cloud_base_pressure",
            "cloud_top_height": "PRODUCT/cloud_top_height",
            "cloud_base_height": "PRODUCT/cloud_base_height",
            "cloud_optical_thickness": "PRODUCT/cloud_optical_thickness",
        },
        "DEFAULT_BANDS": ["cloud_fraction"],
        "FILTER_VALUE": 0.5,  # default filter value for CLOUD as per documentation
    },
    # AER_LH (Aerosol Layer Height) gas/product variables
    "gas_aer_lh": {
        "VARIABLE_LOC_IN_FILE": {
            "aerosol_mid_height": "PRODUCT/aerosol_mid_height",
            "aerosol_mid_pressure": "PRODUCT/aerosol_mid_pressure",
        },
        "DEFAULT_BANDS": ["aerosol_mid_height"],
        "FILTER_VALUE": 0.5,  # default filter value for AER_LH as per documentation
    },
}
############# DO NOT CHANGE THE VARIABLE NAMES ABOVE #############

# Several openEO collection IDs share the same underlying gas/product file type
# (all "CLOUD" sub-products, and the two "AER_AI" wavelength-pair variants), so
# `parse_gas_from_filename` alone cannot distinguish which single band a given
# collection should default to. This maps those openEO collection IDs to the
# band they should load when no explicit `bands` filter is given, overriding
# the (otherwise ambiguous) gas-level "DEFAULT_BANDS" above.
COLLECTION_ID_DEFAULT_BAND: dict[str, str] = {
    "SENTINEL5P_L2_CLOUD_FRACTION": "cloud_fraction",
    "SENTINEL5P_L2_CLOUD_TOP_PRESSURE": "cloud_top_pressure",
    "SENTINEL5P_L2_CLOUD_BASE_PRESSURE": "cloud_base_pressure",
    "SENTINEL5P_L2_CLOUD_TOP_HEIGHT": "cloud_top_height",
    "SENTINEL5P_L2_CLOUD_BASE_HEIGHT": "cloud_base_height",
    "SENTINEL5P_L2_CLOUD_OPTICAL_THICKNESS": "cloud_optical_thickness",
    "SENTINEL5P_L2_AER_AI_340_380": "aerosol_index_340_380",
    "SENTINEL5P_L2_AER_AI_354_388": "aerosol_index_354_388",
}


@typechecked
def parse_gas_from_filename(filename: str) -> str:
    """Extract the gas/product short name from a Sentinel-5P product filename.

    Sentinel-5P L2 filenames follow the fixed-width naming convention
    ``S5P_<processing>_L2__<PPPPPP>_<start>_<end>_<orbit>_<collection>_<processor>_<created>.nc``
    where ``<PPPPPP>`` is a fixed-width (6 character), underscore-padded product code
    (e.g. ``CO____``, ``NO2___``, ``AER_AI``). Splitting on ``"_"`` alone is not reliable
    for product codes that themselves contain an underscore (e.g. ``AER_AI``), so the
    product code is extracted using the fixed ``L2__`` marker instead.

    :param filename: Sentinel-5P product filename (or full path).
    :return: lowercased gas/product short name, e.g. ``"co"``, ``"aer_ai"``.
    """
    name = Path(filename).name
    marker = "L2__"
    idx = name.find(marker)
    if idx == -1:
        raise ValueError(f"Could not find '{marker}' marker in Sentinel-5P filename: {name}")
    product_code = name[idx + len(marker) : idx + len(marker) + 6]
    gas_name = product_code.rstrip("_").lower()
    if not gas_name:
        raise ValueError(f"Could not parse gas/product name from Sentinel-5P filename: {name}")
    return gas_name


@typechecked
def get_gas_variables(gas_type: str, collection_id: Optional[str] = None) -> tuple[dict[str, str], list[str], float]:
    """Get gas variable locations, default bands, and filter values.

    :param gas_type: gas/product short name as returned by :func:`parse_gas_from_filename`.
    :param collection_id: optional openEO collection ID (e.g. ``"SENTINEL5P_L2_CLOUD_TOP_PRESSURE"``).
        Several collection IDs share the same underlying gas/product file type (e.g. all "CLOUD"
        sub-products), so the gas-level default band is ambiguous. When *collection_id* is given and
        known, it overrides the gas-level default with the single band specific to that collection.

    Returns:
        gas_variables (dict): Dictionary containing gas variable locations in file.
        default_bands (list): List of default bands for the gas.
        filter_value (float): Default filter value for the gas.
    """
    gas_type = "gas_" + gas_type.lower()
    gas_vars = all_gases[gas_type]

    variable_loc = gas_vars["VARIABLE_LOC_IN_FILE"]
    if not isinstance(variable_loc, dict):
        raise TypeError(f"VARIABLE_LOC_IN_FILE should be dictionary, but was '{variable_loc}'")

    default_bands = gas_vars["DEFAULT_BANDS"]
    if not isinstance(default_bands, list):
        raise ValueError(f"DEFAULT_BANDS should be dictionary, but was '{default_bands}'")

    collection_default_band = COLLECTION_ID_DEFAULT_BAND.get(collection_id) if collection_id else None
    if collection_default_band is not None:
        if collection_default_band not in variable_loc:
            raise ValueError(
                f"Default band '{collection_default_band}' for collection '{collection_id}' "
                f"is not a known variable for gas type '{gas_type}'"
            )
        default_bands = [collection_default_band]

    filter_value = gas_vars["FILTER_VALUE"]
    if not isinstance(filter_value, float):
        raise TypeError(f"FILTER_VALUE should be dictionary, but was '{filter_value}'")

    variable_locs = {**variable_loc, **COMMON_VARIABLES_IN_FILE}
    return variable_locs, default_bands, filter_value


@typechecked
def load_data_from_file(
    file_path: Path,
    spatial_extent: Optional[Sequence],
    temporal_extent: Optional[Sequence],
    bands: list[str],
    variable_loc_in_file: dict[str, str],
    filter_value=0.5,
) -> dict[str, Any]:
    """Load bands data from the NetCDF file.

    1. Validity checks:
        - First, get temporal extent masks as it reduces the data loading for spatial extents.
        - get spatial extent masks to load based on spatial extents.
        - combine both masks to get valid data mask.
        - apply filter value mask to get final valid data mask.
    2. Load the bands from the valid data mask.

    Args:
        file_path (Path): Path to the NetCDF file.
        spatial_extent (Optional[Sequence]): A tuple containing (min_lon, min_lat, max_lon,
                                max_lat).
        temporal_extent (Optional[Sequence]): A tuple containing start and end times
                                (start_time, end_time) as datetime objects.
        bands (list): List of band names to load.
        variable_loc_in_file (dict): A dictionary mapping standard band names to NetCDF variable names.
        filter_value (float): Minimum acceptable quality value (0.0, 0.4
                                0.7, 1.0).

    Returns:
        data (dict): Dictionary containing loaded data arrays for the specified bands.

    Raises:
        Exception: If no temporal data is available for given temporal extent.
        Exception: If no valid data is available for given spatial extent.
        Exception: If no data is available for combined given spatial and temporal extent.
        Exception: If no data is available after applying quality filter.

    """
    # Open the NetCDF file
    with Dataset(file_path, "r") as f:
        # Check if there is valid data based on spatial temporal extents and filter value
        # If there is no valid data, raise exception with appropriate message
        # if there is valid data, get the pixel indices representing the spatial extents
        # Load time for each row
        var_path = variable_loc_in_file["delta_time"]
        delta_time_raw = f[var_path][0]
        if delta_time_raw.ndim == 2:
            # Some gas products (e.g. SO2, HCHO, O3) store delta_time per ground pixel
            # instead of per scanline, even though the value is constant across the row.
            # Reduce it back to one value per scanline as expected below.
            delta_time_raw = delta_time_raw[:, 0]
        time_array = np.array(
            num2date(
                delta_time_raw,
                f[var_path].units,
                only_use_cftime_datetimes=False,
            )
        )
        # get temporal mask
        temporal_mask = get_temporal_mask_and_time(time_array, temporal_extent)  # to set the start time
        if not temporal_mask.any():
            raise Exception(f"Input temporal extent is not in the file {file_path.name}.")

        # Define a mask where data is present based on spatial extent and temporal extents
        lat_path = variable_loc_in_file["latitude"]
        lon_path = variable_loc_in_file["longitude"]
        file_lat = f[lat_path][0]  # lat and lon are 2-d arrays
        file_lon = f[lon_path][0]
        spatial_mask = get_spatial_extent_mask(file_lat, file_lon, spatial_extent)
        if not spatial_mask.any():
            raise Exception(f"Input spatial extent is not in the file {file_path.name}.")

        # Combine spatial and temporal masks describing the valid data
        spatio_temporal_mask = temporal_mask & spatial_mask
        if not spatio_temporal_mask.any():
            raise Exception(f"No data is available for given spatial and temporal extent in file {file_path.name}.")

        # mask based on filter value
        # load qa_value and create mask
        qa_val_path = variable_loc_in_file["qa_value"]
        filter_mask = f[qa_val_path][0] >= filter_value

        # combine mask with filter_mask
        combined_mask = spatio_temporal_mask & filter_mask
        if not combined_mask.any():
            raise Exception(f"No data is available after applying quality filter in file {file_path.name}.")

        # There is valid data so load the required bands from the above pixels indices
        data = {}
        for band in bands:
            try:
                var_path = variable_loc_in_file[band]
                band_data = f[var_path][0]  # 0 is for time dimension
                # get band data based on combined mask
                data[band] = fill_and_mask_data(band_data, spatio_temporal_mask)
            except KeyError as e:
                raise KeyError(f"Band {band} not found in the NetCDF file.") from e

        # Load lat and lon based on combined mask
        data["latitude"] = _get_2d_data_from_mask(file_lat, spatio_temporal_mask)
        data["longitude"] = _get_2d_data_from_mask(file_lon, spatio_temporal_mask)
        # trim qa_value mask to spatio-temporal mask
        data["qa_value_mask"] = _get_2d_data_from_mask(filter_mask, spatio_temporal_mask)

        # define start_time and end_time for the data
        rows_idx = np.argwhere(spatio_temporal_mask)[:, 0]
        data["start_time"] = time_array[rows_idx.min()]  # add start time to the data
        data["end_time"] = time_array[rows_idx.max()]  # add end time to the data
        return data


# def is_temporal_extent_valid(filename: str, extent: tuple[datetime, datetime] | None) -> bool:
#     """Check temporal extent intersection based on file name.

#     If extent is None, return True. Assumption is the time of the whole file representing orbit is valid.

#     Args:
#         filename (str): filename of the NetCDF file.
#         extent (tuple): A tuple containing start and end times (start_time, end_time) as datetime objects.

#     Returns:
#         bool: True if the temporal extents intersect, False otherwise.
#     """
#     from datetime import datetime
#     if extent is not None:
#         # Extract start and end times from the filename
#         start_time = datetime.strptime(filename[20:35], "%Y%m%dT%H%M%S")
#         end_time = datetime.strptime(filename[36:51], "%Y%m%dT%H%M%S")
#         # check if the extents intersect
#         return max(start_time, extent[0]) <= min(end_time, extent[1])
#     else:
#         return True


@typechecked
def get_temporal_mask_and_time(time_of_rows, temporal_extent: Optional[Sequence]):
    """Get temporal mask based on the temporal extent and get the time of data.

    Args:
        time_of_rows (Array of datetime): Array of datetime objects representing the time of each row.
        temporal_extent (Optional[Sequence]): A tuple containing (start_time, end_time) as datetime objects.

    Returns:
        temporal_mask (2-d Array of bool): Boolean mask for the temporal extent with (n_rows, 1) shape.

    """
    # find intersection of temporal extent and time of data
    if temporal_extent is None:
        mask = np.ones(time_of_rows.size, dtype=bool)  # all rows true
    else:
        mask = (time_of_rows >= temporal_extent[0]) & (time_of_rows <= temporal_extent[1])
    # extend its shape to 2-d for broadcasting
    temporal_mask = np.expand_dims(mask, axis=1)
    return temporal_mask


@typechecked
def get_spatial_extent_mask(
    lat: np.ndarray, lon: np.ndarray, spatial_extent: Optional[Sequence], pixel_pad=1
) -> np.ndarray:
    """Get mask for the spatial extent in lat-lon arrays.

    The lat-lon mask is defined such that the spatial bounds is encapsulated.
    This is the reason why we use np.roll to add pixels on both sides of the bounds.
    Here the anti-meridian crossing case is also handled or lon.

    Args:
        lat (Array of float): Pixel vertices latitude.
        lon (Array of float): Pixel vertices longitude.
        spatial_extent (tuple): A tuple containing (min_lon, min_lat, max_lon, max_lat).
        pixel_pad (int): Number of pixels to pad on each side of the spatial extent. To
                         just encapsulate the bounds, 1 pixel is sufficient.

    Returns:
        mask (Array of bool): Boolean mask for the spatial extent.

    """
    if spatial_extent is None:
        spatial_mask = np.ones(lat.shape, dtype=bool)  # all pixels true
        return spatial_mask

    west, south, east, north = spatial_extent
    # Latitude mask: a pixel is added on both sides by using np.roll. This covers
    # cases where data between two large (>20km) pixels is queried (helps in resampling)
    lat_mask = ((lat >= south) | np.roll(lat >= south, -pixel_pad, axis=0)) & (
        (lat <= north) | np.roll(lat <= north, pixel_pad, axis=0)
    )
    # Longitude mask (handle wrapping)
    if west > east:
        # Crosses anti-meridain:   (lon >= west) | (lon <= east)
        lon_mask = ((lon >= west) | np.roll((lon >= west), -pixel_pad, axis=1)) | (
            (lon <= east) | np.roll((lon <= east), pixel_pad, axis=1)
        )
    else:
        # Normal case or crosses Meridian  (lon >= west) & (lon <= east)
        lon_mask = ((lon >= west) | np.roll((lon >= west), -pixel_pad, axis=1)) & (
            (lon <= east) | np.roll((lon <= east), pixel_pad, axis=1)
        )
    # Combine masks
    mask = lat_mask & lon_mask
    return mask


# def fill_and_mask_data(band_data, mask, resample=False):
#     """Load the required bands and trim data based on mask."""
#     # fill nan values where data is not valid
#     if hasattr(band_data, "filled"):
#         band_data = band_data.filled(np.nan)

#     # if resample then don't mask the data
#     if resample:
#         # data is not set to nan as it will be used fo resampling later
#         data = _get_2d_data_from_mask(band_data, mask)
#     else:
#         # set data to nan based on mask
#         data = np.where(mask, band_data, np.nan)
#         data = _get_2d_data_from_mask(data, mask)
#     return data


@typechecked
def fill_and_mask_data(band_data, spatio_temporal_mask):
    """Fill nan values based on data mask and spatio-temporal mask.

    Args:
        band_data (Array of float): masked 2-d array of band data.
        spatio_temporal_mask (Array of bool): 2-d boolean mask representing valid data

    Returns:
        data (Array of float): 2-d array of band data after filling and masking.

    """
    # fill nan values where data is not valid
    if hasattr(band_data, "filled"):
        band_data = band_data.filled(np.nan)
    # set data to nan based on the spatial-temporal extent.
    data = np.where(spatio_temporal_mask, band_data, np.nan)
    data = _get_2d_data_from_mask(data, spatio_temporal_mask)
    return data


@typechecked
def _get_2d_data_from_mask(data, mask):
    """Extract 2-d arrays based on boolean mask."""
    if (mask.ndim != 2) or (data.ndim != 2):
        raise ValueError("Mask and data must be a 2-dimensional array.")
    data_2d = data[mask.any(1)][:, mask.any(0)]
    return data_2d


@typechecked
def create_resample_grid(bbox: Sequence, resolution: float, pad_pixel=0):
    """Crate grid for resampling based on bounding box and resolution.
    Args:
        bbox (tuple): A tuple containing (min_lon, min_lat, max_lon, max
                                _lat).
        resolution (float): Resolution for resampling in degrees.
        pad_pixel (int): Number of pixels to pad on each side of the bounding box.
    Returns:
        grid_x (Array of float): 2-d array representing the longitude grid.
        grid_y (Array of float): 2-d array representing the latitude grid.
    """
    xmin, ymin, xmax, ymax = bbox
    if xmin > xmax:  # anti-meridian crossing
        xmax += 360  # temporarily shift to continuous range
    xx = np.arange(xmin + resolution / 2 - pad_pixel * resolution, xmax + pad_pixel * resolution, resolution)
    yy = np.arange(ymax - resolution / 2 + pad_pixel * resolution, ymin - pad_pixel * resolution, -resolution)
    # mesh the grid
    grid_x, grid_y = np.meshgrid(xx, yy)
    if xmin > xmax:  # anti-meridian
        grid_x = np.where(grid_x > 180, grid_x - 360, grid_x)  # convert back to -180 to 180
    return grid_x, grid_y


def _scale_lon_by_latitude(coordinates: np.ndarray) -> np.ndarray:
    """Scale the longitude component of (lon, lat) coordinates by cos(latitude).

    A degree of longitude covers far less physical (ground) distance near the poles than
    near the equator, so Euclidean distances computed directly on raw (lon, lat) pairs are
    not comparable across latitude ranges: the very same physical spacing between source
    points corresponds to a much larger longitude *degree* difference near the poles. Scaling
    longitude by cos(latitude) makes nearest-neighbour distances (and thresholds derived from
    them) approximately height/latitude independent, i.e. comparable to physical ground
    distance regardless of how close to the poles the points are.
    """
    lon, lat = coordinates[:, 0], coordinates[:, 1]
    return np.stack((lon * np.cos(np.radians(lat)), lat), axis=-1)


@typechecked
def estimate_source_pixel_spacing(source_coordinates: np.ndarray) -> float:
    """Estimate the typical spacing between neighboring source points.

    Longitude is scaled by cos(latitude) (see :func:`_scale_lon_by_latitude`) before
    computing distances, so the estimate stays meaningful for source data spanning a wide
    range of latitudes (e.g. equator to poles).
    """
    from scipy.spatial import cKDTree

    if len(source_coordinates) < 2:
        return np.inf
    scaled_coordinates = _scale_lon_by_latitude(source_coordinates)
    tree = cKDTree(scaled_coordinates)
    # k=2 because the nearest neighbor of a point in the tree is the point itself (distance 0)
    distances, _ = tree.query(scaled_coordinates, k=2)
    assert isinstance(distances, np.ndarray)
    return float(np.median(distances[:, 1]))


@typechecked
def interpolate(
    source_coordinates: np.ndarray,
    source_data: np.ndarray,
    target_coordinates: np.ndarray,
    method: str = "nearest",
    max_distance: Optional[float] = None,
) -> np.ndarray:
    """Interpolate source data to target grid based on source and target coordinates.

    Args:
        source_coordinates (Array of float): 2-d array of shape (n, 2) representing source coordinates (lon, lat).
        source_data (Array of float): 1-d array of shape (n,) representing source data values.
        target_coordinates (Array of float): 2-d array of shape (m, 2) representing target coordinates (lon, lat).
        method (str): Interpolation method. Options are "Nearest", "Linear", "Cubic".
        max_distance (float, optional): Only relevant for ``method="nearest"``. Target points
            further away from their nearest source point than this distance are set to NaN
            instead of being clamped to that (too distant) source value. "Linear" and "Cubic"
            already return NaN outside the convex hull of the source points, so they are
            unaffected by this parameter.

    Returns:
        interpolated_data (Array of float): 1-d array of shape (m,) representing interpolated data values at target coordinates.
    """
    from typing import Literal, cast

    from scipy.interpolate import griddata

    method = method.lower()
    method = cast(Literal["nearest", "linear", "cubic"], method)  # for mypy
    assert method in ["nearest", "linear", "cubic"]  # for typing
    interpolated_data = griddata(
        source_coordinates,
        source_data,
        target_coordinates,
        method=method,
        fill_value=np.nan,
    )
    if method == "nearest" and max_distance is not None and len(source_coordinates) > 0:
        from scipy.spatial import cKDTree

        # Use latitude-scaled coordinates (see _scale_lon_by_latitude) so that max_distance
        # is comparable across the full latitude range of the target grid, instead of being
        # biased towards whatever latitude the source data happens to be denser/sparser in
        # degree-space.
        scaled_source_coordinates = _scale_lon_by_latitude(source_coordinates)
        scaled_target_coordinates = _scale_lon_by_latitude(target_coordinates)
        tree = cKDTree(scaled_source_coordinates)
        nearest_distances, _ = tree.query(scaled_target_coordinates, k=1)
        interpolated_data = np.where(nearest_distances <= max_distance, interpolated_data, np.nan)
    return interpolated_data


@typechecked
def adapt_coordinates(source_coordinates, target_coordinates) -> tuple:
    """Check and adapt coordinates for anti-meridian crossing.

    Args:
        source_coordinates (Array of float): 2-d array of shape (n, 2) representing source coordinates (lon, lat).
        target_coordinates (Array of float): 2-d array of shape (m, 2) representing target coordinates (lon, lat).

    Returns:
        adapted_source_coordinates (Array of float): Adapted source coordinates.
        adapted_target_coordinates (Array of float): Adapted target coordinates.
    """
    # Check if anti-meridian crossing is needed
    # Adapt longitudes
    source_lon = np.where(source_coordinates[:, 0] < 0, source_coordinates[:, 0] + 360, source_coordinates[:, 0])
    target_lon = np.where(target_coordinates[:, 0] < 0, target_coordinates[:, 0] + 360, target_coordinates[:, 0])
    adapted_source_coordinates = np.stack((source_lon, source_coordinates[:, 1]), axis=-1)
    adapted_target_coordinates = np.stack((target_lon, target_coordinates[:, 1]), axis=-1)
    return adapted_source_coordinates, adapted_target_coordinates


@typechecked
def resample_data(
    data: dict, bands: list, spatial_extent: Sequence, resample_resolution: float, interpolation_method: str
) -> dict[str, np.ndarray]:
    """Resample data based on spatial extent and resample parameters.

    Args:
        data (dict): Dictionary containing data arrays for different bands.
        bands (list): List of band names to resample.
        spatial_extent (tuple): A tuple containing (min_lon, min_lat, max_lon, max_lat).
        resample_resolution (float): Resolution for resampling in degrees.
        interpolation_method (str): Interpolation method. Options are "nearest", "linear", "cubic".

    Returns:
        new_data (dict): Dictionary containing resampled data arrays for different bands.
    """
    interpolated_data = {}  # dictionary to hold resampled data
    # create new grid for resampled data
    resampled_lon, resampled_lat = create_resample_grid(spatial_extent, resample_resolution)
    interpolated_data["latitude"] = resampled_lat
    interpolated_data["longitude"] = resampled_lon

    # Prepare coordinates for interpolation
    source_coordinates = np.stack((data["longitude"].ravel(), data["latitude"].ravel()), axis=-1)
    target_coordinates = np.stack((resampled_lon.ravel(), resampled_lat.ravel()), axis=-1)
    target_shape = resampled_lat.shape
    if spatial_extent[0] > spatial_extent[2]:  # anti-meridian crossing
        source_coordinates, target_coordinates = adapt_coordinates(source_coordinates, target_coordinates)

    # "nearest" interpolation can sample far away pixels, because it uses a KDTree.
    # This threshold helps to avoid that:
    max_nearest_distance = estimate_source_pixel_spacing(source_coordinates) * 1.5

    # Interpolate qa_value_mask to new grid with nearest method for masking
    # Do not use other methods as it can create intermediate values
    # which can lead to incorrect masking.
    qa_value_mask_interp = interpolate(
        source_coordinates,
        data["qa_value_mask"].ravel(),
        target_coordinates,
        method="nearest",
        max_distance=max_nearest_distance,
    ).reshape(target_shape)
    # NaN (no source pixel close enough) means "no valid data", i.e. should not pass quality filtering
    interpolated_data["qa_value_mask"] = np.where(np.isnan(qa_value_mask_interp), False, qa_value_mask_interp).astype(
        bool
    )

    # all other bands
    for key, val in data.items():
        if key in bands:
            # interpolate to new grid
            interpolated_data[key] = interpolate(
                source_coordinates,
                val.ravel(),
                target_coordinates,
                method=interpolation_method,
                max_distance=max_nearest_distance,
            ).reshape(target_shape)
    return interpolated_data


@typechecked
def apply_quality_filter(
    data: dict[str, np.ndarray], bands: list, quality_band: str = "qa_value_mask"
) -> dict[str, np.ndarray]:
    """Apply quality filter to the data based on quality band.

    Args:
        data (dict): Dictionary containing data arrays for different bands.
        bands (list): List of band names to apply quality filter.
        quality_band (str): Name of the quality band in the data dictionary.

    Returns:
        filtered_data (dict): Dictionary containing data arrays after applying quality filter.
    """
    filtered_data = {}
    quality_mask = data[quality_band]
    for key, val in data.items():
        if key in bands:
            filtered_data[key] = np.where(quality_mask, val, np.nan)
        elif (key not in bands) & (key != quality_band):
            filtered_data[key] = val  # copy metadata
    return filtered_data
