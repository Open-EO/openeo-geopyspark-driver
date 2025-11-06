"""Module to load Sentinel-5P satellite data from a NetCDF file.

This module provides functionality to read and filter different level-2
data from Sentinel-5P NetCDF files based on specified spatial and temporal
extents, as well as quality filtering.

Everything should happen in EPSG: 4326 (lat-lon) as Sentinel-5P data is in lat-lon grid.
"""

from pathlib import Path
import numpy as np
from netCDF4 import Dataset, num2date

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

all_gases = {
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
}
############# DO NOT CHANGE THE VARIABLE NAMES ABOVE #############


def get_gas_variables(gas_type):
    """Get gas variable locations, default bands, and filter values.

    Returns:
        gas_variables (dict): Dictionary containing gas variable locations in file.
        default_bands (list): List of default bands for the gas.
        filter_value (float): Default filter value for the gas.
    """
    gas_type = "gas_" + gas_type.lower()
    gas_vars = all_gases.get(gas_type)
    if gas_vars is None:
        raise ValueError(f"Unknown gas type: {gas_type}")
    variable_loc = gas_vars.get("VARIABLE_LOC_IN_FILE")
    if variable_loc is None:
        raise ValueError("No mapping band variable")
    variable_locs = {**variable_loc, **COMMON_VARIABLES_IN_FILE}
    return variable_locs, gas_vars.get("DEFAULT_BANDS"), gas_vars.get("FILTER_VALUE")


def load_data_from_file(
    file_path: Path, spatial_extent, temporal_extent, bands, variable_loc_in_file, filter_value=0.5
):
    """Load bands data from the NetCDF file.

    1. Validity checks:
        - First, get temporal extent masks as it reduces the data loading for spatial extents.
        - get spatial extent masks to load based on spatial extents.
        - combine both masks to get valid data mask.
        - apply filter value mask to get final valid data mask.
    2. Load the bands from the valid data mask.

    Args:
        file_path (Path): Path to the NetCDF file.
        spatial_extent (tuple): A tuple containing (min_lon, min_lat, max_lon,
                                max_lat).
        temporal_extent (tuple): A tuple containing start and end times
                                (start_time, end_time) as datetime objects.
        bands (list): List of band names to load.
        name_list (dict): A dictionary mapping standard band names to NetCDF variable names.
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
        var_path = variable_loc_in_file.get("delta_time")
        time_array = np.array(
            num2date(
                f[var_path][0],
                f[var_path].units,
                only_use_cftime_datetimes=False,
            )
        )
        # get temporal mask
        temporal_mask = get_temporal_mask_and_time(time_array, temporal_extent)  # to set the start time
        if not temporal_mask.any():
            raise Exception(f"Input temporal extent is not in the file {file_path.name}.")

        # Define a mask where data is present based on spatial extent and temporal extents
        lat_path = variable_loc_in_file.get("latitude")
        lon_path = variable_loc_in_file.get("longitude")
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
        qa_val_path = variable_loc_in_file.get("qa_value")
        filter_mask = f[qa_val_path][0] >= filter_value

        # combine mask with filter_mask
        combined_mask = spatio_temporal_mask & filter_mask
        if not combined_mask.any():
            raise Exception(f"No data is available after applying quality filter in file {file_path.name}.")

        # There is valid data so load the required bands from the above pixels indices
        data = {}
        for band in bands:
            try:
                var_path = variable_loc_in_file.get(band)
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


def get_temporal_mask_and_time(time_of_rows, temporal_extent):
    """Get temporal mask based on the temporal extent and get the time of data.

    Args:
        time_of_rows (Array of datetime): Array of datetime objects representing the time of each row.
        temporal_extent (tuple): A tuple containing (start_time, end_time) as datetime objects.

    Returns:
        temporal_mask (2-d Array of bool): Boolean mask for the temporal extent with (n_rows, 1) shape.

    """
    # find intersection of temporal extent and time of data
    if temporal_extent is None:
        mask = np.ones((time_of_rows.size), dtype=bool)  # all rows true
    else:
        mask = (time_of_rows >= temporal_extent[0]) & (time_of_rows <= temporal_extent[1])
    # extend its shape to 2-d for broadcasting
    temporal_mask = np.expand_dims(mask, axis=1)
    return temporal_mask


def get_spatial_extent_mask(lat, lon, spatial_extent, pixel_pad=1):
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


def _get_2d_data_from_mask(data, mask):
    """Extract 2-d arrays based on boolean mask."""
    if (mask.ndim != 2) or (data.ndim != 2):
        raise ValueError("Mask and data must be a 2-dimensional array.")
    data_2d = data[mask.any(1)][:, mask.any(0)]
    return data_2d


def create_resample_grid(bbox, resolution, pad_pixel=0):
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


def interpolate(source_coordinates, source_data, target_coordinates, method="nearest"):
    """Interpolate source data to target grid based on source and target coordinates.

    Args:
        source_coordinates (Array of float): 2-d array of shape (n, 2) representing source coordinates (lon, lat).
        source_data (Array of float): 1-d array of shape (n,) representing source data values.
        target_coordinates (Array of float): 2-d array of shape (m, 2) representing target coordinates (lon, lat).
        method (str): Interpolation method. Options are "Nearest", "Linear", "Cubic".

    Returns:
        interpolated_data (Array of float): 1-d array of shape (m,) representing interpolated data values at target coordinates.
    """
    from scipy.interpolate import griddata

    method = method.lower()
    interpolated_data = griddata(
        source_coordinates,
        source_data,
        target_coordinates,
        method=method,
        fill_value=np.nan,
    )
    return interpolated_data


def adapt_coordinates(source_coordinates, target_coordinates):
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


def resample_data(data, bands, spatial_extent, resample_resolution, interpolation_method):
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

    # Interpolate qa_value_mask to new grid with nearest method for masking
    # Do not use other methods as it can create intermediate values
    # which can lead to incorrect masking.
    interpolated_data["qa_value_mask"] = interpolate(
        source_coordinates, data["qa_value_mask"].ravel(), target_coordinates, method="nearest"
    ).reshape(target_shape)

    # all other bands
    for key, val in data.items():
        if key in bands:
            # interpolate to new grid
            interpolated_data[key] = interpolate(
                source_coordinates, val.ravel(), target_coordinates, method=interpolation_method
            ).reshape(target_shape)
    return interpolated_data


def apply_quality_filter(data, bands, quality_band="qa_value_mask"):
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
