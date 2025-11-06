""" Load Sentinel-5P satellite data from a NetCDF file.

This code provides a functionality to read and filter different gases
data from Sentinel-5P NetCDF files based on specified spatial and temporal
extents, as well as quality filtering.

Filtering:
   The default is set as per Sentinel-5P documentation. It differs per gas
   If the user provides a different value which exists, it can be used as filter.
   According to documentation, the quality values are between 0-1.
   For more details, please refer to the PRF-**** documents on this page
   https://sentiwiki.copernicus.eu/web/s5p-products#S5PProducts-L2S5P-Products-L2


Attributes:
     (dict): A dictionary mapping band names to their respective
                      paths in the NetCDF file for Sentinel-5P CO level-2 data.

Everything should happen in EPSG: 4326 (lat-lon) as Sentinel-5P data is in lat-lon grid.

Algorithm:
    1. Check inputs
        - if the file exists
        - If bands and filter value are provided, load default bands if not.
        - Check if spatial extent is provided.
        - Check if temporal extent is provided and is made of datetime objects.
        - Resampling parameters.
    2. Load data from netCDF file where
        - Validate temporal extent from delta_time field and
        - Validate spatial extents from latitude and longitude fields and
            get pixel indices representing the spatial extents.
        - Load the required bands from the pixels indices.
    3. Resample data if required.
    4. Apply quality filtering based on qa_value_mask band.

"""

from pathlib import Path
from datetime import datetime
from .sentinel5p_functions import (
    get_gas_variables,
    load_data_from_file,
    resample_data,
    apply_quality_filter,
)


def load_level2_data(params):
    """Load Sentinel-5P level-2 data from a NetCDF file.

    Args:
        params (dict): A dict of parameters containing following
            filename: str, path to the NetCDF file.
            spatial_extent: tuple, (min_lon, min_lat, max_lon, max_lat)
            temporal_extent: tuple, (start_time, end_time) as datetime objects.
            bands: list of str, list of band names to load.
            filter_value: float, filter_value, quality filtering value between 0-1.
            resample_factor: [Bool, Float, method], Resample data, resampling resolution and method
                              to downsample the data.
    Returns:
        data: dict, dictionary containing loaded data arrays for the specified bands.

    Raises:
        Exception: If the file does not exist or if the temporal extent does not intersect or
                   if the spatial extent is invalid.

    """
    # filename, spatial_extent, temporal_extent, bands, filter_value
    # check if the file exists
    file_path = Path(params.get("filename", ""))

    if not file_path.exists():
        raise Exception(f"read_product: path {file_path} does not exist.")
    # get the gas
    file_gas = file_path.name.split("_")[4]
    VARIABLE_LOC_IN_FILE, DEFAULT_BANDS, DEFAULT_FILTER_VALUE = get_gas_variables(file_gas)
        
    # check the spatial extent and temporal extent keys
    spatial_extent = params.get("spatial_extent", None)
    temporal_extent = params.get("temporal_extent", None)
    # check if temporal_extent is made of datetime objects
    if temporal_extent is not None:
        if not all(isinstance(x, datetime) for x in temporal_extent):
            raise Exception("temporal_extent should be made of datetime objects.")
    # check the band names and filter_value
    bands = params.get("bands", [])
    # if bands are not defined then load the default bands
    if not bands:
        bands = DEFAULT_BANDS
    # filter value
    filter_value = params.get("filter_value", DEFAULT_FILTER_VALUE)
    # this should be on the client side
    if not ((filter_value >= 0.0) and (filter_value <= 1.0)):
        raise IOError(
            f"Warning: filter_value {filter_value} is not standard as per Sentinel-5P documentation."
            " It should be between 0.0-1.0."
        )
    # resampling parameters
    resample_params = params.get("resample_factor", [False, 0.025, "nearest"])

    # Check if file is temporally valid and if valid, then load data from the file
    # temporally_valid = is_temporal_extent_valid(file_path.name, temporal_extent)
    # if not temporally_valid:
    #     raise Exception(
    #         f"Input temporal extent doesn't intersect with the temporal extent of the file {file_path.name}."
    #     )
    # else:


    # Load raw data from the file based on spatial and temporal extents and resampling
    # This function can raise a lot of exceptions which will be propagated.
    # TODO No idea if we should catch and re-raise them with more context here.
    data = load_data_from_file(
        file_path,
        spatial_extent,
        temporal_extent,
        bands,
        VARIABLE_LOC_IN_FILE,
        filter_value,
    )

    # resample data
    if resample_params[0]:  # if resampling is required
        data = resample_data(data, bands, spatial_extent, resample_params[1], resample_params[2])

    # apply quality filtering
    final_data = apply_quality_filter(data, bands, "qa_value_mask")
    return final_data
