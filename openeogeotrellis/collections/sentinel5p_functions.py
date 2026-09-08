"""Module to load Sentinel-5P satellite data from a NetCDF file.

This module provides functionality to read and filter different level-2
data from Sentinel-5P NetCDF files based on specified spatial and temporal
extents, as well as quality filtering.

Everything should happen in EPSG: 4326 (lat-lon) as Sentinel-5P data is in lat-lon grid.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Sequence
from shapely.geometry import Point, Polygon
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
    "COMMON_VARIABLES_IN_FILE": {
        "time": "PRODUCT/time",
        "delta_time": "PRODUCT/delta_time",
        "latitude": "PRODUCT/latitude",
        "longitude": "PRODUCT/longitude",
        "qa_value": "PRODUCT/qa_value",
    },
    "gas_aer_ai": {
        "VARIABLE_LOC_IN_FILE": {
            "aerosol_index_354_388": "PRODUCT/aerosol_index_354_388",
            "aerosol_index_340_380": "PRODUCT/aerosol_index_340_380",
            "aerosol_index_335_367": "PRODUCT/aerosol_index_335_367",
            "aerosol_index_354_388_scm": "PRODUCT/aerosol_index_354_388_scm",
            "aerosol_index_354_388_precision": "PRODUCT/aerosol_index_354_388_precision",
            "aerosol_index_340_380_precision": "PRODUCT/aerosol_index_340_380_precision",
            "aerosol_index_335_367_precision": "PRODUCT/aerosol_index_335_367_precision",
            "solar_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_zenith_angle",
            "solar_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_azimuth_angle",
            "viewing_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_zenith_angle",
            "viewing_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_azimuth_angle",
            "geolocation_flags": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/geolocation_flags",
            "processing_quality_flags": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/processing_quality_flags",
            "number_of_spectral_points_in_retrieval": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_spectral_points_in_retrieval",
            "scene_albedo_388": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/scene_albedo_388",
            "scene_albedo_388_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/scene_albedo_388_precision",
            "reflectance_measured_354": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_measured_354",
            "reflectance_measured_354_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_measured_354_precision",
            "reflectance_measured_388": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_measured_388",
            "reflectance_measured_388_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_measured_388_precision",
            "reflectance_calculated_354": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_calculated_354",
            "reflectance_calculated_354_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_calculated_354_precision",
            "scene_albedo_380": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/scene_albedo_380",
            "scene_albedo_380_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/scene_albedo_380_precision",
            "reflectance_measured_340": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_measured_340",
            "reflectance_measured_340_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_measured_340_precision",
            "reflectance_measured_380": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_measured_380",
            "reflectance_measured_380_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_measured_380_precision",
            "reflectance_calculated_340": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_calculated_340",
            "reflectance_calculated_340_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_calculated_340_precision",
            "scene_albedo_367": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/scene_albedo_367",
            "scene_albedo_367_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/scene_albedo_367_precision",
            "reflectance_measured_335": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_measured_335",
            "reflectance_measured_335_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_measured_335_precision",
            "reflectance_measured_367": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_measured_367",
            "reflectance_measured_367_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_measured_367_precision",
            "reflectance_calculated_335": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_calculated_335",
            "reflectance_calculated_335_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_calculated_335_precision",
            "cloud_fraction": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_fraction",
            "reflectance_clear_354": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_clear_354",
            "reflectance_clear_388": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_clear_388",
            "reflectance_cloud_354": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_cloud_354",
            "reflectance_cloud_388": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/reflectance_cloud_388",
            "wavelength_calibration_offset": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_calibration_offset",
            "wavelength_calibration_offset_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_calibration_offset_precision",
            "wavelength_calibration_stretch": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_calibration_stretch",
            "wavelength_calibration_chi_square": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_calibration_chi_square",
        },
        "DEFAULT_BANDS": [
            "aerosol_index_354_388",
            "aerosol_index_340_380",
            "aerosol_index_335_367",
            "aerosol_index_354_388_scm",
            "aerosol_index_354_388_precision",
            "aerosol_index_340_380_precision",
            "aerosol_index_335_367_precision",
            "solar_zenith_angle",
            "solar_azimuth_angle",
            "viewing_zenith_angle",
            "viewing_azimuth_angle",
            "geolocation_flags",
            "processing_quality_flags",
            "number_of_spectral_points_in_retrieval",
            "scene_albedo_388",
            "scene_albedo_388_precision",
            "reflectance_measured_354",
            "reflectance_measured_354_precision",
            "reflectance_measured_388",
            "reflectance_measured_388_precision",
            "reflectance_calculated_354",
            "reflectance_calculated_354_precision",
            "scene_albedo_380",
            "scene_albedo_380_precision",
            "reflectance_measured_340",
            "reflectance_measured_340_precision",
            "reflectance_measured_380",
            "reflectance_measured_380_precision",
            "reflectance_calculated_340",
            "reflectance_calculated_340_precision",
            "scene_albedo_367",
            "scene_albedo_367_precision",
            "reflectance_measured_335",
            "reflectance_measured_335_precision",
            "reflectance_measured_367",
            "reflectance_measured_367_precision",
            "reflectance_calculated_335",
            "reflectance_calculated_335_precision",
            "cloud_fraction",
            "reflectance_clear_354",
            "reflectance_clear_388",
            "reflectance_cloud_354",
            "reflectance_cloud_388",
            "wavelength_calibration_offset",
            "wavelength_calibration_offset_precision",
            "wavelength_calibration_stretch",
            "wavelength_calibration_chi_square",
        ],
        "FILTER_VALUE": 0.8,
    },
    "gas_aer_lh": {
        "VARIABLE_LOC_IN_FILE": {
            "aerosol_mid_pressure": "PRODUCT/aerosol_mid_pressure",
            "aerosol_mid_height": "PRODUCT/aerosol_mid_height",
            "aerosol_mid_pressure_precision": "PRODUCT/aerosol_mid_pressure_precision",
            "aerosol_mid_height_precision": "PRODUCT/aerosol_mid_height_precision",
            "solar_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_zenith_angle",
            "solar_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_azimuth_angle",
            "viewing_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_zenith_angle",
            "viewing_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_azimuth_angle",
            "geolocation_flags": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/geolocation_flags",
            "processing_quality_flags": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/processing_quality_flags",
            "number_of_spectral_points_in_retrieval": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_spectral_points_in_retrieval",
            "number_of_iterations": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations",
            "aerosol_mid_pressure_not_clipped": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/aerosol_mid_pressure_not_clipped",
            "aerosol_optical_thickness": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/aerosol_optical_thickness",
            "aerosol_optical_thickness_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/aerosol_optical_thickness_precision",
            "root_mean_square_error_of_fit": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/root_mean_square_error_of_fit",
            "chi_square": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/chi_square",
            "degrees_of_freedom": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/degrees_of_freedom",
            "cloud_mask_viirs": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_mask_viirs",
            "cloud_mask_rfc": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_mask_rfc",
            "cloud_mask_fresco": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_mask_fresco",
            "wavelength_calibration_offset": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_calibration_offset",
            "wavelength_calibration_stretch": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_calibration_stretch",
            "wavelength_calibration_chi_square": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_calibration_chi_square",
        },
        "DEFAULT_BANDS": [
            "aerosol_mid_pressure",
            "aerosol_mid_height",
            "aerosol_mid_pressure_precision",
            "aerosol_mid_height_precision",
            "solar_zenith_angle",
            "solar_azimuth_angle",
            "viewing_zenith_angle",
            "viewing_azimuth_angle",
            "geolocation_flags",
            "processing_quality_flags",
            "number_of_spectral_points_in_retrieval",
            "number_of_iterations",
            "aerosol_mid_pressure_not_clipped",
            "aerosol_optical_thickness",
            "aerosol_optical_thickness_precision",
            "root_mean_square_error_of_fit",
            "chi_square",
            "degrees_of_freedom",
            "cloud_mask_viirs",
            "cloud_mask_rfc",
            "cloud_mask_fresco",
            "wavelength_calibration_offset",
            "wavelength_calibration_stretch",
            "wavelength_calibration_chi_square",
        ],
        "FILTER_VALUE": 0.5,
    },
    "gas_ch4": {
        "VARIABLE_LOC_IN_FILE": {
            "methane_mixing_ratio": "PRODUCT/methane_mixing_ratio",
            "methane_mixing_ratio_precision": "PRODUCT/methane_mixing_ratio_precision",
            "methane_mixing_ratio_bias_corrected": "PRODUCT/methane_mixing_ratio_bias_corrected",
            "methane_mixing_ratio_bias_corrected_destriped": "PRODUCT/methane_mixing_ratio_bias_corrected_destriped",
            "solar_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_zenith_angle",
            "solar_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_azimuth_angle",
            "viewing_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_zenith_angle",
            "viewing_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_azimuth_angle",
            "geolocation_flags": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/geolocation_flags",
            "processing_quality_flags": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/processing_quality_flags",
            "number_of_spectral_points_in_retrieval": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_spectral_points_in_retrieval",
            "carbonmonoxide_total_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/carbonmonoxide_total_column",
            "water_total_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/water_total_column",
            "water_total_column_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/water_total_column_precision",
            "aerosol_size": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/aerosol_size",
            "aerosol_size_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/aerosol_size_precision",
            "aerosol_number_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/aerosol_number_column",
            "aerosol_number_column_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/aerosol_number_column_precision",
            "aerosol_mid_altitude": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/aerosol_mid_altitude",
            "aerosol_mid_altitude_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/aerosol_mid_altitude_precision",
            "surface_albedo_SWIR": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/surface_albedo_SWIR",
            "surface_albedo_SWIR_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/surface_albedo_SWIR_precision",
            "surface_albedo_NIR": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/surface_albedo_NIR",
            "surface_albedo_NIR_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/surface_albedo_NIR_precision",
            "aerosol_optical_thickness_SWIR": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/aerosol_optical_thickness_SWIR",
            "aerosol_optical_thickness_NIR": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/aerosol_optical_thickness_NIR",
            "wavelength_calibration_offset_SWIR": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_calibration_offset_SWIR",
            "wavelength_calibration_offset_NIR": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_calibration_offset_NIR",
            "maximum_reflectance_NIR": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/maximum_reflectance_NIR",
            "maximum_reflectance_SWIR": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/maximum_reflectance_SWIR",
            "chi_square": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/chi_square",
            "chi_square_SWIR": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/chi_square_SWIR",
            "chi_square_NIR": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/chi_square_NIR",
            "degrees_of_freedom": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/degrees_of_freedom",
            "degrees_of_freedom_methane": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/degrees_of_freedom_methane",
            "degrees_of_freedom_aerosol": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/degrees_of_freedom_aerosol",
            "number_of_iterations": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations",
            "fluorescence": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fluorescence",
            "quality_flag_experimental": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/quality_flag_experimental",
        },
        "DEFAULT_BANDS": [
            "methane_mixing_ratio",
            "methane_mixing_ratio_precision",
            "methane_mixing_ratio_bias_corrected",
            "methane_mixing_ratio_bias_corrected_destriped",
            "solar_zenith_angle",
            "solar_azimuth_angle",
            "viewing_zenith_angle",
            "viewing_azimuth_angle",
            "geolocation_flags",
            "processing_quality_flags",
            "number_of_spectral_points_in_retrieval",
            "carbonmonoxide_total_column",
            "water_total_column",
            "water_total_column_precision",
            "aerosol_size",
            "aerosol_size_precision",
            "aerosol_number_column",
            "aerosol_number_column_precision",
            "aerosol_mid_altitude",
            "aerosol_mid_altitude_precision",
            "surface_albedo_SWIR",
            "surface_albedo_SWIR_precision",
            "surface_albedo_NIR",
            "surface_albedo_NIR_precision",
            "aerosol_optical_thickness_SWIR",
            "aerosol_optical_thickness_NIR",
            "wavelength_calibration_offset_SWIR",
            "wavelength_calibration_offset_NIR",
            "maximum_reflectance_NIR",
            "maximum_reflectance_SWIR",
            "chi_square",
            "chi_square_SWIR",
            "chi_square_NIR",
            "degrees_of_freedom",
            "degrees_of_freedom_methane",
            "degrees_of_freedom_aerosol",
            "number_of_iterations",
            "fluorescence",
            "quality_flag_experimental",
        ],
        "FILTER_VALUE": 0.5,
    },
    "gas_cloud": {
        "VARIABLE_LOC_IN_FILE": {
            "cloud_fraction": "PRODUCT/cloud_fraction",
            "cloud_fraction_precision": "PRODUCT/cloud_fraction_precision",
            "cloud_top_pressure": "PRODUCT/cloud_top_pressure",
            "cloud_top_pressure_precision": "PRODUCT/cloud_top_pressure_precision",
            "cloud_base_pressure": "PRODUCT/cloud_base_pressure",
            "cloud_base_pressure_precision": "PRODUCT/cloud_base_pressure_precision",
            "cloud_top_height": "PRODUCT/cloud_top_height",
            "cloud_top_height_precision": "PRODUCT/cloud_top_height_precision",
            "cloud_base_height": "PRODUCT/cloud_base_height",
            "cloud_base_height_precision": "PRODUCT/cloud_base_height_precision",
            "cloud_optical_thickness": "PRODUCT/cloud_optical_thickness",
            "cloud_optical_thickness_precision": "PRODUCT/cloud_optical_thickness_precision",
            "solar_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_zenith_angle",
            "solar_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_azimuth_angle",
            "viewing_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_zenith_angle",
            "viewing_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_azimuth_angle",
            "geolocation_flags": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/geolocation_flags",
            "solar_zenith_angle_nir": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_zenith_angle_nir",
            "solar_azimuth_angle_nir": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_azimuth_angle_nir",
            "viewing_zenith_angle_nir": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_zenith_angle_nir",
            "viewing_azimuth_angle_nir": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_azimuth_angle_nir",
            "geolocation_flags_nir": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/geolocation_flags_nir",
            "cloud_fraction_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_fraction_crb",
            "cloud_fraction_crb_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_fraction_crb_precision",
            "cloud_pressure_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_pressure_crb",
            "cloud_pressure_crb_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_pressure_crb_precision",
            "cloud_height_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_height_crb",
            "cloud_height_crb_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_height_crb_precision",
            "cloud_albedo_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_albedo_crb",
            "cloud_albedo_crb_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_albedo_crb_precision",
            "surface_albedo_fitted_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/surface_albedo_fitted_crb",
            "surface_albedo_fitted_crb_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/surface_albedo_fitted_crb_precision",
            "sun_glint_flag": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/sun_glint_flag",
            "cloud_top_temperature": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_top_temperature",
            "cloud_phase": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_phase",
            "cloud_fraction_apriori": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_fraction_apriori",
            "wavelength_shift": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_shift",
            "wavelength_shift_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_shift_precision",
            "wavelength_shift_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_shift_crb",
            "wavelength_shift_crb_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_shift_crb_precision",
            "number_of_iterations": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations",
            "number_of_iterations_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations_crb",
            "fitted_root_mean_square": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_root_mean_square",
            "fitted_root_mean_square_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_root_mean_square_crb",
            "degrees_of_freedom": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/degrees_of_freedom",
            "degrees_of_freedom_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/degrees_of_freedom_crb",
            "effective_scene_height": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/effective_scene_height",
            "effective_scene_height_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/effective_scene_height_precision",
            "effective_scene_pressure": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/effective_scene_pressure",
            "effective_scene_pressure_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/effective_scene_pressure_precision",
            "cloud_mask": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_mask",
            "cloud_mask_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_mask_nir",
            "surface_albedo_fitted": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/surface_albedo_fitted",
            "surface_albedo_fitted_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/surface_albedo_fitted_precision",
            "cloud_fraction_apriori_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_fraction_apriori_nir",
            "cloud_top_height_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_top_height_nir",
            "cloud_optical_thickness_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_optical_thickness_nir",
            "cloud_fraction_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_fraction_nir",
            "surface_albedo_fitted_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/surface_albedo_fitted_nir",
            "cloud_top_height_precision_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_top_height_precision_nir",
            "cloud_optical_thickness_precision_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_optical_thickness_precision_nir",
            "cloud_fraction_precision_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_fraction_precision_nir",
            "surface_albedo_fitted_precision_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/surface_albedo_fitted_precision_nir",
            "regularization_parameter_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/regularization_parameter_nir",
            "condition_number_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/condition_number_nir",
            "degrees_of_freedom_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/degrees_of_freedom_nir",
            "shannon_information_content_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/shannon_information_content_nir",
            "number_of_iterations_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations_nir",
            "fitted_root_mean_square_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_root_mean_square_nir",
            "convergence_flag_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/convergence_flag_nir",
            "cloud_height_crb_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_height_crb_nir",
            "cloud_albedo_crb_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_albedo_crb_nir",
            "cloud_fraction_crb_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_fraction_crb_nir",
            "surface_albedo_fitted_crb_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/surface_albedo_fitted_crb_nir",
            "cloud_height_crb_precision_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_height_crb_precision_nir",
            "cloud_albedo_crb_precision_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_albedo_crb_precision_nir",
            "cloud_fraction_crb_precision_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_fraction_crb_precision_nir",
            "regularization_parameter_crb_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/regularization_parameter_crb_nir",
            "condition_number_crb_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/condition_number_crb_nir",
            "degrees_of_freedom_crb_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/degrees_of_freedom_crb_nir",
            "shannon_information_content_crb_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/shannon_information_content_crb_nir",
            "number_of_iterations_crb_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations_crb_nir",
            "fitted_root_mean_square_crb_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_root_mean_square_crb_nir",
            "convergence_flag_crb_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/convergence_flag_crb_nir",
            "effective_scene_height_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/effective_scene_height_nir",
            "effective_scene_albedo_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/effective_scene_albedo_nir",
            "effective_scene_height_precision_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/effective_scene_height_precision_nir",
            "effective_scene_albedo_precision_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/effective_scene_albedo_precision_nir",
            "condition_number_ge_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/condition_number_ge_nir",
            "degrees_of_freedom_ge_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/degrees_of_freedom_ge_nir",
            "shannon_information_content_ge_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/shannon_information_content_ge_nir",
            "number_of_iterations_ge_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations_ge_nir",
            "fitted_root_mean_square_ge_nir": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_root_mean_square_ge_nir",
            "processing_quality_flags": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/processing_quality_flags",
            "processing_quality_flags_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/processing_quality_flags_crb",
            "qa_value_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/qa_value_crb",
        },
        "DEFAULT_BANDS": [
            "cloud_fraction",
            "cloud_fraction_precision",
            "cloud_top_pressure",
            "cloud_top_pressure_precision",
            "cloud_base_pressure",
            "cloud_base_pressure_precision",
            "cloud_top_height",
            "cloud_top_height_precision",
            "cloud_base_height",
            "cloud_base_height_precision",
            "cloud_optical_thickness",
            "cloud_optical_thickness_precision",
            "solar_zenith_angle",
            "solar_azimuth_angle",
            "viewing_zenith_angle",
            "viewing_azimuth_angle",
            "geolocation_flags",
            "solar_zenith_angle_nir",
            "solar_azimuth_angle_nir",
            "viewing_zenith_angle_nir",
            "viewing_azimuth_angle_nir",
            "geolocation_flags_nir",
            "cloud_fraction_crb",
            "cloud_fraction_crb_precision",
            "cloud_pressure_crb",
            "cloud_pressure_crb_precision",
            "cloud_height_crb",
            "cloud_height_crb_precision",
            "cloud_albedo_crb",
            "cloud_albedo_crb_precision",
            "surface_albedo_fitted_crb",
            "surface_albedo_fitted_crb_precision",
            "sun_glint_flag",
            "cloud_top_temperature",
            "cloud_phase",
            "cloud_fraction_apriori",
            "wavelength_shift",
            "wavelength_shift_precision",
            "wavelength_shift_crb",
            "wavelength_shift_crb_precision",
            "number_of_iterations",
            "number_of_iterations_crb",
            "fitted_root_mean_square",
            "fitted_root_mean_square_crb",
            "degrees_of_freedom",
            "degrees_of_freedom_crb",
            "effective_scene_height",
            "effective_scene_height_precision",
            "effective_scene_pressure",
            "effective_scene_pressure_precision",
            "cloud_mask",
            "cloud_mask_nir",
            "surface_albedo_fitted",
            "surface_albedo_fitted_precision",
            "cloud_fraction_apriori_nir",
            "cloud_top_height_nir",
            "cloud_optical_thickness_nir",
            "cloud_fraction_nir",
            "surface_albedo_fitted_nir",
            "cloud_top_height_precision_nir",
            "cloud_optical_thickness_precision_nir",
            "cloud_fraction_precision_nir",
            "surface_albedo_fitted_precision_nir",
            "regularization_parameter_nir",
            "condition_number_nir",
            "degrees_of_freedom_nir",
            "shannon_information_content_nir",
            "number_of_iterations_nir",
            "fitted_root_mean_square_nir",
            "convergence_flag_nir",
            "cloud_height_crb_nir",
            "cloud_albedo_crb_nir",
            "cloud_fraction_crb_nir",
            "surface_albedo_fitted_crb_nir",
            "cloud_height_crb_precision_nir",
            "cloud_albedo_crb_precision_nir",
            "cloud_fraction_crb_precision_nir",
            "regularization_parameter_crb_nir",
            "condition_number_crb_nir",
            "degrees_of_freedom_crb_nir",
            "shannon_information_content_crb_nir",
            "number_of_iterations_crb_nir",
            "fitted_root_mean_square_crb_nir",
            "convergence_flag_crb_nir",
            "effective_scene_height_nir",
            "effective_scene_albedo_nir",
            "effective_scene_height_precision_nir",
            "effective_scene_albedo_precision_nir",
            "condition_number_ge_nir",
            "degrees_of_freedom_ge_nir",
            "shannon_information_content_ge_nir",
            "number_of_iterations_ge_nir",
            "fitted_root_mean_square_ge_nir",
            "processing_quality_flags",
            "processing_quality_flags_crb",
            "qa_value_crb",
        ],
        "FILTER_VALUE": 0.5,
    },
    "gas_co": {
        "VARIABLE_LOC_IN_FILE": {
            "carbonmonoxide_total_column": "PRODUCT/carbonmonoxide_total_column",
            "carbonmonoxide_total_column_precision": "PRODUCT/carbonmonoxide_total_column_precision",
            "carbonmonoxide_total_column_corrected": "PRODUCT/carbonmonoxide_total_column_corrected",
            "solar_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_zenith_angle",
            "solar_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_azimuth_angle",
            "viewing_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_zenith_angle",
            "viewing_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_azimuth_angle",
            "geolocation_flags": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/geolocation_flags",
            "processing_quality_flags": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/processing_quality_flags",
            "number_of_spectral_points_in_retrieval": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_spectral_points_in_retrieval",
            "water_total_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/water_total_column",
            "water_total_column_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/water_total_column_precision",
            "semiheavy_water_total_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/semiheavy_water_total_column",
            "scattering_optical_thickness_SWIR": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/scattering_optical_thickness_SWIR",
            "height_scattering_layer": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/height_scattering_layer",
            "surface_albedo_2325": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/surface_albedo_2325",
            "surface_albedo_2335": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/surface_albedo_2335",
            "wavelength_calibration_offset": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_calibration_offset",
            "chi_square": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/chi_square",
            "degrees_of_freedom": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/degrees_of_freedom",
            "number_of_iterations": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations",
            "methane_total_column_prefit": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/methane_total_column_prefit",
            "methane_weak_twoband_total_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/methane_weak_twoband_total_column",
            "water_weak_twoband_total_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/water_weak_twoband_total_column",
            "water_strong_twoband_total_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/water_strong_twoband_total_column",
        },
        "DEFAULT_BANDS": [
            "carbonmonoxide_total_column",
            "carbonmonoxide_total_column_precision",
            "carbonmonoxide_total_column_corrected",
            "solar_zenith_angle",
            "solar_azimuth_angle",
            "viewing_zenith_angle",
            "viewing_azimuth_angle",
            "geolocation_flags",
            "processing_quality_flags",
            "number_of_spectral_points_in_retrieval",
            "water_total_column",
            "water_total_column_precision",
            "semiheavy_water_total_column",
            "scattering_optical_thickness_SWIR",
            "height_scattering_layer",
            "surface_albedo_2325",
            "surface_albedo_2335",
            "wavelength_calibration_offset",
            "chi_square",
            "degrees_of_freedom",
            "number_of_iterations",
            "methane_total_column_prefit",
            "methane_weak_twoband_total_column",
            "water_weak_twoband_total_column",
            "water_strong_twoband_total_column",
        ],
        "FILTER_VALUE": 0.5,
    },
    "gas_hcho": {
        "VARIABLE_LOC_IN_FILE": {
            "formaldehyde_tropospheric_vertical_column": "PRODUCT/formaldehyde_tropospheric_vertical_column",
            "formaldehyde_tropospheric_vertical_column_precision": "PRODUCT/formaldehyde_tropospheric_vertical_column_precision",
            "solar_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_zenith_angle",
            "solar_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_azimuth_angle",
            "viewing_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_zenith_angle",
            "viewing_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_azimuth_angle",
            "geolocation_flags": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/geolocation_flags",
            "formaldehyde_slant_column_corrected": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/formaldehyde_slant_column_corrected",
            "formaldehyde_clear_air_mass_factor": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/formaldehyde_clear_air_mass_factor",
            "formaldehyde_cloudy_air_mass_factor": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/formaldehyde_cloudy_air_mass_factor",
            "air_mass_factor_snow_ice_flag": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/air_mass_factor_snow_ice_flag",
            "fitted_radiance_shift": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_radiance_shift",
            "fitted_radiance_squeeze": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_radiance_squeeze",
            "fitted_root_mean_square": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_root_mean_square",
            "fitted_root_mean_square_win1": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_root_mean_square_win1",
            "number_of_iterations_in_retrieval": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations_in_retrieval",
            "processing_quality_flags": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/processing_quality_flags",
            "cloud_fraction_intensity_weighted": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_fraction_intensity_weighted",
        },
        "DEFAULT_BANDS": [
            "formaldehyde_tropospheric_vertical_column",
            "formaldehyde_tropospheric_vertical_column_precision",
            "solar_zenith_angle",
            "solar_azimuth_angle",
            "viewing_zenith_angle",
            "viewing_azimuth_angle",
            "geolocation_flags",
            "formaldehyde_slant_column_corrected",
            "formaldehyde_clear_air_mass_factor",
            "formaldehyde_cloudy_air_mass_factor",
            "air_mass_factor_snow_ice_flag",
            "fitted_radiance_shift",
            "fitted_radiance_squeeze",
            "fitted_root_mean_square",
            "fitted_root_mean_square_win1",
            "number_of_iterations_in_retrieval",
            "processing_quality_flags",
            "cloud_fraction_intensity_weighted",
        ],
        "FILTER_VALUE": 0.5,
    },
    "gas_no2": {
        "VARIABLE_LOC_IN_FILE": {
            "nitrogendioxide_tropospheric_column": "PRODUCT/nitrogendioxide_tropospheric_column",
            "nitrogendioxide_tropospheric_column_precision": "PRODUCT/nitrogendioxide_tropospheric_column_precision",
            "nitrogendioxide_tropospheric_column_precision_kernel": "PRODUCT/nitrogendioxide_tropospheric_column_precision_kernel",
            "air_mass_factor_troposphere": "PRODUCT/air_mass_factor_troposphere",
            "air_mass_factor_total": "PRODUCT/air_mass_factor_total",
            "tm5_tropopause_layer_index": "PRODUCT/tm5_tropopause_layer_index",
            "solar_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_zenith_angle",
            "solar_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_azimuth_angle",
            "viewing_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_zenith_angle",
            "viewing_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_azimuth_angle",
            "geolocation_flags": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/geolocation_flags",
            "processing_quality_flags": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/processing_quality_flags",
            "number_of_spectral_points_in_retrieval": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_spectral_points_in_retrieval",
            "number_of_iterations": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations",
            "wavelength_calibration_offset": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_calibration_offset",
            "wavelength_calibration_stretch": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_calibration_stretch",
            "wavelength_calibration_chi_square": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/wavelength_calibration_chi_square",
            "nitrogendioxide_stratospheric_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/nitrogendioxide_stratospheric_column",
            "nitrogendioxide_total_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/nitrogendioxide_total_column",
            "nitrogendioxide_summed_total_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/nitrogendioxide_summed_total_column",
            "nitrogendioxide_slant_column_density": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/nitrogendioxide_slant_column_density",
            "nitrogendioxide_geometric_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/nitrogendioxide_geometric_column",
            "ozone_slant_column_density": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/ozone_slant_column_density",
            "ozone_slant_column_density_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/ozone_slant_column_density_precision",
            "water_slant_column_density": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/water_slant_column_density",
            "water_slant_column_density_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/water_slant_column_density_precision",
            "water_liquid_slant_column_density": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/water_liquid_slant_column_density",
            "ring_coefficient": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/ring_coefficient",
            "ring_coefficient_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/ring_coefficient_precision",
            "chi_square": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/chi_square",
            "root_mean_square_error_of_fit": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/root_mean_square_error_of_fit",
            "degrees_of_freedom": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/degrees_of_freedom",
            "air_mass_factor_stratosphere": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/air_mass_factor_stratosphere",
            "air_mass_factor_cloudy": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/air_mass_factor_cloudy",
            "air_mass_factor_clear": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/air_mass_factor_clear",
            "nitrogendioxide_ghost_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/nitrogendioxide_ghost_column",
            "cloud_selection_flag": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_selection_flag",
            "runs_deviation": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/runs_deviation",
            "runs_longest": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/runs_longest",
            "fresco_cloud_fraction_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/FRESCO/fresco_cloud_fraction_crb",
            "fresco_cloud_pressure_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/FRESCO/fresco_cloud_pressure_crb",
            "fresco_scene_albedo": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/FRESCO/fresco_scene_albedo",
            "fresco_cloud_albedo_crb": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/FRESCO/fresco_cloud_albedo_crb",
            "fresco_surface_albedo": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/FRESCO/fresco_surface_albedo",
        },
        "DEFAULT_BANDS": [
            "nitrogendioxide_tropospheric_column",
            "nitrogendioxide_tropospheric_column_precision",
            "nitrogendioxide_tropospheric_column_precision_kernel",
            "air_mass_factor_troposphere",
            "air_mass_factor_total",
            "tm5_tropopause_layer_index",
            "solar_zenith_angle",
            "solar_azimuth_angle",
            "viewing_zenith_angle",
            "viewing_azimuth_angle",
            "geolocation_flags",
            "processing_quality_flags",
            "number_of_spectral_points_in_retrieval",
            "number_of_iterations",
            "wavelength_calibration_offset",
            "wavelength_calibration_stretch",
            "wavelength_calibration_chi_square",
            "nitrogendioxide_stratospheric_column",
            "nitrogendioxide_total_column",
            "nitrogendioxide_summed_total_column",
            "nitrogendioxide_slant_column_density",
            "nitrogendioxide_geometric_column",
            "ozone_slant_column_density",
            "ozone_slant_column_density_precision",
            "water_slant_column_density",
            "water_slant_column_density_precision",
            "water_liquid_slant_column_density",
            "ring_coefficient",
            "ring_coefficient_precision",
            "chi_square",
            "root_mean_square_error_of_fit",
            "degrees_of_freedom",
            "air_mass_factor_stratosphere",
            "air_mass_factor_cloudy",
            "air_mass_factor_clear",
            "nitrogendioxide_ghost_column",
            "cloud_selection_flag",
            "runs_deviation",
            "runs_longest",
            "fresco_cloud_fraction_crb",
            "fresco_cloud_pressure_crb",
            "fresco_scene_albedo",
            "fresco_cloud_albedo_crb",
            "fresco_surface_albedo",
        ],
        "FILTER_VALUE": 0.75,
    },
    "gas_o3": {
        "VARIABLE_LOC_IN_FILE": {
            "ozone_total_vertical_column": "PRODUCT/ozone_total_vertical_column",
            "ozone_total_vertical_column_precision": "PRODUCT/ozone_total_vertical_column_precision",
            "solar_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_zenith_angle",
            "solar_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_azimuth_angle",
            "viewing_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_zenith_angle",
            "viewing_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_azimuth_angle",
            "geolocation_flags": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/geolocation_flags",
            "number_of_iterations_slant_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations_slant_column",
            "fitted_root_mean_square": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_root_mean_square",
            "fitted_radiance_shift": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_radiance_shift",
            "fitted_radiance_squeeze": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_radiance_squeeze",
            "ozone_slant_column_ring_corrected": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/ozone_slant_column_ring_corrected",
            "number_of_spectral_points_in_retrieval": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_spectral_points_in_retrieval",
            "ozone_total_air_mass_factor": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/ozone_total_air_mass_factor",
            "ozone_total_air_mass_factor_trueness": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/ozone_total_air_mass_factor_trueness",
            "ozone_clear_air_mass_factor": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/ozone_clear_air_mass_factor",
            "ozone_clear_air_mass_factor_trueness": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/ozone_clear_air_mass_factor_trueness",
            "ozone_cloudy_air_mass_factor": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/ozone_cloudy_air_mass_factor",
            "cloud_fraction_intensity_weighted": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_fraction_intensity_weighted",
            "ozone_effective_temperature": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/ozone_effective_temperature",
            "ring_scale_factor": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/ring_scale_factor",
            "degrees_of_freedom": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/degrees_of_freedom",
            "condition_number": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/condition_number",
            "shannon_information_content": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/shannon_information_content",
            "regularization_parameter": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/regularization_parameter",
            "smoothing_error": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/smoothing_error",
            "ozone_ghost_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/ozone_ghost_column",
            "number_of_iterations_vertical_column": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations_vertical_column",
            "effective_scene_albedo": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/effective_scene_albedo",
            "effective_scene_albedo_precision": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/effective_scene_albedo_precision",
            "effective_albedo": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/effective_albedo",
            "convergence_flag": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/convergence_flag",
            "scene_pressure": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/scene_pressure",
            "euv": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/euv",
            "processing_quality_flags": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/processing_quality_flags",
        },
        "DEFAULT_BANDS": [
            "ozone_total_vertical_column",
            "ozone_total_vertical_column_precision",
            "solar_zenith_angle",
            "solar_azimuth_angle",
            "viewing_zenith_angle",
            "viewing_azimuth_angle",
            "geolocation_flags",
            "number_of_iterations_slant_column",
            "fitted_root_mean_square",
            "fitted_radiance_shift",
            "fitted_radiance_squeeze",
            "ozone_slant_column_ring_corrected",
            "number_of_spectral_points_in_retrieval",
            "ozone_total_air_mass_factor",
            "ozone_total_air_mass_factor_trueness",
            "ozone_clear_air_mass_factor",
            "ozone_clear_air_mass_factor_trueness",
            "ozone_cloudy_air_mass_factor",
            "cloud_fraction_intensity_weighted",
            "ozone_effective_temperature",
            "ring_scale_factor",
            "degrees_of_freedom",
            "condition_number",
            "shannon_information_content",
            "regularization_parameter",
            "smoothing_error",
            "ozone_ghost_column",
            "number_of_iterations_vertical_column",
            "effective_scene_albedo",
            "effective_scene_albedo_precision",
            "effective_albedo",
            "convergence_flag",
            "scene_pressure",
            "euv",
            "processing_quality_flags",
        ],
        "FILTER_VALUE": 0.5,
    },
    "gas_so2": {
        "VARIABLE_LOC_IN_FILE": {
            "sulfurdioxide_total_vertical_column": "PRODUCT/sulfurdioxide_total_vertical_column",
            "sulfurdioxide_total_vertical_column_precision": "PRODUCT/sulfurdioxide_total_vertical_column_precision",
            "solar_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_zenith_angle",
            "solar_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_azimuth_angle",
            "viewing_zenith_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_zenith_angle",
            "viewing_azimuth_angle": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_azimuth_angle",
            "geolocation_flags": "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/geolocation_flags",
            "selected_fitting_window_flag": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/selected_fitting_window_flag",
            "sulfurdioxide_slant_column_corrected": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/sulfurdioxide_slant_column_corrected",
            "fitted_root_mean_square_win1": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_root_mean_square_win1",
            "fitted_root_mean_square_win2": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_root_mean_square_win2",
            "fitted_root_mean_square_win3": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_root_mean_square_win3",
            "fitted_radiance_shift": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_radiance_shift",
            "fitted_radiance_squeeze": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_radiance_squeeze",
            "fitted_radiance_shift_win1": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_radiance_shift_win1",
            "fitted_radiance_squeeze_win1": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_radiance_squeeze_win1",
            "number_of_spectral_points_in_retrieval": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_spectral_points_in_retrieval",
            "fitted_radiance_shift_win2": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_radiance_shift_win2",
            "fitted_radiance_squeeze_win2": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_radiance_squeeze_win2",
            "fitted_radiance_shift_win3": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_radiance_shift_win3",
            "fitted_radiance_squeeze_win3": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_radiance_squeeze_win3",
            "qa_value_box_profile": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/qa_value_box_profile",
            "air_mass_factor_snow_ice_flag": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/air_mass_factor_snow_ice_flag",
            "sulfurdioxide_detection_flag": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/sulfurdioxide_detection_flag",
            "number_of_iterations_in_retrieval": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations_in_retrieval",
            "number_of_iterations_in_retrieval_win1": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations_in_retrieval_win1",
            "number_of_iterations_in_retrieval_win2": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations_in_retrieval_win2",
            "number_of_iterations_in_retrieval_win3": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/number_of_iterations_in_retrieval_win3",
            "fitted_root_mean_square": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_root_mean_square",
            "sulfurdioxide_slant_column_cobra": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/sulfurdioxide_slant_column_cobra",
            "sulfurdioxide_slant_column_cobra_flag": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/sulfurdioxide_slant_column_cobra_flag",
            "fitted_root_mean_square_cobra": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/fitted_root_mean_square_cobra",
            "cloud_fraction_intensity_weighted": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_fraction_intensity_weighted",
            "processing_quality_flags": "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/processing_quality_flags",
        },
        "DEFAULT_BANDS": [
            "sulfurdioxide_total_vertical_column",
            "sulfurdioxide_total_vertical_column_precision",
            "solar_zenith_angle",
            "solar_azimuth_angle",
            "viewing_zenith_angle",
            "viewing_azimuth_angle",
            "geolocation_flags",
            "selected_fitting_window_flag",
            "sulfurdioxide_slant_column_corrected",
            "fitted_root_mean_square_win1",
            "fitted_root_mean_square_win2",
            "fitted_root_mean_square_win3",
            "fitted_radiance_shift",
            "fitted_radiance_squeeze",
            "fitted_radiance_shift_win1",
            "fitted_radiance_squeeze_win1",
            "number_of_spectral_points_in_retrieval",
            "fitted_radiance_shift_win2",
            "fitted_radiance_squeeze_win2",
            "fitted_radiance_shift_win3",
            "fitted_radiance_squeeze_win3",
            "qa_value_box_profile",
            "air_mass_factor_snow_ice_flag",
            "sulfurdioxide_detection_flag",
            "number_of_iterations_in_retrieval",
            "number_of_iterations_in_retrieval_win1",
            "number_of_iterations_in_retrieval_win2",
            "number_of_iterations_in_retrieval_win3",
            "fitted_root_mean_square",
            "sulfurdioxide_slant_column_cobra",
            "sulfurdioxide_slant_column_cobra_flag",
            "fitted_root_mean_square_cobra",
            "cloud_fraction_intensity_weighted",
            "processing_quality_flags",
        ],
        "FILTER_VALUE": 0.5,
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
def get_bounding_polygon(lat: np.ndarray, lon: np.ndarray) -> Polygon:
    """Get bounding polygon from lat-lon arrays.

    Args:
        lat (Array of float): Pixel centers latitude.
        lon (Array of float): Pixel centers longitude.

    Returns:
        polygon_lat (Array of float): Polygon latitude coordinates.
        polygon_lon (Array of float): Polygon longitude coordinates.

    """

    def expand_edge(edge, neighbor):
        return edge + (neighbor - edge) * 0.5  # only expand the edge by half the distance to the neighbor

    # Bottom (row 0)
    bottom_lat = expand_edge(lat[0, :], lat[1, :]).flatten()
    bottom_lon = expand_edge(lon[0, :], lon[1, :]).flatten()
    # Top (last row)
    top_lat = expand_edge(lat[-1, :], lat[-2, :]).flatten()
    top_lon = expand_edge(lon[-1, :], lon[-2, :]).flatten()
    # Left (column 0)
    left_lat = expand_edge(lat[:, 0], lat[:, 1]).flatten()
    left_lon = expand_edge(lon[:, 0], lon[:, 1]).flatten()
    # Right (last column)
    right_lat = expand_edge(lat[:, -1], lat[:, -2]).flatten()
    right_lon = expand_edge(lon[:, -1], lon[:, -2]).flatten()

    polygon_lat = np.concatenate([top_lat, right_lat[-2::-1], bottom_lat[::-1][1:], left_lat[1:-1]])
    polygon_lon = np.concatenate([top_lon, right_lon[-2::-1], bottom_lon[::-1][1:], left_lon[1:-1]])
    polygon = Polygon(zip(polygon_lon, polygon_lat))
    return polygon


@typechecked
def get_mask_from_polygon(lon: np.ndarray, lat: np.ndarray, polygon: Polygon) -> np.ndarray:
    """Mask coordinates (lat,lon) that are not inside the polygon.

    Args:
        lon (2d Array of float): Pixel centers longitude.
        lat (2d Array of float): Pixel centers latitude.
        polygon (shapely Polygon): Polygon to mask the coordinates.

    Returns:
        mask (Array of bool): Boolean mask for the coordinates inside the polygon.

    """
    # Create meshgrid of lon, lat
    shape_data = lat.shape
    # Flatten meshgrid for vectorized point-in-polygon test
    points = np.column_stack((lon.ravel(), lat.ravel()))
    # Use list comprehension for shapely point-in-polygon
    mask_flat = np.array([polygon.contains(Point(x, y)) for x, y in points])
    mask = mask_flat.reshape(shape_data)
    return mask


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

        # create a bounding polygon for the data based on lat-lon arrays
        data["bounding_polygon"] = get_bounding_polygon(data["latitude"], data["longitude"])

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
def fill_and_mask_data(band_data: np.ndarray, spatio_temporal_mask: np.ndarray):
    """Fill nan values based on data mask and spatio-temporal mask.

    Args:
        band_data (Array of float): masked 2-d array of band data.
        spatio_temporal_mask (Array of bool): 2-d boolean mask representing valid data

    Returns:
        data (Array of float): 2-d array of band data after filling and masking.

    """
    # fill nan values where data is not valid
    if hasattr(band_data, "filled"):
        if np.issubdtype(band_data.dtype, np.integer):
            print(f"converting to float to fill with nan. (Was {band_data.dtype})")
            band_data = band_data.astype(float)
        band_data = band_data.filled(np.nan)
    # set data to nan based on the spatial-temporal extent.
    data = np.where(spatio_temporal_mask, band_data, np.nan)
    data = _get_2d_data_from_mask(data, spatio_temporal_mask)
    return data


@typechecked
def _get_2d_data_from_mask(data: np.ndarray, mask: np.ndarray) -> np.ndarray:
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


@typechecked
def interpolate(
    source_coordinates: np.ndarray, source_data: np.ndarray, target_coordinates: np.ndarray, method: str = "nearest"
) -> np.ndarray:
    """Interpolate source data to target grid based on source and target coordinates.

    Args:
        source_coordinates (Array of float): 2-d array of shape (n, 2) representing source coordinates (lon, lat).
        source_data (Array of float): 1-d array of shape (n,) representing source data values.
        target_coordinates (Array of float): 2-d array of shape (m, 2) representing target coordinates (lon, lat).
        method (str): Interpolation method. Options are "Nearest", "Linear", "Cubic".

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
