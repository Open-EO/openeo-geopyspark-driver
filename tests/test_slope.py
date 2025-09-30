import datetime
from typing import List, Tuple

import numpy as np
import pytest
from geopyspark import SpaceTimeKey, Tile
from numpy import ndarray
from numpy.testing import assert_array_almost_equal
from openeo_driver.utils import EvalEnv

from .data import get_test_data_file

import logging

logger = logging.getLogger(__name__)

def test_slope(imagecollection_with_two_bands_and_three_dates):
    input = imagecollection_with_two_bands_and_three_dates
    logger.info("##### input metadata")
    logger.info(input.metadata)
    logger.info("##### input")
    logger.info(input)
    result = input.slope()
    logger.info("##### result.metadata")
    logger.info(result.metadata)
    logger.info("##### result")
    logger.info(result)

    the_date = datetime.datetime(2017, 9, 25, 11, 37)
    input_xarray = input._to_xarray()
    logger.info("##### input xarray")
    logger.info(input_xarray)
    result_xarray = result._to_xarray()
    logger.info("##### result xarray")
    logger.info(result_xarray)
    red_band = input_xarray.sel(bands='red', t=the_date)
    logger.info("##### input red band")
    logger.info(red_band)
    nir_band = input_xarray.sel(bands='nir', t=the_date)
    logger.info("##### input nir band")
    logger.info(nir_band)
    t1_slope_band = result_xarray.sel(bands='slope', t=the_date)
    logger.info("##### result t1 slope band")
    t1_slope_values: ndarray = t1_slope_band.values
    logger.info(t1_slope_values)
    assert t1_slope_values[0, 0] == 0
    assert t1_slope_values[31,31] == np.NaN
    t2_slope_band = result_xarray.sel(bands='slope', t=the_date)
    logger.info("##### result t2 slope band")
    t2_slope_values: ndarray = t2_slope_band.values
    logger.info(t2_slope_values)
    assert t2_slope_values[0, 0] == np.NaN
    assert t2_slope_values[31,31] == np.NaN
