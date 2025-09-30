import datetime
from typing import List, Tuple

import numpy as np
import pytest
from geopyspark import SpaceTimeKey, Tile
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
    slope_band = result_xarray.sel(bands='slope', t=the_date)
    print("##### slope_band")
    logger.info("##### slope_band")
    print(slope_band)
    logger.info(slope_band)




