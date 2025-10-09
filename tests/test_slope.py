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
    result = input.slope()

    logger.info(result)
    the_date = datetime.datetime(2017, 9, 25, 11, 37)
    result_xarray = result._to_xarray()
    t1_slope_band = result_xarray.sel(bands='slope', t=the_date)
    t1_slope_values: ndarray = t1_slope_band.values
    assert t1_slope_values[0, 0] == 0
    assert np.isnan(t1_slope_values[31,31])
    t2_slope_band = result_xarray.sel(bands='slope', t=the_date)
    t2_slope_values: ndarray = t2_slope_band.values
    assert np.isnan(t2_slope_values[0, 0])
    assert np.isnan(t2_slope_values[31,31])
