import datetime
from typing import List, Tuple

import numpy as np
import pytest
from geopyspark import SpaceTimeKey, Tile
from numpy.testing import assert_array_almost_equal
from openeo_driver.utils import EvalEnv

from .data import get_test_data_file

def test_slope(imagecollection_with_two_bands_and_three_dates):
    result = imagecollection_with_two_bands_and_three_dates.slope()
    print("##### result.metadata")
    print(result.metadata)
    print("##### result")
    print(result)

    the_date = datetime.datetime(2017, 9, 25, 11, 37)
    result_xarray = result._to_xarray()
    slope_band = result_xarray.sel(bands='slope', t=the_date)
    print("##### slope_band")
    print(slope_band)




