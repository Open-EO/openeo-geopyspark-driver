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
    print(result.metadata)
    print(result)


