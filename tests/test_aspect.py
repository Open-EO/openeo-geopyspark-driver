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

def test_aspect(imagecollection_with_two_bands_and_three_dates):
    input = imagecollection_with_two_bands_and_three_dates
    result = input.aspect()
    logger.info(result)
