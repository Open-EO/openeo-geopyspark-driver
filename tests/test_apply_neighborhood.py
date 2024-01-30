import datetime

import numpy as np
from geopyspark import Tile, SpaceTimeKey
from typing import List, Tuple
import pytest
from numpy.testing import assert_array_almost_equal

from openeo_driver.utils import EvalEnv
from .data import get_test_data_file

def test_apply_neighborhood_no_overlap(imagecollection_with_two_bands_and_three_dates):
    the_date = datetime.datetime(2017, 9, 25, 11, 37)
    graph = {
        "abs": {
            "arguments": {
                "p": {
                    "from_parameter": "data"
                },
                "base": 2
            },
            "process_id": "power",
            "result": True
        }
    }
    input = imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    result = imagecollection_with_two_bands_and_three_dates.apply_neighborhood(
        process=graph,
        size=[{'dimension': 'x', 'unit': 'px', 'value': 32}, {'dimension': 'y', 'unit': 'px', 'value': 32},
              {'dimension': 't', 'value': "P1D"}],
        overlap=[],
        context={},
        env=EvalEnv(),
    )
    result_array = result.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    print(result_array)
    expected_result = np.power(2, input)
    print(expected_result)
    assert_array_almost_equal(expected_result, result_array)


@pytest.mark.parametrize("udf", [("udf_noop"), ("udf_noop_jep")])
def test_apply_neighborhood_overlap_udf(imagecollection_with_two_bands_and_three_dates, udf, request):
    udf = request.getfixturevalue(udf)
    the_date = datetime.datetime(2017, 9, 25, 11, 37)
    input = imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    result = imagecollection_with_two_bands_and_three_dates.apply_neighborhood(
        process=udf,
        size=[{'dimension': 'x', 'unit': 'px', 'value': 32}, {'dimension': 'y', 'unit': 'px', 'value': 32}],
        overlap=[{'dimension': 'x', 'unit': 'px', 'value': 8}, {'dimension': 'y', 'unit': 'px', 'value': 8}],
        context={},
        env=EvalEnv()
    )
    result_xarray = result._to_xarray()
    first_band = result_xarray.sel(bands='red', t=the_date)
    # assert_array_almost_equal(input[0],first_band[ :input.shape[1], :input.shape[2]])
    print(first_band)
    result_array = result.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells

    subresult = result_array[:input.shape[0], :input.shape[1], :input.shape[2]]
    assert_array_almost_equal(input, subresult)


def test_apply_neighborhood_on_timeseries(imagecollection_with_two_bands_and_three_dates):
    the_date = datetime.datetime(2017, 9, 25, 11, 37)
    graph = {
        "power": {
            "arguments": {
                "p": {
                    "from_parameter": "data"
                },
                "base": 2
            },
            "process_id": "power",
            "result": True
        }
    }

    result = imagecollection_with_two_bands_and_three_dates.apply_neighborhood(
        process=graph,
        size=[{'dimension': 'x', 'unit': 'px', 'value': 1}, {'dimension': 'y', 'unit': 'px', 'value': 1},
              {'dimension': 't', 'value': "month"}],
        overlap=[],
        context={},
        env=EvalEnv(),
    )
    result_array = result.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    print(result_array)
    input = imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    expected_result = np.power(2, input)
    print(expected_result)
    assert_array_almost_equal(expected_result, result_array)


def overlap_jep_multiresolution_test(image_collection, size, overlap, udf_code, is_time_series=False):
    image_collection.metadata.spatial_extent = None
    image_collection.metadata.temporal_extent = None

    udf_multiresolution = {
        "udf_process": {
            "process_id": "run_udf",
            "arguments": {
                "data": {
                    "from_parameter": "data"
                },
                "udf": udf_code,
                "runtime": "Python-Jep",
            },
            "result": True
        },
    }

    size_dict = [
        {'dimension': 'x', 'unit': 'px', 'value': size},
        {'dimension': 'y', 'unit': 'px', 'value': size}
    ]
    if is_time_series:
        size_dict.append({'dimension': 't', 'value': "P1D"})

    result = image_collection.apply_neighborhood(
        process=udf_multiresolution,
        size=size_dict,
        overlap=[{'dimension': 'x', 'unit': 'px', 'value': overlap},
                 {'dimension': 'y', 'unit': 'px', 'value': overlap}],
        context={},
        env=EvalEnv()
    )

    input_array: List[Tuple[SpaceTimeKey, Tile]] = image_collection.get_max_level().to_numpy_rdd().collect()
    result_array: List[Tuple[SpaceTimeKey, Tile]] = result.get_max_level().to_numpy_rdd().collect()
    input_head_cells = input_array[0][1].cells
    result_head_cells = result_array[0][1].cells

    # Compare if resulting array has 2x the size of the input array
    expected_shape = (2, size * 2, size * 2)
    assert expected_shape == result_head_cells.shape

    # Compare if each pixel is duplicated into 4 pixels
    assert_array_almost_equal(input_head_cells[0, :size, :size], result_head_cells[0, ::2, ::2])
    # Assert that the number of Tiles remains consistent with the resolution change: 4x(256x256)*4 == 256x(64x64)
    input_nr_pixels = len(input_array) * input_head_cells.shape[0] * input_head_cells.shape[1] * input_head_cells.shape[2]
    output_nr_pixels = len(result_array) * result_head_cells.shape[0] * result_head_cells.shape[1] * result_head_cells.shape[2]
    assert input_nr_pixels * 4 == output_nr_pixels
    # Check if the date changed
    assert input_array[0][0].instant == result_array[0][0].instant


def test_apply_neighborhood_overlap_jep_multiresolution(imagecollection_with_two_bands_and_one_date_multiple_values):
    file_name = get_test_data_file("udf_multiresolution.py")
    with open(file_name, "r") as f:
        udf_code = f.read()

    overlap_jep_multiresolution_test(imagecollection_with_two_bands_and_one_date_multiple_values, 32, 8, udf_code)


def test_apply_neighborhood_overlap_jep_multiresolution_timeseries(imagecollection_with_two_bands_and_one_date_multiple_values):
    file_name = get_test_data_file("udf_multiresolution.py")
    with open(file_name, "r") as f:
        udf_code = f.read()

    overlap_jep_multiresolution_test(imagecollection_with_two_bands_and_one_date_multiple_values, 32, 8, udf_code, is_time_series=True)
