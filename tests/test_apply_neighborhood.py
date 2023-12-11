import datetime

import numpy as np
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



def test_apply_neighborhood_overlap_jep_multiresolution(imagecollection_with_two_bands_and_one_date_multiple_values):
    imagecollection_with_two_bands_and_one_date_multiple_values.metadata.spatial_extent = None
    imagecollection_with_two_bands_and_one_date_multiple_values.metadata.temporal_extent = None
    file_name = get_test_data_file("udf_multiresolution.py")
    with open(file_name, "r") as f:
        udf_code = f.read()
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
    the_date = datetime.datetime(2017, 9, 25, 11, 37)
    # input_cells = imagecollection_with_two_bands_and_one_date_multiple_values.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    imagecollection_with_two_bands_and_one_date_multiple_values.save_result("input_multires.nc", "NetCDF")
    result = imagecollection_with_two_bands_and_one_date_multiple_values.apply_neighborhood(
        process=udf_multiresolution,
        size=[{'dimension': 'x', 'unit': 'px', 'value': 32}, {'dimension': 'y', 'unit': 'px', 'value': 32}],
        overlap=[{'dimension': 'x', 'unit': 'px', 'value': 8}, {'dimension': 'y', 'unit': 'px', 'value': 8}],
        context={},
        env=EvalEnv()
    )
    result.save_result("output_multires_fixed.nc", "NetCDF")
