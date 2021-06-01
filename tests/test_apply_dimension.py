import datetime

import pytest
from openeo_driver.errors import FeatureUnsupportedException
from openeo_driver.utils import EvalEnv
from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
from .data import get_test_data_file

import numpy as np
from numpy.testing import assert_array_almost_equal


def test_apply_dimension_array_interpolate_linear(imagecollection_with_two_bands_and_three_dates):
    the_date = datetime.datetime(2017, 9, 25, 11, 37)
    graph = {
        "array_interpolate_linear": {
            "arguments": {
                "data": {
                    "from_argument": "data"
                }
            },
            "process_id": "array_interpolate_linear",
            "result": True
        }
    }
    input = imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    result = imagecollection_with_two_bands_and_three_dates.apply_dimension(
        process=graph,
       dimension='t',
        target_dimension='some_other_dim',
        env=EvalEnv(),
        context={'bla':'bla'}
    )
    result_array = result.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    print(input)
    print(result_array)
    #expected_result = np.power(2, input)
    #print(expected_result)
    #assert_array_almost_equal(expected_result, result_array)



def test_apply_dimension_temporal_udf(imagecollection_with_two_bands_and_three_dates,udf_noop):
    the_date = datetime.datetime(2017, 9, 25, 11, 37)

    input = imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    result = imagecollection_with_two_bands_and_three_dates.apply_dimension(
        process=udf_noop,
       dimension='t',
        target_dimension='some_other_dim',
        env=EvalEnv(),
        context={'bla':'bla'}
    )
    result_xarray = result._to_xarray()
    first_band = result_xarray.sel(bands='red', t=the_date)
    # assert_array_almost_equal(input[0],first_band[ :input.shape[1], :input.shape[2]])
    print(first_band)
    result_array = result.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells

    subresult = result_array[:input.shape[0], :input.shape[1], :input.shape[2]]
    assert_array_almost_equal(input, subresult)



def test_apply_dimension_bands_udf(imagecollection_with_two_bands_and_three_dates,udf_noop):
    the_date = datetime.datetime(2017, 9, 25, 11, 37)

    input = imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    result = imagecollection_with_two_bands_and_three_dates.apply_dimension(
        process=udf_noop,
       dimension='bands',
        env=EvalEnv()
    )
    result_xarray = result._to_xarray()
    first_band = result_xarray.sel(bands='red', t=the_date)
    # assert_array_almost_equal(input[0],first_band[ :input.shape[1], :input.shape[2]])
    print(first_band)
    result_array = result.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells

    subresult = result_array[:input.shape[0], :input.shape[1], :input.shape[2]]
    assert_array_almost_equal(input, subresult)


def test_apply_dimension_invalid_dimension(imagecollection_with_two_bands_and_three_dates,udf_noop):
    the_date = datetime.datetime(2017, 9, 25, 11, 37)
    with pytest.raises(FeatureUnsupportedException):
        result = imagecollection_with_two_bands_and_three_dates.apply_dimension(
            process=udf_noop,
           dimension='bla',
            env=EvalEnv()
        )
