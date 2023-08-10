import datetime

import numpy as np
import pytest
from numpy.testing import assert_array_almost_equal

from openeo_driver.errors import FeatureUnsupportedException
from openeo_driver.utils import EvalEnv


def test_apply_dimension_array_interpolate_linear(imagecollection_with_two_bands_and_three_dates):
    the_date = datetime.datetime(2017, 9, 30, 00, 37)
    graph = {
        "array_interpolate_linear": {
            "arguments": {
                "data": {
                    "from_parameter": "data"
                }
            },
            "process_id": "array_interpolate_linear",
            "result": True
        }
    }
    result = imagecollection_with_two_bands_and_three_dates.apply_dimension(
        process=graph, dimension="t", target_dimension="some_other_dim", context={}, env=EvalEnv()
    )
    result_array = result.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells

    assert np.all(np.isclose(result_array, 1))



def test_apply_dimension_temporal_udf(imagecollection_with_two_bands_and_three_dates,udf_noop):
    the_date = datetime.datetime(2017, 9, 25, 11, 37)

    input = imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    result = imagecollection_with_two_bands_and_three_dates.apply_dimension(
        process=udf_noop, dimension="t", target_dimension="some_other_dim", context={}, env=EvalEnv()
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
        process=udf_noop, dimension="bands", target_dimension=None, context={}, env=EvalEnv()
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
            process=udf_noop, dimension="bla", target_dimension=None, context={}, env=EvalEnv()
        )


true = True

#
# @pytest.mark.parametrize("process", [
#     {
#         "arrayapply1": {
#             "process_id": "array_apply",
#             "arguments": {
#                 "data": {
#                     "from_parameter": "data"
#                 },
#                 "process": {
#                     "process_graph": {
#                         "cos1": {
#                             "process_id": "cos",
#                             "arguments": {
#                                 "x": {
#                                     "from_parameter": "x"
#                                 }
#                             },
#                             "result": true
#                         }
#                     }
#                 }
#             },
#             "result": true
#         }
#     },
#     {
#         "cos1": {
#             "process_id": "cos",
#             "arguments": {
#                 "x": {
#                     "from_parameter": "data"
#                 }
#             },
#             "result": true
#         }
#     },
# ])
# def test_apply_dimension_array_apply(imagecollection_with_two_bands_and_three_dates, tmp_path, process):
#     dc = imagecollection_with_two_bands_and_three_dates
#
#     dc_array_apply = dc.apply_dimension(
#         process=process,
#         dimension='t',
#         env=EvalEnv(),
#     )
#     dc_xarray = dc._to_xarray()
#     dc_array_apply_xarray = dc_array_apply._to_xarray()
#     assert dc_xarray[0, 0].data[0, 0] == 1
#     assert dc_array_apply_xarray[0, 0].data[0, 0] == math.cos(1)
#     assert dc_xarray[2, 0].data[0, 0] == 2
#     assert dc_array_apply_xarray[2, 0].data[0, 0] == math.cos(2)
