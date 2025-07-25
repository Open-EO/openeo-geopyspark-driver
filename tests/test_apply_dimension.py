import datetime
from typing import Tuple, List

import pytest
import numpy as np
import geopyspark as gps
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


@pytest.mark.parametrize("udf", [("udf_noop"), ("udf_noop_jep")])
def test_apply_dimension_temporal_udf(imagecollection_with_two_bands_and_three_dates, udf, request):
    udf = request.getfixturevalue(udf)
    the_date = datetime.datetime(2017, 9, 25, 11, 37)

    input = imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    result = imagecollection_with_two_bands_and_three_dates.apply_dimension(
        process=udf, dimension="t", target_dimension="some_other_dim", context={}, env=EvalEnv()
    )
    result_xarray = result._to_xarray()
    first_band = result_xarray.sel(bands='red', t=the_date)
    # assert_array_almost_equal(input[0],first_band[ :input.shape[1], :input.shape[2]])
    print(first_band)
    result_array = result.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells

    subresult = result_array[:input.shape[0], :input.shape[1], :input.shape[2]]
    assert_array_almost_equal(input, subresult)


@pytest.mark.parametrize("udf", [("udf_noop"), ("udf_noop_jep")])
def test_apply_dimension_bands_udf(imagecollection_with_two_bands_and_three_dates, udf, request):
    udf = request.getfixturevalue(udf)
    the_date = datetime.datetime(2017, 9, 25, 11, 37)

    input = imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells
    result = imagecollection_with_two_bands_and_three_dates.apply_dimension(
        process=udf, dimension="bands", target_dimension=None, context={}, env=EvalEnv()
    )
    result_xarray = result._to_xarray()
    first_band = result_xarray.sel(bands='red', t=the_date)
    # assert_array_almost_equal(input[0],first_band[ :input.shape[1], :input.shape[2]])
    print(first_band)
    result_array = result.pyramid.levels[0].to_spatial_layer(the_date).stitch().cells

    subresult = result_array[:input.shape[0], :input.shape[1], :input.shape[2]]
    assert_array_almost_equal(input, subresult)


def test_apply_metadata(imagecollection_with_two_bands_and_three_dates, identity_udf_rename_bands):

    result = imagecollection_with_two_bands_and_three_dates.apply_dimension(
        process=identity_udf_rename_bands, dimension="bands", target_dimension=None, context={}, env=EvalEnv()
    )

    assert result.metadata.band_names == ["computed_band_1", "computed_band_2"]
    result_xarray = result._to_xarray()
    assert list(result_xarray.bands.values) == ["computed_band_1", "computed_band_2"]


def test_apply_dimension_invalid_dimension(imagecollection_with_two_bands_and_three_dates,udf_noop):
    the_date = datetime.datetime(2017, 9, 25, 11, 37)
    with pytest.raises(FeatureUnsupportedException):
        result = imagecollection_with_two_bands_and_three_dates.apply_dimension(
            process=udf_noop, dimension="bla", target_dimension=None, context={}, env=EvalEnv()
        )


def test_add_dimension_spatial(imagecollection_with_two_bands_spatial_only):
    cube = imagecollection_with_two_bands_spatial_only
    assert cube.metadata.has_temporal_dimension()==False
    date_str = "2011-12-03T10:00:00Z"
    result = cube.add_dimension("t",date_str,"temporal")
    assert cube.metadata.spatial_dimensions == result.metadata.spatial_dimensions
    assert result.metadata.has_temporal_dimension()
    assert result.metadata.temporal_dimension.name=="t"
    assert len(result.metadata.temporal_dimension.extent) == 2
    assert result.metadata.temporal_dimension.extent[0]== date_str
    assert result.metadata.temporal_dimension.extent[1] == date_str
    assert result.get_max_level().layer_type == gps.LayerType.SPACETIME
    numpy_cube: List[Tuple[gps.SpaceTimeKey, gps.Tile]] = result.get_max_level().to_numpy_rdd().collect()
    assert isinstance(numpy_cube[0][0], gps.SpaceTimeKey)
    for tile in numpy_cube:
        assert tile[0].instant == datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")


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
