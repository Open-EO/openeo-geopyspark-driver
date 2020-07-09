import datetime

from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
from .data import get_test_data_file

import numpy as np
from numpy.testing import assert_array_almost_equal

def test_apply_neighborhood_no_overlap(imagecollection_with_two_bands_and_three_dates):
    graph = {
        "abs": {
            "arguments": {
                "p": {
                    "from_argument": "data"
                },
                "base":2
            },
            "process_id": "power",
            "result": True
        }
    }
    input = imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(datetime.datetime(2017, 9, 25, 11, 37)).stitch().cells
    result = imagecollection_with_two_bands_and_three_dates.apply_neighborhood(process=graph,size=[{'dimension':'x','unit':'px','value':32},{'dimension':'y','unit':'px','value':32},{'dimension':'t','value':"P1D"}],overlap=[])
    result_array = result.pyramid.levels[0].to_spatial_layer(datetime.datetime(2017, 9, 25, 11, 37)).stitch().cells
    print(result_array)
    expected_result = np.power(2,input)
    print(expected_result)
    assert_array_almost_equal(expected_result,result_array)


def test_apply_neighborhood_overlap_udf(imagecollection_with_two_bands_and_three_dates, udf_noop):
    input = imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(datetime.datetime(2017, 9, 25, 11, 37)).stitch().cells
    result = imagecollection_with_two_bands_and_three_dates.apply_neighborhood(process=udf_noop,size=[{'dimension':'x','unit':'px','value':32},{'dimension':'y','unit':'px','value':32}],overlap=[{'dimension':'y','unit':'px','value':2}])
    result_array = result.pyramid.levels[0].to_spatial_layer(datetime.datetime(2017, 9, 25, 11, 37)).stitch().cells

    subresult = result_array[:input.shape[0], :input.shape[1], :input.shape[2]]
    assert_array_almost_equal(input,subresult)