from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
from .data import get_test_data_file

import numpy as np
from numpy.testing import assert_array_almost_equal

def test_reduce_bands_reduce_time(imagecollection_with_two_bands_and_three_dates,udf_noop):
    visitor = GeotrellisTileProcessGraphVisitor()
    graph = {
        "sum": {
            "arguments": {
                "data": {
                    "from_argument": "dimension_data"
                }
            },
            "process_id": "sum",
            "result": True
        }
    }


    reducer = visitor.accept_process_graph(graph)
    result = imagecollection_with_two_bands_and_three_dates.reduce_dimension(dimension="bands",reducer=reducer)\
        .reduce_dimension(dimension='t',reducer=udf_noop).pyramid.levels[0].stitch()

    matrix_of_three = np.full((1, 8, 8),3.0)

    assert_array_almost_equal(matrix_of_three,result.cells)
