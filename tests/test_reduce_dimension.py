from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
from .data import get_test_data_file

import numpy as np
from numpy.testing import assert_array_almost_equal

def test_reduce_bands_reduce_time(imagecollection_with_two_bands_and_three_dates):
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

    file_name = get_test_data_file("udf_noop.py")
    with open(file_name, "r")  as f:
        udf_code = f.read()

    noop_udf_callback = {
        "udf_process": {
            "arguments": {
                "data": {
                    "from_argument": "dimension_data"
                },
                "udf": udf_code
            },
            "process_id": "run_udf",
            "result": True
        },
    }

    reducer = visitor.accept_process_graph(graph)
    result = imagecollection_with_two_bands_and_three_dates.reduce_dimension(dimension="bands",reducer=reducer)\
        .reduce_dimension(dimension='t',reducer=noop_udf_callback).pyramid.levels[0].stitch()

    matrix_of_three = np.full((1, 8, 8),3.0)

    assert_array_almost_equal(matrix_of_three,result.cells)
