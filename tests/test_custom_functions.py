import datetime
import textwrap
from unittest import TestCase

import geopyspark as gps
import numpy as np
import pytest
import pytz
from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from pyspark import SparkContext
from shapely.geometry import Point, Polygon

from openeo_driver.save_result import AggregatePolygonResultCSV
from openeo_driver.utils import EvalEnv
from openeogeotrellis.backend import GeoPySparkBackendImplementation
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube


@pytest.mark.usefixtures("imagecollection_with_two_bands_and_one_date")
class TestCustomFunctions(TestCase):

    first = np.zeros((1, 4, 4))
    first.fill(1)

    second = np.zeros((1, 4, 4))
    second.fill(2)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 4.0, 'ymax': 4.0}
    layout = {'layoutCols': 1, 'layoutRows': 1, 'tileCols': 4, 'tileRows': 4}

    now = datetime.datetime.strptime("2017-09-25T11:37:00Z", '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)

    points = [
        Point(1.0, -3.0),
        Point(2.0, 4.0),
        Point(3.0, 3.0),
        Point(1.0, -2.0),
        Point(-10.0, 15.0)
    ]

    labeled_points = {
        'A': points[0],
        'B': points[1],
        'C': points[2],
        'D': points[3],
        'E': points[4]
    }

    expected_spatial_points_list = [
        (Point(1.0, -3.0), [1, 2]),
        (Point(2.0, 4.0), [1, 2]),
        (Point(3.0, 3.0), [1, 2]),
        (Point(1.0, -2.0), [1, 2]),
        (Point(-10.0, 15.0), None)
    ]

    expected_spacetime_points_list = [
        (Point(1.0, -3.0), now, [3]),
        (Point(2.0, 4.0), now, [3]),
        (Point(3.0, 3.0), now, [3]),
        (Point(1.0, -2.0), now, [3]),
        (Point(-10.0, 15.0), None, None)
    ]



    def test_apply_to_tile(self):
        def custom_function(cells:np.ndarray,nd):
            return cells[0]+cells[1]
        cells = np.array([self.first, self.second], dtype='int')
        tile = Tile.from_numpy_array(cells, -1)
        custom_function(tile.cells,0)

    def test_polygon_series(self):
        polygon = Polygon([(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)])
        means = self.imagecollection_with_two_bands_and_one_date.zonal_statistics(regions=polygon, func="mean")
        assert isinstance(means, AggregatePolygonResultCSV)
        assert means.get_data() == {"2017-09-25T11:37:00Z": [[1.0, 2.0]]}

    def _create_spacetime_layer(self, no_data):
        def tile(value):
            cells = np.zeros((4, 4), dtype=float)
            cells.fill(value)
            return Tile.from_numpy_array(cells, no_data)

        tiles = [(SpaceTimeKey(0, 0, self.now), tile(0)),
                 (SpaceTimeKey(1, 0, self.now), tile(1)),
                 (SpaceTimeKey(0, 1, self.now), tile(2)),
                 (SpaceTimeKey(1, 1, self.now), tile(no_data))]

        for tile in tiles:
            print(tile)

        layout = {'layoutCols': 2, 'layoutRows': 2, 'tileCols': 4, 'tileRows': 4}
        extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 8.0, 'ymax': 8.0}

        rdd = SparkContext.getOrCreate().parallelize(tiles)
        print(rdd.count())

        metadata = {'cellType': 'float64ud-1',
                    'extent': extent,
                    'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                    'bounds': {
                        'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(self.now)},
                        'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(self.now)}
                    },
                    'layoutDefinition': {
                        'extent': extent,
                        'tileLayout': layout
                    }
                    }

        return TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)

    def test_another_polygon_series(self):
        input = self._create_spacetime_layer(no_data=-1.0)
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
        polygon = Polygon(shell=[(2.0, 6.0), (6.0, 6.0), (6.0, 2.0), (2.0, 2.0), (2.0, 6.0)])
        means = imagecollection.zonal_statistics(regions=polygon, func="mean")
        assert isinstance(means, AggregatePolygonResultCSV)
        assert means.get_data() == {
            "2017-09-25T11:37:00Z": [
                [(0 + 0 + 0 + 0 + 1 + 1 + 1 + 1 + 2 + 2 + 2 + 2) / 12]
            ]
        }


@pytest.mark.parametrize("udf_code", [
    """
    from openeo_udf.api.datacube import DataCube  # Old style openeo_udf API
    def apply_datacube(cube: DataCube, context: dict) -> DataCube:
        red = cube.array.sel(bands='red')
        nir = cube.array.sel(bands='nir')
        ndvi = (nir - red) / (nir + red)
        ndvi.name = "NDVI"
        return DataCube(array=ndvi)
    """,
    """
    from openeo.udf import XarrayDataCube
    def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
        red = cube.array.sel(bands='red')
        nir = cube.array.sel(bands='nir')
        ndvi = (nir - red) / (nir + red)
        ndvi.name = "NDVI"
        return XarrayDataCube(array=ndvi)
    """
])
def test_point_series_apply_tile(imagecollection_with_two_bands_and_one_date, udf_code):
    udf_code = textwrap.dedent(udf_code)

    reducer = GeoPySparkBackendImplementation(use_job_registry=False).visit_process_graph({
        "udf_process": {
            "process_id": "run_udf",
            "arguments": {
                "data": {
                    "from_parameter": "data"
                },
                "udf": udf_code
            },
            "result": True
        },
    })

    env = EvalEnv()
    transformed_collection = imagecollection_with_two_bands_and_one_date.reduce_dimension(
        dimension="bands", reducer=reducer, env=env
    )

    for p in TestCustomFunctions.points[0:3]:
        result = transformed_collection.timeseries(p.x, p.y)
        print(result)
        value = result.popitem()
        print(value)
        # self.assertEqual(3.0,value[1][0])
