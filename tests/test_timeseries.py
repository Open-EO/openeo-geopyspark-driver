import datetime
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import TestCase

import geopyspark as gps
import numpy as np
import pytest
import pytz
from geopyspark.geotrellis import SpaceTimeKey, Tile, _convert_to_unix_time
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.save_result import AggregatePolygonResult, AggregatePolygonResultCSV
from pyspark import SparkContext
from shapely.geometry import (
    GeometryCollection,
    MultiPolygon,
    Point,
    Polygon,
    box,
    mapping,
)

from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube

from .data import get_test_data_file


class TestTimeSeries(TestCase):

    first = np.zeros((1, 4, 4))
    first.fill(1)

    second = np.zeros((1, 4, 4))
    second.fill(2)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 4.0, 'ymax': 4.0}
    layout = {'layoutCols': 2, 'layoutRows': 2, 'tileCols': 4, 'tileRows': 4}

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

    expected_spacetime_points_list = [
        (Point(1.0, -3.0), [(None, None)]),
        (Point(2.0, 4.0), [(now, [1, 2])]),
        (Point(3.0, 3.0), [(now, [1, 2])]),
        (Point(1.0, -2.0), [(None, None)]),
        (Point(-10.0, 15.0), [(None, None)])
    ]

    def create_spacetime_layer(self):
        cells = np.array([self.first, self.second], dtype='int')
        tile = Tile.from_numpy_array(cells, -1)

        layer = [(SpaceTimeKey(0, 0, self.now), tile),
                 (SpaceTimeKey(1, 0, self.now), tile),
                 (SpaceTimeKey(0, 1, self.now), tile),
                 (SpaceTimeKey(1, 1, self.now), tile)]

        rdd = SparkContext.getOrCreate().parallelize(layer)

        metadata = {'cellType': 'int32ud-1',
                    'extent': self.extent,
                    'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                    'bounds': {
                        'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(self.now)},
                        'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(self.now)}
                    },
                    'layoutDefinition': {
                        'extent': self.extent,
                        'tileLayout': self.layout
                    }
                    }

        return TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata).convert_data_type('int32',no_data_value=-1)

    def create_spacetime_unsigned_byte_layer(self):
        """
        Returns a single-band uint8ud255 layer consisting of four tiles that each look like this:

         ND 220 220 220
        220 220 220 220
        220 220 220 220
        220 220 220 220

        The extent is (0.0, 0.0) to (4.0, 4.0).
        """
        no_data = 255

        single_band = np.zeros((1, 4, 4))
        single_band.fill(220)
        single_band[0, 0, 0] = no_data

        cells = np.array([single_band], dtype='uint8')
        tile = Tile.from_numpy_array(cells, no_data)

        layer = [(SpaceTimeKey(0, 0, self.now), tile),
                 (SpaceTimeKey(1, 0, self.now), tile),
                 (SpaceTimeKey(0, 1, self.now), tile),
                 (SpaceTimeKey(1, 1, self.now), tile)]

        rdd = SparkContext.getOrCreate().parallelize(layer)

        metadata = {
            'cellType': 'uint8ud255',
            'extent': self.extent,
            'crs': '+proj=longlat +datum=WGS84 +no_defs ',
            'bounds': {
                'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(self.now)},
                'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(self.now)}
            },
            'layoutDefinition': {
                'extent': self.extent,
                'tileLayout': self.layout
            }
        }

        return TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)

    def test_point_series(self):
        result = self.create_spacetime_layer().get_point_values(self.points)

        self.assertEqual(len(result), len(self.expected_spacetime_points_list))

        for r in result:
            self.assertTrue(r in self.expected_spacetime_points_list)




    def test_zonal_statistics_for_unsigned_byte_layer(self):
        layer = self.create_spacetime_unsigned_byte_layer()
        # layer.to_spatial_layer().save_stitched('/tmp/unsigned_byte_layer.tif')
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer}))
        polygon = Polygon(shell=[
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 4.0),
            (0.0, 4.0),
            (0.0, 0.0)
        ])
        result = imagecollection.zonal_statistics(polygon, "mean")
        # FIXME: the Python implementation doesn't return a time zone (Z)
        assert isinstance(result, AggregatePolygonResultCSV)
        assert result.get_data() == {"2017-09-25T11:37:00Z": [[220.0]]}

        covjson = result.to_covjson()
        assert covjson["ranges"] == {
            "band0": {
                "type": "NdArray", "dataType": "float", "axisNames": ["t", "composite"],
                "shape": (1, 1),
                "values": [220.0]
            }
        }

def test_zonal_statistics(imagecollection_with_two_bands_and_one_date):

    polygon = Polygon(shell=[
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
        (0.0, 0.0)
    ])
    result = imagecollection_with_two_bands_and_one_date.zonal_statistics(polygon, "mean")
    assert isinstance(result, AggregatePolygonResultCSV)
    assert result.get_data() == {"2017-09-25T11:37:00Z": [[1.0, 2.0]]}

    covjson = result.to_covjson()
    assert covjson["ranges"] == {
        "band0": {
            "type": "NdArray", "dataType": "float", "axisNames": ["t", "composite"],
            "shape": (1, 1),
            "values": [1.0]
        },
        "band1": {
            "type": "NdArray", "dataType": "float", "axisNames": ["t", "composite"],
            "shape": (1, 1),
            "values": [2.0]
        },
    }


def get_near_and_far_polygon_geometry_collection():
    polygon1 = Polygon(shell=[
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
        (0.0, 0.0)
    ])

    # This polygon falls out of the range of 'imagecollection_with_two_bands_and_one_date'
    polygon2 = Polygon(shell=[
        (990.0, 990.0),
        (991.0, 990.0),
        (991.0, 991.0),
        (990.0, 991.0),
        (990.0, 990.0)
    ])
    return GeometryCollection([polygon1, MultiPolygon([polygon2])])


@pytest.mark.parametrize(["get_regions"], [
    [get_near_and_far_polygon_geometry_collection],
    [lambda: DelayedVector(str(get_test_data_file("geometries/near_and_far_polygon.geojson")))],
    [lambda: DriverVectorCube.from_fiona([str(get_test_data_file("geometries/near_and_far_polygon.geojson"))], None, {})],
])
def test_aggregate_spatial_last_polygon_empty(get_regions, imagecollection_with_two_bands_and_one_date):
    regions = get_regions()

    result = imagecollection_with_two_bands_and_one_date.aggregate_spatial(regions, {
        "mean1": {
            "process_id": "mean",
            "arguments": {
                "data": {
                    "from_parameter": "data"
                }
            },
            "result": True
        }
    })
    assert isinstance(result, AggregatePolygonResultCSV)
    assert result.get_data() == {
        "2017-09-25T11:37:00Z": [[1.0, 2.0], [pytest.approx(np.nan, nan_ok=True), pytest.approx(np.nan, nan_ok=True)]]
    }


def test_aggregate_spatial_small_polygon(imagecollection_with_two_bands_and_one_date):
    regions = DriverVectorCube.from_fiona([str(get_test_data_file("geometries/small_polygon.geojson"))], None, {})

    result = imagecollection_with_two_bands_and_one_date.aggregate_spatial(
        regions, {"mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}}
    )
    assert isinstance(result, AggregatePolygonResultCSV)

    spatial_result = AggregatePolygonResult(
        result.get_data(), regions=regions, metadata=imagecollection_with_two_bands_and_one_date.metadata
    )
    filename = spatial_result.to_geoparquet()
    assert Path(filename).exists()


def test_zonal_statistics_median_datacube(imagecollection_with_two_bands_and_three_dates):

    polygon = Polygon(shell=[
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
        (0.0, 0.0)
    ])

    result = imagecollection_with_two_bands_and_three_dates.zonal_statistics(polygon, "median")
    assert isinstance(result, AggregatePolygonResultCSV)
    assert result.get_data() == {
        "2017-09-25T11:37:00Z": [[1.0, 2.0]],
        "2017-09-30T00:37:00Z": [
            [pytest.approx(np.nan, nan_ok=True), pytest.approx(np.nan, nan_ok=True)]
        ],
        "2017-10-25T11:37:00Z": [[2.0, 1.0]],
    }

    covjson = result.to_covjson()
    assert covjson["ranges"] == {
        "band0": {
            "type": "NdArray", "dataType": "float", "axisNames": ["t", "composite"],
            "shape": (3, 1),
            "values": [1.0, pytest.approx(np.nan,nan_ok=True), 2.0]
        },
        "band1": {
            "type": "NdArray", "dataType": "float", "axisNames": ["t", "composite"],
            "shape": (3, 1),
            "values": [2.0, pytest.approx(np.nan,nan_ok=True), 1.0]
        }
    }


def test_zonal_statistics_with_empty_mutliband_tiles(imagecollection_with_two_bands_and_three_dates):
    polygon = Polygon(shell=[
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
        (0.0, 0.0)
    ])
    # Aggregate_temporal for period 2015-01-01 to 2015-02-02 will introduce a tile with no data.
    # The following aggregate_spatial call should be able to handle such tiles.
    from openeo.processes import median
    builder = median({"from_parameter": "data"})
    aggregate_temporal = imagecollection_with_two_bands_and_three_dates.aggregate_temporal(
        [["2015-01-01", "2015-02-02"], ["2017-09-25", "2017-09-26"],
         ["2017-09-30", "2017-10-01"], ["2017-10-25", "2017-10-26"]],
        ["2015-01-01", "2017-09-25", "2017-09-30", "2017-10-25"],
        builder.flat_graph(), dimension = "t"
    )
    result = aggregate_temporal.zonal_statistics(polygon, "median")
    assert isinstance(result, AggregatePolygonResultCSV)
    assert result.get_data() == {
        "2015-01-01T00:00:00Z": [
            [pytest.approx(np.nan, nan_ok = True), pytest.approx(np.nan, nan_ok = True)]
        ],
        "2017-09-25T00:00:00Z": [[1.0, 2.0]],
        "2017-09-30T00:00:00Z": [
            [pytest.approx(np.nan, nan_ok=True), pytest.approx(np.nan, nan_ok=True)]
        ],
        "2017-10-25T00:00:00Z": [[2.0, 1.0]],
    }


@pytest.mark.parametrize(["use_file"], [
    [True],
    [False],
])
def test_zonal_statistics_datacube(use_file, imagecollection_with_two_bands_and_one_date):
    imagecollection = imagecollection_with_two_bands_and_one_date

    polygon = Polygon(shell=[
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
        (0.0, 0.0)
    ])

    polygon2 = Polygon(shell=[
        (2.0, 2.0),
        (3.0, 2.0),
        (3.0, 3.0),
        (2.0, 3.0),
        (2.0, 2.0)
    ])

    regions = GeometryCollection([polygon, MultiPolygon([polygon2])])


    if use_file:
        with NamedTemporaryFile(delete=False,suffix='.json',mode='r+') as fp:
            json.dump(mapping(regions),fp)
            regions_serialized = fp.name
    else:
        regions_serialized = regions

    result = imagecollection.zonal_statistics(regions_serialized, "mean")
    assert isinstance(result, AggregatePolygonResultCSV)
    assert result.get_data() == {"2017-09-25T11:37:00Z": [[1.0, 2.0], [1.0, 2.0]]}
    result._regions = regions

    covjson = result.to_covjson()
    assert covjson["ranges"] == {
        "band0": {
            "type": "NdArray", "dataType": "float", "axisNames": ["t", "composite"],
            "shape": (1, 2),
            "values": [1.0, 1.0]
        },
        "band1": {
            "type": "NdArray", "dataType": "float", "axisNames": ["t", "composite"],
            "shape": (1, 2),
            "values": [2.0, 2.0]
        },
    }

def test_multiple_zonal_statistics(imagecollection_with_two_bands_and_three_dates):

    polygon = Polygon(shell=[
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
        (0.0, 0.0)
    ])

    callback = {
        "sum": {
            "process_id": "sum",
            "arguments": {
                "data": {"from_parameter": "data"}
            }
        },
        "count": {
            "process_id": "count",
            "arguments": {
                "data": {"from_parameter": "data"}
            }
        },
        "max": {
            "process_id": "max",
            "arguments": {
                "data": {"from_parameter": "data"}
            }
        },
        "array":{
            "process_id": "create_array",
            "arguments": {"data": [{"from_node": "sum"}, {"from_node": "count"}, {"from_node": "max"}]},
            "result": True
        }
    }

    result = imagecollection_with_two_bands_and_three_dates.aggregate_spatial(polygon, callback)
    assert isinstance(result, AggregatePolygonResultCSV)
    assert result.get_data() == {
        "2017-09-25T11:37:00Z": [[16.0, 16.0, 1.0, 32.0, 16.0, 2.0]],
        "2017-09-30T00:37:00Z": [
            [
                pytest.approx(np.nan, nan_ok=True),
                0.0,
                pytest.approx(np.nan, nan_ok=True),
                pytest.approx(np.nan, nan_ok=True),
                0.0,
                pytest.approx(np.nan, nan_ok=True),
            ]
        ],
        "2017-10-25T11:37:00Z": [[32.0, 16.0, 2.0, 16.0, 16.0, 1.0]],
    }


def _build_cube():
    openeo_metadata = {
        "cube:dimensions": {
            "x": {"type": "spatial", "axis": "x"},
            "y": {"type": "spatial", "axis": "y"},
            "bands": {"type": "bands", "values": ["red", "nir"]},
            "t": {"type": "temporal"}
        },
        "bands": [

            {
                "band_id": "red",
                "name": "red",
                "offset": 0,
                "res_m": 10,
                "scale": 0.0001,
                "type": "int16",
                "unit": "1",
                "wavelength_nm": 664.5
            },
            {
                "band_id": "nir",
                "name": "nir",
                "offset": 0,
                "res_m": 10,
                "scale": 0.0001,
                "type": "int16",
                "unit": "1",
                "wavelength_nm": 835.1
            }
        ],
        "description": "Sentinel 2 Level-2: Bottom-of-atmosphere reflectances in cartographic geometry",
        "extent": {
            "bottom": 39,
            "crs": "EPSG:4326",
            "left": -34,
            "right": 35,
            "top": 71
        },
        "product_id": "CGS_SENTINEL2_RADIOMETRY_V101",
        "time": {
            "from": "2016-01-01",
            "to": "2019-10-01"
        }
    }

    # TODO: avoid instantiating TestTimeSeries? e.g. use pytest fixtures or simple builder functions.
    layer = TestTimeSeries().create_spacetime_layer()
    cube = GeopysparkDataCube(pyramid=gps.Pyramid({0: layer}),metadata=openeo_metadata)
    return cube


@pytest.mark.parametrize(["func", "expected"], [
    ("mean", {'2017-09-25T11:37:00Z': [[1, 2]]}),
    ("median", {'2017-09-25T11:37:00Z': [[1, 2]]}),
    ("histogram", {'2017-09-25T11:37:00Z': [[{1.0: 4}, {2.0: 4}]]}),
    ("sd", {'2017-09-25T11:37:00Z': [[0, 0]]})
])
def test_zonal_statistics_single_polygon(func, expected):
    cube = _build_cube()
    polygon = box(0.0, 0.0, 1.0, 1.0)
    result = cube.zonal_statistics(polygon, func=func)
    assert isinstance(result, AggregatePolygonResult)
    assert result.get_data() == expected


@pytest.mark.parametrize(["func", "expected"], [
    ("mean", {'2017-09-25T11:37:00Z': [[1.0, 2.0], [1.0, 2.0]]}),
    ("median", {'2017-09-25T11:37:00Z': [[1.0, 2.0], [1.0, 2.0]]}),
    ("histogram", {'2017-09-25T11:37:00Z': [[{1.0: 4}, {2.0: 4}], [{1.0: 23}, {2.0: 23}]]}),
    ("sd", {'2017-09-25T11:37:00Z': [[0.0, 0.0], [0.0, 0.0]]})
])
def test_zonal_statistics_geometry_collection(func, expected):
    cube = _build_cube()
    geometry = GeometryCollection([
        box(0.5, 0.5, 1.5, 1.5),
        MultiPolygon([box(2.0, 0.5, 4.0, 1.5), box(1.5, 2, 4.0, 3.5)])
    ])
    result = cube.zonal_statistics(geometry, func=func)
    assert isinstance(result, AggregatePolygonResult)
    assert result.get_data() == expected


@pytest.mark.parametrize(["func", "expected"], [
    ("mean", {'2017-09-25T11:37:00Z': [[1.0, 2.0], [1.0, 2.0]]}),
    ("median", {'2017-09-25T11:37:00Z': [[1.0, 2.0], [1.0, 2.0]]}),
    ("histogram", {'2017-09-25T11:37:00Z': [[{1.0: 4}, {2.0: 4}], [{1.0: 19}, {2.0: 19}]]}),
    ("sd", {'2017-09-25T11:37:00Z': [[0.0, 0.0], [0.0, 0.0]]})
])
def test_zonal_statistics_shapefile(func, expected):
    cube = _build_cube()
    shapefile = str(get_test_data_file("geometries/polygons01.shp"))
    result = cube.zonal_statistics(regions=shapefile, func=func)
    assert isinstance(result, AggregatePolygonResult)
    assert result.get_data() == expected


@pytest.mark.parametrize(["func", "expected"], [
    ("mean", {'2017-09-25T11:37:00Z': [[1.0, 2.0], [1.0, 2.0]]}),
    ("median", {'2017-09-25T11:37:00Z': [[1.0, 2.0], [1.0, 2.0]]}),
    ("histogram", {'2017-09-25T11:37:00Z': [[{1.0: 4}, {2.0: 4}], [{1.0: 19}, {2.0: 19}]]}),
    ("sd", {'2017-09-25T11:37:00Z': [[0.0, 0.0], [0.0, 0.0]]})
])
def test_zonal_statistics_geojson(func, expected):
    cube = _build_cube()
    shapefile = str(get_test_data_file("geometries/polygons01.geojson"))
    result = cube.zonal_statistics(regions=shapefile, func=func)
    assert isinstance(result, AggregatePolygonResult)
    assert result.get_data() == expected
