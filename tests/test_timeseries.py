import datetime
from unittest import TestCase

from tempfile import NamedTemporaryFile
import json
from shapely.geometry import mapping
import geopyspark as gps
import numpy as np
import pytz
from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from pyspark import SparkContext
from shapely.geometry import Point
from shapely.geometry import Polygon, GeometryCollection, MultiPolygon

from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from openeogeotrellis.service_registry import InMemoryServiceRegistry


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

        return TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)

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

    def test_zonal_statistics(self):
        layer = self.create_spacetime_layer()
        imagecollection = GeotrellisTimeSeriesImageCollection(gps.Pyramid({0: layer}), InMemoryServiceRegistry())

        polygon = Polygon(shell=[
            (0.0, 0.0),
            (1.0, 0.0),
            (1.0, 1.0),
            (0.0, 1.0),
            (0.0, 0.0)
        ])
        result = imagecollection.zonal_statistics(polygon, "mean")
        assert result.data == {'2017-09-25T11:37:00': [[1.0, 2.0]]}

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


    def test_zonal_statistics_datacube(self):
        layer = self.create_spacetime_layer()
        imagecollection = GeotrellisTimeSeriesImageCollection(gps.Pyramid({0: layer}), InMemoryServiceRegistry())

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

        for use_file in [True,False]:
            with self.subTest():
                if use_file:
                    with NamedTemporaryFile(delete=False,suffix='.json',mode='r+') as fp:
                        json.dump(mapping(regions),fp)
                        regions_serialized = fp.name
                else:
                    regions_serialized = regions

                result = imagecollection.zonal_statistics(regions_serialized, "mean")
                assert result.data == {
                    '2017-09-25T11:37:00Z': [[1.0, 2.0], [1.0, 2.0]]
                }
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

    def test_zonal_statistics_median_datacube(self):
        layer = self.create_spacetime_layer()
        imagecollection = GeotrellisTimeSeriesImageCollection(gps.Pyramid({0: layer}), InMemoryServiceRegistry())
        polygon = Polygon(shell=[
            (0.0, 0.0),
            (1.0, 0.0),
            (1.0, 1.0),
            (0.0, 1.0),
            (0.0, 0.0)
        ])
        result = imagecollection.zonal_statistics(polygon, "median")
        assert result.data == {'2017-09-25T11:37:00Z': [[1.0, 2.0]]}

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
            }
        }

    def test_zonal_statistics_for_unsigned_byte_layer(self):
        layer = self.create_spacetime_unsigned_byte_layer()
        # layer.to_spatial_layer().save_stitched('/tmp/unsigned_byte_layer.tif')
        imagecollection = GeotrellisTimeSeriesImageCollection(gps.Pyramid({0: layer}), InMemoryServiceRegistry())
        polygon = Polygon(shell=[
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 4.0),
            (0.0, 4.0),
            (0.0, 0.0)
        ])
        result = imagecollection.zonal_statistics(polygon, "mean")
        # FIXME: the Python implementation doesn't return a time zone (Z)
        assert result.data == {'2017-09-25T11:37:00': [[220.0]]}

        covjson = result.to_covjson()
        assert covjson["ranges"] == {
            "band0": {
                "type": "NdArray", "dataType": "float", "axisNames": ["t", "composite"],
                "shape": (1, 1),
                "values": [220.0]
            }
        }
