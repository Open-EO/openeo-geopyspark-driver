
from .base_test_class import BaseTestClass
from unittest import TestCase
from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
import numpy as np
from geopyspark import Tile


class TestMultiBandUDF(BaseTestClass):


    band_1 = np.array([
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0]])

    band_2 = np.array([
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0]])

    band_3 = np.array([
        [3.0, 3.0, 3.0, 3.0, 3.0],
        [3.0, 3.0, 3.0, 3.0, 3.0],
        [3.0, 3.0, 3.0, 3.0, 3.0],
        [3.0, 3.0, 3.0, 3.0, 3.0],
        [3.0, 3.0, 3.0, 3.0, 3.0]])

    bands = np.array([band_1, band_2, band_3])
    tile = Tile.from_numpy_array(bands,np.nan)

    def test_convert_multiband_tile(self):
        imagecollection = GeotrellisTimeSeriesImageCollection("test")
        imagecollection.metadata = {
            'bands': [
                {
                    'band_id': '2',
                    'name': 'blue',
                    'wavelength_nm': 496.6,
                    'res_m': 10,
                    'scale': 0.0001,
                    'offset': 0,
                    'type': 'int16',
                    'unit': '1'
                },
                {'band_id': '3', 'name': 'green', 'wavelength_nm': 560, 'res_m': 10, 'scale': 0.0001, 'offset': 0,
                 'type': 'int16', 'unit': '1'},
                {'band_id': '4', 'name': 'red', 'wavelength_nm': 664.5, 'res_m': 10, 'scale': 0.0001, 'offset': 0,
                 'type': 'int16', 'unit': '1'}
            ]

        }

        rastercollectiontiles = GeotrellisTimeSeriesImageCollection._tile_to_rastercollectiontile(TestMultiBandUDF.tile, None,imagecollection.metadata['bands'])
        self.assertEqual(3,len(rastercollectiontiles))
        self.assertEqual('blue',rastercollectiontiles[0].id)
        self.assertEqual(496.6, rastercollectiontiles[0].wavelength)

