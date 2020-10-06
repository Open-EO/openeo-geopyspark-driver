from unittest import TestCase

import numpy as np
from geopyspark import Tile

from openeo.metadata import CollectionMetadata
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.service_registry import InMemoryServiceRegistry


class TestMultiBandUDF(TestCase):


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

    def test_convert_multiband_tile_hypercube(self):
        metadata = CollectionMetadata({
            "cube:dimensions": {
                # TODO: also specify other dimensions?
                "bands": {"type": "bands", "values": ["2", "3", "4"]}
            },
            "summaries": {"eo:bands": [
                {
                    'name': '2',
                    'common_name': 'blue',
                    'wavelength_nm': 496.6,
                    'res_m': 10,
                    'scale': 0.0001,
                    'offset': 0,
                    'type': 'int16',
                    'unit': '1'
                },
                {'name': '3', 'common_name': 'green', 'wavelength_nm': 560, 'res_m': 10, 'scale': 0.0001, 'offset': 0,
                 'type': 'int16', 'unit': '1'},
                {'name': '4', 'common_name': 'red', 'wavelength_nm': 664.5, 'res_m': 10, 'scale': 0.0001, 'offset': 0,
                 'type': 'int16', 'unit': '1'}
            ]
            }})
        imagecollection = GeopysparkDataCube("test", InMemoryServiceRegistry(), metadata=metadata)
        datacube = GeopysparkDataCube._tile_to_datacube(
            TestMultiBandUDF.tile.cells,
            None,
            band_dimension=metadata.band_dimension
        )
        the_array = datacube.get_array()
        assert the_array is not None
        print(the_array)
