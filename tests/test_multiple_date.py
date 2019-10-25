import datetime
from unittest import TestCase

import numpy as np
from geopyspark import CellType
from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time, TemporalProjectedExtent, Extent,
                                   RasterLayer)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer, Pyramid
from numpy.testing import assert_array_almost_equal
from pyspark import SparkContext
from shapely.geometry import Point

from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from openeogeotrellis.numpy_aggregators import max_composite
from openeogeotrellis.service_registry import InMemoryServiceRegistry


class TestMultipleDates(TestCase):
    band1 = np.array([
        [-1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0]])

    band2 = np.array([
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, -1.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0]])

    tile = Tile.from_numpy_array(band1,no_data_value=-1.0)
    tile2 = Tile.from_numpy_array(band2,no_data_value=-1.0)
    time_1 = datetime.datetime.strptime("2016-08-24T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
    time_2 = datetime.datetime.strptime("2017-08-24T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
    time_3 = datetime.datetime.strptime("2017-10-17T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')

    layer = [(SpaceTimeKey(0, 0, time_1), tile),
             (SpaceTimeKey(1, 0, time_1), tile2),
             (SpaceTimeKey(0, 1, time_1), tile),
             (SpaceTimeKey(1, 1, time_1), tile),
             (SpaceTimeKey(0, 0, time_2), tile2),
             (SpaceTimeKey(1, 0, time_2), tile2),
             (SpaceTimeKey(0, 1, time_2), tile2),
             (SpaceTimeKey(1, 1, time_2), tile2),
             (SpaceTimeKey(0, 0, time_3), tile),
             (SpaceTimeKey(1, 0, time_3), tile2),
             (SpaceTimeKey(0, 1, time_3), tile),
             (SpaceTimeKey(1, 1, time_3), tile)
             ]

    rdd = SparkContext.getOrCreate().parallelize(layer)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 33.0, 'ymax': 33.0}
    layout = {'layoutCols': 2, 'layoutRows': 2, 'tileCols': 5, 'tileRows': 5}
    metadata = {'cellType': 'float32ud-1.0',
                'extent': extent,
                'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                'bounds': {
                    'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(time_1)},
                    'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(time_3)}},
                'layoutDefinition': {
                    'extent': extent,
                    'tileLayout': {'tileCols': 5, 'tileRows': 5, 'layoutCols': 2, 'layoutRows': 2}}}

    tiled_raster_rdd = TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)

    layer2 = [(TemporalProjectedExtent(Extent(0, 0, 1, 1), epsg=3857, instant=time_1), tile),
              (TemporalProjectedExtent(Extent(1, 0, 2, 1), epsg=3857, instant=time_1), tile),
              (TemporalProjectedExtent(Extent(0, 1, 1, 2), epsg=3857, instant=time_1), tile),
              (TemporalProjectedExtent(Extent(1, 1, 2, 2), epsg=3857, instant=time_1), tile),
              (TemporalProjectedExtent(Extent(1, 0, 2, 1), epsg=3857, instant=time_2), tile),
              (TemporalProjectedExtent(Extent(1, 0, 2, 1), epsg=3857, instant=time_2), tile),
              (TemporalProjectedExtent(Extent(0, 1, 1, 2), epsg=3857, instant=time_2), tile),
              (TemporalProjectedExtent(Extent(1, 1, 2, 2), epsg=3857, instant=time_2), tile),
              (TemporalProjectedExtent(Extent(1, 0, 2, 1), epsg=3857, instant=time_3), tile),
              (TemporalProjectedExtent(Extent(1, 0, 2, 1), epsg=3857, instant=time_3), tile),
              (TemporalProjectedExtent(Extent(0, 1, 1, 2), epsg=3857, instant=time_3), tile),
              (TemporalProjectedExtent(Extent(1, 1, 2, 2), epsg=3857, instant=time_3), tile)]

    rdd2 = SparkContext.getOrCreate().parallelize(layer2)
    raster_rdd = RasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd2)

    points = [
        Point(1.0, -3.0),
        Point(0.5, 0.5),
        Point(20.0, 3.0),
        Point(1.0, -2.0),
        Point(-10.0, 15.0)
    ]

    def test_reduce(self):
        input = Pyramid({0: self.tiled_raster_rdd})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())

        stitched = imagecollection.reduce("max","temporal").pyramid.levels[0].stitch()
        print(stitched)
        self.assertEqual(2.0, stitched.cells[0][0][1])
        self.assertEqual(2.0, stitched.cells[0][0][0])


    def test_reduce_nontemporal(self):
        input = Pyramid({0: self.tiled_raster_rdd})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())
        with self.assertRaises(AttributeError) as context:
            imagecollection.reduce("max","spectral").pyramid.levels[0].stitch()
        print(context.exception)

    def test_aggregate_temporal(self):
        input = Pyramid({0: self.tiled_raster_rdd})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())
        stitched = imagecollection.aggregate_temporal(["2017-01-01","2018-01-01"],["2017-01-03"],"max").pyramid.levels[0].to_spatial_layer().stitch()
        print(stitched)

    def test_max_aggregator(self):
        tiles = [self.tile,self.tile2]
        composite = max_composite(tiles)
        self.assertEqual(2.0, composite.cells[0][0])

    def test_aggregate_max_time(self):

        input = Pyramid( {0:self.tiled_raster_rdd })

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())

        stitched = imagecollection.reduce('max','temporal').pyramid.levels[0].stitch()
        print(stitched)
        self.assertEqual(2.0, stitched.cells[0][0][0])

    def test_min_time(self):
        input = Pyramid( {0:self.tiled_raster_rdd })

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())
        min_time = imagecollection.reduce('min','temporal')
        max_time = imagecollection.reduce('max','temporal')

        stitched = min_time.pyramid.levels[0].stitch()
        print(stitched)

        self.assertEquals(2.0,stitched.cells[0][0][0])

        for p in self.points[1:3]:
            result = min_time.timeseries(p.x, p.y,srs="EPSG:3857")
            print(result)
            print(imagecollection.timeseries(p.x,p.y,srs="EPSG:3857"))
            max_result = max_time.timeseries(p.x, p.y,srs="EPSG:3857")
            self.assertEqual(1.0,result['NoDate'])
            self.assertEqual(2.0,max_result['NoDate'])



    def test_apply_spatiotemporal(self):
        import openeo_udf.functions

        input = Pyramid({0: self.tiled_raster_rdd})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry(), {
            "bands": [
                {
                    "band_id": "2",
                    "name": "blue",
                    "wavelength_nm": 496.6,
                    "res_m": 10,
                    "scale": 0.0001,
                    "offset": 0,
                    "type": "int16",
                    "unit": "1"
                }]
        })
        import os, openeo_udf
        dir = os.path.dirname(openeo_udf.functions.__file__)
        file_name = os.path.join(dir, "raster_collections_reduce_time_sum.py")
        with open(file_name, "r")  as f:
            udf_code = f.read()

        result = imagecollection.apply_tiles_spatiotemporal(udf_code)
        stitched = result.pyramid.levels[0].to_spatial_layer().stitch()
        print(stitched)
        self.assertTrue(np.isnan(stitched.cells[0][0][0]))
        self.assertEqual(6, stitched.cells[0][0][5])
        self.assertEqual(4, stitched.cells[0][5][6])

    def test_apply_dimension_spatiotemporal(self):

        input = Pyramid({0: self.tiled_raster_rdd})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry(), {
            "bands": [
                {
                    "band_id": "2",
                    "name": "blue",
                    "wavelength_nm": 496.6,
                    "res_m": 10,
                    "scale": 0.0001,
                    "offset": 0,
                    "type": "int16",
                    "unit": "1"
                }]
        })


        udf_code = """
def rct_savitzky_golay(udf_data):
    from scipy.signal import savgol_filter

    # Iterate over each tile
    for tile in udf_data.raster_collection_tiles:
        timeseries_array = tile.data
        
        tile.set_data(timeseries_array)


# This function call is the entry point for the UDF.
# The caller will provide all required data in the **data** object.
rct_savitzky_golay(data)
        
        
        """


        result = imagecollection.apply_tiles_spatiotemporal(udf_code)
        local_tiles = result.pyramid.levels[0].to_numpy_rdd().collect()
        print(local_tiles)
        self.assertEquals(len(TestMultipleDates.layer),len(local_tiles))
        ref_dict = {e[0]:e[1] for e in imagecollection.pyramid.levels[0].convert_data_type(CellType.FLOAT64).to_numpy_rdd().collect()}
        result_dict = {e[0]: e[1] for e in local_tiles}
        for k,v in ref_dict.items():
            tile = result_dict[k]
            assert_array_almost_equal(np.squeeze(v.cells),np.squeeze(tile.cells),decimal=2)


    def test_mask_raster(self):
        input = Pyramid({0: self.tiled_raster_rdd})
        def createMask(tile):
            tile.cells[0][0][0] = 0.0
            return tile
        mask_layer = self.tiled_raster_rdd.map_tiles(createMask)
        mask = Pyramid({0: mask_layer})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())
        stitched = imagecollection.mask(rastermask=GeotrellisTimeSeriesImageCollection(mask, InMemoryServiceRegistry()),
                                        replacement=10.0).reduce('max','temporal').pyramid.levels[0].stitch()
        print(stitched)
        self.assertEquals(2.0,stitched.cells[0][0][0])
        self.assertEquals(10.0, stitched.cells[0][0][1])

    def test_apply_kernel(self):
        kernel = np.array([[0.0,1.0,0.0],[1.0,1.0,1.0],[0.0,1.0,0.0]])

        input = Pyramid({0: self.tiled_raster_rdd})
        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())
        stitched = imagecollection.apply_kernel(kernel,2.0).reduce('max','temporal').pyramid.levels[0].stitch()

        self.assertEquals(12.0, stitched.cells[0][0][0])
        self.assertEquals(16.0, stitched.cells[0][0][1])
        self.assertEquals(20.0, stitched.cells[0][1][1])


    def test_resample_spatial(self):


        input = Pyramid({0: self.tiled_raster_rdd})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())

        resampled = imagecollection.resample_spatial(resolution=0.05)

        resampled.max_time().download("resampled.tiff",format="GTIFF",parameters={'tiled':True})

        import rasterio
        with rasterio.open("resampled.tiff") as ds:
            print(ds.profile)
            self.assertAlmostEqual(0.05,ds.res[0],3)

