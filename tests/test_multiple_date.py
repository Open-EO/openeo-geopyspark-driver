import datetime
from pathlib import Path
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

    def setUp(self):
        # TODO: make this reusable (or a pytest fixture)
        self.temp_folder = Path.cwd() / 'tmp'
        if not self.temp_folder.exists():
            self.temp_folder.mkdir()
        assert self.temp_folder.is_dir()

    def test_reproject_spatial(self):
        input = Pyramid({0: self.tiled_raster_rdd})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())

        resampled = imagecollection.resample_spatial(resolution=0,projection="EPSG:3857",method="max")
        metadata = resampled.pyramid.levels[0].layer_metadata
        print(metadata)
        self.assertTrue("proj=merc" in metadata.crs)
        path = str(self.temp_folder / "reprojected.tiff")
        resampled.reduce('max', 'temporal').download(path, format="GTIFF", parameters={'tiled': True})

        import rasterio
        with rasterio.open(path) as ds:
            print(ds.profile)

    def test_reduce(self):
        input = Pyramid({0: self.tiled_raster_rdd})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())

        stitched = imagecollection.reduce("max", "temporal").pyramid.levels[0].stitch()
        print(stitched)
        self.assertEqual(2.0, stitched.cells[0][0][0])
        self.assertEqual(2.0, stitched.cells[0][0][1])

        stitched = imagecollection.reduce("min", "temporal").pyramid.levels[0].stitch()
        print(stitched)
        self.assertEqual(2.0, stitched.cells[0][0][0])
        self.assertEqual(1.0, stitched.cells[0][0][1])

        stitched = imagecollection.reduce("sum", "temporal").pyramid.levels[0].stitch()
        print(stitched)
        self.assertEqual(2.0, stitched.cells[0][0][0])
        self.assertEqual(4.0, stitched.cells[0][0][1])

        stitched = imagecollection.reduce("mean", "temporal").pyramid.levels[0].stitch()
        print(stitched)
        self.assertEqual(2.0, stitched.cells[0][0][0])
        self.assertAlmostEqual(1.3333333, stitched.cells[0][0][1])

        stitched = imagecollection.reduce("variance", "temporal").pyramid.levels[0].stitch()
        print(stitched)
        self.assertEqual(0.0, stitched.cells[0][0][0])
        self.assertAlmostEqual(0.2222222, stitched.cells[0][0][1])

        stitched = imagecollection.reduce("sd", "temporal").pyramid.levels[0].stitch()
        print(stitched)
        self.assertEqual(0.0, stitched.cells[0][0][0])
        self.assertAlmostEqual(0.4714045, stitched.cells[0][0][1])

    def test_reduce_all_data(self):
        input = Pyramid({0: self._single_pixel_layer({
            datetime.datetime.strptime("2016-04-24T04:00:00Z", '%Y-%m-%dT%H:%M:%SZ'): 1.0,
            datetime.datetime.strptime("2017-04-24T04:00:00Z", '%Y-%m-%dT%H:%M:%SZ'): 5.0
        })})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())

        stitched = imagecollection.reduce("min", "temporal").pyramid.levels[0].stitch()
        self.assertEqual(1.0, stitched.cells[0][0][0])

        stitched = imagecollection.reduce("max", "temporal").pyramid.levels[0].stitch()
        self.assertEqual(5.0, stitched.cells[0][0][0])

        stitched = imagecollection.reduce("sum", "temporal").pyramid.levels[0].stitch()
        self.assertEqual(6.0, stitched.cells[0][0][0])

        stitched = imagecollection.reduce("mean", "temporal").pyramid.levels[0].stitch()
        self.assertAlmostEqual(3.0, stitched.cells[0][0][0], delta=0.001)

        stitched = imagecollection.reduce("variance", "temporal").pyramid.levels[0].stitch()
        self.assertAlmostEqual(4.0, stitched.cells[0][0][0], delta=0.001)

        stitched = imagecollection.reduce("sd", "temporal").pyramid.levels[0].stitch()
        self.assertAlmostEqual(2.0, stitched.cells[0][0][0], delta=0.001)

    def test_reduce_some_nodata(self):
        no_data = -1.0

        input = Pyramid({0: self._single_pixel_layer({
            datetime.datetime.strptime("2016-04-24T04:00:00Z", '%Y-%m-%dT%H:%M:%SZ'): no_data,
            datetime.datetime.strptime("2017-04-24T04:00:00Z", '%Y-%m-%dT%H:%M:%SZ'): 5.0
        }, no_data)})

        imagecollection = GeotrellisTimeSeriesImageCollection(input, InMemoryServiceRegistry())

        stitched = imagecollection.reduce("min", "temporal").pyramid.levels[0].stitch()
        #print(stitched)
        self.assertEqual(5.0, stitched.cells[0][0][0])

        stitched = imagecollection.reduce("max", "temporal").pyramid.levels[0].stitch()
        self.assertEqual(5.0, stitched.cells[0][0][0])

        stitched = imagecollection.reduce("sum", "temporal").pyramid.levels[0].stitch()
        self.assertEqual(5.0, stitched.cells[0][0][0])

        stitched = imagecollection.reduce("mean", "temporal").pyramid.levels[0].stitch()
        self.assertAlmostEqual(5.0, stitched.cells[0][0][0], delta=0.001)

        stitched = imagecollection.reduce("variance", "temporal").pyramid.levels[0].stitch()
        self.assertAlmostEqual(0.0, stitched.cells[0][0][0], delta=0.001)

        stitched = imagecollection.reduce("sd", "temporal").pyramid.levels[0].stitch()
        self.assertAlmostEqual(0.0, stitched.cells[0][0][0], delta=0.001)

    def test_reduce_tiles(self):
        print("======")
        tile1 = self._single_pixel_tile(1)
        tile2 = self._single_pixel_tile(5)

        cube = np.array([tile1.cells, tile2.cells])

        # "MIN", "MAX", "SUM", "MEAN", "VARIANCE"

        std = np.std(cube, axis=0)
        var = np.var(cube, axis=0)
        print(var)

    @staticmethod
    def _single_pixel_tile(value, no_data=-1.0):
        cells = np.array([[value]])
        return Tile.from_numpy_array(cells, no_data)

    def _single_pixel_layer(self, grid_value_by_datetime, no_data=-1.0):
        from collections import OrderedDict

        sorted_by_datetime = OrderedDict(sorted(grid_value_by_datetime.items()))

        def elem(timestamp, value):
            tile = self._single_pixel_tile(value, no_data)
            return [(SpaceTimeKey(0, 0, timestamp), tile)]

        layer = [elem(timestamp, value) for timestamp, value in sorted_by_datetime.items()]
        rdd = SparkContext.getOrCreate().parallelize(layer)

        datetimes = list(sorted_by_datetime.keys())

        extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 1.0, 'ymax': 1.0}
        layout = {'layoutCols': 1, 'layoutRows': 1, 'tileCols': 1, 'tileRows': 1}
        metadata = {
            'cellType': 'float32ud%f' % no_data,
            'extent': extent,
            'crs': '+proj=longlat +datum=WGS84 +no_defs ',
            'bounds': {
                'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(datetimes[0])},
                'maxKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(datetimes[-1])}},
            'layoutDefinition': {
                'extent': extent,
                'tileLayout': layout}}

        return TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)

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
        file_name = os.path.join(dir, "datacube_reduce_time_sum.py")
        with open(file_name, "r")  as f:
            udf_code = f.read()

        result = imagecollection.apply_tiles_spatiotemporal(udf_code)
        stitched = result.pyramid.levels[0].to_spatial_layer().stitch()
        print(stitched)
        self.assertEqual(2,stitched.cells[0][0][0])
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
def rct_savitzky_golay(udf_data:UdfData):
    from scipy.signal import savgol_filter

    print(udf_data.get_datacube_list())
    return udf_data
        
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

        path = str(self.temp_folder / "resampled.tiff")
        resampled.reduce('max', 'temporal').download(path, format="GTIFF", parameters={'tiled': True})

        import rasterio
        with rasterio.open(path) as ds:
            print(ds.profile)
            self.assertAlmostEqual(0.05, ds.res[0], 3)
