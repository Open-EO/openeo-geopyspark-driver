import datetime
import textwrap
from pathlib import Path
from unittest import TestCase

import numpy as np
import numpy.testing
import pytest
import rasterio
import geopyspark as gps
from geopyspark import CellType
from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time, TemporalProjectedExtent, Extent,
                                   RasterLayer)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer, Pyramid
from numpy.testing import assert_array_almost_equal
from pyspark import SparkContext
from shapely.geometry import Point

from openeo_driver.errors import FeatureUnsupportedException
from openeo_driver.utils import EvalEnv
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata
from openeogeotrellis.numpy_aggregators import max_composite


def reducer(operation: str):
    return {
        f"{operation}1" : {
            "process_id": operation,
            "arguments": {
                "data": {
                    "from_parameter": "data"
                }
            },
            "result": True
        },
    }

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
    layout = {'layoutCols': 2, 'layoutRows': 2, 'tileCols': 254, 'tileRows': 254}
    metadata = {'cellType': 'float32ud-1.0',
                'extent': extent,
                'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                'bounds': {
                    'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(time_1)},
                    'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(time_3)}},
                'layoutDefinition': {
                    'extent': extent,
                    'tileLayout': layout.copy()}}
    collection_metadata = GeopysparkCubeMetadata({
        "cube:dimensions": {
            "t": {"type": "temporal"},
        }
    })

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

    def test_repartition(self):
        jvm = gps.get_spark_context()._jvm
        p = jvm.org.openeo.geotrellis.OpenEOProcesses()
        spk = jvm.geotrellis.layer.SpaceTimeKey
        datacubeParams = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
        result = p.applySparseSpacetimePartitioner(self.tiled_raster_rdd.srdd.rdd(),[spk(0,0,0),spk(1,0,100000),spk(10000000,454874414,100000)],datacubeParams.partitionerIndexReduction())
        assert result is not None
        assert "SpacePartitioner(KeyBounds(SpaceTimeKey(0,0,0),SpaceTimeKey(10000000,454874414,100000)))" == str(result.partitioner().get())
        assert "SparseSpaceTimePartitioner 2 true" == str(result.partitioner().get().index())

        contextRDD = jvm.geotrellis.spark.ContextRDD(result,self.tiled_raster_rdd.srdd.rdd().metadata())
        srdd = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer.apply(jvm.scala.Option.apply(0), contextRDD)


    def test_reproject_spatial(self):
        input = Pyramid({0: self.tiled_raster_rdd})

        imagecollection = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)

        ref_path = str(self.temp_folder / "reproj_ref.tiff")
        imagecollection.reduce_dimension(reducer=reducer("max"), dimension="t", env=EvalEnv()).save_result(
            ref_path, format="GTIFF"
        )

        resampled = imagecollection.resample_spatial(resolution=0,projection="EPSG:3395",method="max")
        metadata = resampled.pyramid.levels[0].layer_metadata
        print(metadata)
        self.assertTrue("proj=merc" in metadata.crs)
        path = str(self.temp_folder / "reprojected.tiff")
        res = resampled.reduce_dimension(reducer=reducer("max"), dimension="t", env=EvalEnv())
        res.save_result(path, format="GTIFF")

        with rasterio.open(ref_path) as ref_ds:
            with rasterio.open(path) as ds:
                print(ds.profile)
                #this reprojection does not change the shape, so we can compare
                assert ds.read().shape == ref_ds.read().shape

                assert (ds.crs.to_epsg() == 3395)

    def test_drop_dimension(self):
        input = Pyramid({0: self.tiled_raster_rdd})

        cube = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata).filter_temporal("2016-08-23","2016-08-26")
        labels = cube.dimension_labels("t")
        assert labels == [datetime.datetime(2016, 8, 24, 9, 0)]
        spatial_cube = cube.drop_dimension("t")
        assert not spatial_cube.metadata.has_temporal_dimension()

    def test_reduce(self):
        input = Pyramid({0: self.tiled_raster_rdd})

        cube = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)
        env = EvalEnv()

        stitched = cube.reduce_dimension(dimension="t", reducer=reducer("max"), env=env).pyramid.levels[0].stitch()
        numpy.testing.assert_allclose(stitched.cells[:, :2, :2], [[[2, 2], [2, 2]]])

        stitched = cube.reduce_dimension(dimension="t", reducer=reducer("min"), env=env).pyramid.levels[0].stitch()
        numpy.testing.assert_allclose(stitched.cells[:, :2, :2], [[[2, 1], [1, 1]]])

        stitched = cube.reduce_dimension(dimension="t", reducer=reducer("sum"), env=env).pyramid.levels[0].stitch()
        numpy.testing.assert_allclose(stitched.cells[:, :2, :2], [[[2, 4], [4, 4]]])

        stitched = cube.reduce_dimension(dimension="t", reducer=reducer("mean"), env=env).pyramid.levels[0].stitch()
        numpy.testing.assert_allclose(stitched.cells[:, :2, :2], [[[2, 4.0 / 3.0], [4.0 / 3.0, 4.0 / 3.0]]])

        stitched = cube.reduce_dimension(reducer=reducer("variance"), dimension="t", env=env).pyramid.levels[0].stitch()
        numpy.testing.assert_allclose(stitched.cells[:, :2, :2], [[[np.nan, 1.0 / 3.0], [1.0 / 3.0, 1.0 / 3.0]]])

        stitched = cube.reduce_dimension(reducer=reducer("sd"), dimension="t", env=env).pyramid.levels[0].stitch()
        numpy.testing.assert_allclose(stitched.cells[:, :2, :2], [[[np.nan, 0.5773503], [0.5773503, 0.5773503]]])


    def test_reduce_all_data(self):
        input = Pyramid({0: self._single_pixel_layer({
            datetime.datetime.strptime("2016-04-24T04:00:00Z", '%Y-%m-%dT%H:%M:%SZ'): 1.0,
            datetime.datetime.strptime("2017-04-24T04:00:00Z", '%Y-%m-%dT%H:%M:%SZ'): 5.0
        })})

        cube = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)
        env = EvalEnv()
        stitched = cube.reduce_dimension(reducer=reducer("min"), dimension="t", env=env).pyramid.levels[0].stitch()
        self.assertEqual(1.0, stitched.cells[0][0][0])

        stitched = cube.reduce_dimension(reducer=reducer("max"), dimension="t", env=env).pyramid.levels[0].stitch()
        self.assertEqual(5.0, stitched.cells[0][0][0])

        stitched = cube.reduce_dimension(reducer=reducer("sum"), dimension="t", env=env).pyramid.levels[0].stitch()
        self.assertEqual(6.0, stitched.cells[0][0][0])

        stitched = cube.reduce_dimension(reducer=reducer("mean"), dimension="t", env=env).pyramid.levels[0].stitch()
        self.assertAlmostEqual(3.0, stitched.cells[0][0][0], delta=0.001)

        stitched = cube.reduce_dimension(reducer=reducer("variance"), dimension="t", env=env).pyramid.levels[0].stitch()
        self.assertAlmostEqual(8.0, stitched.cells[0][0][0], delta=0.001)

        stitched = cube.reduce_dimension(reducer=reducer("sd"), dimension="t", env=env).pyramid.levels[0].stitch()
        self.assertAlmostEqual(2.8284271, stitched.cells[0][0][0], delta=0.001)

    def test_reduce_some_nodata(self):
        no_data = -1.0

        input = Pyramid({0: self._single_pixel_layer({
            datetime.datetime.strptime("2016-04-24T04:00:00Z", '%Y-%m-%dT%H:%M:%SZ'): no_data,
            datetime.datetime.strptime("2017-04-24T04:00:00Z", '%Y-%m-%dT%H:%M:%SZ'): 5.0
        }, no_data)})
        env = EvalEnv()

        imagecollection = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)

        stitched = imagecollection.reduce_dimension(reducer("min"), dimension="t", env=env).pyramid.levels[0].stitch()
        #print(stitched)
        self.assertEqual(5.0, stitched.cells[0][0][0])

        stitched = imagecollection.reduce_dimension(reducer("max"), dimension="t", env=env).pyramid.levels[0].stitch()
        self.assertEqual(5.0, stitched.cells[0][0][0])

        stitched = imagecollection.reduce_dimension(reducer("sum"), dimension="t", env=env).pyramid.levels[0].stitch()
        self.assertEqual(5.0, stitched.cells[0][0][0])

        stitched = imagecollection.reduce_dimension(reducer("mean"), dimension="t", env=env).pyramid.levels[0].stitch()
        self.assertAlmostEqual(5.0, stitched.cells[0][0][0], delta=0.001)

        stitched = imagecollection.reduce_dimension(reducer("variance"), dimension="t", env=env).pyramid.levels[0].stitch()
        self.assertTrue(np.isnan(stitched.cells[0][0][0]))

        stitched = imagecollection.reduce_dimension(reducer("sd"), dimension="t", env=env).pyramid.levels[0].stitch()
        self.assertTrue(np.isnan(stitched.cells[0][0][0]))

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

        imagecollection = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)
        with self.assertRaises(FeatureUnsupportedException) as context:
            imagecollection.reduce_dimension(reducer("max"), dimension="gender", env=EvalEnv()).pyramid.levels[0].stitch()
        print(context.exception)

    def test_aggregate_temporal(self):
        """
        Tests deprecated process spec! To be phased out.
        @return:
        """
        interval_list = ["2017-01-01", "2018-01-01"]
        self._test_aggregate_temporal(interval_list)

    def _median_reducer(self):
        from openeo.processes import median
        builder = median({"from_parameter": "data"})
        return builder.flat_graph()

    def test_aggregate_temporal_median(self):
        input = Pyramid({0: self.tiled_raster_rdd})
        imagecollection = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)
        stitched = (
            imagecollection.aggregate_temporal(["2015-01-01", "2018-01-01"], ["2017-01-03"], self._median_reducer(), dimension="t")
                .pyramid.levels[0].to_spatial_layer().stitch()
        )
        print(stitched)
        expected_median = np.median([self.tile.cells, self.tile2.cells, self.tile.cells], axis=0)
        #TODO nodata handling??
        assert_array_almost_equal(stitched.cells[0, 1:2, 1:2], expected_median[ 1:2, 1:2])


    def _test_aggregate_temporal(self, interval_list):
        input = Pyramid({0: self.tiled_raster_rdd})
        imagecollection = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)
        stitched = (
            imagecollection.aggregate_temporal(interval_list, ["2017-01-03"], "min", dimension="t")
                .pyramid.levels[0].to_spatial_layer().stitch()
        )
        print(stitched)
        expected_max = np.min([self.tile2.cells, self.tile.cells],axis=0)
        assert_array_almost_equal(stitched.cells[0, 0:5, 0:5], expected_max)


    def test_aggregate_temporal_100(self):
        self._test_aggregate_temporal([["2017-01-01", "2018-01-01"]])

    def test_max_aggregator(self):
        tiles = [self.tile,self.tile2]
        composite = max_composite(tiles)
        self.assertEqual(2.0, composite.cells[0][0])

    def test_aggregate_max_time(self):
        input = Pyramid( {0:self.tiled_raster_rdd })
        imagecollection = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)

        layer = imagecollection.reduce_dimension(reducer('max'), dimension='t', env=EvalEnv()).pyramid.levels[0]
        stitched = layer.stitch()
        assert 'float32ud-1.0' == layer.layer_metadata.cell_type
        print(stitched)
        self.assertEqual(2.0, stitched.cells[0][0][0])

    def test_min_time(self):
        input = Pyramid( {0:self.tiled_raster_rdd })

        cube = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)
        env = EvalEnv()
        min_time = cube.reduce_dimension(reducer=reducer('min'), dimension='t', env=env)
        max_time = cube.reduce_dimension(reducer=reducer('max'), dimension='t', env=env)

        stitched = min_time.pyramid.levels[0].stitch()
        print(stitched)

        self.assertEquals(2.0,stitched.cells[0][0][0])

        for p in self.points[1:3]:
            # TODO #421 drop old unsued "point timeseries" feature
            result = min_time.timeseries(p.x, p.y,srs="EPSG:3857")
            print(result)
            print(cube.timeseries(p.x,p.y,srs="EPSG:3857"))
            max_result = max_time.timeseries(p.x, p.y,srs="EPSG:3857")
            self.assertEqual(1.0,result['NoDate'])
            self.assertEqual(2.0,max_result['NoDate'])

    def test_apply_dimension_spatiotemporal(self):

        input = Pyramid({0: self.tiled_raster_rdd})

        imagecollection = GeopysparkDataCube(
            pyramid=input,
            metadata=GeopysparkCubeMetadata({
                "cube:dimensions": {
                    # TODO: also specify other dimensions?
                    "bands": {"type": "bands", "values": ["2"]}
                },
                "summaries": {"eo:bands": [
                    {
                        "name": "2",
                        "common_name": "blue",
                        "wavelength_nm": 496.6,
                        "res_m": 10,
                        "scale": 0.0001,
                        "offset": 0,
                        "type": "int16",
                        "unit": "1"
                    }
                ]}
            })
        )

        udf_code = """
def rct_savitzky_golay(udf_data:UdfData):
    from scipy.signal import savgol_filter

    print(udf_data.get_datacube_list())
    return udf_data

        """


        result = imagecollection.apply_tiles_spatiotemporal(udf_code=udf_code)
        local_tiles = result.pyramid.levels[0].to_numpy_rdd().collect()
        print(local_tiles)
        self.assertEquals(len(TestMultipleDates.layer),len(local_tiles))
        ref_dict = {e[0]:e[1] for e in imagecollection.pyramid.levels[0].convert_data_type(CellType.FLOAT64).to_numpy_rdd().collect()}
        result_dict = {e[0]: e[1] for e in local_tiles}
        for k,v in ref_dict.items():
            tile = result_dict[k]
            assert_array_almost_equal(np.squeeze(v.cells),np.squeeze(tile.cells),decimal=2)

    def test_mask_raster_replacement_default_none(self):
        def createMask(tile):
            tile.cells[0][0][0] = 0.0
            return tile

        input = Pyramid({0: self.tiled_raster_rdd})
        mask_layer = self.tiled_raster_rdd.map_tiles(createMask)
        mask = Pyramid({0: mask_layer})

        cube = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)
        mask_cube = GeopysparkDataCube(pyramid=mask)
        stitched = cube.mask(mask=mask_cube)\
            .reduce_dimension(reducer('max'), dimension="t", env=EvalEnv())\
            .pyramid.levels[0].stitch()
        print(stitched)
        assert stitched.cells[0][0][0] == 2.0
        assert stitched.cells[0][0][1] == -1.0

    def test_mask_raster_replacement_float(self):
        def createMask(tile):
            tile.cells[0][0][0] = 0.0
            return tile

        input = Pyramid({0: self.tiled_raster_rdd})
        mask_layer = self.tiled_raster_rdd.map_tiles(createMask)
        mask = Pyramid({0: mask_layer})

        cube = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)
        mask_cube = GeopysparkDataCube(pyramid=mask)
        stitched = cube.mask(mask=mask_cube, replacement=10.0)\
            .reduce_dimension(reducer('max'), dimension="t", env=EvalEnv())\
            .pyramid.levels[0].stitch()
        print(stitched)
        assert stitched.cells[0][0][0] == 2.0
        assert stitched.cells[0][0][1] == 10.0

    def test_mask_raster_replacement_int(self):
        def createMask(tile):
            tile.cells[0][0][0] = 0.0
            return tile

        input = Pyramid({0: self.tiled_raster_rdd})
        mask_layer = self.tiled_raster_rdd.map_tiles(createMask)
        mask = Pyramid({0: mask_layer})

        cube = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)
        mask_cube = GeopysparkDataCube(pyramid=mask)
        stitched = cube.mask(mask=mask_cube, replacement=10)\
            .reduce_dimension(reducer('max'), dimension="t", env=EvalEnv())\
            .pyramid.levels[0].stitch()
        print(stitched)
        assert stitched.cells[0][0][0] == 2.0
        assert stitched.cells[0][0][1] == 10.0

    def test_apply_kernel_float(self):
        kernel = np.array([[0.0, 1.0, 0.0], [1.0, 1.0, 1.0], [0.0, 1.0, 0.0]])

        input = Pyramid({0: self.tiled_raster_rdd})
        img = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)
        stitched = img.apply_kernel(kernel, 2.0)\
            .reduce_dimension(reducer('max'), dimension="t", env=EvalEnv()).pyramid.levels[0].stitch()

        assert stitched.cells[0][0][0] == 12.0
        assert stitched.cells[0][0][1] == 16.0
        assert stitched.cells[0][1][1] == 20.0

    def test_apply_kernel_int(self):
        kernel = np.array([[0, 1, 0], [1, 1, 1], [0, 1, 0]])

        input = Pyramid({0: self.tiled_raster_rdd})
        img = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)
        stitched = img.apply_kernel(kernel)\
            .reduce_dimension(reducer('max'), dimension="t", env=EvalEnv)\
            .pyramid.levels[0].stitch()

        assert stitched.cells[0][0][0] == 6.0
        assert stitched.cells[0][0][1] == 8.0
        assert stitched.cells[0][1][1] == 10.0

    def test_resample_spatial(self):
        input = Pyramid({0: self.tiled_raster_rdd})

        imagecollection = GeopysparkDataCube(pyramid=input, metadata=self.collection_metadata)

        resampled = imagecollection.resample_spatial(resolution=0.05)

        path = str(self.temp_folder / "resampled.tiff")
        res = resampled.reduce_dimension(reducer('max'), dimension="t", env=EvalEnv())
        res.save_result(path, format="GTIFF")

        import rasterio
        with rasterio.open(path) as ds:
            print(ds.profile)
            self.assertAlmostEqual(0.05, ds.res[0], 3)

    def test_rename_dimension(self):
        imagecollection = GeopysparkDataCube(pyramid=Pyramid({0: self.tiled_raster_rdd}),
                                             metadata=self.collection_metadata)

        dim_renamed = imagecollection.rename_dimension('t','myNewTimeDim')

        dim_renamed.metadata.assert_valid_dimension('myNewTimeDim')


@pytest.mark.parametrize(
    "udf_code",
    [
        """
        from openeo_udf.api.datacube import DataCube  # Old style openeo_udf API
        from openeo_udf.api.udf_data import UdfData  # Old style openeo_udf API

        def hyper_sum(udf_data: UdfData):
            # Iterate over each tile
            cube_list = []
            for cube in udf_data.get_datacube_list():
                mean = cube.array.sum(dim="t")
                mean.name = cube.id + "_sum"
                cube_list.append(DataCube(array=mean))
            udf_data.set_datacube_list(cube_list)
    """,
        """
        from openeo.udf import XarrayDataCube, UdfData

        def hyper_sum(udf_data: UdfData):
            # Iterate over each tile
            cube_list = []
            for cube in udf_data.get_datacube_list():
                mean = cube.array.sum(dim="t")
                mean.name = cube.id + "_sum"
                cube_list.append(XarrayDataCube(array=mean))
            udf_data.set_datacube_list(cube_list)
    """,
])
def test_apply_spatiotemporal(udf_code):
    udf_code = textwrap.dedent(udf_code)

    input = Pyramid({0: TestMultipleDates.tiled_raster_rdd})

    imagecollection = GeopysparkDataCube(
        pyramid=input,
        metadata=GeopysparkCubeMetadata({
            "cube:dimensions": {
                # TODO: also specify other dimensions?
                "bands": {"type": "bands", "values": ["2"]}
            },
            "summaries": {"eo:bands": [
                {
                    "name": "2",
                    "common_name": "blue",
                    "wavelength_nm": 496.6,
                    "res_m": 10,
                    "scale": 0.0001,
                    "offset": 0,
                    "type": "int16",
                    "unit": "1"
                }
            ]}
        })
    )

    result = imagecollection.apply_tiles_spatiotemporal(udf_code=udf_code)
    stitched = result.pyramid.levels[0].to_spatial_layer().stitch()
    print(stitched)

    assert stitched.cells[0][0][0] == 2
    assert stitched.cells[0][0][5] == 6
    assert stitched.cells[0][5][6] == 4
