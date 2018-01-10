import os
import unittest

from geopyspark import geopyspark_conf
from pyspark import SparkContext


class BaseTestClass(unittest.TestCase):
    if 'TRAVIS' in os.environ:
        master_str = "local[2]"
    else:
        master_str = "local[*]"

    conf = geopyspark_conf(master=master_str, appName="test")
    conf.set('spark.kryoserializer.buffer.max', value='1G')
    conf.set('spark.ui.enabled', True)

    if 'TRAVIS' in os.environ:
        conf.set(key='spark.driver.memory', value='2G')
        conf.set(key='spark.executor.memory', value='2G')

    pysc = SparkContext(conf=conf)

    #dir_path = geotiff_test_path("all-ones.tif")

    # rdd = get(LayerType.SPATIAL, dir_path, max_tile_size=1024)
    # value = rdd.to_numpy_rdd().collect()[0]
    #
    # projected_extent = value[0]
    # extent = projected_extent.extent
    #
    # expected_tile = value[1].cells
    # (_, rows, cols) = expected_tile.shape
    #
    # layout = TileLayout(1, 1, cols, rows)
