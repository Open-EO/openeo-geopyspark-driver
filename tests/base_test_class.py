import unittest

from pyspark import find_spark_home
import os,sys
from glob import glob

spark_python = os.path.join(find_spark_home._find_spark_home(), 'python')
py4j = glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
sys.path[:0] = [spark_python, py4j]



from geopyspark import geopyspark_conf, Pyramid, TiledRasterLayer
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

    pysc = SparkContext.getOrCreate(conf)

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

    @classmethod
    def to_pyramid(cls,tiled_rdd:TiledRasterLayer):
        return Pyramid( {0:tiled_rdd})