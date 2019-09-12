import datetime
import os

import numpy as np
from geopyspark import geopyspark_conf
from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from pyspark import SparkContext

from openeogeotrellis.configparams import ConfigParams


class TestLayers:

    def __init__(self):
        master_str = "local[*]"

        conf = geopyspark_conf(master=master_str, appName="test")
        conf.set('spark.kryoserializer.buffer.max', value='1G')
        conf.set('spark.ui.enabled', True)

        if ConfigParams().is_ci_context:
            conf.set(key='spark.driver.memory', value='2G')
            conf.set(key='spark.executor.memory', value='2G')


        self.pysc = SparkContext.getOrCreate(conf)

        self.first = np.zeros((1, 4, 4))
        self.first.fill(1)

        self.second = np.zeros((1, 4, 4))
        self.second.fill(2)

        self.extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 4.0, 'ymax': 4.0}
        self.layout = {'layoutCols': 1, 'layoutRows': 1, 'tileCols': 4, 'tileRows': 4}

        self.now = datetime.datetime.strptime("2017-09-25T11:37:00Z", '%Y-%m-%dT%H:%M:%SZ')

    def create_spacetime_layer(self):
        cells = np.array([self.first, self.second], dtype='int')
        tile = Tile.from_numpy_array(cells, -1)

        layer = [(SpaceTimeKey(0, 0, self.now), tile),
                 (SpaceTimeKey(1, 0, self.now), tile),
                 (SpaceTimeKey(0, 1, self.now), tile),
                 (SpaceTimeKey(1, 1, self.now), tile)]

        rdd = self.pysc.parallelize(layer)

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
