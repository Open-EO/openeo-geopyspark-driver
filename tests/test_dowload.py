import datetime
from pathlib import Path
from unittest import TestCase,skip

import geopyspark as gps
import numpy as np
from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from pyspark import SparkContext
from shapely import geometry
from shapely.geometry import Point

from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.service_registry import InMemoryServiceRegistry
from openeo.metadata import Band


class TestDownload(TestCase):

    first = np.zeros((1, 4, 4))
    first.fill(1)

    second = np.zeros((1, 4, 4))
    second.fill(2)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 4.0, 'ymax': 4.0}
    layout = {'layoutCols': 2, 'layoutRows': 2, 'tileCols': 4, 'tileRows': 4}

    now = datetime.datetime.strptime("2017-09-25T11:37:00Z", '%Y-%m-%dT%H:%M:%SZ')

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

    expected_spatial_points_list = [
        (Point(1.0, -3.0), [1, 2]),
        (Point(2.0, 4.0), [1, 2]),
        (Point(3.0, 3.0), [1, 2]),
        (Point(1.0, -2.0), [1, 2]),
        (Point(-10.0, 15.0), None)
    ]

    expected_spacetime_points_list = [
        (Point(1.0, -3.0), now, [3]),
        (Point(2.0, 4.0), now, [3]),
        (Point(3.0, 3.0), now, [3]),
        (Point(1.0, -2.0), now, [3]),
        (Point(-10.0, 15.0), None, None)
    ]

    def setUp(self):
        # TODO: make this reusable (or a pytest fixture)
        self.temp_folder = Path.cwd() / 'tmp'
        if not self.temp_folder.exists():
            self.temp_folder.mkdir()
        assert self.temp_folder.is_dir()

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

    def download_no_bands(self, format):
        input = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(gps.Pyramid({0: input}), InMemoryServiceRegistry())
        res = imagecollection.save_result(str(self.temp_folder / "test_download_result.") + format, format=format)
        print(res)

    def download_no_args(self,format):
        input = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(gps.Pyramid({0: input}), InMemoryServiceRegistry())
        imagecollection.metadata=imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata=imagecollection.metadata.append_band(Band('band_two','',''))

        res = imagecollection.save_result(str(self.temp_folder / "test_download_result.") + format, format=format)
        print(res)
        #TODO how can we verify downloaded geotiffs, preferably without introducing a dependency on another library.

    def download_masked(self,format):
        input = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(gps.Pyramid({0: input}), InMemoryServiceRegistry())
        imagecollection.metadata=imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata=imagecollection.metadata.append_band(Band('band_two','',''))

        polygon = geometry.Polygon([[0, 0], [1.9, 0], [1.9, 1.9], [0, 1.9]])
        imagecollection = imagecollection.mask_polygon(mask=polygon)

        filename = str(self.temp_folder / "test_download_masked_result.") + format
        res = imagecollection.save_result(filename, format=format)
        print(res)
        #TODO how can we verify downloaded geotiffs, preferably without introducing a dependency on another library.

    def download_masked_reproject(self,format):
        input = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(gps.Pyramid({0: input}), InMemoryServiceRegistry())
        imagecollection.metadata=imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata=imagecollection.metadata.append_band(Band('band_two','',''))

        polygon = geometry.Polygon([[0, 0], [1.9, 0], [1.9, 1.9], [0, 1.9]])
        import pyproj
        from shapely.ops import transform
        from functools import partial
        project = partial(
            pyproj.transform,
            pyproj.Proj(init="EPSG:4326"),  # source coordinate system
            pyproj.Proj(init="EPSG:3857"))  # destination coordinate system
        reprojected = transform(project, polygon)
        imagecollection = imagecollection.mask_polygon(mask=reprojected, srs="EPSG:3857")

        filename = str(self.temp_folder / "test_download_masked_result.3857.") + format
        res = imagecollection.save_result(filename, format=format)
        print(res)
        #TODO how can we verify downloaded geotiffs, preferably without introducing a dependency on another library.


    def test_download_geotiff_no_args(self):
        self.download_no_args('gtiff')

    def test_download_netcdf_no_bands(self):
        self.download_no_bands('netcdf')

    def test_download_netcdf_no_args(self):
        self.download_no_args('netcdf')

    def test_download_json_no_args(self):
        self.download_no_args('json')

    def test_download_masked_geotiff(self):
        self.download_masked('gtiff')

    def test_download_masked_netcdf(self):
        self.download_masked('netcdf')

    def test_download_masked_json(self):
        self.download_masked('json')

    def test_download_masked_geotiff_reproject(self):
        self.download_masked_reproject('gtiff')

    def test_download_masked_netcdf_reproject(self):
        self.download_masked_reproject('netcdf')

    def test_download_masked_json_reproject(self):
        self.download_masked_reproject('json')


    #skipped because gdal_merge.py is not available on jenkins and Travis
    @skip
    def test_download_as_catalog(self):
        input = self.create_spacetime_layer()

        imagecollection = GeopysparkDataCube(gps.Pyramid({0: input}), InMemoryServiceRegistry())
        imagecollection.save_result("catalogresult.tiff", format="GTIFF", format_options={"parameters": {"catalog": True}})
