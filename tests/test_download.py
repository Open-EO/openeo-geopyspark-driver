import datetime
import json
from pathlib import Path
from unittest import TestCase, skip

import geopyspark as gps
import numpy as np
from geopyspark.geotrellis import (SpaceTimeKey, Tile, _convert_to_unix_time)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from openeo_driver.util.geometry import geojson_to_geometry
from pyspark import SparkContext
from shapely import geometry
from shapely.geometry import Point

from openeo.metadata import Band
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from tests.data import get_test_data_file


class TestDownload:

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

    def download_no_bands(self, tmp_path, format):
        input = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
        res = imagecollection.save_result(str(tmp_path / "test_download_result.") + format, format=format)
        print(res)

    def download_no_args(self, tmp_path, format, format_options={}):
        input = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
        imagecollection.metadata=imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata=imagecollection.metadata.append_band(Band('band_two','',''))

        res = imagecollection.save_result(str(tmp_path / "test_download_result.") + format, format=format, format_options=format_options)
        print(res)
        return res
        #TODO how can we verify downloaded geotiffs, preferably without introducing a dependency on another library.

    def download_no_args_single_byte_band(self, tmp_path, format, format_options={}):
        input = self.create_spacetime_layer().convert_data_type(gps.CellType.UINT8)
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
        imagecollection.metadata=imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata=imagecollection.metadata.append_band(Band('band_two','',''))
        imagecollection.filter_bands(['band_one'])


        res = imagecollection.save_result(str(tmp_path / "test_download_result_single_band.") + format, format=format, format_options=format_options)
        print(res)
        return res

    def download_masked(self, tmp_path, format):
        input = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
        imagecollection.metadata=imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata=imagecollection.metadata.append_band(Band('band_two','',''))

        polygon = geometry.Polygon([[0, 0], [1.9, 0], [1.9, 1.9], [0, 1.9]])
        imagecollection = imagecollection.mask_polygon(mask=polygon)

        filename = str(tmp_path / "test_download_masked_result.") + format
        res = imagecollection.save_result(filename, format=format)
        print(res)
        #TODO how can we verify downloaded geotiffs, preferably without introducing a dependency on another library.

    def download_masked_reproject(self, tmp_path, format):
        input = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
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

        filename = str(tmp_path / "test_download_masked_result.3857.") + format
        res = imagecollection.save_result(filename, format=format)
        print(res)
        #TODO how can we verify downloaded geotiffs, preferably without introducing a dependency on another library.

    def test_download_geotiff_no_args(self, tmp_path):
        self.download_no_args(tmp_path, 'gtiff')

    def test_download_netcdf_no_bands(self, tmp_path):
        self.download_no_bands(tmp_path, 'netcdf')

    def test_download_netcdf_no_args(self, tmp_path):
        self.download_no_args(tmp_path, 'netcdf')

    def test_download_netcdf_no_args_batch(self, tmp_path):
        res = self.download_no_args(tmp_path, 'netcdf', {"batch_mode": True, "multidate": True})
        print(res)

    def test_download_json_no_args(self, tmp_path):
        self.download_no_args(tmp_path, 'json')

    def test_download_geotiff_colormap(self, tmp_path):
        self.download_no_args_single_byte_band(tmp_path, 'gtiff', {'colormap':{"0":0,"1":[0.5,0.1,0.2,0.5],"2":40000}})

    def test_download_png_colormap(self, tmp_path):
        self.download_no_args_single_byte_band(tmp_path, 'png', {'colormap':{"0":0,"1":[0.5,0.1,0.2,0.5],"2":40000}})

    def test_download_masked_geotiff(self, tmp_path):
        self.download_masked(tmp_path, 'gtiff')

    def test_download_masked_netcdf(self, tmp_path):
        self.download_masked(tmp_path, 'netcdf')

    def test_download_masked_json(self, tmp_path):
        self.download_masked(tmp_path, 'json')

    def test_download_masked_geotiff_reproject(self, tmp_path):
        self.download_masked_reproject(tmp_path, 'gtiff')

    def test_download_masked_netcdf_reproject(self, tmp_path):
        self.download_masked_reproject(tmp_path, 'netcdf')

    def test_download_masked_json_reproject(self, tmp_path):
        self.download_masked_reproject(tmp_path, 'json')

    def test_write_assets(self, tmp_path):
        input = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
        imagecollection.metadata = imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata = imagecollection.metadata.append_band(Band('band_two', '', ''))
        format = 'GTiff'

        res = imagecollection.write_assets(str(tmp_path / "test_download_result.") + format, format=format,format_options={
            "multidate":True,
            "batch_mode":True
        })
        assert 1 == len(res)
        name, asset = res.popitem()
        assert Path(asset['href']).parent == tmp_path
        assert asset['nodata'] == -1
        assert asset['roles'] == ['data']
        assert 2 == len(asset['bands'])
        assert 'image/tiff; application=geotiff' == asset['type']
        assert asset['datetime'] == "2017-09-25T11:37:00Z"

    with get_test_data_file("geometries/polygons02.geojson").open() as f:
        features = json.load(f)

    def test_write_assets_samples(self, tmp_path):
        input = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
        imagecollection.metadata = imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata = imagecollection.metadata.append_band(Band('band_two', '', ''))
        format = 'GTiff'

        res = imagecollection.write_assets(str(tmp_path / "test_download_result.") + format, format=format,format_options={
            "multidate":True,
            "batch_mode":True,
            "geometries":geojson_to_geometry(self.features),
            "sample_by_feature": True,
            "feature_id_property": 'id'
        })
        assert len(res) == 3
        name,asset = res.popitem()
        assert Path(asset['href']).parent == tmp_path
        assert asset['nodata'] == -1
        assert asset['roles'] == ['data']
        assert 2 == len(asset['bands'])
        assert 'image/tiff; application=geotiff' == asset['type']
        assert asset['datetime'] == "2017-09-25T11:37:00Z"

    def test_write_assets_samples_netcdf(self, tmp_path):
        input = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
        imagecollection.metadata = imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata = imagecollection.metadata.append_band(Band('band_two', '', ''))
        format = 'netCDF'

        res = imagecollection.write_assets(str(tmp_path / "test_download_result.") + format, format=format,format_options={
            "batch_mode":True,
            "geometries":geojson_to_geometry(self.features),
            "sample_by_feature": True,
            "feature_id_property": 'id'
        })
        assert len(res) == 3
        name,asset = res.popitem()
        file = asset['href']
        assert asset['nodata'] == -1
        assert asset['roles'] == ['data']
        assert 2 == len(asset['bands'])
        assert 'application/x-netcdf' == asset['type']



    #skipped because gdal_merge.py is not available on jenkins and Travis
    @skip
    def test_download_as_catalog(self):
        input = self.create_spacetime_layer()

        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input}))
        imagecollection.save_result("catalogresult.tiff", format="GTIFF", format_options={"parameters": {"catalog": True}})
