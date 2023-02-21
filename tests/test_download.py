import datetime
import json
import os
import shutil
from pathlib import Path
from unittest import TestCase, skip

import geopyspark as gps
import numpy as np
import pytest
from geopyspark.geotrellis import (SpatialKey, SpaceTimeKey, Tile, _convert_to_unix_time)
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

    def create_spatial_layer(self):
        cells = np.array([self.first, self.second], dtype='int')
        tile = Tile.from_numpy_array(cells, -1)

        layer = [(SpatialKey(0, 0), tile),
                 (SpatialKey(1, 0), tile),
                 (SpatialKey(0, 1), tile),
                 (SpatialKey(1, 1), tile)]

        rdd = SparkContext.getOrCreate().parallelize(layer)

        metadata = {'cellType': 'int32ud-1',
                    'extent': self.extent,
                    'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                    'bounds': {
                        'minKey': {'col': 0, 'row': 0},
                        'maxKey': {'col': 1, 'row': 1}
                    },
                    'layoutDefinition': {
                        'extent': self.extent,
                        'tileLayout': self.layout
                    }
                    }

        return TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, metadata)

    def download_no_bands(self, tmp_path, format):
        input_layer = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        res = imagecollection.save_result(str(tmp_path / "test_download_result.") + format, format=format)
        print(res)

    def download_no_args(self, tmp_path, format, format_options={}):
        input_layer = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.metadata=imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata=imagecollection.metadata.append_band(Band('band_two','',''))

        res = imagecollection.save_result(str(tmp_path / "test_download_result.") + format, format=format, format_options=format_options)
        print(res)
        return res
        #TODO how can we verify downloaded geotiffs, preferably without introducing a dependency on another library.

    def download_no_args_single_byte_band(self, tmp_path, format, format_options={}):
        input_layer = self.create_spacetime_layer().convert_data_type(gps.CellType.UINT8)
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.metadata=imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata=imagecollection.metadata.append_band(Band('band_two','',''))
        imagecollection.filter_bands(['band_one'])


        res = imagecollection.save_result(str(tmp_path / "test_download_result_single_band.") + format, format=format, format_options=format_options)
        print(res)
        return res

    def download_masked(self, tmp_path, format):
        input_layer = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.metadata=imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata=imagecollection.metadata.append_band(Band('band_two','',''))

        polygon = geometry.Polygon([[0, 0], [1.9, 0], [1.9, 1.9], [0, 1.9]])
        imagecollection = imagecollection.mask_polygon(mask=polygon)

        filename = str(tmp_path / "test_download_masked_result.") + format
        res = imagecollection.save_result(filename, format=format)
        print(res)
        #TODO how can we verify downloaded geotiffs, preferably without introducing a dependency on another library.

    def download_masked_reproject(self, tmp_path, format):
        input_layer = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
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
        input_layer = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.metadata = imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata = imagecollection.metadata.append_band(Band('band_two', '', ''))
        format = 'GTiff'

        print("tmp_path: " + str(tmp_path))
        res = imagecollection.write_assets(str(tmp_path / "ignored.tiff"), format=format,format_options={
            "multidate": True,
            "batch_mode": True,
            "filename_prefix": "filenamePrefixTest"
        })
        assert 1 == len(res)
        name, asset = next(iter(res.items()))
        assert Path(asset['href']).parent == tmp_path
        assert asset['nodata'] == -1
        assert asset['roles'] == ['data']
        assert 2 == len(asset['bands'])
        assert "filenamePrefixTest" in asset['href']
        assert 'image/tiff; application=geotiff' == asset['type']
        assert asset['datetime'] == "2017-09-25T11:37:00Z"
        assert "filenamePrefixTest" in name

    with get_test_data_file("geometries/polygons02.geojson").open() as f:
        features = json.load(f)

    def test_write_assets_samples(self, tmp_path):
        input_layer = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.metadata = imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata = imagecollection.metadata.append_band(Band('band_two', '', ''))
        format = 'GTiff'

        res = imagecollection.write_assets(str(tmp_path / "ignored.tiff"), format=format, format_options={
            "multidate": True,
            "batch_mode": True,
            "geometries": geojson_to_geometry(self.features),
            "sample_by_feature": True,
            "feature_id_property": 'id',
            "filename_prefix": "filenamePrefixTest",
        })
        assert len(res) == 3
        name, asset = next(iter(res.items()))
        assert Path(asset['href']).parent == tmp_path
        assert asset['nodata'] == -1
        assert asset['roles'] == ['data']
        assert 2 == len(asset['bands'])
        assert "filenamePrefixTest" in asset['href']
        assert 'image/tiff; application=geotiff' == asset['type']
        assert asset['datetime'] == "2017-09-25T11:37:00Z"

    def test_write_assets_samples_tile_grid(self, tmp_path):
        input_layer = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.metadata = imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata = imagecollection.metadata.append_band(Band('band_two', '', ''))
        format = 'GTiff'

        res = imagecollection.write_assets(str(tmp_path / "test_download_result.tiff"), format=format, format_options={
            "multidate": True,  # not used
            "batch_mode": False,
            "geometries": geojson_to_geometry(self.features),
            "sample_by_feature": True,
            "feature_id_property": 'id',
            "filename_prefix": "filenamePrefixTest",
            "tile_grid": "100km",
        })
        assert len(res) == 30
        name, asset = next(iter(res.items()))
        assert Path(asset['href']).parent == tmp_path
        assert asset['roles'] == ['data']
        assert 'image/tiff; application=geotiff' == asset['type']

    def test_write_assets_samples_tile_grid_batch(self, tmp_path):
        input_layer = self.create_spacetime_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.metadata = imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata = imagecollection.metadata.append_band(Band('band_two', '', ''))
        format = 'GTiff'

        res = imagecollection.write_assets(str(tmp_path / "ignored.tiff"), format=format, format_options={
            "multidate": True,
            "batch_mode": True,
            "geometries": geojson_to_geometry(self.features),
            "sample_by_feature": True,
            "feature_id_property": 'id',
            "filename_prefix": "filenamePrefixTest",
            "tile_grid": "100km",
        })
        assert len(res) == 30
        name, asset = next(iter(res.items()))
        assert Path(asset['href']).parent == tmp_path
        assert asset['nodata'] == -1
        assert asset['roles'] == ['data']
        assert 2 == len(asset['bands'])
        assert "filenamePrefixTest" in asset['href']
        assert 'image/tiff; application=geotiff' == asset['type']
        assert asset['datetime'] == "2017-09-25T11:37:00Z"

    test_write_assets_parameterize_batch_path = "tmp/test_write_assets_parameterize_batch/"
    shutil.rmtree(test_write_assets_parameterize_batch_path, ignore_errors=True)
    os.makedirs(test_write_assets_parameterize_batch_path)

    @pytest.mark.parametrize("filename_prefix", [None, "prefixTest"])
    @pytest.mark.parametrize("tile_grid", [None, "100km"])
    @pytest.mark.parametrize("space_type", ["spacetime", "spatial"])
    @pytest.mark.parametrize("stitch", [False, True])
    @pytest.mark.parametrize("catalog", [False, True])
    @pytest.mark.parametrize("sample_by_feature", [False, True])
    @pytest.mark.parametrize("format_arg", ["netCDF"])  # "GTIFF" behaves different from "netCDF", so not testing now
    def test_write_assets_parameterize_batch(self, format_arg, sample_by_feature, catalog, stitch, space_type,
                                             tile_grid, filename_prefix, tmp_path):
        d = locals()
        d = {i: d[i] for i in d if i != 'self' and i != "tmp_path" and i != "d"}
        test_name = "-".join(map(str, list(d.values())))  # a bit like how pytest names it

        if space_type == "spacetime":
            input_layer = self.create_spacetime_layer()
        else:
            input_layer = self.create_spatial_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.metadata = imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata = imagecollection.metadata.append_band(Band('band_two', '', ''))

        assets = imagecollection.write_assets(
            str(tmp_path / "ignored<\0>.extension"),  # null byte to cause error if filename would be written to fs
            format=format_arg,
            format_options={
                "batch_mode": True,
                "geometries": geojson_to_geometry(self.features),
                "sample_by_feature": True,
                "feature_id_property": 'id',
                "filename_prefix": filename_prefix,
                "stitch": stitch,
                "tile_grid": tile_grid,
            }
        )
        with open(self.test_write_assets_parameterize_batch_path + test_name + ".json", 'w') as fp:
            json.dump(assets, fp, indent=2)

        if format_arg == "netCDF":
            extension = ".nc"
        else:
            extension = ".tif"
        assert len(assets) == 3
        if format_arg == "netCDF":
            if filename_prefix:
                assert assets[filename_prefix + "_0" + extension]
            else:
                assert assets["openEO_0" + extension]
        name, asset = next(iter(assets.items()))
        assert Path(asset['href']).parent == tmp_path
        if filename_prefix:
            assert filename_prefix in asset['href']
        assert asset['nodata'] == -1
        assert asset['roles'] == ['data']
        assert 2 == len(asset['bands'])
        if format_arg == "netCDF":
            assert 'application/x-netcdf' == asset['type']
        else:
            assert 'image/tiff; application=geotiff' == asset['type']

    test_write_assets_parameterize_path = "tmp/test_write_assets_parameterize/"
    shutil.rmtree(test_write_assets_parameterize_path, ignore_errors=True)
    os.makedirs(test_write_assets_parameterize_path)

    # Parameters found inside 'write_assets'. 768 cases, would be OK to reduce the combinations a bit.
    # Now runs for about 2 minutes
    @pytest.mark.parametrize("tiled", [True])
    @pytest.mark.parametrize("stitch", [True])
    @pytest.mark.parametrize("catalog", [True])
    @pytest.mark.parametrize("tile_grid", [None, "100km"])
    @pytest.mark.parametrize("sample_by_feature", [True, False])
    @pytest.mark.parametrize("batch_mode", [True, False])
    @pytest.mark.parametrize("filename_prefix", [None, "prefixTest"])
    @pytest.mark.parametrize("space_type", ["spacetime", "spatial"])
    @pytest.mark.parametrize("format_arg", ["NETCDF", "GTIFF", "PNG"])
    def test_write_assets_parameterize(self, tmp_path,
                                       tiled,
                                       stitch,
                                       catalog,
                                       tile_grid,
                                       sample_by_feature,
                                       batch_mode,
                                       filename_prefix,
                                       space_type,
                                       format_arg,
                                       ):
        d = locals()
        d = {i: d[i] for i in d if i != 'self' and i != "tmp_path" and i != "d"}
        test_name = "-".join(map(str, list(d.values())))  # a bit like how pytest names it
        if batch_mode and sample_by_feature:
            # 'sample_by_feature' is only relevant in 'batch_mode'
            return

        if space_type == "spacetime":
            input_layer = self.create_spacetime_layer()
        else:
            input_layer = self.create_spatial_layer()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.metadata = imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata = imagecollection.metadata.append_band(Band('band_two', '', ''))

        if format_arg == "NETCDF":
            extension = ".nc"
        elif format_arg == "PNG":
            extension = ".png"
        elif format_arg == "GTIFF":
            extension = ".tif"
        else:
            assert False
        filename = "test_download_result" + extension
        assets = imagecollection.write_assets(
            str(tmp_path / filename),
            format=format_arg,
            format_options={
                "tiled": tiled,
                "stitch": stitch,
                "parameters": {
                    "catalog": catalog,
                },
                "tile_grid": tile_grid,
                "sample_by_feature": sample_by_feature,
                "batch_mode": batch_mode,
                "filename_prefix": filename_prefix,  # no effect when outputting single file

                # non parametrized:
                "geometries": geojson_to_geometry(self.features),
                # "feature_id_property": 'id',  # not used
                # "multidate": True,  # not used
            }
        )
        # with open(self.test_write_assets_parameterize_path + test_name + ".json", 'w') as fp:
        #     json.dump(assets, fp, indent=2)

        name, asset = next(iter(assets.items()))
        print("href of first asset: " + asset['href'])

        # Special case for saveRDDTemporal as it only returns one case, but is designed to return multiple cases
        # But WHY is there only one file returned?
        saveRDDTemporalCase = format_arg == "GTIFF" and not catalog and not stitch and batch_mode \
                              and space_type == "spacetime" and not tile_grid and not sample_by_feature

        if len(assets) == 1 and not saveRDDTemporalCase:
            assert assets[filename]
            assert filename in asset['href']
        else:
            if filename_prefix:
                assert filename_prefix in asset['href']
            else:
                if (tile_grid and stitch and not catalog and format_arg == "GTIFF") \
                        or (tile_grid and not (batch_mode and space_type != "spatial") and not stitch and not catalog
                            and format_arg == "GTIFF"):
                    # special case for _save_stitched_tile_grid
                    assert "/test_download_result" in asset['href']
                else:
                    assert "/openEO" in asset['href']
        assert Path(asset['href']).parent == tmp_path

    #skipped because gdal_merge.py is not available on jenkins
    @skip
    def test_download_as_catalog(self):
        input_layer = self.create_spacetime_layer()

        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.save_result("catalogresult.tiff", format="GTIFF", format_options={"parameters": {"catalog": True}})
