import json
import os
import shutil
from pathlib import Path
from unittest import skip

import geopyspark as gps
import pytest
from openeo.metadata import Band
from openeo_driver.util.geometry import geojson_to_geometry
from shapely import geometry

from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from .data import get_test_data_file
from .datacube_fixtures import layer_with_two_bands_and_one_date


class TestDownload:
    def download_no_bands(self, tmp_path, format):
        input_layer = layer_with_two_bands_and_one_date()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        res = imagecollection.save_result(str(tmp_path / "test_download_result.") + format, format=format)
        print(res)

    def download_no_args(self, tmp_path, format, format_options={}):
        input_layer = layer_with_two_bands_and_one_date()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.metadata=imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata=imagecollection.metadata.append_band(Band('band_two','',''))

        res = imagecollection.save_result(str(tmp_path / "test_download_result.") + format, format=format, format_options=format_options)
        print(res)
        return res
        #TODO how can we verify downloaded geotiffs, preferably without introducing a dependency on another library.

    def download_no_args_single_byte_band(self, tmp_path, format, format_options={}):
        input_layer = layer_with_two_bands_and_one_date().convert_data_type(gps.CellType.UINT8)
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.metadata=imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata=imagecollection.metadata.append_band(Band('band_two','',''))
        imagecollection.filter_bands(['band_one'])


        res = imagecollection.save_result(str(tmp_path / "test_download_result_single_band.") + format, format=format, format_options=format_options)
        print(res)
        return res

    def download_masked(self, tmp_path, format):
        input_layer = layer_with_two_bands_and_one_date()
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
        input_layer = layer_with_two_bands_and_one_date()
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
        input_layer = layer_with_two_bands_and_one_date()
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
        input_layer = layer_with_two_bands_and_one_date()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.metadata = imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata = imagecollection.metadata.append_band(Band('band_two', '', ''))
        format = 'GTiff'

        geometries = geojson_to_geometry(self.features)
        res = imagecollection.write_assets(str(tmp_path / "ignored.tiff"), format=format, format_options={
            "multidate": True,
            "batch_mode": True,
            "geometries": geometries,
            "sample_by_feature": True,
            "feature_id_property": 'id',
            "filename_prefix": "filenamePrefixTest",
        })
        assert len(res) >= 3
        assert len(res) <= geometries.length
        name, asset = next(iter(res.items()))
        assert Path(asset['href']).parent == tmp_path
        assert asset['nodata'] == -1
        assert asset['roles'] == ['data']
        assert 2 == len(asset['bands'])
        assert "filenamePrefixTest" in asset['href']
        assert 'image/tiff; application=geotiff' == asset['type']
        assert asset['datetime'] == "2017-09-25T11:37:00Z"

    def test_write_assets_samples_tile_grid(self, tmp_path):
        input_layer = layer_with_two_bands_and_one_date()
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
        input_layer = layer_with_two_bands_and_one_date()
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
    def test_write_assets_parameterize_batch(self, tmp_path, imagecollection_with_two_bands_and_three_dates,
                                             imagecollection_with_two_bands_spatial_only,
                                             format_arg, sample_by_feature, catalog, stitch, space_type,
                                             tile_grid, filename_prefix):
        d = locals()
        d = {i: d[i] for i in d if i != 'self' and i != "tmp_path" and i != "d"}
        test_name = "-".join(map(str, list(d.values())))  # a bit like how pytest names it

        if space_type == "spacetime":
            imagecollection = imagecollection_with_two_bands_and_three_dates
        else:
            imagecollection = imagecollection_with_two_bands_spatial_only

        geometries = geojson_to_geometry(self.features)
        assets = imagecollection.write_assets(
            str(tmp_path / "ignored<\0>.extension"),  # null byte to cause error if filename would be written to fs
            format=format_arg,
            format_options={
                "batch_mode": True,
                "geometries": geometries,
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
        assert len(assets) >= 3
        assert len(assets) <= geometries.length
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

    # Parameters found inside 'write_assets'. If all parameters are tested: 768 cases that take 2min to run.
    @pytest.mark.parametrize("tiled", [True, False])
    @pytest.mark.parametrize("stitch", [True, False])
    @pytest.mark.parametrize("catalog", [True, False])
    @pytest.mark.parametrize("useColorMap", [True])
    @pytest.mark.parametrize("tile_grid", [None, "100km"])
    @pytest.mark.parametrize("sample_by_feature", [True, False])
    @pytest.mark.parametrize("batch_mode", [True, False])
    @pytest.mark.parametrize("filename_prefix", [None, "prefixTest"])
    @pytest.mark.parametrize("space_type", ["spacetime", "spatial"])
    @pytest.mark.parametrize("format_arg", ["NETCDF", "GTIFF", "PNG"])
    def test_write_assets_parameterize(self, tmp_path, imagecollection_with_two_bands_and_three_dates,
                                       imagecollection_with_two_bands_spatial_only,
                                       tiled,
                                       stitch,
                                       catalog,
                                       useColorMap,
                                       tile_grid,
                                       sample_by_feature,
                                       batch_mode,
                                       filename_prefix,
                                       space_type,
                                       format_arg,
                                       ):
        d = locals()
        d = {i: d[i] for i in d if i != 'self' and i != "tmp_path" and i != "d"}
        print(d)
        test_name = "-".join(map(str, list(d.values())))  # a bit like how pytest names it
        if batch_mode and sample_by_feature:
            # 'sample_by_feature' is only relevant in 'batch_mode'
            return

        if space_type == "spacetime":
            imagecollection = imagecollection_with_two_bands_and_three_dates
        else:
            imagecollection = imagecollection_with_two_bands_spatial_only

        colormap = None
        if useColorMap:
            from matplotlib.colors import ListedColormap
            col_palette = [
                [174, 199, 232, 255],
                [214, 39, 40, 255],
                [247, 182, 210, 255],
                [219, 219, 141, 255],
                [199, 199, 199, 255]

            ]
            cmap = ListedColormap(col_palette)
            colormap = {x: [c / 255.0 for c in cmap(x)] for x in range(0, len(col_palette))}

        if format_arg == "NETCDF":
            extension = ".nc"
        elif format_arg == "PNG":
            extension = ".png"
        elif format_arg == "GTIFF":
            extension = ".tif"
        else:
            assert False
        filename = "test_download_result" + extension
        geometries = geojson_to_geometry(self.features)
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
                "geometries": geometries,
                # "feature_id_property": 'id',  # not used
                # "multidate": True,  # not used
                "colormap": colormap,
            }
        )
        # with open(self.test_write_assets_parameterize_path + test_name + ".json", 'w') as fp:
        #     json.dump(assets, fp, indent=2)

        name, asset = next(iter(assets.items()))
        print("href of first asset: " + asset['href'])

        if len(assets) == 1:
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
        input_layer = layer_with_two_bands_and_one_date()

        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.save_result("catalogresult.tiff", format="GTIFF", format_options={"parameters": {"catalog": True}})
