import json
import os
import shutil
from pathlib import Path
from unittest import skip

import dirty_equals
import geopyspark as gps
import pytest
import xarray as xr
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
        res = imagecollection.write_assets(
            str(tmp_path / "ignored.tiff"),
            format=format,
            format_options={
                "multidate": True,
                "batch_mode": True,
                "geometries": geometries,
                "sample_by_feature": True,
                "feature_id_property": "id",
                "filename_prefix": "filenamePrefixTest",
            },
        )

        expected = {
            name: {
                "bands": [
                    dirty_equals.IsPartialDict(name="band_one"),
                    dirty_equals.IsPartialDict(name="band_two"),
                ],
                "bbox": dirty_equals.IsListOrTuple(length=4),
                "datetime": "2017-09-25T11:37:00Z",
                "geometry": dirty_equals.IsPartialDict(type="Polygon"),
                "href": str(tmp_path / name),
                "nodata": -1,
                "roles": ["data"],
                "type": "image/tiff; application=geotiff",
            }
            for name in [
                "filenamePrefixTest_2017-09-25Z_0.tif",
                "filenamePrefixTest_2017-09-25Z_1.tif",
                "filenamePrefixTest_2017-09-25Z_2.tif",
                "filenamePrefixTest_2017-09-25Z_3.tif",
                "filenamePrefixTest_2017-09-25Z_4.tif",
            ]
        }
        assert res == expected

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

    @pytest.mark.parametrize("filename_prefix", [None, "prefixTest"])
    @pytest.mark.parametrize("tile_grid", [None, "100km"])
    @pytest.mark.parametrize("space_type", ["spacetime", "spatial"])
    @pytest.mark.parametrize("stitch", [False, True])
    @pytest.mark.parametrize("catalog", [False, True])
    @pytest.mark.parametrize("sample_by_feature", [False, True])
    @pytest.mark.parametrize("format_arg", ["netCDF"])  # "GTIFF" behaves different from "netCDF", so not testing now
    @pytest.mark.parametrize("bands_metadata", [{},"s","so"])
    def test_write_assets_parameterize_batch(self, tmp_path, imagecollection_with_two_bands_and_three_dates,
                                             imagecollection_with_two_bands_spatial_only,
                                             format_arg, sample_by_feature, catalog, stitch, space_type,
                                             tile_grid, filename_prefix,bands_metadata):
        if space_type == "spacetime":
            imagecollection = imagecollection_with_two_bands_and_three_dates
        else:
            imagecollection = imagecollection_with_two_bands_spatial_only

        if bands_metadata == "s":
            bands_metadata = {"red":{"SCALE":1.23}}
        elif bands_metadata == "so":
            bands_metadata = {"red": {"SCALE": 1.23,"OFFSET":4.56}}
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
                "bands_metadata": bands_metadata
            }
        )

        if format_arg.lower() in {"netcdf"}:
            expected_extension, expected_type = ".nc", "application/x-netcdf"
        elif format_arg.lower() in {"gtiff"}:
            expected_extension, expected_type = ".tif", "image/tiff; application=geotiff"
        else:
            raise ValueError(format_arg)

        expected_filenames = [f"{filename_prefix or 'openEO'}_{i}{expected_extension}" for i in range(5)]
        expected = {
            name: {
                "bands": [
                    dirty_equals.IsPartialDict(name="red"),
                    dirty_equals.IsPartialDict(name="nir"),
                ],
                "bbox": dirty_equals.IsListOrTuple(length=4),
                "geometry": dirty_equals.IsPartialDict(type="Polygon"),
                "href": str(tmp_path / name),
                "nodata": -1,
                "roles": ["data"],
                "type": expected_type,
            }
            for name in expected_filenames
        }
        assert assets == expected

    # Parameters found inside 'write_assets'. If all parameters are tested: 768 cases that take 2min to run.
    @pytest.mark.parametrize("tiled", [True])  # Specify [True, False] to run more tests
    @pytest.mark.parametrize("stitch", [True])  # Specify [True, False] to run more tests
    @pytest.mark.parametrize("catalog", [True, False])
    @pytest.mark.parametrize("tile_grid", [None, "100km"])
    @pytest.mark.parametrize("sample_by_feature", [True, False])
    @pytest.mark.parametrize("batch_mode", [True, False])
    @pytest.mark.parametrize("filename_prefix", [None, "prefixTest"])
    @pytest.mark.parametrize("attach_gdalinfo_assets", [True, False])
    @pytest.mark.parametrize("space_type", ["spacetime", "spatial"])
    @pytest.mark.parametrize("format_arg", ["NETCDF", "GTIFF", "PNG"])
    def test_write_assets_parameterize(self, tmp_path, imagecollection_with_two_bands_and_three_dates,
                                       imagecollection_with_two_bands_spatial_only,
                                       tiled,
                                       stitch,
                                       catalog,
                                       tile_grid,
                                       sample_by_feature,
                                       batch_mode,
                                       filename_prefix,
                                       attach_gdalinfo_assets,
                                       space_type,
                                       format_arg,
                                       ):
        if batch_mode and sample_by_feature:
            # 'sample_by_feature' is only relevant in 'batch_mode'
            return
        if attach_gdalinfo_assets and format_arg != "GTIFF":
            # 'attach_gdalinfo_assets' is only relevant when outputting 'GTIFF'
            return

        if space_type == "spacetime":
            imagecollection = imagecollection_with_two_bands_and_three_dates
        else:
            imagecollection = imagecollection_with_two_bands_spatial_only

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
        assets_all = imagecollection.write_assets(
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
                "attach_gdalinfo_assets": attach_gdalinfo_assets,

                # non parametrized:
                "geometries": geometries,
                # "feature_id_property": 'id',  # not used
                # "multidate": True,  # not used
                "ZLEVEL": 6,
            }
        )

        assets_data = {k: v for (k, v) in assets_all.items() if "data" in v["roles"]}
        name, asset = next(iter(assets_data.items()))
        print("href of first asset: " + asset["href"])
        assets_metadata = {k: v for (k, v) in assets_all.items() if "data" not in v["roles"]}
        if format_arg == "GTIFF" and not catalog:
            if attach_gdalinfo_assets:
                assert len(assets_metadata) == len(assets_data)
            else:
                assert len(assets_metadata) == 0

        if len(assets_data) == 1:
            assert assets_data[filename]
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

    @pytest.mark.parametrize("to_zip", [True, False])
    def test_zarr(self,tmp_path,to_zip):
        input_layer = layer_with_two_bands_and_one_date()
        imagecollection = GeopysparkDataCube(pyramid=gps.Pyramid({0: input_layer}))
        imagecollection.metadata = imagecollection.metadata.add_dimension('band_one', 'band_one', 'bands')
        imagecollection.metadata = imagecollection.metadata.append_band(Band('band_two', '', ''))
        file_path = str(tmp_path / "res.zarr")
        zarr_options = {"toZip":to_zip}
        imagecollection.write_assets(file_path, format="ZARR",format_options=zarr_options)
        ds = xr.open_zarr(file_path)
        assert len(ds.variables) == 5
        assert "band_one" in ds.variables
        band1 = ds.band_one
        assert len(band1.dims) == 3
        assert band1.dims == ("time","y","x")
        assert len(band1.attrs) == 2
        assert "_CRS" in band1.attrs
        assert "wkt" in band1.attrs.get("_CRS")
        assert "extent" in band1.attrs
        assert len(band1.extent) == 2
        assert "spatial" in band1.extent
        assert band1.extent.get("spatial").get("bbox") == [[0.0,0.0,4.0,4.0]]
        assert "temporal" in band1.extent
        assert band1.extent.get("temporal").get("interval") == [['2017-09-25T11:37:00Z', '2017-09-25T11:37:00Z']]

        assert "band_two" in ds.variables
        band2 = ds.band_two
        assert len(band2.dims) == 3
        assert band2.dims == ("time","y","x")
        assert len(band2.attrs) == 2
        assert "_CRS" in band2.attrs
        assert "wkt" in band2.attrs.get("_CRS")
        assert "extent" in band2.attrs
        assert len(band2.extent) == 2
        assert "spatial" in band2.extent
        assert band2.extent.get("spatial").get("bbox") == [[0.0,0.0,4.0,4.0]]
        assert "temporal" in band2.extent
        assert band2.extent.get("temporal").get("interval") == [['2017-09-25T11:37:00Z', '2017-09-25T11:37:00Z']]

        assert "x" in ds.variables
        x = ds.x
        assert x.dims == ("x",)
        assert "y" in ds.variables
        y = ds.y
        assert y.dims == ("y",)
        assert "time" in ds.variables
        time = ds.time
        assert time.dims == ("time",)

        file_path_netcdf = str(tmp_path / "res.nc")
        imagecollection.write_assets(file_path_netcdf,format="NETCDF")
        ds_netcdf = xr.open_dataset(file_path_netcdf)
        assert (ds_netcdf.t.data == ds.time.values).all()
        assert (ds_netcdf.x.data == ds.x.values).all()
        assert (ds_netcdf.y.data == ds.y.values).all()
        assert (ds_netcdf.band_one.data==band1.data[0]).all()
        assert (ds_netcdf.band_two.data == band2.data[0]).all()

        if to_zip:
            Path(file_path + ".zip").exists()
