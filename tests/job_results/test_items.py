import pytest
from openeo_driver.errors import OpenEOApiException

from openeogeotrellis.job_results.items import (
    SaveResultFormatOptions,
    Variant,
    WrittenAsset,
    WrittenItem,
    add_gdalinfo_objects,
    build_items,
    select_variant,
    single_asset_item,
)


class TestSaveResultFormatOptionsParse:
    def test_defaults(self):
        options = SaveResultFormatOptions.parse("gtiff", None, has_temporal_dimension=False)
        assert options.format == "GTIFF"
        assert options.stitch is False
        assert options.tile_grid is None
        assert options.separate_asset_per_band is False
        assert options.bands_metadata == {}
        assert options.to_zip is True

    def test_unsupported_format(self):
        with pytest.raises(OpenEOApiException, match="Format 'FOO' is not supported"):
            SaveResultFormatOptions.parse("foo", {}, has_temporal_dimension=False)

    def test_attach_gdalinfo_assets_requires_gtiff(self):
        with pytest.raises(
            OpenEOApiException, match="attach_gdalinfo_assets is only supported with format GTIFF. Was: NETCDF"
        ):
            SaveResultFormatOptions.parse("netcdf", {"attach_gdalinfo_assets": True}, has_temporal_dimension=False)

    def test_separate_asset_per_band_requires_gtiff(self):
        with pytest.raises(
            OpenEOApiException, match="separate_asset_per_band is only supported with format GTIFF. Was: NETCDF"
        ):
            SaveResultFormatOptions.parse("netcdf", {"separate_asset_per_band": "true"}, has_temporal_dimension=False)

    def test_separate_asset_per_band_accepts_string_bool(self):
        options = SaveResultFormatOptions.parse("gtiff", {"separate_asset_per_band": "true"}, has_temporal_dimension=False)
        assert options.separate_asset_per_band is True

    def test_filepath_per_band_with_temporal_dimension(self):
        with pytest.raises(OpenEOApiException, match="filepath_per_band is not supported with temporal dimension"):
            SaveResultFormatOptions.parse(
                "gtiff", {"filepath_per_band": ["a.tif"]}, has_temporal_dimension=True
            )

    def test_filepath_per_band_with_temporal_dimension_ignored_when_stitching(self):
        # Quirk: filepath_per_band is only read by the non-stitch GTIFF writer, so
        # combining it with stitch=True never raised, even with a temporal dimension.
        options = SaveResultFormatOptions.parse(
            "gtiff", {"filepath_per_band": ["a.tif"], "stitch": True}, has_temporal_dimension=True
        )
        assert options.filepath_per_band == ["a.tif"]

    def test_tile_grid_with_separate_asset_per_band(self):
        with pytest.raises(OpenEOApiException, match="separate_asset_per_band is not supported with tile_grid"):
            SaveResultFormatOptions.parse(
                "gtiff",
                {"tile_grid": "wgs84-1degree", "separate_asset_per_band": "true"},
                has_temporal_dimension=False,
            )

    def test_tile_grid_with_separate_asset_per_band_ignored_when_stitching(self):
        # Quirk: the stitch+tile_grid writer never reads separate_asset_per_band, so this
        # combination silently proceeded (and still does).
        options = SaveResultFormatOptions.parse(
            "gtiff",
            {"tile_grid": "wgs84-1degree", "separate_asset_per_band": "true", "stitch": True},
            has_temporal_dimension=False,
        )
        assert options.separate_asset_per_band is True

    def test_validate_sample_by_feature_with_separate_asset_per_band(self):
        options = SaveResultFormatOptions.parse("gtiff", {"separate_asset_per_band": "true"}, has_temporal_dimension=False)
        with pytest.raises(OpenEOApiException, match="separate_asset_per_band is not supported with sample_by_feature"):
            options.validate_sample_by_feature_with_separate_asset_per_band()

    def test_validate_sample_by_feature_with_separate_asset_per_band_ok(self):
        options = SaveResultFormatOptions.parse("gtiff", {}, has_temporal_dimension=False)
        options.validate_sample_by_feature_with_separate_asset_per_band()  # does not raise


class TestSelectVariant:
    def test_stitch_wins_regardless_of_other_flags(self):
        assert (
            select_variant(stitch=True, tile_grid="g", batch_mode=True, is_temporal_layer=True, sample_by_feature=True)
            == Variant.STITCH
        )

    def test_batch_temporal(self):
        assert (
            select_variant(stitch=False, tile_grid=None, batch_mode=True, is_temporal_layer=True, sample_by_feature=False)
            == Variant.BATCH
        )

    def test_batch_spatial_sample_by_feature(self):
        assert (
            select_variant(
                stitch=False, tile_grid=None, batch_mode=True, is_temporal_layer=False, sample_by_feature=True
            )
            == Variant.BATCH
        )

    def test_plain_when_not_batch(self):
        assert (
            select_variant(
                stitch=False, tile_grid="g", batch_mode=False, is_temporal_layer=False, sample_by_feature=True
            )
            == Variant.PLAIN
        )

    def test_plain_when_batch_spatial_without_sample_by_feature(self):
        assert (
            select_variant(
                stitch=False, tile_grid=None, batch_mode=True, is_temporal_layer=False, sample_by_feature=False
            )
            == Variant.PLAIN
        )


def _item(**kwargs):
    kwargs.setdefault("id", "item1")
    kwargs.setdefault("bbox", (10.0, 40.0, 11.0, 41.0))
    kwargs.setdefault("crs", "EPSG:4326")
    return WrittenItem(**kwargs)


class TestBuildItemsStitch:
    def test_stitch_item_shape(self):
        item = _item(
            datetime="2020-01-01T00:00:00Z",
            assets=[WrittenAsset(key="openEO", path="/a.tif", proj_bbox=(1, 2, 3, 4), proj_shape=(5, 6), proj_epsg=4326)],
        )
        items = build_items([item], variant=Variant.STITCH)
        result = items["item1"]
        assert result["properties"] == {"datetime": "2020-01-01T00:00:00Z"}
        asset = result["assets"]["openEO"]
        assert asset["href"] == "/a.tif"
        assert asset["type"] == "image/tiff; application=geotiff"
        assert asset["proj:epsg"] == 4326
        assert "bands" not in asset

    def test_stitch_item_proj_absent_when_none(self):
        item = _item(assets=[WrittenAsset(key="openEO", path="/a.tif")])
        result = build_items([item], variant=Variant.STITCH)["item1"]
        asset = result["assets"]["openEO"]
        assert "proj:bbox" not in asset
        assert "proj:epsg" not in asset


class TestBuildItemsBatch:
    def test_band_indices_truthy_filters_bands(self):
        bands = [{"name": "B01"}, {"name": "B02"}, {"name": "B03"}]
        item = _item(
            datetime="2020-01-01T00:00:00Z",
            assets=[WrittenAsset(key="openEO", path="/a.tif", band_indices=[0, 2])],
        )
        result = build_items([item], variant=Variant.BATCH, bands=bands, nodata=0)["item1"]
        asset = result["assets"]["openEO"]
        assert asset["bands"] == [{"name": "B01"}, {"name": "B03"}]
        assert asset["nodata"] == 0
        assert asset["datetime"] == "2020-01-01T00:00:00Z"

    def test_band_indices_empty_falls_back_to_all_bands(self):
        # Quirk: an empty (but non-None) band_indices list is falsy, so BATCH includes all bands.
        bands = [{"name": "B01"}, {"name": "B02"}]
        item = _item(assets=[WrittenAsset(key="openEO", path="/a.tif", band_indices=[])])
        result = build_items([item], variant=Variant.BATCH, bands=bands, nodata=None)["item1"]
        assert result["assets"]["openEO"]["bands"] == bands

    def test_band_indices_none_uses_all_bands(self):
        bands = [{"name": "B01"}]
        item = _item(assets=[WrittenAsset(key="openEO", path="/a.tif", band_indices=None)])
        result = build_items([item], variant=Variant.BATCH, bands=bands, nodata=None)["item1"]
        assert result["assets"]["openEO"]["bands"] == bands


class TestBuildItemsPlain:
    def test_band_indices_none_omits_bands_key(self):
        item = _item(assets=[WrittenAsset(key="openEO", path="/a.tif", band_indices=None)])
        result = build_items([item], variant=Variant.PLAIN, bands=[{"name": "B01"}], nodata=None)["item1"]
        assert "bands" not in result["assets"]["openEO"]
        assert "properties" not in result

    def test_band_indices_empty_includes_empty_bands_list(self):
        # Quirk: unlike BATCH, PLAIN distinguishes "None" (omit) from "empty" (include as []).
        item = _item(assets=[WrittenAsset(key="openEO", path="/a.tif", band_indices=[])])
        result = build_items([item], variant=Variant.PLAIN, bands=[{"name": "B01"}], nodata=None)["item1"]
        assert result["assets"]["openEO"]["bands"] == []

    def test_geometry_and_bbox_always_present(self):
        item = _item(assets=[WrittenAsset(key="openEO", path="/a.tif")])
        result = build_items([item], variant=Variant.PLAIN, bands=[], nodata=None)["item1"]
        asset = result["assets"]["openEO"]
        assert asset["geometry"]["type"] == "Polygon"
        assert len(asset["bbox"]) == 4


class TestBuildItemsNetcdf:
    def test_netcdf_item_shape(self):
        item = _item(
            assets=[
                WrittenAsset(
                    key="openEO",
                    path="/a.nc",
                    nc_bands=[{"name": "B01", "statistics": {"minimum": 0}}],
                    proj_epsg=4326,
                )
            ]
        )
        result = build_items([item], variant=Variant.NETCDF, nodata=-1)["item1"]
        assert "properties" not in result
        asset = result["assets"]["openEO"]
        assert asset["bands"] == asset["raster:bands"] == [{"name": "B01", "statistics": {"minimum": 0}}]
        assert asset["nodata"] == -1
        assert asset["proj:epsg"] == 4326

    def test_netcdf_item_without_bbox_has_no_geometry(self):
        item = _item(bbox=None, assets=[WrittenAsset(key="openEO", path="/a.nc")])
        result = build_items([item], variant=Variant.NETCDF, nodata=None)["item1"]
        assert result["geometry"] is None
        assert result["bbox"] is None
        # dict_no_none strips the per-asset geometry/bbox keys entirely when absent.
        assert "geometry" not in result["assets"]["openEO"]
        assert "bbox" not in result["assets"]["openEO"]


class TestSingleAssetItem:
    def test_basic_shape(self):
        result = single_asset_item(asset_key="openEO", asset={"href": "/a.png", "type": "image/png", "roles": ["data"]})
        (item_id, item), = result.items()
        assert item["id"] == item_id
        assert item["assets"] == {"openEO": {"href": "/a.png", "type": "image/png", "roles": ["data"]}}
        assert "properties" not in item

    def test_item_extra_merged_in(self):
        result = single_asset_item(
            asset_key="openEO",
            asset={"href": "/a.json", "roles": ["data"]},
            item_extra={"properties": {"datetime": None}, "geometry": None, "bbox": None},
        )
        (_, item), = result.items()
        assert item["properties"] == {"datetime": None}
        assert item["geometry"] is None
        assert item["bbox"] is None

    def test_ids_are_unique(self):
        a = single_asset_item(asset_key="openEO", asset={"href": "/a"})
        b = single_asset_item(asset_key="openEO", asset={"href": "/b"})
        assert list(a.keys()) != list(b.keys())


class TestAddGdalinfoObjects:
    def test_disabled_returns_unchanged(self, tmp_path):
        assets = {"openEO": {"href": str(tmp_path / "a.tif")}}
        assert add_gdalinfo_objects(assets, attach_gdalinfo_assets=False, save_directory=str(tmp_path)) is assets

    def test_adds_gdalinfo_asset_when_file_exists(self, tmp_path):
        tif_path = tmp_path / "a.tif"
        gdalinfo_path = tmp_path / "a.tif_gdalinfo.json"
        gdalinfo_path.write_text("{}")
        assets = {"openEO": {"href": str(tif_path), "bbox": [0, 0, 1, 1], "geometry": {"type": "Point"}, "datetime": "x"}}

        result = add_gdalinfo_objects(assets, attach_gdalinfo_assets=True, save_directory=str(tmp_path))

        assert "openEO" in result
        gdalinfo_key = "a.tif_gdalinfo.json"
        assert gdalinfo_key in result
        assert result[gdalinfo_key]["href"] == str(gdalinfo_path)
        assert result[gdalinfo_key]["type"] == "application/json"
        assert result[gdalinfo_key]["bbox"] == [0, 0, 1, 1]
        assert result[gdalinfo_key]["geometry"] == {"type": "Point"}
        assert result[gdalinfo_key]["datetime"] == "x"

    def test_no_gdalinfo_asset_when_file_missing(self, tmp_path):
        assets = {"openEO": {"href": str(tmp_path / "missing.tif")}}
        result = add_gdalinfo_objects(assets, attach_gdalinfo_assets=True, save_directory=str(tmp_path))
        assert result == assets
