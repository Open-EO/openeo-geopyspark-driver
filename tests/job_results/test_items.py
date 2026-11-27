import json

import pytest
from openeo_driver.errors import OpenEOApiException

from openeogeotrellis.job_results.items import (
    SaveResultFormatOptions,
    add_gdalinfo_objects,
    items_from_java,
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


class _FakeJavaItem:
    def __init__(self, stac_item: dict):
        self._json = json.dumps(stac_item)

    def toJson(self):
        return self._json


class TestItemsFromJava:
    def test_keyed_by_id(self):
        stac_item = {
            "id": "item1",
            "properties": {"datetime": "2020-01-01T00:00:00Z"},
            "assets": {"openEO": {"href": "/a.tif"}},
        }
        assert items_from_java([_FakeJavaItem(stac_item)]) == {"item1": stac_item}

    def test_multiple_items(self):
        items = [{"id": "a", "assets": {}}, {"id": "b", "assets": {}}]
        assert list(items_from_java(_FakeJavaItem(i) for i in items)) == ["a", "b"]


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
