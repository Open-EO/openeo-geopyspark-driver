"""
Focused unit tests for `openeogeotrellis.stac.asset_table`, exercised through
its own public surface (no JVM/GeoPySpark dependency), independent of
`test_load_stac.py`'s end-to-end JVM-backed checks.
"""
import pystac
import pytest
from openeo.testing.stac import StacDummyBuilder
from openeo_driver.util.geometry import BoundingBox

from openeogeotrellis.stac.asset_table import (
    PixelValueScalingMode,
    ResolutionTracker,
    _get_pixel_value_scale_and_offset,
    _get_raster_scale_and_offset,
    _is_sentinel2_reflectance_asset,
    build_asset_table,
    get_pixel_value_scaling_mode,
)
from openeogeotrellis.stac.extents import SpatioTemporalExtent
from openeogeotrellis.stac.item_collection import ItemCollection


def _item(*, id="item1", bbox=(3, 45, 3.1, 45.1), properties=None, assets):
    west, south, east, north = bbox
    geometry = {
        "type": "Polygon",
        "coordinates": [[[west, south], [east, south], [east, north], [west, north], [west, south]]],
    }
    return pystac.Item.from_dict(
        StacDummyBuilder.item(id=id, bbox=list(bbox), geometry=geometry, properties=properties, assets=assets)
    )


def _build(item, *, band_selection=None, available_band_names=None, feature_flags=None, **kwargs):
    return build_asset_table(
        item_collection=ItemCollection(items=[item] if isinstance(item, pystac.Item) else item),
        band_selection=band_selection,
        available_band_names=available_band_names if available_band_names is not None else [],
        spatiotemporal_extent=kwargs.pop("spatiotemporal_extent", SpatioTemporalExtent()),
        pixel_value_scaling_mode=kwargs.pop("pixel_value_scaling_mode", PixelValueScalingMode.NO_SCALING),
        use_raw_asset_href=kwargs.pop("use_raw_asset_href", False),
        feature_flags=feature_flags or {},
    )


class TestResolutionTracker:
    def test_empty(self):
        tracker = ResolutionTracker()
        assert tracker.finest_for() == (set(), None)

    def test_no_keys(self):
        tracker = ResolutionTracker()
        tracker.track(epsg=4326, res=(1, 2))
        assert tracker.finest_for() == ({4326}, (1, 2))

    def test_basic(self):
        tracker = ResolutionTracker()
        tracker.track(key="B01", epsg=4326, res=(0.1, 0.1))
        tracker.track(key="B01", epsg=4326, res=(0.2, 0.2))
        tracker.track(key="B02", epsg=4326, res=(0.3, 0.3))

        assert tracker.finest_for(["B01"]) == ({4326}, (0.1, 0.1))
        assert tracker.finest_for(["B02"]) == ({4326}, (0.3, 0.3))
        assert tracker.finest_for(["B01", "B02"]) == ({4326}, (0.1, 0.1))
        assert tracker.finest_for(["B03"]) == (set(), None)
        assert tracker.finest_for(["B01", "B03"]) == ({4326}, (0.1, 0.1))

    def test_multi_epsg(self):
        tracker = ResolutionTracker()
        tracker.track(key="B01", epsg=4326, res=(0.1, 0.1))
        tracker.track(key="B01", epsg=32631, res=(10, 10))

        assert tracker.finest_for(["B01"]) == ({4326, 32631}, None)

    def test_multi_utm(self):
        tracker = ResolutionTracker()
        tracker.track(key="B01", epsg=32629, res=(10, 10))
        tracker.track(key="B01", epsg=32631, res=(10, 10))
        tracker.track(key="B02", epsg=32631, res=(20, 20))

        assert tracker.finest_for(["B01"]) == ({32629, 32631}, (10, 10))
        assert tracker.finest_for(["B02"]) == (
            {
                32631,
            },
            (20, 20),
        )
        assert tracker.finest_for(["B01", "B02"]) == (
            {
                32629,
                32631,
            },
            (10, 10),
        )


@pytest.mark.parametrize(
    ["feature_flags", "url", "expected"],
    [
        (
            {},
            "https://stac.test/foo",
            PixelValueScalingMode.NO_SCALING,
        ),
        (
            {"apply_sentinel2_reflectance_offset": False},
            "https://stac.test/foo",
            PixelValueScalingMode.NO_SCALING,
        ),
        (
            {"apply_sentinel2_reflectance_offset": True},
            "https://stac.test/foo",
            PixelValueScalingMode.S2_REFLECTANCE_SCALED_OFFSET,
        ),
        (
            {},
            "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-2-l2a",
            PixelValueScalingMode.S2_REFLECTANCE_SCALED_OFFSET,
        ),
        (
            {},
            "https://stac.terrascope.be/collections/terrascope-s2-toc-v2",
            PixelValueScalingMode.S2_REFLECTANCE_SCALED_OFFSET,
        ),
        (
            {"apply_raster_scale_and_offset": True},
            "https://stac.test/foo",
            PixelValueScalingMode.SCALE_AND_OFFSET,
        ),
    ],
)
def test_get_pixel_value_scaling_mode(feature_flags, url, expected):
    apply_sentinel2_reflectance_offset = get_pixel_value_scaling_mode(feature_flags=feature_flags, url=url)
    assert apply_sentinel2_reflectance_offset == expected


@pytest.mark.parametrize(
    ["item", "asset", "expected"],
    [
        (
            pystac.Item.from_dict(StacDummyBuilder.item()),
            pystac.Asset(href="https://stac.test/asset.tiff"),
            (1, 0),
        ),
        (
            pystac.Item.from_dict(StacDummyBuilder.item(properties={"raster:scale": 1.2, "raster:offset": 3.4})),
            pystac.Asset(href="https://stac.test/asset.tiff"),
            (1.2, 3.4),
        ),
        (
            pystac.Item.from_dict(StacDummyBuilder.item()),
            pystac.Asset(
                href="https://stac.test/asset.tiff", extra_fields={"raster:scale": 1.2, "raster:offset": 3.4}
            ),
            (1.2, 3.4),
        ),
    ],
)
def test_get_raster_scale_and_offset(item, asset, expected):
    assert _get_raster_scale_and_offset(item=item, asset=asset) == expected


@pytest.mark.parametrize(
    ["mode", "with_reflectance_band", "expected"],
    [
        (PixelValueScalingMode.NO_SCALING, False, (1, 0)),
        (PixelValueScalingMode.S2_REFLECTANCE_SCALED_OFFSET, False, (1, 0)),
        (PixelValueScalingMode.S2_REFLECTANCE_SCALED_OFFSET, True, (1, 2.8)),
        (PixelValueScalingMode.SCALE_AND_OFFSET, False, (1.25, 3.5)),
    ],
)
def test_get_pixel_value_scale_and_offset(mode, with_reflectance_band, expected):
    item = pystac.Item.from_dict(StacDummyBuilder.item())
    asset_properties = {"raster:scale": 1.25, "raster:offset": 3.5}
    if with_reflectance_band:
        asset_properties["bands"] = [{"name": "B02", "eo:center_wavelength": 0.493}]
    asset = pystac.Asset(
        href="https://stac.test/asset.tiff",
        extra_fields=asset_properties,
    )
    assert _get_pixel_value_scale_and_offset(item=item, asset=asset, pixel_value_scaling_mode=mode) == expected


@pytest.mark.parametrize(
    ["asset", "expected"],
    [
        (pystac.Asset(href="https://stac.test/B02.tiff"), False),
        (
            # Minimal match
            pystac.Asset(
                href="https://stac.test/B02.tiff",
                extra_fields={"bands": [{"eo:center_wavelength": 0.475}]},
            ),
            True,
        ),
        (
            # Old style (eo:bands > center_wavelength)
            pystac.Asset(
                href="https://stac.test/B02.tiff",
                extra_fields={"eo:bands": [{"center_wavelength": 0.475}]},
            ),
            True,
        ),
        (
            # Based on B02 on CDSE https://stac.dataspace.copernicus.eu/v1/collections/sentinel-2-l2a
            pystac.Asset.from_dict(
                {
                    "href": "s3://test/Sentinel-2/2025/09/01/T31UES_20250901T105041_B02_20m.jp2",
                    "bands": [
                        {
                            "name": "B02",
                            "eo:center_wavelength": 0.493,
                            "eo:full_width_half_max": 0.267,
                            "eo:common_name": "blue",
                        }
                    ],
                    "type": "image/jp2",
                    "roles": ["data", "reflectance", "sampling:downsampled", "gsd:20m"],
                    "raster:scale": 0.0001,
                    "raster:offset": -0.1,
                }
            ),
            True,
        ),
        (
            # Based on WVP on CDSE https://stac.dataspace.copernicus.eu/v1/collections/sentinel-2-l2a
            pystac.Asset.from_dict(
                {
                    "href": "s3://test/Sentinel-2/MSI/L2A/2025/09/01/T31UES_20250901T105041_WVP_10m.jp2",
                    "roles": ["data", "gsd:10m"],
                    "title": "Water vapour (WVP) - 10m",
                    "raster:scale": 0.0001,
                    "raster:offset": -0.1,
                }
            ),
            False,
        ),
        (
            # Based on B02 on Terrascope https://stac.terrascope.be/collections/terrascope-s2-toc-v2
            pystac.Asset.from_dict(
                {
                    "href": "https://terrascope.test/dl/Sentinel2/TOC_V2/2025/09/01/S2C_20250901T105041_31UES_TOC-B02_10M_V210.tif",
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "title": "B02",
                    "roles": ["data"],
                    "raster:scale": 0.0001,
                    "raster:offset": 0.0,
                    "bands": [
                        {
                            "name": "B02",
                            "eo:common_name": "blue",
                            "eo:center_wavelength": 0.49,
                            "eo:full_width_half_max": 0.098,
                        }
                    ],
                }
            ),
            True,
        ),
    ],
)
def test_is_sentinel2_reflectance_asset(asset, expected):
    assert _is_sentinel2_reflectance_asset(asset) == expected


class TestBuildAssetTable:
    def test_band_selection_none_falls_back_to_available_band_names(self):
        item = _item(assets={"B02": {"href": "https://stac.test/B02.tif", "roles": ["data"], "bands": [{"name": "B02"}]}})
        asset_table = _build(item, band_selection=None, available_band_names=["B02"])
        assert [l.band_names for l in asset_table.items[0].links] == [["B02"]]
        assert asset_table.asset_band_names == ["B02"]

    def test_band_selection_explicit_selects_only_named_bands(self):
        item = _item(
            assets={
                "B02": {"href": "https://stac.test/B02.tif", "roles": ["data"], "bands": [{"name": "B02"}]},
                "B03": {"href": "https://stac.test/B03.tif", "roles": ["data"], "bands": [{"name": "B03"}]},
            }
        )
        asset_table = _build(item, band_selection=["B03"], available_band_names=["B02", "B03"])
        assert [l.asset_id for l in asset_table.items[0].links] == ["B03"]

    def test_band_selection_naming_band_no_asset_provides(self):
        """A band selection that no asset provides yields no links, so the item is dropped entirely."""
        item = _item(assets={"B02": {"href": "https://stac.test/B02.tif", "roles": ["data"], "bands": [{"name": "B02"}]}})
        asset_table = _build(item, band_selection=["B99"], available_band_names=["B02"])
        assert asset_table.items == []

    @pytest.mark.parametrize(
        ["mode", "expected_scale", "expected_offset"],
        [
            (PixelValueScalingMode.NO_SCALING, 1.0, 0.0),
            (PixelValueScalingMode.S2_REFLECTANCE_SCALED_OFFSET, 1.0, -1000.0),
            (PixelValueScalingMode.SCALE_AND_OFFSET, 0.0001, -0.1),
        ],
    )
    def test_pixel_value_scaling_modes(self, mode, expected_scale, expected_offset):
        item = _item(
            assets={
                "B02": {
                    "href": "https://stac.test/B02.tif",
                    "roles": ["data"],
                    "bands": [{"name": "B02", "eo:center_wavelength": 0.493}],
                    "raster:scale": 0.0001,
                    "raster:offset": -0.1,
                }
            }
        )
        asset_table = _build(item, pixel_value_scaling_mode=mode)
        link = asset_table.items[0].links[0]
        assert (link.pixel_value_scale, link.pixel_value_offset) == (expected_scale, expected_offset)

    def test_datatype_and_nodata(self):
        item = _item(
            assets={
                "B02": {
                    "href": "https://stac.test/B02.tif",
                    "roles": ["data"],
                    "bands": [{"name": "B02"}],
                    "data_type": "uint16",
                    "nodata": 0,
                },
                "B03": {"href": "https://stac.test/B03.tif", "roles": ["data"], "bands": [{"name": "B03"}]},
            }
        )
        asset_table = _build(item)
        links_by_id = {l.asset_id: l for l in asset_table.items[0].links}
        assert (links_by_id["B02"].data_type, links_by_id["B02"].nodata) == ("uint16", 0.0)
        assert (links_by_id["B03"].data_type, links_by_id["B03"].nodata) == (None, None)

    def test_projection_metadata_item_level_proj_code(self):
        item = _item(
            bbox=(3, 45, 3.1, 45.1),
            properties={"proj:code": "EPSG:32631"},
            assets={"B02": {"href": "https://stac.test/B02.tif", "roles": ["data"], "bands": [{"name": "B02"}]}},
        )
        asset_table = _build(item)
        assert asset_table.observed_epsgs == {32631}
        assert asset_table.items[0].crs_epsg == 32631

    def test_projection_metadata_asset_level_proj_bbox_and_shape(self):
        item = _item(
            bbox=(3, 45, 3.1, 45.1),
            properties={"proj:code": "EPSG:32631"},
            assets={
                "B02": {
                    "href": "https://stac.test/B02.tif",
                    "roles": ["data"],
                    "bands": [{"name": "B02"}],
                    "proj:bbox": [500000, 4990200, 509800, 5000000],
                    "proj:shape": [980, 980],
                }
            },
        )
        asset_table = _build(item)
        assert asset_table.resolution_tracker.finest_for(["B02"]) == ({32631}, (10.0, 10.0))
        item_result = asset_table.items[0]
        assert item_result.crs_epsg == 32631
        assert item_result.raster_extent == (500000.0, 4990200.0, 509800.0, 5000000.0)
        assert item_result.resolution == 10.0

    def test_projection_metadata_conflicting_epsgs_across_assets(self):
        item = _item(
            bbox=(3, 45, 3.1, 45.1),
            assets={
                "B02": {
                    "href": "https://stac.test/B02.tif",
                    "roles": ["data"],
                    "bands": [{"name": "B02"}],
                    "proj:code": "EPSG:32631",
                },
                "B8A": {
                    "href": "https://stac.test/B8A.tif",
                    "roles": ["data"],
                    "bands": [{"name": "B8A"}],
                    "proj:code": "EPSG:32632",
                },
            },
        )
        asset_table = _build(item)
        assert asset_table.observed_epsgs == {32631, 32632}

    def test_stac_bbox_accumulation_across_items(self):
        item1 = _item(id="item1", bbox=(3, 45, 4, 46), assets={"B02": {"href": "https://stac.test/a.tif", "roles": ["data"], "bands": [{"name": "B02"}]}})
        item2 = _item(id="item2", bbox=(5, 47, 6, 48), assets={"B02": {"href": "https://stac.test/b.tif", "roles": ["data"], "bands": [{"name": "B02"}]}})
        asset_table = _build([item1, item2])
        assert asset_table.stac_bbox.as_wsen_tuple() == (3.0, 45.0, 6.0, 48.0)

    def test_implausible_bbox_is_skipped_when_outside_extent(self):
        item = _item(
            id="item-bad",
            bbox=(-170, 60, 170, 70),
            properties={"proj:code": "EPSG:32601"},
            assets={"B02": {"href": "https://stac.test/B02.tif", "roles": ["data"], "bands": [{"name": "B02"}]}},
        )
        narrow_extent = SpatioTemporalExtent(bbox=BoundingBox(west=10, south=10, east=11, north=11, crs=4326))
        asset_table = _build(item, spatiotemporal_extent=narrow_extent)
        assert asset_table.items == []

    def test_implausible_bbox_is_kept_when_intersecting_extent(self):
        item = _item(
            id="item-bad",
            bbox=(-170, 60, 170, 70),
            properties={"proj:code": "EPSG:32601"},
            assets={"B02": {"href": "https://stac.test/B02.tif", "roles": ["data"], "bands": [{"name": "B02"}]}},
        )
        overlapping_extent = SpatioTemporalExtent(bbox=BoundingBox(west=-1, south=60, east=1, north=70, crs=4326))
        asset_table = _build(item, spatiotemporal_extent=overlapping_extent)
        assert [i.item_id for i in asset_table.items] == ["item-bad"]

    def test_granule_metadata_band_map_produces_metadata_links(self):
        item = _item(
            assets={
                "B02": {"href": "https://stac.test/B02.tif", "roles": ["data"], "bands": [{"name": "B02"}]},
                "granule_metadata": {
                    "href": "https://stac.test/MTD_TL.xml",
                    "title": "MTD_TL.xml",
                    "roles": ["metadata"],
                },
            }
        )
        available_band_names = ["B02"]
        asset_table = _build(
            item,
            available_band_names=available_band_names,
            feature_flags={"granule_metadata_band_map": {"sunAzimuthAngles": "granule_metadata##0"}},
        )
        metadata_link = asset_table.items[0].metadata_links[0]
        assert metadata_link.asset_id == "granule_metadata"
        assert metadata_link.band_names == ["granule_metadata##0"]
        assert asset_table.opensearch_link_titles_map == {"sunAzimuthAngles": "granule_metadata##0"}
        # feature flag handling extends available_band_names in place
        assert available_band_names == ["B02", "sunAzimuthAngles"]

    def test_skipped_assets_and_preferred_url_prefix(self):
        item = _item(
            assets={
                "B02": {
                    "href": "https://stac.test/B02.tif",
                    "roles": ["data"],
                    "bands": [{"name": "B02"}],
                    "alternate": {"s3": {"href": "s3://bucket/B02.tif"}},
                },
                "B03": {"href": "https://stac.test/B03.tif", "roles": ["data"], "bands": [{"name": "B03"}]},
            }
        )
        asset_table = _build(item, feature_flags={"skipped_assets": ["B03"], "preferred_url_prefix": "s3://"})
        links = asset_table.items[0].links
        assert [l.asset_id for l in links] == ["B02"]
        assert links[0].href == "s3://bucket/B02.tif"

    def test_use_raw_asset_href(self):
        item = _item(
            assets={
                "B02": {
                    "href": "https://stac.test/B02.tif",
                    "roles": ["data"],
                    "bands": [{"name": "B02"}],
                    "alternate": {"s3": {"href": "s3://bucket/B02.tif"}},
                }
            }
        )
        asset_table = _build(item, use_raw_asset_href=True)
        assert asset_table.items[0].links[0].href == "https://stac.test/B02.tif"
