import sys

import dirty_equals
import pytest

if sys.version_info < (3, 10):
    # TODO #1060 clean up once python 3.8/3.9 support can be dropped
    pytest.skip("openeogeotrellis.catalog.manage requires at least Python 3.10+", allow_module_level=True)


from openeo.testing.stac import StacDummyBuilder
from openeogeotrellis.catalog.manage import (
    BandMetadata,
    build_stac_collection_metadata,
    CRS_AUTO_42001,
    ENRICHMENT_MODE,
    extract_band_metadata_list,
    apply_raster_scale_and_offset_to_band_metadata,
)


class TestBandMetadata:
    def test_to_common_bands(self):
        assert BandMetadata(name="blue").to_common_bands() == {
            "name": "blue",
        }

        assert BandMetadata(name="blue", description="Not red").to_common_bands() == {
            "name": "blue",
            "description": "Not red",
        }

    def test_to_summaries_raster_bands(self):
        assert BandMetadata(
            name="blue",
            raster_scale=0.01,
            raster_offset=42,
            unit="scoville",
            gsd=30,
            data_type="uint8",
            nodata=12345,
        ).to_summaries_raster_bands() == {
            "name": "blue",
            "scale": 0.01,
            "offset": 42,
            "data_type": "uint8",
            "nodata": 12345,
            "unit": "scoville",
        }

    def test_to_summaries_eo_bands(self):
        assert BandMetadata(
            name="blue",
            raster_scale=0.01,
            raster_offset=42,
            unit="scoville",
            gsd=30,
            data_type="uint8",
            nodata=12345,
        ).to_summaries_eo_bands() == {
            "name": "blue",
            "gsd": 30,
            "data_type": "uint8",
            "nodata": 12345,
            "unit": "scoville",
        }

    def test_to_summaries_bands(self):
        assert BandMetadata(
            name="blue",
            raster_scale=0.01,
            raster_offset=42,
            unit="scoville",
            gsd=30,
            data_type="uint8",
            nodata=12345,
        ).to_summaries_bands() == {
            "name": "blue",
            "raster:scale": 0.01,
            "raster:offset": 42,
            "data_type": "uint8",
            "nodata": 12345,
            "unit": "scoville",
            "gsd": 30,
        }


class TestBuildMetadata:
    @pytest.fixture(autouse=True)
    def _default_stac_collections(self, requests_mock):
        requests_mock.get(
            "https://stac.test/c/foobar1",
            json=StacDummyBuilder.collection(
                id="foobar1",
                stac_extensions=["https://stac-extensions.github.io/datacube/v2.2.0/schema.json"],
                stac_version="1.1.0",
                links=[
                    {
                        "rel": "root",
                        "href": "https://stac.test/",
                    }
                ],
            ),
        )

    def test_minimal(self):
        metadata = build_stac_collection_metadata(
            id="FOOBAR",
            stac_url="https://stac.test/c/foobar1",
            bands=[BandMetadata(name="blue", description="Not red")],
        )
        assert metadata == {
            "stac_version": "1.1.0",
            "type": "Collection",
            "id": "FOOBAR",
            "title": "FOOBAR",
            "description": "Collection 123",
            "_vito": {
                "data_source": {
                    "type": "stac",
                    "url": "https://stac.test/c/foobar1",
                    "enrich": True,
                },
            },
            "bands": [{"name": "blue", "description": "Not red"}],
            "cube:dimensions": {
                "bands": {"type": "bands", "values": ["blue"]},
                "t": {"type": "temporal"},
                "x": {"type": "spatial", "axis": "x", "reference_system": CRS_AUTO_42001, "step": 10},
                "y": {"type": "spatial", "axis": "y", "reference_system": CRS_AUTO_42001, "step": 10},
            },
            "extent": {
                "spatial": {"bbox": [[3, 4, 5, 6]]},
                "temporal": {"interval": [["2024-01-01", "2024-05-05"]]},
            },
            "license": "proprietary",
            "providers": [],
            "summaries": {
                "bands": [{"name": "blue", "description": "Not red"}],
                "eo:bands": [{"name": "blue", "description": "Not red"}],
                "raster:bands": [{"name": "blue"}],
            },
        }

    def test_enrich_at_build_time(self):
        metadata = build_stac_collection_metadata(
            id="FOOBAR",
            stac_url="https://stac.test/c/foobar1",
            bands=[BandMetadata(name="blue", description="Not red")],
            enrichment_mode=ENRICHMENT_MODE.LEGACY_AT_BUILD_TIME,
        )
        assert metadata == {
            "stac_version": "1.1.0",
            "stac_extensions": ["https://stac-extensions.github.io/datacube/v2.2.0/schema.json"],
            "type": "Collection",
            "id": "FOOBAR",
            "title": "FOOBAR",
            "description": "Collection 123",
            "_vito": {
                "data_source": {
                    "type": "stac",
                    "url": "https://stac.test/c/foobar1",
                    "enrich": False,
                },
            },
            "bands": [{"name": "blue", "description": "Not red"}],
            "cube:dimensions": {
                "bands": {"type": "bands", "values": ["blue"]},
                "t": {"type": "temporal"},
                "x": {"type": "spatial", "axis": "x", "reference_system": CRS_AUTO_42001, "step": 10},
                "y": {"type": "spatial", "axis": "y", "reference_system": CRS_AUTO_42001, "step": 10},
            },
            "extent": {
                "spatial": {"bbox": [[3, 4, 5, 6]]},
                "temporal": {"interval": [["2024-01-01", "2024-05-05"]]},
            },
            "license": "proprietary",
            "providers": [],
            "summaries": {
                "bands": [{"name": "blue", "description": "Not red"}],
                "eo:bands": [{"name": "blue", "description": "Not red"}],
                "raster:bands": [{"name": "blue"}],
            },
            "links": [{"href": "https://stac.test/", "rel": "root"}],
        }

    def test_bands_metadata(self):
        metadata = build_stac_collection_metadata(
            id="FOOBAR",
            stac_url="https://stac.test/c/foobar1",
            bands=[
                BandMetadata(
                    name="B02",
                    description="Not red",
                    eo_common_name="blue",
                    raster_scale=0.1,
                    raster_offset=0.5,
                    aliases=["bleu"],
                    gsd=123,
                    data_type="uint8",
                )
            ],
        )
        assert metadata == dirty_equals.IsPartialDict(
            {
                "bands": [
                    {
                        "name": "B02",
                        "description": "Not red",
                    }
                ],
                "cube:dimensions": dirty_equals.IsPartialDict(
                    {
                        "bands": {
                            "type": "bands",
                            "values": ["B02"],
                        },
                    }
                ),
                "summaries": {
                    "bands": [
                        {
                            "name": "B02",
                            "description": "Not red",
                            "aliases": ["bleu"],
                            "eo:common_name": "blue",
                            "gsd": 123,
                            "raster:offset": 0.5,
                            "raster:scale": 0.1,
                            "data_type": "uint8",
                        }
                    ],
                    "eo:bands": [
                        {
                            "name": "B02",
                            "description": "Not red",
                            "aliases": ["bleu"],
                            "common_name": "blue",
                            "gsd": 123,
                            "data_type": "uint8",
                        }
                    ],
                    "raster:bands": [
                        {
                            "name": "B02",
                            "offset": 0.5,
                            "scale": 0.1,
                            "data_type": "uint8",
                        }
                    ],
                },
            }
        )


class TestExtractBandMetadata:
    def test_empty(self):
        assert extract_band_metadata_list({}) == []

    def test_toplevel_bands(self):
        metadata = {
            "bands": [{"name": "blue", "description": "Not red"}, {"name": "green", "description": "A frog"}],
        }
        assert extract_band_metadata_list(metadata) == [
            BandMetadata(name="blue", description="Not red"),
            BandMetadata(name="green", description="A frog"),
        ]

    def test_clms_lcm_global(self):
        """Use case clms_lcm_global_10m_yearly_v1_cog"""
        metadata = {
            "stac_version": "1.1.0",
            "bands": [{"name": "map", "description": "MAP"}],
            "summaries": {
                "gsd": [10.0],
            },
            "item_assets": {
                "map": {
                    "bands": [
                        {
                            "name": "map",
                            "description": "MAP",
                            "classification:classes": [
                                {"name": "tree_cover", "value": 10},
                                {"name": "shrubland", "value": 20},
                            ],
                        }
                    ],
                    "nodata": 255,
                    "data_type": "uint8",
                },
                "Product": {
                    "title": "Zipped product",
                    "type": "application/zip",
                },
            },
        }
        assert extract_band_metadata_list(metadata) == [
            BandMetadata(
                name="map",
                description="MAP",
                gsd=10.0,
                data_type="uint8",
                nodata=255,
                classification_classes=[
                    {"name": "tree_cover", "value": 10},
                    {"name": "shrubland", "value": 20},
                ],
            )
        ]

    def test_clms_ba_global(self):
        """Use case clms_ba_global_300m_daily_v4_cog"""
        metadata = {
            "stac_version": "1.1.0",
            "bands": [
                {"name": "cp_nrt", "description": "Probability corresponds"},
                {
                    "name": "bf_nrt",
                    "description": "Fraction of pixel surface",
                },
                {"name": "dob_nrt", "description": "Day of burn"},
                {"name": "lfp_nrt", "description": "Probability fractional"},
            ],
            "item_assets": {
                "Product": {
                    "type": "application/zip",
                    "title": "Zipped product",
                },
                "ba300_bf_nrt": {
                    "bands": [{"name": "bf_nrt", "description": "Fraction"}],
                    "roles": ["data"],
                    "nodata": -32768.0,
                    "data_type": "int16",
                    "raster:scale": 0.001,
                },
                "ba300_cp_nrt": {
                    "bands": [
                        {
                            "name": "cp_nrt",
                            "description": "Probability corresponds",
                        }
                    ],
                    "roles": ["data"],
                    "nodata": -32768.0,
                    "data_type": "int16",
                    "raster:scale": 0.001,
                },
                "ba300_dob_nrt": {
                    "bands": [{"name": "dob_nrt", "description": "Day of burn"}],
                    "roles": ["data"],
                    "nodata": -32768.0,
                    "data_type": "int16",
                },
                "ba300_lfp_nrt": {
                    "bands": [{"name": "lfp_nrt", "description": "Probability fractional"}],
                    "roles": ["data"],
                    "nodata": -32768.0,
                    "data_type": "int16",
                    "raster:scale": 0.001,
                },
            },
        }

        assert extract_band_metadata_list(metadata) == [
            BandMetadata(
                name="cp_nrt",
                description="Probability corresponds",
                raster_scale=0.001,
                raster_offset=None,
                gsd=None,
                data_type="int16",
                nodata=-32768,
            ),
            BandMetadata(
                name="bf_nrt",
                description="Fraction",
                raster_scale=0.001,
                raster_offset=None,
                gsd=None,
                data_type="int16",
                nodata=-32768,
            ),
            BandMetadata(
                name="dob_nrt",
                description="Day of burn",
                raster_scale=None,
                raster_offset=None,
                gsd=None,
                data_type="int16",
                nodata=-32768,
            ),
            BandMetadata(
                name="lfp_nrt",
                description="Probability fractional",
                raster_scale=0.001,
                raster_offset=None,
                gsd=None,
                data_type="int16",
                nodata=-32768,
            ),
        ]

    def test_clms_ndvi_global(self):
        """Use case clms_ndvi_global_300m_10daily_v3_cog"""
        metadata = {
            "stac_version": "1.1.0",
            "bands": [
                {"name": "nobs", "description": "Number of observation"},
                {"name": "ndvi", "description": "Normalized Difference Vegetation Index"},
                {"name": "unc", "description": "Uncertainty on Normalized Difference Vegetation Index"},
                {"name": "qflag", "description": "Quality Flag on Normalized Difference Vegetation Index"},
            ],
            "summaries": {
                "gsd": [300.0],
            },
            "item_assets": {
                "Product": {
                    "type": "application/zip",
                    "title": "Zipped product",
                },
                "ndvi300_unc": {
                    "bands": [{"name": "unc", "description": "Uncertainty on Normalized Difference Vegetation Index"}],
                    "roles": ["data"],
                    "nodata": -1.0,
                    "data_type": "int16",
                    "raster:scale": 0.001,
                },
                "ndvi300_ndvi": {
                    "bands": [
                        {
                            "name": "ndvi",
                            "description": "Normalized Difference Vegetation Index",
                            "classification:classes": [
                                {"name": "unknown", "value": 252},
                                {"name": "snow", "value": 253},
                                {"name": "water", "value": 254},
                                {"name": "missing", "value": 255},
                            ],
                        }
                    ],
                    "roles": ["data"],
                    "nodata": 255.0,
                    "data_type": "uint8",
                    "raster:scale": 0.004,
                    "raster:offset": -0.08,
                },
                "ndvi300_nobs": {
                    "bands": [{"name": "nobs", "description": "Number of observation"}],
                    "roles": ["data"],
                    "nodata": 255.0,
                    "data_type": "uint8",
                },
                "ndvi300_qflag": {
                    "bands": [
                        {
                            "name": "qflag",
                            "description": "Quality Flag on Normalized Difference Vegetation Index",
                            "classification:bitfields": [
                                {
                                    "name": "no_observations",
                                    "length": 1,
                                    "offset": 0,
                                    "classes": [
                                        {"name": "observations_available", "value": 0},
                                        {"name": "no_observations", "value": 1},
                                    ],
                                },
                                {
                                    "name": "snow_observation",
                                    "length": 1,
                                    "offset": 1,
                                    "classes": [
                                        {"name": "no_snow", "value": 0},
                                        {"name": "snow_detected", "value": 1},
                                    ],
                                },
                            ],
                        }
                    ],
                    "roles": ["data"],
                    "nodata": 255.0,
                    "data_type": "uint8",
                },
            },
        }
        assert extract_band_metadata_list(metadata) == [
            BandMetadata(
                name="nobs",
                description="Number of observation",
                gsd=300,
                data_type="uint8",
                nodata=255,
            ),
            BandMetadata(
                name="ndvi",
                description="Normalized Difference Vegetation Index",
                raster_scale=0.004,
                raster_offset=-0.08,
                gsd=300,
                data_type="uint8",
                nodata=255,
                classification_classes=[
                    {"name": "unknown", "value": 252},
                    {"name": "snow", "value": 253},
                    {"name": "water", "value": 254},
                    {"name": "missing", "value": 255},
                ],
            ),
            BandMetadata(
                name="unc",
                description="Uncertainty on Normalized Difference Vegetation " "Index",
                raster_scale=0.001,
                raster_offset=None,
                gsd=300,
                data_type="int16",
                nodata=-1,
            ),
            BandMetadata(
                name="qflag",
                description="Quality Flag on Normalized Difference Vegetation " "Index",
                gsd=300,
                data_type="uint8",
                nodata=255,
            ),
        ]


class TestBandMetadataList:
    def test_apply_raster_scale_and_offset_simple(self):
        bands = [
            BandMetadata(name="foo"),
            BandMetadata(name="bar"),
        ]
        assert apply_raster_scale_and_offset_to_band_metadata(bands) == [
            BandMetadata(name="foo"),
            BandMetadata(name="bar"),
        ]

    def test_apply_raster_scale_and_offset_with_scale(self):
        bands = [
            BandMetadata(name="foo", data_type="int8", nodata=-128),
            BandMetadata(name="bar", data_type="uint16", raster_scale=0.001),
        ]
        assert apply_raster_scale_and_offset_to_band_metadata(bands) == [
            BandMetadata(name="foo", data_type="float64"),
            BandMetadata(name="bar", data_type="float64"),
        ]

    def test_apply_raster_scale_and_offset_with_offset(self):
        bands = [
            BandMetadata(name="foo", data_type="int8", nodata=-128),
            BandMetadata(name="bar", data_type="uint16", raster_offset=-1.5),
        ]
        assert apply_raster_scale_and_offset_to_band_metadata(bands) == [
            BandMetadata(name="foo", data_type="float64"),
            BandMetadata(name="bar", data_type="float64"),
        ]
