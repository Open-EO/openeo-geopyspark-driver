import dirty_equals
import pytest

from openeo.testing.stac import StacDummyBuilder
from openeogeotrellis.catalog.manage import (
    BandMetadata,
    build_stac_collection_metadata,
    CRS_AUTO_42001,
    ENRICHMENT_MODE,
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
                        }
                    ],
                    "eo:bands": [
                        {
                            "name": "B02",
                            "description": "Not red",
                            "aliases": ["bleu"],
                            "common_name": "blue",
                            "gsd": 123,
                            "offset": 0.5,
                            "scale": 0.1,
                            "type": "uint8",
                        }
                    ],
                    "raster:bands": [
                        {
                            "name": "B02",
                            "offset": 0.5,
                            "scale": 0.1,
                        }
                    ],
                },
            }
        )
