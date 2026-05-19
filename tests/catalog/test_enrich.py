import pytest

from openeogeotrellis.catalog.enrich import _enrichment_metadata_from_stac


class TestEnrichment:
    @pytest.mark.parametrize(
        ["summaries"],
        [
            ({"bands": [{"name": "B01"}, {"name": "B02"}, {"name": "B03"}]},),
            ({"eo:bands": [{"name": "B01"}, {"name": "B02"}, {"name": "B03"}]},),
        ],
    )
    def test_from_stac_add_cube_dimensions_from_summaries_bands(self, requests_mock, summaries):
        stac_url = "http://stac.test/collection.json"
        requests_mock.get(
            stac_url,
            json={
                "id": "Test123",
                "stac_version": "1.1.0",
                "type": "Collection",
                "summaries": summaries,
            },
        )
        enriched_metadata = _enrichment_metadata_from_stac(stac_url)
        assert enriched_metadata == {
            "id": "Test123",
            "stac_version": "1.1.0",
            "type": "Collection",
            "summaries": summaries,
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B01", "B02", "B03"]}},
        }

    @pytest.mark.parametrize(
        ["summaries", "expected"],
        [
            (
                # Add aliases
                {"bands": [{"name": "B02"}, {"name": "B03"}]},
                {"bands": [{"name": "B02", "aliases": ["blue", "sky"]}, {"name": "B03"}]},
            ),
            (
                # Extend to existing aliases
                {"bands": [{"name": "B02", "aliases": ["aqua"]}, {"name": "B03", "aliases": ["0f0"]}]},
                {"bands": [{"name": "B02", "aliases": ["aqua", "blue", "sky"]}, {"name": "B03", "aliases": ["0f0"]}]},
            ),
            (
                # eo:bands variant
                {"eo:bands": [{"name": "B02"}, {"name": "B03"}]},
                {"eo:bands": [{"name": "B02", "aliases": ["blue", "sky"]}, {"name": "B03"}]},
            ),
        ],
    )
    def test_from_stac_inject_aliases(self, requests_mock, summaries, expected):
        stac_url = "http://stac.test/collection.json"
        requests_mock.get(
            stac_url,
            json={
                "id": "Test123",
                "stac_version": "1.1.0",
                "type": "Collection",
                "summaries": summaries,
            },
        )
        enriched_metadata = _enrichment_metadata_from_stac(stac_url, band_aliases={"B02": ["blue", "sky"]})
        assert enriched_metadata == {
            "id": "Test123",
            "stac_version": "1.1.0",
            "type": "Collection",
            "summaries": expected,
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B02", "B03"]}},
        }
