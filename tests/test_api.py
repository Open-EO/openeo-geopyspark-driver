"""
Tests for non-processing related parts of the API
"""


class TestDataDiscovery:
    """
    Tests for "EO Data Discovery" API endpoints.
    https://openeo.org/documentation/1.0/developers/api/reference.html#tag/EO-Data-Discovery
    """

    def test_queryables(self, api):
        resp = api.get("/collections/STAC_123/queryables")
        assert resp.status_code == 302
        assert resp.headers["Location"] == "https://stac.test/collections/stac-123/queryables"
