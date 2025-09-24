import pytest
import responses

from openeogeotrellis.integrations.stac import ResilientStacIO


class TestStacApiIO:

    @responses.activate
    def test_basic(self):
        responses.get("https://example.test", json={"hello": "world"})

        stac_api_io = ResilientStacIO()
        assert stac_api_io.read_text_from_href("https://example.test") == '{"hello": "world"}'

    @responses.activate
    def test_http_error(self):
        responses.get("https://example.test", status=500)

        stac_api_io = ResilientStacIO()
        with pytest.raises(Exception, match="Could not read uri https://example.test"):
            stac_api_io.read_text_from_href("https://example.test")
