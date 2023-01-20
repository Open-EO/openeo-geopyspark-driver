import mock

import pytest

from openeogeotrellis.logs import elasticsearch_logs
from openeo_driver.errors import OpenEOApiException

from elasticsearch.exceptions import ConnectionTimeout


@mock.patch("openeogeotrellis.logs.Elasticsearch.search")
def test_connection_timeout_raises_openeoapiexception(mock_search):
    mock_search.side_effect = ConnectionTimeout(500, "Simulating connection timeout")

    with pytest.raises(OpenEOApiException) as exc:
        list(elasticsearch_logs("job-foo", offset=None))

    assert exc.value.message == (
        "Temporary failure while retrieving logs (ConnectionTimeout). "
        + "Please try again and report this error if it persists."
    )
