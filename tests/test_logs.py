import mock

import pytest

from openeogeotrellis.logs import elasticsearch_logs
from openeo_driver.errors import OpenEOApiException

from elasticsearch.exceptions import ConnectionTimeout


@mock.patch("openeogeotrellis.logs.Elasticsearch.search")
def test_elasticsearch_logs_skips_entry_with_empty_loglevel_simple_case(mock_search):
    search_hit = {
        "_source": {"levelname": None, "message": "A message with an empty loglevel"},
        "sort": 1,
    }
    mock_search.return_value = {
        "hits": {"hits": [search_hit]},
    }

    actual_log_entries = list(
        elasticsearch_logs("job-foo", create_time=None, offset=None)
    )
    assert actual_log_entries == []


@mock.patch("openeogeotrellis.logs.Elasticsearch.search")
def test_elasticsearch_logs_keeps_entry_when_loglevel_filled_in(mock_search):
    search_hit = {
        "_source": {
            "levelname": "ERROR",
            "message": "A message with the loglevel filled in",
        },
        "sort": 1,
    }
    mock_search.return_value = {
        "hits": {"hits": [search_hit]},
    }

    actual_log_entries = list(
        elasticsearch_logs("job-foo", create_time=None, offset=None)
    )

    expected_log_entries = [
        {
            "id": "1",
            "level": "error",
            "message": "A message with the loglevel filled in",
        }
    ]
    assert actual_log_entries == expected_log_entries


@mock.patch("openeogeotrellis.logs.Elasticsearch.search")
def test_elasticsearch_logs_skips_entries_with_empty_loglevel(mock_search):
    search_hits = [
        {
            "_source": {
                "levelname": "ERROR",
                "message": "error message",
            },
            "sort": 1,
        },
        {
            "_source": {
                "levelname": None,
                "message": "First message with empty loglevel",
            },
            "sort": 2,
        },
        {
            "_source": {
                "levelname": None,
                "message": "Second message with empty loglevel",
            },
            "sort": 3,
        },
        {
            "_source": {"levelname": "INFO", "message": "info message"},
            "sort": 4,
        },
    ]
    mock_search.return_value = {
        "hits": {"hits": search_hits},
    }

    actual_log_entries = list(
        elasticsearch_logs("job-foo", create_time=None, offset=None)
    )

    expected_log_entries = [
        {
            "id": "1",
            "level": "error",
            "message": "error message",
        },
        {
            "id": "4",
            "level": "info",
            "message": "info message",
        },
    ]
    assert actual_log_entries == expected_log_entries


@mock.patch("openeogeotrellis.logs.Elasticsearch.search")
def test_connection_timeout_raises_openeoapiexception(mock_search):
    mock_search.side_effect = ConnectionTimeout(500, "Simulating connection timeout")

    with pytest.raises(OpenEOApiException) as raise_context:
        list(elasticsearch_logs("job-foo", create_time=None, offset=None))

    expected_message = (
        "Temporary failure while retrieving logs: ConnectionTimeout. "
        + "Please try again and report this error if it persists. (ref: no-request)"
    )
    assert raise_context.value.message == expected_message
