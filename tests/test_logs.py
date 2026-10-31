
import mock
import pytest
from elasticsearch.exceptions import ConnectionTimeout, TransportError
from openeo_driver.errors import OpenEOApiException

from openeogeotrellis.logs import elasticsearch_logs


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
def test_elasticsearch_logs_keeps_entry_with_value_for_loglevel(mock_search):
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


@mock.patch("openeogeotrellis.logs.Elasticsearch.search")
def test_circuit_breaker_raises_openeoapiexception(mock_search):
    mock_search.side_effect = TransportError(
        429, "Simulating circuit breaker exception"
    )

    with pytest.raises(OpenEOApiException) as raise_context:
        list(elasticsearch_logs("job-foo", create_time=None, offset=None))

    expected_message = (
        "Temporary failure while retrieving logs: Elasticsearch has interrupted "
        + "the search request because it used too memory. Please try again later"
        + "and report this error if it persists. (ref: no-request)"
    )
    assert raise_context.value.message == expected_message


@pytest.mark.parametrize(
    ["audience", "expected_filter"],
    [
        (None, True),  # default: user-facing only
        ("developer", True),
    ],
)
@mock.patch("openeogeotrellis.logs.Elasticsearch.search")
def test_elasticsearch_logs_audience_filter(mock_search, audience, expected_filter):
    mock_search.return_value = {"hits": {"hits": []}}
    kwargs = {} if audience is None else {"audience": audience}
    list(elasticsearch_logs("job-foo", **kwargs))
    query = mock_search.call_args.kwargs["query"]
    assert {"term": {"audience": audience or "user"}} in query["bool"]["filter"]


@mock.patch("openeogeotrellis.logs.Elasticsearch.search")
def test_elasticsearch_logs_audience_none_disables_filter(mock_search):
    mock_search.return_value = {"hits": {"hits": []}}
    list(elasticsearch_logs("job-foo", audience=None))
    query = mock_search.call_args.kwargs["query"]
    assert not any("audience" in str(f) for f in query["bool"]["filter"])
