
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
        ("user", True),
        ("developer", True),
    ],
)
@mock.patch("openeogeotrellis.logs.Elasticsearch.search")
def test_elasticsearch_logs_audience_filter(mock_search, audience, expected_filter):
    mock_search.return_value = {"hits": {"hits": []}}
    list(elasticsearch_logs("job-foo", audience=audience))
    query = mock_search.call_args.kwargs["query"]
    assert {"term": {"audience": audience}} in query["bool"]["filter"]


@pytest.mark.parametrize("kwargs", [{}, {"audience": None}, {"audience": "all"}])
@mock.patch("openeogeotrellis.logs.Elasticsearch.search")
def test_elasticsearch_logs_audience_none_disables_filter(mock_search, kwargs):
    mock_search.return_value = {"hits": {"hits": []}}
    list(elasticsearch_logs("job-foo", **kwargs))
    query = mock_search.call_args.kwargs["query"]
    assert not any("audience" in str(f) for f in query["bool"]["filter"])


@pytest.mark.parametrize(
    ["audience", "expected_audience"],
    [
        (None, "all"),
        ("user", "user"),
        ("developer", "developer"),
        ("all", "all"),
    ],
)
def test_get_log_entries_audience(audience, expected_audience):
    from openeogeotrellis.backend import GpsBatchJobs

    batch_jobs = GpsBatchJobs(catalog=None, jvm=None)
    job_info = mock.Mock(status="finished", created=None)
    kwargs = {} if audience is None else {"audience": audience}
    with mock.patch.object(batch_jobs, "get_job_info", return_value=job_info), mock.patch(
        "openeogeotrellis.backend.elasticsearch_logs", return_value=iter(())
    ) as es_logs:
        list(batch_jobs.get_log_entries("j-123", user_id="john", **kwargs))
    assert es_logs.call_args.kwargs["audience"] == expected_audience


def test_get_log_entries_invalid_audience():
    from openeogeotrellis.backend import GpsBatchJobs

    batch_jobs = GpsBatchJobs(catalog=None, jvm=None)
    with pytest.raises(OpenEOApiException) as exc_info:
        batch_jobs.get_log_entries("j-123", user_id="john", audience="dev")
    assert exc_info.value.status_code == 400
    assert exc_info.value.code == "InvalidAudience"
