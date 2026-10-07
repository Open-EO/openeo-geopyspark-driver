
from datetime import datetime
from types import SimpleNamespace

import mock
import pytest
from elasticsearch.exceptions import ConnectionTimeout, TransportError
from openeo_driver.errors import OpenEOApiException
from openeo_driver.jobregistry import JOB_STATUS

from openeogeotrellis.backend import GpsBatchJobs
from openeogeotrellis.logs import elasticsearch_logs


@pytest.mark.parametrize("status", [JOB_STATUS.CREATED, JOB_STATUS.QUEUED])
def test_get_job_logs_before_start(status):
    created = datetime(2024, 1, 1)
    entry = {"level": "info", "message": "Job submitted"}
    with mock.patch.object(GpsBatchJobs, "get_job_info", return_value=SimpleNamespace(status=status, created=created)), \
         mock.patch("openeogeotrellis.backend.elasticsearch_logs", return_value=iter([entry])) as search_logs:
        jobs = object.__new__(GpsBatchJobs)
        assert list(jobs.get_log_entries("job-foo", user_id="alice", offset="offset", level="debug")) == [entry]

    search_logs.assert_called_once_with(job_id="job-foo", create_time=created, offset="offset", level="debug")


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
