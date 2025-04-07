import datetime as dt
import json
from typing import Iterable, Optional


from elasticsearch import Elasticsearch, ConnectionTimeout, TransportError

try:
    from openeo.rest.models.logs import normalize_log_level
except ImportError:
    # TODO remove old deprecated import (since openeo 0.38.0)
    from openeo.api.logs import normalize_log_level

from openeo.util import dict_no_none, rfc3339
from openeo_driver.errors import OpenEOApiException
from openeo_driver.util.logging import FlaskRequestCorrelationIdLogging
from openeogeotrellis.config import get_backend_config

import logging

_logger = logging.getLogger(__name__)

ES_TAGS = ["openeo"]


def elasticsearch_logs(
    job_id: str,
    create_time: Optional[dt.datetime] = None,
    offset: Optional[str] = None,
    level: Optional[str] = None,
) -> Iterable[dict]:
    """Retrieve a job's logs from Elasticsearch.

    :param job_id:
        ID of the Job

    :param create_time:
        Time the job was created, only log records starting from that time will be retrieved

    :param offset:
        Search only after this offset.
        If used, then this offset expects a combination of the timestamp and log.offset
        encoded as a JSON string that contains a list of the form [timestamp, log_offset]
        where timestamp is the Unix Epoch (int) and log_offset is an integer referring to
        log.offset in Elasticsearch.

        For example: "[1673351608383, 102790]"

    :param level:
        Return only logs with this log level or higher.

    :raises OpenEOApiException:
        - Either when the offset is not valid JSON
        - or when Elasticsearch had a connection timeout

    :return: an generator that yields a dict for each log record.
    """
    try:
        search_after = None if offset in [None, ""] else json.loads(offset)
        return _elasticsearch_logs(
            job_id=job_id,
            create_time=create_time,
            search_after=search_after,
            level=level,
        )
    except json.decoder.JSONDecodeError:
        raise OpenEOApiException(status_code=400, code="OffsetInvalid",
                                 message=f"The value passed for the query parameter 'offset' is invalid: {offset}")


def _elasticsearch_logs(
    job_id: str,
    create_time: Optional[dt.datetime] = None,
    search_after: Optional[list] = None,
    level: Optional[str] = None,
) -> Iterable[dict]:
    """Internal helper function to retrieve a job's logs from Elasticsearch.

    :param job_id:
        ID of the Job

    :param create_time:
        Time the job was created, only log records starting from that time will be retrieved

    :param search_after:
        Search only after this offset.
        This offset is a list which takes two elements: the timestamp and log.offset
        where timestamp is the Unix Epoch (int) and log.offset (int) in Elasticsearch.

        For example: [1673351608383, 102790]

    :param level:
        Return only logs with this log level or higher.

    :raises OpenEOApiException:
        - Either when the offset is not valid JSON
        - or when Elasticsearch had a connection timeout

    :return: an generator that yields a dict for each log record.
    """

    req_id = FlaskRequestCorrelationIdLogging.get_request_id()
    log_level_int = normalize_log_level(level)
    level_filter = None
    if log_level_int:
        levels_to_include = {
            logging.ERROR: ["ERROR"],
            logging.WARNING: ["ERROR", "WARNING"],
            logging.INFO: ["ERROR", "WARNING", "INFO"],
            logging.DEBUG: ["ERROR", "WARNING", "INFO", "DEBUG"],
        }
        level_filter = {"terms": {"levelname": levels_to_include[log_level_int]}}

    page_size = 100
    query = {
        "bool": {
            "filter": [
                {"term": {"job_id": job_id}},
                {"terms": {"tags": ES_TAGS}},
            ],
        }
    }
    if create_time:
        query["bool"]["filter"].append(
            {
                "range": {
                    "@timestamp": {
                        "format": "strict_date_time",
                        "gte": rfc3339.datetime(create_time),
                    }
                }
            }
        )
    if level_filter:
        query["bool"]["filter"].append(level_filter)

    config = get_backend_config()
    with Elasticsearch(config.logging_es_hosts) as es:
        while True:
            try:
                search_result = es.search(
                    index=config.logging_es_index_pattern,
                    query=query,
                    search_after=search_after,
                    size=page_size,
                    sort=[
                        {"@timestamp": {"order": "asc"}},
                        {"log.offset": {"order": "asc"}},  # tie-breaker
                        # chances are slim that two processes write on the same line within the same millisecond
                        # 'log.file.path', 'host.name'
                    ],
                    request_timeout=120,
                )

            except ConnectionTimeout as exc:
                # TODO: add a test that verifies: doesn't leak sensitive info + it does log the ConnectionTimeout
                message = (
                    "Temporary failure while retrieving logs: ConnectionTimeout. "
                    + f"Please try again and report this error if it persists. (ref: {req_id})"
                )
                raise OpenEOApiException(status_code=504, message=message) from exc

            except TransportError as exc:
                # TODO: Retry when ES raises circuit breaker exception + better error if still fails.
                #   https://github.com/Open-EO/openeo-python-driver/issues/170
                if exc.status_code == 429:
                    message = (
                        "Temporary failure while retrieving logs: Elasticsearch has interrupted "
                        + "the search request because it used too memory. Please try again later"
                        + f"and report this error if it persists. (ref: {req_id})"
                    )
                    raise OpenEOApiException(status_code=429, message=message) from exc
                else:
                    raise

            else:
                hits = search_result["hits"]["hits"]

                for hit in hits:
                    search_after = hit["sort"]

                    # Skip the log line if the log level is empty
                    entry = _as_log_entry(log_id=json.dumps(search_after), hit=hit)
                    if "level" in entry:
                        yield entry

                if len(hits) < page_size:
                    break


def _as_log_entry(log_id: str, hit: dict) -> dict:
    _source = hit['_source']

    time = _source.get('@timestamp')
    internal_log_level = _source.get('levelname')
    message = _source.get('message')
    data = _source.get('data')
    code = _source.get('code')

    return dict_no_none(
        id=log_id,
        time=time,
        level=_openeo_log_level(internal_log_level),
        message=message,
        data=data,
        code=code
    )


def _openeo_log_level(internal_log_level: Optional[str]) -> Optional[str]:
    if internal_log_level in ['CRITICAL', 'ERROR']:
        return 'error'
    elif internal_log_level == 'WARNING':
        return 'warning'
    elif internal_log_level == 'INFO':
        return 'info'
    elif internal_log_level == 'DEBUG':
        return 'debug'

    return None


def main():
    from itertools import islice

    # job_id = 'j-7e274ba1eb2e4a20a0e100d53d44d692'  # short
    # search_after = [1670939264990, "c2m9C4UBFu4FfsUu8ClI"]

    # job_id = 'j-db27cdaaca7a4d288346eaf01ceeef16'  # long  But seems to be gone now
    # search_after = [1671009758258, "1vfxD4UBVWXUH_mWk-OT"]
    # search_after = None

    job_id = "j-65b73756031c4955aadb3d42753de2e9"  # also long
    # search_after = "[1673351608383, 102790]"
    search_after = None

    create_time = rfc3339.parse_datetime("2023-01-10T11:53:28Z")
    logs = elasticsearch_logs(job_id, create_time, search_after)

    output_limit = None

    with open(f"/tmp/{job_id}_logs.json", "w") as f:
        for log in islice(logs, output_limit):
            f.write(json.dumps(log) + "\n")


if __name__ == '__main__':
    main()
