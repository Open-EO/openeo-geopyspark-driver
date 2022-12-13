from typing import Iterable, Optional

from elasticsearch import Elasticsearch
from openeo.util import dict_no_none

ES_HOSTS = "https://es-infra.vgt.vito.be"
ES_INDEX_PATTERN = "openeo-*-index-1m*"
ES_TAGS = ["openeo"]


def elasticsearch_logs(job_id: str, from_: int) -> Iterable[dict]:
    page_size = 100

    with Elasticsearch(ES_HOSTS) as es:
        while True:
            search_result = es.search(
                index=ES_INDEX_PATTERN,
                query={
                    'bool': {
                        'filter': [{
                            'term': {
                                'job_id': job_id
                            }
                        }, {
                            'terms': {
                                'tags': ES_TAGS
                            }
                        }]
                    }
                },
                from_=from_,
                size=page_size,
                sort='@timestamp'
            )

            hits = search_result['hits']['hits']

            for hit in hits:
                yield _as_log_entry(log_id=str(from_), hit=hit)
                from_ += 1

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

    job_id = 'j-0ec6fb2b02a741cd91139cbf08eb8dff'
    offset = 8

    logs = elasticsearch_logs(job_id, offset)

    for log in islice(logs, 20):
        print(log)


if __name__ == '__main__':
    main()
