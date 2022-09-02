from typing import Iterable, Optional

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


ES_HOSTS = "https://es-infra.vgt.vito.be"
ES_INDEX_PATTERN = "openeo-index-1m*"
ES_TAGS = ["openeo"]


def elasticsearch_logs(job_id: str) -> Iterable[dict]:
    with Elasticsearch(ES_HOSTS) as es:
        hits = scan(
            es,
            index=ES_INDEX_PATTERN,
            query={
                'query': {
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
                }
            },
            sort='@timestamp',
            preserve_order=True,
            size=100)

        for hit in hits:
            yield _as_log_entry(hit)


def _as_log_entry(hit: dict) -> dict:
    time = hit['_source'].get('@timestamp')
    slf4j_log_level = hit['_source'].get('levelname')
    message = hit['_source'].get('message')

    return {
        'id': "%(_index)s/%(_id)s" % hit,
        'time': time,
        'level': _openeo_log_level(slf4j_log_level),
        'message': message
    }


def _openeo_log_level(slf4j_log_level: Optional[str]) -> Optional[str]:
    if slf4j_log_level == 'ERROR':
        return 'error'
    elif slf4j_log_level == 'WARN':
        return 'warning'
    elif slf4j_log_level == 'INFO':
        return 'info'
    elif slf4j_log_level in ['DEBUG', 'TRACE']:
        return 'debug'

    return None


def main():
    index = "filebeat-index-3m-2019.10.14-000064"
    job_id = '9978cebf-1cb5-4833-abe6-8f1bbc118ce9'

    with Elasticsearch(hosts=ES_HOSTS) as es:
        res = es.get(index=index, id="08kxjH4BFu4FfsUusCWt")

    print(res['_source'])
    
    logs = iter(elasticsearch_logs(job_id))
    print(next(logs))
    print(next(logs))
    print(next(logs))


if __name__ == '__main__':
    main()
