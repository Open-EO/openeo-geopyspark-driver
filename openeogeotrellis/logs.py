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
    internal_log_level = hit['_source'].get('levelname')
    message = hit['_source'].get('message')

    return {
        'id': "%(_index)s/%(_id)s" % hit,
        'time': time,
        'level': _openeo_log_level(internal_log_level),
        'message': message,
        # TODO: include 'data' and 'code'
    }


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
    index = "openeo-index-1m"
    job_id = 'j-93e8f12ba1a0404f984e55c4e1e6ea3f'

    with Elasticsearch(hosts=ES_HOSTS) as es:
        res = es.get(index=index, id="p3tgQIMBVWXUH_mWoFM2")

    print(res['_source'])
    
    for log in elasticsearch_logs(job_id):
        if "len" in log['message']:
            print(log)


if __name__ == '__main__':
    main()
