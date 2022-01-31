from typing import List, Optional

from elasticsearch import Elasticsearch


ES_HOSTS = "https://es-infra.vgt.vito.be"
ES_INDEX_PATTERN = "filebeat-index-3m*"
ES_TAGS = ["openeo-yarn"]


def elasticsearch_logs(job_id: str) -> List[dict]:
    es = Elasticsearch(ES_HOSTS)

    try:
        res = es.search(
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
            sort='@timestamp',
            size=10000)  # TODO: implement paging

        return [_as_log_entry(hit) for hit in res['hits']['hits']]
    finally:
        es.close()


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

    es = Elasticsearch(hosts=ES_HOSTS)

    try:
        res = es.get(index=index, id="08kxjH4BFu4FfsUusCWt")
        print(res['_source'])

        for log_entry in elasticsearch_logs(job_id)[:3]:
            print(log_entry)
    finally:
        es.close()


if __name__ == '__main__':
    main()
