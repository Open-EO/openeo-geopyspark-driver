from typing import Optional

import requests


class Prometheus:
    # TODO: does filtering on pod namespace speed things up?

    def __init__(self, endpoint: str):
        """Point this to e.g. http://example.org:9090/api/v1."""
        self.endpoint = endpoint

    def _query_for_float(self, query: str, time: str = None) -> Optional[float]:
        params = {
            'query': query
        }

        if time is not None:
            params['time'] = time

        with requests.get(self.endpoint + "/query", params=params) as resp:
            resp.raise_for_status()
            entity = resp.json()

        status = entity["status"]
        if status == "error":
            raise Exception(f"query {query} at {time} returned status {status}: {entity['error']}")

        results = entity['data']['result']

        return float(results[0]['value'][1]) if len(results) > 0 else None

    def get_cpu_usage(self, application_id: str, at: str = None) -> Optional[float]:
        """Returns CPU usage in cpu-seconds."""

        query = f'sum(last_over_time(container_cpu_usage_seconds_total{{pod=~"{application_id}.+",image=""}}[5d]))'
        return self._query_for_float(query, at)

    def get_network_received_usage(self, application_id: str, at: str = None) -> Optional[float]:
        """Returns bytes received."""

        query = f'sum(last_over_time(container_network_receive_bytes_total{{pod=~"{application_id}-.+"}}[5d]))'
        return self._query_for_float(query, at)

    def get_memory_usage(self, application_id: str, application_duration_s: float, at: str = None) -> Optional[float]:
        """Returns memory usage in byte-seconds."""

        # Prometheus doesn't expose this as a counter: do integration over time ourselves
        query = f'sum(avg_over_time(container_memory_usage_bytes{{pod=~"{application_id}-.+",image=""}}[5d])) ' \
                f'* {application_duration_s}'
        return self._query_for_float(query, at)

    def get_max_executor_memory_usage(self, application_id: str, at: str = None) -> Optional[float]:
        """Returns memory usage in byte-seconds."""

        # Prometheus doesn't expose this as a counter: do integration over time ourselves
        query = f'max(max_over_time(container_memory_working_set_bytes{{pod=~"{application_id}-.+exec.+",image=""}}[5d]))/(1024*1024*1024) '
        return self._query_for_float(query, at)


if __name__ == '__main__':
    prometheus = Prometheus("https://prometheus.stag.warsaw.marketplace.dataspace.copernicus.eu/api/v1")

    application_id = 'job-90206ae556-df7ea45d'
    application_finish_time = "2023-06-22T11:30:46Z"

    cpu_seconds = prometheus.get_cpu_usage(application_id=application_id, at=application_finish_time)
    byte_seconds = prometheus.get_memory_usage(application_id=application_id, application_duration_s=52,
                                               at=application_finish_time)
    bytes_received = prometheus.get_network_received_usage(application_id=application_id, at=application_finish_time)

    print(cpu_seconds if cpu_seconds is not None else "unknown", "cpu-seconds")
    print(byte_seconds / 1024 / 1024 if byte_seconds is not None else "unknown", "mb-seconds")
    print(bytes_received / 1024 / 1024 if bytes_received is not None else "unknown", "mb")
