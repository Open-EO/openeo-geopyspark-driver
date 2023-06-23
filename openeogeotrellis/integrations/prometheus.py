import requests


class Prometheus:
    # TODO: reconsider currently excluded time series?
    # TODO: does filtering on pod namespace offer advantages?

    def __init__(self, api_endpoint: str):
        """Point this to e.g. http://example.org:9090/api/v1."""
        self._api_endpoint = api_endpoint

    def _query_for_value(self, query: str, time: str = None) -> float:
        params = {
            'query': query
        }

        if time is not None:
            params['time'] = time

        with requests.get(self._api_endpoint + "/query", params=params) as resp:
            resp.raise_for_status()

            value = float(resp.json()['data']['result'][0]['value'][1])

        return value

    def get_cpu_usage(self, application_id: str, at: str = None) -> float:
        """Returns CPU usage in cpu-seconds."""

        query = f'sum(container_cpu_usage_seconds_total{{pod=~"{application_id}.+", container!=""}})'
        return self._query_for_value(query, at)

    def get_network_received_usage(self, application_id: str, at: str = None) -> float:
        """Returns bytes received."""

        query = f'sum(container_network_receive_bytes_total{{pod=~"{application_id}-.+"}})'
        return self._query_for_value(query, at)

    def get_memory_usage(self, application_id: str, application_duration_s: float, at: str = None) -> float:
        """Returns memory usage in byte-seconds."""

        # Prometheus doesn't expose this as a counter: do integration over time ourselves
        query = f'sum(avg_over_time(container_memory_usage_bytes{{pod=~"{application_id}-.+", container!=""}}[5d])) ' \
                f'* {application_duration_s}'
        return self._query_for_value(query, at)


if __name__ == '__main__':
    prometheus = Prometheus("https://prometheus.openeo-cdse-staging.vgt.vito.be/api/v1")

    application_id = 'job-90206ae556-df7ea45d'
    application_finish_time = "2023-06-22T11:30:46Z"

    print(prometheus.get_cpu_usage(application_id=application_id, at=application_finish_time), "cpu-seconds")
    print(prometheus
          .get_memory_usage(application_id=application_id, application_duration_s=52, at=application_finish_time)
          / 1024 / 1024, "mb-seconds")
    print(prometheus.get_network_received_usage(
        application_id=application_id, at=application_finish_time) / 1024 / 1024, "mb")
