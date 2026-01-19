from __future__ import annotations

import datetime
import os
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

import logging
import requests
import sys

if TYPE_CHECKING:
    from typing import List, Dict, Tuple, Union

    if sys.version_info >= (3, 12):
        from typing import TypedDict
    else:
        from typing_extensions import TypedDict

    # In order to be compatible with py3.8 & 3.11 no real type alias
    T_PromTime = float  # Prometheus metrics use epoch timestamps
    T_PromValue = Union[int, float]
    T_PromDatapoint = Tuple[T_PromTime, T_PromValue]
    T_PromDatapoints = List[T_PromDatapoint]

    class T_PromQueryRespTimeSerie(TypedDict):
        metric: Dict[str, str]
        values: T_PromDatapoints

    T_PromQueryRespTimeSeries = List[T_PromQueryRespTimeSerie]

_log = logging.getLogger(__name__)


def compact_time_serie_values(values: T_PromDatapoints) -> T_PromDatapoints:
    """
    If a timeserie is tracking a value that does not change often then multiple samples can be compacted in one without
    losing real information.
    """
    if len(values) <= 2:
        return values
    # Bounds are need to be kept anyway
    first_datapoint = values[0]
    last_datapoint = values[-1]

    new_values = [first_datapoint]
    last_processed_datapoint = first_datapoint
    for datapoint in values[1:]:
        if datapoint[1] == last_processed_datapoint[1]:
            continue
        else:
            new_values.append(datapoint)
            last_processed_datapoint = datapoint

    if new_values[-1] != last_datapoint:
        new_values.append(last_datapoint)
    return new_values


def compact_time_series(prom_time_series: T_PromQueryRespTimeSeries) -> T_PromQueryRespTimeSeries:
    """
    For a query response we generally have the same metric and so compaction should happen for each time serie.
    """
    for i in range(0, len(prom_time_series)):
        prom_time_series[i]["values"] = compact_time_serie_values(prom_time_series[i]["values"])
    return prom_time_series


@dataclass(frozen=True)
class TimeSerieFloatStats:
    """
    Statistics for a timeseries of float|int values
    """

    start: float
    end: float
    min: float
    max: float
    weighted_sum: float

    @classmethod
    def from_timeserie(cls, timeserie_values: T_PromDatapoints) -> Optional[TimeSerieFloatStats]:
        start: Optional[float] = None
        min_val: Optional[float] = None
        max_val: Optional[float] = None
        weighted_sum: Optional[float] = 0.0
        prev_point: Optional[T_PromDatapoint] = None

        for point in timeserie_values:
            if start is None:
                start = point[0]

            if min_val is None:
                min_val = point[1]
            else:
                min_val = min(min_val, point[1])

            if max_val is None:
                max_val = point[1]
            else:
                max_val = max(max_val, point[1])

            if prev_point is not None:
                weighted_sum += float(prev_point[1]) * (point[0] - prev_point[0])

            prev_point = point

        if start is None:
            return None
        end = timeserie_values[-1][0]

        return cls(start=start, end=end, min=min_val, max=max_val, weighted_sum=weighted_sum)

    @property
    def corrected_weighted_sum(self) -> Optional[float]:
        """
        A corrected_weighted_sum takes into account that due to metric scraping there is actual time that is not
        accounted for. It is not really possible to do a correction that is accurate in all cases but one can correct
        so that on average it would converge to what would have been exact. If the scrape time is `st` seconds then
        on average `st/2` seconds of time is missed from the start and `st/2` is missed from past the end hence on
        average `st` is missed.

        The environment variable CORRECTION_SCRAPE_TIME_SECS can be set to the scrape time. If not defined than no
        correction takes place.
        """
        scrape_time_seconds = float(os.environ.get("CORRECTION_SCRAPE_TIME_SECS", "0"))
        if self.start is None:
            return None
        elif self.start == self.end:
            # Single sample so no duration available
            assert self.min == self.max
            return self.min * scrape_time_seconds
        else:
            duration_seconds = self.end - self.start
            return self.weighted_sum * ((duration_seconds + scrape_time_seconds) / duration_seconds)


def sum_timeseries_weighted(data: T_PromQueryRespTimeSeries, *, compactable: bool = False) -> float:
    if compactable:
        data = compact_time_series(data)
    summed_value = 0.0
    for timeserie in data:
        metric = timeserie["metric"]
        stats = TimeSerieFloatStats.from_timeserie(timeserie["values"])
        _log.debug(
            f"Summed value for {metric.get('resource', 'unknown')}",
            extra={"pod": metric.get("pod", "unknown"), "unit": metric.get("unit", "unknown"), "stats": stats},
        )
        if stats is not None:
            summed_value += stats.corrected_weighted_sum
    return summed_value


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

    def _query_for_range(self, query: str, start: float, end: float, step: int = 30) -> T_PromQueryRespTimeSeries:
        """
        Query Prometheus for a time-range defined with epoch timestamps start & end.
        It evaluates at start and then at each time it can reach by stepping `step` seconds until reaching the end.

        The data returned is defined by the Prometheus API and will be a list of timeseries
        """
        params = {
            "query": query,
            "start": start,
            "end": end,
            "step": step,  # single query can return 50M samples so seems like 30 can be used for all actions
        }

        with requests.get(self.endpoint + "/query_range", params=params) as resp:
            resp.raise_for_status()
            entity = resp.json()

        status = entity["status"]
        if status == "error":
            raise Exception(f"query {query} for {start} -> {end} returned status {status}: {entity['error']}")

        return entity["data"]["result"]

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

    def get_memory_requested(self, application_id: str, at: str = None) -> Optional[float]:
        """Returns memory usage in byte-seconds."""

        # Prometheus doesn't expose this as a counter: do integration over time ourselves
        query = f'sum(sum_over_time(kube_pod_container_resource_requests{{job="kube-state-metrics", resource="memory", pod=~"{application_id}-.+"}}[5d])) * 60 / (1024*1024) '
        return self._query_for_float(query, at)

    def get_max_executor_memory_usage(self, application_id: str, at: str = None) -> Optional[float]:
        """Returns memory usage in byte-seconds."""

        # Prometheus doesn't expose this as a counter: do integration over time ourselves
        query = f'max(max_over_time(container_memory_working_set_bytes{{pod=~"{application_id}-.+exec.+",image=""}}[5d]))/(1024*1024*1024) '
        return self._query_for_float(query, at)

    def get_summed_float_values(
        self, query: str, job_id: str, start: float, end: float, *, compactable: bool = False
    ) -> Optional[float]:
        with ExtraLoggingFilter.with_extra_logging(job_id=job_id):
            return sum_timeseries_weighted(
                self._query_for_range(
                    query=query,
                    start=start,
                    end=end,
                ),
                compactable=compactable,
            )

    # Billable metrics are metrics that are only emitted when a pod is active. This avoids metrics of pods that are in
    # a status that should not be billed (e.g. a finished pod). It is not a silver bullet because metrics are scraped
    # and therefore there will always be part of the lifetime that is missed.
    def get_billable_memory_requested(self, job_id: str, start: float, end: float) -> Optional[float]:
        """
        Billable memory requested is memory that is requested and thus reserved for the job. Requests do  not change
        during pod lifetime
        """
        query = f'cluster:namespace:pod_memory:billable:kube_pod_container_resource_requests{{label_correlation_id="{job_id}"}}'
        return self.get_summed_float_values(query, job_id, start, end, compactable=True)

    def get_billable_cpu_requested(self, job_id: str, start: float, end: float) -> Optional[float]:
        """
        Billable CPU requested is memory that is requested and thus reserved for the job.
        """
        query = f'cluster:namespace:pod_cpu:billable:kube_pod_container_resource_requests{{label_correlation_id="{job_id}"}}'
        return self.get_summed_float_values(query, job_id, start, end, compactable=True)


if __name__ == "__main__":
    from openeo_driver.util.logging import (
        setup_logging,
        get_logging_config,
        ExtraLoggingFilter,
        LOG_HANDLER_STDOUT_JSON,
    )

    setup_logging(
        get_logging_config(
            loggers={"__main__": {"level": "DEBUG", "format": "json"}},
            root_handlers=[LOG_HANDLER_STDOUT_JSON],
            enable_global_extra_logging=True,
        )
    )
    prometheus_hostname = os.environ.get(
        "PROMETHEUS_HOSTNAME", "https://prometheus.stag.warsaw.marketplace.dataspace.copernicus.eu"
    )
    prometheus = Prometheus("https://prometheus.stag.waw3-1.openeo-int.v1.dataspace.copernicus.eu/api/v1")

    application_id = 'job-90206ae556-df7ea45d'
    application_finish_time = "2023-06-22T11:30:46Z"
    job_id = "j-2601161045254a24b34c1973109d0b43"
    end = datetime.datetime.now().timestamp()
    start = end - (12 * 3600)

    report = {
        "billable_cpu": prometheus.get_billable_cpu_requested(job_id, start, end),
        "billable_memory": prometheus.get_billable_memory_requested(job_id, start, end),
    }
    _log.info("metrics", extra=report)

    cpu_seconds = prometheus.get_cpu_usage(application_id=application_id, at=application_finish_time)
    byte_seconds = prometheus.get_memory_usage(application_id=application_id, application_duration_s=52,
                                               at=application_finish_time)
    bytes_received = prometheus.get_network_received_usage(application_id=application_id, at=application_finish_time)

    print(cpu_seconds if cpu_seconds is not None else "unknown", "cpu-seconds")
    print(byte_seconds / 1024 / 1024 if byte_seconds is not None else "unknown", "mb-seconds")
    print(bytes_received / 1024 / 1024 if bytes_received is not None else "unknown", "mb")
