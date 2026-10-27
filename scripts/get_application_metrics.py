"""Report application metrics using the existing Prometheus integration.

Run from the repository root in the project's Python environment:

    python -m scripts.get_application_metrics job-90206ae556-df7ea45d
    python -m scripts.get_application_metrics job-90206ae556-df7ea45d \
        --at 2026-10-26T08:00:00Z --duration-seconds 52
    python -m scripts.get_application_metrics a-example --job-id j-2610251633414ceab499ab0a5f6423d1

Resource queries use the client's five-day lookback before --at (defaults to now).
Missing metrics are JSON null, not zero. Memory usage is included only when
--duration-seconds is supplied. With --job-id, each load-speed series is averaged
over the 24 hours before --at, then averaged per collection (an empty object if unavailable).
The minimum load speed per collection over the same window is also reported.
Loaded megapixel totals per collection sum the latest counter values found within
the same 24-hour window; they are not increases restricted to that window.
With --job-id, average and maximum UDF execution times in milliseconds are also
reported over 24 hours. The average is an average of per-series sample averages,
not an invocation-weighted mean: the metric is a last-execution-duration gauge.
The UDF RSS-delta Prometheus query methods are available separately, but those
values are not included in this CLI report.
Spark cumulative task execution time in milliseconds is reported per executor,
using the latest counter values found within 24 hours. This is summed task time,
not executor wall-clock lifetime; concurrent tasks can overlap.
With --job-id, executor_utilization is each executor's cumulative task time
divided by its wall-clock lifetime, expressed as a percentage rounded to two
decimal places. Concurrent tasks can make it exceed 100%; executors with missing
or zero lifetime are omitted.
Estimated executor wall-clock lifetimes in seconds use container start-to-last-seen
timestamps found within 24 hours, summing container lifetimes across restarts and
excluding restart gaps. These are scrape-based estimates, not exact exit times.
Executor memory metrics report independent 24-hour peaks for usage, working set
and memory limits in bytes, as separate metrics with executor IDs
mapped to values. Missing per-executor metrics are omitted.
Billable metrics are not queried here.

Each metric value is represented as an object with its unit and the reported
value under ``data``. For per-executor and per-collection metrics, ``data`` is
the corresponding mapping and the unit applies to every value in that mapping.
"""

import argparse
import datetime
import json
import math
import re
from typing import Optional, Sequence

from openeogeotrellis.integrations.prometheus import Prometheus

DEFAULT_ENDPOINT = "https://prometheus.stag.waw3-1.openeo-int.v1.dataspace.copernicus.eu"


def _metric(data: object, unit: str) -> dict[str, object]:
    return {"unit": unit, "data": data}


def application_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise argparse.ArgumentTypeError("application ID must contain only letters, digits, underscores or hyphens")
    return value


def positive_seconds(value: str) -> float:
    seconds = float(value)
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("duration must be a finite, positive number of seconds")
    return seconds


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("application_id", type=application_id)
    parser.add_argument("--job-id", help="openEO job/correlation ID for load, UDF and Spark executor metrics")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="Prometheus base URL (default: %(default)s)")
    parser.add_argument("--at", help="Evaluation time as an RFC3339 timestamp or Unix timestamp (default: now)")
    parser.add_argument(
        "--duration-seconds", type=positive_seconds, help="Application duration for memory usage estimation"
    )
    args = parser.parse_args(argv)

    endpoint = args.endpoint.rstrip("/")
    if not endpoint.endswith("/api/v1"):
        endpoint += "/api/v1"
    prometheus = Prometheus(endpoint)
    at = args.at if args.at is not None else datetime.datetime.now(datetime.timezone.utc).isoformat()

    executor_memory_metrics = prometheus.get_executor_memory_metrics(args.application_id, at=at)
    executor_lifetimes = prometheus.get_executor_wall_clock_lifetime_seconds(args.application_id, at=at)
    metrics: dict[str, object] = {"executor_wall_clock_lifetime": _metric(executor_lifetimes, "s")}
    for metric_name, output_name in (
        ("peak_usage_bytes", "executor_memory_peak_usage"),
        ("peak_working_set_bytes", "executor_memory_peak_working_set"),
        ("peak_limit_bytes", "executor_memory_peak_limit"),
    ):
        metric_data = {
            executor_id: executor_metrics[metric_name]
            for executor_id, executor_metrics in executor_memory_metrics.items()
            if metric_name in executor_metrics
        }
        metrics[output_name] = _metric(metric_data, "byte")
    if args.duration_seconds is not None:
        metrics["memory_usage_byte_seconds"] = _metric(
            prometheus.get_memory_usage(args.application_id, application_duration_s=args.duration_seconds, at=at),
            "byte*s",
        )
    if args.job_id is not None:
        metrics["load_pixel_speed_avg"] = _metric(
            prometheus.get_avg_load_megapixel_per_second(args.job_id, at=at), "megapixel/s"
        )
        metrics["load_pixel_speed_min"] = _metric(
            prometheus.get_min_load_megapixel_per_second(args.job_id, at=at), "megapixel/s"
        )
        metrics["load_pixel_total"] = _metric(prometheus.get_load_megapixel_total(args.job_id, at=at), "megapixel")
        metrics["udf_execution_time_avg"] = _metric(prometheus.get_avg_udf_execution_time_ms(args.job_id, at=at), "ms")
        metrics["udf_execution_time_max"] = _metric(prometheus.get_max_udf_execution_time_ms(args.job_id, at=at), "ms")
        executor_run_times = prometheus.get_executor_run_time_ms(args.job_id, at=at)
        metrics["executor_run_time"] = _metric(executor_run_times, "ms")
        metrics["executor_utilization"] = _metric(
            {
                executor_id: round(100 * run_time_ms / (executor_lifetimes[executor_id] * 1000), 2)
                for executor_id, run_time_ms in executor_run_times.items()
                if executor_id in executor_lifetimes and executor_lifetimes[executor_id] > 0
            },
            "percentage",
        )

    print(json.dumps({"metrics": metrics}, indent=2))


if __name__ == "__main__":
    main()
