import json
from unittest.mock import patch

import pytest
import requests

from scripts.get_application_metrics import DEFAULT_ENDPOINT, main


@pytest.mark.parametrize("endpoint", [None, DEFAULT_ENDPOINT + "/", DEFAULT_ENDPOINT + "/api/v1/"])
@pytest.mark.parametrize("duration", [None, 52])
def test_application_metrics(endpoint, duration, capsys):
    argv = ["job-90206ae556-df7ea45d", "--at", "2026-10-26T08:00:00Z"]
    if endpoint is not None:
        argv += ["--endpoint", endpoint]
    if duration is not None:
        argv += ["--duration-seconds", str(duration)]

    with patch("scripts.get_application_metrics.Prometheus") as client:
        prometheus = client.return_value
        prometheus.get_memory_usage.return_value = 4096.0
        prometheus.get_executor_wall_clock_lifetime_seconds.return_value = {"1": 663.0, "2": 0.0}
        prometheus.get_executor_memory_metrics.return_value = {
            "1": {"peak_usage_bytes": 1073741824.0, "peak_working_set_bytes": 900.0},
            "2": {"peak_limit_bytes": 3000.0},
        }
        main(argv)

    client.assert_called_once_with(DEFAULT_ENDPOINT + "/api/v1")
    for method in (prometheus.get_executor_wall_clock_lifetime_seconds, prometheus.get_executor_memory_metrics):
        method.assert_called_once_with(argv[0], at=argv[2])
    metrics = {
        "executor_wall_clock_lifetime": {"unit": "s", "data": {"1": 663.0, "2": 0.0}},
        "executor_memory_peak_usage": {"unit": "byte", "data": {"1": 1073741824.0}},
        "executor_memory_peak_working_set": {"unit": "byte", "data": {"1": 900.0}},
        "executor_memory_peak_limit": {"unit": "byte", "data": {"2": 3000.0}},
    }
    if duration is not None:
        prometheus.get_memory_usage.assert_called_once_with(argv[0], application_duration_s=duration, at=argv[2])
        metrics["memory_usage_byte_seconds"] = {"unit": "byte*s", "data": 4096.0}
    else:
        prometheus.get_memory_usage.assert_not_called()
    prometheus.get_avg_load_megapixel_per_second.assert_not_called()
    prometheus.get_min_load_megapixel_per_second.assert_not_called()
    prometheus.get_load_megapixel_total.assert_not_called()
    prometheus.get_avg_udf_execution_time_ms.assert_not_called()
    prometheus.get_max_udf_execution_time_ms.assert_not_called()
    prometheus.get_avg_udf_max_rss_delta_bytes.assert_not_called()
    prometheus.get_max_udf_max_rss_delta_bytes.assert_not_called()
    prometheus.get_executor_run_time_ms.assert_not_called()
    assert json.loads(capsys.readouterr().out) == {"metrics": metrics}


@pytest.mark.parametrize("speeds", [{}, {"SENTINEL2_L2A": 3.5, "SENTINEL1_GRD": 1.25}])
def test_application_metrics_load_speed(speeds, capsys):
    with patch("scripts.get_application_metrics.Prometheus") as client:
        prometheus = client.return_value
        prometheus.get_avg_load_megapixel_per_second.return_value = speeds
        prometheus.get_executor_wall_clock_lifetime_seconds.return_value = {
            "1": 10.0,
            "2": 0.0,
            "3": 5.0,
        }
        prometheus.get_executor_memory_metrics.return_value = {}
        minimum_speeds = {collection: speed / 2 for collection, speed in speeds.items()}
        prometheus.get_min_load_megapixel_per_second.return_value = minimum_speeds
        totals = {collection: speed * 100 for collection, speed in speeds.items()}
        prometheus.get_load_megapixel_total.return_value = totals
        prometheus.get_avg_udf_execution_time_ms.return_value = 120.5 if speeds else None
        prometheus.get_max_udf_execution_time_ms.return_value = 250.0 if speeds else None
        executor_times = {"1": 5000.0, "2": 2000.0, "3": 3000.0, "4": 1000.0} if speeds else {}
        prometheus.get_executor_run_time_ms.return_value = executor_times
        main(["a-example", "--job-id", "j-example", "--at", "2026-10-26T08:00:00Z"])
    prometheus.get_avg_load_megapixel_per_second.assert_called_once_with("j-example", at="2026-10-26T08:00:00Z")
    prometheus.get_min_load_megapixel_per_second.assert_called_once_with("j-example", at="2026-10-26T08:00:00Z")
    prometheus.get_load_megapixel_total.assert_called_once_with("j-example", at="2026-10-26T08:00:00Z")
    prometheus.get_avg_udf_execution_time_ms.assert_called_once_with("j-example", at="2026-10-26T08:00:00Z")
    prometheus.get_max_udf_execution_time_ms.assert_called_once_with("j-example", at="2026-10-26T08:00:00Z")
    prometheus.get_avg_udf_max_rss_delta_bytes.assert_not_called()
    prometheus.get_max_udf_max_rss_delta_bytes.assert_not_called()
    prometheus.get_executor_run_time_ms.assert_called_once_with("j-example", at="2026-10-26T08:00:00Z")
    report = json.loads(capsys.readouterr().out)
    assert set(report) == {"metrics"}
    assert report["metrics"]["load_pixel_speed_avg"] == {"unit": "megapixel/s", "data": speeds}
    assert report["metrics"]["load_pixel_speed_min"] == {"unit": "megapixel/s", "data": minimum_speeds}
    assert report["metrics"]["load_pixel_total"] == {"unit": "megapixel", "data": totals}
    assert report["metrics"]["udf_execution_time_avg"] == {"unit": "ms", "data": 120.5 if speeds else None}
    assert report["metrics"]["udf_execution_time_max"] == {"unit": "ms", "data": 250.0 if speeds else None}
    assert "avg_udf_max_rss_delta_bytes" not in report["metrics"]
    assert "max_udf_max_rss_delta_bytes" not in report["metrics"]
    assert report["metrics"]["executor_run_time"] == {"unit": "ms", "data": executor_times}
    assert report["metrics"]["executor_utilization"] == {
        "unit": "percentage",
        "data": {"1": 50.0, "3": 60.0} if speeds else {},
    }
    prometheus.get_executor_wall_clock_lifetime_seconds.assert_called_once_with("a-example", at="2026-10-26T08:00:00Z")
    assert report["metrics"]["executor_wall_clock_lifetime"] == {
        "unit": "s",
        "data": {"1": 10.0, "2": 0.0, "3": 5.0},
    }
    prometheus.get_executor_memory_metrics.assert_called_once_with("a-example", at="2026-10-26T08:00:00Z")
    for metric in (
        "executor_memory_peak_usage",
        "executor_memory_peak_working_set",
        "executor_memory_peak_limit",
    ):
        assert report["metrics"][metric] == {"unit": "byte", "data": {}}


def test_application_metrics_default_time_and_error(capsys):
    with patch("scripts.get_application_metrics.Prometheus") as client:
        client.return_value.get_executor_wall_clock_lifetime_seconds.side_effect = requests.HTTPError("Forbidden")
        with pytest.raises(requests.HTTPError, match="Forbidden"):
            main(["job-example"])
    assert client.return_value.get_executor_wall_clock_lifetime_seconds.call_args.kwargs["at"].endswith("+00:00")
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ['job-".*'],
        ["job-example", "--duration-seconds", "0"],
        ["job-example", "--duration-seconds", "-1"],
        ["job-example", "--duration-seconds", "nan"],
        ["job-example", "--duration-seconds", "inf"],
        ["job-example", "--duration-seconds", "invalid"],
    ],
)
def test_application_metrics_invalid_arguments(argv):
    with patch("scripts.get_application_metrics.Prometheus") as client:
        with pytest.raises(SystemExit) as exc:
            main(argv)
    assert exc.value.code == 2
    client.assert_not_called()
