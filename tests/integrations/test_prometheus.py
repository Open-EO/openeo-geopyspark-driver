import json
import pytest

from openeogeotrellis.integrations.prometheus import (
    Prometheus,
    compact_single_time_series_values,
    sum_timeseries_weighted,
)
from pathlib import Path

prom_responses = Path(__file__).parent.joinpath("prom_responses")


def get_test_data(filename: str):
    with open(prom_responses.joinpath(filename)) as inf:
        return json.load(inf)


def test_endpoint_property():
    prometheus = Prometheus("https://prometheus.example.org")

    assert prometheus.endpoint == "https://prometheus.example.org"


@pytest.mark.parametrize(
    "values",
    [{}, {"SENTINEL2_L2A": 3.5, "SENTINEL1_GRD": 1.25}],
)
@pytest.mark.parametrize("aggregation", ["avg", "min"])
def test_load_megapixel_per_second(requests_mock, values, aggregation):
    endpoint = "https://prometheus.example.org/api/v1"
    requests_mock.get(
        endpoint + "/query",
        json={
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {"metric": {"collection_ref": collection}, "value": [1793000000, str(value)]}
                    for collection, value in values.items()
                ],
            },
        },
    )
    job_id = 'j-example"\\'
    method = getattr(Prometheus(endpoint), f"get_{aggregation}_load_megapixel_per_second")
    assert method(job_id, at="2026-10-26T08:00:00Z") == values
    from urllib.parse import parse_qs, urlsplit

    params = parse_qs(urlsplit(requests_mock.last_request.url).query)
    assert params == {
        "query": [
            f"{aggregation} by (collection_ref) ("
            f"{aggregation}_over_time(openeo_load_megapixel_per_second{{correlation_id={json.dumps(job_id)}}}[24h]))"
        ],
        "time": ["2026-10-26T08:00:00Z"],
    }


@pytest.mark.parametrize("values", [{}, {"SENTINEL2_L2A": 123.5, "SENTINEL1_GRD": 0.0}])
def test_load_megapixel_total(requests_mock, values):
    from urllib.parse import parse_qs, urlsplit

    endpoint = "https://prometheus.example.org/api/v1"
    requests_mock.get(
        endpoint + "/query",
        json={
            "status": "success",
            "data": {
                "result": [
                    {"metric": {"collection_ref": collection}, "value": [1793000000, str(value)]}
                    for collection, value in values.items()
                ],
            },
        },
    )
    job_id = 'j-example"\\'
    assert Prometheus(endpoint).get_load_megapixel_total(job_id, at="2026-10-26T08:00:00Z") == values
    assert parse_qs(urlsplit(requests_mock.last_request.url).query) == {
        "query": [
            "sum by (collection_ref) ("
            f"last_over_time(openeo_load_megapixel_total{{correlation_id={json.dumps(job_id)}}}[24h]))"
        ],
        "time": ["2026-10-26T08:00:00Z"],
    }


@pytest.mark.parametrize("values", [{}, {"1": 526345.0, "2": 0.0}])
def test_executor_run_time_ms(requests_mock, values):
    from urllib.parse import parse_qs, urlsplit

    endpoint = "https://prometheus.example.org/api/v1"
    requests_mock.get(
        endpoint + "/query",
        json={
            "status": "success",
            "data": {
                "result": [
                    {"metric": {"executor_id": executor}, "value": [1793000000, str(value)]}
                    for executor, value in values.items()
                ],
            },
        },
    )
    job_id = 'j-example"\\'
    assert Prometheus(endpoint).get_executor_run_time_ms(job_id, at="2026-10-26T08:00:00Z") == values
    assert parse_qs(urlsplit(requests_mock.last_request.url).query) == {
        "query": [
            "max by (executor_id) ("
            f"last_over_time(spark_executor_runtime_count{{job_id={json.dumps(job_id)}}}[24h]))"
        ],
        "time": ["2026-10-26T08:00:00Z"],
    }


@pytest.mark.parametrize("values", [{}, {"1": 663.0, "2": 0.0}])
def test_executor_wall_clock_lifetime_seconds(requests_mock, values):
    from urllib.parse import parse_qs, urlsplit

    endpoint = "https://prometheus.example.org/api/v1"
    requests_mock.get(
        endpoint + "/query",
        json={
            "status": "success",
            "data": {
                "result": [
                    {"metric": {"executor_id": executor}, "value": [1793000000, str(value)]}
                    for executor, value in values.items()
                ],
            },
        },
    )
    assert (
        Prometheus(endpoint).get_executor_wall_clock_lifetime_seconds("a-example", at="2026-10-26T08:00:00Z") == values
    )
    selector = '{pod=~"a\\\\-example-.+-exec-[0-9]+",container="spark-kubernetes-executor"}'
    assert parse_qs(urlsplit(requests_mock.last_request.url).query) == {
        "query": [
            "sum by (executor_id) (label_replace(max by (namespace, pod, id) ("
            f"last_over_time(container_last_seen{selector}[24h]) - "
            f"last_over_time(container_start_time_seconds{selector}[24h])"
            '), "executor_id", "$1", "pod", ".*-exec-([0-9]+)"))'
        ],
        "time": ["2026-10-26T08:00:00Z"],
    }


def test_executor_wall_clock_lifetime_seconds_error(requests_mock):
    requests_mock.get(
        "https://prometheus.example.org/query",
        json={"status": "error", "error": "query failed"},
    )
    with pytest.raises(Exception, match="query failed"):
        Prometheus("https://prometheus.example.org").get_executor_wall_clock_lifetime_seconds("a-example")


@pytest.mark.parametrize("has_data", [False, True])
def test_executor_memory_metrics(requests_mock, has_data):
    from urllib.parse import parse_qs, urlsplit

    endpoint = "https://prometheus.example.org/api/v1"
    specifications = [
        ("peak_usage_bytes", "container_memory_usage_bytes", "", {"1": 1024.0, "2": 0.0}),
        ("peak_working_set_bytes", "container_memory_working_set_bytes", "", {"1": 512.0}),
        (
            "peak_limit_bytes",
            "kube_pod_container_resource_limits",
            ',resource="memory",unit="byte"',
            {"1": 4096.0},
        ),
    ]
    requests_mock.get(
        endpoint + "/query",
        [
            {
                "json": {
                    "status": "success",
                    "data": {
                        "result": [
                            {"metric": {"executor_id": executor}, "value": [1793000000, str(value)]}
                            for executor, value in (values.items() if has_data else [])
                        ]
                    },
                }
            }
            for _, _, _, values in specifications
        ],
    )
    assert Prometheus(endpoint).get_executor_memory_metrics("a.example", at="2026-10-26T08:00:00Z") == (
        {
            "1": {
                "peak_usage_bytes": 1024.0,
                "peak_working_set_bytes": 512.0,
                "peak_limit_bytes": 4096.0,
            },
            "2": {"peak_usage_bytes": 0.0},
        }
        if has_data
        else {}
    )
    assert requests_mock.call_count == 3
    for request, (_, metric, extra_labels, _) in zip(requests_mock.request_history, specifications):
        selector = '{pod=~"a\\\\.example-.+-exec-[0-9]+",container="spark-kubernetes-executor"' + extra_labels + "}"
        assert parse_qs(urlsplit(request.url).query) == {
            "query": [
                "max by (executor_id) (label_replace("
                f"max_over_time({metric}{selector}[24h]), "
                '"executor_id", "$1", "pod", ".*-exec-([0-9]+)"))'
            ],
            "time": ["2026-10-26T08:00:00Z"],
        }


def test_executor_memory_metrics_error(requests_mock):
    requests_mock.get(
        "https://prometheus.example.org/query",
        json={"status": "error", "error": "query failed"},
    )
    with pytest.raises(Exception, match="query failed"):
        Prometheus("https://prometheus.example.org").get_executor_memory_metrics("a-example")


def test_executor_run_time_ms_error(requests_mock):
    requests_mock.get(
        "https://prometheus.example.org/query",
        json={"status": "error", "error": "query failed"},
    )
    with pytest.raises(Exception, match="query failed"):
        Prometheus("https://prometheus.example.org").get_executor_run_time_ms("j-example")


def test_load_megapixel_total_error(requests_mock):
    requests_mock.get(
        "https://prometheus.example.org/query",
        json={"status": "error", "error": "query failed"},
    )
    with pytest.raises(Exception, match="query failed"):
        Prometheus("https://prometheus.example.org").get_load_megapixel_total("j-example")


@pytest.mark.parametrize("aggregation", ["avg", "max"])
@pytest.mark.parametrize("value", [None, 0.0, 123.5])
@pytest.mark.parametrize("metric", ["udf_execution_time_ms", "udf_max_rss_delta_bytes"])
def test_udf_gauge_metrics(requests_mock, aggregation, value, metric):
    from urllib.parse import parse_qs, urlsplit

    endpoint = "https://prometheus.example.org/api/v1"
    requests_mock.get(
        endpoint + "/query",
        json={
            "status": "success",
            "data": {"result": [] if value is None else [{"metric": {}, "value": [1793000000, str(value)]}]},
        },
    )
    job_id = 'j-example"\\'
    method = getattr(Prometheus(endpoint), f"get_{aggregation}_{metric}")
    assert method(job_id, at="2026-10-26T08:00:00Z") == value
    assert parse_qs(urlsplit(requests_mock.last_request.url).query) == {
        "query": [
            f"{aggregation}(" f"{aggregation}_over_time(openeo_{metric}{{correlation_id={json.dumps(job_id)}}}[24h]))"
        ],
        "time": ["2026-10-26T08:00:00Z"],
    }


@pytest.mark.parametrize("aggregation", ["avg", "max"])
@pytest.mark.parametrize("metric", ["udf_execution_time_ms", "udf_max_rss_delta_bytes"])
def test_udf_gauge_metrics_error(requests_mock, aggregation, metric):
    requests_mock.get(
        "https://prometheus.example.org/query",
        json={"status": "error", "error": "query failed"},
    )
    method = getattr(Prometheus("https://prometheus.example.org"), f"get_{aggregation}_{metric}")
    with pytest.raises(Exception, match="query failed"):
        method("j-example")


def test_instant_query_float(requests_mock):
    requests_mock.get(
        "https://prometheus.example.org/query",
        json={"status": "success", "data": {"result": [{"metric": {}, "value": [1793000000, "12.5"]}]}},
    )
    assert Prometheus("https://prometheus.example.org").get_cpu_usage("a-example") == 12.5


@pytest.mark.parametrize("aggregation", ["avg", "min"])
def test_load_megapixel_per_second_error(requests_mock, aggregation):
    requests_mock.get(
        "https://prometheus.example.org/query",
        json={"status": "error", "error": "query failed"},
    )
    with pytest.raises(Exception, match="query failed"):
        method = getattr(Prometheus("https://prometheus.example.org"), f"get_{aggregation}_load_megapixel_per_second")
        method("j-example")


compaction_testdata = [
    ("reduce_empty_timeserie_values_should_not_error_out", [], []),
    (
        "single_sampletimeserie_values_should_not_cause_changes",
        [[1768485185.542, "268435456"]],
        [[1768485185.542, "268435456"]],
    ),
    (
        "two_entry_sampletimeserie_values_should_not_cause_changes",
        [[1768485185.542, "268435456"], [1768485189.542, "268435456"]],
        [[1768485185.542, "268435456"], [1768485189.542, "268435456"]],
    ),
    (
        "reduce_duplicate_values",
        [[1768485185.542, "268435456"], [1768485189.542, "268435456"], [1768485193.542, "268435456"]],
        [[1768485185.542, "268435456"], [1768485193.542, "268435456"]],
    ),
    (
        "reduce_duplicate_values_cardinality_of_2",
        [
            [1768485185.542, "268435456"],
            [1768485189.542, "268435456"],
            [1768485193.542, "268435456"],
            [1768485197.542, "268435856"],
        ],
        [[1768485185.542, "268435456"], [1768485197.542, "268435856"]],
    ),
    (
        "reduce_duplicate_values_cardinality_of_2_not_last",
        [
            [1768485185.542, "268435456"],
            [1768485189.542, "268435456"],
            [1768485193.542, "268435856"],
            [1768485197.542, "268435856"],
        ],
        [[1768485185.542, "268435456"], [1768485193.542, "268435856"], [1768485197.542, "268435856"]],
    ),
]


@pytest.mark.parametrize("description,input_data,expected", compaction_testdata)
def test_compact_time_series_values(description, input_data, expected):
    calculated = compact_single_time_series_values(input_data)
    assert calculated == expected, f"Failed compaction scenario {description}"


def test_time_series_to_single_value(monkeypatch):
    monkeypatch.setenv("CORRECTION_SCRAPE_TIME_SECS", "0")
    prom_response = get_test_data("cwl_simple.json")
    # We expect the values of each time series to be integrated over the time bounds
    expected_value = 268435456 * 4 + 4294967296 * 4
    calculated_value = sum_timeseries_weighted(prom_response, compactable=True)
    assert calculated_value == expected_value
