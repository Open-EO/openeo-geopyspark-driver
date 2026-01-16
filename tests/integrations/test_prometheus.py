import json
import pytest

from openeogeotrellis.integrations.prometheus import (
    Prometheus,
    compact_time_serie_values,
    time_series_to_single_float_value,
)
from pathlib import Path

prom_responses = Path(__file__).parent.joinpath("prom_responses")


def get_test_data(filename: str):
    with open(prom_responses.joinpath(filename)) as inf:
        return json.load(inf)


def test_endpoint_property():
    prometheus = Prometheus("https://prometheus.example.org")

    assert prometheus.endpoint == "https://prometheus.example.org"


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
    calculated = compact_time_serie_values(input_data)
    assert calculated == expected, f"Failed compaction scenario {description}"


def test_time_series_to_single_value():
    prom_response = get_test_data("cwl_simple.json")
    # We expect the values of each time series to be integrated over the time bounds
    expected_value = 268435456 * 4 + 4294967296 * 4
    calculated_value = time_series_to_single_float_value(prom_response, compactable=True)
    assert calculated_value == expected_value
