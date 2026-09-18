import datetime

import pytest

from openeogeotrellis.util.datetime import (
    normalize_temporal_extent,
    parse_approximate_isoduration,
    to_datetime_naive,
    to_datetime_utc,
    to_datetime_utc_unless_none,
)


@pytest.mark.parametrize(
    ["obj", "expected"],
    [
        ("2025-07-24", (2025, 7, 24, 0, 0, 0)),
        ("2025-07-24T12:34:56", (2025, 7, 24, 12, 34, 56)),
        ("2025-07-24T12:34:56.123", (2025, 7, 24, 12, 34, 56, 123000)),
        ("2025-07-24T12:34:56Z", (2025, 7, 24, 12, 34, 56)),
        ("2025-07-24T12:34:56+03", (2025, 7, 24, 9, 34, 56)),
        ("2025-07-24T12:34:56+03:30", (2025, 7, 24, 9, 4, 56)),
        ("2025-07-24T12:34:56-03", (2025, 7, 24, 15, 34, 56)),
        ("2025-07-24T12:34:56-03:30", (2025, 7, 24, 16, 4, 56)),
        (datetime.date(2025, 7, 24), (2025, 7, 24, 0, 0, 0)),
        (datetime.datetime(2025, 7, 24, 12, 34, 56), (2025, 7, 24, 12, 34, 56)),
        (
            datetime.datetime(2025, 7, 24, 12, 34, 56, tzinfo=datetime.timezone(offset=datetime.timedelta(hours=+2))),
            (2025, 7, 24, 10, 34, 56),
        ),
        (
            datetime.datetime(
                2025, 7, 24, 12, 34, 56, tzinfo=datetime.timezone(offset=datetime.timedelta(hours=-4, minutes=-30))
            ),
            (2025, 7, 24, 17, 4, 56),
        ),
    ],
)
def test_to_datetime_utc(obj, expected):
    actual = to_datetime_utc(obj)
    assert actual == datetime.datetime(*expected, tzinfo=datetime.timezone.utc)
    assert actual.tzinfo == datetime.timezone.utc


@pytest.mark.parametrize(
    ["obj", "expected"],
    [
        ("2025-07-24", (2025, 7, 24, 0, 0, 0)),
        ("2025-07-24T12:34:56", (2025, 7, 24, 12, 34, 56)),
        ("2025-07-24T12:34:56.123", (2025, 7, 24, 12, 34, 56, 123000)),
        ("2025-07-24T12:34:56Z", (2025, 7, 24, 12, 34, 56)),
        ("2025-07-24T12:34:56+03", (2025, 7, 24, 9, 34, 56)),
        ("2025-07-24T12:34:56+03:30", (2025, 7, 24, 9, 4, 56)),
        ("2025-07-24T12:34:56-03", (2025, 7, 24, 15, 34, 56)),
        ("2025-07-24T12:34:56-03:30", (2025, 7, 24, 16, 4, 56)),
        (datetime.date(2025, 7, 24), (2025, 7, 24, 0, 0, 0)),
        (datetime.datetime(2025, 7, 24, 12, 34, 56), (2025, 7, 24, 12, 34, 56)),
        (
            datetime.datetime(2025, 7, 24, 12, 34, 56, tzinfo=datetime.timezone(offset=datetime.timedelta(hours=+2))),
            (2025, 7, 24, 10, 34, 56),
        ),
        (
            datetime.datetime(
                2025, 7, 24, 12, 34, 56, tzinfo=datetime.timezone(offset=datetime.timedelta(hours=-4, minutes=-30))
            ),
            (2025, 7, 24, 17, 4, 56),
        ),
    ],
)
def test_to_datetime_naive(obj, expected):
    actual = to_datetime_naive(obj)
    assert actual == datetime.datetime(*expected, tzinfo=None)
    assert actual.tzinfo is None


@pytest.mark.parametrize(
    ["obj", "expected"],
    [
        (None, None),
        ("2025-07-24", datetime.datetime(2025, 7, 24, 0, 0, 0, tzinfo=datetime.timezone.utc)),
        (
            datetime.datetime(2025, 7, 24, 12, 34, 56),
            datetime.datetime(2025, 7, 24, 12, 34, 56, tzinfo=datetime.timezone.utc),
        ),
    ],
)
def test_to_datetime_utc_unless_none(obj, expected):
    assert to_datetime_utc_unless_none(obj) == expected


@pytest.mark.parametrize(
    ["duration_str", "expected"],
    [
        ("PT1H30M15.460S", "1:30:15.460000"),
        ("P5DT4M", "5 days, 0:04:00"),
        ("P2WT3H", "14 days, 3:00:00"),
        ("P16D", "16 days, 0:00:00"),
        ("PT1H", "1:00:00"),
        ("P1DT1S", "1 day, 0:00:01"),
        ("P1D", "1 day, 0:00:00"),
        ("P1M", "30 days, 9:36:00"),
        ("P1Y", "365 days, 0:00:00"),
        ("P2D", "2 days, 0:00:00"),
        ("P5D", "5 days, 0:00:00"),
        ("P6Y", "2190 days, 0:00:00"),
        ("P999D", "999 days, 0:00:00"),
        ("P999M", "30369 days, 14:24:00"),
        ("P999Y", "364635 days, 0:00:00"),
    ],
)
def test_parse_approximate_isoduration(duration_str, expected):
    # This function needed some adjustments to work with durations found in layercatalog metadata:
    duration = parse_approximate_isoduration(duration_str)
    print(f"duration={duration}")
    assert str(duration) == expected


@pytest.mark.parametrize(
    ["temporal_extent", "expected"],
    [
        ((None, None), None),
        (("2020-01-01", "2020-02-01"), ("2020-01-01T00:00:00+00:00", "2020-02-01T00:00:00+00:00")),
        ((None, "2020-02-01"), ("2000-01-01T00:00:00+00:00", "2020-02-01T00:00:00+00:00")),
    ],
)
def test_normalize_temporal_extent(temporal_extent, expected):
    start, end = normalize_temporal_extent(temporal_extent)
    if expected is None:
        assert start == "2000-01-01T00:00:00+00:00"
        assert end is not None
    else:
        assert (start, end) == expected
