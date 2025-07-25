import datetime
import pytest

from openeogeotrellis.util.datetime import to_datetime_utc, to_datetime_utc_unless_none


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
