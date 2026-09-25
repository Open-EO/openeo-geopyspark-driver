import datetime
from typing import Tuple, Union

import dateutil.parser
import pytz
from openeo.util import rfc3339


# TODO: move these utilities to openeo-python-driver or even openeo-python-client?

# Some type aliases for convenience
DateLike = Union[str, datetime.date]
DateTimeLike = Union[str, datetime.datetime, datetime.date]
DateTimeLikeOrNone = Union[DateTimeLike, None]


def to_datetime_utc(d: DateTimeLike) -> datetime.datetime:
    """Parse/convert to datetime in UTC."""
    if isinstance(d, str):
        d = dateutil.parser.parse(d)
    elif isinstance(d, datetime.datetime):
        pass
    elif isinstance(d, datetime.date):
        d = datetime.datetime.combine(d, datetime.time.min)
    else:
        raise ValueError(f"Expected str/datetime, but got {type(d)}")
    if d.tzinfo is None:
        d = d.replace(tzinfo=datetime.timezone.utc)
    else:
        d = d.astimezone(datetime.timezone.utc)
    return d


def to_datetime_naive(d: DateTimeLike) -> datetime.datetime:
    """Convert to datetime, assuming UTC where necessary, but return as naive."""
    return to_datetime_utc(d).replace(tzinfo=None)


def to_datetime_utc_unless_none(d: DateTimeLikeOrNone) -> Union[datetime.datetime, None]:
    """Parse/convert to datetime in UTC, but preserve None."""
    return None if d is None else to_datetime_utc(d)


def _normalize_date(date_string: Union[str, None]) -> Union[str, None]:
    if date_string is not None:
        date = dateutil.parser.parse(date_string)
        if date.tzinfo is None:
            date = date.replace(tzinfo=pytz.UTC)
        return date.isoformat()
    return None


def normalize_temporal_extent(temporal_extent: Tuple[Union[str, None], Union[str, None]]) -> Tuple[str, str]:
    start, end = temporal_extent
    return (
        _normalize_date(start or "2000-01-01"),  # TODO: better fallback start date?
        _normalize_date(end or rfc3339.now_utc()),
    )
