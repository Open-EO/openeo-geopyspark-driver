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


def parse_approximate_isoduration(s: str) -> datetime.timedelta:
    """
    Parse the ISO8601 duration as years,months,weeks,days, hours,minutes,seconds.
    Approximate, because it does not care about leap years, months with different number of days, etc.
    Examples: "PT1H30M15.460S", "P5DT4M", "P2WT3H", "P1D"
    Based on: https://stackoverflow.com/questions/36976138/is-there-an-easy-way-to-convert-iso-8601-duration-to-timedelta
    """

    def get_isosplit(s_arg, split):
        if split in s_arg:
            n, s_arg = s_arg.split(split, 1)
        else:
            n = '0'
        return float(n.replace(',', '.')), s_arg  # to handle like "P0,5Y"

    s = s.split('P', 1)[-1]  # Remove prefix
    # M can mean month or minute, so we split the day and time part:
    if 'T' in s:
        s_date0, s_time0 = s.split('T', 1)
    else:
        s_date0 = s
        s_time0 = ''
    s_date, s_time = s_date0, s_time0
    s_yr, s_date = get_isosplit(s_date, 'Y')  # Step through letter dividers
    s_mo, s_date = get_isosplit(s_date, 'M')
    s_wk, s_date = get_isosplit(s_date, 'W')
    s_dy, s_date = get_isosplit(s_date, 'D')

    s_hr, s_time = get_isosplit(s_time, 'H')
    s_mi, s_time = get_isosplit(s_time, 'M')
    s_sc, s_time = get_isosplit(s_time, 'S')
    n_yr = s_yr * 365  # approx days for year, month, week
    n_mo = s_mo * 30.4  # Average days per month
    n_wk = s_wk * 7
    dt = datetime.timedelta(days=n_yr + n_mo + n_wk + s_dy, hours=s_hr, minutes=s_mi,
                            seconds=s_sc)
    return dt
