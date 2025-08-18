from typing import Union
import datetime

import dateutil.parser


def to_datetime_utc(d: Union[str, datetime.datetime, datetime.date]) -> datetime.datetime:
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


def to_datetime_utc_unless_none(
    d: Union[str, datetime.datetime, datetime.date, None]
) -> Union[datetime.datetime, None]:
    """Parse/convert to datetime in UTC, but preserve None."""
    return None if d is None else to_datetime_utc(d)
