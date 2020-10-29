from datetime import date, datetime
import decimal
import json
import math
import uuid
from time import time, gmtime, struct_time
import typing as t

from json.encoder import JSONEncoder

WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")

MONTHS = (
    "Jan", "Feb", "Mar", "Apr",
    "May", "Jun", "Jul", "Aug",
    "Sep", "Oct", "Nov", "Dec",
)


def _dump_date(d: t.Union[None, datetime, int, float], delim: str):
    """Used for `http_date` and `cookie_date`."""

    s: struct_time

    if d is None:
        s = gmtime()
    elif isinstance(d, datetime):
        s = d.utctimetuple()
    elif isinstance(d, (int, float)):
        s = gmtime(d)
    else:
        s = d

    weekday = WEEKDAYS[s.tm_wday]
    month = MONTHS[s.tm_mon - 1]
    return "%s, %02d%s%s%s%s %02d:%02d:%02d GMT" % (
        weekday,
        s.tm_mday,
        delim,
        month,
        delim,
        str(s.tm_year),
        s.tm_hour,
        s.tm_min,
        s.tm_sec,
    )


def rfc_1223(timestamp=None):
    return _dump_date(timestamp, " ")


class CustomJSONEncoder(JSONEncoder):

    def default(self, o):
        if isinstance(o, datetime):
            return rfc_1223(o.utctimetuple())
        if isinstance(o, date):
            return rfc_1223(o.timetuple())
        if isinstance(o, uuid.UUID):
            return str(o)
        if isinstance(o, decimal.Decimal):
            return float(o)
        if not isinstance(o, (str, int, float, bool)):
            return str(o)
        return super().default(o)


class JSONDecoder(json.JSONDecoder):
    pass


def dumps(d, cls=CustomJSONEncoder):
    return json.dumps(d, cls=cls, allow_nan=False)


def loads(d, cls=JSONDecoder):
    return json.loads(d, cls=cls)
