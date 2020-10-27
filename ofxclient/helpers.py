import datetime as dt
import re
import typing as t

OFX_DATE_FORMAT = "%Y%m%d%H%M%S"


def to_ofx_date(date: dt.datetime) -> str:
    return date.strftime(OFX_DATE_FORMAT)


def from_ofx_date(
    date_str: str,
    format: t.Optional[str] = None,
) -> dt.datetime:
    # dateAsString looks something like 20101106160000.00[-5:EST]
    # for 6 Nov 2010 4pm UTC-5 aka EST

    # Some places (e.g. Newfoundland) have non-integer offsets.
    res = re.search(r"\[(?P<tz>[-+]?\d+\.?\d*)\:\w*\]$", date_str)
    if res:
        tz = float(res.group("tz"))
    else:
        tz = 0

    tz_offset = dt.timedelta(hours=tz)

    res = re.search(r"^[0-9]*\.([0-9]{0,5})", date_str)
    if res:
        msec = dt.timedelta(seconds=float("0." + res.group(1)))
    else:
        msec = dt.timedelta(seconds=0)

    try:
        local_date = dt.datetime.strptime(date_str[:14], "%Y%m%d%H%M%S")
        return local_date - tz_offset + msec
    except ValueError:
        if date_str[:8] == "00000000":
            raise

        if not format:
            return dt.datetime.strptime(
                date_str[:8], "%Y%m%d") - tz_offset + msec
        else:
            return dt.datetime.strptime(
                date_str[:8], format) - tz_offset + msec

