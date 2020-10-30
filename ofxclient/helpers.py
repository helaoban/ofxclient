import datetime as dt
import re
import typing as t
import uuid
import xml.etree.ElementTree as ET
import decimal


OFX_DATE_FORMAT = "%Y%m%d%H%M%S"


def xml_strip(node: ET.Element):
    for child_node in node:
        for x in child_node.iter():
            if x.text:
                x.text = x.text.strip()
            if x.tail:
                x.tail = x.tail.strip()
    return node


def clean_query(query: str) -> str:
    tree = ET.fromstring(query.strip())
    return ET.tostring(
        xml_strip(tree),
        encoding="unicode"
    ).replace("\n", "")


def ofx_uid():
    return str(uuid.uuid4().hex)


def ofx_now() -> str:
    now = dt.datetime.utcnow()
    return to_ofx_date(now)


def to_ofx_date(date: dt.datetime) -> str:
    millisecond = round(date.microsecond / 1000)
    millisec_str = "{0:03d}".format(millisecond)
    fmt = "%Y%m%d%H%M%S.{}[0:GMT]".format(millisec_str)
    return date.strftime(fmt)


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


def to_decimal(d: str) -> decimal.Decimal:
    # Handle 10,000.50 formatted numbers
    if re.search(r".*\..*,", d):
        d = d.replace(".", "")
    # Handle 10.000,50 formatted numbers
    if re.search(r".*,.*\.", d):
        d = d.replace(",", "")
    # Handle 10000,50 formatted numbers
    if "." not in d and "," in d:
        d = d.replace(",", ".")
    # Handle 1 025,53 formatted numbers
    d = d.replace(" ", "")
    # Handle +1058,53 formatted numbers
    d = d.replace("+", "")
    # Handle null, -null
    d = d.replace("null|-null", "0")
    try:
        return decimal.Decimal(d)
    except decimal.InvalidOperation as error :
        raise ValueError(
            "Invalid Transaction Amount: '%s'"
            "" % d
        ) from error
