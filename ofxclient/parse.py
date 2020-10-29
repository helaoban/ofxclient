import codecs
from collections import OrderedDict
from collections.abc import Iterable
import contextlib
import decimal
import datetime
import re
import sys
import typing as t
import xml.etree.ElementTree as ET
from itertools import chain

from . import mcc, types as tp
from .helpers import from_ofx_date

if t.TYPE_CHECKING:
    Transformer = t.Callable[[str], t.Any]


TX_TYPES = [
    "posmf",
    "posstock",
    "posopt",
    "posother",
    "posdebt"
]

AGGREGATE_TYPES = [
    "buydebt",
    "buymf",
    "buyopt",
    "buyother",
    "buystock",
    "closureopt",
    "income",
    "invexpense",
    "jrnlfund",
    "jrnlsec",
    "margininterest",
    "reinvest",
    "retofcap",
    "selldebt",
    "sellmf",
    "sellopt",
    "sellother",
    "sellstock",
    "split",
    "transfer"
]


def raise_error() -> None:
    raise RuntimeError()


def with_node(node: ET.Element, name: str) -> t.Iterable[ET.Element]:
    child_node = node.find(name.upper())
    if not child_node:
        return []
    return [child_node]


def with_nodes(node: ET.Element, name: str) -> t.Iterable[ET.Element]:
    return node.findall(name.upper())


@contextlib.contextmanager
def has_node(node: ET.Element, name: str) -> t.Iterator[ET.Element]:
    child_node = node.find(name)
    if not child_node:
        return
    yield child_node


@contextlib.contextmanager
def has_nodes(node: ET.Element, name: str) -> t.Iterator[t.Iterable[ET.Element]]:
    child_nodes = node.findall(name)
    if not child_nodes:
        return
    yield child_nodes


def noop(val: str) -> str:
    return val


def get_text_or_raise(node: ET.Element) -> str:
    if not node.text:
        raise ValueError("Element does not have text")
    return node.text.strip()


def get_child_text_or_raise(node: ET.Element, child_name: str) -> str:
    child = node.find(child_name)
    if not child:
        raise ValueError("Element does not have child '{child_name}'")
    return get_text_or_raise(child)


def extract_contents(node: ET.Element, name: str) -> t.Optional[str]:
    child_node = node.find(name)
    if not child_node or not child_node.text:
        return None
    return child_node.text.strip()


def apply_contents(d: t.Any) -> t.Callable[..., None]:
    def _do_apply(
        node: ET.Element,
        name: str,
        *transform: t.Callable[[str], t.Any],
        alias: t.Optional[str] = None,
    ) -> None:
        nonlocal d
        alias = alias or name
        child_node = node.find(name.upper())

        # TODO WARNING, Element is falsey if it
        # has no children! This is incredibly bad,
        # but we cannoo change eeet.
        if child_node is None or child_node.text is None:
            return

        try:
            contents = child_node.text.strip()
        except Exception as error:
            raise ValueError(
                "Error while trying to extract contents "
                f"from tag {name.upper()}"
            ) from error

        try:
            for t in transform:
                contents = t(contents)
        except Exception as error:
            raise RuntimeError(
                "Error while trying to transform contents "
                f"from tag {name.upper()} using transform {t}"
            ) from error

        d[alias] = contents

    return _do_apply


def clean_headers(headers: OrderedDict) -> OrderedDict:
    rv: "OrderedDict[str, t.Any]" = OrderedDict()
    for header, value in headers.items():
        if value.upper() == "NONE":
            rv[header] = None
        else:
            rv[header] = value
    return rv


def decode_headers(headers: OrderedDict) -> OrderedDict:
    """
    Decode the headers and wrap self.fh in a decoder such that it
    subsequently returns only text.
    """
    ascii_headers = OrderedDict()

    for key, value in headers.items():
        k = key.decode("ascii", "replace")
        v = value.decode("ascii", "replace")
        ascii_headers[k] = v

    enc_type = ascii_headers.get("ENCODING")

    if not enc_type:
        return ascii_headers

    if enc_type == "USASCII":
        cp = ascii_headers.get("CHARSET", "1252")
        if cp == "8859-1":
            encoding = "iso-8859-1"
        else:
            encoding = "cp%s" % (cp, )

    if enc_type in ("UNICODE", "UTF-8"):
        encoding = "utf-8"

    rv = OrderedDict()
    for key, value in headers.items():
        rv[key.decode(encoding)] = value.decode(encoding)

    return rv


def extract_headers(ofx_str: str) -> OrderedDict:
    rv = OrderedDict()

    for index, char in enumerate(ofx_str):
        if char == "<":
            break

    head_data = ofx_str[:index]

    for line in head_data.splitlines():
        # Newline?
        if line.strip() == "":
            break

        header, value = line.split(":")
        header, value = header.strip().upper(), value.strip()
        rv[header] = value

    return rv


def clean_ofx_xml(ofx_str: str) -> str:
    # find all closing tags as hints
    closing_tags = [
        t.upper() for t in
        re.findall(r"(?i)</([a-z0-9_\.]+)>", ofx_str)
    ]

    # close all tags that don't have closing tags and
    # leave all other data intact
    last_open_tag = None
    tokens = re.split(r"(?i)(</?[a-z0-9_\.]+>)", ofx_str)
    cleaned = ""
    for token in tokens:
        is_closing_tag = token.startswith("</")
        is_processing_tag = token.startswith("<?")
        is_cdata = token.startswith("<!")
        is_tag = token.startswith("<") and not is_cdata
        is_open_tag = is_tag and not is_closing_tag \
            and not is_processing_tag
        if is_tag:
            if last_open_tag is not None:
                cleaned = cleaned + "</%s>" % last_open_tag
                last_open_tag = None
        if is_open_tag:
            tag_name = re.findall(r"(?i)<([a-z0-9_\.]+)>", token)[0]
            if tag_name.upper() not in closing_tags:
                last_open_tag = tag_name
        cleaned = cleaned + token
    return cleaned


def serialize_signon(signon: tp.Signon) -> str:
        return f"""
<SIGNONMSGSRSV1>
    <SONRS>
        <STATUS>
            <CODE>{signon['code']}</CODE>
            <SEVERITY>{signon['severity']}</SEVERITY>
            <MESSAGE>{signon['message']}</MESSAGE>
        </STATUS>
        <DTSERVER>{signon['dtserver']}</DTSERVER>
        <LANGUAGE>{signon['language']}</LANGUAGE>
        <DTPROFUP>{signon['dtprofup']}</DTPROFUP>
        <FI>
            <ORG>{signon['org']}</ORG>
            <FID>{signon['fid']}</FID>
        </FI>
    </SONRS>
    <INTU.BID>{signon['intu_bid']}</INTU.BID>
</SIGNONMSGSRSV1>
"""


def _default_parse_result() -> tp.ParseResult:
    return {
        "accounts": [],
        "securities": [],
        "status": None,
        "signon": None,
    }


def parse_ofx(
    ofx_str: str,
    custom_date_format: t.Optional[str] = None,
) -> tp.ParseResult:
    rv = _default_parse_result()

    headers = clean_headers(
        decode_headers(extract_headers(ofx_str)))
    cleaned = clean_ofx_xml(ofx_str)
    node = ET.fromstring(cleaned)

    for signon_messages in with_node(node, "signonmsgsrsv1"):
        for sonrs in with_node(signon_messages, "sonrs"):
            rv["signon"] = parse_signon_response(sonrs)

    for transactions in with_node(node, "stmttrnrs"):
        for status_node in with_node(transactions, "status"):
            apply = apply_contents(rv["status"])
            apply(status_node, "code", int)
            apply(status_node, "severity")
            apply(status_node, "message")

    for cc_transactions in with_node(node, "ccstmtrs"):
        for status_node in with_node(cc_transactions, "status"):
            apply = apply_contents(rv["status"])
            apply(status_node, "code", int)
            apply(status_node, "severity")
            apply(status_node, "message")

    for statement in with_nodes(node, "stmtrs"):
        account = parse_account(statement)
        rv["accounts"].append(account)

    for cc_statement in with_nodes(node, "ccstmtrs"):
        account = parse_account(statement)
        rv["accounts"].append(account)

    for inv_statement in with_nodes(node, "invstmtrs"):
        inv_account = parse_investment_account(inv_statement)
        rv["accounts"].append(inv_account)

    for investments in with_node(node, "invstmtrs"):
        for security_node in with_nodes(investments, "seclist"):
            security = parse_security(security_node)
            rv["securities"].append(security)

    for account_info_node in node.iter("ACCTINFORS"):
        for account_node in account_info_node.iter("ACCTINFO"):
            account_ = parse_account(account_node)
            rv["accounts"].append(account_)

    return rv


def parse_security_list(node: ET.Element) -> t.Iterable[tp.Security]:
    rv: t.List[tp.Security] = []
    for security_info in node.findall("secinfo"):
        yield parse_security(security_info)


def _default_security() -> tp.Security:
    return {
        "unique_id": "",
        "name": "",
        "ticker": "",
        "memo": "",
    }


def parse_security(node: ET.Element) -> tp.Security:
    security = _default_security()
    apply = apply_contents(security)

    unique_id_node = node.find("uniqueid")
    name_node = node.find("secname")

    if not unique_id_node:
        raise ValueError("Security node missing UNIQUEID node")

    if not name_node:
        raise ValueError("Security node missing SECNAME node")

    unique_id = get_text_or_raise(unique_id_node)
    name = get_text_or_raise(name_node)

    ticker = None
    ticker_node = node.find("ticker")
    if ticker_node and ticker_node.text:
        ticker = ticker_node.text.strip()

    memo = None
    memo_node = node.find("memo")
    if memo_node and memo_node.text:
        ticker = memo_node.text.strip()

    return {
        "unique_id": unique_id,
        "name": name,
        "ticker": ticker,
        "memo": memo,
    }



def _default_position() -> tp.Position:
    return {
        "security": "N/A",
        "units": decimal.Decimal(0),
        "unit_price": decimal.Decimal(0),
        "market_value": decimal.Decimal(0),
        "date": datetime.datetime(1970, 12, 31)
    }


def parse_investment_position(node: ET.Element) -> tp.Position:
    position = _default_position()
    apply = apply_contents(position)
    apply(node, "uniqueid", alias="security")
    apply(node, "units", to_decimal)
    apply(node, "unit_price", to_decimal, alias="unit_price")
    apply(node, "mktval", to_decimal, alias="market_value")
    apply(node, "dtpriceasof", from_ofx_date, alias="market_value")
    return position


def _default_investment_transaction(name: str) -> tp.InvestmentTransaction:
    pass


def parse_investment_transaction(node) -> tp.InvestmentTransaction:
    transaction = _default_investment_transaction(node.name)
    apply = apply_contents(transaction)
    apply(node, "fitid")
    apply(node, "memo")
    apply(node, "dttrade", "trade_date", transform=from_ofx_date)
    apply(node, "dtsettle", "settle_date", transform=from_ofx_date)
    apply(node, "uniqueid", "security")
    apply(node, "incometype", "income_type")
    apply(node, "units", transform=to_decimal)
    apply(node, "unitprice", "unit_price", transform=to_decimal)
    apply(node, "commission", transform=to_decimal)
    apply(node, "fees", transform=to_decimal)
    apply(node, "total", transform=to_decimal)
    apply(node, "inv401ksource")
    apply(node, "tferaction")
    return transaction


def _default_investment_statement() -> tp.InvestmentStatement:
    return {
        "start_date": datetime.datetime(1970, 12, 31),
        "end_date": datetime.datetime(1970, 12, 31),
        "currency": "USD",
        "transactions": [],
        "discarded_entries": [],
        "warnings": [],
        "balances": [],
        "positions": [],
        "available_cash": decimal.Decimal(0),
        "margin_balance": decimal.Decimal(0),
        "short_balance": decimal.Decimal(0),
        "buying_power": decimal.Decimal(0),
    }


def _default_brokerage_balance() -> tp.BrokerageBalance:
    return {
        "name": "",
        "description": "",
        "value": decimal.Decimal(0),
    }


def parse_investment_statement(
    node: ET.Element,
) -> tp.InvestmentStatement:
    statement = _default_investment_statement()
    apply = apply_contents(statement)
    apply(node, "curdef", str.lower, alias="currency")

    with has_node(node, "invtranlist") as invtranlist_ofx:
        apply(node, "dtstart", str.lower, alias="start_date")
        apply(node, "dtend", str.lower, alias="end_date")

    for transaction_type in TX_TYPES:
        for tx_node in node.findall(transaction_type):
            statement["positions"].append(
                parse_investment_position(tx_node))

    for transaction_type in AGGREGATE_TYPES:
        for tx_node in node.findall(transaction_type):
            statement["transactions"].append(
                parse_investment_transaction(tx_node))

    for transaction_node in node.findall("invbanktran"):
        for tx_node in transaction_node.findall("stmttrn"):
            statement["transactions"].append(parse_transaction(tx_node))

    with has_node(node, "invbal") as invbal:
        apply(invbal, "availcash", to_decimal, "available_cash")
        apply(invbal, "marginbalance", to_decimal, "margin_balance")
        apply(invbal, "shortbalance", to_decimal, "short_balance")
        apply(invbal, "buypower", to_decimal, "buying_power")

        with has_node(invbal, "ballist") as ballist:
            for balance_node in ballist.findall("bal"):
                balance = _default_brokerage_balance()
                apply = apply_contents(balance)
                apply(balance_node, "name")
                apply(balance_node, "desc", alias="description")
                apply(balance_node, "value", to_decimal)
                statement["balances"].append(balance)

    return statement


def _default_institution():
    return {"organization": str, "fid": str}


def parse_org(node) -> tp.Institution:
    institution = _default_institution()
    apply = apply_contents(institution)
    apply(node, "org", alias="organization")
    apply(node, "fid")
    return institution


def _default_signon() -> tp.Signon:
    return {
        "code": None,
        "severity": None,
        "message": None,
        "dtserver": None,
        "language": None,
        "dtprofup": None,
        "org": None,
        "fid": None,
        "intu_bid": None,
    }

def parse_signon_response(node: ET.Element) -> tp.Signon:
    signon = _default_signon()
    apply = apply_contents(signon)

    for status_node in with_node(node, "status"):
        apply(status_node, "code", int)
        apply(status_node, "severity")
        apply(status_node, "message", lambda x: "" if x is None else x)

    for status_node in with_node(node, "fi"):
        apply(status_node, "org")
        apply(status_node, "fid")

    apply(node, "intu.bid")
    apply(node, "language")
    apply(node, "dtserver", from_ofx_date)
    apply(node, "dtprofup", from_ofx_date)

    return signon


def _default_account() -> tp.Account:
    return {
        "account_id": "",
        "routing_number": "",
        "account_type": "",
        "description": "",
        "branch_id": None,
        "broker_id": None,
    }


def parse_account(node: ET.Element) -> tp.Account:
    account = _default_account()
    apply = apply_contents(account)
    apply(node, "desc", alias="description")

    try:
        account_info = next(chain(
            node.iter("BANKACCTFROM"),
            node.iter("CCACCTROM"),
            node.iter("INVACCTFROM"),
        ))
    except StopIteration as error:
        raise ValueError(
            "None of 'BANKACCTFROM', 'CCACCTFROM' "
            "or 'INVACCTFROM' was found"
        ) from error

    apply(account_info, "acctid", alias="account_id")
    apply(account_info, "bankid", alias="routing_number")
    apply(account_info, "branchid", alias="branch_id")
    apply(account_info, "accttype", alias="account_type")
    apply(account_info, "brokerid", alias="broker_id")
    return account


def parse_balance(
    statement: tp.Statement,
    node,
    bal_tag_name,
    bal_attr,
    bal_date_attr,
    bal_type_string,
) -> None:
    bal_tag = node.find(bal_tag_name)
    if not bal_tag or not bal_tag.text:
        return

    with has_node(bal_tag, "balamt") as balamt_tag:
        try:
            statement["balance"] = to_decimal(balamt_tag)
        except (IndexError, decimal.InvalidOperation) as error:
            raise ValueError(
                "Empty %s balance " % bal_type_string
            ) from error

    with has_node(bal_tag, "dtasof") as dtasof_tag:
        if not dtasof_tag.text:
            raise ValueError("Missing balance date")
        statement["balance_date"] = from_ofx_date(dtasof_tag.text)


def _default_statement() -> tp.Statement:
    return {
        "start_date": datetime.datetime(1970, 12, 31),
        "end_date": datetime.datetime(1970, 12, 31),
        "currency": "USD",
        "transactions": [],
        "discarded_entries": [],
        "warnings": [],
        "balance": decimal.Decimal(0),
        "balance_date": datetime.datetime(1970, 12, 31),
        "available_balance": decimal.Decimal(0),
        "available_balance_date": datetime.datetime(1970, 12, 31),
    }


def parse_statement(node: ET.Element) -> tp.Statement:
    """
    Parse a statement in ofx-land and return a Statement object.
    """
    statement = _default_statement()
    apply = apply_contents(statement)
    apply(node, "dstart", from_ofx_date, alias="start_date")
    apply(node, "dtend", from_ofx_date, alias="end_date")
    apply(node, "curdef", from_ofx_date, alias="currency")
    parse_balance(statement, node, "ledgerbal",
                     "balance", "balance_date", "ledger")
    parse_balance(statement, node, "availbal", "available_balance",
                     "available_balance_date", "ledger")

    for tx_node in node.findall("stmttrn"):
        statement["transactions"].append(
            parse_transaction(tx_node))

    return statement


def _default_transaction() -> tp.Transaction:
    return {
        "payee": "",
        "type": "",
        "date": datetime.datetime(1970, 12, 31),
        "user_date": datetime.datetime(1970, 12, 31),
        "amount": decimal.Decimal(0),
        "id": "",
        "memo": "",
        "sic": None,
        "mcc": "",
        "checknum": "",
    }


def _amount_to_decimal(val: str) -> decimal.Decimal:

    try:
        return decimal.Decimal(val)
    except decimal.InvalidOperation:
        if val in {"null", "-null"}:
            return decimal.Decimal(0)
        else:
            raise ValueError(
                "Invalid Transaction Amount: '%s'"
                "" % val
            )


def parse_transaction(node) -> tp.Transaction:
    """
    Parse a transaction in ofx-land and return a Transaction object.
    """
    transaction = _default_transaction()
    apply = apply_contents(transaction)
    apply(node, "trntype", str.lower, alias="type")
    apply(node, "name", alias="payee")
    apply(node, "memo")
    apply(node, "trnamt", _amount_to_decimal, alias="amount")
    apply(node, "dtposted", from_ofx_date, alias="date")
    apply(node, "dtuser", from_ofx_date, alias="user_date")
    apply(node, "fitid", alias="id")
    apply(node, "sic")
    apply(node, "checknum")

    if transaction["sic"] is not None and transaction["sic"] in mcc.codes:
        try:
            transaction["mcc"] = mcc.codes[
                transaction["sic"]]["combined description"]
        except IndexError:
            raise ValueError(
                "Empty transaction Merchant Category Code (MCC)")

    return transaction


def to_decimal(tag) -> decimal.Decimal:
    d = tag.contents[0].strip()
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
    return decimal.Decimal(d)
