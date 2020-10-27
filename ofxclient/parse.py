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

from . import mcc, types as tp

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
        child_node = node.find(name)
        if not child_node or not child_node.text:
            return

        try:
            contents = child_node.text.strip()
        except Exception as error:
            raise ValueError(
                "Error while trying to extract contents "
                f"from tag {name}"
            ) from error

        try:
            for t in transform:
                contents = t(contents)
        except Exception as error:
            raise RuntimeError(
                "Error while trying to transform contents "
                f"from tag {name} using transform {t}"
            ) from error

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
            <ORG>{signon['fi_org']}</ORG>
            <FID>{signon['fi_fid']}</FID>
        </FI>
    </SONRS>
    <INTU.BID>{signon['intu_bid']}</INTU.BID>
</SIGNONMSGSRSV1>
"""


def parse_ofx(
    file_path: str,
    fail_fast: bool = True,
    custom_date_format: t.Optional[str] = None,
) -> t.Iterable[tp.OFXAccount]:
    """
    parse is the main entry point for an OfxParser. It takes a file
    handle and an optional log_errors flag.

    If fail_fast is True, the parser will fail on any errors.
    If fail_fast is False, the parser will log poor statements in the
    statement class and continue to run. Note: the library does not
    guarantee that no exceptions will be raised to the caller, only
    that statements will include bad transactions (which are marked).

    """
    with open(file_path, "r") as file:
        ofx_str = file.read()

    headers = clean_headers(
        decode_headers(extract_headers(ofx_str)))
    cleaned = clean_ofx_xml(ofx_str)
    accounts = []
    security_list = []
    status: t.Optional[t.Dict[str, t.Any]] = None
    signon = None

    node = ET.fromstring(cleaned)

    if node.find("ofx") is None:
        raise ValueError("The ofx file is empty!")

    with has_node(node, "sonrs") as sonrs:
        signon = parse_signon_response(sonrs)

    with has_node(node, "stmttrnrs") as transactions:
        with has_node(node, "status") as status_node:
            status = {}
            apply = apply_contents(status)
            apply(status_node, "code", int)
            apply(status_node, "severity")
            apply(status_node, "message")

    with has_node(node, "ccstmtrs") as cc_transactions:
        cc_transactions_status = cc_transactions.find("status")
        if cc_transactions_status:
            status = {}
            status["code"] = int(
                cc_transactions_status.find("code").contents[0].strip()
            )
            status["severity"] = cc_transactions_status.find(
                "severity").contents[0].strip()
            message = cc_transactions_status.find("message")
            status["message"] = (
                message.contents[0].strip()
                if message else None
            )

    with has_nodes(node, "stmtrs") as statements:
        for account in parse_accounts(statements, tp.AccountType.Bank):
            accounts.append(account)

    with has_nodes(node, "ccstmtrs") as cc_statements:
        for account in parse_accounts(cc_statements, tp.AccountType.CreditCard):
            accounts.append(account)

    with has_nodes(node, "invstmtrs") as investments:
        for account in parse_investment_accounts(investments):
            accounts.append(account)

        with has_node(investments, "seclist") as securities:
            for security in parse_security_list(securities):
                security_list.append(security)

    with has_node(node, "acctinfors") as account_info:
        for account in parse_account_info(account_info, node):
            accounts.append(account)

    with has_node(node, "fi") as fi_ofx:
        for account in accounts:
            account["institution"] = parse_org(fi_ofx)

    return accounts


def parse_ofx_date(
    date_str: str,
    format: t.Optional[str] = None,
) -> datetime.datetime:
    # dateAsString looks something like 20101106160000.00[-5:EST]
    # for 6 Nov 2010 4pm UTC-5 aka EST

    # Some places (e.g. Newfoundland) have non-integer offsets.
    res = re.search(r"\[(?P<tz>[-+]?\d+\.?\d*)\:\w*\]$", date_str)
    if res:
        tz = float(res.group("tz"))
    else:
        tz = 0

    tz_offset = datetime.timedelta(hours=tz)

    res = re.search(r"^[0-9]*\.([0-9]{0,5})", date_str)
    if res:
        msec = datetime.timedelta(seconds=float("0." + res.group(1)))
    else:
        msec = datetime.timedelta(seconds=0)

    try:
        local_date = datetime.datetime.strptime(date_str[:14], "%Y%m%d%H%M%S")
        return local_date - tz_offset + msec
    except ValueError:
        if date_str[:8] == "00000000":
            raise

        if not format:
            return datetime.datetime.strptime(
                date_str[:8], "%Y%m%d") - tz_offset + msec
        else:
            return datetime.datetime.strptime(
                date_str[:8], format) - tz_offset + msec


def parse_account_info(
    account_info: ET.Element,
    node: ET.Element,
) -> t.Iterable[tp.OFXAccount]:
    fi = node.find("fi")

    institution: t.Optional[tp.Institution]

    if fi:
        institution = parse_org(fi)
    else:
        institution = None

    for node in account_info.findall("acctinfo"):
        if node.find("bankacctinfo"):
            for account in parse_accounts([node], tp.AccountType.Bank):
                account["institution"] = institution
                yield account

        if node.find("ccacctinfo"):
            for account in parse_accounts([node], tp.AccountType.CreditCard):
                account["institution"] = institution
                yield account

        if node.find("invacctinfo"):
            for inv_account in parse_investment_accounts([node]):
                inv_account["institution"] = institution
                yield account

        # TODO: description field for accounts.


def _default_investment_account() -> tp.InvestmentAccount:
    return {
        "currency": "USD",
        "statement": None,
        "account_id": "",
        "routing_number": "",
        "branch_id": "",
        "account_type": "",
        "institution": None,
        "type": tp.AccountType.Unknown,
        "warnings": [],
        "description": "",
        "broker_id": "",
    }


def parse_investment_accounts(
    node: t.Iterable[ET.Element],
    fail_fast: bool = True,
) -> t.Iterable[tp.InvestmentAccount]:
    for child_node in node:
        account = _default_investment_account()
        apply = apply_contents(account)
        apply(child_node, "acctid", alias="account_id")
        apply(child_node, "brokerid", alias="broker_id")
        account["type"] = tp.AccountType.Investment
        account["statement"] = parse_investment_statement(child_node)
        yield account


def parse_security_list(
    node: ET.Element,
) -> t.Iterable[tp.Security]:
    rv: t.List[tp.Security] = []
    for security_info in node.findall("secinfo"):
        unique_id_node = security_info.find("uniqueid")
        name_node = security_info.find("secname")

        if unique_id_node and name_node:
            unique_id = get_text_or_raise(unique_id_node)
            name = get_text_or_raise(name_node)

            ticker = None
            ticker_node = security_info.find("ticker")
            if ticker_node and ticker_node.text:
                ticker = ticker_node.text.strip()


            memo = None
            memo_node = security_info.find("memo")
            if memo_node and memo_node.text:
                ticker = memo_node.text.strip()

            yield {
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
        "date": datetime.datetime(0, 0, 0)
    }


def parse_investment_position(node) -> tp.Position:
    position = _default_position()
    apply = apply_contents(position)
    apply(node, "uniqueid", alias="security")
    apply(node, "units", to_decimal)
    apply(node, "unit_price", to_decimal, alias="unit_price")
    apply(node, "mktval", to_decimal, alias="market_value")
    apply(node, "dtpriceasof", parse_ofx_date, alias="market_value")
    return position


def _default_investment_transaction(name: str) -> tp.InvestmentTransaction:
    pass


def parse_investment_transaction(node) -> tp.InvestmentTransaction:
    transaction = _default_investment_transaction(node.name)
    apply = apply_contents(transaction)
    apply(node, "fitid")
    apply(node, "memo")
    apply(node, "dttrade", "trade_date", transform=parse_ofx_date)
    apply(node, "dtsettle", "settle_date", transform=parse_ofx_date)
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
        "start_date": datetime.datetime(0, 0, 0),
        "end_date": datetime.datetime(0, 0, 0),
        "currency": "USD",
        "transactions": [],
        "discarded_entries": [],
        "warnings": [],
        "balance_list": [],
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
    fail_fast: bool = True
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
            for balance_node in ballist.findAll("bal"):
                balance = _default_brokerage_balance()
                apply = apply_contents(balance)
                apply(balance_node, "name")
                apply(balance_node, "desc", alias="description")
                apply(balance_node, "value", to_decimal)
                statement["balance_list"].append(balance)

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
        "fi_org": None,
        "fi_fid": None,
        "intu_bid": None,
        "success": False,
    }

def parse_signon_response(node) -> tp.Signon:
    signon = _default_signon()
    apply = apply_contents(signon)
    apply(node, "code", int)
    apply(node, "severity")
    apply(node, "dserver")
    apply(node, "language")
    apply(node, "dtprofup")
    apply(node, "org")
    apply(node, "fid")
    apply(node, "intu.bid")
    apply(node, "message", lambda x: "" if x is None else x)
    return signon


def _default_account() -> tp.OFXAccount:
    return {
        "currency": "USD",
        "statement": None,
        "account_id": "",
        "routing_number": "",
        "branch_id": "",
        "account_type": "",
        "institution": None,
        "type": tp.AccountType.Unknown,
        "warnings": [],
        "description": "",
    }


def parse_accounts(
    statements: t.Iterable[ET.Element],
    account_type: tp.AccountType,
) -> t.Iterable[tp.OFXAccount]:
    """ Parse the <STMTRS> tags and return a list of Accounts object. """
    for statement in statements:
        account = _default_account()
        apply = apply_contents(account)
        apply(statement, "curdef", alias="currency")
        apply(statement, "acctid", alias="account_id")
        apply(statement, "bankid", alias="bank_id")
        apply(statement, "branchid", alias="branch_id")
        apply(statement, "accttype", alias="account_type")
        account["statement"] = parse_statement(statement)
        yield account


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
        statement["balance_date"] = parse_ofx_date(dtasof_tag.text)


def _default_statement() -> tp.Statement:
    return {
        "start_date": datetime.datetime(0, 0, 0),
        "end_date": datetime.datetime(0, 0, 0),
        "currency": "USD",
        "transactions": [],
        "discarded_entries": [],
        "warnings": [],
        "balance": decimal.Decimal(0),
        "balance_date": datetime.datetime(0, 0, 0),
        "available_balance": decimal.Decimal(0),
        "available_balance_date": datetime.datetime(0, 0, 0),
    }


def parse_statement(node: ET.Element, fail_fast: bool = True) -> tp.Statement:
    """
    Parse a statement in ofx-land and return a Statement object.
    """
    statement = _default_statement()
    apply = apply_contents(statement)
    apply(node, "dstart", parse_ofx_date, alias="start_date")
    apply(node, "dtend", parse_ofx_date, alias="end_date")
    apply(node, "curdef", parse_ofx_date, alias="currency")
    parse_balance(statement, node, "ledgerbal",
                     "balance", "balance_date", "ledger")
    parse_balance(statement, node, "availbal", "available_balance",
                     "available_balance_date", "ledger")

    for tx_node in node.findall("stmttrn"):
        try:
            statement["transactions"].append(
                parse_transaction(tx_node))
        except ValueError:
            ofx_error = sys.exc_info()[1]
            statement["discarded_entries"].append({
                "error": str(ofx_error),
                "content": tx_node,
            })
            if fail_fast:
                raise

    return statement


def _default_transaction() -> tp.Transaction:
    return {
        "payee": "",
        "type": "",
        "date": datetime.datetime(0, 0, 0),
        "user_date": datetime.datetime(0, 0, 0),
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
    apply(node, "dtposted", parse_ofx_date, alias="date")
    apply(node, "dtuser", parse_ofx_date, alias="user_date")
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
