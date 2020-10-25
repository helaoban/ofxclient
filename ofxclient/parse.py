import codecs
from collections import OrderedDict
from collections.abc import Iterable
import contextlib
import decimal
import datetime
from io import StringIO, BytesIO
import re
import sys
import typing as t

from bs4 import BeautifulSoup

from . import mcc

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


def clean_headers(headers: OrderedDict) -> OrderedDict:
    rv = OrderedDict()
    for header, value in self.headers.items():
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
    # decode the headers using ascii
    ascii_headers = OrderedDict(
        (
            key.decode("ascii", "replace"),
            value.decode("ascii", "replace"),
        )
        for key, value in headers.items()
    )

    enc_type = ascii_headers.get("ENCODING")

    if not enc_type:
        # no encoding specified, use the ascii-decoded headers
        return ascii_headers

    if enc_type == "USASCII":
        cp = ascii_headers.get("CHARSET", "1252")
        if cp == "8859-1":
            encoding = "iso-8859-1"
        else:
            encoding = "cp%s" % (cp, )

    elif enc_type in ("UNICODE", "UTF-8"):
        encoding = "utf-8"

    # Decode the headers using the encoding
    return OrderedDict(
        (key.decode(encoding), value.decode(encoding))
        for key, value in headers.items()
    )


def extract_headers(ofx_str: str) -> OrderedDict:
    rv = OrderedDict()
    head_data = self.fh.read(1024 * 10)
    head_data = head_data[:head_data.find(b"<")]

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
        re.findall(r"(?i)</([a-z0-9_\.]+)>", ofx_string)
    ]

    # close all tags that don't have closing tags and
    # leave all other data intact
    last_open_tag = None
    tokens = re.split(r"(?i)(</?[a-z0-9_\.]+>)", ofx_string)
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
                last_open_tag = tag_nam
        cleaned = cleaned + token
    return cleaned


class AccountType(object):
    Unknown = 0
    Bank = 1
    CreditCard = 2
    Investment = 3


class Account(object):
    def __init__(self):
        self.curdef = None
        self.statement = None
        self.account_id = ""
        self.routing_number = ""
        self.branch_id = ""
        self.account_type = ""
        self.institution = None
        self.type = AccountType.Unknown
        # Used for error tracking
        self.warnings = []


class InvestmentAccount(Account):
    def __init__(self):
        super(InvestmentAccount, self).__init__()
        self.brokerid = ""


class BrokerageBalance:
    def __init__(self):
        self.name = None
        self.description = None
        self.value = None  # decimal


class Security:
    def __init__(self, uniqueid, name, ticker, memo):
        self.uniqueid = uniqueid
        self.name = name
        self.ticker = ticker
        self.memo = memo


class Signon:
    def __init__(self, keys):
        self.code = keys["code"]
        self.severity = keys["severity"]
        self.message = keys["message"]
        self.dtserver = keys["dtserver"]
        self.language = keys["language"]
        self.dtprofup = keys["dtprofup"]
        self.fi_org = keys["org"]
        self.fi_fid = keys["fid"]
        self.intu_bid = keys["intu.bid"]

        if int(self.code) == 0:
            self.success = True
        else:
            self.success = False

    def __str__(self):
        rv = "\t<SIGNONMSGSRSV1>\r\n" + "\t\t<SONRS>\r\n" + \
              "\t\t\t<STATUS>\r\n"
        rv += "\t\t\t\t<CODE>%s\r\n" % self.code
        rv += "\t\t\t\t<SEVERITY>%s\r\n" % self.severity
        if self.message:
            rv += "\t\t\t\t<MESSAGE>%s\r\n" % self.message
        rv += "\t\t\t</STATUS>\r\n"
        if self.dtserver is not None:
            rv += "\t\t\t<DTSERVER>" + self.dtserver + "\r\n"
        if self.language is not None:
            rv += "\t\t\t<LANGUAGE>" + self.language + "\r\n"
        if self.dtprofup is not None:
            rv += "\t\t\t<DTPROFUP>" + self.dtprofup + "\r\n"
        if (self.fi_org is not None) or (self.fi_fid is not None):
            rv += "\t\t\t<FI>\r\n"
            if self.fi_org is not None:
                rv += "\t\t\t\t<ORG>" + self.fi_org + "\r\n"
            if self.fi_fid is not None:
                rv += "\t\t\t\t<FID>" + self.fi_fid + "\r\n"
            rv += "\t\t\t</FI>\r\n"
        if self.intu_bid is not None:
            rv += "\t\t\t<INTU.BID>" + self.intu_bid + "\r\n"
        rv += "\t\t</SONRS>\r\n"
        rv += "\t</SIGNONMSGSRSV1>\r\n"
        return rv


class Statement(object):
    def __init__(self):
        self.start_date = ""
        self.end_date = ""
        self.currency = ""
        self.transactions = []
        # Error tracking:
        self.discarded_entries = []
        self.warnings = []


class InvestmentStatement(object):
    def __init__(self):
        self.positions = []
        self.transactions = []
        # Error tracking:
        self.discarded_entries = []
        self.warnings = []


class Transaction(object):
    def __init__(self):
        self.payee = ""
        self.type = ""
        self.date = None
        self.user_date = None
        self.amount = None
        self.id = ""
        self.memo = ""
        self.sic = None
        self.mcc = ""
        self.checknum = ""

    def __repr__(self):
        return "<Transaction units=" + str(self.amount) + ">"


class InvestmentTransaction(object):
    def __init__(self, type):
        self.type = type.lower()
        self.tradeDate = None
        self.settleDate = None
        self.memo = ""
        self.security = ""
        self.income_type = ""
        self.units = decimal.Decimal(0)
        self.unit_price = decimal.Decimal(0)
        self.commission = decimal.Decimal(0)
        self.fees = decimal.Decimal(0)
        self.total = decimal.Decimal(0)
        self.tferaction = None

    def __repr__(self):
        return "<InvestmentTransaction type=" + str(self.type) + ", \
            units=" + str(self.units) + ">"


class Position(object):
    def __init__(self):
        self.security = ""
        self.units = decimal.Decimal(0)
        self.unit_price = decimal.Decimal(0)
        self.market_value = decimal.Decimal(0)


class Institution(object):
    def __init__(self):
        self.organization = ""
        self.fid = ""


def parse_ofx(
    file_path: str,
    fail_fast: bool = True,
    custom_date_format: t.Optional[str] = None
):
    """
    parse is the main entry point for an OfxParser. It takes a file
    handle and an optional log_errors flag.

    If fail_fast is True, the parser will fail on any errors.
    If fail_fast is False, the parser will log poor statements in the
    statement class and continue to run. Note: the library does not
    guarantee that no exceptions will be raised to the caller, only
    that statements will include bad transactions (which are marked).

    """
    with open(file_path, "r"):
        ofx_str = file_path.read()

    headers = clean_headers(
        decode_headers(extract_headers(ofx_str))
    cleaned = clean_ofx_xml(ofx_str)
    accounts = []
    signon = None

    tree = BeautifulSoup(cleaned, "html.parser")
    if tree.find("ofx") is None:
        raise ValueError("The ofx file is empty!")

    signon_ofx = tree.find("sonrs")
    if signon_ofx:
        signon = parse_signon_response(signon_ofx)

    transactions = tree.find("stmttrnrs")
    if transactions:
        transactions_trnuid = transactions.find("trnuid")
        if transactions_trnuid:
            trnuid = transactions_trnuid.contents[0].strip()

        transactions_status = transactions.find("status")
        if transactions_status:
            status = {}
            status["code"] = int(
                transactions_status.find("code").contents[0].strip()
            )
            status["severity"] = \
                transactions_status.find("severity").contents[0].strip()
            message = transactions_status.find("message")
            status["message"] = \
                message.contents[0].strip() if message else None

    cc_transactions = tree.find("ccstmttrnrs")
    if cc_transactions:
        cc_transactions_trnuid = cc_transactions.find("trnuid")
        if cc_transactions_trnuid:
            trnuid = cc_transactions_trnuid.contents[0].strip()

        cc_transactions_status = cc_transactions.find("status")
        if cc_transactions_status:
            status = {}
            status["code"] = int(
                cc_transactions_status.find("code").contents[0].strip()
            )
            status["severity"] = \
                cc_transactions_status.find("severity").contents[0].strip()
            message = cc_transactions_status.find("message")
            status["message"] = \
                message.contents[0].strip() if message else None

    statements = tree.findAll("stmtrs")
    if statements:
        accounts += parse_accounts(statements, AccountType.Bank)

    ccstmtrs_ofx = tree.findAll("ccstmtrs")
    cc_statements = tree.findAll("ccstmtrs")
    if ccstmtrs_ofx:
        accounts += parse_accounts(
            ccstmtrs_ofx, AccountType.CreditCard)

    investments = tree.findAll("invstmtrs")
    if investments:
        accounts += parse_investment_accounts(investments)
        security_list = tree.find("seclist")
        if security_list:
            security_list = parse_security_list(security_list)
        else:
            security_list = None

    account_info = tree.find("acctinfors")
    if account_info:
        accounts += parse_account_info(account_info, tree)

    fi_ofx = tree.find("fi")
    if fi_ofx:
        for account in accounts:
            account.institution = parse_org(fi_ofx)

    if accounts:
        account = accounts[0]

    return ofx_obj


def parse_ofx_date(
    date_str: str,
    format: t.Optional[str] = None,
) -> datetime:
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
            return None

        if not format:
            return datetime.datetime.strptime(
                date_str[:8], "%Y%m%d") - tz_offset + msec
        else:
            return datetime.datetime.strptime(
                date_str[:8], format) - tz_offset + msec


def parse_account_info(account_info, tree) -> t.Iterable[Account]:
    rv = []
    for account in account_info.findAll("acctinfo"):
        accounts = []
        if account.find("bankacctinfo"):
            accounts += parse_accounts([account], AccountType.Bank)
        elif account.find("ccacctinfo"):
            accounts += parse_accounts([account], AccountType.CreditCard)
        elif account.find("invacctinfo"):
            accounts += parse_investment_accounts([account])
        else:
            continue

        fi = tree.find("fi")
        if fi:
            for account in rv:
                account.institution = parse_org(fi)

        desc = account.find("desc")
        if hasattr(desc, "contents"):
            for account in accounts:
                account.desc = desc.contents[0].strip()
        rv += accounts
    return rv


def parse_investment_accounts(
    investments, fail_fast: bool = True
) -> t.Iterable[InvestmentAccount]:
    rv = []
    for investment in investments:
        account = InvestmentAccount()
        account_id = investment.find("acctid")
        if hasattr(account_id, "contents"):
            try:
                account.account_id = account_id.contents[0].strip()
            except IndexError:
                account.warnings.append(
                    "Empty acctid tag for %s" % investment)
                if fail_fast:
                    raise

        broker_id = investment.find("brokerid")
        if hasattr(broker_id, "contents"):
            try:
                account.brokerid = broker_id.contents[0].strip()
            except IndexError:
                account.warnings.append(
                    "Empty brokerid tag for %s" % investment)
                if fail_fast:
                    raise

        account.type = AccountType.Investment

        if investment:
            account.statement = parse_investment_statement(investment)
        rv.append(account)
    return rv


def parse_security_list(node) -> t.Iterable[Security]:
    rv = []
    for security_info in node.findAll("secinfo"):
        unique_id = security_info.find("uniqueid")
        name = security_info.find("secname")
        ticker = security_info.find("ticker")
        memo = security_info.find("memo")
        if unique_id and name:
            try:
                ticker = ticker.contents[0].strip()
            except AttributeError:
                # ticker can be empty
                ticker = None
            try:
                memo = memo.contents[0].strip()
            except AttributeError:
                # memo can be empty
                memo = None
            rv.append(
                Security(
                    unique_id.contents[0].strip(),
                     name.contents[0].strip(),
                     ticker,
                     memo
                )
            )
    return rv


def parse_investment_position(tree):
    position = Position()
    tag = tree.find("uniqueid")
    if hasattr(tag, "contents"):
        position.security = tag.contents[0].strip()
    tag = tree.find("units")
    if hasattr(tag, "contents"):
        position.units = to_decimal(tag)
    tag = tree.find("unitprice")
    if hasattr(tag, "contents"):
        position.unit_price = to_decimal(tag)
    tag = tree.find("mktval")
    if hasattr(tag, "contents"):
        position.market_value = to_decimal(tag)
    tag = tree.find("dtpriceasof")
    if hasattr(tag, "contents"):
        try:
            position.date = parse_ofx_date(tag.contents[0].strip())
        except ValueError:
            raise
    return position


def parse_investment_transaction(tree):
    transaction = InvestmentTransaction(tree.name)
    tag = tree.find("fitid")
    if hasattr(tag, "contents"):
        transaction.id = tag.contents[0].strip()
    tag = tree.find("memo")
    if hasattr(tag, "contents"):
        transaction.memo = tag.contents[0].strip()
    tag = tree.find("dttrade")
    if hasattr(tag, "contents"):
        try:
            transaction.tradeDate = parse_ofx_date(
                tag.contents[0].strip())
        except ValueError:
            raise
    tag = tree.find("dtsettle")
    if hasattr(tag, "contents"):
        try:
            transaction.settleDate = parse_ofx_date(
                tag.contents[0].strip())
        except ValueError:
            raise
    tag = tree.find("uniqueid")
    if hasattr(tag, "contents"):
        transaction.security = tag.contents[0].strip()
    tag = tree.find("incometype")
    if hasattr(tag, "contents"):
        transaction.income_type = tag.contents[0].strip()
    tag = tree.find("units")
    if hasattr(tag, "contents"):
        transaction.units = to_decimal(tag)
    tag = tree.find("unitprice")
    if hasattr(tag, "contents"):
        transaction.unit_price = to_decimal(tag)
    tag = tree.find("commission")
    if hasattr(tag, "contents"):
        transaction.commission = to_decimal(tag)
    tag = tree.find("fees")
    if hasattr(tag, "contents"):
        transaction.fees = to_decimal(tag)
    tag = tree.find("total")
    if hasattr(tag, "contents"):
        transaction.total = to_decimal(tag)
    tag = tree.find("inv401ksource")
    if hasattr(tag, "contents"):
        transaction.inv401ksource = tag.contents[0].strip()
    tag = tree.find("tferaction")
    if hasattr(tag, "contents"):
        transaction.tferaction = tag.contents[0].strip()
    return transaction


def parse_investment_statement(node):
    statement = InvestmentStatement()
    currency_tag = node.find("curdef")
    if hasattr(currency_tag, "contents"):
        statement.currency = currency_tag.contents[0].strip().lower()
    invtranlist_ofx = node.find("invtranlist")
    if invtranlist_ofx is not None:
        tag = invtranlist_ofx.find("dtstart")
        if hasattr(tag, "contents"):
            try:
                statement.start_date = parse_ofx_date(
                    tag.contents[0].strip())
            except IndexError:
                statement.warnings.append("Empty start date.")
                if fail_fast:
                    raise
            except ValueError:
                e = sys.exc_info()[1]
                statement.warnings.append("Invalid start date: %s" % e)
                if fail_fast:
                    raise

        tag = invtranlist_ofx.find("dtend")
        if hasattr(tag, "contents"):
            try:
                statement.end_date = parse_ofx_date(
                    tag.contents[0].strip())
            except IndexError:
                statement.warnings.append("Empty end date.")
            except ValueError:
                e = sys.exc_info()[1]
                statement.warnings.append("Invalid end date: %s" % e)
                if fail_fast:
                    raise

    for transaction_type in TX_TYPES:
        try:
            for investment_ofx in node.findAll(transaction_type):
                statement.positions.append(
                    parse_investment_position(investment_ofx))
        except (ValueError, IndexError, decimal.InvalidOperation,
                TypeError):
            e = sys.exc_info()[1]
            if fail_fast:
                raise
            statement.discarded_entries.append({
                "error": "Error parsing positions: " + str(e),
                "content": investment_ofx
            })

    for transaction_type in AGGREGATE_TYPES:
        try:
            for investment_ofx in node.findAll(transaction_type):
                statement.transactions.append(
                    parse_investment_transaction(investment_ofx))
        except (ValueError, IndexError, decimal.InvalidOperation):
            e = sys.exc_info()[1]
            if fail_fast:
                raise
            statement.discarded_entries.append({
                "error": transaction_type + ": " + str(e),
                "content": investment_ofx
            })

    for transaction_node in node.findAll("invbanktran"):
        for tree in transaction_node.findAll("stmttrn"):
            try:
                statement.transactions.append(
                    parse_transaction(tree))
            except ValueError:
                ofxError = sys.exc_info()[1]
                statement.discarded_entries.append(
                    {"error": str(ofxError), "content": transaction_node})
                if fail_fast:
                    raise

    invbal_ofx = node.find("invbal")
    if invbal_ofx is not None:
        # <AVAILCASH>18073.98<MARGINBALANCE>+00000000000.00<SHORTBALANCE>+00000000000.00<BUYPOWER>+00000000000.00
        availcash_ofx = invbal_ofx.find("availcash")
        if availcash_ofx is not None:
            statement.available_cash = to_decimal(availcash_ofx)
        margin_balance_ofx = invbal_ofx.find("marginbalance")
        if margin_balance_ofx is not None:
            statement.margin_balance = to_decimal(margin_balance_ofx)
        short_balance_ofx = invbal_ofx.find("shortbalance")
        if short_balance_ofx is not None:
            statement.short_balance = to_decimal(short_balance_ofx)
        buy_power_ofx = invbal_ofx.find("buypower")
        if buy_power_ofx is not None:
            statement.buy_power = to_decimal(buy_power_ofx)

        ballist_ofx = invbal_ofx.find("ballist")
        if ballist_ofx is not None:
            statement.balance_list = []
            for balance_ofx in ballist_ofx.findAll("bal"):
                brokerage_balance = BrokerageBalance()
                name_ofx = balance_ofx.find("name")
                if name_ofx is not None:
                    brokerage_balance.name = name_ofx.contents[0].strip()
                description_ofx = balance_ofx.find("desc")
                if description_ofx is not None:
                    brokerage_balance.description = \
                        description_ofx.contents[0].strip()
                value_ofx = balance_ofx.find("value")
                if value_ofx is not None:
                    brokerage_balance.value = to_decimal(value_ofx)
                statement.balance_list.append(brokerage_balance)

    return statement


def parse_org(node):
    institution = Institution()
    org = node.find("org")
    if hasattr(org, "contents"):
        institution.organization = org.contents[0].strip()

    fid = node.find("fid")
    if hasattr(fid, "contents"):
        institution.fid = fid.contents[0].strip()

    return institution


def parse_signon_response(sonrs):
    items = [
        "code",
        "severity",
        "dtserver",
        "language",
        "dtprofup",
        "org",
        "fid",
        "intu.bid",
        "message"
    ]
    idict = {}
    for i in items:
        try:
            idict[i] = sonrs.find(i).contents[0].strip()
        except Exception:
            idict[i] = None
    idict["code"] = int(idict["code"])
    if idict["message"] is None:
        idict["message"] = ""

    return Signon(idict)


def parse_accounts(
    statements: t.Iterable,
    account_type: AccountType
) -> t.Iterable[Account]:
    """ Parse the <STMTRS> tags and return a list of Accounts object. """
    rv = []
    for statement in statements:
        account = Account()

        currency = statement.find("curdef")
        if currency and currency.contents:
            account.curdef = currency.contents[0].strip()

        account_id = statement.find("acctid")
        if account_id and account_id.contents:
            account.account_id = account_id.contents[0].strip()

        bank_id = statement.find("bankid")
        if bank_id and bank_id.contents:
            account.routing_number = bank_id.contents[0].strip()

        branch_id = statement.find("branchid")
        if branch_id and branch_id.contents:
            account.branch_id = branch_id.contents[0].strip()

        type_ = statement.find("accttype")
        if type_ and type_.contents:
            account.account_type = type_.contents[0].strip()
        account.type = account_type

        if statement:
            account.statement = parse_statement(statement)
        rv.append(account)
    return rv


def parse_balance(
    statement,
    stmt_ofx,
    bal_tag_name,
    bal_attr,
    bal_date_attr,
    bal_type_string
):
    bal_tag = stmt_ofx.find(bal_tag_name)
    if hasattr(bal_tag, "contents"):
        balamt_tag = bal_tag.find("balamt")
        dtasof_tag = bal_tag.find("dtasof")
        if hasattr(balamt_tag, "contents"):
            try:
                setattr(statement, bal_attr, to_decimal(balamt_tag))
            except (IndexError, decimal.InvalidOperation):
                statement.warnings.append(
                    "%s balance amount was empty for %s"
                    "" % (bal_type_string, stmt_ofx))
                if fail_fast:
                    raise ValueError(
                        "Empty %s balance " % bal_type_string)
        if hasattr(dtasof_tag, "contents"):
            try:
                setattr(statement, bal_date_attr, parse_ofx_date(
                    dtasof_tag.contents[0].strip()))
            except IndexError:
                statement.warnings.append(
                    "%s balance date was empty for %s"
                    "" % (bal_type_string, stmt_ofx)
                )
                if fail_fast:
                    raise
            except ValueError:
                statement.warnings.append(
                    "%s balance date was not allowed for %s"
                    "" % (bal_type_string, stmt_ofx))
                if fail_fast:
                    raise


def parse_statement(node, fail_fast: bool = True) -> Statement:
    """
    Parse a statement in ofx-land and return a Statement object.
    """
    statement = Statement()
    dtstart_tag = node.find("dtstart")
    if hasattr(dtstart_tag, "contents"):
        try:
            statement.start_date = parse_ofx_date(
                dtstart_tag.contents[0].strip())
        except IndexError:
            statement.warnings.append(
                "Statement start date was empty for %s" % node)
            if fail_fast:
                raise
        except ValueError:
            statement.warnings.append(
                "Statement start date was not allowed for %s"
                "" % node
            )
            if fail_fast:
                raise

    date_end = node.find("dtend")
    if hasattr(date_end, "contents"):
        try:
            statement.end_date = parse_ofx_date(
                date_end.contents[0].strip())
        except IndexError:
            statement.warnings.append(
                "Statement start date was empty for %s" % node)
            if fail_fast:
                raise
        except ValueError:
            msg = (
                "Statement start date was not formatted "
                "correctly for %s"
            )
            statement.warnings.append(msg % node)
            if fail_fast:
                raise
        except TypeError:
            statement.warnings.append(
                "Statement start date was not allowed for %s"
                "" % node
            )
            if fail_fast:
                raise

    currency_tag = node.find("curdef")
    if hasattr(currency_tag, "contents"):
        try:
            statement.currency = currency_tag.contents[0].strip().lower()
        except IndexError:
            statement.warnings.append(
                "Currency definition was empty for %s" % node)
            if fail_fast:
                raise

    parse_balance(statement, node, "ledgerbal",
                     "balance", "balance_date", "ledger")

    parse_balance(statement, node, "availbal", "available_balance",
                     "available_balance_date", "ledger")

    for transaction_node in node.findAll("stmttrn"):
        try:
            statement.transactions.append(
                parse_transaction(transaction_node))
        except ValueError:
            ofxError = sys.exc_info()[1]
            statement.discarded_entries.append({
                "error": str(ofxError),
                "content": transaction_node,
            })
            if fail_fast:
                raise

    return statement


def parse_transaction(node) -> Transaction:
    """
    Parse a transaction in ofx-land and return a Transaction object.
    """
    transaction = Transaction()

    type_ = tree.find("trntype")
    if hasattr(type_, "contents"):
        try:
            transaction.type = type_.contents[0].lower().strip()
        except IndexError:
            raise ValueError("Empty transaction type")
        except TypeError:
            raise ValueError("No Transaction type (a required field)")

    name = tree.find("name")
    if hasattr(name, "contents"):
        try:
            transaction.payee = name.contents[0].strip()
        except IndexError:
            raise ValueError("Empty transaction name")
        except TypeError:
            raise ValueError("No Transaction name (a required field)")

    memo = tree.find("memo")
    if hasattr(memo, "contents"):
        try:
            transaction.memo = memo.contents[0].strip()
        except IndexError:
            # Memo can be empty.
            pass
        except TypeError:
            pass

    amount = tree.find("trnamt")
    if hasattr(amount, "contents"):
        try:
            transaction.amount = to_decimal(amount)
        except IndexError:
            raise ValueError("Invalid Transaction Date")
        except decimal.InvalidOperation:
            # Some banks use a null transaction for including interest
            # rate changes on your statement.
            if amount.contents[0].strip() in ("null", "-null"):
                transaction.amount = 0
            else:
                raise ValueError(
                    "Invalid Transaction Amount: '%s'" % amount.contents[0])
        except TypeError:
            raise ValueError(
                "No Transaction Amount (a required field)")
    else:
        raise ValueError(
            "Missing Transaction Amount (a required field)")

    date = tree.find("dtposted")
    if hasattr(date, "contents"):
        try:
            transaction.date = parse_ofx_date(
                date.contents[0].strip())
        except IndexError:
            raise ValueError("Invalid Transaction Date")
        except ValueError:
            ve = sys.exc_info()[1]
            raise ValueError(str(ve))
        except TypeError:
            raise ValueError(
                "No Transaction Date (a required field)")
    else:
        raise ValueError(
            "Missing Transaction Date (a required field)")

    user_date = tree.find("dtuser")
    if hasattr(user_date, "contents"):
        try:
            transaction.user_date = parse_ofx_date(
                user_date.contents[0].strip())
        except IndexError:
            raise ValueError("Invalid Transaction User Date")
        except ValueError:
            ve = sys.exc_info()[1]
            raise ValueError(str(ve))
        except TypeError:
            pass

    id_ = tree.find("fitid")
    if hasattr(id_, "contents"):
        try:
            transaction.id = id_.contents[0].strip()
        except IndexError:
            raise ValueError(
                "Empty FIT id (a required field)")
        except TypeError:
            raise ValueError(
                "No FIT id (a required field)")
    else:
        raise ValueError(
            "Missing FIT id (a required field)")

    sic_tag = tree.find("sic")
    if hasattr(sic_tag, "contents"):
        try:
            transaction.sic = sic_tag.contents[0].strip()
        except IndexError:
            raise ValueError(
                "Empty transaction Standard Industry Code (SIC)")
    else:
        raise ValuError("Missing SIC tag")

    if transaction.sic is not None and transaction.sic in mcc.codes:
        try:
            transaction.mcc = mcc.codes.get(transaction.sic, "").get("combined \
                description")
        except IndexError:
            raise ValueError(
                "Empty transaction Merchant Category Code (MCC)")
        except AttributeError:
            if fail_fast:
                raise

    checknum = tree.find("checknum")
    if hasattr(checknum, "contents"):
        try:
            transaction.checknum = checknum.contents[0].strip()
        except IndexError:
            raise ValueError(
                "Empty Check (or other reference) number")

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
