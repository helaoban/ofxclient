import datetime
import decimal
import hashlib
from io import StringIO, BytesIO
import time
import enum
import typing as t
import typing_extensions as te


class Status(te.TypedDict):
    code: t.Optional[int]
    severity: t.Optional[str]
    message: t.Optional[str]


class Signon(te.TypedDict):
    status: t.Optional[Status]
    dtserver: t.Optional[str]
    language: t.Optional[str]
    dtprofup: t.Optional[str]
    org: t.Optional[str]
    fid: t.Optional[str]
    intu_bid: t.Optional[str]


class BrokerageBalance(te.TypedDict):
    name: str
    description: str
    value: decimal.Decimal


class Security(te.TypedDict):
    unique_id: str
    name: str
    ticker: t.Optional[str]
    memo: t.Optional[str]


class Position(te.TypedDict):
    security: str
    units: decimal.Decimal
    unit_price: decimal.Decimal
    market_value: decimal.Decimal
    date: datetime.datetime


class Transaction(te.TypedDict):
    payee: str
    type: str
    date: t.Optional[datetime.datetime]
    amount: t.Optional[decimal.Decimal]
    id: str
    memo: str


class InvestmentTransaction(te.TypedDict):
    type: str
    trade_date: t.Optional[datetime.datetime]
    settle_date: None
    memo: str
    security: str
    income_type: str
    units: decimal.Decimal
    unit_price: decimal.Decimal
    commission: decimal.Decimal
    fees: decimal.Decimal
    total: decimal.Decimal
    tferaction: t.Optional[str]


class Statement(te.TypedDict):
    start_date: datetime.datetime
    end_date: datetime.datetime
    currency: str
    transactions: t.List[Transaction]
    # Error tracking:
    discarded_entries: t.List[t.Dict[str, t.Any]]
    warnings: t.List[str]
    balance: decimal.Decimal
    balance_date: datetime.datetime
    available_balance: decimal.Decimal
    available_balance_date: datetime.datetime


class InvestmentStatement(te.TypedDict):
    start_date: datetime.datetime
    end_date: datetime.datetime
    currency: str
    transactions: t.List[t.Union[InvestmentTransaction, Transaction]]
    # Error tracking:
    discarded_entries: t.List[t.Dict[str, t.Any]]
    warnings: t.List[str]
    positions: t.List[Position]
    available_cash: decimal.Decimal
    margin_balance: decimal.Decimal
    buying_power: decimal.Decimal
    short_balance: decimal.Decimal
    balances: t.List[BrokerageBalance]


class Institution(te.TypedDict):
    organization: str
    fid: str


class Account(te.TypedDict):
    account_id: str
    routing_number: str
    account_type: str
    description: str
    branch_id: t.Optional[str]
    broker_id: t.Optional[str]


class ParseResult(te.TypedDict):
    accounts: t.List[Account]
    transactions: t.List[Transaction]
    securities: t.List[Security]
    status: t.Optional[Status]
    signon: t.Optional[Signon]


class defaults:

    @staticmethod
    def parse_result() -> ParseResult:
        return {
            "accounts": [],
            "transactions": [],
            "securities": [],
            "status": None,
            "signon": None,
        }

    @staticmethod
    def transaction() -> Transaction:
        return {
            "payee": "",
            "type": "",
            "date": datetime.datetime(1970, 12, 31),
            "amount": decimal.Decimal(0),
            "id": "",
            "memo": "",
        }

    @staticmethod
    def statement() -> Statement:
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


    @staticmethod
    def status() -> Status:
        return {
            "code": None,
            "severity": None,
            "message": None,
        }

    @staticmethod
    def signon() -> Signon:
        return {
            "status": t.Optional[Status],
            "dtserver": None,
            "language": None,
            "dtprofup": None,
            "org": None,
            "fid": None,
            "intu_bid": None,
        }

    @staticmethod
    def institution():
        return {"organization": str, "fid": str}

    @staticmethod
    def investment_statement() -> InvestmentStatement:
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


    @staticmethod
    def investment_transaction() -> InvestmentTransaction:
        pass

    @staticmethod
    def brokerage_balance() -> BrokerageBalance:
        return {
            "name": "",
            "description": "",
            "value": decimal.Decimal(0),
        }

    @staticmethod
    def position() -> Position:
        return {
            "security": "N/A",
            "units": decimal.Decimal(0),
            "unit_price": decimal.Decimal(0),
            "market_value": decimal.Decimal(0),
            "date": datetime.datetime(1970, 12, 31)
        }

    @staticmethod
    def security() -> Security:
        return {
            "unique_id": "",
            "name": "",
            "ticker": "",
            "memo": "",
        }


    @staticmethod
    def account() -> Account:
        return {
            "account_id": "",
            "routing_number": "",
            "account_type": "",
            "description": "",
            "branch_id": None,
            "broker_id": None,
        }


