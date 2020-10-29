import datetime
import decimal
import hashlib
from io import StringIO, BytesIO
import time
import enum
import typing as t
import typing_extensions as te


class Signon(te.TypedDict):
    code: t.Optional[str]
    severity: t.Optional[str]
    message: t.Optional[str]
    dtserver: t.Optional[str]
    language: t.Optional[str]
    dtprofup: t.Optional[str]
    org: t.Optional[str]
    fid: t.Optional[str]
    intu_bid: t.Optional[str]


class AccountType(enum.Enum):
    Unknown = 0
    Bank = 1
    CreditCard = 2
    Investment = 3


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
    user_date: t.Optional[datetime.datetime]
    amount: t.Optional[decimal.Decimal]
    id: str
    memo: str
    sic: t.Optional[str]
    mcc: str
    checknum: str


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
    status: t.Optional[t.Dict[str, t.Any]]
    signon: t.Optional[Signon]
