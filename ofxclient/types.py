import typing as t
import typing_extensions as te


class Transaction(te.TypedDict):
    payee: str
    type: str
    date: t.Optional[datetime]
    user_date: t.Optional[datetime]
    amount str t.Optional[Decimal]
    id: str
    memo: str
    sic: t.Optional[str]
    mcc: str
    checknum: str
