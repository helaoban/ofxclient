import argparse
import getpass
import io
import logging
import os
import os.path
import sys
import typing as t
import datetime as dt
from . import json
import csv

from .client import Client, DEFAULT_OFX_VERSION
from .parse import parse_ofx

AUTO_OPEN_DOWNLOADS = 1
DOWNLOAD_DAYS = 30

def environ_or_required(key):
    return (
        {'default': os.environ.get(key)} if os.environ.get(key)
        else {'required': True}
    )


def parse_test_args(subparsers) -> None:
    test = subparsers.add_parser('test', help='Test OFX parse')
    test.set_defaults(func=test_parse)


def parse_acctinfo_args(subparsers) -> None:
    acctinfo = subparsers.add_parser('acctinfo', help='List accounts')
    acctinfo.set_defaults(func=account_info)


def parse_stmt_args(subparsers) -> None:
    stmt = subparsers.add_parser('stmt', help='Query account statemens')
    stmt.add_argument(
        "-a", "--account",
        help=(
            "Account ID to query. If not passed then program "
            "looks for OFX_ACCOUNT_ID in the environment."
        ),
        **environ_or_required("OFX_ACCOUNT_ID"),
    )
    stmt.add_argument(
        "-r", "--routing-number",
        help=(
            "Routing number of bank. If not passed then program "
            "looks for OFX_ACCOUNT_ID in the environment."
        ),
        **environ_or_required("OFX_ROUTING_NUMBER"),
    )
    stmt.add_argument(
        "-t", "--account-type",
        help=(
            "Account type (eg CHECKING, MONEYMRKT). If not "
            "passed then program looks for OFX_ACCOUNT_TYPE "
            "in the environment."
        ),
        **environ_or_required("OFX_ACCOUNT_TYPE"),
    )
    stmt.add_argument(
        "--days",
        default=DOWNLOAD_DAYS,
        type=int,
        help="number of days to download (default: %s)" % DOWNLOAD_DAYS,
    )
    stmt.add_argument(
        "-o", "--output-format",
        type=str,
        choices=("csv", "json"),
        default="json",
        help="Set verbosity level",
    )
    stmt.set_defaults(func=statements)


def parse_args() -> t.Dict[str, t.Any]:
    parser = argparse.ArgumentParser(prog="ofxclient")
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Set verbosity level",
    )
    parser.add_argument(
        "--ofx-version",
        default=DEFAULT_OFX_VERSION,
        type=int,
        help="OFX version to use (default: %s)" % DEFAULT_OFX_VERSION,
    )
    subparsers = parser.add_subparsers()
    parse_test_args(subparsers)
    parse_acctinfo_args(subparsers)
    parse_stmt_args(subparsers)
    return vars(parser.parse_args())


def test_parse(args: dict) -> None:
    input = sys.stdin.read()
    result = parse_ofx(input)
    print(json.dumps(result))


def account_info(args: dict) -> None:
    client = Client()
    result = client.query_account_list()
    print(json.dumps(result["accounts"]))


def statements(args: dict) -> None:
    client = Client()
    now = dt.datetime.utcnow()
    start_date = now - dt.timedelta(days=args["days"])
    result = client.query_statements(
        account_id=args["account"],
        routing_number=args["routing_number"],
        start_date=start_date,
        account_type=args["account_type"],
    )

    transactions = result["transactions"]

    if args["output_format"] == "csv":
        if len(transactions) == 0:
            return
        keys = transactions[0].keys()
        writer = csv.DictWriter(sys.stdout, keys)
        writer.writeheader()
        writer.writerows(transactions)
    else:
        print(json.dumps(result["transactions"]))


def main():
    args = parse_args()
    if args["verbose"]:
        logging.basicConfig(level=logging.DEBUG)
    f = args["func"]
    f(args)


if __name__ == "__main__":
    main()
