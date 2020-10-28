import argparse
import getpass
import io
import logging
import os
import os.path
import sys
import typing as t

from ofxclient.types import BankAccount, BrokerageAccount, CreditCardAccount
from ofxclient.institution import Institution
from ofxclient.util import combined_download
from ofxclient.client import Client, DEFAULT_OFX_VERSION, working_query
from ofxclient.parse import parse_ofx

AUTO_OPEN_DOWNLOADS = 1
DOWNLOAD_DAYS = 30


def parse_args() -> t.Dict[str, t.Any]:
    parser = argparse.ArgumentParser(prog="ofxclient")
    subparsers = parser.add_subparsers()
    test = subparsers.add_parser('test', help='Test OFX parse')
    test.set_defaults(func=test_parse)

    acctinfo = subparsers.add_parser('acctinfo', help='List Accounts')
    acctinfo.add_argument("-a", "--account")
    acctinfo.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Set verbosity level",
    )
    acctinfo.add_argument(
        "--days",
        default=DOWNLOAD_DAYS,
        type=int,
        help="number of days to download (default: %s)" % DOWNLOAD_DAYS,
    )
    acctinfo.add_argument(
        "--ofx-version",
        default=DEFAULT_OFX_VERSION,
        type=int,
        help="ofx version to use for new accounts (default: %s)" % DEFAULT_OFX_VERSION,
    )
    acctinfo.set_defaults(func=account_info)
    return vars(parser.parse_args())


def test_parse(args: dict) -> None:
    input = sys.stdin.read()
    result = parse_ofx(input)
    print(result)


def account_info(args: dict) -> None:
    if args["verbose"]:
        logging.basicConfig(level=logging.DEBUG)
    client = Client()
    response = client.post(working_query())
    print(response)


def main():
    args = parse_args()
    f = args["func"]
    f(args)


if __name__ == "__main__":
    main()
