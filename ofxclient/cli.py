import argparse
import getpass
import io
import logging
import os
import os.path
import sys
import typing as t

from ofxhome import OFXHome

from ofxclient.types import BankAccount, BrokerageAccount, CreditCardAccount
from ofxclient.config import Config
from ofxclient.institution import Institution
from ofxclient.util import combined_download
from ofxclient.client import DEFAULT_OFX_VERSION

AUTO_OPEN_DOWNLOADS = 1
DOWNLOAD_DAYS = 30


def parse_args() -> t.Dict[str, t.Any]:
    parser = argparse.ArgumentParser(prog="ofxclient")
    parser.add_argument("-a", "--account")
    parser.add_argument("-d", "--download", type=argparse.FileType("wb", 0))
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--days",
        default=DOWNLOAD_DAYS,
        type=int,
        help="number of days to download (default: %s)" % DOWNLOAD_DAYS,
    )
    parser.add_argument(
        "--ofx-version",
        default=DEFAULT_OFX_VERSION,
        type=int,
        help="ofx version to use for new accounts (default: %s)" % DEFAULT_OFX_VERSION,
    )
    return vars(parser.parse_args())


def download():
    args = parse_args()
    client = Client("chase")

    if args["verbose"]:
        logging.basicConfig(level=logging.DEBUG)

    ofx = client.get_bank_accounts(


if __name__ == "__main__":
    download()
