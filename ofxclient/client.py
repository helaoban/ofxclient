from http.client import HTTPSConnection, HTTPResponse
import datetime as dt
import logging
import time
from itertools import chain
from urllib.parse import urlparse
import os
import typing as t
import uuid
import re
import xml.etree.ElementTree as ET
import requests

import pytz

from . import types as tp
from .helpers import to_ofx_date, ofx_uid, ofx_now, clean_query
from .parse import parse_ofx


DEFAULT_APP_ID = "QWIN"
DEFAULT_APP_VERSION = "2700"
DEFAULT_OFX_VERSION = "220"
DEFAULT_USER_AGENT = "httpclient"
DEFAULT_ACCEPT = "*/*, application/x-ofx"
DEFAULT_HEADERS = {
    "Accept": DEFAULT_ACCEPT,
    "User-Agent": DEFAULT_USER_AGENT,
    "Content-Type": "application/x-ofx",
    "Connection": "Keep-Alive",
}

LINE_ENDING = "\r\n"


def working_query():
    return f"""
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?OFX OFXHEADER="200" VERSION="220" SECURITY="NONE" OLDFILEUID="NONE" NEWFILEUID="{uuid.uuid4()}"?>
<OFX>
    <SIGNONMSGSRQV1>
        <SONRQ>
            <DTCLIENT>{ofx_now()}</DTCLIENT>
            <USERID>edwardh759</USERID>
            <USERPASS>Ardmore759!</USERPASS>
            <LANGUAGE>ENG</LANGUAGE>
            <FI>
                <ORG>B1</ORG>
                <FID>10898</FID>
            </FI>
            <APPID>QWIN</APPID>
            <APPVER>2700</APPVER>
        </SONRQ>
    </SIGNONMSGSRQV1>
    <SIGNUPMSGSRQV1>
        <ACCTINFOTRNRQ>
            <TRNUID>b0302a78-cea3-407a-a54a-3d95012beff5</TRNUID>
            <ACCTINFORQ>
                <DTACCTUP>19901231000000.000[0:GMT]</DTACCTUP>
            </ACCTINFORQ>
        </ACCTINFOTRNRQ>
    </SIGNUPMSGSRQV1>
</OFX>
"""


class Client:
    """This communicates with the banks via the OFX protocol

    :param institution: institution to connect to
    :type institution: :py:class:`ofxclient.Institution`
    :param id: client id (optional need for OFX version >= 103)
    :type id: string
    :param app_id: OFX app id
    :type app_id: string
    :param app_version: OFX app version
    :type app_version: string
    :param ofx_version: OFX spec version
    :type ofx_version: string
    :param user_agent: Value to send for User-Agent HTTP header. Leave as
      None to send default. Set to False to not send User-Agent header.
    :type user_agent: str, None or False
    :param accept: Value to send for Accept HTTP header. Leave as
      None to send default. Set to False to not send User-Agent header.
    :type accept: str, None or False
    """

    def __init__(
        self,
        institution: t.Optional[t.Tuple[str, str, str]] = None,
        username: t.Optional[str] = None,
        password: t.Optional[str] = None,
        client_id: t.Optional[str] = None,
        app_id: str = DEFAULT_APP_ID,
        app_version: str = DEFAULT_APP_VERSION,
        ofx_version: str = DEFAULT_OFX_VERSION,
        user_agent: str = DEFAULT_USER_AGENT,
        accept: str = DEFAULT_ACCEPT,
    ) -> None:
        if institution is None:
            org = os.environ["OFX_ORG"]
            fid = os.environ["OFX_FID"]
            url = os.environ["OFX_URL"]
            institution = org, fid, url

        self.institution = institution

        if username is None:
            try:
                username = os.environ["OFX_USERNAME"]
            except KeyError:
                username = ""
        self.username = username

        if password is None:
            try:
                password = os.environ["OFX_PASSWORD"]
            except KeyError:
                password = ""
        self.password = password

        if client_id is None:
            try:
                client_id = os.environ["OFX_CLIENT_ID"]
            except KeyError:
                client_id = ofx_uid()

        self.client_id = client_id
        self.app_id = app_id
        self.app_version = app_version
        self.ofx_version = ofx_version
        self.user_agent = user_agent
        self.accept = accept
        # used when serializing Institutions
        self.cookie = 3

        self.init_args = {
            "id": self.client_id,
            "app_id": self.app_id,
            "app_version": self.app_version,
            "ofx_version": self.ofx_version,
            "user_agent": self.user_agent,
            "accept": self.accept,
        }

    def _add_header(self, query: str) -> str:
        v = self.ofx_version
        header = f"""
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?OFX OFXHEADER="200"
    VERSION="{v}"
    SECURITY="NONE"
    OLDFILEUID="NONE"
    NEWFILEUID="{uuid.uuid4()}"
?>
"""
        return re.sub(
            "\s+", " ", header.strip().replace("\n", "")) + query

    def _wrap(self, query: str):
        return self._add_header(clean_query(query))

    def authenticated_query(
        self,
        query: str = "",
        username: t.Optional[str] = None,
        password: t.Optional[str] = None,
    ) -> str:
        """Authenticated query

        If you pass a 'with_messages' array those queries will be passed along
        otherwise this will just be an authentication probe query only.
        """
        u = username or self.username
        p = password or self.password
        v = self.ofx_version
        signon = self._sign_on(u, p)
        return f"""
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?OFX OFXHEADER="200"
    VERSION="{v}"
    SECURITY="NONE"
    OLDFILEUID="NONE"
    NEWFILEUID="{uuid.uuid4()}"
?>
<OFX>{signon}{query}</OFX>
"""

    def query_profile(self) -> tp.ParseResult:
        return self.post(
            self.authenticated_query(self._profile_request()))

    def query_account_list(
        self,
        date: t.Optional[dt.datetime] = None,
    ) -> tp.ParseResult:
        date = date or dt.datetime(
            1990, 12, 31, tzinfo=pytz.UTC)
        return self.post(
            self.authenticated_query(
                self._account_request(date)))

    def query_bank_accounts(
        self,
        account_id: str,
        date: dt.datetime,
        account_type: str,
        bank_id: str,
    ) -> tp.ParseResult:
        account_req = self._bare_request(
            account_id, date, account_type, bank_id)
        query = self.authenticated_query(account_req)
        return self.post(query)

    def query_credit_cards(
        self,
        account_id: str,
        date: dt.datetime,
    ) -> tp.ParseResult:
        query = self.authenticated_query(
            self._credit_card_request(account_id, date))
        return self.post(query)

    def query_brokerage_accounts(
        self,
        account_id: str,
        date: dt.datetime,
        broker_id: str,
    ) -> tp.ParseResult:
        query = self.authenticated_query(
            self._investment_request(broker_id, account_id, date))
        return self.post(query)

    def post(self, query: str) -> tp.ParseResult:
        """
        Wrapper around ``_do_post()`` to handle accounts that require
        sending back session cookies (``self.set_cookies`` True).
        """
        with requests.Session() as http_session:
            http_response, ofx_data = self._do_post(query, session=http_session)
            cookies = http_response.headers.get("Set-Cookie", None)
            if (
                len(ofx_data) == 0
                and cookies is not None
            ):
                logging.debug(
                    "Got 0-length 200 response with Set-Cookies header; "
                    "retrying request with cookies"
                )
                _, ofx_data = self._do_post(query, session=http_session)
        return parse_ofx(ofx_data)

    def _print_headers(self, headers: t.Dict[str, str]) -> str:
        rv = ""
        for key, val in headers.items():
            rv = rv + "%s: %s\n" % (key, val)
        return rv

    def _do_post(
        self,
        query: str,
        *extra_headers: t.Tuple[str, str],
        session: t.Optional[requests.Session] = None,
    ) -> t.Tuple[requests.Response, str]:
        """
        Do a POST to the Institution.

        :param query: Body content to POST (OFX Query)
        :type query: str
        :param extra_headers: Extra headers to send with the request, as a list
          of (Name, Value) header 2-tuples.
        :type extra_headers: list
        :return: 2-tuple of (HTTPResponse, str response body)
        :rtype: tuple
        """
        if session is not None:
            post = session.post
        else:
            post = requests.post

        _, _, url = self.institution
        parsed_url = urlparse(url)
        host = parsed_url.netloc

        logging.debug("posting data to %s" % url)

        query = self._wrap(query)
        headers = {}
        for key, val in chain(DEFAULT_HEADERS.items(), extra_headers):
            headers[key] = val

        logging.debug(
            "\n---- request headers ----\n\n"
            f"{self._print_headers(headers)}\n"
            "---- request body ----\n\n"
            f"{query}\n"
        )

        response = post(url, data=query, headers=headers)

        logging.debug(
            "\n---- response headers ----\n\n"
            f"{self._print_headers(dict(response.headers))}\n"
            "---- response body ----\n\n"
            f"{response.text}\n"
        )

        if response.status_code != 200:
            response.raise_for_status()

        return response, response.text

    def next_cookie(self) -> str:
        self.cookie += 1
        return str(self.cookie)

    def _sign_on(self, username: str, password: str) -> str:
        """Generate signon message"""
        org, fid, _ = self.institution
        return f"""
<SIGNONMSGSRQV1>
    <SONRQ>
        <DTCLIENT>{to_ofx_date(dt.datetime.utcnow())}</DTCLIENT>
        <USERID>{username}</USERID>
        <USERPASS>{password}</USERPASS>
        <LANGUAGE>ENG</LANGUAGE>
        <FI>
            <ORG>{org}</ORG>
            <FID>{fid}</FID>
        </FI>
        <APPID>{self.app_id}</APPID>
        <APPVER>{self.app_version}</APPVER>
        <CLIENTUID>{self.client_id}</CLIENTUID>
    </SONRQ>
</SIGNONMSGSRQV1>
"""

    def _profile_request(
        self,
    ):
        return f"""
<PROFMSGSRQV1>
    <PROFTRNRQ>
        <TRNUID>{ofx_uid()}</TRNUID>
        <PROFRQ>
            <CLIENTROUTING>MSGSET</CLIENTROUTING>
            <DTPROFUP>{to_ofx_date(dt.datetime.utcnow())}</DTPROFUP>
        </PROFRQ>
    </PROFTRNRQ>
</PROFMSGSRQV1>
"""

    def _account_request(
        self,
        start_date: dt.datetime,
    ) -> str:
        req = f"""
<ACCTINFORQ>
    <DTACCTUP>{to_ofx_date(start_date)}</DTACCTUP>
</ACCTINFORQ>
"""
        return self._message("SIGNUP", "ACCTINFO", req)

    # this is from _credit_card_request below and reading
    # page 176 of the latest OFX doc.
    def _bare_request(
        self,
        account_id: str,
        start_date: dt.datetime,
        account_type: str,
        bank_id: str,
    ) -> str:
        req = f"""
<STMRQ>
    <BANKACCTFROM>
        <BANKID>{bank_id}</BANKID>
        <ACCTID>{account_id}</ACCTID>
        <ACCTTYPE>{account_type}</ACCTTYPE>
    </BANKACCTFROM>
    <INCTRAN>
        <DSTART>{to_ofx_date(start_date)}</DSTART>
        <INCLUDE>Y</INCLUDE>
    </INCTRAN>
</STMRQ>
"""
        return self._message("BANK", "STMT", req)

    def _credit_card_request(
        self,
        account_id: str,
        start_date: dt.datetime,
    ) -> str:
        req = f"""
<CCSTMRQ>
    <CCACCTFROM>
        <ACCTID>{account_id}</ACCTID>
    </CCACCTFROM>
    <INCTRAN>
        <DSTART>{to_ofx_date(start_date)}</DSTART>
        <INCLUDE>Y</INCLUDE>
    </INCTRAN>
</CCSTMRQ>
"""
        return self._message("CREDITCARD", "CCSTMT", req)

    def _investment_request(
        self,
        broker_id: str,
        account_id: str,
        start_date: dt.datetime,
    ) -> str:
        req = f"""
<INVSTMTRQ>
    <INVACCTFROM>
        <BROKERID>{broker_id}</BROKERID>
        <ACCTID>{account_id}</ACCTID>
    </INVACCTFROM>
    <INCTRAN>
        <DSTART>{to_ofx_date(start_date)}</DSTART>
        <INCLUDE>Y</INCLUDE>
    </INCTRAN>
    <INCOO>Y</INCOO>
    <INCTRAN>
        <DSTART>{to_ofx_date(dt.datetime.utcnow())}</DSTART>
        <INCLUDE>Y</INCLUDE>
    </INCTRAN>
    <INCBAL>Y</INCBAL>
</INVSTMTRQ>
"""
        return self._message("INVSTMT", "INVSTMT", req)


    def _message(
        self,
        msg_type: str,
        trn_type: str,
        request: str
    ) -> str:
        return f"""
<{msg_type}MSGSRQV1>
    <{trn_type}TRNRQ>
        <TRNUID>{ofx_uid()}</TRNUID>
        {request}
    </{trn_type}TRNRQ>
</{msg_type}MSGSRQV1>
"""
