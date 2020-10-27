from http.client import HTTPSConnection
import datetime as dt
import logging
import time
from itertools import chain
from urllib.parse import splittype, splithost
import os
import uuid
import typing as t

from . import types as tp
from .parse import parse_ofx


if t.TYPE_CHECKING:
    from http.client import HTTPResponse

DEFAULT_APP_ID = "QWIN"
DEFAULT_APP_VERSION = "2500"
DEFAULT_OFX_VERSION = "102"
DEFAULT_USER_AGENT = "httpclient"
DEFAULT_ACCEPT = "*/*, application/x-ofx"

DEFAULT_HEADERS = {
    "Accept": DEFAULT_ACCEPT,
    "User-Agent": DEFAULT_USER_AGENT,
    "Content-Type": "application/x-ofx",
    "Connection": "Keep-Alive",
}

LINE_ENDING = "\r\n"


def ofx_uid():
    return str(uuid.uuid4().hex)


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

        self.client_id = client_id or ofx_uid()
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
        signon = self._sign_on(u, p)
        return f"""

OFXHEADER:100
DATA:OFXSGML
VERSION:{self.ofx_version}
SECURITY:NONE
ENCODING:USASCII
CHARSET:1252
COMPRESSION:NONE
OLDFILEUID:NONE
NEWFILEUID:{ofx_uid()}

<OFX>
    {signon}
    {query}
</OFX>
"""

    def query_account_list(
        self,
        date="19700101000000"
    ) -> tp.ParseResult:
        return self.post(
            self.authenticated_query(self._account_request(date)))

    def query_bank_accounts(
        self,
        account_id: str,
        date: str,
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
        date: str,
    ) -> tp.ParseResult:
        query = self.authenticated_query(
            self._credit_card_request(account_id, date))
        return self.post(query)

    def query_brokerage_accounts(
        self,
        account_id: str,
        date: str,
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
        res, response = self._do_post(query)
        cookies = res.getheader("Set-Cookie", None)
        if len(response) == 0 and cookies is not None and res.status == 200:
            logging.debug(
                "Got 0-length 200 response with Set-Cookies header; "
                "retrying request with cookies"
            )
            _, response = self._do_post(query, ("Cookie", cookies))
        return parse_ofx(response)

    def _do_post(
        self,
        query: str,
        *extra_headers: t.Tuple[str, str],
    ) -> t.Tuple[HTTPResponse, str]:
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
        _, _, url = self.institution
        logging.debug("posting data to %s" % url)
        garbage, path = splittype(url)
        host, selector = splithost(path)
        h = HTTPSConnection(host, timeout=60)
        # Discover requires a particular ordering of headers, so send the
        # request step by step.
        h.putrequest("POST", selector, skip_host=True, skip_accept_encoding=True)

        headers = {"Host": host, "Content-Length": str(len(query))}
        for key, val in chain(DEFAULT_HEADERS.items(), extra_headers):
            headers[key] = val

        logging.debug("---- request headers ----")
        for name, value in headers.items():
            logging.debug("%s: %s", name, value)
            h.putheader(name, value)
        logging.debug("---- request body (query) ----")
        logging.debug(query)
        h.endheaders(query.encode())
        response = h.getresponse()
        decoded = response.read().decode("ascii", "ignore")
        logging.debug("---- response ----")
        logging.debug(response.__dict__)
        logging.debug("Headers: %s", response.getheaders())
        logging.debug(decoded)
        response.close()
        return response, decoded

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

    def _account_request(self, dtstart):
        req = f"""
<ACCTINFORQ>
    <DTACCTUP>{start_date}</DTACCTUP>
</ACCTINFORQ>
"""
        return self._message("SIGNUP", "ACCTINFO", req)

    # this is from _credit_card_request below and reading
    # page 176 of the latest OFX doc.
    def _bare_request(
        self,
        account_id: str,
        start_date: str,
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
        <DSTART>{start_date}</DSTART>
        <INCLUDE>Y</INCLUDE>
    </INCTRAN>
</STMRQ>
"""
        return self._message("BANK", "STMT", req)

    def _credit_card_request(
        self,
        account_id: str,
        start_date: str,
    ) -> str:
        req = f"""
<CCSTMRQ>
    <CCACCTFROM>
        <ACCTID>{account_id}</ACCTID>
    </CCACCTFROM>
    <INCTRAN>
        <DSTART>{start_date}</DSTART>
        <INCLUDE>Y</INCLUDE>
    </INCTRAN>
</CCSTMRQ>
"""
        return self._message("CREDITCARD", "CCSTMT", req)

    def _investment_request(
        self,
        broker_id: str,
        account_id: str,
        start_date: str,
    ) -> str:
        req = f"""
<INVSTMTRQ>
    <INVACCTFROM>
        <BROKERID>{broker_id}</BROKERID>
        <ACCTID>{account_id}</ACCTID>
    </INVACCTFROM>
    <INCTRAN>
        <DSTART>{start_date}</DSTART>
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
<{msg_type}MSGRQV1>
    <{trn_type}TRNRQ>
        <TRNUID>{ofx_uid}</TRNUID>
        <CLTCOOKIE>{self.next_cookie()}</TRNUID>
    </{trn_type}TRNRQ>
    {request}
</{msg_type}MSGRQV1>
"""
