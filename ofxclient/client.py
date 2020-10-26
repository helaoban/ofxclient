from http.client import HTTPSConnection
import logging
import time
from urllib.parse import splittype, splithost
import uuid
import typing as t


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
        institution,
        client_id: t.Optional[str] = None,
        app_id=DEFAULT_APP_ID,
        app_version=DEFAULT_APP_VERSION,
        ofx_version=DEFAULT_OFX_VERSION,
        user_agent=DEFAULT_USER_AGENT,
        accept=DEFAULT_ACCEPT,
    ) -> None:
        self.institution = institution
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
        with_message: t.Optional[str] = None,
        username: t.Optional[str] = None,
        password: t.Optional[str] = None,
    ) -> str:
        """Authenticated query

        If you pass a 'with_messages' array those queries will be passed along
        otherwise this will just be an authentication probe query only.
        """
        u = username or self.institution.username
        p = password or self.institution.password

        contents = ["OFX", self._sign_on(username=u, password=p)]
        if with_message:
            contents.append(with_message)
        return LINE_ENDING.join([self.header(), _tag(*contents)])

    def bank_account_query(self, number, date, account_type, bank_id):
        """Bank account statement request"""
        return self.authenticated_query(
            self._bare_request(number, date, account_type, bank_id)
        )

    def credit_card_account_query(self, number, date):
        """CC Statement request"""
        return self.authenticated_query(self._credit_card_request(number, date))

    def brokerage_account_query(self, number, date, broker_id):
        return self.authenticated_query(
            self._investment_request(broker_id, number, date)
        )

    def account_list_query(self, date="19700101000000"):
        return self.authenticated_query(self._account_request(date))

    def post(self, query):
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
            _, response = self._do_post(query, [("Cookie", cookies)])
        return response

    def _do_post(self, query, extra_headers=[]) -> t.Tuple[HTTPResponse, str]:
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
        logging.debug("posting data to %s" % self.institution.url)
        garbage, path = splittype(self.institution.url)
        host, selector = splithost(path)
        h = HTTPSConnection(host, timeout=60)
        # Discover requires a particular ordering of headers, so send the
        # request step by step.
        h.putrequest("POST", selector, skip_host=True, skip_accept_encoding=True)

        headers = {
            **DEFAULT_HEADERS,
            **{
                "Host": host,
                "Content-Length": len(query),
            }
            ** extra_headers,
        }
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

    def header(self):
        parts = [
            "OFXHEADER:100",
            "DATA:OFXSGML",
            "VERSION:%d" % int(self.ofx_version),
            "SECURITY:NONE",
            "ENCODING:USASCII",
            "CHARSET:1252",
            "COMPRESSION:NONE",
            "OLDFILEUID:NONE",
            "NEWFILEUID:" + ofx_uid(),
            "",
        ]
        return LINE_ENDING.join(parts)

    def _sign_on(self, username: str, password: str) -> str:
        """Generate signon message"""
        fidata = [_field("ORG", self.institution.org)]
        if self.institution.id:
            fidata.append(_field("FID", self.institution.id))

        if str(self.ofx_version) == "103":
            client_uid = _field("CLIENTUID", self.client_id)
        else:
            client_uid = ""

        return _tag(
            "SIGNONMSGSRQV1",
            _tag(
                "SONRQ",
                _field("DTCLIENT", now()),
                _field("USERID", username),
                _field("USERPASS", password),
                _field("LANGUAGE", "ENG"),
                _tag("FI", *fidata),
                _field("APPID", self.app_id),
                _field("APPVER", self.app_version),
                client_uid,
            ),
        )

    def _account_request(self, dtstart):
        req = _tag("ACCTINFORQ", _field("DTACCTUP", dtstart))
        return self._message("SIGNUP", "ACCTINFO", req)

    # this is from _credit_card_request below and reading
    # page 176 of the latest OFX doc.
    def _bare_request(self, acctid, dtstart, accttype, bankid):
        req = _tag(
            "STMTRQ",
            _tag(
                "BANKACCTFROM",
                _field("BANKID", bankid),
                _field("ACCTID", acctid),
                _field("ACCTTYPE", accttype),
            ),
            _tag("INCTRAN", _field("DTSTART", dtstart), _field("INCLUDE", "Y")),
        )
        return self._message("BANK", "STMT", req)

    def _credit_card_request(self, acctid, dtstart):
        req = _tag(
            "CCSTMTRQ",
            _tag("CCACCTFROM", _field("ACCTID", acctid)),
            _tag("INCTRAN", _field("DTSTART", dtstart), _field("INCLUDE", "Y")),
        )
        return self._message("CREDITCARD", "CCSTMT", req)

    def _investment_request(
        self,
        broker_id: str,
        account_id: str,
        start_date: str,
    ) -> str:
        req = _tag(
            "INVSTMTRQ",
            _tag(
                "INVACCTFROM",
                _field("BROKERID", broker_id),
                _field("ACCTID", account_id),
            ),
            _tag("INCTRAN", _field("DTSTART", start_date), _field("INCLUDE", "Y")),
            _field("INCOO", "Y"),
            _tag("INCPOS", _field("DTASOF", now()), _field("INCLUDE", "Y")),
            _field("INCBAL", "Y"),
        )
        return self._message("INVSTMT", "INVSTMT", req)

    def _message(self, msgType, trnType, request) -> str:
        return _tag(
            msgType + "MSGSRQV1",
            _tag(
                trnType + "TRNRQ",
                _field("TRNUID", ofx_uid()),
                _field("CLTCOOKIE", self.next_cookie()),
                request,
            ),
        )


def _field(tag: str, value: str) -> str:
    return "<" + tag + ">" + value


def _tag(tag: str, *contents: str) -> str:
    return LINE_ENDING.join(["<" + tag + ">"] + list(contents) + ["</" + tag + ">"])


def now():
    return time.strftime("%Y%m%d%H%M%S", time.localtime())
