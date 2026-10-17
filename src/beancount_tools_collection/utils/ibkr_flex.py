"""Client for IBKR's Flex Web Service that stays inside IBKR's request limits.

IBKR allows one request per second and ten per minute per token, and blocks the
token (code 1025, "Too many failed attempts") when a client keeps failing.
ibflex's own download loop polls faster than that and re-sends without pause on
HTTP errors, so only its response parsing is used here.

fetch() is what importers call: it adds a short statement cache and stops
contacting IBKR for a while after answers that retrying cannot fix.
"""

import threading
import time

import requests
from ibflex.client import (
    REQUEST_URL,
    STMT_URL,
    BadResponseError,
    IbflexClientError,
    ResponseCodeError,
    StatementAccess,
    StatementError,
    parse_stmt_response,
)
from loguru import logger

FIRST_POLL_DELAY = 5
POLL_DELAY_STEP = 5
MAX_POLL_DELAY = 30
THROTTLED_DELAY = 60
MAX_WAIT = 300

HTTP_ATTEMPTS = 3
HTTP_RETRY_DELAYS = (10, 30)
RETRYABLE_HTTP_STATUS = frozenset({429, 500, 502, 503, 504})
HTTP_CONNECT_TIMEOUT = 10
HTTP_READ_TIMEOUT = 60
# Subclasses of ConnectionError that a retry cannot fix: certificate or proxy setup.
PERMANENT_NETWORK_ERRORS = (requests.exceptions.SSLError, requests.exceptions.ProxyError)

THROTTLED_CODE = '1018'
LOCKOUT_CODE = '1025'
# GetStatement answers meaning "not ready yet": poll the same reference code again.
NOT_READY_CODES = frozenset(
    {'1001', '1004', '1005', '1006', '1007', '1008', '1009', '1019', '1021'}
)

CACHE_TTL = 300
# Seconds fetch() stops contacting IBKR after these codes, for the token and query.
COOLDOWN_SECONDS = {
    LOCKOUT_CODE: 15 * 60,
    THROTTLED_CODE: 60,
    **dict.fromkeys(('1010', '1011', '1012', '1013', '1014', '1015', '1016', '1020'), 5 * 60),
}
# Codes about the token or account rather than one query: the pause covers every query.
TOKEN_WIDE_CODES = frozenset({LOCKOUT_CODE, THROTTLED_CODE, '1011', '1012', '1013', '1015'})


class StatementTimeoutError(IbflexClientError):
    """IBKR had not delivered the statement MAX_WAIT seconds after download() started."""


class CooldownError(ResponseCodeError):
    """IBKR was not contacted: it answered `code` recently for this token and query."""

    def __init__(self, code: str, msg: str, remaining: float):
        super().__init__(code, msg)
        self.remaining = remaining


def download(
    token: str, query_id, *, http_get=None, sleep=time.sleep, clock=time.monotonic
) -> bytes:
    """Generate a Flex statement and return its XML, giving up MAX_WAIT seconds after starting.

    Raises:
        ResponseCodeError: IBKR answered with an error code that polling cannot fix.
        StatementTimeoutError: no statement within MAX_WAIT seconds, slow requests included.
        BadResponseError: IBKR answered with an HTTP error or an unrecognised body.
        requests.exceptions.RequestException: the network kept failing.
    """
    get = http_get or requests.get
    deadline = clock() + MAX_WAIT

    def request(url, query):
        return _request(get, sleep, clock, deadline, url, token, query)

    access = _statement_access(request(REQUEST_URL, query_id))
    delay = FIRST_POLL_DELAY
    last_answer = ''
    while clock() + delay <= deadline:
        sleep(delay)
        response = request(access.Url or STMT_URL, access.ReferenceCode)
        if b'<FlexQueryResponse' in response.content:
            return response.content

        error = _statement_error(response)
        if error.ErrorCode == THROTTLED_CODE:
            delay = THROTTLED_DELAY
        elif error.ErrorCode in NOT_READY_CODES:
            delay = min(delay + POLL_DELAY_STEP, MAX_POLL_DELAY)
        else:
            raise ResponseCodeError(error.ErrorCode, error.ErrorMessage)
        last_answer = f' (last answer: code {error.ErrorCode}: {error.ErrorMessage})'

    raise StatementTimeoutError(
        f'IBKR did not finish the statement within {MAX_WAIT} s{last_answer}'
    )


def _request(get, sleep, clock, deadline, url: str, token: str, query) -> requests.Response:
    """GET a Flex endpoint before `deadline`, retrying network failures and 5xx/429.

    At most HTTP_ATTEMPTS requests; each one's timeout is cut to the time left.
    """
    for attempt in range(1, HTTP_ATTEMPTS + 1):
        remaining = deadline - clock()
        if remaining <= 0:
            raise StatementTimeoutError(f'IBKR did not answer within {MAX_WAIT} s')
        try:
            response = get(
                url,
                params={'v': '3', 't': token, 'q': query},
                headers={'user-agent': 'Java'},
                timeout=(min(HTTP_CONNECT_TIMEOUT, remaining), min(HTTP_READ_TIMEOUT, remaining)),
            )
        except PERMANENT_NETWORK_ERRORS:
            raise
        except requests.exceptions.Timeout as e:
            if deadline - clock() <= 0:
                raise StatementTimeoutError(f'IBKR did not answer within {MAX_WAIT} s') from e
            failure, throttled = e, False
        except requests.exceptions.ConnectionError as e:
            failure, throttled = e, False
        else:
            if response.ok:
                return response
            if response.status_code not in RETRYABLE_HTTP_STATUS:
                raise BadResponseError(response)
            failure, throttled = BadResponseError(response), response.status_code == 429

        if attempt == HTTP_ATTEMPTS:
            raise failure
        wait = THROTTLED_DELAY if throttled else HTTP_RETRY_DELAYS[attempt - 1]
        if clock() + wait >= deadline:
            raise failure
        sleep(wait)


def _statement_access(response: requests.Response) -> StatementAccess:
    answer = parse_stmt_response(response)
    if isinstance(answer, StatementError):
        raise ResponseCodeError(answer.ErrorCode, answer.ErrorMessage)
    return answer


def _statement_error(response: requests.Response) -> StatementError:
    if b'<FlexStatementResponse' not in response.content:
        raise BadResponseError(response)
    answer = parse_stmt_response(response)
    if not isinstance(answer, StatementError):
        raise BadResponseError(response)
    return answer


class StatementFetcher:
    """One download per token at a time, reusing recent statements and honouring pauses.

    Pauses for TOKEN_WIDE_CODES cover every query of the token; others cover one query.
    """

    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._guard = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}
        self._statements: dict[tuple[str, str], tuple[float, bytes]] = {}
        self._cooldowns: dict[tuple[str, str | None], tuple[float, str, str]] = {}

    def fetch(self, token: str, query_id) -> bytes:
        """Statement XML for a Flex query.

        Raises:
            CooldownError: IBKR recently answered with a code in COOLDOWN_SECONDS.
            Anything download() raises.
        """
        token_key, query_key = str(token), (str(token), str(query_id))
        with self._guard:
            lock = self._locks.setdefault(token_key, threading.Lock())
        with lock:
            now = self._clock()
            for key in ((token_key, None), query_key):
                if key in self._cooldowns:
                    until, code, msg = self._cooldowns[key]
                    if now < until:
                        raise CooldownError(code, msg, until - now)
                    del self._cooldowns[key]

            if query_key in self._statements:
                fetched_at, content = self._statements[query_key]
                if now - fetched_at < CACHE_TTL:
                    logger.info(
                        f'Reusing IBKR statement for query {query_id} '
                        f'downloaded {now - fetched_at:.0f} s ago'
                    )
                    return content

            try:
                content = download(token, query_id)
            except ResponseCodeError as e:
                if e.code in COOLDOWN_SECONDS:
                    key = (token_key, None) if e.code in TOKEN_WIDE_CODES else query_key
                    self._cooldowns[key] = (self._clock() + COOLDOWN_SECONDS[e.code], e.code, e.msg)
                raise
            self._statements[query_key] = (self._clock(), content)
            return content


_fetcher = StatementFetcher()


def fetch(token: str, query_id) -> bytes:
    """Statement XML for a Flex query, shared by every importer in this process."""
    return _fetcher.fetch(token, query_id)
