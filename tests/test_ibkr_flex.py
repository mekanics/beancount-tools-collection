"""Offline tests for the IBKR Flex Web Service client.

IBKR allows one request per second and ten per minute per token, and blocks the
token (code 1025) after too many failed attempts. These tests drive the client
against a scripted fake on a virtual clock and assert what IBKR would observe:
when each request arrives, how many arrive, and what the caller gets back.
"""

import threading
import time

import pytest
import requests
from ibflex.client import BadResponseError, ResponseCodeError

from beancount_tools_collection.utils import ibkr_flex
from beancount_tools_collection.utils.ibkr_flex import (
    CooldownError,
    StatementFetcher,
    StatementTimeoutError,
)


def flex_response(status, **children):
    body = ''.join(f'<{tag}>{text}</{tag}>' for tag, text in children.items())
    return (
        f'<FlexStatementResponse timestamp="17 October, 2026 09:00 AM EDT">'
        f'<Status>{status}</Status>{body}</FlexStatementResponse>'
    ).encode()


def flex_error(code, message='IBKR says no.'):
    return flex_response('Fail', ErrorCode=code, ErrorMessage=message)


ACCESS = flex_response('Success', ReferenceCode='42', Url='https://flex.test/GetStatement')
STATEMENT = (
    b'<FlexQueryResponse queryName="q" type="AF"><FlexStatements count="0"/></FlexQueryResponse>'
)


class FakeIBKR:
    """Scripted Flex endpoints on a virtual clock.

    Each script is a list of bodies, (status, body) tuples or exceptions; the
    last item repeats forever. Every request takes `latency` seconds, or times
    out once that exceeds the client's read timeout.
    """

    def __init__(self, send=(ACCESS,), poll=(STATEMENT,), latency=0.0):
        self.now = 0.0
        self.latency = latency
        self.requests = []
        self._scripts = {'send': list(send), 'poll': list(poll)}

    def sleep(self, seconds):
        self.now += seconds

    def clock(self):
        return self.now

    def get(self, url, params, headers, timeout):
        endpoint = 'send' if url.endswith('SendRequest') else 'poll'
        self.requests.append((self.now, endpoint))
        _, read_timeout = timeout
        if self.latency > read_timeout:
            self.now += read_timeout
            raise requests.exceptions.ReadTimeout('read timed out')
        self.now += self.latency
        script = self._scripts[endpoint]
        item = script.pop(0) if len(script) > 1 else script[0]
        if isinstance(item, Exception):
            raise item
        status, body = item if isinstance(item, tuple) else (200, item)
        response = requests.Response()
        response.status_code = status
        response._content = body
        return response

    def download(self):
        return ibkr_flex.download(
            'tok', '123', http_get=self.get, sleep=self.sleep, clock=self.clock
        )

    @property
    def times(self):
        return [t for t, _ in self.requests]


def busiest_minute(times):
    return max(sum(1 for t in times if start <= t < start + 60) for start in times)


# ---------------------------------------------------------------------------
# download(): request cadence
# ---------------------------------------------------------------------------


def test_returns_statement_once_ibkr_has_generated_it():
    ibkr = FakeIBKR(poll=[flex_error('1019')] * 3 + [STATEMENT])
    assert ibkr.download() == STATEMENT


def test_first_poll_waits_more_than_a_second_after_the_request():
    ibkr = FakeIBKR()
    ibkr.download()
    send_time, poll_time = ibkr.times
    assert poll_time - send_time >= 1


def test_slow_statement_never_exceeds_ibkr_rate_limit():
    ibkr = FakeIBKR(poll=[flex_error('1019')] * 10 + [STATEMENT])
    assert ibkr.download() == STATEMENT
    gaps = [b - a for a, b in zip(ibkr.times, ibkr.times[1:], strict=False)]
    assert min(gaps) >= 1
    assert busiest_minute(ibkr.times) <= 10


def test_gives_up_when_statement_is_never_ready():
    ibkr = FakeIBKR(poll=[flex_error('1019', 'Statement generation in progress.')])
    with pytest.raises(StatementTimeoutError) as excinfo:
        ibkr.download()
    assert '1019' in str(excinfo.value)
    assert ibkr.now <= ibkr_flex.MAX_WAIT
    assert busiest_minute(ibkr.times) <= 10


@pytest.mark.parametrize('latency', [50, 59])
def test_slow_requests_count_towards_the_time_limit(latency):
    ibkr = FakeIBKR(poll=[flex_error('1019')], latency=latency)
    with pytest.raises(StatementTimeoutError):
        ibkr.download()
    assert ibkr.now <= ibkr_flex.MAX_WAIT


def test_server_error_near_the_time_limit_is_not_retried_past_it():
    ibkr = FakeIBKR(poll=[flex_error('1019')] * 11 + [(503, b'Service Unavailable')])
    with pytest.raises(BadResponseError):
        ibkr.download()
    assert ibkr.now <= ibkr_flex.MAX_WAIT


def test_unresponsive_ibkr_fails_within_the_time_limit():
    ibkr = FakeIBKR(latency=1000)
    with pytest.raises(requests.exceptions.Timeout):
        ibkr.download()
    assert len(ibkr.requests) == ibkr_flex.HTTP_ATTEMPTS
    assert ibkr.now <= ibkr_flex.MAX_WAIT


@pytest.mark.parametrize('code', ['1001', '1004', '1009', '1021'])
def test_not_ready_codes_keep_polling(code):
    ibkr = FakeIBKR(poll=[flex_error(code), STATEMENT])
    assert ibkr.download() == STATEMENT


def test_throttled_poll_waits_a_full_minute_before_the_next():
    ibkr = FakeIBKR(poll=[flex_error('1018'), STATEMENT])
    assert ibkr.download() == STATEMENT
    _, throttled, retried = ibkr.times
    assert retried - throttled >= 60


# ---------------------------------------------------------------------------
# download(): failures stop instead of retrying
# ---------------------------------------------------------------------------


def test_lockout_on_request_is_not_retried():
    ibkr = FakeIBKR(send=[flex_error('1025', 'Too many failed attempts.')])
    with pytest.raises(ResponseCodeError) as excinfo:
        ibkr.download()
    assert excinfo.value.code == '1025'
    assert len(ibkr.requests) == 1


def test_lockout_while_polling_is_not_retried():
    ibkr = FakeIBKR(poll=[flex_error('1025', 'Too many failed attempts.')])
    with pytest.raises(ResponseCodeError) as excinfo:
        ibkr.download()
    assert excinfo.value.code == '1025'
    assert len(ibkr.requests) == 2


def test_permanent_poll_error_is_not_retried():
    ibkr = FakeIBKR(poll=[flex_error('1017', 'Reference code is invalid.')])
    with pytest.raises(ResponseCodeError) as excinfo:
        ibkr.download()
    assert excinfo.value.code == '1017'
    assert len(ibkr.requests) == 2


def test_http_server_error_is_retried_a_bounded_number_of_times():
    ibkr = FakeIBKR(send=[(503, b'Service Unavailable')])
    with pytest.raises(BadResponseError):
        ibkr.download()
    assert len(ibkr.requests) == ibkr_flex.HTTP_ATTEMPTS
    gaps = [b - a for a, b in zip(ibkr.times, ibkr.times[1:], strict=False)]
    assert min(gaps) >= 1


def test_http_server_error_recovers_on_retry():
    ibkr = FakeIBKR(send=[(502, b'Bad Gateway'), ACCESS])
    assert ibkr.download() == STATEMENT


def test_http_429_waits_a_full_minute_before_retrying():
    ibkr = FakeIBKR(send=[(429, b'Too Many Requests'), ACCESS])
    assert ibkr.download() == STATEMENT
    first, retried, _ = ibkr.times
    assert retried - first >= 60


@pytest.mark.parametrize(
    'error',
    [
        requests.exceptions.SSLError('certificate verify failed'),
        requests.exceptions.ProxyError('Unable to connect to proxy'),
    ],
)
def test_certificate_and_proxy_errors_are_not_retried(error):
    ibkr = FakeIBKR(send=[error])
    with pytest.raises(type(error)):
        ibkr.download()
    assert len(ibkr.requests) == 1


def test_http_client_error_is_not_retried():
    ibkr = FakeIBKR(send=[(403, b'Forbidden')])
    with pytest.raises(BadResponseError):
        ibkr.download()
    assert len(ibkr.requests) == 1


def test_network_errors_are_retried_then_raised():
    ibkr = FakeIBKR(send=[requests.exceptions.ConnectionError('connection refused')])
    with pytest.raises(requests.exceptions.ConnectionError):
        ibkr.download()
    assert len(ibkr.requests) == ibkr_flex.HTTP_ATTEMPTS


@pytest.mark.parametrize('body', [b'<html>Scheduled maintenance</html>', ACCESS])
def test_unrecognised_poll_body_raises_bad_response(body):
    ibkr = FakeIBKR(poll=[body])
    with pytest.raises(BadResponseError):
        ibkr.download()
    assert len(ibkr.requests) == 2


# ---------------------------------------------------------------------------
# StatementFetcher: cooldowns and cache
# ---------------------------------------------------------------------------


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


@pytest.fixture
def scripted(monkeypatch):
    """A fetcher whose downloads return/raise the given outcomes in order."""

    def make(*outcomes):
        clock = FakeClock()
        calls = []

        def fake_download(token, query_id):
            calls.append((token, query_id))
            outcome = outcomes[min(len(calls), len(outcomes)) - 1]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        monkeypatch.setattr(ibkr_flex, 'download', fake_download)
        return StatementFetcher(clock=clock), clock, calls

    return make


def test_lockout_pauses_requests_for_this_token_and_query(scripted):
    fetcher, clock, calls = scripted(
        ResponseCodeError('1025', 'Too many failed attempts.'), STATEMENT
    )
    with pytest.raises(ResponseCodeError):
        fetcher.fetch('tok', 123)

    clock.now = 14 * 60
    with pytest.raises(CooldownError) as excinfo:
        fetcher.fetch('tok', 123)
    assert excinfo.value.code == '1025'
    assert excinfo.value.remaining == pytest.approx(60)
    assert len(calls) == 1

    clock.now = 15 * 60
    assert fetcher.fetch('tok', 123) == STATEMENT
    assert len(calls) == 2


def test_cooldown_does_not_block_a_new_token(scripted):
    fetcher, _, calls = scripted(ResponseCodeError('1025', 'Too many failed attempts.'), STATEMENT)
    with pytest.raises(ResponseCodeError):
        fetcher.fetch('old-token', 123)
    assert fetcher.fetch('new-token', 123) == STATEMENT
    assert len(calls) == 2


@pytest.mark.parametrize('code', ['1025', '1018', '1012', '1013'])
def test_token_wide_pause_covers_every_query_of_the_token(scripted, code):
    fetcher, _, calls = scripted(ResponseCodeError(code, 'rejected'), STATEMENT)
    with pytest.raises(ResponseCodeError):
        fetcher.fetch('tok', 1)
    with pytest.raises(CooldownError) as excinfo:
        fetcher.fetch('tok', 2)
    assert excinfo.value.code == code
    assert len(calls) == 1


def test_query_pause_does_not_block_other_queries(scripted):
    fetcher, _, calls = scripted(ResponseCodeError('1014', 'Query is invalid.'), STATEMENT)
    with pytest.raises(ResponseCodeError):
        fetcher.fetch('tok', 1)
    with pytest.raises(CooldownError):
        fetcher.fetch('tok', 1)
    assert fetcher.fetch('tok', 2) == STATEMENT
    assert len(calls) == 2


def test_throttle_pauses_requests_for_a_minute(scripted):
    fetcher, clock, calls = scripted(ResponseCodeError('1018', 'Too many requests.'), STATEMENT)
    with pytest.raises(ResponseCodeError):
        fetcher.fetch('tok', 123)
    clock.now = 59
    with pytest.raises(CooldownError):
        fetcher.fetch('tok', 123)
    clock.now = 60
    assert fetcher.fetch('tok', 123) == STATEMENT


@pytest.mark.parametrize('code', ['1012', '1013', '1015', '1020'])
def test_rejected_credentials_pause_requests_for_five_minutes(scripted, code):
    fetcher, clock, calls = scripted(ResponseCodeError(code, 'rejected'), STATEMENT)
    with pytest.raises(ResponseCodeError):
        fetcher.fetch('tok', 123)
    clock.now = 5 * 60 - 1
    with pytest.raises(CooldownError) as excinfo:
        fetcher.fetch('tok', 123)
    assert excinfo.value.code == code
    clock.now = 5 * 60
    assert fetcher.fetch('tok', 123) == STATEMENT


def test_slow_statement_does_not_pause_requests(scripted):
    fetcher, _, calls = scripted(StatementTimeoutError('still generating'), STATEMENT)
    with pytest.raises(StatementTimeoutError):
        fetcher.fetch('tok', 123)
    assert fetcher.fetch('tok', 123) == STATEMENT
    assert len(calls) == 2


def test_statement_is_reused_for_five_minutes(scripted):
    fetcher, clock, calls = scripted(STATEMENT)
    assert fetcher.fetch('tok', 123) == STATEMENT
    clock.now = 5 * 60 - 1
    assert fetcher.fetch('tok', 123) == STATEMENT
    assert len(calls) == 1
    clock.now = 5 * 60
    assert fetcher.fetch('tok', 123) == STATEMENT
    assert len(calls) == 2


def test_cache_is_per_query(scripted):
    fetcher, _, calls = scripted(STATEMENT)
    fetcher.fetch('tok', 1)
    fetcher.fetch('tok', 2)
    assert calls == [('tok', 1), ('tok', 2)]


def test_concurrent_fetches_share_one_download(monkeypatch):
    started = threading.Event()
    release = threading.Event()
    calls = []

    def slow_download(token, query_id):
        calls.append(query_id)
        started.set()
        release.wait(5)
        return STATEMENT

    monkeypatch.setattr(ibkr_flex, 'download', slow_download)
    fetcher = StatementFetcher()
    results = []
    threads = [
        threading.Thread(target=lambda: results.append(fetcher.fetch('tok', 123))) for _ in range(2)
    ]
    for t in threads:
        t.start()
    started.wait(5)
    time.sleep(0.2)  # let the second thread reach fetch() while the first is downloading
    release.set()
    for t in threads:
        t.join(5)

    assert results == [STATEMENT, STATEMENT]
    assert len(calls) == 1


def test_downloads_for_one_token_never_overlap(monkeypatch):
    counter = threading.Lock()
    active = peak = 0

    def slow_download(token, query_id):
        nonlocal active, peak
        with counter:
            active += 1
            peak = max(peak, active)
        time.sleep(0.05)
        with counter:
            active -= 1
        return STATEMENT

    monkeypatch.setattr(ibkr_flex, 'download', slow_download)
    fetcher = StatementFetcher()
    threads = [threading.Thread(target=fetcher.fetch, args=('tok', query)) for query in (1, 2, 3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)

    assert peak == 1
