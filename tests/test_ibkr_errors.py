"""Offline unit tests for IBKR importer fetch/credential error surfacing.

No network and no .env credentials required.
"""

from unittest.mock import MagicMock

import pytest
import requests
import yaml
from beangulp.exceptions import ExceptionsTrap
from ibflex import Types
from ibflex.client import ERROR_CODES, BadResponseError, ResponseCodeError
from ibflex.parser import FlexParserError

from beancount_tools_collection.importers.ibkr import (
    _PERMANENT_CODES,
    _TEMPORARY_CODES,
    IBKRConfigError,
    IBKRImporter,
    IBKRImportError,
    IBKRStatementError,
    IBKRTemporaryError,
    _classify_response_code_error,
    _redact,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def importer():
    return IBKRImporter(
        Mainaccount='Assets:Invest:IB',
        DivAccount='Income:Dividends:IB',
        WHTAccount='Expenses:Taxes:IB:WHT',
        PnLAccount='Income:PnL:IB',
        FeesAccount='Expenses:Fees:IB',
        configFile='ibkr.yaml',
        cashAccountType='Cash',
        stockAccountType='Invest',
    )


@pytest.fixture
def ibkr_yaml(tmp_path):
    cfg = tmp_path / 'ibkr.yaml'
    cfg.write_text(yaml.dump({'token': 'SECRETTOKEN123', 'queryId': 999999}))
    return str(cfg)


# ---------------------------------------------------------------------------
# Pure helpers / taxonomy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize('code', sorted(_PERMANENT_CODES))
def test_permanent_codes_raise_config_error(code):
    err = ResponseCodeError(code, 'boom')
    classified = _classify_response_code_error(err, '/path/ibkr.yaml')
    assert isinstance(classified, IBKRConfigError)
    assert code in str(classified)
    assert '/path/ibkr.yaml' in str(classified)


@pytest.mark.parametrize('code', sorted(_TEMPORARY_CODES))
def test_temporary_codes_raise_temporary_error(code):
    err = ResponseCodeError(code, 'try again shortly')
    classified = _classify_response_code_error(err, '/path/ibkr.yaml')
    assert isinstance(classified, IBKRTemporaryError)
    assert code in str(classified)


def test_code_tables_partition_ibflex_error_codes():
    covered = set(_PERMANENT_CODES) | set(_TEMPORARY_CODES)
    assert covered == set(ERROR_CODES)
    assert set(_PERMANENT_CODES).isdisjoint(_TEMPORARY_CODES)


def test_unknown_code_falls_back_to_base_import_error():
    err = ResponseCodeError('9999', 'mysterious failure')
    classified = _classify_response_code_error(err, '/path/ibkr.yaml')
    assert type(classified) is IBKRImportError
    assert '9999' in str(classified)
    assert 'mysterious failure' in str(classified)


def test_permanent_message_shape_includes_remediation():
    err = ResponseCodeError('1012', 'Token has expired.')
    classified = _classify_response_code_error(err, '/ledger/ibkr.yaml')
    msg = str(classified)
    assert msg.startswith('IBKR import failed:')
    assert '1012' in msg
    assert 'expired' in msg.lower()
    assert '/ledger/ibkr.yaml' in msg
    assert 'token' in msg.lower()


def test_redact_replaces_token():
    assert _redact('url?t=SECRETTOKEN123&q=1', 'SECRETTOKEN123') == 'url?t=<redacted>&q=1'
    assert _redact('also SECRETTOKEN123 in prose', 'SECRETTOKEN123') == 'also <redacted> in prose'


def test_redact_short_token_only_scrubs_query_param():
    # Live tests use token="0"; must not scramble HTTP status codes etc.
    msg = 'SendRequest?v=3&t=0&q=999 (status 403 Forbidden)'
    assert _redact(msg, '0') == ('SendRequest?v=3&t=<redacted>&q=999 (status 403 Forbidden)')


def test_redact_noop_without_token():
    assert _redact('nothing to hide', '') == 'nothing to hide'


# ---------------------------------------------------------------------------
# extract() end-to-end with monkeypatched download
# ---------------------------------------------------------------------------


def test_extract_1012_raises_config_error(importer, ibkr_yaml, monkeypatch):
    def boom(token, query_id):
        raise ResponseCodeError('1012', 'Token has expired.')

    monkeypatch.setattr('beancount_tools_collection.importers.ibkr.client.download', boom)
    with pytest.raises(IBKRConfigError) as excinfo:
        importer.extract(ibkr_yaml)
    assert '1012' in str(excinfo.value)
    assert 'expired' in str(excinfo.value).lower()
    assert ibkr_yaml in str(excinfo.value)


def test_extract_1018_raises_temporary_error(importer, ibkr_yaml, monkeypatch):
    def boom(token, query_id):
        raise ResponseCodeError('1018', 'Too many requests have been made from this token.')

    monkeypatch.setattr('beancount_tools_collection.importers.ibkr.client.download', boom)
    with pytest.raises(IBKRTemporaryError) as excinfo:
        importer.extract(ibkr_yaml)
    assert '1018' in str(excinfo.value)


def _format_exception_chain(exc: BaseException) -> str:
    """Format exception including __cause__/__context__ (what Fava embeds)."""
    import traceback

    return ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))


def test_token_never_leaks_in_exception_or_logs(importer, ibkr_yaml, monkeypatch):
    token = 'SECRETTOKEN123'

    def boom(tok, query_id):
        raise requests.exceptions.ProxyError(
            f"HTTPSConnectionPool(host='gdcdyn.interactivebrokers.com', "
            f'port=443): Max retries exceeded with url: '
            f'/Universal/servlet/FlexStatementService.SendRequest'
            f'?v=3&t={tok}&q=999'
        )

    monkeypatch.setattr('beancount_tools_collection.importers.ibkr.client.download', boom)

    # Capture loguru output on stderr (default sink).
    from loguru import logger

    log_lines = []
    handler_id = logger.add(lambda msg: log_lines.append(msg), format='{message}')
    try:
        with pytest.raises(IBKRImportError) as excinfo:
            importer.extract(ibkr_yaml)
    finally:
        logger.remove(handler_id)

    err = excinfo.value
    tb = _format_exception_chain(err)
    assert token not in str(err)
    assert '<redacted>' in str(err)
    # Fava embeds traceback.format_exc() in ImporterExtractError — must be clean.
    assert token not in tb
    assert err.__cause__ is None
    # raise ... from None keeps __context__ attached but suppresses it in display.
    assert err.__suppress_context__ is True
    joined_logs = '\n'.join(log_lines)
    assert token not in joined_logs


def test_bad_response_traceback_excludes_body_and_token(importer, ibkr_yaml, monkeypatch):
    token = 'SECRETTOKEN123'
    response = MagicMock()
    response.status_code = 500
    response.content = f'body leaking {token}'.encode()

    def boom(tok, query_id):
        raise BadResponseError(response)

    monkeypatch.setattr('beancount_tools_collection.importers.ibkr.client.download', boom)
    with pytest.raises(IBKRImportError) as excinfo:
        importer.extract(ibkr_yaml)
    err = excinfo.value
    tb = _format_exception_chain(err)
    assert token not in str(err)
    assert token not in tb
    assert 'body leaking' not in tb
    assert err.__cause__ is None
    assert err.__suppress_context__ is True


# ---------------------------------------------------------------------------
# Config errors
# ---------------------------------------------------------------------------


def test_missing_credentials_file_raises_config_error(importer, tmp_path):
    missing = str(tmp_path / 'does-not-exist.yaml')
    with pytest.raises(IBKRConfigError) as excinfo:
        importer.extract(missing)
    assert missing in str(excinfo.value)


def test_missing_token_key_raises_config_error(importer, tmp_path):
    cfg = tmp_path / 'ibkr.yaml'
    cfg.write_text(yaml.dump({'queryId': 123}))
    with pytest.raises(IBKRConfigError) as excinfo:
        importer.extract(str(cfg))
    assert 'token' in str(excinfo.value)
    assert str(cfg) in str(excinfo.value)


def test_non_yaml_credentials_raises_config_error(importer, tmp_path):
    cfg = tmp_path / 'ibkr.yaml'
    cfg.write_bytes(b'\x00\x01\x02\xff binary not yaml: [')
    # Also try with invalid YAML text that safe_load rejects
    cfg.write_text(':\n  - this: is: broken: yaml: [[[')
    with pytest.raises(IBKRConfigError) as excinfo:
        importer.extract(str(cfg))
    assert str(cfg) in str(excinfo.value)


def test_keyboard_interrupt_during_config_load_propagates(importer, ibkr_yaml, monkeypatch):
    def interrupt(_stream):
        raise KeyboardInterrupt()

    monkeypatch.setattr('beancount_tools_collection.importers.ibkr.yaml.safe_load', interrupt)
    with pytest.raises(KeyboardInterrupt):
        importer.extract(ibkr_yaml)


# ---------------------------------------------------------------------------
# Inverse / parse / BadResponse
# ---------------------------------------------------------------------------


def test_empty_statement_still_returns_empty_list(importer, ibkr_yaml, monkeypatch):
    empty = Types.FlexQueryResponse(queryName='test', type='AF', FlexStatements=())
    monkeypatch.setattr(
        importer,
        '_download_statement',
        lambda token, queryId, filepath: empty,
    )
    assert importer.extract(ibkr_yaml) == []


def test_flex_parser_error_raises_statement_error(importer, ibkr_yaml, monkeypatch):
    monkeypatch.setattr(
        'beancount_tools_collection.importers.ibkr.client.download',
        lambda token, query_id: b'<FlexQueryResponse/>',
    )
    monkeypatch.setattr(
        'beancount_tools_collection.importers.ibkr.parser.parse',
        lambda response: (_ for _ in ()).throw(FlexParserError('bad notes code')),
    )
    with pytest.raises(IBKRStatementError) as excinfo:
        importer.extract(ibkr_yaml)
    assert '_IBKR_CODE_ALIASES' in str(excinfo.value)


def test_bad_response_error_excludes_body(importer, ibkr_yaml, monkeypatch):
    response = MagicMock()
    response.status_code = 500
    response.content = b'SECRET BODY THAT MUST NOT LEAK'

    def boom(token, query_id):
        raise BadResponseError(response)

    monkeypatch.setattr('beancount_tools_collection.importers.ibkr.client.download', boom)
    with pytest.raises(IBKRImportError) as excinfo:
        importer.extract(ibkr_yaml)
    msg = str(excinfo.value)
    assert 'HTTP 500' in msg
    assert 'SECRET BODY' not in msg
    assert '26 bytes' in msg or 'bytes' in msg


# ---------------------------------------------------------------------------
# identify / account must not hit the network
# ---------------------------------------------------------------------------


def test_identify_and_account_perform_no_network_io(importer, ibkr_yaml, monkeypatch):
    def fail_get(*args, **kwargs):
        raise AssertionError('requests.get must not be called')

    monkeypatch.setattr(requests, 'get', fail_get)
    assert importer.identify(ibkr_yaml) is True
    assert importer.account(ibkr_yaml) == 'Assets:Invest:IB'
    assert importer.date(ibkr_yaml) is None
    assert importer.filename(ibkr_yaml) is None


# ---------------------------------------------------------------------------
# beangulp CLI ExceptionsTrap shape
# ---------------------------------------------------------------------------


def test_exceptions_trap_prints_bare_message_without_traceback():
    logs = []

    def log(msg, **kwargs):
        logs.append(str(msg))

    trap = ExceptionsTrap(log)
    with trap:
        raise IBKRConfigError('IBKR import failed: the IBKR Flex token has expired (code 1012)')

    assert trap
    joined = '\n'.join(logs)
    assert 'IBKR import failed: the IBKR Flex token has expired (code 1012)' in joined
    assert 'Traceback' not in joined
    assert 'Exception in importer code' not in joined


def test_fava_style_extract_failure_is_not_empty_list(importer, ibkr_yaml, monkeypatch):
    """Fava shows the yellow warning only when extract returns []. Raising must win.

    Mirrors fava.core.ingest.IngestModule.extract + Import.svelte:
    - exception -> red notify_err
    - empty list -> yellow "No entries to import from this file."
    """

    def boom(token, query_id):
        raise ResponseCodeError('1012', 'Token has expired.')

    monkeypatch.setattr('beancount_tools_collection.importers.ibkr.client.download', boom)

    try:
        entries = importer.extract(ibkr_yaml)
    except IBKRConfigError as err:
        # Simulate Fava's notify_err path: non-empty error message, not [].
        assert '1012' in str(err)
        assert 'expired' in str(err).lower()
        return

    pytest.fail(
        f'extract() returned {entries!r}; Fava would show '
        f"'No entries to import from this file.' instead of an error"
    )
