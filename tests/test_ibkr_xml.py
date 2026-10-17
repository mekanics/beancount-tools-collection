"""Activity Flex XML files are imported without the Flex Web Service."""

import pickle
import traceback
from datetime import date
from pathlib import Path

import pytest
import yaml
from beancount.core import data
from beancount.parser import printer
from ibflex import parser
from ibflex.client import ResponseCodeError
from ibflex.parser import FlexParserError
from loguru import logger

from beancount_tools_collection.importers.ibkr import (
    IBKRImporter,
    IBKRLockoutError,
    IBKRStatementError,
)

SAMPLE_XML = (Path(__file__).parent / 'data' / 'ibkr_trades_sample.xml').read_text()

EMPTY_XML = """\
<FlexQueryResponse queryName="Beancount" type="AF">
<FlexStatements count="0">
</FlexStatements>
</FlexQueryResponse>
"""

BUY = 'BUY 0.8 WIDG @ 40.25 USD'


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


def write_xml(tmp_path, contents, name='Beancount.xml'):
    path = tmp_path / name
    if isinstance(contents, bytes):
        path.write_bytes(contents)
    else:
        path.write_text(contents)
    return path


def test_identify_accepts_an_unrenamed_flex_download(importer, tmp_path):
    assert importer.identify(str(write_xml(tmp_path, SAMPLE_XML))) is True


@pytest.mark.parametrize(
    'mutate',
    [
        pytest.param(lambda raw: b'\xef\xbb\xbf' + raw, id='bom'),
        pytest.param(
            lambda raw: b'<?xml version="1.0" encoding="UTF-8"?>\n' + raw, id='declaration'
        ),
        pytest.param(lambda raw: raw.replace(b'\n', b'\r\n'), id='crlf'),
    ],
)
def test_identify_accepts_bom_declaration_and_crlf(importer, tmp_path, mutate):
    path = write_xml(tmp_path, mutate(SAMPLE_XML.encode()))
    assert importer.identify(str(path)) is True


@pytest.mark.parametrize(
    ('name', 'contents'),
    [
        ('trades.xml', '<FlexQueryResponse queryName="x" type="TCF"></FlexQueryResponse>'),
        ('error.xml', '<FlexStatementResponse><ErrorCode>1012</ErrorCode></FlexStatementResponse>'),
        ('export.csv', 'symbol,quantity\nACME,1\n'),
        ('empty.xml', ''),
    ],
)
def test_identify_rejects_non_activity_flex(importer, tmp_path, name, contents):
    assert importer.identify(str(write_xml(tmp_path, contents, name))) is False


def test_identify_rejects_a_missing_file(importer, tmp_path):
    assert importer.identify(str(tmp_path / 'absent.xml')) is False


def test_identify_rejects_a_directory(importer, tmp_path):
    assert importer.identify(str(tmp_path)) is False


def test_xml_identify_date_filename_perform_no_network_io(importer, tmp_path, monkeypatch):
    def fail(token, query_id):
        raise AssertionError('IBKR must not be contacted')

    monkeypatch.setattr('beancount_tools_collection.utils.ibkr_flex.download', fail)
    path = str(write_xml(tmp_path, SAMPLE_XML))
    assert importer.identify(path) is True
    assert importer.date(path) == date(2025, 3, 7)
    assert importer.filename(path) == 'ibkr_2025-03-03_2025-03-07.xml'


def test_date_and_filename_of_a_missing_file_are_none(importer, tmp_path):
    missing = str(tmp_path / 'absent.xml')
    assert importer.date(missing) is None
    assert importer.filename(missing) is None


def test_xml_date_is_statement_to_date(importer, tmp_path):
    assert importer.date(str(write_xml(tmp_path, SAMPLE_XML))) == date(2025, 3, 7)


def test_xml_filename_names_the_period(importer, tmp_path):
    assert importer.filename(str(write_xml(tmp_path, SAMPLE_XML))) == (
        'ibkr_2025-03-03_2025-03-07.xml'
    )


def test_config_file_has_no_date_or_filename(importer, tmp_path):
    cfg = tmp_path / 'ibkr.yaml'
    cfg.write_text('token: x\nqueryId: 1\n')
    assert importer.date(str(cfg)) is None
    assert importer.filename(str(cfg)) is None


def test_xml_and_api_paths_produce_identical_entries(importer, tmp_path, monkeypatch):
    cfg = tmp_path / 'ibkr.yaml'
    cfg.write_text(yaml.dump({'token': 'SECRETTOKEN123', 'queryId': 999999}))
    monkeypatch.setattr(
        'beancount_tools_collection.utils.ibkr_flex.download',
        lambda token, query_id: SAMPLE_XML.encode(),
    )
    via_api = [printer.format_entry(entry) for entry in importer.extract(str(cfg))]

    via_xml = [
        printer.format_entry(entry)
        for entry in importer.extract(str(write_xml(tmp_path, SAMPLE_XML)))
    ]
    assert via_xml == via_api


def test_xml_extract_without_config_file(importer, tmp_path, monkeypatch):
    def fail(token, query_id):
        raise AssertionError('IBKR must not be contacted')

    monkeypatch.setattr('beancount_tools_collection.utils.ibkr_flex.download', fail)
    entries = importer.extract(str(write_xml(tmp_path, SAMPLE_XML, 'statement.xml')))
    assert any(isinstance(entry, data.Transaction) for entry in entries)


def test_xml_extract_succeeds_during_lockout_cooldown(importer, tmp_path, monkeypatch):
    calls = []

    def locked(token, query_id):
        calls.append(query_id)
        raise ResponseCodeError('1025', 'Too many failed attempts.')

    monkeypatch.setattr('beancount_tools_collection.utils.ibkr_flex.download', locked)
    cfg = tmp_path / 'ibkr.yaml'
    cfg.write_text(yaml.dump({'token': 'SECRETTOKEN123', 'queryId': 999999}))
    with pytest.raises(IBKRLockoutError):
        importer.extract(str(cfg))

    entries = importer.extract(str(write_xml(tmp_path, SAMPLE_XML)))
    assert calls == [999999]
    assert any(isinstance(entry, data.Transaction) for entry in entries)


def test_xml_extract_drops_unknown_note_code(importer, tmp_path):
    xml = SAMPLE_XML.replace('notes="R"', 'notes="ZZ"', 1)
    entries = importer.extract(str(write_xml(tmp_path, xml)))
    buys = [
        entry for entry in entries if isinstance(entry, data.Transaction) and entry.narration == BUY
    ]
    assert len(buys) == 1


def test_truncated_xml_raises_statement_error(importer, tmp_path):
    path = write_xml(tmp_path, '<FlexQueryResponse queryName="Beancount" type="AF"><FlexStatements')
    with pytest.raises(IBKRStatementError) as excinfo:
        importer.extract(str(path))
    assert str(path) in str(excinfo.value)
    assert 'could not parse' in str(excinfo.value)


def test_missing_xml_at_extract_raises_statement_error(importer, tmp_path):
    path = tmp_path / 'gone.xml'
    with pytest.raises(IBKRStatementError) as excinfo:
        importer.extract(str(path))
    assert str(path) in str(excinfo.value)
    assert 'cannot read' in str(excinfo.value)


def test_xml_extract_ignores_fpath_pickle(importer, tmp_path):
    importer.fpath = str(tmp_path / 'missing.pickle')
    entries = importer.extract(str(write_xml(tmp_path, SAMPLE_XML)))
    assert any(isinstance(entry, data.Transaction) for entry in entries)


def test_invalid_statement_dates_do_not_raise(importer, tmp_path):
    xml = SAMPLE_XML.replace('fromDate="20250303"', 'fromDate="20250231"', 1)
    path = str(write_xml(tmp_path, xml))
    assert importer.identify(path) is True
    assert importer.date(path) is None
    assert importer.filename(path) is None


def test_parser_error_other_than_flex_parser_error_names_the_file(importer, tmp_path, monkeypatch):
    path = write_xml(tmp_path, SAMPLE_XML)

    def boom(response):
        raise RuntimeError('not xml')

    monkeypatch.setattr('beancount_tools_collection.importers.ibkr.parser.parse', boom)
    with pytest.raises(IBKRStatementError) as excinfo:
        importer.extract(str(path))
    message = str(excinfo.value)
    assert str(path) in message
    assert 'RuntimeError' in message
    assert 'not xml' in message


@pytest.mark.parametrize('error', [FlexParserError, RuntimeError])
def test_api_path_redacts_a_token_leaked_by_the_parser(importer, tmp_path, monkeypatch, error):
    token = 'SECRETTOKEN123'
    cfg = tmp_path / 'ibkr.yaml'
    cfg.write_text(yaml.dump({'token': token, 'queryId': 999999}))
    monkeypatch.setattr(
        'beancount_tools_collection.utils.ibkr_flex.download',
        lambda _token, query_id: SAMPLE_XML.encode(),
    )

    def boom(response):
        raise error(f'bad {token}')

    monkeypatch.setattr('beancount_tools_collection.importers.ibkr.parser.parse', boom)
    log_lines = []
    handler_id = logger.add(lambda msg: log_lines.append(str(msg)), format='{message}')
    try:
        with pytest.raises(IBKRStatementError) as excinfo:
            importer.extract(str(cfg))
    finally:
        logger.remove(handler_id)

    err = excinfo.value
    rendered = ''.join(traceback.format_exception(type(err), err, err.__traceback__))
    logged = '\n'.join(log_lines)
    assert token not in str(err)
    assert '<redacted>' in str(err)
    assert token not in rendered
    assert token not in logged
    assert '<redacted>' in logged
    assert err.__cause__ is None


def test_config_file_with_fpath_uses_the_pickle_and_skips_the_api(importer, tmp_path, monkeypatch):
    pickled = tmp_path / 'statement.pickle'
    pickled.write_bytes(pickle.dumps(parser.parse(SAMPLE_XML.encode())))
    importer.fpath = str(pickled)

    def fail(token, query_id):
        raise AssertionError('IBKR must not be contacted')

    monkeypatch.setattr('beancount_tools_collection.utils.ibkr_flex.download', fail)
    cfg = tmp_path / 'ibkr.yaml'
    cfg.write_text(yaml.dump({'token': 'SECRETTOKEN123', 'queryId': 999999}))
    entries = importer.extract(str(cfg))
    assert any(isinstance(entry, data.Transaction) for entry in entries)


def test_unexpected_statement_type_raises_statement_error(importer, tmp_path, monkeypatch):
    path = write_xml(tmp_path, SAMPLE_XML)
    monkeypatch.setattr(
        'beancount_tools_collection.importers.ibkr.parser.parse',
        lambda response: 'not a statement',
    )
    with pytest.raises(IBKRStatementError) as excinfo:
        importer.extract(str(path))
    message = str(excinfo.value)
    assert 'unexpected statement type' in message
    assert str(path) in message


def test_xml_with_no_statements_returns_empty_list(importer, tmp_path):
    path = str(write_xml(tmp_path, EMPTY_XML))
    assert importer.extract(path) == []
    assert importer.date(path) is None
    assert importer.filename(path) is None
