"""Unit tests for the Yuh CSV importer identify() fingerprint.

No network and no credentials required. The committed fixture is synthetic: a
real export carries personal activity, and this repo is public.

A full extract suite is out of scope; the smoke test exists so HEADER_COLUMNS
cannot drift from the names pandas reads.
"""

from pathlib import Path

import pytest
from beancount.core import data

from beancount_tools_collection.importers.yuh import HEADER_COLUMNS, YuhImporter

SAMPLE = Path(__file__).parent / 'data' / 'yuh_sample.csv'

HEADER_LINE = ';'.join(HEADER_COLUMNS)

ACCOUNT = 'Assets:Cash:Yuh:Pay:CHF'

DEFAULT_ROW = {
    'DATE': '05/08/2026',
    'ACTIVITY TYPE': 'PAYMENT_TRANSACTION_OUT',
    'ACTIVITY NAME': 'Sample Merchant',
    'DEBIT': '-12.50',
    'DEBIT CURRENCY': 'CHF',
    'CREDIT': '',
    'CREDIT CURRENCY': '',
    'CARD NUMBER': '',
    'LOCALITY': '',
    'RECIPIENT': '',
    'SENDER': '',
    'FEES/COMMISSION': '0',
    'BUY/SELL': '',
    'QUANTITY': '',
    'ASSET': '',
    'PRICE PER UNIT': '',
}


def make_csv(tmp_path, *rows, header=HEADER_LINE, name='statement.csv', bom=False):
    """Write a CSV built from DEFAULT_ROW plus per-row overrides."""
    lines = [] if header is None else [header]
    for row in rows:
        merged = {**DEFAULT_ROW, **row}
        lines.append(';'.join(merged[column] for column in HEADER_COLUMNS))
    text = '\n'.join(lines)
    if lines:
        text += '\n'
    path = tmp_path / name
    path.write_text(('\ufeff' if bom else '') + text, encoding='utf-8')
    return str(path)


@pytest.fixture
def importer():
    return YuhImporter(account=ACCOUNT)


def test_identify_accepts_the_sample(importer):
    assert importer.identify(str(SAMPLE)) is True


def test_identify_accepts_an_unrenamed_download(importer, tmp_path):
    path = make_csv(tmp_path, {}, name='Account activities export.csv')
    assert importer.identify(path) is True


def test_identify_rejects_a_viseca_csv(tmp_path):
    path = tmp_path / 'yuh_2026.csv'
    path.write_text(
        'TransactionId,CardId,Date,Amount,Currency\nTRX1,CARD1,2026-08-05,10.00,CHF\n',
        encoding='utf-8',
    )
    assert YuhImporter().identify(str(path)) is False


def test_identify_rejects_a_reordered_header(tmp_path):
    reordered = ';'.join(['ACTIVITY TYPE', 'DATE', *HEADER_COLUMNS[2:]])
    path = make_csv(tmp_path, header=reordered)
    assert YuhImporter().identify(path) is False


def test_identify_rejects_comma_separated_same_names(tmp_path):
    path = make_csv(tmp_path, header=','.join(HEADER_COLUMNS))
    assert YuhImporter().identify(path) is False


def test_identify_rejects_a_missing_file(tmp_path):
    assert YuhImporter().identify(str(tmp_path / 'absent.csv')) is False


@pytest.mark.parametrize(
    ('name', 'regex', 'expected'),
    [
        ('yuh_2026.csv', r'^yuh_', True),
        ('statement.csv', r'^yuh_', False),
    ],
)
def test_identify_respects_filename_regex(tmp_path, name, regex, expected):
    path = make_csv(tmp_path, {}, name=name)
    importer = YuhImporter(regex=regex)
    assert importer.identify(path) is expected


def test_identify_default_does_not_require_yuh_in_the_name(importer, tmp_path):
    path = make_csv(tmp_path, {}, name='statement.csv')
    assert importer.identify(path) is True


def test_identify_rejects_a_yuh_named_foreign_header(tmp_path):
    path = tmp_path / 'yuh_2026.csv'
    path.write_text(
        'TransactionId,CardId,Date,Amount,Currency\nTRX1,CARD1,2026-08-05,10.00,CHF\n',
        encoding='utf-8',
    )
    assert YuhImporter().identify(str(path)) is False


def test_identify_accepts_a_byte_order_mark(importer, tmp_path):
    path = make_csv(tmp_path, {'ACTIVITY NAME': 'Café Zürich'}, bom=True)
    assert Path(path).read_bytes().startswith(b'\xef\xbb\xbf')
    assert importer.identify(path) is True


def test_extract_smoke_on_the_sample(importer):
    entries = importer.extract(str(SAMPLE))
    assert entries
    assert all(isinstance(entry, data.Transaction) for entry in entries)
