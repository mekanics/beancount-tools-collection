"""Unit tests for the Yuh CSV importer.

No network and no credentials required. The committed fixture is synthetic: a
real export carries personal activity, and this repo is public.

identify() tests lock HEADER_COLUMNS to the names pandas reads. Goal extract
tests cover English and German GOAL_DEPOSIT / GOAL_WITHDRAWAL rows.
"""

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from beancount.core import data

from beancount_tools_collection.importers.yuh import HEADER_COLUMNS, YuhImporter

SAMPLE = Path(__file__).parent / 'data' / 'yuh_sample.csv'

HEADER_LINE = ';'.join(HEADER_COLUMNS)

ACCOUNT = 'Assets:Cash:Yuh:Pay:CHF'
GOALS_BASE = 'Assets:Cash:Yuh:Save'

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


def _accounts(entry):
    return [posting.account for posting in entry.postings]


def _numbers(entry):
    return [posting.units.number for posting in entry.postings]


def _extract_one(importer, tmp_path, **row):
    entries = importer.extract(make_csv(tmp_path, row))
    assert len(entries) == 1
    return entries[0]


def test_german_goal_deposit_uses_recipient_for_the_account(importer, tmp_path):
    entry = _extract_one(
        importer,
        tmp_path,
        DATE='29/05/2026',
        **{
            'ACTIVITY TYPE': 'GOAL_DEPOSIT',
            'ACTIVITY NAME': 'Einzahlung für «S3a»',
            'DEBIT': '',
            'DEBIT CURRENCY': '',
            'CREDIT': '500.00',
            'CREDIT CURRENCY': 'CHF',
            'RECIPIENT': 'S3a',
        },
    )
    assert entry.date == date(2026, 5, 29)
    assert entry.payee == 'self'
    assert entry.narration == 'Einzahlung für «S3a»'
    assert _accounts(entry) == [ACCOUNT, f'{GOALS_BASE}:S3a']
    assert _numbers(entry) == [Decimal('-500.00'), Decimal('500.00')]
    assert entry.postings[0].units.currency == 'CHF'


def test_german_goal_deposit_steuern_uses_recipient_for_the_account(importer, tmp_path):
    entry = _extract_one(
        importer,
        tmp_path,
        DATE='29/05/2026',
        **{
            'ACTIVITY TYPE': 'GOAL_DEPOSIT',
            'ACTIVITY NAME': 'Einzahlung für «Steuern»',
            'DEBIT': '',
            'DEBIT CURRENCY': '',
            'CREDIT': '500.00',
            'CREDIT CURRENCY': 'CHF',
            'RECIPIENT': 'Steuern',
        },
    )
    assert entry.payee == 'self'
    assert entry.narration == 'Einzahlung für «Steuern»'
    assert _accounts(entry) == [ACCOUNT, f'{GOALS_BASE}:Steuern']
    assert _numbers(entry) == [Decimal('-500.00'), Decimal('500.00')]


def test_german_goal_withdrawal_uses_sender_for_the_account(importer, tmp_path):
    entry = _extract_one(
        importer,
        tmp_path,
        DATE='13/07/2026',
        **{
            'ACTIVITY TYPE': 'GOAL_WITHDRAWAL',
            'ACTIVITY NAME': 'Abhebung vom «Steuern»',
            'DEBIT': '-1000.00',
            'DEBIT CURRENCY': 'CHF',
            'CREDIT': '',
            'CREDIT CURRENCY': '',
            'SENDER': 'Steuern',
        },
    )
    assert entry.date == date(2026, 7, 13)
    assert entry.payee == 'self'
    assert entry.narration == 'Abhebung vom «Steuern»'
    assert _accounts(entry) == [ACCOUNT, f'{GOALS_BASE}:Steuern']
    assert _numbers(entry) == [Decimal('1000.00'), Decimal('-1000.00')]
    assert entry.postings[0].units.currency == 'CHF'


def test_english_goal_deposit_keeps_account_and_narration(importer, tmp_path):
    entry = _extract_one(
        importer,
        tmp_path,
        **{
            'ACTIVITY TYPE': 'GOAL_DEPOSIT',
            'ACTIVITY NAME': 'Deposit to «MwSt (6.2%)»',
            'DEBIT': '',
            'DEBIT CURRENCY': '',
            'CREDIT': '283.50',
            'CREDIT CURRENCY': 'CHF',
            'RECIPIENT': 'MwSt (6.2%)',
        },
    )
    assert entry.payee == 'self'
    assert entry.narration == 'Deposit to «MwSt (6.2%)»'
    assert _accounts(entry) == [ACCOUNT, f'{GOALS_BASE}:MwSt']
    assert _numbers(entry) == [Decimal('-283.50'), Decimal('283.50')]


def test_english_goal_withdrawal_strips_parentheses_from_the_account(importer, tmp_path):
    entry = _extract_one(
        importer,
        tmp_path,
        DATE='18/04/2026',
        **{
            'ACTIVITY TYPE': 'GOAL_WITHDRAWAL',
            'ACTIVITY NAME': 'Withdrawal from «Taxes (16%)»',
            'DEBIT': '-20000.00',
            'DEBIT CURRENCY': 'CHF',
            'CREDIT': '',
            'CREDIT CURRENCY': '',
            'SENDER': 'Taxes (16%)',
        },
    )
    assert entry.payee == 'self'
    assert entry.narration == 'Withdrawal from «Taxes (16%)»'
    assert _accounts(entry) == [ACCOUNT, f'{GOALS_BASE}:Taxes']
    assert _numbers(entry) == [Decimal('20000.00'), Decimal('-20000.00')]


def test_goal_account_falls_back_to_guillemets_when_recipient_is_empty(importer, tmp_path):
    entry = _extract_one(
        importer,
        tmp_path,
        **{
            'ACTIVITY TYPE': 'GOAL_DEPOSIT',
            'ACTIVITY NAME': 'Einzahlung für «S3a»',
            'DEBIT': '',
            'DEBIT CURRENCY': '',
            'CREDIT': '500.00',
            'CREDIT CURRENCY': 'CHF',
            'RECIPIENT': '',
        },
    )
    assert entry.narration == 'Einzahlung für «S3a»'
    assert _accounts(entry) == [ACCOUNT, f'{GOALS_BASE}:S3a']


def test_quoted_german_goal_csv_extracts_clean_accounts(importer, tmp_path):
    path = tmp_path / 'yuh_german_goals.csv'
    path.write_text(
        HEADER_LINE
        + '\n'
        + '29/05/2026;GOAL_DEPOSIT;"""Einzahlung für «S3a»""";;;500.00;CHF;;;"""S3a""";;;;;;\n'
        + '29/05/2026;GOAL_DEPOSIT;"""Einzahlung für «Steuern»""";;;500.00;CHF;;;"""Steuern""";;;;;;\n'
        + '13/07/2026;GOAL_WITHDRAWAL;"""Abhebung vom «Steuern»""";-1000.00;CHF;;;;;"""Steuern""";;;;;;\n',
        encoding='utf-8',
    )
    entries = importer.extract(str(path))
    assert [entry.narration for entry in entries] == [
        'Einzahlung für «S3a»',
        'Einzahlung für «Steuern»',
        'Abhebung vom «Steuern»',
    ]
    assert [_accounts(entry)[1] for entry in entries] == [
        f'{GOALS_BASE}:S3a',
        f'{GOALS_BASE}:Steuern',
        f'{GOALS_BASE}:Steuern',
    ]
    assert [_numbers(entry) for entry in entries] == [
        [Decimal('-500.00'), Decimal('500.00')],
        [Decimal('-500.00'), Decimal('500.00')],
        [Decimal('1000.00'), Decimal('-1000.00')],
    ]


def _residual(entry):
    from beancount.core import convert, interpolate

    return interpolate.compute_residual(entry.postings).reduce(convert.get_units)


def test_combined_foreign_card_transaction_leaves_the_expense_leg_blank(importer, tmp_path):
    """USD card purchase + matching CHF auto-exchange: bank and fee only, residual is the net."""
    path = make_csv(
        tmp_path,
        {
            'DATE': '05/08/2026',
            'ACTIVITY TYPE': 'CARD_TRANSACTION_OUT',
            'ACTIVITY NAME': 'Foreign Merchant',
            'DEBIT': '-50.00',
            'DEBIT CURRENCY': 'USD',
            'CREDIT': '',
            'CREDIT CURRENCY': '',
            'FEES/COMMISSION': '0',
        },
        {
            'DATE': '05/08/2026',
            'ACTIVITY TYPE': 'BANK_AUTO_ORDER_EXECUTED',
            'ACTIVITY NAME': 'Auto FX',
            'DEBIT': '-45.50',
            'DEBIT CURRENCY': 'CHF',
            'CREDIT': '50.00',
            'CREDIT CURRENCY': 'USD',
            'FEES/COMMISSION': '0.50',
            'PRICE PER UNIT': '0.91',
        },
    )
    entries = importer.extract(path)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.payee == 'Foreign Merchant'
    assert entry.meta['original-amount'] == '50.0 USD'
    assert entry.meta['exchange-rate'] == '0.91'
    assert _accounts(entry) == [ACCOUNT, 'Expenses:Fees:Yuh']
    assert _numbers(entry) == [Decimal('-45.50'), Decimal('0.50')]
    residual = _residual(entry)
    assert list(residual)[0].units.number == Decimal('-45.00')
    assert list(residual)[0].units.currency == 'CHF'


def test_combined_foreign_card_transaction_without_fee_is_single_legged(importer, tmp_path):
    path = make_csv(
        tmp_path,
        {
            'DATE': '05/08/2026',
            'ACTIVITY TYPE': 'CARD_TRANSACTION_OUT',
            'ACTIVITY NAME': 'Foreign Merchant',
            'DEBIT': '-20.00',
            'DEBIT CURRENCY': 'EUR',
            'CREDIT': '',
            'CREDIT CURRENCY': '',
            'FEES/COMMISSION': '0',
        },
        {
            'DATE': '05/08/2026',
            'ACTIVITY TYPE': 'BANK_AUTO_ORDER_EXECUTED',
            'ACTIVITY NAME': 'Auto FX',
            'DEBIT': '-19.00',
            'DEBIT CURRENCY': 'CHF',
            'CREDIT': '20.00',
            'CREDIT CURRENCY': 'EUR',
            'FEES/COMMISSION': '0',
        },
    )
    entries = importer.extract(path)
    assert len(entries) == 1
    entry = entries[0]
    assert _accounts(entry) == [ACCOUNT]
    assert _numbers(entry) == [Decimal('-19.00')]
    residual = _residual(entry)
    assert list(residual)[0].units.number == Decimal('-19.00')


def test_standalone_auto_exchange_leaves_the_expense_leg_blank(importer, tmp_path):
    """Auto-exchange with no matching foreign row: bank and fee only."""
    entry = _extract_one(
        importer,
        tmp_path,
        DATE='05/08/2026',
        **{
            'ACTIVITY TYPE': 'BANK_AUTO_ORDER_EXECUTED',
            'ACTIVITY NAME': 'Auto FX',
            'DEBIT': '-40.00',
            'DEBIT CURRENCY': 'CHF',
            'CREDIT': '45.00',
            'CREDIT CURRENCY': 'USD',
            'FEES/COMMISSION': '1.00',
            'PRICE PER UNIT': '0.89',
        },
    )
    assert entry.narration == 'Auto-exchange'
    assert entry.meta['original-amount'] == '45.0 USD'
    assert _accounts(entry) == [ACCOUNT, 'Expenses:Fees:Yuh']
    # Bank posts -(debit + fee); fee posts +fee; residual is -debit.
    assert _numbers(entry) == [Decimal('-41.00'), Decimal('1.00')]
    residual = _residual(entry)
    assert list(residual)[0].units.number == Decimal('-40.00')
    assert list(residual)[0].units.currency == 'CHF'
