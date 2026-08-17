"""Unit tests for the Viseca JSON importer."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest
from beancount.core import convert, data, interpolate

from beancount_tools_collection.importers.viseca import VisecaImporter

LIABILITY = 'Liabilities:CreditCard:Viseca'
PARTNER = 'Assets:Owed-to-Me:Partner'


def make_json(tmp_path, *transactions, name='viseca_export.json'):
    path = tmp_path / name
    path.write_text(json.dumps({'list': list(transactions)}), encoding='utf-8')
    return str(path)


def booked_expense(**overrides):
    row = {
        'transactionId': 'TRX1',
        'stateType': 'booked',
        'date': '2026-08-05T12:00:00',
        'prettyName': 'Sample Merchant',
        'merchantName': 'Sample Merchant GmbH',
        'details': 'Card purchase',
        'currency': 'CHF',
        'amount': 20.00,
        'pfmCategory': {'id': 'groceries'},
    }
    row.update(overrides)
    return row


@pytest.fixture
def importer():
    return VisecaImporter(account=LIABILITY)


def _accounts(entry):
    return [posting.account for posting in entry.postings]


def _numbers(entry):
    return [posting.units.number for posting in entry.postings]


def _residual(entry):
    return interpolate.compute_residual(entry.postings).reduce(convert.get_units)


def test_identify_matches_viseca_json_name(importer, tmp_path):
    path = make_json(tmp_path, booked_expense())
    assert importer.identify(path) is True


def test_extract_emits_only_the_card_liability(importer, tmp_path):
    path = make_json(tmp_path, booked_expense())
    entries = importer.extract(path)
    assert len(entries) == 1
    entry = entries[0]
    assert isinstance(entry, data.Transaction)
    assert entry.payee == 'Sample Merchant'
    assert entry.meta['category'] == 'groceries'
    assert _accounts(entry) == [LIABILITY]
    assert _numbers(entry) == [Decimal('-20.00')]
    residual = _residual(entry)
    assert list(residual)[0].units.number == Decimal('-20.00')


def test_extract_with_partner_share_leaves_the_residual(tmp_path):
    importer = VisecaImporter(
        account=LIABILITY,
        split_expense_account=PARTNER,
        split_ratio=0.5,
    )
    path = make_json(tmp_path, booked_expense(amount=20.00))
    entries = importer.extract(path)
    assert len(entries) == 1
    entry = entries[0]
    assert _accounts(entry) == [LIABILITY, PARTNER]
    assert _numbers(entry) == [Decimal('-20.00'), Decimal('10.00')]
    residual = _residual(entry)
    assert list(residual)[0].units.number == Decimal('-10.00')


def test_missing_payee_stays_blank(importer, tmp_path):
    path = make_json(
        tmp_path,
        booked_expense(prettyName=None, merchantName=None),
    )
    entry = importer.extract(path)[0]
    assert entry.payee is None


def test_deposits_are_skipped(importer, tmp_path):
    path = make_json(
        tmp_path,
        booked_expense(pfmCategory={'id': 'deposits'}, amount=-100.00),
    )
    assert importer.extract(path) == []


def test_pending_rows_are_skipped(importer, tmp_path):
    path = make_json(tmp_path, booked_expense(stateType='pending'))
    assert importer.extract(path) == []
