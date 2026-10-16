"""Offline tests for IBKR stock trades: sells must reduce the position.

The sample is a synthetic export modelled on a real Flex query with Execution
and Closed Lots enabled in the Trades section. Stripping the <Lot> rows gives
the Execution-only export the importer also has to handle.
"""

import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from beancount import loader
from beancount.core import amount, data
from beancount.parser import printer

from beancount_tools_collection.importers.ibkr import IBKRImporter

SAMPLE_XML = (Path(__file__).parent / 'data' / 'ibkr_trades_sample.xml').read_text()

ACCOUNT = 'Assets:Invest:IB:Main'
CASH = 'Assets:Cash:IB:Main:USD'

ACME_SELL = 'SELL -8 ACME @ 212.35 USD'
WIDG_SELL = 'SELL -40 WIDG @ 42.10 USD'
WIDG_FRACTION_SELL = 'SELL -0.3 WIDG @ 42.10 USD'


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
def extract(importer, tmp_path, monkeypatch):
    cfg = tmp_path / 'ibkr.yaml'
    cfg.write_text(yaml.dump({'token': 'SECRETTOKEN123', 'queryId': 999999}))

    def run(xml):
        monkeypatch.setattr(
            'beancount_tools_collection.importers.ibkr.client.download',
            lambda token, query_id: xml.encode(),
        )
        return importer.extract(str(cfg))

    return run


def without_closed_lots(xml):
    return re.sub(r'<Lot [^>]*/>\n', '', xml)


def txn(entries, narration):
    matches = [e for e in entries if isinstance(e, data.Transaction) and e.narration == narration]
    assert len(matches) == 1, f'expected one {narration!r}, got {len(matches)}'
    return matches[0]


def stock_postings(transaction, symbol):
    return [p for p in transaction.postings if p.account == f'{ACCOUNT}:{symbol}']


def opening_lots(xml):
    """Ledger lots for every closed lot that was not bought inside the statement.

    Booked one dollar under IBKR's lot price: IBKR folds the buy commission into
    a closed lot's price, the importer books buys at the bare execution price.
    """
    lines = []
    for lot in re.findall(r'<Lot [^>]*/>', xml):
        attrs = dict(re.findall(r'(\w+)="([^"]*)"', lot))
        if attrs['openDateTime'].startswith('20250303'):
            continue  # the WIDG dividend reinvestment bought inside this statement
        day = datetime.strptime(attrs['openDateTime'][:8], '%Y%m%d').date()
        price = round(Decimal(attrs['tradePrice']) - 1, 4)
        lines.append(
            f'{day} * "opening lot"\n'
            f'  {ACCOUNT}:{attrs["symbol"]}  {attrs["quantity"]} {attrs["symbol"]} '
            f'{{{price} USD, {day}}}\n'
            f'  Equity:Opening\n'
        )
    return '\n'.join(lines)


def load(entries, opening, booking):
    text = '\n'.join(
        [
            'plugin "beancount.plugins.auto_accounts"',
            f'option "booking_method" "{booking}"',
            opening,
            *(printer.format_entry(e) for e in entries),
        ]
    )
    ledger, errors, _ = loader.load_string(text)
    return ledger, [e.message for e in errors]


def units_held(ledger, symbol):
    account = f'{ACCOUNT}:{symbol}'
    return sum(
        (
            p.units.number
            for e in ledger
            if isinstance(e, data.Transaction)
            for p in e.postings
            if p.account == account
        ),
        Decimal(0),
    )


def pnl(ledger, narration):
    (posting,) = [p for p in txn(ledger, narration).postings if p.account == 'Income:PnL:IB']
    return posting.units


# ---------------------------------------------------------------------------
# Execution-only export: the ledger's booking method picks the lots
# ---------------------------------------------------------------------------


def test_sell_without_closed_lots_reduces_position(extract):
    sell = txn(extract(without_closed_lots(SAMPLE_XML)), ACME_SELL)

    (posting,) = stock_postings(sell, 'ACME')
    assert posting.units == amount.Amount(Decimal('-8'), 'ACME')
    assert posting.cost.number_per is None
    assert posting.cost.date is None
    assert posting.price == amount.Amount(Decimal('212.35'), 'USD')
    assert sell.meta['lot_source'] == 'ledger_booking'
    assert sell.meta['ibkr_cost'] == amount.Amount(Decimal('1203.44'), 'USD')
    assert sell.flag == '*'


def test_sell_without_closed_lots_books_under_fifo(extract):
    entries = extract(without_closed_lots(SAMPLE_XML))

    ledger, errors = load(entries, opening_lots(SAMPLE_XML), 'FIFO')

    assert errors == []
    assert units_held(ledger, 'ACME') == 0
    assert units_held(ledger, 'WIDG') == 0
    assert pnl(ledger, ACME_SELL) == amount.Amount(Decimal('-503.36'), 'USD')


def test_sell_without_closed_lots_is_ambiguous_under_strict(extract):
    entries = extract(without_closed_lots(SAMPLE_XML))

    _, errors = load(entries, opening_lots(SAMPLE_XML), 'STRICT')

    assert any('Ambiguous matches' in e and 'WIDG' in e for e in errors)


# ---------------------------------------------------------------------------
# Closed lots present: IBKR picks the lots, matched by open date only
# ---------------------------------------------------------------------------


def test_closed_lots_become_date_only_postings(extract):
    entries = extract(SAMPLE_XML)

    acme = txn(entries, ACME_SELL)
    (posting,) = stock_postings(acme, 'ACME')
    assert posting.units == amount.Amount(Decimal('-8'), 'ACME')
    assert posting.cost.date == date(2022, 9, 14)
    assert posting.cost.number_per is None
    assert posting.price == amount.Amount(Decimal('212.35'), 'USD')
    assert acme.meta['lot_source'] == 'ibkr_closed_lots'
    assert acme.flag == '*'


def test_closed_lots_sharing_an_open_date_merge_into_one_posting(extract):
    widg = txn(extract(SAMPLE_XML), WIDG_SELL)

    postings = stock_postings(widg, 'WIDG')
    by_date = {p.cost.date: p.units.number for p in postings}
    assert len(postings) == 7  # 8 IBKR lots, two opened on 2021-11-04
    assert by_date[date(2021, 11, 4)] == Decimal('-6')
    assert sum(by_date.values()) == Decimal('-40')


def test_closed_lots_book_under_strict_despite_commission_inclusive_lot_price(extract):
    entries = extract(SAMPLE_XML)

    ledger, errors = load(entries, opening_lots(SAMPLE_XML), 'STRICT')

    assert errors == []
    assert units_held(ledger, 'ACME') == 0
    assert units_held(ledger, 'WIDG') == 0
    assert units_held(ledger, 'WRLD') == 45
    assert pnl(ledger, ACME_SELL) == amount.Amount(Decimal('-503.36'), 'USD')


def test_closed_lots_stay_with_their_own_execution(extract):
    xml = re.sub(r'<Lot [^>]*dateTime="20250307;093001"[^>]*/>\n', '', SAMPLE_XML)
    entries = extract(xml)

    (big,) = stock_postings(txn(entries, WIDG_SELL), 'WIDG')
    assert big.units == amount.Amount(Decimal('-40'), 'WIDG')
    assert big.cost.date is None

    (fraction,) = stock_postings(txn(entries, WIDG_FRACTION_SELL), 'WIDG')
    assert fraction.units == amount.Amount(Decimal('-0.3'), 'WIDG')
    assert fraction.cost.date == date(2025, 3, 3)


def test_closed_lots_not_covering_the_sell_fall_back_and_flag_for_review(extract):
    xml = SAMPLE_XML.replace(
        'quantity="8" proceeds="" ibCommission=""', 'quantity="7" proceeds="" ibCommission=""', 1
    )
    sell = txn(extract(xml), ACME_SELL)

    (posting,) = stock_postings(sell, 'ACME')
    assert posting.units == amount.Amount(Decimal('-8'), 'ACME')
    assert posting.cost.date is None
    assert sell.meta['lot_source'] == 'ledger_booking'
    assert sell.flag == '!'


def test_closed_lot_without_open_date_falls_back_and_flags_for_review(extract):
    xml = SAMPLE_XML.replace('openDateTime="20220914"', 'openDateTime=""')
    sell = txn(extract(xml), ACME_SELL)

    (posting,) = stock_postings(sell, 'ACME')
    assert posting.cost.date is None
    assert sell.flag == '!'


# ---------------------------------------------------------------------------
# Shape of the sell and buy transactions
# ---------------------------------------------------------------------------


def test_sell_keeps_payee_date_narration_and_cash_legs(extract):
    sell = txn(extract(SAMPLE_XML), ACME_SELL)

    assert sell.payee == 'ACME'
    assert sell.date == date(2025, 3, 7)
    cash = [p.units for p in sell.postings if p.account == CASH]
    assert cash == [
        amount.Amount(Decimal('1698.80'), 'USD'),
        amount.Amount(Decimal('-0.35'), 'USD'),
    ]
    fees = [p.units for p in sell.postings if p.account == 'Expenses:Fees:IB:USD']
    assert fees == [amount.Amount(Decimal('0.35'), 'USD')]


def test_fractional_sell_keeps_exact_units_and_drops_zero_commission(extract):
    sell = txn(extract(SAMPLE_XML), WIDG_FRACTION_SELL)

    (posting,) = stock_postings(sell, 'WIDG')
    assert posting.units == amount.Amount(Decimal('-0.3'), 'WIDG')
    accounts = [p.account for p in sell.postings]
    assert 'Expenses:Fees:IB:USD' not in accounts
    assert accounts.count(CASH) == 1


def test_trades_import_without_description_column(extract):
    entries = extract(re.sub(r' description="[^"]*"', '', SAMPLE_XML))

    assert stock_postings(txn(entries, ACME_SELL), 'ACME')
    assert len([e for e in entries if isinstance(e, data.Transaction) and e.payee == 'WRLD']) == 3


def test_cancelled_sell_is_flagged_without_stock_leg(extract):
    xml = SAMPLE_XML.replace(
        'quantity="-8" proceeds="1698.8" ibCommission="-0.35" ibCommissionCurrency="USD" '
        'cost="-1203.44" origTradePrice="0" buySell="SELL"',
        'quantity="8" proceeds="-1698.8" ibCommission="0" ibCommissionCurrency="USD" '
        'cost="1203.44" origTradePrice="0" buySell="SELL (Ca.)"',
    )
    xml = re.sub(r'<Lot [^>]*symbol="ACME"[^>]*/>\n', '', xml)
    sell = txn(extract(xml), 'SELL 8 ACME @ 212.35 USD')

    assert stock_postings(sell, 'ACME') == []
    assert sell.meta['lot_source'] == 'manual'
    assert sell.flag == '!'


def test_buys_open_lots_at_execution_price(extract):
    entries = extract(SAMPLE_XML)

    wrld = [e for e in entries if isinstance(e, data.Transaction) and e.payee == 'WRLD']
    for buy in wrld:
        (posting,) = stock_postings(buy, 'WRLD')
        assert posting.cost.number_per == Decimal('98.40')
        assert posting.cost.date == date(2025, 3, 6)
    assert sorted(stock_postings(b, 'WRLD')[0].units.number for b in wrld) == [5, 20, 20]

    drip = txn(entries, 'BUY 0.8 WIDG @ 40.25 USD')
    assert drip.tags == frozenset({'drip'})
