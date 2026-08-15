"""Unit tests for the Viseca One CSV bill importer.

No network and no credentials required. The committed fixture is synthetic: a
real export carries a live CardId and personal merchant history, and this repo
is public.

One case from the test matrix is not automated here: confirming that a
file-level failure surfaces in Fava as an error rather than the yellow "No
entries to import from this file" notice. That is a manual QA step, exercised by
the VisecaCsvError type which Fava turns into an ImporterExtractError.
"""

import os
from decimal import Decimal
from pathlib import Path

import pytest
from loguru import logger

from beancount_tools_collection.importers.viseca_csv import (
    HEADER_COLUMNS,
    VisecaCsvError,
    VisecaCsvImporter,
)

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

SAMPLE = Path(__file__).parent / "data" / "viseca_bill_sample.csv"

HEADER_LINE = ",".join(HEADER_COLUMNS)

LIABILITY = "Liabilities:CreditCard:Viseca"
SETTLEMENT = "Assets:Bank:Checking"

DEFAULT_ROW = {
    "TransactionId": "TRX0000000000000000900",
    "CardId": "CARD000000000001",
    "Date": "2026-08-05 12:00:00",
    "ValutaDate": "2026-08-06 00:00:00",
    "Amount": "10.000",
    "Currency": "CHF",
    "OriginalAmount": "10.000",
    "OriginalCurrency": "CHF",
    "MerchantName": "Sample Merchant",
    "MerchantPlace": "Bern",
    "MerchantCountry": "CHE",
    "StateType": "BOOKED",
    "Details": "Sample Detail",
    "Type": "merchant",
    "Exchange Rate": "1.000000",
}


def make_csv(tmp_path, *rows, header=HEADER_LINE, name="bill.csv", bom=False):
    """Write a CSV built from DEFAULT_ROW plus per-row overrides."""
    lines = [] if header is None else [header]
    for row in rows:
        merged = {**DEFAULT_ROW, **row}
        lines.append(",".join(merged[column] for column in HEADER_COLUMNS))
    text = "\n".join(lines)
    if lines:
        text += "\n"
    path = tmp_path / name
    path.write_text(("\ufeff" if bom else "") + text, encoding="utf-8")
    return str(path)


@pytest.fixture
def importer():
    return VisecaCsvImporter(account=LIABILITY)


@pytest.fixture
def settling_importer():
    return VisecaCsvImporter(account=LIABILITY, settlement_account=SETTLEMENT)


@pytest.fixture
def log_messages():
    """Capture loguru output, which does not flow through pytest's caplog."""
    messages = []
    sink_id = logger.add(messages.append, level="DEBUG", format="{message}")
    yield messages
    logger.remove(sink_id)


def only(entries, payee):
    matches = [entry for entry in entries if entry.payee == payee]
    assert len(matches) == 1, f"expected exactly one {payee!r}, got {len(matches)}"
    return matches[0]


def accounts(entry):
    return [posting.account for posting in entry.postings]


def numbers(entry):
    return [posting.units.number for posting in entry.postings]


# ---------------------------------------------------------------------------
# identify()
# ---------------------------------------------------------------------------


def test_identify_accepts_the_sample_bill(importer):
    assert importer.identify(str(SAMPLE)) is True


def test_identify_rejects_a_different_csv(tmp_path):
    path = tmp_path / "yuh_2026.csv"
    path.write_text(
        "DATE;ACTIVITY TYPE;ACTIVITY NAME;DEBIT;CREDIT\n01/08/2026;X;Y;1;0\n",
        encoding="utf-8",
    )
    assert VisecaCsvImporter().identify(str(path)) is False


def test_identify_rejects_a_reordered_header(tmp_path):
    reordered = ",".join(["CardId", "TransactionId", *HEADER_COLUMNS[2:]])
    path = make_csv(tmp_path, header=reordered)
    assert VisecaCsvImporter().identify(path) is False


def test_identify_rejects_a_missing_file(tmp_path):
    assert VisecaCsvImporter().identify(str(tmp_path / "absent.csv")) is False


@pytest.mark.parametrize(
    ("name", "regex", "expected"),
    [
        ("Bill - August 2026.csv", r"^bill", True),
        ("Bill - August 2026.csv", r"^statement", False),
    ],
)
def test_identify_respects_filename_regex(tmp_path, name, regex, expected):
    path = make_csv(tmp_path, {}, name=name)
    importer = VisecaCsvImporter(filename_regex=regex)
    assert importer.identify(path) is expected


# ---------------------------------------------------------------------------
# Expense rows
# ---------------------------------------------------------------------------


def test_expense_is_single_legged(importer, tmp_path):
    entries = importer.extract(make_csv(tmp_path, {}))
    assert len(entries) == 1
    # smart_importer only fills postings that are left incomplete.
    assert len(entries[0].postings) == 1
    assert entries[0].postings[0].units is not None


def test_expense_reduces_the_liability(importer, tmp_path):
    entries = importer.extract(make_csv(tmp_path, {"Amount": "42.500"}))
    posting = entries[0].postings[0]
    assert posting.account == LIABILITY
    assert posting.units.number == Decimal("-42.50")
    assert posting.units.currency == "CHF"


def test_expense_payee_and_narration(importer, tmp_path):
    entries = importer.extract(
        make_csv(tmp_path, {"MerchantName": "Sample Grocer", "Details": "Store 1345"})
    )
    assert entries[0].payee == "Sample Grocer"
    assert entries[0].narration == "Store 1345"


def test_transaction_date_comes_from_the_date_column(importer, tmp_path):
    path = make_csv(
        tmp_path,
        {"Date": "2026-08-02 18:59:00", "ValutaDate": "2026-08-06 00:00:00"},
    )
    entries = importer.extract(path)
    assert str(entries[0].date) == "2026-08-02"


def test_valuta_date_is_kept_in_metadata(importer, tmp_path):
    path = make_csv(
        tmp_path,
        {"Date": "2026-08-02 18:59:00", "ValutaDate": "2026-08-06 00:00:00"},
    )
    entries = importer.extract(path)
    assert str(entries[0].meta["valutaDate"]) == "2026-08-06"


def test_transaction_id_is_kept_in_metadata(importer, tmp_path):
    path = make_csv(tmp_path, {"TransactionId": "TRX123"})
    entries = importer.extract(path)
    assert entries[0].meta["transactionId"] == "TRX123"


# ---------------------------------------------------------------------------
# merchant_map
# ---------------------------------------------------------------------------


def test_merchant_map_adds_a_balanced_expense_leg(tmp_path):
    importer = VisecaCsvImporter(
        account=LIABILITY, merchant_map={"Sample Grocer": "Expenses:Groceries"}
    )
    path = make_csv(tmp_path, {"MerchantName": "Sample Grocer", "Amount": "12.000"})
    entry = importer.extract(path)[0]
    assert accounts(entry) == [LIABILITY, "Expenses:Groceries"]
    assert sum(numbers(entry)) == 0


def test_unmapped_merchant_stays_single_legged(tmp_path):
    importer = VisecaCsvImporter(
        account=LIABILITY, merchant_map={"Somebody Else": "Expenses:Groceries"}
    )
    path = make_csv(tmp_path, {"MerchantName": "Sample Grocer"})
    assert len(importer.extract(path)[0].postings) == 1


# ---------------------------------------------------------------------------
# Payments and refunds
# ---------------------------------------------------------------------------


PAYMENT_ROW = {
    "CardId": "",
    "MerchantName": "",
    "MerchantPlace": "",
    "MerchantCountry": "",
    "Amount": "-500.000",
    "OriginalAmount": "-500.000",
    "Details": "Ihre Zahlung - Danke",
}

REFUND_ROW = {
    "Amount": "-45.000",
    "OriginalAmount": "-45.000",
    "MerchantName": "Sample Grocer",
    "Details": "Sample Grocer Refund",
}


def test_payment_becomes_a_balanced_transfer(settling_importer, tmp_path):
    entry = settling_importer.extract(make_csv(tmp_path, PAYMENT_ROW))[0]
    assert accounts(entry) == [LIABILITY, SETTLEMENT]
    assert numbers(entry) == [Decimal("500.00"), Decimal("-500.00")]
    assert sum(numbers(entry)) == 0
    # A payment repays debt, so the liability leg is positive.
    assert entry.postings[0].units.number > 0


def test_payment_has_no_payee_and_keeps_details(settling_importer, tmp_path):
    entry = settling_importer.extract(make_csv(tmp_path, PAYMENT_ROW))[0]
    assert entry.payee is None
    assert entry.narration == "Ihre Zahlung - Danke"


def test_payment_is_single_legged_without_a_settlement_account(importer, tmp_path):
    entry = importer.extract(make_csv(tmp_path, PAYMENT_ROW))[0]
    assert len(entry.postings) == 1
    assert entry.postings[0].units.number == Decimal("500.00")


def test_refund_is_positive_single_legged_and_flagged(importer, tmp_path):
    entry = importer.extract(make_csv(tmp_path, REFUND_ROW))[0]
    assert len(entry.postings) == 1
    assert entry.postings[0].units.number == Decimal("45.00")
    assert entry.flag == "!"


def test_refund_flag_can_be_disabled(tmp_path):
    importer = VisecaCsvImporter(account=LIABILITY, flag_unverified=False)
    entry = importer.extract(make_csv(tmp_path, REFUND_ROW))[0]
    assert entry.flag == "*"


def test_negative_row_naming_a_merchant_without_a_card_is_a_refund(
    importer, tmp_path, log_messages
):
    path = make_csv(tmp_path, {**REFUND_ROW, "CardId": ""})
    entry = importer.extract(path)[0]
    # Not a bill payment: it names a merchant, so it stays single-legged.
    assert len(entry.postings) == 1
    assert entry.payee == "Sample Grocer"
    assert any("treating it as a refund" in message for message in log_messages)


# ---------------------------------------------------------------------------
# Skipped rows
# ---------------------------------------------------------------------------


def test_pending_row_is_skipped_quietly(importer, tmp_path, log_messages):
    entries = importer.extract(make_csv(tmp_path, {"StateType": "PENDING"}))
    assert entries == []
    assert not any("unrecognised" in message for message in log_messages)


def test_unknown_state_is_skipped_and_warned(importer, tmp_path, log_messages):
    entries = importer.extract(make_csv(tmp_path, {"StateType": "REVERSED"}))
    assert entries == []
    assert any("unrecognised StateType" in message for message in log_messages)


def test_zero_amount_row_is_skipped(importer, tmp_path):
    assert importer.extract(make_csv(tmp_path, {"Amount": "0.000"})) == []


def test_malformed_row_is_skipped_and_others_still_import(importer, tmp_path):
    path = make_csv(
        tmp_path,
        {"Date": "not-a-date", "TransactionId": "TRX_BAD"},
        {"TransactionId": "TRX_GOOD", "MerchantName": "Sample Good"},
    )
    entries = importer.extract(path)
    assert [entry.meta["transactionId"] for entry in entries] == ["TRX_GOOD"]


def test_skipped_rows_are_reported_with_a_count(importer, tmp_path, log_messages):
    path = make_csv(
        tmp_path,
        {"StateType": "PENDING"},
        {"StateType": "REVERSED"},
        {"Amount": "0.000"},
        {"MerchantName": "Sample Good"},
    )
    entries = importer.extract(path)
    assert len(entries) == 1
    assert any("skipped 3 of 4 rows" in message for message in log_messages)


# ---------------------------------------------------------------------------
# Currency and precision
# ---------------------------------------------------------------------------


def test_foreign_currency_posts_the_settled_amount(importer, tmp_path):
    path = make_csv(
        tmp_path,
        {
            "Amount": "10.750",
            "Currency": "CHF",
            "OriginalAmount": "11.000",
            "OriginalCurrency": "EUR",
            "Exchange Rate": "0.977273",
        },
    )
    entry = importer.extract(path)[0]
    assert entry.postings[0].units == entry.postings[0].units._replace(
        number=Decimal("-10.75"), currency="CHF"
    )
    assert entry.meta["originalAmount"] == Decimal("11.000")
    assert entry.meta["originalCurrency"] == "EUR"
    assert entry.meta["exchangeRate"] == Decimal("0.977273")
    assert entry.flag == "!"


def test_same_currency_amount_mismatch_is_recorded_but_not_flagged(importer, tmp_path):
    # Observed in the wild: Amount and OriginalAmount differ at rate 1.0.
    path = make_csv(
        tmp_path,
        {"Amount": "355.150", "OriginalAmount": "355.130", "OriginalCurrency": "CHF"},
    )
    entry = importer.extract(path)[0]
    assert entry.postings[0].units.number == Decimal("-355.15")
    assert entry.meta["originalAmount"] == Decimal("355.130")
    assert entry.flag == "*"


def test_matching_original_amount_is_not_recorded(importer, tmp_path):
    entry = importer.extract(make_csv(tmp_path, {}))[0]
    assert "originalAmount" not in entry.meta
    assert "exchangeRate" not in entry.meta


def test_blank_optional_numerics_do_not_raise(importer, tmp_path):
    path = make_csv(
        tmp_path, {"OriginalAmount": "", "OriginalCurrency": "", "Exchange Rate": ""}
    )
    entry = importer.extract(path)[0]
    assert entry.postings[0].units.number == Decimal("-10.00")
    assert "originalAmount" not in entry.meta
    assert "exchangeRate" not in entry.meta


def test_trailing_zero_third_decimal_is_narrowed_to_cents(importer, tmp_path):
    entry = importer.extract(make_csv(tmp_path, {"Amount": "3.300"}))[0]
    number = entry.postings[0].units.number
    assert number == Decimal("-3.30")
    assert number.as_tuple().exponent == -2


def test_non_zero_third_decimal_is_preserved_and_warned(
    importer, tmp_path, log_messages
):
    entry = importer.extract(make_csv(tmp_path, {"Amount": "5.005"}))[0]
    number = entry.postings[0].units.number
    assert number == Decimal("-5.005")
    assert number.as_tuple().exponent == -3
    assert any("does not fit two decimals" in message for message in log_messages)


# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------


def test_byte_order_mark_and_non_ascii_are_handled(importer, tmp_path):
    path = make_csv(tmp_path, {"MerchantPlace": "Zürich"}, bom=True)
    assert Path(path).read_bytes().startswith(b"\xef\xbb\xbf")
    assert importer.identify(path) is True
    entry = importer.extract(path)[0]
    # The BOM must not end up glued to the first column name.
    assert entry.meta["transactionId"] == DEFAULT_ROW["TransactionId"]
    assert entry.meta["merchantPlace"] == "Zürich"


# ---------------------------------------------------------------------------
# Multiple cards
# ---------------------------------------------------------------------------


def test_multiple_cards_without_a_mapping_raises(importer, tmp_path):
    path = make_csv(tmp_path, {"CardId": "CARD_A"}, {"CardId": "CARD_B"})
    with pytest.raises(VisecaCsvError, match="card_accounts"):
        importer.extract(path)


def test_multiple_cards_with_a_mapping_route_per_card(tmp_path):
    importer = VisecaCsvImporter(
        account=LIABILITY,
        card_accounts={"CARD_A": "Liabilities:Card:A", "CARD_B": "Liabilities:Card:B"},
    )
    path = make_csv(
        tmp_path,
        {"CardId": "CARD_A", "MerchantName": "Sample A"},
        {"CardId": "CARD_B", "MerchantName": "Sample B"},
    )
    entries = importer.extract(path)
    assert accounts(only(entries, "Sample A")) == ["Liabilities:Card:A"]
    assert accounts(only(entries, "Sample B")) == ["Liabilities:Card:B"]


def test_unknown_card_raises(tmp_path):
    importer = VisecaCsvImporter(
        account=LIABILITY, card_accounts={"CARD_A": "Liabilities:Card:A"}
    )
    path = make_csv(tmp_path, {"CardId": "CARD_UNKNOWN"})
    with pytest.raises(VisecaCsvError, match="CARD_UNKNOWN"):
        importer.extract(path)


def test_payment_row_uses_the_main_account_when_cards_are_mapped(tmp_path):
    importer = VisecaCsvImporter(
        account=LIABILITY,
        settlement_account=SETTLEMENT,
        card_accounts={"CARD_A": "Liabilities:Card:A"},
    )
    entry = importer.extract(make_csv(tmp_path, PAYMENT_ROW))[0]
    assert accounts(entry) == [LIABILITY, SETTLEMENT]


# ---------------------------------------------------------------------------
# File-level failures
# ---------------------------------------------------------------------------


def test_header_only_file_returns_empty_without_raising(importer, tmp_path):
    assert importer.extract(make_csv(tmp_path)) == []


def test_completely_empty_file_raises(importer, tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text("", encoding="utf-8")
    with pytest.raises(VisecaCsvError, match="no header row"):
        importer.extract(str(path))


def test_file_without_a_header_raises_rather_than_returning_empty(importer, tmp_path):
    path = make_csv(tmp_path, {}, header=None)
    with pytest.raises(VisecaCsvError, match="unexpected header"):
        importer.extract(path)


@pytest.mark.skipif(
    hasattr(os, "getuid") and os.getuid() == 0, reason="root bypasses file permissions"
)
def test_unreadable_file_raises(importer, tmp_path):
    path = make_csv(tmp_path, {})
    os.chmod(path, 0o000)
    try:
        with pytest.raises(VisecaCsvError, match="cannot be read"):
            importer.extract(path)
    finally:
        os.chmod(path, 0o644)


# ---------------------------------------------------------------------------
# Archival helpers
# ---------------------------------------------------------------------------


def test_date_is_the_latest_transaction_date(importer):
    assert str(importer.date(str(SAMPLE))) == "2026-08-03"


def test_filename_is_derived_from_the_billing_month(importer):
    assert importer.filename(str(SAMPLE)) == "viseca_2026-08.csv"


def test_account_is_the_configured_liability(importer):
    assert importer.account(str(SAMPLE)) == LIABILITY


# ---------------------------------------------------------------------------
# Subclassing
# ---------------------------------------------------------------------------


def test_subclass_inherits_column_declarations(tmp_path):
    """csvbase's metaclass rebuilds `columns` per class body.

    Without the merging metaclass a subclass silently parses nothing and the
    import comes back empty instead of failing.
    """

    class Customised(VisecaCsvImporter):
        pass

    entries = Customised(account=LIABILITY).extract(make_csv(tmp_path, {}))
    assert len(entries) == 1
    assert entries[0].postings[0].units.number == Decimal("-10.00")


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


def build_entry(importer, tmp_path, name, **overrides):
    return importer.extract(make_csv(tmp_path, overrides, name=name))[0]


def test_cmp_matches_on_transaction_id(importer, tmp_path):
    first = build_entry(importer, tmp_path, "a.csv", TransactionId="TRX_SAME")
    second = build_entry(
        importer,
        tmp_path,
        "b.csv",
        TransactionId="TRX_SAME",
        Date="2026-09-09 08:00:00",
        Amount="99.000",
    )
    assert VisecaCsvImporter.cmp(first, second) is True


def test_cmp_rejects_different_transaction_ids(importer, tmp_path):
    first = build_entry(importer, tmp_path, "a.csv", TransactionId="TRX_ONE")
    second = build_entry(importer, tmp_path, "b.csv", TransactionId="TRX_TWO")
    assert VisecaCsvImporter.cmp(first, second) is False


def test_cmp_falls_back_to_the_heuristic_without_ids(importer, tmp_path):
    first = build_entry(importer, tmp_path, "a.csv", TransactionId="")
    second = build_entry(importer, tmp_path, "b.csv", TransactionId="")
    assert "transactionId" not in first.meta
    assert VisecaCsvImporter.cmp(first, second) is True


def test_deduplicate_marks_matches_beyond_the_two_day_window(importer, tmp_path):
    existing = build_entry(
        importer,
        tmp_path,
        "old.csv",
        TransactionId="TRX_SAME",
        Date="2026-07-01 09:00:00",
    )
    new = build_entry(
        importer,
        tmp_path,
        "new.csv",
        TransactionId="TRX_SAME",
        Date="2026-08-01 09:00:00",
    )
    assert (new.date - existing.date).days > 2

    importer.deduplicate([new], [existing])
    assert new.meta.get("__duplicate__") is existing


def test_deduplicate_leaves_distinct_transactions_alone(importer, tmp_path):
    existing = build_entry(
        importer,
        tmp_path,
        "old.csv",
        TransactionId="TRX_ONE",
        Date="2026-07-01 09:00:00",
    )
    new = build_entry(
        importer,
        tmp_path,
        "new.csv",
        TransactionId="TRX_TWO",
        Date="2026-08-01 09:00:00",
    )
    importer.deduplicate([new], [existing])
    assert "__duplicate__" not in new.meta


# ---------------------------------------------------------------------------
# Whole-file behaviour
# ---------------------------------------------------------------------------


def test_sample_bill_extracts_the_expected_ledger():
    importer = VisecaCsvImporter(
        account=LIABILITY,
        settlement_account=SETTLEMENT,
        merchant_map={"Sample Grocer": "Expenses:Groceries"},
    )
    entries = importer.extract(str(SAMPLE))

    snapshot = [
        (
            str(entry.date),
            entry.flag,
            entry.payee,
            [(posting.account, str(posting.units)) for posting in entry.postings],
        )
        for entry in entries
    ]

    assert snapshot == [
        (
            "2026-07-16",
            "*",
            "Sample Grocer",
            [
                (LIABILITY, "-3.30 CHF"),
                ("Expenses:Groceries", "3.30 CHF"),
            ],
        ),
        ("2026-07-18", "*", "Sample Market", [(LIABILITY, "-24.55 CHF")]),
        ("2026-07-20", "*", "Sample Outfitter", [(LIABILITY, "-355.15 CHF")]),
        ("2026-07-22", "!", "Sample Bookshop", [(LIABILITY, "-10.75 CHF")]),
        ("2026-07-24", "*", "Sample Kiosk", [(LIABILITY, "-12.00 CHF")]),
        (
            "2026-07-26",
            "!",
            "Sample Grocer",
            [(LIABILITY, "45.00 CHF")],
        ),
        ("2026-08-02", "*", "Sample Odd Precision", [(LIABILITY, "-5.005 CHF")]),
        (
            "2026-08-03",
            "*",
            None,
            [
                (LIABILITY, "500.00 CHF"),
                (SETTLEMENT, "-500.00 CHF"),
            ],
        ),
    ]


def test_sample_bill_liability_nets_to_the_expected_balance():
    importer = VisecaCsvImporter(account=LIABILITY, settlement_account=SETTLEMENT)
    entries = importer.extract(str(SAMPLE))
    liability_total = sum(
        posting.units.number
        for entry in entries
        for posting in entry.postings
        if posting.account == LIABILITY
    )
    # 8 booked rows: charges 3.30 + 24.55 + 355.15 + 10.75 + 12.00 + 5.005,
    # less a 45.00 refund and a 500.00 payment.
    assert liability_total == Decimal("500.00") + Decimal("45.00") - Decimal("410.755")
