"""Beancount importer for Viseca One CSV bill exports.

The Viseca One app exports a monthly bill as CSV. Unlike the JSON transaction
export handled by :mod:`viseca`, the CSV carries no PFM category, so this
importer emits single-legged postings and leaves the expense account to a
categorization layer: an explicit ``merchant_map``, ``smart_importer``, or
manual entry in Fava. ``smart_importer`` only fills postings that are left
incomplete, so writing a placeholder expense account here would disable it.

The monthly "Ihre Zahlung - Danke" row settles the *previous* bill, not the one
it appears on. It is emitted as a transfer against ``settlement_account`` so the
credit-card liability returns to zero each cycle; discarding it, as the JSON
importer does, lets the liability grow without bound and hides dropped
transactions.

``Amount``/``Currency`` is always the settled amount in the card's currency, so
foreign-currency rows need no conversion: the original amount and rate are
recorded as metadata only. ``OriginalAmount`` is never authoritative -- it can
differ from ``Amount`` even at exchange rate 1.0 in the same currency.
"""

from __future__ import annotations

import collections
import enum
import os
import re
from collections.abc import Mapping
from decimal import Decimal

import beangulp
from beancount.core import amount, data
from beangulp import utils
from beangulp.exceptions import Error
from beangulp.extract import DUPLICATE
from beangulp.importers import csvbase
from beangulp.similar import heuristic_comparator
from loguru import logger

__all__ = [
    "HEADER_COLUMNS",
    "KNOWN_PENDING_STATES",
    "OptionalAmount",
    "RowKind",
    "SkipReason",
    "VisecaCsvError",
    "VisecaCsvImporter",
]


class VisecaCsvError(Error):
    """A file-level failure that must not be reported as an empty statement.

    Subclassing beangulp's Error makes the CLI print the message without a
    traceback and exit non-zero, and makes Fava raise instead of showing the
    misleading "No entries to import from this file" notice. An empty list is
    reserved for a bill that genuinely contains no transactions.
    """


HEADER_COLUMNS = (
    "TransactionId",
    "CardId",
    "Date",
    "ValutaDate",
    "Amount",
    "Currency",
    "OriginalAmount",
    "OriginalCurrency",
    "MerchantName",
    "MerchantPlace",
    "MerchantCountry",
    "StateType",
    "Details",
    "Type",
    "Exchange Rate",
)

HEADER_RE = r"^\s*" + ",".join(re.escape(column) for column in HEADER_COLUMNS)

# Enough to cover the header line without reading the whole bill.
HEADER_PROBE_CHARS = 512

BOOKED_STATE = "booked"

# States that are known and intentionally not imported. Anything outside this
# set is treated as unrecognised and reported rather than dropped in silence.
KNOWN_PENDING_STATES = frozenset({"pending", "authorized", "authorised", "reserved"})

DEFAULT_CURRENCY = "CHF"

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

CENTS = Decimal("0.01")
ONE = Decimal(1)

META_TRANSACTION_ID = "transactionId"
META_VALUTA_DATE = "valutaDate"
META_CARD_ID = "cardId"
META_MERCHANT_PLACE = "merchantPlace"
META_MERCHANT_COUNTRY = "merchantCountry"
META_ORIGINAL_AMOUNT = "originalAmount"
META_ORIGINAL_CURRENCY = "originalCurrency"
META_EXCHANGE_RATE = "exchangeRate"


class RowKind(enum.Enum):
    """What a booked row represents."""

    EXPENSE = "expense"
    REFUND = "refund"
    PAYMENT = "payment"


class SkipReason(enum.Enum):
    """Why a row produced no entry."""

    PENDING = "not yet booked"
    UNKNOWN_STATE = "unrecognised StateType"
    ZERO_AMOUNT = "zero amount"
    UNPARSEABLE = "unparseable"


class OptionalAmount(csvbase.Column):
    """Decimal column that yields None for a blank field.

    ``csvbase.Column`` only substitutes its ``default`` when the default is
    truthy, so ``default=None`` never fires and a blank field would reach
    ``Decimal('')`` and raise.
    """

    def parse(self, value: str) -> Decimal | None:
        value = value.strip()
        return Decimal(value) if value else None


def _narrow_to_cents(value: Decimal) -> tuple[Decimal, bool]:
    """Narrow a three-decimal amount to two decimals when that is lossless.

    Viseca reports amounts with three decimals whose last digit is always zero.
    Returns the value and whether the narrowing was exact.
    """
    narrowed = value.quantize(CENTS)
    if narrowed == value:
        return narrowed, True
    return value, False


def _text(value: str | None) -> str:
    return value.strip() if value else ""


class _InheritingCSVMeta(csvbase.CSVMeta):
    """CSV metaclass that lets subclasses inherit column declarations.

    ``csvbase.CSVMeta`` rebuilds ``columns`` from the class body alone, so a
    subclass that declares no columns of its own silently ends up with none at
    all: every row then fails to parse and the import comes back empty. Merging
    the bases' columns keeps subclassing a safe way to adjust behaviour.
    """

    def __new__(mcs, name, bases, dct):
        cls = super().__new__(mcs, name, bases, dct)
        merged = {}
        for base in reversed(cls.__mro__[1:]):
            merged.update(getattr(base, "columns", None) or {})
        merged.update(cls.columns)
        cls.columns = merged
        return cls


class VisecaCsvImporter(
    beangulp.Importer, csvbase.CSVReader, metaclass=_InheritingCSVMeta
):
    """Importer for Viseca One CSV bill exports.

    Args:
      account: The credit-card liability account.
      settlement_account: Account the monthly bill is paid from. When set,
        payment rows become balanced transfers; when unset they stay
        single-legged.
      merchant_map: Optional exact ``MerchantName`` to expense-account mapping.
        Merchants that are not mapped stay single-legged.
      card_accounts: Optional ``CardId`` to liability-account mapping, required
        when one file covers more than one card.
      filename_regex: Optional additional constraint on the file name, useful
        when several Viseca accounts are imported separately.
      flag_unverified: Flag rows whose semantics are not yet confirmed against
        real data (refunds and foreign-currency rows) with ``!`` so they surface
        for review.
    """

    encoding = "utf-8-sig"  # the export carries a UTF-8 BOM
    comments = None  # a Details field must never be read as a comment
    dialect = "excel"

    # Column names must not collide with Importer method names: CSVMeta strips
    # Column attributes out of the class namespace, so a column named `date`
    # would shadow the date() method.
    transaction_id = csvbase.Column("TransactionId")
    card_id = csvbase.Column("CardId")
    txn_date = csvbase.Date("Date", TIMESTAMP_FORMAT)
    valuta_date = csvbase.Date("ValutaDate", TIMESTAMP_FORMAT)
    amount = csvbase.Amount("Amount")
    currency = csvbase.Column("Currency")
    original_amount = OptionalAmount("OriginalAmount")
    original_currency = csvbase.Column("OriginalCurrency")
    merchant = csvbase.Column("MerchantName")
    place = csvbase.Column("MerchantPlace")
    country = csvbase.Column("MerchantCountry")
    state = csvbase.Column("StateType")
    details = csvbase.Column("Details")
    kind = csvbase.Column("Type")
    exchange_rate = OptionalAmount("Exchange Rate")

    def __init__(
        self,
        account: str = "Liabilities:CreditCard:Viseca",
        settlement_account: str | None = None,
        merchant_map: Mapping[str, str] | None = None,
        card_accounts: Mapping[str, str] | None = None,
        filename_regex: str | None = None,
        flag_unverified: bool = True,
    ) -> None:
        self.main_account = account
        self.settlement_account = settlement_account
        self.merchant_map = dict(merchant_map or {})
        self.card_accounts = dict(card_accounts or {})
        self.filename_regex = filename_regex
        self.flag_unverified = flag_unverified

    # -- beangulp.Importer interface ---------------------------------------

    def identify(self, filepath: str) -> bool:
        if self.filename_regex and not re.search(
            self.filename_regex, os.path.basename(filepath), re.IGNORECASE
        ):
            return False
        try:
            return utils.search_file_regexp(
                filepath,
                HEADER_RE,
                nbytes=HEADER_PROBE_CHARS,
                encoding=self.encoding,
            )
        except OSError:
            return False

    def account(self, filepath: str) -> str:
        return self.main_account

    def date(self, filepath: str):
        dates = []
        for row in self._read_rows(filepath):
            try:
                dates.append(row.txn_date)
            except Exception:  # noqa: BLE001 - a bad row must not block archiving
                continue
        return max(dates) if dates else None

    def filename(self, filepath: str) -> str | None:
        billing_date = self.date(filepath)
        return f"viseca_{billing_date:%Y-%m}.csv" if billing_date else None

    def extract(self, filepath: str, existing: list | None = None) -> list:
        rows = self._read_rows(filepath)
        if not rows:
            logger.info(f"{filepath}: no data rows in bill")
            return []

        self._validate_cards(rows, filepath)

        entries = []
        skipped: collections.Counter = collections.Counter()

        # Line 1 is the header, so the first data row is line 2.
        for lineno, row in enumerate(rows, 2):
            try:
                outcome = self._classify(row, filepath, lineno)
                if isinstance(outcome, SkipReason):
                    self._note_skip(skipped, outcome, filepath, lineno)
                    continue
                entries.append(self._build_entry(filepath, lineno, row, outcome))
            except VisecaCsvError:
                raise
            except Exception as exc:  # noqa: BLE001 - contained per row by design
                self._note_skip(skipped, SkipReason.UNPARSEABLE, filepath, lineno, exc)

        self._report_skipped(filepath, len(rows), skipped)
        logger.info(f"Extracted {len(entries)} entries from {filepath}")
        return entries

    # -- Deduplication -----------------------------------------------------

    @staticmethod
    def cmp(a, b) -> bool:
        """Compare on TransactionId when both entries carry one.

        Viseca ids are stable, so an exact match beats the heuristic. Entries
        without an id (hand-written, or from another importer) fall back to
        beangulp's default comparator.
        """
        left = a.meta.get(META_TRANSACTION_ID) if a.meta else None
        right = b.meta.get(META_TRANSACTION_ID) if b.meta else None
        if left and right:
            return left == right
        return _HEURISTIC_CMP(a, b)

    def deduplicate(self, entries: list, existing: list) -> None:
        """Mark duplicates, matching on TransactionId across the whole ledger.

        beangulp's default only compares against existing entries within two
        days, which misses a transaction whose date moved between exports. An
        exact id match is authoritative at any distance, so it runs first; the
        windowed heuristic then handles whatever is left.
        """
        if existing:
            by_id = {}
            for entry in data.filter_txns(existing):
                transaction_id = (
                    entry.meta.get(META_TRANSACTION_ID) if entry.meta else None
                )
                if transaction_id:
                    by_id.setdefault(transaction_id, entry)

            if by_id:
                for entry in data.filter_txns(entries):
                    if entry.meta.get(DUPLICATE) is not None:
                        continue
                    transaction_id = entry.meta.get(META_TRANSACTION_ID)
                    match = by_id.get(transaction_id) if transaction_id else None
                    if match is not None:
                        entry.meta[DUPLICATE] = match

        super().deduplicate(entries, existing)

    # -- Reading -----------------------------------------------------------

    def _read_rows(self, filepath: str) -> list:
        """Materialise the data rows, converting file-level faults into errors.

        Column values are parsed lazily on attribute access, so this surfaces
        only structural problems: an unreadable file, a missing header, or a
        missing column.
        """
        try:
            return list(self.read(filepath))
        except OSError as exc:
            raise VisecaCsvError(
                f"{filepath}: cannot be read ({exc.strerror})"
            ) from None
        except UnicodeDecodeError:
            raise VisecaCsvError(
                f"{filepath}: is not valid {self.encoding} text"
            ) from None
        except IndexError:
            raise VisecaCsvError(f"{filepath}: contains no header row") from None
        except KeyError as exc:
            detail = exc.args[0] if exc.args else "unknown column"
            raise VisecaCsvError(f"{filepath}: unexpected header ({detail})") from None

    def _validate_cards(self, rows: list, filepath: str) -> None:
        cards = set()
        for row in rows:
            try:
                card = _text(row.card_id)
            except Exception:  # noqa: BLE001 - reported per row during extract
                continue
            if card:
                cards.add(card)

        if len(cards) > 1 and not self.card_accounts:
            raise VisecaCsvError(
                f"{filepath}: covers {len(cards)} cards but no card_accounts "
                f"mapping is configured; add one mapping each CardId to its "
                f"liability account"
            )

    # -- Row handling ------------------------------------------------------

    def _classify(self, row, filepath: str, lineno: int) -> RowKind | SkipReason:
        state = _text(row.state).casefold()
        if state != BOOKED_STATE:
            if state in KNOWN_PENDING_STATES:
                return SkipReason.PENDING
            return SkipReason.UNKNOWN_STATE

        value = row.amount
        if value == 0:
            return SkipReason.ZERO_AMOUNT
        if value > 0:
            return RowKind.EXPENSE

        # Negative rows are either the monthly bill payment or a merchant
        # refund. The payment carries neither a card nor a merchant.
        card = _text(row.card_id)
        merchant = _text(row.merchant)
        if not card and not merchant:
            return RowKind.PAYMENT
        if not card:
            logger.warning(
                f"{filepath}:{lineno}: negative row has no CardId but names "
                f"merchant {merchant!r}; treating it as a refund rather than a "
                f"bill payment"
            )
        return RowKind.REFUND

    def _build_entry(self, filepath: str, lineno: int, row, kind: RowKind):
        raw_amount = row.amount
        value, exact = _narrow_to_cents(raw_amount)
        if not exact:
            logger.warning(
                f"{filepath}:{lineno}: amount {raw_amount} does not fit two "
                f"decimals; keeping full precision"
            )

        currency = _text(row.currency) or DEFAULT_CURRENCY
        merchant = _text(row.merchant)

        # The liability leg is always -amount: a positive CSV amount is a
        # charge that deepens the debt, a negative one repays it.
        postings = [
            data.Posting(
                self._account_for(row, filepath),
                amount.Amount(-value, currency),
                None,
                None,
                None,
                None,
            )
        ]

        counter_account = self._counter_account(kind, merchant)
        if counter_account:
            postings.append(
                data.Posting(
                    counter_account,
                    amount.Amount(value, currency),
                    None,
                    None,
                    None,
                    None,
                )
            )

        return data.Transaction(
            meta=data.new_metadata(
                filepath, lineno, self._metadata(row, currency, raw_amount)
            ),
            date=row.txn_date,
            flag=self._flag(kind, row, currency),
            payee=merchant or None,
            narration=_text(row.details),
            tags=data.EMPTY_SET,
            links=data.EMPTY_SET,
            postings=postings,
        )

    def _counter_account(self, kind: RowKind, merchant: str) -> str | None:
        if kind is RowKind.PAYMENT:
            return self.settlement_account
        if kind is RowKind.EXPENSE:
            return self.merchant_map.get(merchant)
        # Refunds stay single-legged: which expense they reverse is a
        # categorization question this importer deliberately does not answer.
        return None

    def _account_for(self, row, filepath: str) -> str:
        if not self.card_accounts:
            return self.main_account

        card = _text(row.card_id)
        if not card:
            # Payment rows settle the account as a whole, not a single card.
            return self.main_account

        try:
            return self.card_accounts[card]
        except KeyError:
            raise VisecaCsvError(
                f"{filepath}: CardId {card!r} is not in card_accounts; add it "
                f"to the importer configuration"
            ) from None

    def _flag(self, kind: RowKind, row, currency: str) -> str:
        if self.flag_unverified and (
            kind is RowKind.REFUND or self._is_foreign(row, currency)
        ):
            return "!"
        return "*"

    @staticmethod
    def _is_foreign(row, currency: str) -> bool:
        original_currency = _text(row.original_currency)
        return bool(original_currency) and original_currency != currency

    def _metadata(self, row, currency: str, raw_amount: Decimal) -> dict:
        meta = {META_VALUTA_DATE: row.valuta_date}

        transaction_id = _text(row.transaction_id)
        if transaction_id:
            meta[META_TRANSACTION_ID] = transaction_id

        for key, value in (
            (META_CARD_ID, _text(row.card_id)),
            (META_MERCHANT_PLACE, _text(row.place)),
            (META_MERCHANT_COUNTRY, _text(row.country)),
        ):
            if value:
                meta[key] = value

        # Record the original only when it says something the posting does not.
        original_amount = row.original_amount
        original_currency = _text(row.original_currency)
        if original_amount is not None and (
            original_currency != currency or original_amount != raw_amount
        ):
            # Strings, not Decimal: Fava JSON-encodes Decimal as a number,
            # loads it back as float, and beancount's printer then raises
            # ValueError: Unexpected value: '355.13'.
            meta[META_ORIGINAL_AMOUNT] = str(original_amount)
            if original_currency:
                meta[META_ORIGINAL_CURRENCY] = original_currency

        rate = row.exchange_rate
        if rate is not None and rate != ONE:
            meta[META_EXCHANGE_RATE] = str(rate)

        return meta

    # -- Reporting ---------------------------------------------------------

    @staticmethod
    def _note_skip(
        skipped: collections.Counter,
        reason: SkipReason,
        filepath: str,
        lineno: int,
        exc: Exception | None = None,
    ) -> None:
        skipped[reason] += 1
        message = f"{filepath}:{lineno}: skipped, {reason.value}"
        if exc is not None:
            message = f"{message} ({exc})"
        if reason is SkipReason.PENDING:
            logger.debug(message)
        else:
            logger.warning(message)

    @staticmethod
    def _report_skipped(
        filepath: str, total: int, skipped: collections.Counter
    ) -> None:
        if not skipped:
            return
        detail = ", ".join(
            f"{count} {reason.value}"
            for reason, count in sorted(skipped.items(), key=lambda item: item[0].value)
        )
        logger.warning(
            f"{filepath}: skipped {sum(skipped.values())} of {total} rows ({detail})"
        )


_HEURISTIC_CMP = heuristic_comparator()
