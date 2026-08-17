"""Beancount Importer for Viseca JSON transaction exports.

The Viseca JSON export carries a PFM category id. That category is recorded as
metadata so a categorization layer can match on it; this importer does not map
it to an expense account. Writing a placeholder expense account here would make
the transaction look complete and disable hooks such as ``beancount-hooks``.

When ``split_expense_account`` is set, the partner's share is configuration —
post it explicitly and leave the residual for the hooks to fill.
"""

import json
import math
import re

import pandas as pd
from beancount.core import amount, data
from beancount.core.number import Decimal
from beangulp.importer import Importer
from loguru import logger


class VisecaImporter(Importer):
    """
    Beancount Importer for Viseca JSON transaction exports.
    """

    def __init__(
        self,
        account='Liabilities:CreditCard:Viseca',
        regex=r'viseca.*\.json',
        split_expense_account=None,
        split_ratio=0.5,
    ):
        self.main_account = account
        self.regex = regex
        self.flag = '*'
        self.split_expense_account = split_expense_account
        self.split_ratio = Decimal(str(split_ratio))

    def identify(self, filepath):
        result = bool(re.search(self.regex, filepath, re.IGNORECASE))
        logger.info(f"identify assertion for viseca importer and file '{filepath}': {result}")
        return result

    def account(self, filepath):
        return self.main_account

    def extract(self, filepath, existing=None):
        logger.info(f'Starting extraction from file: {filepath}')
        entries = []
        with open(filepath, encoding='utf-8-sig') as f:
            data_json = json.load(f)

        txs = data_json['list']
        df = pd.json_normalize(txs)

        # pandas.json_normalize fills missing keys with NaN. NaN is a float
        # that is *truthy* in Python and survives chained `or` expressions,
        # which would otherwise leak into Transaction fields (payee, meta, ...)
        # and break Fava's JSON serialization.
        def safe_value(v):
            if v is None:
                return None
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            try:
                if pd.isna(v):
                    return None
            except (TypeError, ValueError):
                pass
            return v

        for idx, row in df.iterrows():
            logger.debug(f'Processing transaction {idx}: {row.get("transactionId")}')
            try:
                # Skip non-booked transactions
                state_type = safe_value(row.get('stateType'))
                if state_type != 'booked':
                    logger.debug(
                        f'Skipping non-booked transaction {row.get("transactionId")} '
                        f'(stateType={state_type})'
                    )
                    continue

                pfm_cat = safe_value(row.get('pfmCategory.id')) or 'other'
                if pfm_cat == 'deposits':
                    continue  # Ignore payment transactions

                # Parse date
                date = pd.to_datetime(row['date']).date()
                payee = (
                    safe_value(row.get('prettyName')) or safe_value(row.get('merchantName')) or None
                )
                details = safe_value(row.get('details')) or ''
                currency = safe_value(row.get('currency')) or 'CHF'
                amt = Decimal(str(row['amount']))
                # Viseca: negative = refund, positive = expense
                amt = -amt if amt < 0 else amt

                # Foreign currency handling
                orig_amt = safe_value(row.get('originalAmount'))
                orig_cur = safe_value(row.get('originalCurrency'))
                postings = []

                # Main posting: always the credit card liability
                postings.append(
                    data.Posting(
                        self.main_account,
                        amount.Amount(-amt, currency),
                        None,
                        None,
                        None,
                        None,
                    )
                )
                # Partner share is configuration — post it explicitly and leave
                # the residual for a categorization layer to fill.
                if self.split_expense_account:
                    amt_main = (amt * self.split_ratio).quantize(Decimal('0.001'))
                    amt_split = amt - amt_main  # Ensure total matches original

                    def format_amount(value):
                        return (
                            value.quantize(Decimal('0.01'))
                            if value % Decimal('0.01') == 0
                            else value
                        )

                    postings.append(
                        data.Posting(
                            self.split_expense_account,
                            amount.Amount(format_amount(amt_split), currency),
                            None,
                            None,
                            None,
                            None,
                        )
                    )

                meta_dict = {
                    'transactionId': safe_value(row.get('transactionId')),
                    'category': safe_value(pfm_cat),
                    'merchant': safe_value(payee),
                    'details': safe_value(details),
                    'originalAmount': str(orig_amt) if orig_amt is not None else None,
                    'originalCurrency': orig_cur,
                }
                if orig_cur is not None and orig_cur != 'CHF':
                    meta_dict['conversionRate'] = safe_value(row.get('conversionRate'))
                    meta_dict['conversionRateDate'] = safe_value(row.get('conversionRateDate'))

                # Drop None entries and stringify any residual floats so nothing
                # NaN-shaped slips into Fava's JSON encoder.
                meta_dict = {
                    k: (str(v) if isinstance(v, float) else v)
                    for k, v in meta_dict.items()
                    if v is not None
                }

                meta = data.new_metadata(filepath, idx, meta_dict)
                txn = data.Transaction(
                    meta=meta,
                    date=date,
                    flag=self.flag,
                    payee=payee,
                    narration=None,
                    tags=data.EMPTY_SET,
                    links=data.EMPTY_SET,
                    postings=postings,
                )
                entries.append(txn)
            except Exception as e:
                logger.warning(f'Error processing transaction at index {idx}: {e}')
                continue
        logger.info(f'Extracted {len(entries)} entries from {filepath}')
        return entries
