import json
import pandas as pd
import re
from loguru import logger
from datetime import datetime, timedelta
import math

from beangulp.importer import Importer
from beancount.core.number import Decimal
from beancount.core import data, amount


class VisecaImporter(Importer):
    """
    Beancount Importer for Viseca JSON transaction exports.
    """
    def __init__(self, account="Liabilities:CreditCard:Viseca", regex=r"viseca.*\.json", category_map=None, split_expense_account=None, split_ratio=0.5):
        self.main_account = account
        self.regex = regex
        self.flag = "*"
        self.split_expense_account = split_expense_account
        self.split_ratio = Decimal(str(split_ratio))
        # Default category mapping if none provided
        self.category_map = category_map or {
            "food_and_drink": "Expenses:Food",
            "groceries": "Expenses:Groceries",
            "shopping": "Expenses:Shopping",
            "travel": "Expenses:Travel",
            "personal_care": "Expenses:PersonalCare",
            "leisure": "Expenses:Leisure",
            "transport": "Expenses:Transport",
            # Add more mappings as needed
        }

    def identify(self, filepath):
        result = bool(re.search(self.regex, filepath, re.IGNORECASE))
        logger.info(f"identify assertion for viseca importer and file '{filepath}': {result}")
        return result

    def account(self, filepath):
        return self.main_account

    def extract(self, filepath, existing=None):
        logger.info(f"Starting extraction from file: {filepath}")
        entries = []
        with open(filepath, encoding="utf-8-sig") as f:
            data_json = json.load(f)
        
        txs = data_json["list"]
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
            logger.debug(f"Processing transaction {idx}: {row.get('transactionId')}")
            try:
                # Skip non-booked transactions
                state_type = safe_value(row.get("stateType"))
                if state_type != "booked":
                    logger.debug(
                        f"Skipping non-booked transaction {row.get('transactionId')} "
                        f"(stateType={state_type})"
                    )
                    continue

                # Category mapping
                pfm_cat = safe_value(row.get("pfmCategory.id")) or "other"
                if pfm_cat == "deposits":
                    continue  # Ignore payment transactions

                # Parse date
                date = pd.to_datetime(row["date"]).date()
                payee = (
                    safe_value(row.get("prettyName"))
                    or safe_value(row.get("merchantName"))
                    or "Unknown"
                )
                details = safe_value(row.get("details")) or ""
                currency = safe_value(row.get("currency")) or "CHF"
                amt = Decimal(str(row["amount"]))
                # Viseca: negative = refund, positive = expense
                amt = -amt if amt < 0 else amt
        
                expense_account = self.category_map.get(pfm_cat, "Expenses:Unknown")
                
                # Foreign currency handling
                orig_amt = safe_value(row.get("originalAmount"))
                orig_cur = safe_value(row.get("originalCurrency"))
                postings = []
                
                # Main posting: always the credit card liability
                postings.append(
                    data.Posting(
                        self.main_account,
                        amount.Amount(-amt, currency),
                        None, None, None, None
                    )
                )
                # Expense posting(s)
                if self.split_expense_account:
                    # Split the amount according to split_ratio and round to 3 decimal places
                    amt_main = (amt * self.split_ratio).quantize(Decimal("0.001"))
                    amt_split = amt - amt_main  # Ensure total matches original
                    
                    # Format amounts to 2 decimals if they end with 0, otherwise keep 3 decimals
                    def format_amount(amt):
                        return amt.quantize(Decimal("0.01")) if amt % Decimal("0.01") == 0 else amt
                    
                    postings.append(
                        data.Posting(
                            expense_account,
                            amount.Amount(format_amount(amt_main), currency),
                            None, None, None, None
                        )
                    )
                    postings.append(
                        data.Posting(
                            self.split_expense_account,
                            amount.Amount(format_amount(amt_split), currency),
                            None, None, None, None
                        )
                    )
                else:
                    postings.append(
                        data.Posting(
                            expense_account,
                            amount.Amount(amt, currency),
                            None, None, None, None
                        )
                    )
    
                # # If foreign currency, add a posting for the original amount
                # if orig_amt and orig_cur and orig_cur != currency:
                #     postings.append(
                #         data.Posting(
                #             expense_account,
                #             amount.Amount(Decimal(str(orig_amt)), orig_cur),
                #             None, None, None, None
                #         )
                #     )
    
                meta_dict = {
                    "transactionId": safe_value(row.get("transactionId")),
                    "category": safe_value(pfm_cat),
                    "merchant": safe_value(payee),
                    "details": safe_value(details),
                    "originalAmount": str(orig_amt) if orig_amt is not None else None,
                    "originalCurrency": orig_cur,
                }
                if orig_cur is not None and orig_cur != "CHF":
                    meta_dict["conversionRate"] = safe_value(row.get("conversionRate"))
                    meta_dict["conversionRateDate"] = safe_value(row.get("conversionRateDate"))

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
                logger.warning(f"Error processing transaction at index {idx}: {e}")
                continue
        logger.info(f"Extracted {len(entries)} entries from {filepath}")
        return entries
