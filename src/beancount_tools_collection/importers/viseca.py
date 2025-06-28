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

        for idx, row in df.iterrows():
            logger.debug(f"Processing transaction {idx}: {row.get('transactionId')}")
            try:
                # Category mapping
                pfm_cat = row.get("pfmCategory.id", "other")
                if pfm_cat == "deposits":
                    continue  # Ignore payment transactions

                # Parse date
                date = pd.to_datetime(row["date"]).date()
                payee = row.get("prettyName") or row.get("merchantName") or "Unknown"
                details = row.get("details", "")
                currency = row.get("currency", "CHF")
                amt = Decimal(str(row["amount"]))
                # Viseca: negative = refund, positive = expense
                amt = -amt if amt < 0 else amt
        
                expense_account = self.category_map.get(pfm_cat, "Expenses:Unknown")
                
                # Foreign currency handling
                orig_amt = row.get("originalAmount")
                orig_cur = row.get("originalCurrency")
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
    
                # Build metadata dict and convert floats to strings
                def safe_meta_value(v):
                    if v is None:
                        return None
                    if isinstance(v, float):
                        if math.isnan(v) or math.isinf(v):
                            return None
                    if pd.isna(v):
                        return None
                    return v

                meta_dict = {
                    "transactionId": safe_meta_value(row.get("transactionId")),
                    "category": safe_meta_value(pfm_cat),
                    "merchant": safe_meta_value(payee),
                    "details": safe_meta_value(details),
                    "originalAmount": str(safe_meta_value(orig_amt)) if orig_amt is not None else None,
                    "originalCurrency": safe_meta_value(orig_cur),
                }
                if orig_cur != "CHF":
                    meta_dict["conversionRate"] = safe_meta_value(row.get("conversionRate"))
                    meta_dict["conversionRateDate"] = safe_meta_value(row.get("conversionRateDate"))
                
                # Convert any float values to strings (for safety)
                for k, v in meta_dict.items():
                    if isinstance(v, float):
                        meta_dict[k] = str(v)

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
