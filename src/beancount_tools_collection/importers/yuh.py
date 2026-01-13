"""
Beancount importer for Yuh CSV exports.

This importer is designed to handle the Yuh CSV exports, which contain a variety of transaction types.
It supports transactions, goal deposits, goal withdrawals.

The name of the goal account is used to identify the goal account. You can add more info in parrentheses, it will be removed.
Example: "Taxes (16%)" will be saved as "Taxes" (Assets:Cash:Yuh:Save:Taxes)

Foreign currency transactions (e.g., CARD_TRANSACTION_OUT in USD) are automatically combined with their
corresponding BANK_AUTO_ORDER_EXECUTED entries into a single CHF transaction with metadata for the
original currency details.

"""

from beangulp.importer import Importer
from beancount.core import data, amount
from beancount.core.number import D
from loguru import logger
import pandas as pd
import re


class YuhImporter(Importer):
    def __init__(self, account="Assets:Cash:Yuh:Pay:CHF", 
                 goals_base_account="Assets:Cash:Yuh:Save",
                 fees_account="Expenses:Fees:Yuh",
                 regex="yuh_.*\\.csv"):
        self.main_account = account
        self.goals_base_account = goals_base_account
        self.fees_account = fees_account
        self.regex = regex

    def identify(self, filepath):
        result = bool(re.search(self.regex, filepath, re.IGNORECASE))
        logger.info(f"identify assertion for yuh importer and file '{filepath}': {result}")
        return result

    def account(self, filepath):
        """The account to associate with this importer."""
        return self.main_account

    def _to_decimal(self, val):
        """Convert a value to Decimal, returning D('0') for empty/null values."""
        if pd.isna(val) or val == "":
            return D("0")
        # Handle case where column might contain non-numeric values
        str_val = str(val).strip()
        if not str_val or not str_val.lstrip('-').replace('.', '', 1).isdigit():
            return D("0")
        return D(str_val)

    def _clean_payee(self, activity_name, activity_type):
        """Clean up payee name and return (payee, narration, tags)."""
        payee = str(activity_name).strip('"')
        narration = ""
        tags = set()
        
        # Clean up transfer payees
        if activity_type in ["PAYMENT_TRANSACTION_IN", "PAYMENT_TRANSACTION_OUT"]:
            payee = payee.replace("Transfer from ", "").replace("Transfer to ", "").replace("Überweisung von ", "").replace("Überweisung an ", "")
        
        # Clean up Twint transactions and use title case
        if "twint" in payee.lower():
            payee = payee.replace("Twint from ", "").replace("Twint to ", "").replace("Twint von ", "").replace("Twint an ", "").title()
            narration = "Twint"
        
        # Clean up SumUp transactions 
        if payee.lower().startswith("sumup"):
            payee = payee.replace("SumUp  *", "").replace("SumUp from ", "").replace("SumUp to ", "").replace("SumUp von ", "").replace("SumUp an ", "").title()
            narration = "SumUp"

        # Clean up standing orders
        if activity_type in ["PAYMENT_TRANSACTION_IN", "PAYMENT_TRANSACTION_OUT"] and ("standing order" in payee.lower() or "dauerauftrag" in payee.lower()):
            payee = payee.replace("Dauerauftrag an  ", "")
            tags.add('recurring')

        return payee, narration, tags

    def extract(self, filepath, existing=None):
        logger.info(f"Starting extraction from file: {filepath}")
        entries = []
        
        # Read CSV with pandas
        df = pd.read_csv(filepath, sep=";", encoding="utf-8-sig")
        logger.info(f"CSV headers: {list(df.columns)}")
        
        # Vectorized date parsing
        df["DATE"] = pd.to_datetime(df["DATE"], format="%d/%m/%Y", errors="coerce")
        
        # Convert amounts to Decimal
        df["DEBIT"] = df["DEBIT"].apply(self._to_decimal)
        df["CREDIT"] = df["CREDIT"].apply(self._to_decimal)
        df["FEES/COMMISSION"] = df["FEES/COMMISSION"].apply(self._to_decimal)
        
        # Add absolute debit for matching
        df["DEBIT_ABS"] = df["DEBIT"].apply(abs)
        
        # Keep original index for metadata
        df["_orig_idx"] = df.index
        
        # Skip rows with invalid dates
        df = df[df["DATE"].notna()]
        
        # Filter out reward entries
        df = df[df["ACTIVITY TYPE"] != "REWARD_RECEIVED"]
        
        # Identify foreign currency transactions (non-CHF debits)
        foreign_mask = (df["DEBIT_ABS"] > 0) & (df["DEBIT CURRENCY"] != "CHF") & (df["DEBIT CURRENCY"].notna())
        foreign_txns = df[foreign_mask].copy()
        
        # Identify auto-exchange transactions
        auto_exchange_mask = df["ACTIVITY TYPE"] == "BANK_AUTO_ORDER_EXECUTED"
        auto_exchanges = df[auto_exchange_mask].copy()
        
        # Match auto-exchanges with foreign transactions using merge
        if not auto_exchanges.empty and not foreign_txns.empty:
            matched = auto_exchanges.merge(
                foreign_txns[["_orig_idx", "DATE", "ACTIVITY NAME", "ACTIVITY TYPE", "DEBIT_ABS", "DEBIT CURRENCY"]],
                left_on=["CREDIT", "CREDIT CURRENCY"],
                right_on=["DEBIT_ABS", "DEBIT CURRENCY"],
                suffixes=("", "_orig"),
                how="left"
            )
            
            # Track matched foreign transaction indices
            matched_foreign_indices = set(matched["_orig_idx_orig"].dropna().astype(int))
            matched_auto_indices = set(matched[matched["_orig_idx_orig"].notna()]["_orig_idx"].astype(int))
        else:
            matched = pd.DataFrame()
            matched_foreign_indices = set()
            matched_auto_indices = set()
        
        # Process matched auto-exchange transactions (combined with foreign)
        if not matched.empty:
            for _, row in matched[matched["_orig_idx_orig"].notna()].iterrows():
                entry = self._create_combined_transaction(filepath, row)
                if entry:
                    entries.append(entry)
        
        # Process unmatched auto-exchange transactions
        unmatched_auto = auto_exchanges[~auto_exchanges["_orig_idx"].isin(matched_auto_indices)]
        for _, row in unmatched_auto.iterrows():
            entry = self._create_standalone_exchange(filepath, row)
            if entry:
                entries.append(entry)
        
        # Process goal transactions
        goals = df[df["ACTIVITY TYPE"].isin(["GOAL_DEPOSIT", "GOAL_WITHDRAWAL"])]
        for _, row in goals.iterrows():
            entry = self._create_goal_transaction(filepath, row)
            if entry:
                entries.append(entry)
        
        # Process regular transactions (excluding auto-exchange, goals, and matched foreign)
        excluded_types = ["BANK_AUTO_ORDER_EXECUTED", "GOAL_DEPOSIT", "GOAL_WITHDRAWAL"]
        regular = df[~df["ACTIVITY TYPE"].isin(excluded_types)]
        regular = regular[~regular["_orig_idx"].isin(matched_foreign_indices)]
        
        for _, row in regular.iterrows():
            entry = self._create_regular_transaction(filepath, row)
            if entry:
                entries.append(entry)

        logger.info(f"Extracted {len(entries)} entries from {filepath}")
        return entries

    def _create_combined_transaction(self, filepath, row):
        """Create a combined transaction from matched auto-exchange and foreign currency transaction."""
        try:
            chf_debit = abs(row["DEBIT"])
            credit_amount = row["CREDIT"]
            credit_currency = row["CREDIT CURRENCY"]
            fee = row["FEES/COMMISSION"]
            exchange_rate = row.get("PRICE PER UNIT", "")
            
            total_chf = chf_debit + fee
            orig_idx = int(row["_orig_idx_orig"])
            orig_date = row["DATE_orig"].date() if pd.notna(row["DATE_orig"]) else row["DATE"].date()
            
            payee, narration, tags = self._clean_payee(row["ACTIVITY NAME_orig"], row["ACTIVITY TYPE_orig"])
            
            meta = data.new_metadata(filepath, orig_idx)
            meta["original-amount"] = f"{credit_amount} {credit_currency}"
            if exchange_rate and str(exchange_rate).strip():
                meta["exchange-rate"] = str(exchange_rate)
            
            postings = [
                data.Posting(
                    self.main_account,
                    amount.Amount(-total_chf, "CHF"),
                    None, None, None, None
                ),
            ]
            
            if fee > 0:
                postings.append(data.Posting(
                    self.fees_account,
                    amount.Amount(fee, "CHF"),
                    None, None, None, None
                ))
            
            postings.append(data.Posting(
                "Expenses:Unknown",
                amount.Amount(chf_debit, "CHF"),
                None, None, None, None
            ))
            
            txn = data.Transaction(
                meta=meta,
                date=orig_date,
                flag="*",
                payee=payee,
                narration=narration,
                tags=frozenset(tags),
                links=data.EMPTY_SET,
                postings=postings,
            )
            logger.info(f"Created combined transaction for {payee} with original amount {credit_amount} {credit_currency}")
            return txn
            
        except Exception as e:
            logger.error(f"Error creating combined transaction: {e}")
            return None

    def _create_standalone_exchange(self, filepath, row):
        """Create a standalone exchange transaction when no matching foreign transaction found."""
        try:
            chf_debit = abs(row["DEBIT"])
            credit_amount = row["CREDIT"]
            credit_currency = row["CREDIT CURRENCY"]
            fee = row["FEES/COMMISSION"]
            exchange_rate = row.get("PRICE PER UNIT", "")
            
            total_chf = chf_debit + fee
            idx = int(row["_orig_idx"])
            date = row["DATE"].date()
            
            meta = data.new_metadata(filepath, idx)
            meta["original-amount"] = f"{credit_amount} {credit_currency}"
            if exchange_rate and str(exchange_rate).strip():
                meta["exchange-rate"] = str(exchange_rate)
            
            postings = [
                data.Posting(
                    self.main_account,
                    amount.Amount(-total_chf, "CHF"),
                    None, None, None, None
                ),
            ]
            
            if fee > 0:
                postings.append(data.Posting(
                    self.fees_account,
                    amount.Amount(fee, "CHF"),
                    None, None, None, None
                ))
            
            postings.append(data.Posting(
                "Expenses:Unknown",
                amount.Amount(chf_debit, "CHF"),
                None, None, None, None
            ))
            
            txn = data.Transaction(
                meta=meta,
                date=date,
                flag="*",
                payee=str(row["ACTIVITY NAME"]).strip('"'),
                narration="Auto-exchange",
                tags=data.EMPTY_SET,
                links=data.EMPTY_SET,
                postings=postings,
            )
            logger.warning(f"Created standalone exchange transaction at row {idx + 1}")
            return txn
            
        except Exception as e:
            logger.error(f"Error creating standalone exchange: {e}")
            return None

    def _create_goal_transaction(self, filepath, row):
        """Create a goal deposit or withdrawal transaction."""
        try:
            is_deposit = row["ACTIVITY TYPE"] == "GOAL_DEPOSIT"
            goal_name = str(row["ACTIVITY NAME"]).strip('"')
            goal_name = goal_name.replace("Deposit to «", "").replace("Withdrawal from «", "").replace("»", "")
            goal_name = re.sub(r'\s*\([^)]*\)', '', goal_name).strip()
            goal_account = f"{self.goals_base_account}:{goal_name}"
            
            idx = int(row["_orig_idx"])
            date = row["DATE"].date()
            
            if is_deposit:
                amount_num = abs(row["CREDIT"])
                currency = row["CREDIT CURRENCY"]
            else:
                amount_num = abs(row["DEBIT"])
                currency = row["DEBIT CURRENCY"]
            
            logger.info(f"Processing {'deposit to' if is_deposit else 'withdrawal from'} goal: {goal_name}")
            
            # For deposit: main account loses money (-), goal gains (+)
            # For withdrawal: main account gains money (+), goal loses (-)
            txn = data.Transaction(
                meta=data.new_metadata(filepath, idx),
                date=date,
                flag="*",
                payee="self",
                narration=f"{'Deposit to' if is_deposit else 'Withdrawal from'} {goal_name}",
                tags=data.EMPTY_SET,
                links=data.EMPTY_SET,
                postings=[
                    data.Posting(
                        self.main_account,
                        amount.Amount(-amount_num if is_deposit else amount_num, currency),
                        None, None, None, None
                    ),
                    data.Posting(
                        goal_account,
                        amount.Amount(amount_num if is_deposit else -amount_num, currency),
                        None, None, None, None
                    ),
                ],
            )
            return txn
            
        except Exception as e:
            logger.error(f"Error creating goal transaction: {e}")
            return None

    def _create_regular_transaction(self, filepath, row):
        """Create a regular transaction."""
        try:
            idx = int(row["_orig_idx"])
            date = row["DATE"].date()
            
            if row["DEBIT"] != 0:
                amount_num = row["DEBIT"]
                currency = row["DEBIT CURRENCY"]
            elif row["CREDIT"] != 0:
                amount_num = row["CREDIT"]
                currency = row["CREDIT CURRENCY"]
            else:
                logger.debug(f"Skipping row {idx + 1} - no amount found")
                return None
            
            payee, narration, tags = self._clean_payee(row["ACTIVITY NAME"], row["ACTIVITY TYPE"])
            logger.debug(f"Processing transaction: {payee}")
            
            txn = data.Transaction(
                meta=data.new_metadata(filepath, idx),
                date=date,
                flag="*",
                payee=payee,
                narration=narration,
                tags=frozenset(tags),
                links=data.EMPTY_SET,
                postings=[
                    data.Posting(
                        self.main_account,
                        amount.Amount(amount_num, currency),
                        None, None, None, None
                    ),
                ],
            )
            return txn
            
        except Exception as e:
            logger.error(f"Error creating regular transaction: {e}")
            return None
