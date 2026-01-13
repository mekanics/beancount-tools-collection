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

from datetime import datetime
from beangulp.importer import Importer
from beancount.core import data, amount
from beancount.core.number import D
from loguru import logger
import csv
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

    def _parse_date(self, date_str, index):
        """Parse a date string and return a date object, or None if parsing fails."""
        date_str = date_str.strip() if date_str else None
        if not date_str:
            logger.warning(f"Empty date in row {index + 1}")
            return None
        try:
            date = datetime.strptime(date_str, "%d/%m/%Y").date()
            logger.debug(f"Parsed date '{date_str}' to {date}")
            return date
        except ValueError as e:
            logger.warning(f"Could not parse date '{date_str}' in row {index + 1}: {e}")
            return None

    def _clean_payee(self, row):
        """Clean up payee name and return (payee, narration, tags)."""
        payee = row["ACTIVITY NAME"].strip('"')
        narration = ""
        tags = set()
        
        # Clean up transfer payees
        if row["ACTIVITY TYPE"] in ["PAYMENT_TRANSACTION_IN", "PAYMENT_TRANSACTION_OUT"]:
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
        if row["ACTIVITY TYPE"] in ["PAYMENT_TRANSACTION_IN", "PAYMENT_TRANSACTION_OUT"] and ("standing order" in payee.lower() or "dauerauftrag" in payee.lower()):
            payee = payee.replace("Dauerauftrag an  ", "")
            tags.add('recurring')

        return payee, narration, tags

    def extract(self, filepath, existing=None):
        logger.info(f"Starting extraction from file: {filepath}")
        entries = []
        
        # First pass: Read all rows and index foreign currency transactions
        rows = []
        pending_foreign_txns = {}  # Key: (abs(amount), currency), Value: list of (index, row, date, payee info)
        
        with open(filepath, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=";")
            logger.info(f"CSV headers: {reader.fieldnames}")
            rows = list(reader)
        
        # Index foreign currency transactions (non-CHF debits that might need auto-exchange)
        for index, row in enumerate(rows):
            activity_type = row.get("ACTIVITY TYPE", "")
            
            # Skip rewards and auto-exchange entries in first pass
            if activity_type in ["REWARD_RECEIVED", "BANK_AUTO_ORDER_EXECUTED"]:
                continue
            
            date = self._parse_date(row.get("DATE", ""), index)
            if not date:
                continue
            
            # Check for foreign currency debit (non-CHF)
            if row.get("DEBIT") and row.get("DEBIT CURRENCY") and row["DEBIT CURRENCY"] != "CHF":
                try:
                    debit_amount = abs(D(row["DEBIT"]))
                    debit_currency = row["DEBIT CURRENCY"]
                    key = (debit_amount, debit_currency)
                    
                    payee, narration, tags = self._clean_payee(row)
                    
                    if key not in pending_foreign_txns:
                        pending_foreign_txns[key] = []
                    pending_foreign_txns[key].append({
                        "index": index,
                        "row": row,
                        "date": date,
                        "payee": payee,
                        "narration": narration,
                        "tags": tags,
                        "consumed": False
                    })
                    logger.debug(f"Indexed foreign currency transaction: {debit_amount} {debit_currency} at row {index + 1}")
                except (ValueError, KeyError) as e:
                    logger.debug(f"Could not index foreign transaction at row {index + 1}: {e}")
        
        # Track which rows have been consumed by auto-exchange matching
        consumed_rows = set()
        
        # Second pass: Process all rows
        for index, row in enumerate(rows):
            logger.debug(f"Processing row {index + 1}: {row}")
            activity_type = row.get("ACTIVITY TYPE", "")
            
            # Skip reward entries
            if activity_type == "REWARD_RECEIVED":
                logger.debug(f"Skipping REWARD_RECEIVED entry at row {index + 1}")
                continue
            
            # Skip rows that were consumed by auto-exchange matching
            if index in consumed_rows:
                logger.debug(f"Skipping consumed foreign currency transaction at row {index + 1}")
                continue

            date = self._parse_date(row.get("DATE", ""), index)
            if not date:
                continue
            
            # Handle BANK_AUTO_ORDER_EXECUTED - try to match with foreign currency transaction
            if activity_type == "BANK_AUTO_ORDER_EXECUTED":
                entry = self._handle_auto_exchange(filepath, index, row, date, pending_foreign_txns, consumed_rows)
                if entry:
                    entries.append(entry)
                continue
            
            # Handle goal deposits and withdrawals
            if activity_type in ["GOAL_DEPOSIT", "GOAL_WITHDRAWAL"]:
                entry = self._handle_goal_transaction(filepath, index, row, date)
                if entry:
                    entries.append(entry)
                continue

            # Handle regular transactions (skip foreign currency ones that are pending matching)
            if row.get("DEBIT") and row.get("DEBIT CURRENCY") and row["DEBIT CURRENCY"] != "CHF":
                # Check if this is a pending foreign currency transaction
                try:
                    debit_amount = abs(D(row["DEBIT"]))
                    debit_currency = row["DEBIT CURRENCY"]
                    key = (debit_amount, debit_currency)
                    if key in pending_foreign_txns:
                        # Check if this specific transaction is still pending
                        for pending in pending_foreign_txns[key]:
                            if pending["index"] == index and not pending["consumed"]:
                                logger.debug(f"Deferring foreign currency transaction at row {index + 1} for potential auto-exchange matching")
                                continue
                except (ValueError, KeyError):
                    pass
            
            entry = self._handle_regular_transaction(filepath, index, row, date)
            if entry:
                entries.append(entry)

        # Handle any unmatched foreign currency transactions (create them as-is)
        for key, pending_list in pending_foreign_txns.items():
            for pending in pending_list:
                if not pending["consumed"]:
                    logger.warning(f"Unmatched foreign currency transaction at row {pending['index'] + 1}: {key}")
                    entry = self._handle_regular_transaction(filepath, pending["index"], pending["row"], pending["date"])
                    if entry:
                        entries.append(entry)

        logger.info(f"Extracted {len(entries)} entries from {filepath}")
        return entries

    def _handle_auto_exchange(self, filepath, index, row, date, pending_foreign_txns, consumed_rows):
        """Handle BANK_AUTO_ORDER_EXECUTED transactions, combining with matched foreign currency transactions."""
        try:
            # Extract auto-exchange details
            chf_debit = abs(D(row["DEBIT"])) if row.get("DEBIT") else D("0")
            credit_amount = D(row["CREDIT"]) if row.get("CREDIT") else None
            credit_currency = row.get("CREDIT CURRENCY", "")
            fee = D(row.get("FEES/COMMISSION", "0") or "0")
            exchange_rate = row.get("PRICE PER UNIT", "")
            
            if not credit_amount or not credit_currency:
                logger.warning(f"Auto-exchange at row {index + 1} missing credit details")
                return None
            
            # Try to find matching foreign currency transaction
            key = (credit_amount, credit_currency)
            matched_txn = None
            
            if key in pending_foreign_txns:
                for pending in pending_foreign_txns[key]:
                    if not pending["consumed"]:
                        matched_txn = pending
                        pending["consumed"] = True
                        consumed_rows.add(pending["index"])
                        logger.info(f"Matched auto-exchange at row {index + 1} with foreign transaction at row {pending['index'] + 1}")
                        break
            
            if matched_txn:
                # Create combined transaction with original transaction details
                total_chf = chf_debit + fee
                
                meta = data.new_metadata(filepath, matched_txn["index"])
                meta["original-amount"] = f"{credit_amount} {credit_currency}"
                if exchange_rate:
                    meta["exchange-rate"] = exchange_rate
                
                postings = [
                    data.Posting(
                        self.main_account,
                        amount.Amount(-total_chf, "CHF"),
                        None, None, None, None
                    ),
                ]
                
                # Add fee posting if there's a fee
                if fee > 0:
                    postings.append(data.Posting(
                        self.fees_account,
                        amount.Amount(fee, "CHF"),
                        None, None, None, None
                    ))
                
                # Add expense posting (the converted CHF amount)
                postings.append(data.Posting(
                    "Expenses:Unknown",
                    amount.Amount(chf_debit, "CHF"),
                    None, None, None, None
                ))
                
                txn = data.Transaction(
                    meta=meta,
                    date=matched_txn["date"],
                    flag="*",
                    payee=matched_txn["payee"],
                    narration=matched_txn["narration"],
                    tags=frozenset(matched_txn["tags"]),
                    links=data.EMPTY_SET,
                    postings=postings,
                )
                return txn
            else:
                # No match found - create standalone exchange transaction
                logger.warning(f"No matching foreign transaction for auto-exchange at row {index + 1}")
                total_chf = chf_debit + fee
                
                meta = data.new_metadata(filepath, index)
                meta["original-amount"] = f"{credit_amount} {credit_currency}"
                if exchange_rate:
                    meta["exchange-rate"] = exchange_rate
                
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
                    payee=row["ACTIVITY NAME"].strip('"'),
                    narration="Auto-exchange",
                    tags=data.EMPTY_SET,
                    links=data.EMPTY_SET,
                    postings=postings,
                )
                return txn
                
        except (KeyError, ValueError) as e:
            logger.error(f"Error processing auto-exchange at row {index + 1}: {e}")
            return None

    def _handle_goal_transaction(self, filepath, index, row, date):
        """Handle GOAL_DEPOSIT and GOAL_WITHDRAWAL transactions."""
        is_deposit = row["ACTIVITY TYPE"] == "GOAL_DEPOSIT"
        goal_name = row["ACTIVITY NAME"].strip('"')
        goal_name = goal_name.replace("Deposit to «", "").replace("Withdrawal from «", "").replace("»", "")
        # Remove percentages in parentheses
        goal_name = re.sub(r'\s*\([^)]*\)', '', goal_name).strip()
        goal_account = f"{self.goals_base_account}:{goal_name}"
        logger.info(f"Processing {'deposit to' if is_deposit else 'withdrawal from'} goal: {goal_name}")
        
        try:
            if is_deposit:
                amount_num = D(row["CREDIT"])
                currency = row["CREDIT CURRENCY"]
            else:
                amount_num = D(row["DEBIT"])
                currency = row["DEBIT CURRENCY"]
            logger.debug(f"Goal transaction amount: {amount_num} {currency}")
        except (KeyError, ValueError) as e:
            logger.error(f"Error processing amount for goal transaction in row {index + 1}: {e}")
            return None
        
        txn = data.Transaction(
            meta=data.new_metadata(filepath, index),
            date=date,
            flag="*",
            payee="self",
            narration=f"{'Deposit to' if is_deposit else 'Withdrawal from'} {goal_name}",
            tags=data.EMPTY_SET,
            links=data.EMPTY_SET,
            postings=[
                data.Posting(
                    self.main_account,
                    amount.Amount(amount_num * (-1 if is_deposit else 1), currency),
                    None, None, None, None
                ),
                data.Posting(
                    goal_account,
                    amount.Amount(amount_num * (1 if is_deposit else -1), currency),
                    None, None, None, None
                ),
            ],
        )
        return txn

    def _handle_regular_transaction(self, filepath, index, row, date):
        """Handle regular transactions."""
        try:
            if row.get("DEBIT"):
                amount_num = D(row["DEBIT"])
                currency = row["DEBIT CURRENCY"]
                logger.debug(f"Processing debit transaction: {amount_num} {currency}")
            elif row.get("CREDIT"):
                amount_num = D(row["CREDIT"])
                currency = row["CREDIT CURRENCY"]
                logger.debug(f"Processing credit transaction: {amount_num} {currency}")
            else:
                logger.debug(f"Skipping row {index + 1} - no amount found")
                return None
        except (KeyError, ValueError) as e:
            logger.error(f"Error processing amount in row {index + 1}: {e}")
            return None

        payee, narration, tags = self._clean_payee(row)
        logger.debug(f"Transaction payee: {payee}")

        meta = data.new_metadata(filepath, index)
        
        txn = data.Transaction(
            meta=meta,
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
