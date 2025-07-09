"""
Beancount importer for Yuh CSV exports.

This importer is designed to handle the Yuh CSV exports, which contain a variety of transaction types.
It supports transactions, goal deposits, goal withdrawals.

The name of the goal account is used to identify the goal account. You can add more info in parrentheses, it will be removed.
Example: "Taxes (16%)" will be saved as "Taxes" (Assets:Cash:Yuh:Save:Taxes)

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
                 regex="yuh_.*\\.csv"):
        self.main_account = account
        self.goals_base_account = goals_base_account
        self.regex = regex

    def identify(self, filepath):
        result = bool(re.search(self.regex, filepath, re.IGNORECASE))
        logger.info(f"identify assertion for yuh importer and file '{filepath}': {result}")
        return result

    def account(self, filepath):
        """The account to associate with this importer."""
        return self.main_account

    def extract(self, filepath, existing=None):
        logger.info(f"Starting extraction from file: {filepath}")
        entries = []
        
        with open(filepath, encoding='utf-8-sig') as f:  # Use utf-8-sig to handle BOM
            reader = csv.DictReader(f, delimiter=";")
            logger.info(f"CSV headers: {reader.fieldnames}")
            
            for index, row in enumerate(reader):
                logger.debug(f"Processing row {index + 1}: {row}")
                
                # Skip reward entries
                if row["ACTIVITY TYPE"] == "REWARD_RECEIVED":
                    logger.debug(f"Skipping REWARD_RECEIVED entry at row {index + 1}")
                    continue

                # Parse date - strip whitespace and handle potential empty values
                date_str = row["DATE"].strip() if "DATE" in row and row["DATE"] else None
                if not date_str:
                    logger.warning(f"Empty date in row {index + 1}")
                    continue
                    
                try:
                    date = datetime.strptime(date_str, "%d/%m/%Y").date()
                    logger.debug(f"Parsed date '{date_str}' to {date}")
                except ValueError as e:
                    logger.warning(f"Could not parse date '{date_str}' in row {index + 1}: {e}")
                    continue
                
                # Handle goal deposits and withdrawals
                if row["ACTIVITY TYPE"] in ["GOAL_DEPOSIT", "GOAL_WITHDRAWAL"]:
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
                        continue
                    
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
                    entries.append(txn)
                    continue

                # Handle regular transactions
                try:
                    if row["DEBIT"]:
                        amount_num = D(row["DEBIT"])
                        currency = row["DEBIT CURRENCY"]
                        logger.debug(f"Processing debit transaction: {amount_num} {currency}")
                    elif row["CREDIT"]:
                        amount_num = D(row["CREDIT"])
                        currency = row["CREDIT CURRENCY"]
                        logger.debug(f"Processing credit transaction: {amount_num} {currency}")
                    else:
                        logger.debug(f"Skipping row {index + 1} - no amount found")
                        continue  # Skip if no amount
                except (KeyError, ValueError) as e:
                    logger.error(f"Error processing amount in row {index + 1}: {e}")
                    continue

                # Clean up payee
                payee = row["ACTIVITY NAME"].strip('"')
                logger.debug(f"Transaction payee: {payee}")
                
                narration = ""
                
                # Clean up transfer payees
                if row["ACTIVITY TYPE"] in ["PAYMENT_TRANSACTION_IN", "PAYMENT_TRANSACTION_OUT"]:
                    payee = payee.replace("Transfer from ", "").replace("Transfer to ", "").replace("Überweisung von ", "").replace("Überweisung an ", "")
     
                
                # Clean up Twint transactions and use title case for narration
                if row["ACTIVITY TYPE"] in ["CARD_TRANSACTION_IN", "CARD_TRANSACTION_OUT"]:
                    payee = payee.replace("Twint an ", "").replace("Twint von ", "").replace("Twint an ", "").title()
                    narration = "Twint"

                logger.debug(f"Cleaned transfer payee: {payee}")

                # Create transaction
                meta = data.new_metadata(filepath, index)
                
                txn = data.Transaction(
                    meta=meta,
                    date=date,
                    flag="*",
                    payee=payee,
                    narration=narration,
                    tags=data.EMPTY_SET,
                    links=data.EMPTY_SET,
                    postings=[
                        data.Posting(
                            self.main_account,
                            amount.Amount(amount_num, currency),
                            None, None, None, None
                        ),
                    ],
                )
                entries.append(txn)

        logger.info(f"Extracted {len(entries)} entries from {filepath}")
        return entries 