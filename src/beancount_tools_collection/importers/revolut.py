# https://raw.githubusercontent.com/tarioch/beancounttools/master/src/tariochbctools/importers/revolut/importer.py

import csv
from loguru import logger
from datetime import timedelta
import re

from beancount.core import amount, data
from beancount.core.number import D
from beangulp.importer import Importer
from dateutil.parser import parse


class RevolutImporter(Importer):
    """An importer for Revolut CSV files."""

    def __init__(self, regexps, account, currency, regex=r"revolut_.*\.csv"):
        self.regexps = regexps
        self.main_account = account
        self.currency = currency
        self.regex = regex

    def identify(self, filepath):
        result = bool(re.search(self.regex, filepath, re.IGNORECASE))
        logger.info(f"identify assertion for revolut importer and file '{filepath}': {result}")
        return result

    def account(self, filepath):
        return self.main_account

    def extract(self, filepath, existing=None):
        logger.info(f"Starting extraction from file: {filepath}")
        entries = []

        with open(filepath, encoding='utf-8-sig') as csvfile:
            logger.debug(f"Successfully opened file {filepath}")
            reader = csv.DictReader(
                csvfile,
                [
                    "Type",
                    "Product",
                    "Started Date",
                    "Completed Date",
                    "Description",
                    "Amount",
                    "Fee",
                    "Currency",
                    "State",
                    "Balance",
                ],
                delimiter=",",
                skipinitialspace=True,
            )
            logger.debug("Created CSV DictReader with headers")
            next(reader)  # Skip header row
            logger.debug("Skipped header row")
            
            row_count = 0
            for row in reader:
                row_count += 1
                logger.debug(f"Processing row {row_count}: {row}")
                try:
                    logger.debug(f"Raw Balance: {row['Balance']}")
                    bal = D(row["Balance"].replace("'", "").strip())
                    logger.debug(f"Parsed Balance: {bal}")
                    
                    logger.debug(f"Raw Amount: {row['Amount']}")
                    amount_raw = D(row["Amount"].replace("'", "").strip())
                    logger.debug(f"Parsed Amount: {amount_raw}")
                    
                    amt = amount.Amount(amount_raw, row["Currency"])
                    logger.debug(f"Created Amount object: {amt}")
                    
                    balance = amount.Amount(bal, self.currency)
                    logger.debug(f"Created Balance object: {balance}")
                    
                    logger.debug(f"Raw Completed Date: {row['Completed Date']}")
                    book_date = parse(row["Completed Date"].strip()).date()
                    logger.debug(f"Parsed date: {book_date}")
                except Exception as e:
                    logger.error(f"Error processing row {row_count}: {e}")
                    logger.error(f"Problematic row data: {row}")
                    continue

                entry = data.Transaction(
                    data.new_metadata(filepath, 0, {}),
                    book_date,
                    "*",
                    row["Description"].strip(),
                    "",
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.main_account, amt, None, None, None, None),
                    ],
                )
                logger.debug(f"Created transaction entry: {entry}")
                entries.append(entry)

            logger.info(f"Processed {row_count} rows successfully")
            
            # only add balance after the last (newest) transaction
            try:
                book_date = book_date + timedelta(days=1)
                entry = data.Balance(
                    data.new_metadata(filepath, 0, {}),
                    book_date,
                    self.main_account,
                    balance,
                    None,
                    None,
                )
                logger.debug(f"Created balance entry: {entry}")
                entries.append(entry)
            except NameError as e:
                logger.warning(f"Could not create balance entry: {e}")

        logger.info(f"Finished extraction, created {len(entries)} entries")
        return entries