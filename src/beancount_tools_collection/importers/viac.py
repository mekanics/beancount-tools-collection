"""
Open the Network tab in the developer mode of your prefered browser. 
Open https://app.viac.ch/, choose a portfolio and look for a 'transactions' request.
Download the request and pass the resulting file to this importer.
"""

### https://app.viac.ch/files/document/21V-KOM-JHE


import json
import pandas as pd
import re
from loguru import logger
from datetime import datetime, timedelta

from beangulp.importer import Importer
from beancount.core.number import Decimal
from beancount.core import data, amount, position


class ViacImporter(Importer):
    """
    Beancount Importer for Viac
    """

    def __init__(
        self,
        deposit_account=None,
        root_account=None,
        share_lookup=None,
        div_suffix="Div",  # suffix for dividend Account , like Assets:Invest:IB:VT:Div
        interest_suffix="Interest",
        interest_account=None,
        fees_suffix="Fees",
        fees_account=None,
        pnl_suffix="PnL",
        file_encoding="utf-8-sig",
        regex=r"viac_(S[2,3][a]?)_(Portfolio\d|Freizuegigkeit|Ueberobligatorium|transactions)",
        account_map=None,  # Optional mapping for custom account names
    ):
        self.deposit_account = deposit_account
        self.root_account = (
            root_account  # root account from  which others can be derived
        )
        self.div_suffix = div_suffix
        self.interest_suffix = interest_suffix
        self.interest_account = interest_account
        self.fees_suffix = fees_suffix
        self.fees_account = fees_account
        self.pnl_suffix = pnl_suffix
        self.share_lookup = share_lookup
        self.file_encoding = file_encoding
        self.regex = regex
        self.flag = "*"
        # Default account mapping that can be overridden
        self.account_map = {
            "S2": {
                "Obligatorium": "Assets:Vorsorge:S2:Viac:Freizuegigkeit",
                "Ueberobligatorium": "Assets:Vorsorge:S2:Viac:Ueberobligatorium",
                "Interest": {
                    "Obligatorium": "Income:Vorsorge:S2:Viac:Freizuegigkeit:Interest",
                    "Ueberobligatorium": "Income:Vorsorge:S2:Viac:Ueberobligatorium:Interest"
                },
                "Fees": {
                    "Obligatorium": "Expenses:Vorsorge:S2:Viac:Freizuegigkeit:Fees",
                    "Ueberobligatorium": "Expenses:Vorsorge:S2:Viac:Ueberobligatorium:Fees"
                }
            },
            "S3a": {
                "Portfolio": "Assets:Vorsorge:S3a:Viac:Portfolio{}"
            }
        }
        # Override with custom mapping if provided
        if account_map:
            self.account_map.update(account_map)

    def identify(self, filepath):
        # intended file format is *viac_*
        result = bool(re.search(self.regex, filepath, re.IGNORECASE))
        logger.info(
            f"identify assertion for viac importer and file '{filepath}': {result}"
        )
        return result

    def _get_s2_account_type(self, source_account=None):
        """Helper method to determine S2 account type based on source account."""
        if source_account and source_account.endswith('.O'):
            return 'Obligatorium'
        elif source_account and source_account.endswith('.U'):
            return 'Ueberobligatorium'
        else:
            # Fallback to current main account type if no source account
            return 'Obligatorium' if 'Obligatorium' in self.main_account else 'Ueberobligatorium'

    def _get_mapped_s2_account(self, category, source_account=None):
        """Helper method to get mapped account for S2 accounts.
        
        Args:
            category (str): Category in account map (e.g., 'Interest', 'Fees')
            source_account (str, optional): Source account key from transaction
        
        Returns:
            str: Mapped account name or None if not found
        """
        if not self.main_account.startswith('Assets:Vorsorge:S2'):
            return None
            
        account_type = self._get_s2_account_type(source_account)
        
        # Handle nested mappings (like Interest and Fees) vs direct mappings
        if category in ['Interest', 'Fees']:
            return self.account_map.get('S2', {}).get(category, {}).get(account_type)
        else:
            return self.account_map.get('S2', {}).get(account_type)

    def getLiquidityAccount(self, currency, source_account=None):
        """Get the liquidity account for a transaction."""
        mapped_account = self._get_mapped_s2_account('', source_account)
        if mapped_account:
            return ":".join([mapped_account, currency])
        
        # Fall back to default behavior
        return ":".join([self.main_account, currency])

    def getDivIncomeAccount(self, currency, symbol):
        return ":".join(
            [self.main_account.replace("Assets", "Income"), symbol, self.div_suffix]
        )

    def getInterestIncomeAccount(self, currency, source_account=None):
        """Get the interest income account for a transaction."""
        if self.interest_account:
            return self.interest_account

        mapped_account = self._get_mapped_s2_account('Interest', source_account)
        if mapped_account:
            return mapped_account

        # Fall back to default behavior
        return ":".join([
            self.main_account.replace("Assets", "Income"),
            self.interest_suffix,
            currency,
        ])

    def getAssetAccount(self, symbol):
        return ":".join([self.main_account, symbol])

    def getFeesAccount(self, currency, source_account=None):
        """Get the fees account for a transaction."""
        if self.fees_account:
            return self.fees_account

        mapped_account = self._get_mapped_s2_account('Fees', source_account)
        if mapped_account:
            return mapped_account

        # Fall back to default behavior
        return ":".join([
            self.main_account.replace("Assets", "Expenses"),
            self.fees_suffix,
            currency,
        ])

    def account(self, filepath):
        """The account to associate with this importer."""
        self.fix_accounts(filepath)
        return self.main_account or self.root_account

    def getDocumentUrl(self, document):
        return f"https://app.viac.ch/files/document/{document}"

    def _get_mapped_account(self, pillar, account_type, portfolio_num=None):
        """Get the mapped account based on pillar and type"""
        if pillar not in self.account_map:
            return None
            
        mapping = self.account_map[pillar]
        if pillar == "S2":
            return mapping.get(account_type)
        elif pillar == "S3a" and account_type == "Portfolio":
            return mapping["Portfolio"].format(portfolio_num) if portfolio_num else None
        return None

    def fix_accounts(self, filepath):
        try:
            pillar, portfolio = re.search(self.regex, filepath, re.IGNORECASE).groups()
        except AttributeError as e:
            logger.error(
                f"could not extract pillar and/or portfolio from filename {filepath} with regex pattern {self.regex}."
            )
            raise AttributeError(e)
        
        # Try to get mapped account first
        if pillar == "S2":
            # For S2, we'll set the base account and let extract() method handle O/U distinction
            self.main_account = self._get_mapped_account(pillar, "Obligatorium") or self.root_account
        elif pillar == "S3a" and portfolio.startswith("Portfolio"):
            portfolio_num = portfolio[9:]  # Extract number from "PortfolioX"
            self.main_account = self._get_mapped_account(pillar, "Portfolio", portfolio_num) or self.root_account
        
        # If no mapping found, fall back to original behavior
        if not self.main_account:
            new_account = re.sub(r"S[2,3]a?", pillar, self.root_account)
            if pillar == "S2":
                portfolio = "Freizuegigkeit"
            self.main_account = re.sub(r"Portfolio\d|Freizuegigkeit|Ueberobligatorium", portfolio, new_account)

    def extract(self, filepath, existing=None):
        # the actual processing of the json export

        # fix Account names with regard to pillar 2/3 and different portfolios.
        self.fix_accounts(filepath)

        with open(filepath) as data_file:
            data = json.load(data_file)

        transactions = data["transactions"]
        return_txn = []

        # Skip transfer accounts (D1, D2)
        for account_key in [k for k in transactions.keys() if not k.endswith(('D1', 'D2'))]:
            # Store original main_account to restore after processing each account
            original_account = self.main_account
            
            # For S2 accounts, determine if this is Obligatorium or Überobligatorium
            if self.main_account.startswith('Assets:Vorsorge:S2'):
                if account_key.endswith('.O'):
                    self.main_account = self._get_mapped_account('S2', 'Obligatorium') or \
                        self.main_account.replace('Freizuegigkeit', 'Obligatorium')
                elif account_key.endswith('.U'):
                    self.main_account = self._get_mapped_account('S2', 'Ueberobligatorium') or \
                        self.main_account.replace('Freizuegigkeit', 'Ueberobligatorium')

            account_transactions = transactions[account_key]
            df = pd.json_normalize(account_transactions)

            # Add source account info to dataframe
            df['source_account'] = account_key

            # convert specific columns to Decimal with specific precisions
            to_decimal_dict = {
                "amountInChf": 4,
                "balanceAfterBooking": 3,
            }

            for col, digits in to_decimal_dict.items():
                df[col] = df[col].apply(lambda x: Decimal(x).__round__(digits))

            df["valueDate"] = pd.to_datetime(df["valueDate"]).apply(datetime.date)
            df["documentNumber"] = df["documentNumber"].astype(str)

            # disect the complete report in similar transactions
            interests = df[df.type == "INTEREST"]
            fees = df[df.type == "FEE_CHARGE"]
            deposits = df[df.type == "CONTRIBUTION"]
            trades = df[df.type.isin(["TRADE_SELL", "TRADE_BUY"])]
            dividends = df[df.type.isin(["DIVIDEND", "DIVIDEND_CANCELLATION"])]

            return_txn.extend(
                self.Trades(trades)
                + self.Interest(interests)
                + self.Fees(fees)
                + self.Deposits(deposits)
                + self.Dividends(dividends)
                + self.Balances(df)
            )
            
            # Restore original main_account for next iteration
            self.main_account = original_account

        return return_txn

    def Trades(self, trades):
        bean_transactions = []
        for idx, row in trades.iterrows():
            currency = "CHF"
            asset = row["description"]
            share = self.share_lookup.get(asset)
            if share is None:
                logger.error(
                    f"Could not fetch share '{row['description']}' from supplied lookup {list(self.share_lookup.keys())}"
                )
                continue
            symbol = share["symbol"]
            isin = share["isin"]

            if symbol is None:
                logger.error(
                    f"Could not fetch isin {row['ISIN']} from supplied ISINs {list(self.isin_lookup.keys())}"
                )
                continue
            proceeds = amount.Amount(row["amountInChf"], currency)
            quantity = amount.Amount(Decimal(0), symbol)
            price = amount.Amount(Decimal(0), currency)
            cost = position.CostSpec(
                number_per=None,
                number_total=None,
                currency=None,
                date=None,
                label=None,
                merge=False,
            )

            postings = [
                data.Posting(
                    self.getAssetAccount(symbol), quantity, cost, None, None, None
                ),
                data.Posting(
                    self.getLiquidityAccount(currency, row["source_account"]), proceeds, None, None, None, None
                ),
            ]

            metadata = {"source_account": row["source_account"]}
            document = row["documentNumber"]
            if document != None and document != "nan":
                metadata["link"] = self.getDocumentUrl(document)

            meta = data.new_metadata("Trade", idx, metadata)

            if proceeds.number < 0:
                buy_sell = "BUY"
            else:
                buy_sell = "SELL"
            bean_transactions.append(
                data.Transaction(
                    meta,
                    row["valueDate"],
                    "!",
                    isin,  # payee
                    " ".join(
                        [
                            buy_sell,
                            quantity.to_string(),
                            "@",
                            price.to_string() + ";",
                            asset,
                        ]
                    ),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return bean_transactions

    def Interest(self, int_):
        # calculates interest payments from IBKR data
        bean_transactions = []
        for idx, row in int_.iterrows():
            currency = "CHF"
            amount_ = amount.Amount(row["amountInChf"], currency)

            # make the postings, two for interest payments
            # received and paid interests are booked on the same account
            postings = [
                data.Posting(
                    self.getInterestIncomeAccount(currency, row["source_account"]),
                    -amount_,
                    None,
                    None,
                    None,
                    None,
                ),
                data.Posting(
                    self.getLiquidityAccount(currency, row["source_account"]), amount_, None, None, None, None
                ),
            ]

            metadata = {"source_account": row["source_account"]}
            document = row["documentNumber"]
            if document != None and document != "nan":
                metadata["link"] = self.getDocumentUrl(document)

            meta = data.new_metadata("Interest", idx, metadata)

            bean_transactions.append(
                data.Transaction(
                    meta,
                    row["valueDate"],
                    self.flag,
                    "Viac",  # payee
                    "Interest",
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return bean_transactions

    def Fees(self, fees):
        bean_transactions = []
        for idx, row in fees.iterrows():
            currency = "CHF"
            amount_ = amount.Amount(row["amountInChf"], currency)

            # make the postings, two for fees
            postings = [
                data.Posting(
                    self.getFeesAccount(currency, row["source_account"]), -amount_, None, None, None, None
                ),
                data.Posting(
                    self.getLiquidityAccount(currency, row["source_account"]), amount_, None, None, None, None
                ),
            ]

            metadata = {"source_account": row["source_account"]}
            document = row["documentNumber"]
            if document != None and document != "nan":
                metadata["link"] = self.getDocumentUrl(document)

            meta = data.new_metadata("fee", idx, metadata)

            bean_transactions.append(
                data.Transaction(
                    meta,
                    row["valueDate"],
                    self.flag,
                    "Viac",  # payee
                    "Fees",
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return bean_transactions

    def Deposits(self, deposits):
        bean_transactions = []
        if len(self.deposit_account) == 0:  # control this from the config file
            return []
        for idx, row in deposits.iterrows():
            currency = "CHF"
            amount_ = amount.Amount(row["amountInChf"], currency)

            # make the postings. two for deposits
            postings = [
                data.Posting(self.deposit_account, -amount_, None, None, None, None),
                data.Posting(
                    self.getLiquidityAccount(currency, row["source_account"]), amount_, None, None, None, None
                ),
            ]

            metadata = {"source_account": row["source_account"]}
            document = row["documentNumber"]
            if document != None and document != "nan":
                metadata["link"] = self.getDocumentUrl(document)

            meta = data.new_metadata("deposit/withdrawal", idx, metadata)

            bean_transactions.append(
                data.Transaction(
                    meta,
                    row["valueDate"],
                    self.flag,
                    "self",  # payee
                    "deposit / withdrawal",
                    set(['s3a-deposit']),
                    data.EMPTY_SET,
                    postings,
                )
            )
        return bean_transactions

    def Dividends(self, dividends):
        bean_transactions = []
        for idx, row in dividends.iterrows():
            currency = "CHF"
            share = self.share_lookup.get(row["description"])
            if share is None:
                logger.error(
                    f"Could not fetch share '{row['description']}' from supplied lookup {list(self.share_lookup.keys())}"
                )
                continue
            symbol = share["symbol"]
            amount_div = amount.Amount(row["amountInChf"], currency)

            postings = [
                data.Posting(
                    self.getDivIncomeAccount(currency, symbol),
                    -amount_div,
                    None,
                    None,
                    None,
                    None,
                ),
                data.Posting(
                    self.getLiquidityAccount(currency, row["source_account"]),
                    amount_div,
                    None,
                    None,
                    None,
                    None,
                ),
            ]

            metadict = {
                "isin": share["isin"],
                "source_account": row["source_account"]
            }

            meta = data.new_metadata("dividend", 0, metadict)

            bean_transactions.append(
                data.Transaction(
                    meta,  # could add div per share, ISIN,....
                    row["valueDate"],
                    self.flag,
                    share["isin"],  # payee
                    f"Dividend {symbol}; {row['description']}",
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )

        return bean_transactions

    def Balances(self, df):
        # generate Balance statements for every latest transaction
        transaction = df[df["valueDate"] == df["valueDate"].max()].iloc[0]

        currency = "CHF"
        amount_ = amount.Amount(transaction["balanceAfterBooking"], currency)
        meta = data.new_metadata("balance", 0, {"source_account": transaction["source_account"]})

        return [
            data.Balance(
                meta,
                transaction["valueDate"] + timedelta(days=1),
                self.getLiquidityAccount(currency, transaction["source_account"]),
                amount_,
                None,
                None,
            )
        ]
