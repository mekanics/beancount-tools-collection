"""
This is a beancount importer for Interactive Brokers.
Setup:
1) have a running beancount system
2) activate IB FlexQuery with the entries specified in the supported transaction types below
3) create an ibkr.yaml file with your IBKR FlexQuery credentials (see Configuration File section)
4) in the config.py file, specify the IBKRImporter with the ibkr.yaml file location
5) run 'bean-extract config.py ibkr.yaml -f mainLedgerFile.bean'

Configuration File (ibkr.yaml):
The ibkr.yaml file must contain the following properties:

token: 12345678912345678912345    # Your IB FlexQuery token (string of digits)
queryId: 123456                  # Your FlexQuery ID number
baseCcy: 'USD'                   # Base currency for your account

To obtain these credentials:
1. Log into your Interactive Brokers account
2. Go to Reports > Flex Queries
3. Create a new FlexQuery or use an existing one
4. The FlexQuery must include: CashReport, Trades, CashTransactions, and CorporateActions
5. Generate a token for the FlexQuery - this becomes your 'token' value
6. Note the Query ID - this becomes your 'queryId' value
"""

import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import warnings
import pickle
import re
import numpy as np

import yaml
from os import path
from ibflex import client, parser, Types
from ibflex.enums import CashAction, BuySell
from ibflex.client import ResponseCodeError
from beangulp.importer import Importer

from beanquery import query
from beancount.parser import options
from beancount.core import data, amount
from beancount.core.number import D
from beancount.core.number import Decimal
from beancount.core import position
from beancount.core.number import MISSING

from loguru import logger


class IBKRImporter(Importer):
    """
    Beancount Importer for the Interactive Brokers XML FlexQueries

    This importer processes Interactive Brokers FlexQuery XML files and converts them into
    Beancount transactions. It supports multiple accounts within a single FlexQuery and uses
    the account aliases defined in Interactive Brokers to structure the account hierarchy.

    Supported Transaction Types:
    - Stock trades (buy/sell)
    - Forex trades
    - Dividends and withholding taxes
    - Interest payments
    - Fees
    - Deposits/withdrawals
    - Corporate actions (forward and reverse stock splits)

    Account Alias Handling:
    - The account alias is automatically extracted from each FlexStatement's AccountInformation
    - Spaces in aliases are replaced with hyphens (e.g., "Long Term" -> "Long-Term")
    - First letter is always capitalized
    - The formatted alias is inserted into the account hierarchy, e.g.:
        Assets:Invest:IB:Long-Term:VTI
        Income:Dividends:IB:Long-Term:USD
        Assets:Invest:IB:Casino:MSTY

    Configurable Account Types:
    - cashAccountType: What to replace stockAccountType with for cash accounts (default: "Cash")
    - stockAccountType: The account type to replace for cash accounts (default: "Stocks")
    - Example configurations:
      
      Default behavior (stockAccountType="Stocks", cashAccountType="Cash"):
      - Mainaccount="Assets:Stocks:IB" -> Cash account: "Assets:Cash:IB"
      
      Custom configuration (stockAccountType="Invest", cashAccountType="Liquidity"):
      - Mainaccount="Assets:Invest:IB" -> Cash account: "Assets:Liquidity:IB"
      
      GnuCash-style (stockAccountType="Investment", cashAccountType="Current"):
      - Mainaccount="Assets:Investment:IB" -> Cash account: "Assets:Current:IB"

    Corporate Actions:
    - Forward stock splits (FS): Additional shares are added with zero cost basis
    - Reverse stock splits (RS): Old shares are removed and new consolidated shares added
    - Split ratio is extracted from the action description (e.g., "SPLIT 4 FOR 1")
    - Entries are grouped by actionID to match paired removal/addition entries

    Example account structures:
    Income:Stocks:Interactive-Brokers:Long-Term:VT:Div
    Assets:Invest:IB:Long-Term:USD
    Assets:Invest:IB:Casino:CHF
    """

    # Income:Stocks:Interactive-Brokers:Long-Term:VT:Div

    def __init__(
        self,
        Mainaccount=None,  # for example Assets:Invest:IB
        DivAccount=None,  # for example Income:Dividends:IB
        WHTAccount=None,  # for example Expenses:Stocks:IB:WhT
        PnLAccount=None,  # for example Income:Stocks:IB:PnL
        FeesAccount=None,  # for example Expenses:Stocke:IB:Fees
        currency="CHF",
        interestSuffix="Interest",
        fpath=None,  #
        depositAccount="",
        suppressClosedLotPrice=False,
        configFile="ibkr.yaml",
        # Configurable account type mappings
        cashAccountType="Cash",  # What to replace with for cash accounts (e.g., "Stocks" -> "Cash")
        stockAccountType="Stocks",  # What to replace for stock accounts (default: "Stocks")
    ):
        self.Mainaccount = Mainaccount  # main IB account in beancount
        self.DivAccount = DivAccount  # main IB dividend account in beancount
        self.WHTAccount = WHTAccount  # Withholding account
        self.PnLAccount = PnLAccount
        self.FeesAccount = FeesAccount
        self.currency = currency  # main currency of IB account
        self.interestSuffix = interestSuffix
        self.fpath = fpath  # optional file path specification,
        # if flex query should not be used online (loading time...)
        # Cash deposits are usually already covered
        self.depositAccount = depositAccount
        # by checkings account statements. If you want anyway the
        # deposit transactions, provide a True value
        self.suppressClosedLotPrice = suppressClosedLotPrice
        self.flag = "*"
        self.configFile = configFile
        self._account_alias = None  # Will be set when processing statements
        
        # Configurable account type mappings
        self.cashAccountType = cashAccountType
        self.stockAccountType = stockAccountType

    def account(self, filepath):
        """The account to associate with this importer."""
        return self.Mainaccount

    def identify(self, filepath):
        result = self.configFile == path.basename(filepath)
        logger.info(
            f"identify assertion for ibkr importer and file '{filepath}': {result}"
        )
        return result

    # def name(self) -> str:
    #     return self.configFile

    def getLiquidityAccount(self, currency):
        # Assets:Invest:IB:USD
        return ":".join(
            filter(
                None,
                [
                    self.Mainaccount.replace(self.stockAccountType, self.cashAccountType),
                    self._account_alias,
                    currency,
                ],
            )
        )

    def getDivIncomeAccount(self, currency, symbol):
        # Income:Dividend:IB:USD
        return ":".join(
            filter(
                None,
                [self.DivAccount, self._account_alias, currency],
            )
        )

    def getInterestIncomeAcconut(self, currency):
        # Income:Invest:IB:USD
        # Convert Assets:Invest:IB to Income:Invest:IB
        account_parts = self.Mainaccount.split(":")
        if account_parts[0] == "Assets":
            account_parts[0] = "Income"
        income_base = ":".join(account_parts)
        
        return ":".join(
            filter(
                None,
                [
                    income_base,
                    self._account_alias,
                    self.interestSuffix,
                    currency,
                ],
            )
        )

    def getAssetAccount(self, symbol):
        # Assets:Invest:IB:VTI
        return ":".join(filter(None, [self.Mainaccount, self._account_alias, symbol]))

    def getWHTAccount(self):
        # Expenses:Invest:IB
        # return ":".join([self.WHTAccount, symbol])
        return self.WHTAccount

    def getFeesAccount(self, currency):
        return ":".join([self.FeesAccount, currency])

    def getPNLAccount(self, _):
        return self.PnLAccount

    def file_account(self, _):
        return self.Mainaccount

    def _format_account_alias(self, alias):
        """Format the account alias to be Beancount compliant:
        - Replace spaces with hyphens
        - Ensure first letter is uppercase
        """
        if not alias:
            return None
        # Replace spaces with hyphens and capitalize first letter
        formatted = alias.replace(" ", "-")
        return formatted[0].upper() + formatted[1:] if formatted else formatted

    def _get_cost_basis_from_existing(self, account, symbol, as_of_date=None):
        """
        Query existing beancount entries to get the total cost basis for a symbol.
        
        Args:
            account: The beancount account holding the position
            symbol: The commodity/stock symbol
            as_of_date: Optional date to query as of (defaults to all entries)
            
        Returns:
            Tuple of (total_cost_basis, total_units, cost_currency) or (None, None, None) if not found
        """
        if not self._existing_entries:
            logger.warning("No existing entries available for cost basis lookup")
            return None, None, None
        
        try:
            # Build query to get holdings with cost basis
            query_str = f"""
                SELECT sum(units(position)) as units, 
                       sum(cost(position)) as cost_basis
                FROM open_at(Filtered, date('{as_of_date}')) if '{as_of_date}' else open
                WHERE account = '{account}'
                AND currency(units(position)) = '{symbol}'
            """
            
            # Simpler approach: iterate through entries to calculate cost basis
            from beancount.core import inventory
            from beancount.core import realization
            
            # Create a realization of the accounts
            real_account = realization.realize(self._existing_entries)
            
            # Navigate to the specific account
            account_parts = account.split(':')
            current = real_account
            for part in account_parts:
                if part in current:
                    current = current[part]
                else:
                    logger.info(f"Account {account} not found in existing entries")
                    return None, None, None
            
            # Get the balance (inventory) for this account
            balance = current.balance
            
            # Find the position for our symbol
            for pos in balance:
                if pos.units.currency == symbol:
                    total_units = pos.units.number
                    if pos.cost and pos.cost.number is not None:
                        total_cost = pos.units.number * pos.cost.number
                        cost_currency = pos.cost.currency
                        logger.info(f"Found cost basis for {symbol}: {total_units} units, {total_cost} {cost_currency}")
                        return total_cost, total_units, cost_currency
            
            logger.info(f"No position found for {symbol} in {account}")
            return None, None, None
            
        except Exception as e:
            logger.warning(f"Error querying cost basis: {e}")
            return None, None, None

    def extract(self, filepath, existing=None):
        # the actual processing of the flex query
        
        # Store existing entries for cost basis lookup in corporate actions
        self._existing_entries = existing

        # get the IBKR creentials ready
        try:
            with open(filepath, "r") as f:
                config = yaml.safe_load(f)
                token = config["token"]
                queryId = config["queryId"]
        except BaseException:
            warnings.warn(
                f"cannot read IBKR credentials file. Check filepath. '{filepath}'"
            )
            return []

        if self.fpath is None:
            # get the report from IB. might take a while, when IB is queuing due to
            # traffic
            try:
                # try except in case of connection interrupt
                # Warning: queries sometimes take a few minutes until IB provides
                # the data due to busy servers
                response = client.download(token, queryId)
                statement = parser.parse(response)
            except ResponseCodeError as E:
                print(E)
                print("aborting.")
                return []
            except Exception as e:
                if hasattr(e, "message"):
                    warnings.warn(str(e.message))
                else:
                    warnings.warn(str(e))
                warnings.warn("could not fetch IBKR Statement. exiting.")
                # another option would be to try again
                return []
            assert isinstance(statement, Types.FlexQueryResponse)
        else:
            print("**** loading from pickle")
            with open(self.fpath, "rb") as pf:
                statement = pickle.load(pf)

        all_transactions = []
        
        # Process each FlexStatement
        for flex_stmt in statement.FlexStatements:
            # Get account alias for this statement
            if hasattr(flex_stmt, 'AccountInformation'):
                raw_alias = flex_stmt.AccountInformation.acctAlias
                self._account_alias = self._format_account_alias(raw_alias)
                logger.info(f"Processing statement for account alias: {self._account_alias} (original: {raw_alias})")
            else:
                warnings.warn(f"Could not find account alias in FlexStatement for account {flex_stmt.accountId}")
                self._account_alias = None

            # relevant items from report
            reports = ["CashReport", "Trades", "CashTransactions", "CorporateActions"]
            
            tabs = {
                report: pd.DataFrame(
                    [
                        {key: val for key, val in entry.__dict__.items()}
                        for entry in flex_stmt.__dict__.get(report, [])
                    ]
                )
                for report in reports
            }

            # get single dataFrames
            ct = tabs["CashTransactions"]
            tr = tabs["Trades"]
            cr = tabs["CashReport"]
            ca = tabs["CorporateActions"]

            # throw out IBKR jitter, mostly None
            ct.drop(columns=[col for col in ct if all(ct[col].isnull())], inplace=True)
            tr.drop(columns=[col for col in tr if all(tr[col].isnull())], inplace=True)
            cr.drop(columns=[col for col in cr if all(cr[col].isnull())], inplace=True)
            ca.drop(columns=[col for col in ca if all(ca[col].isnull())], inplace=True)
            
            # Process transactions for this statement
            transactions = self.Trades(tr) + self.CashTransactions(ct) + self.Balances(cr) + self.CorporateActions(ca)
            all_transactions.extend(transactions)

        return all_transactions

    def CashTransactions(self, ct):
        """
        This function turns the cash transactions table into beancount transactions
        for dividends, Witholding Tax, Cash deposits (if the flag is set in the
        ConfigIBKR.py) and Interests.
        arg ct: pandas DataFrame with the according data
        returns: list of Beancount transactions
        """
        if len(ct) == 0:  # catch case of empty dataframe
            return []

        # First, extract all dividend and WHT entries
        div_wht = ct[
            ct["type"].map(
                lambda t: t == CashAction.DIVIDEND 
                or t == CashAction.PAYMENTINLIEU 
                or t == CashAction.WHTAX
            )
        ].copy()

        # Process combined dividend and WHT transactions
        div_wht_transactions = self.ProcessDividendsAndWHT(div_wht) if not div_wht.empty else []

        # Process other transaction types
        dep = ct[ct["type"] == CashAction.DEPOSITWITHDRAW].copy()
        deps = self.Deposits(dep) if len(dep) > 0 else []

        int_ = ct[
            ct["type"].map(
                lambda t: t == CashAction.BROKERINTRCVD or t == CashAction.BROKERINTPAID
            )
        ].copy()
        ints = self.Interest(int_) if len(int_) > 0 else []

        fee = ct[ct["type"] == CashAction.FEES].copy()
        fees = self.Fee(fee) if len(fee) > 0 else []

        return div_wht_transactions + deps + ints + fees

    def ProcessDividendsAndWHT(self, div_wht):
        """Process Dividend and WHT entries together.
        
        Args:
            div_wht: DataFrame containing both dividend and WHT entries
        
        Returns:
            List of beancount transactions
        """
        if div_wht.empty:
            return []

        # Extract key information for matching
        div_wht['div_rate'] = div_wht['description'].apply(
            lambda d: re.search(r'USD ([\d.]+) PER SHARE', d).group(1) 
            if re.search(r'USD ([\d.]+) PER SHARE', d) else None
        )
        div_wht['isin'] = div_wht['description'].apply(
            lambda d: re.search(r'\((.*?)\)', d).group(1) 
            if re.search(r'\((.*?)\)', d) else None
        )
        div_wht['is_correction'] = div_wht['description'].str.contains('CORRECTION', case=False)

        # Create a matching key for grouping related entries
        div_wht['group_key'] = div_wht.apply(
            lambda row: f"{row['symbol']}_{row['div_rate']}_{row['reportDate']}",
            axis=1
        )

        transactions = []
        
        # Group by the matching key to process related entries together
        for group_key, group in div_wht.groupby('group_key'):
            if group.empty:
                continue

            symbol = group.iloc[0]['symbol']
            date = group.iloc[0]['reportDate']
            currency = group.iloc[0]['currency']
            div_rate = group.iloc[0]['div_rate']
            isin = group.iloc[0]['isin']

            # Separate dividend and WHT entries
            div_entries = group[group['type'].map(
                lambda t: t == CashAction.DIVIDEND or t == CashAction.PAYMENTINLIEU
            )]
            wht_entries = group[group['type'] == CashAction.WHTAX]

            # Calculate totals
            total_div = sum(div_entries['amount'])
            total_wht = sum(wht_entries['amount'])

            # Skip if we don't have both dividend and WHT (might be in different reports)
            if div_entries.empty or wht_entries.empty:
                logger.info(
                    f"Incomplete dividend group for {symbol} on {date}: "
                    f"Dividend entries: {len(div_entries)}, WHT entries: {len(wht_entries)}"
                )
                # Process them individually for now
                if not div_entries.empty:
                    transactions.extend(self._process_single_dividend(div_entries))
                if not wht_entries.empty:
                    transactions.extend(self._process_single_wht(wht_entries))
                continue

            # Create metadata with all related entries
            meta = data.new_metadata("dividend", 0, {
                "symbol": symbol,
                "isin": isin,
                "dividend_rate": div_rate,
                "dividend_entries": "\n".join(div_entries['description'].tolist()),
                "wht_entries": "\n".join(wht_entries['description'].tolist()),
                "is_correction": "1" if any(group['is_correction']) else "0",
                "correction_group": group_key
            })

            # Create postings
            postings = [
                # Dividend income posting
                data.Posting(
                    self.getDivIncomeAccount(currency, symbol),
                    minus(amount.Amount(total_div, currency)),
                    None, None, None, None
                ),
                # WHT posting
                data.Posting(
                    self.getWHTAccount(),
                    minus(amount.Amount(total_wht, currency)),
                    None, None, None, None
                ),
                # Net cash posting
                data.Posting(
                    self.getLiquidityAccount(currency),
                    amount.Amount(total_div + total_wht, currency),
                    None, None, None, None
                ),
            ]

            # Create transaction
            narration = (
                f"Dividend {symbol} ({div_rate} USD per share)"
                + (" - Correction" if any(group['is_correction']) else "")
            )

            transactions.append(
                data.Transaction(
                    meta,
                    date,
                    self.flag,
                    symbol,  # payee
                    narration,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )

            logger.info(
                f"Processed dividend group: {date} {symbol} "
                f"(rate: {div_rate}): {len(group)} entries in single transaction"
            )

        return transactions

    def _process_single_dividend(self, div_entries):
        """Process dividend entries that don't have matching WHT entries."""
        transactions = []
        for _, row in div_entries.iterrows():
            currency = row["currency"]
            symbol = row["symbol"]
            amount_ = amount.Amount(row["amount"], currency)
            
            meta = data.new_metadata("dividend", 0, {
                "symbol": symbol,
                "isin": row.get('isin'),
                "original_description": row["description"],
                "awaiting_wht": True  # Flag that this might be matched later
            })

            postings = [
                data.Posting(
                    self.getDivIncomeAccount(currency, symbol),
                    minus(amount_),
                    None, None, None, None
                ),
                data.Posting(
                    self.getLiquidityAccount(currency),
                    amount_,
                    None, None, None, None
                ),
            ]

            transactions.append(
                data.Transaction(
                    meta,
                    row["reportDate"],
                    self.flag,
                    symbol,
                    f"Dividend {symbol} (awaiting WHT)",
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return transactions

    def _process_single_wht(self, wht_entries):
        """Process WHT entries that don't have matching dividend entries."""
        transactions = []
        for _, row in wht_entries.iterrows():
            currency = row["currency"]
            symbol = row["symbol"]
            amount_ = amount.Amount(row["amount"], currency)
            
            meta = data.new_metadata("WHT", 0, {
                "symbol": symbol,
                "isin": row.get('isin'),
                "original_description": row["description"],
                "awaiting_dividend": True  # Flag that this might be matched later
            })

            postings = [
                data.Posting(
                    self.getWHTAccount(),
                    minus(amount_),
                    None, None, None, None
                ),
                data.Posting(
                    self.getLiquidityAccount(currency),
                    amount_,
                    None, None, None, None
                ),
            ]

            transactions.append(
                data.Transaction(
                    meta,
                    row["reportDate"],
                    self.flag,
                    symbol,
                    f"WHT {symbol} (awaiting dividend)",
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return transactions

    def Fee(self, fee):
        # calculates fees from IBKR data
        feeTransactions = []
        for idx, row in fee.iterrows():
            currency = row["currency"]
            amount_ = amount.Amount(row["amount"], currency)
            text = row["description"]
            month = ""

            try:
                month = re.findall("\\w{3} \\d{4}", text)[0]
            except:
                # just ignore
                warnings.warn(f"No month found in '{text}'")

            # make the postings, two for fees
            postings = [
                data.Posting(
                    self.getFeesAccount(currency), -amount_, None, None, None, None
                ),
                data.Posting(
                    self.getLiquidityAccount(currency), amount_, None, None, None, None
                ),
            ]
            meta = data.new_metadata(__file__, 0, {})  # actually no metadata
            feeTransactions.append(
                data.Transaction(
                    meta,
                    row["reportDate"],
                    self.flag,
                    "IB",  # payee
                    " ".join(["Fee", currency, month]).strip(),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return feeTransactions

    def Interest(self, int_):
        # calculates interest payments from IBKR data
        intTransactions = []
        for idx, row in int_.iterrows():
            currency = row["currency"]
            amount_ = amount.Amount(row["amount"], currency)
            text = row["description"]
            month = re.findall("\\w{3}-\\d{4}", text)[0]

            # make the postings, two for interest payments
            # received and paid interests are booked on the same account
            postings = [
                data.Posting(
                    self.getInterestIncomeAcconut(currency),
                    -amount_,
                    None,
                    None,
                    None,
                    None,
                ),
                data.Posting(
                    self.getLiquidityAccount(currency), amount_, None, None, None, None
                ),
            ]
            meta = data.new_metadata("Interest", 0)
            intTransactions.append(
                data.Transaction(
                    meta,  # could add div per share, ISIN,....
                    row["reportDate"],
                    self.flag,
                    "IB",  # payee
                    " ".join(["Interest ", currency, month]),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return intTransactions

    def Deposits(self, dep):
        # creates deposit transactions from IBKR Data

        depTransactions = []
        # assumes you figured out how to deposit/ withdrawal without fees
        if len(self.depositAccount) == 0:  # control this from the config file
            return []
        for idx, row in dep.iterrows():
            currency = row["currency"]
            amount_ = amount.Amount(row["amount"], currency)

            # make the postings. two for deposits
            postings = [
                data.Posting(self.depositAccount, -amount_, None, None, None, None),
                data.Posting(
                    self.getLiquidityAccount(currency), amount_, None, None, None, None
                ),
            ]
            meta = data.new_metadata("deposit/withdrawel", 0)
            depTransactions.append(
                data.Transaction(
                    meta,  # could add div per share, ISIN,....
                    row["reportDate"],
                    self.flag,
                    "self",  # payee
                    "deposit / withdrawal",
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return depTransactions

    def Trades(self, tr):
        """
        This function turns the IBKR Trades table into beancount transactions
        for Trades
        arg tr: pandas DataFrame with the according data
        returns: list of Beancount transactions
        """
        if len(tr) == 0:  # catch the case of no transactions
            return []
        # forex transactions
        fx = tr[tr["symbol"].apply(isForex)]
        # Stocks transactions
        stocks = tr[~tr["symbol"].apply(isForex)]

        trTransactions = self.Forex(fx) + self.Stocktrades(stocks)

        return trTransactions

    def Forex(self, fx):
        # returns beancount transactions for IBKR forex transactions

        fxTransactions = []
        for idx, row in fx.iterrows():
            symbol = row["symbol"]
            curr_prim, curr_sec = getForexCurrencies(symbol)
            currency_IBcommision = row["ibCommissionCurrency"]
            proceeds = amount.Amount(round(row["proceeds"], 2), curr_sec)
            quantity = amount.Amount(round(row["quantity"], 2), curr_prim)
            price = amount.Amount(row["tradePrice"], curr_sec)
            commission = amount.Amount(
                round(row["ibCommission"], 2), currency_IBcommision
            )
            buysell = row["buySell"].name

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
                    self.getLiquidityAccount(curr_prim),
                    quantity,
                    None,
                    price,
                    None,
                    None,
                ),
                data.Posting(
                    self.getLiquidityAccount(curr_sec), proceeds, None, None, None, None
                ),
                data.Posting(
                    self.getLiquidityAccount(currency_IBcommision),
                    commission,
                    None,
                    None,
                    None,
                    None,
                ),
                data.Posting(
                    self.getFeesAccount(currency_IBcommision),
                    minus(commission),
                    None,
                    None,
                    None,
                    None,
                ),
            ]

            fxTransactions.append(
                data.Transaction(
                    data.new_metadata("FX Transaction", 0),
                    row["tradeDate"],
                    self.flag,
                    symbol,  # payee
                    " ".join([buysell, quantity.to_string(), "@", price.to_string()]),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return fxTransactions

    def Stocktrades(self, stocks):
        # return the stocks transactions

        stocktrades = stocks[stocks["levelOfDetail"] == "EXECUTION"]  # actual trades
        buy = stocktrades[
            (stocktrades["buySell"] == BuySell.BUY)
            | (  # purchases, including cancelled ones
                stocktrades["buySell"] == BuySell.CANCELBUY
            )
        ]  # and the cancellation transactions to keep balance
        sale = stocktrades[
            (stocktrades["buySell"] == BuySell.SELL)
            | (  # sales, including cancelled ones
                stocktrades["buySell"] == BuySell.CANCELSELL
            )
        ]  # and the cancellation transactions to keep balance
        # closed lots; keep index to match with sales
        lots = stocks[stocks["levelOfDetail"] == "CLOSED_LOT"]

        stockTransactions = self.Panic(sale, lots) + self.Shopping(buy)

        return stockTransactions

    def Shopping(self, buy):
        # let's go shopping!!

        Shoppingbag = []
        for idx, row in buy.iterrows():
            # continue # debugging
            currency = row["currency"]
            currency_IBcommision = row["ibCommissionCurrency"]
            symbol = row["symbol"]
            proceeds = amount.Amount(row["proceeds"].__round__(2), currency)
            commission = amount.Amount(
                (row["ibCommission"].__round__(2)), currency_IBcommision
            )
            quantity = amount.Amount(row["quantity"], symbol)
            price = amount.Amount(row["tradePrice"], currency)
            text = row["description"]

            number_per = D(row["tradePrice"])
            currency_cost = currency
            cost = position.CostSpec(
                number_per=price.number,
                number_total=None,
                currency=currency,
                date=row["tradeDate"],
                label=None,
                merge=False,
            )

            postings = [
                data.Posting(
                    self.getAssetAccount(symbol), quantity, cost, None, None, None
                ),
                data.Posting(
                    self.getLiquidityAccount(currency), proceeds, None, None, None, None
                ),
                data.Posting(
                    self.getLiquidityAccount(currency_IBcommision),
                    commission,
                    None,
                    None,
                    None,
                    None,
                ),
                data.Posting(
                    self.getFeesAccount(currency_IBcommision),
                    minus(commission),
                    None,
                    None,
                    None,
                    None,
                ),
            ]

            Shoppingbag.append(
                data.Transaction(
                    data.new_metadata("Buy", 0),
                    row["dateTime"].date(),
                    self.flag,
                    symbol,  # payee
                    " ".join(["BUY", quantity.to_string(), "@", price.to_string()]),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return Shoppingbag

    def Panic(self, sale, lots):
        # OMG, IT is happening!!

        Doom = []
        for idx, row in sale.iterrows():
            # continue # debugging
            currency = row["currency"]
            currency_IBcommision = row["ibCommissionCurrency"]
            symbol = row["symbol"]
            proceeds = amount.Amount(row["proceeds"].__round__(2), currency)
            commission = amount.Amount(
                (row["ibCommission"].__round__(2)), currency_IBcommision
            )
            quantity = amount.Amount(row["quantity"], symbol)
            price = amount.Amount(row["tradePrice"], currency)
            text = row["description"]
            date = row["dateTime"].date()
            number_per = D(row["tradePrice"])
            currency_cost = currency

            # Closed lot rows (potentially multiple) follow sell row
            lotpostings = []
            sum_lots_quantity = 0
            # mylots: lots closed by sale 'row'
            # symbol must match; begin at the row after the sell row
            # we do not know the number of lot rows; stop iteration if quantity is enough
            mylots = lots[(lots["symbol"] == row["symbol"]) & (lots.index > idx)]
            for li, clo in mylots.iterrows():
                sum_lots_quantity += clo["quantity"]
                if sum_lots_quantity > -row["quantity"]:
                    # oops, too many lots (warning issued below)
                    break

                cost = position.CostSpec(
                    number_per=(
                        Decimal(0)
                        if self.suppressClosedLotPrice
                        else round(clo["tradePrice"], 2)
                    ),
                    number_total=None,
                    currency=clo["currency"],
                    date=clo["openDateTime"].date(),
                    label=None,
                    merge=False,
                )

                lotpostings.append(
                    data.Posting(
                        self.getAssetAccount(symbol),
                        amount.Amount(-clo["quantity"], clo["symbol"]),
                        cost,
                        price,
                        None,
                        None,
                    )
                )

                if sum_lots_quantity == -row["quantity"]:
                    # Exact match is expected:
                    # all lots found for this sell transaction
                    break

            if sum_lots_quantity != -row["quantity"]:
                warnings.warn(f"Lots matching failure: sell index={idx}")

            postings = (
                [
                    # data.Posting(self.getAssetAccount(symbol),  # this first posting is probably wrong
                    # quantity, None, price, None, None),
                    data.Posting(
                        self.getLiquidityAccount(currency),
                        proceeds,
                        None,
                        None,
                        None,
                        None,
                    )
                ]
                + lotpostings
                + [
                    data.Posting(
                        self.getPNLAccount(symbol), None, None, None, None, None
                    ),
                    data.Posting(
                        self.getLiquidityAccount(currency_IBcommision),
                        commission,
                        None,
                        None,
                        None,
                        None,
                    ),
                    data.Posting(
                        self.getFeesAccount(currency_IBcommision),
                        minus(commission),
                        None,
                        None,
                        None,
                        None,
                    ),
                ]
            )

            Doom.append(
                data.Transaction(
                    data.new_metadata("Buy", 0),
                    date,
                    self.flag,
                    symbol,  # payee
                    " ".join(["SELL", quantity.to_string(), "@", price.to_string()]),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return Doom

    def Balances(self, cr):
        # generate Balance statements from IBKR Cash reports
        # balances
        crTransactions = []
        for idx, row in cr.iterrows():
            currency = row["currency"]
            if currency == "BASE_SUMMARY":
                continue  # this is a summary balance that is not needed for beancount
            amount_ = amount.Amount(row["endingCash"].__round__(2), currency)

            # make the postings. two for deposits
            postings = [
                data.Posting(self.depositAccount, -amount_, None, None, None, None),
                data.Posting(
                    self.getLiquidityAccount(currency), amount_, None, None, None, None
                ),
            ]
            meta = data.new_metadata("balance", 0)

            crTransactions.append(
                data.Balance(
                    meta,
                    row["toDate"] + timedelta(days=1),  # see tariochtools EC imp.
                    self.getLiquidityAccount(currency),
                    amount_,
                    None,
                    None,
                )
            )
        return crTransactions

    def CorporateActions(self, ca):
        """
        Process corporate actions from IBKR data, including forward and reverse stock splits.
        
        Args:
            ca: pandas DataFrame with corporate actions data
            
        Returns:
            List of beancount transactions for corporate actions
        """
        if len(ca) == 0:  # catch case of empty dataframe
            return []

        caTransactions = []
        
        # Filter for DETAIL level only (skip SUMMARY entries which are duplicates)
        ca_detail = ca[ca["levelOfDetail"] == "DETAIL"] if "levelOfDetail" in ca.columns else ca
        
        # Process forward splits (FS)
        forward_splits = ca_detail[ca_detail["type"].astype(str).str.contains("FS", case=False, na=False)].copy()
        caTransactions.extend(self._process_forward_splits(forward_splits))
        
        # Process reverse splits (RS)
        reverse_splits = ca_detail[ca_detail["type"].astype(str).str.contains("RS", case=False, na=False)].copy()
        caTransactions.extend(self._process_reverse_splits(reverse_splits))
        
        return caTransactions

    def _process_forward_splits(self, splits):
        """
        Process forward stock splits from IBKR data.
        
        Args:
            splits: pandas DataFrame with forward split corporate actions
            
        Returns:
            List of beancount transactions for forward splits
        """
        if len(splits) == 0:
            return []

        transactions = []
        
        for idx, row in splits.iterrows():
            symbol = row["symbol"]
            currency = row["currency"]
            split_quantity = amount.Amount(D(str(row["quantity"])), symbol)
            date = row["dateTime"].date() if hasattr(row["dateTime"], 'date') else row["reportDate"]
            
            # Extract split ratio from description (e.g., "SPLIT 4 FOR 1")
            description = row["actionDescription"]
            split_match = re.search(r'SPLIT (\d+) FOR (\d+)', description)
            if split_match:
                new_shares = split_match.group(1)
                old_shares = split_match.group(2)
                split_ratio = f"{new_shares}:{old_shares}"
            else:
                split_ratio = "unknown"
            
            # Create metadata
            meta = data.new_metadata("stock_split", 0, {
                "symbol": symbol,
                "isin": row.get("isin", ""),
                "split_ratio": split_ratio,
                "split_type": "forward",
                "split_description": description,
            })
            
            # For stock splits, we receive additional shares at zero cost
            # The cost basis is adjusted automatically by beancount when using zero cost
            cost = position.CostSpec(
                number_per=D('0'),  # Zero cost for stock split shares
                number_total=None,
                currency=currency,
                date=date,
                label=None,
                merge=False,
            )
            
            # Create postings - only one posting needed for stock splits
            # Beancount will automatically balance with an Equity account
            postings = [
                data.Posting(
                    self.getAssetAccount(symbol),
                    split_quantity,
                    cost,
                    None,
                    None,
                    None,
                ),
            ]
            
            # Create transaction
            narration = f"Stock split {symbol} ({split_ratio})"
            
            transactions.append(
                data.Transaction(
                    meta,
                    date,
                    self.flag,
                    symbol,  # payee
                    narration,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
            
            logger.info(f"Processed forward stock split: {date} {symbol} {split_ratio} (+{split_quantity})")
        
        return transactions

    def _process_reverse_splits(self, splits):
        """
        Process reverse stock splits from IBKR data.
        
        Reverse splits have two paired entries with the same actionID:
        - Removal entry: negative quantity (old shares removed)
        - Addition entry: positive quantity (new consolidated shares added)
        
        Args:
            splits: pandas DataFrame with reverse split corporate actions
            
        Returns:
            List of beancount transactions for reverse splits
        """
        if len(splits) == 0:
            return []

        transactions = []
        
        # Group by actionID to match removal/addition pairs
        for action_id, group in splits.groupby("actionID"):
            # Identify removal (negative qty) and addition (positive qty) entries
            removal = group[group["quantity"] < 0]
            addition = group[group["quantity"] > 0]
            
            if removal.empty or addition.empty:
                logger.warning(f"Incomplete reverse split pair for actionID {action_id}")
                continue
            
            # Extract data from both entries
            removal_row = removal.iloc[0]
            addition_row = addition.iloc[0]
            
            old_symbol = removal_row["symbol"]
            new_symbol = addition_row["symbol"]
            old_qty = abs(removal_row["quantity"])
            new_qty = addition_row["quantity"]
            currency = addition_row["currency"]
            
            # Parse date - handle both dateTime formats
            date_value = addition_row["dateTime"]
            if hasattr(date_value, 'date'):
                date = date_value.date()
            elif isinstance(date_value, str) and ";" in date_value:
                # Handle format like "20251205;202500"
                date = datetime.strptime(date_value.split(";")[0], "%Y%m%d").date()
            else:
                date = addition_row["reportDate"]
            
            # Extract split ratio from description (e.g., "SPLIT 1 FOR 5")
            description = addition_row["actionDescription"]
            split_match = re.search(r'SPLIT (\d+) FOR (\d+)', description)
            if split_match:
                new_shares = split_match.group(1)
                old_shares = split_match.group(2)
                split_ratio = f"{new_shares}:{old_shares}"
            else:
                split_ratio = "unknown"
            
            # Try to get cost basis from existing entries
            asset_account = self.getAssetAccount(new_symbol)
            total_cost, total_units, cost_currency = self._get_cost_basis_from_existing(
                asset_account, new_symbol
            )
            
            # Calculate new per-share cost if we found the original cost basis
            cost_basis_found = total_cost is not None and total_units is not None
            if cost_basis_found and total_cost is not None:
                # Transfer the total cost basis to the new shares
                new_cost_per_share = D(str(round(total_cost / D(str(new_qty)), 6)))
                cost_currency = cost_currency or currency
                narration_suffix = ""
                logger.info(
                    f"Cost basis lookup successful: {total_cost} {cost_currency} / {new_qty} = "
                    f"{new_cost_per_share} {cost_currency} per share"
                )
            else:
                # Fallback to zero cost with warning
                new_cost_per_share = D('0')
                cost_currency = currency
                narration_suffix = " - REVIEW COST BASIS"
                logger.warning(
                    f"Could not determine cost basis for {new_symbol} in {asset_account}. "
                    f"Using zero cost - manual adjustment required."
                )
            
            # Create metadata
            meta_dict = {
                "symbol": new_symbol,
                "old_symbol": old_symbol,
                "isin": addition_row.get("isin", ""),
                "split_ratio": split_ratio,
                "split_type": "reverse",
                "split_description": description,
                "actionID": str(action_id),
            }
            if cost_basis_found:
                meta_dict["original_total_cost"] = str(total_cost)
                meta_dict["original_units"] = str(total_units)
            
            meta = data.new_metadata("reverse_split", 0, meta_dict)
            
            # Cost spec for removal - use None/empty to match any existing lot
            cost_removal = position.CostSpec(
                number_per=None,
                number_total=None,
                currency=None,
                date=None,
                label=None,
                merge=False,
            )
            
            # Cost spec for addition - use calculated cost basis or zero if not found
            cost_addition = position.CostSpec(
                number_per=new_cost_per_share,
                number_total=None,
                currency=cost_currency,
                date=date,
                label=None,
                merge=False,
            )
            
            # Create postings using the clean symbol for both sides
            # This ensures the removal matches existing lots under the clean symbol (e.g., MSTY)
            # rather than the timestamped symbol IBKR uses internally (e.g., 20251205172827MSTY)
            postings = [
                data.Posting(
                    asset_account,
                    amount.Amount(D(str(-old_qty)), new_symbol),
                    cost_removal,
                    None,
                    None,
                    None,
                ),
                data.Posting(
                    asset_account,
                    amount.Amount(D(str(new_qty)), new_symbol),
                    cost_addition,
                    None,
                    None,
                    None,
                ),
            ]
            
            # Create transaction
            narration = f"Reverse stock split {new_symbol} ({split_ratio}){narration_suffix}"
            
            transactions.append(
                data.Transaction(
                    meta,
                    date,
                    self.flag,
                    new_symbol,  # payee
                    narration,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
            
            logger.info(
                f"Processed reverse stock split: {date} {new_symbol} "
                f"({split_ratio}): -{old_qty} -> +{new_qty} @ {new_cost_per_share} {cost_currency}"
            )
        
        return transactions


def CollapseTradeSplits(tr):
    # to be implemented
    """
    This function collapses two trades into once if they have same date,symbol
    and trade price. IB sometimes splits up trades.
    """
    pass


def isForex(symbol):
    # retruns True if a transaction is a forex transaction.
    b = re.search("(\\w{3})[.](\\w{3})", symbol)  # find something lile "USD.CHF"
    if b is None:  # no forex transaction, rather a normal stock transaction
        return False
    else:
        return True


def getForexCurrencies(symbol):
    b = re.search("(\\w{3})[.](\\w{3})", symbol)
    c = b.groups()
    return [c[0], c[1]]


class InvalidFormatError(Exception):
    pass


def fmt_number_de(value: str) -> Decimal:
    # a fix for region specific number formats
    thousands_sep = "."
    decimal_sep = ","

    return Decimal(value.replace(thousands_sep, "").replace(decimal_sep, "."))


def DecimalOrZero(value):
    # for string to number conversion with empty strings
    try:
        return Decimal(value)
    except BaseException:
        return Decimal(0.0)


def AmountAdd(A1, A2):
    # add two amounts
    if A1.currency == A2.currency:
        quant = A1.number + A2.number
        return amount.Amount(quant, A1.currency)
    else:
        raise (
            "Cannot add amounts of differnent currencies: {} and {}".format(
                A1.currency, A1.currency
            )
        )


def minus(A):
    # a minus operator
    return amount.Amount(-A.number, A.currency)
