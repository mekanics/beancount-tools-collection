"""
This is a beancount importer for Interactive Brokers.
Setup:
1) have a running beancount system
2) activate IB FlexQuery with the entries specified in the supported transaction types below.
   Set the query format to XML.
3) create an ibkr.yaml file with your IBKR FlexQuery credentials (see Configuration File section)
4) in the config.py file, specify the IBKRImporter with the ibkr.yaml file location
5) run 'bean-extract config.py ibkr.yaml -f mainLedgerFile.bean'

To import without the API, run the same Flex query in Client Portal (XML format) and pass
the downloaded file to bean-extract, or upload it on Fava's Import page. The same
IBKRImporter recognises an Activity Flex XML file; no credentials are read for it.

Configuration File (ibkr.yaml):
The ibkr.yaml file must contain the following properties:

token: 12345678912345678912345    # Your IB FlexQuery token (string of digits)
queryId: 123456                  # Your FlexQuery ID number
baseCcy: 'USD'                   # Base currency for your account

To obtain these credentials:
1. Log into your Interactive Brokers account
2. Go to Reports > Flex Queries
3. Create a new FlexQuery or use an existing one
4. The FlexQuery must include: CashReport, Trades, CashTransactions, and CorporateActions.
   In Trades, enable both "Execution" and "Closed Lots" so sells close the right lots
   (without Closed Lots the ledger needs option "booking_method" "FIFO").
5. Generate a token for the FlexQuery - this becomes your 'token' value
6. Note the Query ID - this becomes your 'queryId' value
"""

import math
import pickle
import re
import warnings
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from os import path

import pandas as pd
import requests
import yaml
from beancount.core import amount, data, position
from beancount.core.number import D, Decimal
from beangulp.exceptions import Error as BeangulpError
from beangulp.importer import Importer
from ibflex import Types, parser
from ibflex.client import BadResponseError, ResponseCodeError
from ibflex.enums import BuySell, CashAction, Code
from ibflex.parser import FlexParserError
from loguru import logger

from beancount_tools_collection.utils import ibkr_flex

# Maps IBKR note codes to the ibflex Code values they should parse as. Aliases win over
# ibflex's own values, so they also correct codes ibflex reads wrongly. IBKR adds codes
# without notice; unknown ones are dropped so the import does not crash.
# To add a new mapping: run with DEBUG logging, look for "Dropped unrecognised" warnings,
# identify the code's meaning from IBKR docs, and add it below.
#
# Known gaps vs ibflex v1.1 (IBKR: Ri = Reimbursement, RI = Recurring Investment):
#   RI  - Recurring Investment (IBKR auto-invest / DCA plan). ibflex 1.1 has no such
#         code and reads "RI" as REIMBURSEMENT. Distinct from "R" (Dividend
#         Reinvestment / DRIP). Mapped to ["I"] (Internal) as the closest available
#         equivalent; must NOT map to "R" or it will be mis-tagged as #drip.
#   Ri  - Reimbursement. ibflex 1.1 stores REIMBURSEMENT under "RI", so "Ri" is unknown.
# Targets are written as Code members so they follow ibflex if it renames a value.
_IBKR_CODE_ALIASES: dict[str, list[str]] = {
    'RI': [Code.INTERNAL.value],
    'Ri': [Code.REIMBURSEMENT.value],
}

# IBKR's note code for a recurring investment (auto-invest plan). The alias above
# loses it, so buys are tagged from the raw notes instead.
_RECURRING_INVESTMENT_CODE = 'RI'


def _raw_trade_notes(xml: bytes) -> list[list[str]] | None:
    """IBKR's unaltered notes for every row ibflex parses from each statement's Trades.

    One list per FlexStatement, in document order, skipping the elements ibflex
    skips, so position i matches FlexStatement.Trades[i]. None when the XML
    cannot be read; the ibflex parse then reports the error.
    """
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return None
    notes = []
    for statement in root.iter('FlexStatement'):
        trades = statement.find('Trades')
        children = () if trades is None else trades
        notes.append(
            [
                child.get('notes', '')
                for child in children
                if getattr(Types, child.tag, None) is not None
            ]
        )
    return notes


# Activity Flex XML is recognised by its root element. IBKR also serves Trade
# Confirmation Flex (type TCF) and error documents (FlexStatementResponse); those
# are not statements this importer can book.
_FLEX_SNIFF_BYTES = 4096
_FLEX_ACTIVITY_ROOT = re.compile(
    rb'\A(?:\xef\xbb\xbf)?\s*(?:<\?xml[^>]*\?>\s*)?<FlexQueryResponse\b[^>]*\btype="AF"'
)
_FLEX_PERIOD = re.compile(rb'<FlexStatement\b[^>]*\bfromDate="(\d{8})"[^>]*\btoDate="(\d{8})"')


class IBKRImportError(BeangulpError):
    """Base: the IBKR statement could not be turned into entries.

    Subclasses beangulp's Error so the CLI prints the message without a
    traceback; Fava turns any exception into a visible error notification.
    """


class IBKRConfigError(IBKRImportError):
    """Import cannot succeed until the user changes something.

    Expired/invalid token, inactive service account, IP restriction,
    invalid query id, unreadable or incomplete ibkr.yaml.
    """


class IBKRTemporaryError(IBKRImportError):
    """IBKR is busy or throttling; the same request should work later."""


class IBKRLockoutError(IBKRTemporaryError):
    """IBKR blocked Flex requests after too many failed attempts (code 1025).

    A new token does not lift the block.
    """


class IBKRStatementError(IBKRImportError):
    """A statement was returned but could not be parsed."""


# Permanent Flex API codes: retrying changes nothing; the user must act.
# Texts match ibflex.client.ERRORS and lead with the cause + remediation.
_PERMANENT_CODES: dict[str, str] = {
    '1010': (
        'Legacy Flex Queries are no longer supported — recreate the query as an Activity Flex query'
    ),
    '1011': (
        'the IBKR Flex Web Service account is inactive — re-enable it under '
        'Reports > Flex Web Service'
    ),
    '1012': (
        'the IBKR Flex token has expired — generate a new token under '
        "Reports > Flex Web Service and update 'token'"
    ),
    '1013': (
        'IBKR rejected the request due to an IP restriction — allow this '
        "machine's IP in the Flex Web Service settings"
    ),
    '1014': ("the Flex query is invalid — check 'queryId' and that the query still exists"),
    '1015': ("the IBKR Flex token is invalid — copy a fresh token into 'token'"),
    '1016': 'the IBKR account referenced by the query is invalid',
    '1017': ('the Flex reference code is invalid — retry; if it persists, recreate the query'),
    '1020': ("IBKR could not validate the request — verify 'token' and 'queryId'"),
}

# Transient Flex API codes: the same request should succeed later.
_TEMPORARY_CODES = frozenset(
    {
        '1001',
        '1003',
        '1004',
        '1005',
        '1006',
        '1007',
        '1008',
        '1009',
        '1018',
        '1019',
        '1021',
    }
)


def _redact(text: str, token: str) -> str:
    """Replace the Flex token with '<redacted>' in text.

    Always scrubs the URL query form ``t=<token>`` (how ``requests`` leaks the
    Flex token). Blanket substring replacement only runs for tokens long enough
    to be unique — short values like ``"0"`` must not scramble unrelated digits
    (e.g. HTTP status ``403``).
    """
    if not token:
        return text
    text = str(text)
    token = str(token)
    text = text.replace(f't={token}', 't=<redacted>')
    if len(token) >= 8:
        text = text.replace(token, '<redacted>')
    return text


def _token_hint(token: str) -> str:
    """', token ending in 1234' so users can tell which token was sent; '' for short tokens."""
    token = str(token or '')
    return f', token ending in {token[-4:]}' if len(token) >= 12 else ''


def _classify_response_code_error(error: ResponseCodeError, filepath: str) -> IBKRImportError:
    """Map an IBKR Flex error code (or a cooldown after one) to one of our exceptions."""
    code = error.code
    ibkr_text = (error.msg or '').rstrip('.')
    remedy = ''
    if code == ibkr_flex.LOCKOUT_CODE:
        cls = IBKRLockoutError
        reason = f'IBKR is blocking Flex requests after too many failed attempts (code {code})'
        remedy = (
            ' A new token does not lift this block, and IBKR does not say how long it lasts'
            ' (users report up to a day): stop importing, log in to Client Portal once,'
            ' and contact IBKR if 1025 persists the next day.'
            ' Meanwhile you can import a statement downloaded from Client Portal:'
            " run the Flex query as XML and upload the file on Fava's Import page"
            ' (or pass it to bean-extract).'
        )
    elif code in _PERMANENT_CODES:
        cls, reason = IBKRConfigError, f'{_PERMANENT_CODES[code]} (code {code})'
    elif code == ibkr_flex.THROTTLED_CODE:
        cls = IBKRTemporaryError
        reason = f'IBKR is throttling this token: too many requests (code {code})'
    elif code in _TEMPORARY_CODES:
        cls = IBKRTemporaryError
        reason = (
            f'IBKR is busy or still generating the statement '
            f'(code {code}: {ibkr_text}) — retry in a minute'
        )
    else:
        cls, reason = IBKRImportError, f'unexpected Flex API error code {code}: {ibkr_text}'

    if code in ibkr_flex.ANY_TOKEN_CODES:
        scope = 'this query (with any token) and every query of this token'
    elif code in ibkr_flex.TOKEN_WIDE_CODES:
        scope = 'every query of this token'
    else:
        scope = 'this query with this token'
    if isinstance(error, ibkr_flex.CooldownError):
        minutes = math.ceil(error.remaining / 60)
        return cls(
            f'IBKR import skipped: {reason}. IBKR was not contacted: to avoid adding failed '
            f'attempts, requests for {scope} stay paused for another {minutes} min '
            f'(restart Fava to retry sooner).{remedy} Config: {filepath}'
        )
    pause = ''
    if code in ibkr_flex.COOLDOWN_SECONDS:
        minutes = ibkr_flex.COOLDOWN_SECONDS[code] // 60
        pause = (
            f' To avoid adding failed attempts, Fava pauses requests for {scope} for '
            f'{minutes} min; from the command line, wait as long before importing again.'
        )
    return cls(f'IBKR import failed: {reason}.{pause}{remedy} Config: {filepath}')


class IBKRImporter(Importer):
    """
    Beancount Importer for the Interactive Brokers XML FlexQueries

    This importer processes Interactive Brokers FlexQuery XML files and converts them into
    Beancount transactions. It supports multiple accounts within a single FlexQuery and uses
    the account aliases defined in Interactive Brokers to structure the account hierarchy.

    The file named by configFile (default ibkr.yaml) is fetched from the Flex Web Service.
    Any other file that is an Activity Flex XML is read directly, so a statement downloaded
    from Client Portal can be imported without contacting IBKR.

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

    Sells:
    - With Closed Lots in the export, one posting per open date: -3 ACME {2022-09-14} @ 212.35 USD.
      IBKR's lot price includes the buy commission, so it is never written as the cost.
    - Without them, -8 ACME {} @ price; the ledger's booking method picks the lots.
    - Metadata lot_source is 'ibkr_closed_lots', 'ledger_booking' or 'manual';
      ibkr_cost is a string, e.g. "1203.44 USD" (buy commissions included).
      An Amount here shows up in Fava as "Unsupported metadata value".
    - Flagged '!' when closed lots don't cover the sell, and for cancelled sells.
    - suppressClosedLotPrice is ignored: no lot price is written any more.

    Note codes:
    - Trades keep IBKR's note codes unaltered as ibkr_notes metadata, e.g. "FP;P",
      including codes ibflex does not know. Not available for a pickled statement (fpath).
    - Buys IBKR marks as a recurring investment (RI) get the recurringTag tag
      (default "recurring"; None or "" disables it). Dividend reinvestments get #drip.

    Corporate Actions:
    - Forward stock splits (FS): Additional shares are added with zero cost basis
    - Reverse stock splits (RS): Old shares are removed and new consolidated shares added
    - Split ratio is extracted from the action description (e.g., "SPLIT 4 FOR 1")
    - Entries are grouped by actionID (if available) or dateTime+underlyingSymbol+type to match paired removal/addition entries

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
        currency='CHF',
        interestSuffix='Interest',
        fpath=None,  #
        depositAccount='',
        suppressClosedLotPrice=False,
        configFile='ibkr.yaml',
        # Configurable account type mappings
        cashAccountType='Cash',  # What to replace with for cash accounts (e.g., "Stocks" -> "Cash")
        stockAccountType='Stocks',  # What to replace for stock accounts (default: "Stocks")
        recurringTag='recurring',  # tag for recurring-investment buys; None or '' disables it
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
        self.flag = '*'
        self.configFile = configFile
        self.recurringTag = recurringTag
        self._account_alias = None  # Will be set when processing statements
        self._raw_notes = None  # per statement, set by _parse_statement

        # Configurable account type mappings
        self.cashAccountType = cashAccountType
        self.stockAccountType = stockAccountType

    def account(self, filepath):
        """The account to associate with this importer."""
        return self.Mainaccount

    def identify(self, filepath):
        """ibkr.yaml by basename, or an Activity Flex XML by content. Never raises."""
        result = self._is_config_file(filepath) or self._is_flex_xml(filepath)
        logger.info(f"identify assertion for ibkr importer and file '{filepath}': {result}")
        return result

    def date(self, filepath):
        """Statement end date for an Activity Flex XML; None for the credentials file."""
        period = self._statement_period(filepath)
        return period[1] if period else None

    def filename(self, filepath):
        """Archive name 'ibkr_<from>_<to>.xml' for an Activity Flex XML; else None."""
        period = self._statement_period(filepath)
        if period is None:
            return None
        start, end = period
        return f'ibkr_{start:%Y-%m-%d}_{end:%Y-%m-%d}.xml'

    def _is_config_file(self, filepath: str) -> bool:
        return self.configFile == path.basename(filepath)

    def _sniff(self, filepath: str) -> bytes:
        """First bytes of filepath, or b'' when the file cannot be read.

        identify, date and filename must not raise: Fava turns any exception
        there into an error for the whole import listing.
        """
        try:
            with open(filepath, 'rb') as f:
                return f.read(_FLEX_SNIFF_BYTES)
        except OSError:
            return b''

    def _is_flex_xml(self, filepath: str) -> bool:
        return _FLEX_ACTIVITY_ROOT.search(self._sniff(filepath)) is not None

    def _statement_period(self, filepath: str):
        """(fromDate, toDate) of the first FlexStatement, or None."""
        if self._is_config_file(filepath):
            return None
        match = _FLEX_PERIOD.search(self._sniff(filepath))
        if match is None:
            return None
        try:
            start = datetime.strptime(match.group(1).decode(), '%Y%m%d').date()
            end = datetime.strptime(match.group(2).decode(), '%Y%m%d').date()
        except ValueError:
            return None
        return start, end

    # def name(self) -> str:
    #     return self.configFile

    def getLiquidityAccount(self, currency):
        # Assets:Invest:IB:USD
        return ':'.join(
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
        return ':'.join(
            filter(
                None,
                [self.DivAccount, self._account_alias, currency],
            )
        )

    def getInterestIncomeAcconut(self, currency):
        # Income:Invest:IB:USD
        # Convert Assets:Invest:IB to Income:Invest:IB
        account_parts = self.Mainaccount.split(':')
        if account_parts[0] == 'Assets':
            account_parts[0] = 'Income'
        income_base = ':'.join(account_parts)

        return ':'.join(
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
        return ':'.join(filter(None, [self.Mainaccount, self._account_alias, symbol]))

    def getWHTAccount(self):
        # Expenses:Invest:IB
        # return ":".join([self.WHTAccount, symbol])
        return self.WHTAccount

    def getFeesAccount(self, currency):
        return ':'.join([self.FeesAccount, currency])

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
        formatted = alias.replace(' ', '-')
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
            logger.warning('No existing entries available for cost basis lookup')
            return None, None, None

        try:
            # Build query to get holdings with cost basis

            # Simpler approach: iterate through entries to calculate cost basis
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
                    logger.info(f'Account {account} not found in existing entries')
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
                        logger.info(
                            f'Found cost basis for {symbol}: {total_units} units, {total_cost} {cost_currency}'
                        )
                        return total_cost, total_units, cost_currency

            logger.info(f'No position found for {symbol} in {account}')
            return None, None, None

        except Exception as e:
            logger.warning(f'Error querying cost basis: {e}')
            return None, None, None

    def _sanitize_ibkr_xml(self, response):
        """Rewrite Trade.notes codes so ibflex parses them correctly or not at all.

        IBKR occasionally introduces new notes codes that are not yet in
        ibflex's Code enum, causing a FlexParserError before any of our
        processing runs. Codes in _IBKR_CODE_ALIASES are rewritten first;
        remaining codes ibflex does not know are dropped with a warning rather
        than crashing the entire import.
        """
        known_codes = {c.value for c in Code}

        def _filter(match):
            raw = match.group(1).decode()
            expanded = []
            for c in raw.split(';'):
                if c in _IBKR_CODE_ALIASES:
                    expanded.extend(_IBKR_CODE_ALIASES[c])
                    logger.debug(f"Mapped IBKR note code '{c}' -> {_IBKR_CODE_ALIASES[c]}")
                elif c in known_codes:
                    expanded.append(c)
                elif c:
                    logger.warning(f"Dropped unrecognised IBKR note code: '{c}'")
            return b'notes="' + ';'.join(expanded).encode() + b'"'

        return re.sub(rb'notes="([^"]*)"', _filter, response)

    def _load_credentials(self, filepath: str):
        """Read token/queryId from the ibkr.yaml at filepath.

        Raises:
            IBKRConfigError: file missing, unreadable, not a mapping, or missing keys.
        """
        try:
            with open(filepath) as f:
                config = yaml.safe_load(f)
        except (OSError, yaml.YAMLError) as e:
            raise IBKRConfigError(
                f"IBKR import failed: cannot read credentials file '{filepath}': {e}"
            ) from e

        if not isinstance(config, dict):
            raise IBKRConfigError(
                f"IBKR import failed: credentials file '{filepath}' must be a "
                f"YAML mapping with 'token' and 'queryId'"
            )

        missing = [
            key for key in ('token', 'queryId') if key not in config or config[key] in (None, '')
        ]
        if missing:
            raise IBKRConfigError(
                f"IBKR import failed: credentials file '{filepath}' is missing "
                f'required key(s): {", ".join(missing)}'
            )

        return str(config['token']), config['queryId']

    def _download_statement(self, token: str, queryId, filepath: str):
        """Fetch and parse the Flex statement.

        Raises:
            IBKRConfigError, IBKRTemporaryError, IBKRStatementError, IBKRImportError
        """
        try:
            response = ibkr_flex.fetch(token, queryId)
        except ResponseCodeError as e:
            # Safe to chain: ResponseCodeError messages are code + IBKR text only.
            classified = _classify_response_code_error(e, filepath)
            msg = _redact(str(classified), token) + _token_hint(token)
            logger.error(msg)
            raise type(classified)(msg) from e
        except ibkr_flex.StatementTimeoutError as e:
            # Safe to chain: the message is a duration and IBKR's last code + text.
            msg = f'IBKR import failed: {e} — try again later. Config: {filepath}'
            logger.error(msg)
            raise IBKRTemporaryError(msg) from e
        except BadResponseError as e:
            # Do not chain: BadResponseError's message is the raw response body.
            status = getattr(e.response, 'status_code', '?')
            length = len(getattr(e.response, 'content', b'') or b'')
            msg = (
                f'IBKR import failed: malformed Flex response '
                f"(HTTP {status}, {length} bytes) for '{filepath}'"
            )
            logger.error(msg)
            raise IBKRImportError(msg) from None
        except requests.exceptions.RequestException as e:
            # Do not chain: requests exceptions embed the URL including ?t=<token>.
            # Fava surfaces traceback.format_exc(), which would otherwise leak it.
            msg = _redact(
                f'IBKR import failed: network error fetching statement for '
                f"'{filepath}': {type(e).__name__}: {e}",
                token,
            )
            logger.error(msg)
            raise IBKRImportError(msg) from None
        except Exception as e:
            # Do not chain: unknown upstream errors may embed secrets.
            msg = _redact(
                f'IBKR import failed: unexpected error fetching statement for '
                f"'{filepath}': {type(e).__name__}: {e}",
                token,
            )
            logger.error(msg)
            raise IBKRImportError(msg) from None

        try:
            return self._parse_statement(response, filepath)
        except IBKRStatementError as e:
            # Redact before logging. Do not chain when the text changed: the
            # original message is what would put the token in the traceback.
            msg = _redact(str(e), token)
            logger.error(msg)
            if msg != str(e):
                raise IBKRStatementError(msg) from None
            raise

    def _read_statement_file(self, filepath: str):
        """Read an Activity Flex XML from disk. No credentials, no network."""
        try:
            with open(filepath, 'rb') as f:
                xml = f.read()
        except OSError as e:
            raise IBKRStatementError(
                f"IBKR import failed: cannot read Flex statement file '{filepath}': {e}"
            ) from e
        try:
            return self._parse_statement(xml, filepath)
        except IBKRStatementError as e:
            logger.error(str(e))
            raise

    def _parse_statement(self, xml: bytes, source: str):
        """Sanitize and parse Flex XML.

        Raises:
            IBKRStatementError: the bytes are not a FlexQueryResponse. `source`
            (a path or the credentials file) is named in the message.
        """
        try:
            statement = parser.parse(self._sanitize_ibkr_xml(xml))
        except FlexParserError as e:
            # The caller logs this, after redacting any Flex token on the API path.
            raise IBKRStatementError(
                f'IBKR import failed: could not parse Flex statement from '
                f"'{source}': {e}. If this is an unknown note code, extend "
                f'_IBKR_CODE_ALIASES.'
            ) from e
        except Exception as e:
            raise IBKRStatementError(
                f'IBKR import failed: could not parse Flex statement from '
                f"'{source}': {type(e).__name__}: {e}"
            ) from e

        if not isinstance(statement, Types.FlexQueryResponse):
            raise IBKRStatementError(
                f"IBKR import failed: unexpected statement type {type(statement)!r} from '{source}'"
            )
        self._raw_notes = _raw_trade_notes(xml)
        return statement

    def _load_pickled_statement(self, fpath: str):
        """Offline path for self.fpath. Raises IBKRStatementError on a bad pickle."""
        logger.info(f'Loading IBKR statement from pickle: {fpath}')
        try:
            with open(fpath, 'rb') as pf:
                return pickle.load(pf)
        except Exception as e:
            raise IBKRStatementError(
                f'IBKR import failed: cannot load pickled statement from '
                f"'{fpath}': {type(e).__name__}: {e}"
            ) from e

    def extract(self, filepath, existing=None):
        # the actual processing of the flex query

        # Store existing entries for cost basis lookup in corporate actions
        self._existing_entries = existing
        self._raw_notes = None

        if self._is_config_file(filepath):
            token, queryId = self._load_credentials(filepath)
            if self.fpath is None:
                statement = self._download_statement(token, queryId, filepath)
            else:
                statement = self._load_pickled_statement(self.fpath)
        else:
            statement = self._read_statement_file(filepath)

        return self._entries_from_statement(statement)

    def _entries_from_statement(self, statement):
        all_transactions = []

        # Process each FlexStatement
        for index, flex_stmt in enumerate(statement.FlexStatements):
            # Get account alias for this statement
            if hasattr(flex_stmt, 'AccountInformation'):
                raw_alias = flex_stmt.AccountInformation.acctAlias
                self._account_alias = self._format_account_alias(raw_alias)
                logger.info(
                    f'Processing statement for account alias: {self._account_alias} (original: {raw_alias})'
                )
            else:
                warnings.warn(
                    f'Could not find account alias in FlexStatement for account {flex_stmt.accountId}',
                    stacklevel=2,
                )
                self._account_alias = None

            # relevant items from report
            reports = ['CashReport', 'Trades', 'CashTransactions', 'CorporateActions']

            # dtype=object keeps ibflex's str-based enums (BuySell, CashAction) as enum
            # objects. pandas 3 would otherwise infer its str dtype for those columns,
            # and `df[col] == BuySell.BUY` then matches no row.
            tabs = {
                report: pd.DataFrame(
                    [dict(entry.__dict__.items()) for entry in flex_stmt.__dict__.get(report, [])],
                    dtype=object,
                )
                for report in reports
            }

            # get single dataFrames
            ct = tabs['CashTransactions']
            tr = tabs['Trades']
            cr = tabs['CashReport']
            ca = tabs['CorporateActions']

            raw_notes = (
                self._raw_notes[index] if self._raw_notes and index < len(self._raw_notes) else None
            )
            if raw_notes is not None and len(raw_notes) == len(tr):
                tr['rawNotes'] = raw_notes
            elif raw_notes is not None:
                logger.warning(
                    f'IBKR note codes not kept: {len(raw_notes)} raw Trades rows vs '
                    f'{len(tr)} parsed for account {flex_stmt.accountId}'
                )

            # throw out IBKR jitter, mostly None
            ct.drop(columns=[col for col in ct if all(ct[col].isnull())], inplace=True)
            tr.drop(columns=[col for col in tr if all(tr[col].isnull())], inplace=True)
            cr.drop(columns=[col for col in cr if all(cr[col].isnull())], inplace=True)
            ca.drop(columns=[col for col in ca if all(ca[col].isnull())], inplace=True)

            # Process transactions for this statement
            transactions = (
                self.Trades(tr)
                + self.CashTransactions(ct)
                + self.Balances(cr)
                + self.CorporateActions(ca)
            )
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
            ct['type'].map(
                lambda t: (
                    t == CashAction.DIVIDEND
                    or t == CashAction.PAYMENTINLIEU
                    or t == CashAction.WHTAX
                )
            )
        ].copy()

        # Process combined dividend and WHT transactions
        div_wht_transactions = self.ProcessDividendsAndWHT(div_wht) if not div_wht.empty else []

        # Process other transaction types
        dep = ct[ct['type'] == CashAction.DEPOSITWITHDRAW].copy()
        deps = self.Deposits(dep) if len(dep) > 0 else []

        int_ = ct[
            ct['type'].map(lambda t: t == CashAction.BROKERINTRCVD or t == CashAction.BROKERINTPAID)
        ].copy()
        ints = self.Interest(int_) if len(int_) > 0 else []

        fee = ct[ct['type'] == CashAction.FEES].copy()
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
            lambda d: (
                re.search(r'USD ([\d.]+) PER SHARE', d).group(1)
                if re.search(r'USD ([\d.]+) PER SHARE', d)
                else None
            )
        )
        div_wht['isin'] = div_wht['description'].apply(
            lambda d: re.search(r'\((.*?)\)', d).group(1) if re.search(r'\((.*?)\)', d) else None
        )
        div_wht['is_correction'] = div_wht['description'].str.contains('CORRECTION', case=False)

        # Create a matching key for grouping related entries
        div_wht['group_key'] = div_wht.apply(
            lambda row: f'{row["symbol"]}_{row["div_rate"]}_{row["reportDate"]}', axis=1
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
            div_entries = group[
                group['type'].map(
                    lambda t: t == CashAction.DIVIDEND or t == CashAction.PAYMENTINLIEU
                )
            ]
            wht_entries = group[group['type'] == CashAction.WHTAX]

            # Calculate totals
            total_div = sum(div_entries['amount'])
            total_wht = sum(wht_entries['amount'])

            # Skip if we don't have both dividend and WHT (might be in different reports)
            if div_entries.empty or wht_entries.empty:
                logger.info(
                    f'Incomplete dividend group for {symbol} on {date}: '
                    f'Dividend entries: {len(div_entries)}, WHT entries: {len(wht_entries)}'
                )
                # Process them individually for now
                if not div_entries.empty:
                    transactions.extend(self._process_single_dividend(div_entries))
                if not wht_entries.empty:
                    transactions.extend(self._process_single_wht(wht_entries))
                continue

            # Create metadata with all related entries
            meta = data.new_metadata(
                'dividend',
                0,
                {
                    'symbol': symbol,
                    'isin': isin,
                    'dividend_rate': div_rate,
                    'dividend_entries': '\n'.join(div_entries['description'].tolist()),
                    'wht_entries': '\n'.join(wht_entries['description'].tolist()),
                    'is_correction': '1' if any(group['is_correction']) else '0',
                    'correction_group': group_key,
                },
            )

            # Create postings
            postings = [
                # Dividend income posting
                data.Posting(
                    self.getDivIncomeAccount(currency, symbol),
                    minus(amount.Amount(total_div, currency)),
                    None,
                    None,
                    None,
                    None,
                ),
                # WHT posting
                data.Posting(
                    self.getWHTAccount(),
                    minus(amount.Amount(total_wht, currency)),
                    None,
                    None,
                    None,
                    None,
                ),
                # Net cash posting
                data.Posting(
                    self.getLiquidityAccount(currency),
                    amount.Amount(total_div + total_wht, currency),
                    None,
                    None,
                    None,
                    None,
                ),
            ]

            # Create transaction
            narration = f'Dividend {symbol} ({div_rate} USD per share)' + (
                ' - Correction' if any(group['is_correction']) else ''
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
                f'Processed dividend group: {date} {symbol} '
                f'(rate: {div_rate}): {len(group)} entries in single transaction'
            )

        return transactions

    def _process_single_dividend(self, div_entries):
        """Process dividend entries that don't have matching WHT entries."""
        transactions = []
        for _, row in div_entries.iterrows():
            currency = row['currency']
            symbol = row['symbol']
            amount_ = amount.Amount(row['amount'], currency)

            meta = data.new_metadata(
                'dividend',
                0,
                {
                    'symbol': symbol,
                    'isin': row.get('isin'),
                    'original_description': row['description'],
                    'awaiting_wht': True,  # Flag that this might be matched later
                },
            )

            postings = [
                data.Posting(
                    self.getDivIncomeAccount(currency, symbol),
                    minus(amount_),
                    None,
                    None,
                    None,
                    None,
                ),
                data.Posting(self.getLiquidityAccount(currency), amount_, None, None, None, None),
            ]

            transactions.append(
                data.Transaction(
                    meta,
                    row['reportDate'],
                    self.flag,
                    symbol,
                    f'Dividend {symbol} (awaiting WHT)',
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
            currency = row['currency']
            symbol = row['symbol']
            amount_ = amount.Amount(row['amount'], currency)

            meta = data.new_metadata(
                'WHT',
                0,
                {
                    'symbol': symbol,
                    'isin': row.get('isin'),
                    'original_description': row['description'],
                    'awaiting_dividend': True,  # Flag that this might be matched later
                },
            )

            postings = [
                data.Posting(self.getWHTAccount(), minus(amount_), None, None, None, None),
                data.Posting(self.getLiquidityAccount(currency), amount_, None, None, None, None),
            ]

            transactions.append(
                data.Transaction(
                    meta,
                    row['reportDate'],
                    self.flag,
                    symbol,
                    f'WHT {symbol} (awaiting dividend)',
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return transactions

    def Fee(self, fee):
        # calculates fees from IBKR data
        feeTransactions = []
        for _idx, row in fee.iterrows():
            currency = row['currency']
            amount_ = amount.Amount(row['amount'], currency)
            text = row['description']
            month = ''

            try:
                month = re.findall('\\w{3} \\d{4}', text)[0]
            except Exception:
                # just ignore
                warnings.warn(f"No month found in '{text}'", stacklevel=2)

            # make the postings, two for fees
            postings = [
                data.Posting(self.getFeesAccount(currency), -amount_, None, None, None, None),
                data.Posting(self.getLiquidityAccount(currency), amount_, None, None, None, None),
            ]
            meta = data.new_metadata(__file__, 0, {})  # actually no metadata
            feeTransactions.append(
                data.Transaction(
                    meta,
                    row['reportDate'],
                    self.flag,
                    'IB',  # payee
                    ' '.join(['Fee', currency, month]).strip(),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return feeTransactions

    def Interest(self, int_):
        # calculates interest payments from IBKR data
        intTransactions = []
        for _idx, row in int_.iterrows():
            currency = row['currency']
            amount_ = amount.Amount(row['amount'], currency)
            text = row['description']
            month = re.findall('\\w{3}-\\d{4}', text)[0]

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
                data.Posting(self.getLiquidityAccount(currency), amount_, None, None, None, None),
            ]
            meta = data.new_metadata('Interest', 0)
            intTransactions.append(
                data.Transaction(
                    meta,  # could add div per share, ISIN,....
                    row['reportDate'],
                    self.flag,
                    'IB',  # payee
                    ' '.join(['Interest ', currency, month]),
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
        for _idx, row in dep.iterrows():
            currency = row['currency']
            amount_ = amount.Amount(row['amount'], currency)

            # make the postings. two for deposits
            postings = [
                data.Posting(self.depositAccount, -amount_, None, None, None, None),
                data.Posting(self.getLiquidityAccount(currency), amount_, None, None, None, None),
            ]
            meta = data.new_metadata('deposit/withdrawel', 0)
            depTransactions.append(
                data.Transaction(
                    meta,  # could add div per share, ISIN,....
                    row['reportDate'],
                    self.flag,
                    'self',  # payee
                    'deposit / withdrawal',
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
        fx = tr[tr['symbol'].apply(isForex)]
        # Stocks transactions
        stocks = tr[~tr['symbol'].apply(isForex)]

        trTransactions = self.Forex(fx) + self.Stocktrades(stocks)

        return trTransactions

    def Forex(self, fx):
        # returns beancount transactions for IBKR forex transactions

        fxTransactions = []
        for _idx, row in fx.iterrows():
            symbol = row['symbol']
            curr_prim, curr_sec = getForexCurrencies(symbol)
            currency_IBcommision = row['ibCommissionCurrency']
            proceeds = amount.Amount(round(row['proceeds'], 2), curr_sec)
            quantity = amount.Amount(round(row['quantity'], 2), curr_prim)
            price = amount.Amount(row['tradePrice'], curr_sec)
            commission = amount.Amount(round(row['ibCommission'], 2), currency_IBcommision)
            buysell = row['buySell'].name

            position.CostSpec(
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
                data.Posting(self.getLiquidityAccount(curr_sec), proceeds, None, None, None, None),
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
                    data.new_metadata('FX Transaction', 0, self._note_meta(row)),
                    row['tradeDate'],
                    self.flag,
                    symbol,  # payee
                    ' '.join([buysell, quantity.to_string(), '@', price.to_string()]),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return fxTransactions

    @staticmethod
    def _raw_notes_of(row):
        """IBKR's notes for a trade row as written in the XML, '' when unknown."""
        raw = row.get('rawNotes')
        return raw if isinstance(raw, str) else ''

    def _note_meta(self, row):
        raw = self._raw_notes_of(row)
        return {'ibkr_notes': raw} if raw else {}

    def _buy_tags(self, row):
        tags = set()
        if Code.REINVESTMENT in row.get('notes', ()):
            tags.add('drip')
        if self.recurringTag and _RECURRING_INVESTMENT_CODE in self._raw_notes_of(row).split(';'):
            tags.add(self.recurringTag)
        return frozenset(tags)

    def Stocktrades(self, stocks):
        # return the stocks transactions

        stocktrades = stocks[stocks['levelOfDetail'] == 'EXECUTION']  # actual trades
        buy = stocktrades[
            (stocktrades['buySell'] == BuySell.BUY)
            | (  # purchases, including cancelled ones
                stocktrades['buySell'] == BuySell.CANCELBUY
            )
        ]  # and the cancellation transactions to keep balance
        sale = stocktrades[
            (stocktrades['buySell'] == BuySell.SELL)
            | (  # sales, including cancelled ones
                stocktrades['buySell'] == BuySell.CANCELSELL
            )
        ]  # and the cancellation transactions to keep balance
        stockTransactions = self.Panic(sale, stocks) + self.Shopping(buy)

        return stockTransactions

    def _commission_postings(self, row):
        """Cash and fees legs for a trade's commission; none when it rounds to zero."""
        currency = row['ibCommissionCurrency']
        commission = amount.Amount(row['ibCommission'].__round__(2), currency)
        if commission.number == 0:
            return []
        return [
            data.Posting(self.getLiquidityAccount(currency), commission, None, None, None, None),
            data.Posting(self.getFeesAccount(currency), minus(commission), None, None, None, None),
        ]

    def _closed_lots_by_execution(self, stocks):
        """Map each EXECUTION row's index to the CLOSED_LOT rows that directly follow it.

        IBKR writes the lots a sell closed right after its execution row, and
        ibflex keeps document order.
        """
        groups = {}
        current = None
        for idx, row in stocks.iterrows():
            if row['levelOfDetail'] == 'EXECUTION':
                current = idx
                groups[current] = []
            elif (
                row['levelOfDetail'] == 'CLOSED_LOT'
                and current is not None
                and row['symbol'] == stocks.at[current, 'symbol']
            ):
                groups[current].append(idx)
            else:
                current = None
        return {idx: stocks.loc[rows] for idx, rows in groups.items() if rows}

    def _sell_lot_postings(self, row, lots):
        """Asset postings that reduce the position for one sell execution.

        Returns (postings, lot_source, needs_review). The cost spec never carries
        a price: IBKR's lot price includes the buy commission, the ledger's lots
        don't. With IBKR's closed lots, lots are matched by open date; without
        them, an empty cost spec leaves the choice to the ledger's booking method.
        """
        symbol = row['symbol']
        account = self.getAssetAccount(symbol)
        price = amount.Amount(row['tradePrice'], row['currency'])
        sold = -row['quantity']

        def reduce(units, open_date):
            cost = position.CostSpec(
                number_per=None,
                number_total=None,
                currency=None,
                date=open_date,
                label=None,
                merge=False,
            )
            return data.Posting(account, amount.Amount(-units, symbol), cost, price, None, None)

        if lots is None:
            return [reduce(sold, None)], 'ledger_booking', False

        usable = (
            'openDateTime' in lots
            and not lots['openDateTime'].isna().any()
            and sum(lots['quantity']) == sold
        )
        if not usable:
            logger.warning(
                f'Closed lots for {symbol} sell on {row["tradeDate"]} do not cover '
                f'{sold} units with open dates; leaving lot choice to the ledger'
            )
            return [reduce(sold, None)], 'ledger_booking', True

        units_by_date = {}
        for _, lot in lots.iterrows():
            open_date = lot['openDateTime'].date()
            units_by_date[open_date] = units_by_date.get(open_date, 0) + lot['quantity']
        postings = [reduce(units, open_date) for open_date, units in units_by_date.items()]
        return postings, 'ibkr_closed_lots', False

    def Shopping(self, buy):
        # let's go shopping!!

        Shoppingbag = []
        for _idx, row in buy.iterrows():
            # continue # debugging
            currency = row['currency']
            symbol = row['symbol']
            proceeds = amount.Amount(row['proceeds'].__round__(2), currency)
            quantity = amount.Amount(row['quantity'], symbol)
            price = amount.Amount(row['tradePrice'], currency)

            cost = position.CostSpec(
                number_per=price.number,
                number_total=None,
                currency=currency,
                date=row['tradeDate'],
                label=None,
                merge=False,
            )

            postings = [
                data.Posting(self.getAssetAccount(symbol), quantity, cost, None, None, None),
                data.Posting(self.getLiquidityAccount(currency), proceeds, None, None, None, None),
            ] + self._commission_postings(row)

            Shoppingbag.append(
                data.Transaction(
                    data.new_metadata('Buy', 0, self._note_meta(row)),
                    row['dateTime'].date(),
                    self.flag,
                    symbol,  # payee
                    ' '.join(['BUY', quantity.to_string(), '@', price.to_string()]),
                    self._buy_tags(row),
                    data.EMPTY_SET,
                    postings,
                )
            )
        return Shoppingbag

    def Panic(self, sale, stocks):
        # OMG, IT is happening!!

        closed_lots = self._closed_lots_by_execution(stocks)
        Doom = []
        for idx, row in sale.iterrows():
            currency = row['currency']
            symbol = row['symbol']
            proceeds = amount.Amount(row['proceeds'].__round__(2), currency)
            quantity = amount.Amount(row['quantity'], symbol)
            price = amount.Amount(row['tradePrice'], currency)
            date = row['dateTime'].date()

            if row['buySell'] == BuySell.CANCELSELL:
                logger.warning(f'Cancelled {symbol} sell on {date} needs its lots restored by hand')
                lotpostings, lot_source, needs_review = [], 'manual', True
            else:
                lotpostings, lot_source, needs_review = self._sell_lot_postings(
                    row, closed_lots.get(idx)
                )

            meta = {'lot_source': lot_source, **self._note_meta(row)}
            if not pd.isna(row.get('cost')):
                # A string, not an Amount: Fava's metadata UI cannot render an
                # Amount and shows "Unsupported metadata value" instead.
                meta['ibkr_cost'] = f'{abs(row["cost"])} {currency}'

            postings = (
                [data.Posting(self.getLiquidityAccount(currency), proceeds, None, None, None, None)]
                + lotpostings
                + [data.Posting(self.getPNLAccount(symbol), None, None, None, None, None)]
                + self._commission_postings(row)
            )

            Doom.append(
                data.Transaction(
                    data.new_metadata('Sell', 0, meta),
                    date,
                    '!' if needs_review else self.flag,
                    symbol,  # payee
                    ' '.join(['SELL', quantity.to_string(), '@', price.to_string()]),
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
        for _idx, row in cr.iterrows():
            currency = row['currency']
            if currency == 'BASE_SUMMARY':
                continue  # this is a summary balance that is not needed for beancount
            amount_ = amount.Amount(row['endingCash'].__round__(2), currency)

            meta = data.new_metadata('balance', 0)

            crTransactions.append(
                data.Balance(
                    meta,
                    row['toDate'] + timedelta(days=1),  # see tariochtools EC imp.
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

        # Process forward splits (FS)
        forward_splits = ca[ca['type'].astype(str).str.contains('FS', case=False, na=False)].copy()
        caTransactions.extend(self._process_forward_splits(forward_splits))

        # Process reverse splits (RS)
        reverse_splits = ca[ca['type'].astype(str).str.contains('RS', case=False, na=False)].copy()
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

        for _idx, row in splits.iterrows():
            symbol = row['symbol']
            currency = row['currency']
            split_quantity = amount.Amount(D(str(row['quantity'])), symbol)
            date = row['dateTime'].date() if hasattr(row['dateTime'], 'date') else row['reportDate']

            # Extract split ratio from description (e.g., "SPLIT 4 FOR 1")
            description = row['actionDescription']
            split_match = re.search(r'SPLIT (\d+) FOR (\d+)', description)
            if split_match:
                new_shares = split_match.group(1)
                old_shares = split_match.group(2)
                split_ratio = f'{new_shares}:{old_shares}'
            else:
                split_ratio = 'unknown'

            # Create metadata
            meta = data.new_metadata(
                'stock_split',
                0,
                {
                    'symbol': symbol,
                    'isin': row.get('isin', ''),
                    'split_ratio': split_ratio,
                    'split_type': 'forward',
                    'split_description': description,
                },
            )

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
            narration = f'Stock split {symbol} ({split_ratio})'

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
                f'Processed forward stock split: {date} {symbol} {split_ratio} (+{split_quantity})'
            )

        return transactions

    def _process_reverse_splits(self, splits):
        """
        Process reverse stock splits from IBKR data.

        Reverse splits have two paired entries that need to be matched:
        - Removal entry: negative quantity (old shares removed)
        - Addition entry: positive quantity (new consolidated shares added)

        Pairing strategy:
        1. If actionID is available, group by actionID (preferred)
        2. If actionID is not available (e.g., in ibflex), group by dateTime + underlyingSymbol + type

        Args:
            splits: pandas DataFrame with reverse split corporate actions

        Returns:
            List of beancount transactions for reverse splits
        """
        if len(splits) == 0:
            return []

        transactions = []

        # Group by actionID or fallback to dateTime + underlyingSymbol + type
        # This is an alternative to actionID which may not be available in ibflex
        grouping_columns = []
        if 'actionID' in splits.columns:
            grouping_columns = ['actionID']
            logger.debug('Using actionID for reverse split grouping')
        else:
            # Fallback grouping when actionID is not available
            # Use underlyingSymbol instead of symbol because the symbol changes in splits
            # but underlyingSymbol stays the same for paired entries
            grouping_columns = ['dateTime', 'underlyingSymbol', 'type']
            logger.debug(
                'actionID not available, using dateTime+underlyingSymbol+type for reverse split grouping'
            )

        for group_key, group in splits.groupby(grouping_columns):
            # Identify removal (negative qty) and addition (positive qty) entries
            removal = group[group['quantity'] < 0]
            addition = group[group['quantity'] > 0]

            if removal.empty or addition.empty:
                group_desc = group_key if isinstance(group_key, (str, int)) else str(group_key)
                logger.warning(f'Incomplete reverse split pair for group {group_desc}')
                continue

            # Extract data from both entries
            removal_row = removal.iloc[0]
            addition_row = addition.iloc[0]

            old_symbol = removal_row['symbol']
            new_symbol = addition_row['symbol']
            old_qty = abs(removal_row['quantity'])
            new_qty = addition_row['quantity']
            currency = addition_row['currency']

            # Parse date - handle both dateTime formats
            date_value = addition_row['dateTime']
            if hasattr(date_value, 'date'):
                date = date_value.date()
            elif isinstance(date_value, str) and ';' in date_value:
                # Handle format like "20251205;202500"
                date = datetime.strptime(date_value.split(';')[0], '%Y%m%d').date()
            else:
                date = addition_row['reportDate']

            # Extract split ratio from description (e.g., "SPLIT 1 FOR 5")
            description = addition_row['actionDescription']
            split_match = re.search(r'SPLIT (\d+) FOR (\d+)', description)
            if split_match:
                new_shares = split_match.group(1)
                old_shares = split_match.group(2)
                split_ratio = f'{new_shares}:{old_shares}'
            else:
                split_ratio = 'unknown'

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
                narration_suffix = ''
                logger.info(
                    f'Cost basis lookup successful: {total_cost} {cost_currency} / {new_qty} = '
                    f'{new_cost_per_share} {cost_currency} per share'
                )
            else:
                # Fallback to zero cost with warning
                new_cost_per_share = D('0')
                cost_currency = currency
                narration_suffix = ' - REVIEW COST BASIS'
                logger.warning(
                    f'Could not determine cost basis for {new_symbol} in {asset_account}. '
                    f'Using zero cost - manual adjustment required.'
                )

            # Create metadata
            meta_dict = {
                'symbol': new_symbol,
                'old_symbol': old_symbol,
                'isin': addition_row.get('isin', ''),
                'split_ratio': split_ratio,
                'split_type': 'reverse',
                'split_description': description,
            }

            # Add actionID to metadata if available, otherwise use group key info
            if 'actionID' in splits.columns:
                meta_dict['actionID'] = str(group_key)
            else:
                meta_dict['group_key'] = str(group_key)
            if cost_basis_found:
                meta_dict['original_total_cost'] = str(total_cost)
                meta_dict['original_units'] = str(total_units)

            meta = data.new_metadata('reverse_split', 0, meta_dict)

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
            narration = f'Reverse stock split {new_symbol} ({split_ratio}){narration_suffix}'

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
                f'Processed reverse stock split: {date} {new_symbol} '
                f'({split_ratio}): -{old_qty} -> +{new_qty} @ {new_cost_per_share} {cost_currency}'
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
    # True when the symbol looks like a forex pair, e.g. "USD.CHF".
    return re.search('(\\w{3})[.](\\w{3})', symbol) is not None


def getForexCurrencies(symbol):
    b = re.search('(\\w{3})[.](\\w{3})', symbol)
    c = b.groups()
    return [c[0], c[1]]


class InvalidFormatError(Exception):
    pass


def fmt_number_de(value: str) -> Decimal:
    # a fix for region specific number formats
    thousands_sep = '.'
    decimal_sep = ','

    return Decimal(value.replace(thousands_sep, '').replace(decimal_sep, '.'))


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
        raise (f'Cannot add amounts of differnent currencies: {A1.currency} and {A1.currency}')


def minus(A):
    # a minus operator
    return amount.Amount(-A.number, A.currency)
