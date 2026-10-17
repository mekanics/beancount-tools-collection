"""Cash transactions from an Activity Flex statement become ledger entries."""

import pytest
from beancount.core import data
from beancount.core.number import D

from beancount_tools_collection.importers.ibkr import IBKRImporter

CASH_XML = """\
<FlexQueryResponse queryName="Beancount" type="AF">
<FlexStatements count="1">
<FlexStatement accountId="U0000000" fromDate="20251013" toDate="20251016" period="LastNCalendarDays" whenGenerated="20251017;022810">
<AccountInformation accountId="U0000000" acctAlias="Long Term" currency="USD" />
<CashTransactions>
<CashTransaction accountId="U0000000" currency="USD" symbol="WPC" description="WPC(US92936U1097) CASH DIVIDEND USD 0.95 PER SHARE - US TAX" isin="US92936U1097" amount="-1.43" type="Withholding Tax" transactionID="1002" reportDate="20251015" />
<CashTransaction accountId="U0000000" currency="USD" symbol="WPC" description="WPC(US92936U1097) CASH DIVIDEND USD 0.95 PER SHARE (Ordinary Dividend)" isin="US92936U1097" amount="9.5" type="Dividends" transactionID="1001" reportDate="20251015" />
<CashTransaction accountId="U0000000" currency="USD" symbol="" description="SNAPSHOT VALUE FEE FOR OCT 2025" isin="" amount="-1.5" type="Other Fees" transactionID="1003" reportDate="20251016" />
<CashTransaction accountId="U0000000" currency="CHF" symbol="" description="CASH RECEIPTS / ELECTRONIC FUND TRANSFERS" isin="" amount="3200" type="Deposits/Withdrawals" transactionID="1004" reportDate="20251014" />
</CashTransactions>
</FlexStatement>
</FlexStatements>
</FlexQueryResponse>
"""


@pytest.fixture
def entries(tmp_path):
    importer = IBKRImporter(
        Mainaccount='Assets:Invest:IB',
        DivAccount='Income:Dividends:IB',
        WHTAccount='Expenses:Taxes:IB:WHT',
        PnLAccount='Income:PnL:IB',
        FeesAccount='Expenses:Fees:IB',
        depositAccount='Assets:Bank:Checking',
        configFile='ibkr.yaml',
        cashAccountType='Cash',
        stockAccountType='Invest',
    )
    path = tmp_path / 'Beancount.xml'
    path.write_text(CASH_XML)
    return importer.extract(str(path))


def postings_of(entry):
    return {(p.account, p.units.number, p.units.currency) for p in entry.postings}


def txns_with_payee(entries, payee):
    return [e for e in entries if isinstance(e, data.Transaction) and e.payee == payee]


def test_dividend_and_its_withholding_tax_book_as_one_transaction(entries):
    (dividend,) = txns_with_payee(entries, 'WPC')
    assert dividend.narration == 'Dividend WPC (0.95 USD per share)'
    assert postings_of(dividend) == {
        ('Income:Dividends:IB:Long-Term:USD', D('-9.5'), 'USD'),
        ('Expenses:Taxes:IB:WHT', D('1.43'), 'USD'),
        ('Assets:Cash:IB:Long-Term:USD', D('8.07'), 'USD'),
    }


def test_fee_is_booked_against_the_fees_account(entries):
    (fee,) = txns_with_payee(entries, 'IB')
    assert fee.narration == 'Fee USD OCT 2025'
    assert postings_of(fee) == {
        ('Expenses:Fees:IB:USD', D('1.5'), 'USD'),
        ('Assets:Cash:IB:Long-Term:USD', D('-1.5'), 'USD'),
    }


def test_deposit_is_booked_when_a_deposit_account_is_configured(entries):
    (deposit,) = txns_with_payee(entries, 'self')
    assert postings_of(deposit) == {
        ('Assets:Bank:Checking', D('-3200'), 'CHF'),
        ('Assets:Cash:IB:Long-Term:CHF', D('3200'), 'CHF'),
    }
