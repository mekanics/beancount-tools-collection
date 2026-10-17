# Beancount Tools Collection

🧮 My personal collection of beancount tools including importers, price fetchers, plugins, and utilities for various financial institutions.

[![CI](https://github.com/mekanics/beancount-tools-collection/actions/workflows/ci.yml/badge.svg)](https://github.com/mekanics/beancount-tools-collection/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub issues](https://img.shields.io/github/issues/mekanics/beancount-tools-collection)](https://github.com/mekanics/beancount-tools-collection/issues)

## Features

### 📥 Data Importers

**Swiss Institutions:**

- **Yuh** - CSV exports
- **Viseca** - CSV bill exports (primary) and JSON transaction exports (archival), including Migros Cumulus Credit Card
- **VIAC** - JSON transaction exports (pillar 2 & 3a)
- **Finpension** - CSV transaction reports (pillar 3a)

**International Institutions:**

- **Interactive Brokers** - FlexQuery XML reports (global); fetch/credential failures surface as errors in Fava and exit non-zero in the CLI (a genuinely empty statement still shows "No entries to import")
- **Revolut** - CSV exports (multi-country)

**Other Formats:**

- **Firefly III** - CSV exports

### 💰 Price Fetchers

- **Interactive Brokers** - Real-time prices from FlexQuery

### 🔌 Beancount Plugins

- _Crickets chirping_ 🦗 - This section is as empty as my wallet after buying crypto at the peak

### 🛠️ Utility Scripts

- **Transaction Processor** - Example transaction processing with ImporterProtocolAdapter and TransactionInspector for automatic categorization and payee standardization

## Installation

### From PyPI

```bash
pip install beancount-tools-collection
```

### From Source

```bash
git clone https://github.com/mekanics/beancount-tools-collection.git
cd beancount-tools-collection
make install
uv run pre-commit install
```

`make install` syncs the lockfile into `.venv`. The pre-commit hooks format
staged files; `pre-push` runs `make check` (the same gate CI uses).

## Quick Start

### Basic Importer Configuration

```python
from beancount_tools_collection.importers import (
    finpension,
    ibkr,
    revolut,
    viac,
    viseca_csv,
    yuh,
)

# Example configuration
CONFIG = [
    # Swiss institutions
    finpension.FinpensionImporter(
        root_account='Assets:Pension:S3:Finpension:Portfolio1',
        deposit_account='Assets:Checking',
        isin_lookup={
            'CH0132501898': 'CH0132501898',  # Example ISIN mapping
            # ... more ISINs
        },
    ),
    viac.ViacImporter(
        root_account='Assets:Pension:S3a:Viac:Portfolio1',
        deposit_account='Assets:Checking',
        share_lookup={
            'UBS SMI': {'isin': 'CH0033782431', 'symbol': 'CH0033782431'},
            # ... more share mappings
        },
    ),
    yuh.YuhImporter(account='Assets:Cash:Yuh:CHF', goals_base_account='Assets:Savings:Yuh'),
    viseca_csv.VisecaCsvImporter(
        account='Liabilities:CreditCard:Viseca',
        # The monthly "Ihre Zahlung - Danke" row settles the previous bill.
        # Point it at the account you pay from and the liability returns to
        # zero each cycle, so a dropped transaction shows up as a balance error.
        settlement_account='Assets:Cash:Yuh:CHF',
        # Optional. Unmapped merchants stay single-legged on purpose.
        merchant_map={
            'Coop': 'Expenses:Groceries',
            'Migros': 'Expenses:Groceries',
            'SBB CFF FFS': 'Expenses:Transport',
        },
    ),
    # International institutions
    ibkr.IBKRImporter(
        Mainaccount='Assets:Invest:InteractiveBrokers',
        DivAccount='Income:Dividends:InteractiveBrokers',
        WHTAccount='Expenses:Taxes:WithholdingTax',
        PnLAccount='Income:Invest:Gains',
        FeesAccount='Expenses:Invest:Fees',
        configFile='ibkr.yaml',  # Your IBKR FlexQuery config
    ),
    revolut.RevolutImporter('revolut_chf', 'Assets:Cash:Revolut:CHF', 'CHF'),
]
```

### Automatic Categorization

The Viseca CSV export carries no category, so `VisecaCsvImporter` emits
single-legged postings and leaves the expense account to you. Merchants in
`merchant_map` get a balanced expense leg; anything else stays incomplete
for you to fill in Fava.

### Price Fetcher Configuration

```python
# In your beancount price configuration
from beancount_tools_collection.prices import ibkr

# The IBKR price source will be available for bean-price
```

## Documentation

### Importer-Specific Setup

Each importer has specific requirements and configuration options:

- **[Finpension](docs/importers/finpension.md)** - CSV transaction reports
- **[Interactive Brokers](docs/importers/ibkr.md)** - FlexQuery configuration
- **[VIAC](docs/importers/viac.md)** - JSON export setup
- **[Viseca](#viseca-csv-bill-exports)** - CSV bill exports (primary) and JSON (archival)
- **[Yuh](#yuh-csv-exports)** - CSV account-activities exports

### Account Structure Examples

The importers work best with structured account hierarchies:

```
Assets:
  Cash:
    Yuh:
      CHF
      USD
    Revolut:
      CHF
      EUR
  Invest:
    InteractiveBrokers:
      Long-Term:
        VTI
        VXUS
        USD
  Pension:
    S3a:
      Finpension:
        Portfolio1:
          CHF
      Viac:
        Portfolio1:
          CH0132501898

Income:
  Dividends:
    InteractiveBrokers:
      Long-Term:
        USD
  Pension:
    S3a:
      Finpension:
        Portfolio1:
          Interest:
            CHF

Expenses:
  Invest:
    Fees:
      CHF
      USD
  Taxes:
    WithholdingTax
```

## Notes

### Yuh CSV exports

`YuhImporter` reads the account-activities CSV the Yuh app exports. It recognises
the file by its column header, so the download works unrenamed; pass `regex` as
well if you import several Yuh accounts separately.

### Viseca CSV bill exports

`VisecaCsvImporter` reads the CSV bill the Viseca One app exports. It recognises
the file by its column header, so the download works unrenamed; pass
`filename_regex` as well if you import several Viseca accounts separately.

A few behaviours worth knowing:

- **Payments become transfers.** The monthly "Ihre Zahlung - Danke" row settles
  the _previous_ bill. With `settlement_account` set it posts as a balanced
  transfer, so the liability returns to zero each cycle. The JSON importer drops
  these rows, which lets the liability grow without bound.
- **Refunds and foreign-currency rows are flagged `!`** for review, since their
  semantics have not yet been confirmed against real data. Set
  `flag_unverified=False` to turn that off.
- **Amounts are posted verbatim** from `Amount`/`Currency`, which is always the
  settled amount in the card's currency. `OriginalAmount` is recorded as
  metadata but never used for arithmetic: it can differ from `Amount` even at
  exchange rate 1.0 in the same currency.
- **Bad rows are skipped, not fatal**, and the count is reported in the log so
  the loss is never silent. Failures affecting the whole file (unreadable file,
  unexpected header, several cards with no `card_accounts` mapping) raise
  instead, so Fava shows a real error rather than "No entries to import".
- **Entries carry `transactionId`**, the same metadata key the JSON importer
  writes, so re-imports and overlapping bills deduplicate exactly rather than
  heuristically.

### Interactive Brokers sells

Enable **Execution** and **Closed Lots** in the Trades section of your Flex
query. Each sell then closes the lots IBKR closed, matched by open date
(`-3 ACME {2022-09-14} @ 212.35 USD`). This works with Beancount's default
booking method.

IBKR's closed-lot price includes the buy commission, while the importer books
buys at the execution price and the commission as a fee. That's why sells never
write a lot price, and why the profit Beancount computes differs from IBKR's by
the original buy commission. IBKR's own cost basis is kept as `ibkr_cost`
metadata for comparison.

Without Closed Lots, a sell posts `-8 ACME {}` and your ledger's booking method
picks the lots. With several lots of one stock that needs
`option "booking_method" "FIFO"`; the default STRICT method reports an
"Ambiguous matches" error. Sells whose closed lots don't add up, and cancelled
sells, are flagged `!` for review.

### Interactive Brokers errors (v1.1.0+)

IBKR Flex fetch and credential failures (expired/invalid token, bad `ibkr.yaml`, network errors, unparseable statements) now raise typed errors instead of returning an empty entry list. In Fava this surfaces as an import/API error (rather than the yellow "No entries to import from this file." warning that used to appear on hard failures). The CLI exits non-zero with a short remediation message.

Exception messages, log lines, and raised tracebacks redact the Flex token (and do not chain secret-bearing upstream exceptions). If older logs were shared while a token was still live, rotate the token under Reports > Flex Web Service.

IBKR allows one Flex request per second and ten per minute per token, and blocks a token that keeps failing with code 1025 ("Too many failed attempts"). The importer polls within those limits and gives up five minutes after it starts, slow or unanswered requests included. To stop repeated Extract clicks in Fava from adding failed attempts, it then leaves IBKR alone for a while:

| IBKR answer                                                                 | Pause  | Covers                   |
| --------------------------------------------------------------------------- | ------ | ------------------------ |
| 1025, too many failed attempts                                              | 15 min | every query of the token |
| Inactive service, expired, invalid or IP-restricted token (1011–1013, 1015) | 5 min  | every query of the token |
| Invalid query, account or request (1010, 1014, 1016, 1020)                  | 5 min  | that query               |
| 1018, too many requests                                                     | 1 min  | every query of the token |

During a pause the import fails with "IBKR import skipped" and says how long is left. A new token is not affected; restarting Fava clears all pauses. Pauses only last as long as the Fava process: each command-line import starts fresh, so wait out the time the error message gives before running it again. A downloaded statement is reused for five minutes. If 1025 persists after the pause, generate a new Flex token and leave "Valid For IP Address" empty.

### Publishing to PyPI

Requires Python 3.11+. Bump the version, commit, then push a matching tag:

```bash
uv version --bump minor   # or patch / major
git add pyproject.toml uv.lock
git commit -m "Release 1.3.0"
git tag v1.3.0
git push origin main --tags
```

The tag must equal `v` plus the version in `pyproject.toml`. A mismatch fails
before any PyPI contact. That tag push is the only publish trigger: the
Release workflow tests, uploads to PyPI, then creates the matching GitHub
Release. Creating or editing a Release in the GitHub UI does not upload again.

## Contributing

We welcome contributions! Here's how you can help:

1. **Add new importers** for financial institutions
2. **Improve existing importers** with bug fixes and features
3. **Add price fetchers** for different data sources
4. **Create plugins** for common beancount workflows
5. **Improve documentation** and examples

### Development Setup

Same as [From Source](#from-source): `make install` and
`uv run pre-commit install`.

```bash
make check    # lint, format check, lockfile
make format   # rewrite the tree
make test     # pytest with a 30% coverage ratchet
make build    # sdist + wheel, no local path sources
make audit    # zizmor over the workflows
```

CI and `pre-push` call these targets. Do not invoke Ruff directly if you
want the same result as the pipeline.

`tests/test_ibkr_live.py` calls the real IBKR Flex API and is skipped unless you
opt in with `IBKR_LIVE=1` and put `IBKR_TOKEN` / `IBKR_QUERY_ID` in `.env`.
Failed live requests count towards IBKR's 1025 block on your token.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- The [beancount](https://github.com/beancount/beancount) project for the excellent accounting framework
- Various open-source beancount importers that served as inspiration

## Support

- 📖 [Documentation](https://github.com/mekanics/beancount-tools-collection#readme)
- 🐛 [Bug Reports](https://github.com/mekanics/beancount-tools-collection/issues)
- 💬 [Discussions](https://github.com/mekanics/beancount-tools-collection/discussions)

---

**Made with ❤️ for personal finance tracking**
