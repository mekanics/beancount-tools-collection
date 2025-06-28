# Beancount Tools Collection

🧮 My personal collection of beancount tools including importers, price fetchers, plugins, and utilities for various financial institutions.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub issues](https://img.shields.io/github/issues/mekanics/beancount-tools-collection)](https://github.com/mekanics/beancount-tools-collection/issues)

## Features

### 📥 Data Importers

**Swiss Institutions:**

- **Yuh** - CSV exports
- **Viseca** - JSON transaction exports (including Migros Cumulus Credit Card)
- **VIAC** - JSON transaction exports (pillar 2 & 3a)
- **Finpension** - CSV transaction reports (pillar 3a)

**International Institutions:**

- **Interactive Brokers** - FlexQuery XML reports (global)
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

### From PyPI (when published)

```bash
pip install beancount-tools-collection
```

### From Source

```bash
git clone https://github.com/mekanics/beancount-tools-collection.git
cd beancount-tools-collection
pip install -e .
```

## Quick Start

### Basic Importer Configuration

```python
from beancount_tools_collection.importers import (
    finpension, ibkr, revolut,
    viac, viseca, yuh
)

# Example configuration
CONFIG = [
    # Swiss institutions
    finpension.FinpensionImporter(
        root_account="Assets:Pension:S3:Finpension:Portfolio1",
        deposit_account="Assets:Checking",
        isin_lookup={
            "CH0132501898": "CH0132501898",  # Example ISIN mapping
            # ... more ISINs
        }
    ),

    viac.ViacImporter(
        root_account="Assets:Pension:S3a:Viac:Portfolio1",
        deposit_account="Assets:Checking",
        share_lookup={
            "UBS SMI": {"isin": "CH0033782431", "symbol": "CH0033782431"},
            # ... more share mappings
        }
    ),

    yuh.YuhImporter(
        account="Assets:Cash:Yuh:CHF",
        goals_base_account="Assets:Savings:Yuh"
    ),

    # International institutions
    ibkr.IBKRImporter(
        Mainaccount="Assets:Invest:InteractiveBrokers",
        DivAccount="Income:Dividends:InteractiveBrokers",
        WHTAccount="Expenses:Taxes:WithholdingTax",
        PnLAccount="Income:Invest:Gains",
        FeesAccount="Expenses:Invest:Fees",
        configFile="ibkr.yaml"  # Your IBKR FlexQuery config
    ),

    revolut.RevolutImporter(
        "revolut_chf",
        "Assets:Cash:Revolut:CHF",
        "CHF"
    ),
]
```

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
- **[Yuh](docs/importers/yuh.md)** - CSV export configuration

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

## Contributing

We welcome contributions! Here's how you can help:

1. **Add new importers** for financial institutions
2. **Improve existing importers** with bug fixes and features
3. **Add price fetchers** for different data sources
4. **Create plugins** for common beancount workflows
5. **Improve documentation** and examples

### Development Setup

```bash
git clone https://github.com/mekanics/beancount-tools-collection.git
cd beancount-tools-collection
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest
```

### Code Formatting

```bash
black src/
isort src/
```

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
