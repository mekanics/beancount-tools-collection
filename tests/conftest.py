import pytest

from beancount_tools_collection.utils import ibkr_flex


@pytest.fixture(autouse=True)
def fresh_ibkr_fetcher(monkeypatch):
    """Isolate the process-wide IBKR statement cache and cooldowns per test."""
    monkeypatch.setattr(ibkr_flex, '_fetcher', ibkr_flex.StatementFetcher())
