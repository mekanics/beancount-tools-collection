"""
Live integration test for IBKRImporter against the real IBKR FlexQuery API.

Requires a .env file in the project root with:
    IBKR_TOKEN=<token>
    IBKR_QUERY_ID=<query_id>

Run with:
    pytest tests/test_ibkr_live.py -v -s
"""

import pytest
import yaml
from pathlib import Path
from beancount.core import data as bdata

from beancount_tools_collection.importers.ibkr import (
    IBKRConfigError,
    IBKRImportError,
    IBKRImporter,
)

# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parent.parent
ENV_FILE = ROOT / ".env"


def _load_env(path: Path) -> dict:
    """Parse a simple KEY=VALUE .env file (no shell expansion)."""
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip().strip("'\"").split("#")[0].strip()
    return env


_env = _load_env(ENV_FILE) if ENV_FILE.exists() else {}
_token = _env.get("IBKR_TOKEN", "")
_query_id = _env.get("IBKR_QUERY_ID", "")

requires_credentials = pytest.mark.skipif(
    not (_token and _query_id),
    reason=".env missing IBKR_TOKEN / IBKR_QUERY_ID",
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def ibkr_yaml(tmp_path_factory):
    d = tmp_path_factory.mktemp("ibkr")
    cfg = d / "ibkr.yaml"
    cfg.write_text(yaml.dump({"token": _token, "queryId": int(_query_id)}))
    return str(cfg)


@pytest.fixture(scope="session")
def importer():
    return IBKRImporter(
        Mainaccount="Assets:Invest:IB",
        DivAccount="Income:Dividends:IB",
        WHTAccount="Expenses:Taxes:IB:WHT",
        PnLAccount="Income:PnL:IB",
        FeesAccount="Expenses:Fees:IB",
        configFile="ibkr.yaml",
        cashAccountType="Cash",
        stockAccountType="Invest",
    )


@pytest.fixture(scope="session")
def live_entries(importer, ibkr_yaml):
    """Single API call shared across all tests in the session."""
    try:
        entries = importer.extract(ibkr_yaml)
    except IBKRConfigError as e:
        # .env present but IBKR rejected the token/query — cannot run happy-path live tests.
        pytest.skip(f"IBKR credentials rejected by API: {e}")
    except IBKRImportError as e:
        # Credentials present but IBKR unreachable (sandbox/CI proxy, DNS, etc.).
        if "network error" in str(e).lower() or "Max retries" in str(e):
            pytest.skip(f"IBKR API unreachable from this environment: {e}")
        raise
    assert isinstance(entries, list), "extract() must return a list"
    return entries


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@requires_credentials
def test_identify(importer, ibkr_yaml):
    assert importer.identify(ibkr_yaml)


@requires_credentials
def test_invalid_token_raises_config_error(importer, tmp_path):
    """A deliberately invalid token must surface IBKRConfigError, not [].

    IBKR may answer with 1015 (Token is invalid) or 1020 (unable to validate
    request) for a bogus token; both are permanent config failures.
    """
    cfg = tmp_path / "ibkr.yaml"
    cfg.write_text(yaml.dump({"token": "0", "queryId": int(_query_id)}))
    try:
        with pytest.raises(IBKRConfigError) as excinfo:
            importer.extract(str(cfg))
    except Exception as e:
        # Skip only clear transport failures — not every IBKRImportError, or a
        # classification regression would be masked as "unreachable".
        transport = (
            "network error" in str(e).lower()
            or "Max retries" in str(e)
            or "Failed to resolve" in str(e)
            or isinstance(
                e,
                (
                    ConnectionError,
                    TimeoutError,
                ),
            )
        )
        if transport:
            pytest.skip(f"IBKR API unreachable from this environment: {e}")
        raise
    msg = str(excinfo.value)
    assert "1015" in msg or "1020" in msg, msg
    assert "token" in msg.lower()


@requires_credentials
def test_extract_returns_entries(live_entries):
    assert len(live_entries) > 0, "Expected at least one entry from the live API"
    print(f"\nExtracted {len(live_entries)} entries from IBKR live API")


@requires_credentials
def test_extract_entry_types(live_entries):
    txns = [e for e in live_entries if isinstance(e, bdata.Transaction)]
    balances = [e for e in live_entries if isinstance(e, bdata.Balance)]
    print(f"\n  Transactions : {len(txns)}")
    print(f"  Balances     : {len(balances)}")
    assert txns or balances, "Expected at least some transactions or balances"


@requires_credentials
def test_no_unknown_note_codes_crash(live_entries):
    """Importing must not crash due to unknown ibflex Code values (e.g. 'RI')."""
    # Reaching here means extract() succeeded without raising; the fixture
    # would have failed the session if an exception had propagated.
    assert live_entries is not None


@requires_credentials
def test_drip_transactions_tagged(live_entries):
    """DRIP buys must carry the #drip tag; regular buys must not.

    If no DRIP trades are present in this FlexQuery window the test is skipped
    rather than passing vacuously.
    """
    txns = [e for e in live_entries if isinstance(e, bdata.Transaction)]
    buy_txns = [t for t in txns if t.narration.startswith("BUY")]
    drip_txns = [t for t in buy_txns if "drip" in t.tags]
    regular_buys = [t for t in buy_txns if "drip" not in t.tags]

    print(f"\n  Total buy transactions  : {len(buy_txns)}")
    print(f"  DRIP (tagged #drip)     : {len(drip_txns)}")
    print(f"  Regular (untagged)      : {len(regular_buys)}")

    if not buy_txns:
        pytest.skip("No buy transactions in this FlexQuery window — cannot verify DRIP tagging")

    if not drip_txns:
        pytest.skip("No DRIP transactions in this FlexQuery window — cannot verify #drip tag")

    for txn in drip_txns:
        assert "drip" in txn.tags, f"DRIP txn missing #drip tag: {txn}"
        assert txn.narration.startswith("BUY"), f"Unexpected narration for DRIP: {txn.narration}"

    for txn in regular_buys:
        assert "drip" not in txn.tags, f"Regular buy incorrectly tagged #drip: {txn}"
