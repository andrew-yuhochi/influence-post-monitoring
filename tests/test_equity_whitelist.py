"""Unit tests for SymbolWhitelist."""

import time

import pytest

from influence_monitor.extraction.equity_whitelist import SymbolWhitelist


@pytest.fixture(scope="module")
def whitelist() -> SymbolWhitelist:
    """Load whitelist once for all tests in this module."""
    return SymbolWhitelist.load()


# --- 10 tickers that should pass ---

@pytest.mark.parametrize("ticker", [
    "AAPL",   # S&P 500 — Apple
    "MSFT",   # S&P 500 — Microsoft
    "GOOGL",  # S&P 500 — Alphabet
    "TSLA",   # S&P 500 — Tesla
    "AMZN",   # S&P 500 — Amazon
    "JPM",    # S&P 500 — JPMorgan
    "NVDA",   # S&P 500 — NVIDIA
    "FNMA",   # Manual supplement — Fannie Mae (OTC)
    "FMCC",   # Manual supplement — Freddie Mac (OTC)
    "GME",    # Manual supplement — GameStop
])
def test_valid_tickers(whitelist: SymbolWhitelist, ticker: str) -> None:
    assert whitelist.contains(ticker), f"{ticker} should be in whitelist"


# --- 10 common false positives that should fail ---

@pytest.mark.parametrize("ticker", [
    "CEO",    # Common word
    "IPO",    # Finance acronym
    "ETF",    # Finance acronym
    "GDP",    # Economics acronym
    "FED",    # Federal Reserve
    "SEC",    # Securities & Exchange Commission
    "NYSE",   # Exchange name
    "THE",    # English word
    "FOR",    # English word
    "BUT",    # English word
])
def test_false_positives(whitelist: SymbolWhitelist, ticker: str) -> None:
    assert not whitelist.contains(ticker), f"{ticker} should NOT be in whitelist"


def test_case_insensitive(whitelist: SymbolWhitelist) -> None:
    """contains() should be case-insensitive."""
    assert whitelist.contains("aapl")
    assert whitelist.contains("Aapl")
    assert whitelist.contains("AAPL")


def test_load_time() -> None:
    """Whitelist should load in under 5 seconds.

    The threshold is 5s (not 2s) to tolerate network round-trips when
    russell3000.csv is absent and the download URLs fail gracefully.
    S&P 500 is cached after first run so subsequent loads are ~instant.
    """
    start = time.monotonic()
    SymbolWhitelist.load()
    elapsed = time.monotonic() - start
    assert elapsed < 5.0, f"Whitelist load took {elapsed:.2f}s (limit: 5.0s)"


def test_whitelist_not_empty(whitelist: SymbolWhitelist) -> None:
    """Sanity check: whitelist should have at least 500 symbols."""
    assert len(whitelist) >= 500, f"Only {len(whitelist)} symbols loaded"
