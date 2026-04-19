"""Unit tests for MarketCapResolver (TASK-009).

Mocks SignalRepository and finvizfinance to keep tests pure and network-free.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from influence_monitor.scoring.market_cap_resolver import (
    MarketCapResolver,
    _classify,
    _parse_market_cap_to_millions,
)


# ---------------------------------------------------------------------------
# Unit tests for parser and classifier helpers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected_m", [
    ("3911.50B", 3_911_500.0),
    ("498.22M", 498.22),
    ("1.23T", 1_230_000.0),
    ("150K", 0.15),
    ("10.00B", 10_000.0),
    ("", None),
    ("-", None),
    ("N/A", None),
    ("n/a", None),
])
def test_parse_market_cap_to_millions(raw: str, expected_m: float | None) -> None:
    result = _parse_market_cap_to_millions(raw)
    if expected_m is None:
        assert result is None
    else:
        assert result == pytest.approx(expected_m, rel=1e-3)


@pytest.mark.parametrize("market_cap_m,expected_class", [
    (200_000, "Mega"),      # == Mega threshold → Mega
    (250_000, "Mega"),
    (10_000, "Large"),      # == Large threshold → Large
    (50_000, "Large"),
    (2_000, "Mid"),         # == Mid threshold → Mid
    (5_000, "Mid"),
    (300, "Small"),         # == Small threshold → Small
    (1_000, "Small"),
    (299, "Micro"),
    (0, "Micro"),
    (None, "Micro"),
])
def test_classify(market_cap_m: float | None, expected_class: str) -> None:
    assert _classify(market_cap_m) == expected_class


# ---------------------------------------------------------------------------
# MarketCapResolver integration (mocked repo + finvizfinance)
# ---------------------------------------------------------------------------

def _make_repo(cached_row: dict | None = None) -> MagicMock:
    repo = MagicMock()
    repo.get_cached_market_cap.return_value = cached_row
    return repo


def _make_fundamentals(market_cap: str, sector: str = "Finance", industry: str = "Banks") -> dict:
    return {"Market Cap": market_cap, "Sector": sector, "Industry": industry}


# Scenario 1: "3911.50B" → 3_911_500M → Mega
def test_resolve_mega_cap(tmp_path) -> None:
    """'3911.50B' → Mega; finvizfinance called; result cached."""
    repo = _make_repo(cached_row=None)

    with patch("finvizfinance.quote.finvizfinance") as mock_fvf:
        mock_stock = MagicMock()
        mock_fvf.return_value = mock_stock
        mock_stock.TickerFundamentals.return_value = _make_fundamentals("3911.50B")

        resolver = MarketCapResolver(repo)
        result = resolver.resolve("BRK.A")

    assert result == "Mega"
    repo.upsert_price_cache.assert_called_once()
    call_kwargs = repo.upsert_price_cache.call_args.kwargs
    assert call_kwargs["market_cap_class"] == "Mega"
    assert call_kwargs["ticker"] == "BRK.A"


# Scenario 2: "498.22M" → 498M → Small
def test_resolve_small_cap() -> None:
    """'498.22M' → Small; correct class returned and cached."""
    repo = _make_repo(cached_row=None)

    with patch("finvizfinance.quote.finvizfinance") as mock_fvf:
        mock_stock = MagicMock()
        mock_fvf.return_value = mock_stock
        mock_stock.TickerFundamentals.return_value = _make_fundamentals("498.22M")

        resolver = MarketCapResolver(repo)
        result = resolver.resolve("SMID")

    assert result == "Small"


# Scenario 3: "" → None → Micro
def test_resolve_empty_market_cap_returns_micro() -> None:
    """Empty market cap string → Micro; result still cached."""
    repo = _make_repo(cached_row=None)

    with patch("finvizfinance.quote.finvizfinance") as mock_fvf:
        mock_stock = MagicMock()
        mock_fvf.return_value = mock_stock
        mock_stock.TickerFundamentals.return_value = {"Market Cap": "", "Sector": None, "Industry": None}

        resolver = MarketCapResolver(repo)
        result = resolver.resolve("OTC")

    assert result == "Micro"
    repo.upsert_price_cache.assert_called_once()
    call_kwargs = repo.upsert_price_cache.call_args.kwargs
    assert call_kwargs["market_cap_class"] == "Micro"
    assert call_kwargs["market_cap_b"] is None


# Scenario 4: Cache hit → returns cached class, no finvizfinance call
def test_cache_hit_skips_finvizfinance() -> None:
    """Cache hit → returns stored market_cap_class; finvizfinance not called."""
    cached = {
        "ticker": "AAPL",
        "market_cap_b": 3_200.0,
        "market_cap_class": "Mega",
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "last_updated": "2026-04-18 10:00:00",
    }
    repo = _make_repo(cached_row=cached)

    with patch("finvizfinance.quote.finvizfinance") as mock_fvf:
        resolver = MarketCapResolver(repo)
        result = resolver.resolve("AAPL")

    assert result == "Mega"
    mock_fvf.assert_not_called()
    repo.upsert_price_cache.assert_not_called()


# Scenario 5: Cache miss → finvizfinance called, result cached
def test_cache_miss_calls_finvizfinance_and_caches() -> None:
    """Cache miss → finvizfinance called; repo.upsert_price_cache called once."""
    repo = _make_repo(cached_row=None)

    with patch("finvizfinance.quote.finvizfinance") as mock_fvf:
        mock_stock = MagicMock()
        mock_fvf.return_value = mock_stock
        mock_stock.TickerFundamentals.return_value = _make_fundamentals("25.50B", "Technology", "Software")

        resolver = MarketCapResolver(repo)
        result = resolver.resolve("MSFT")

    assert result == "Large"
    mock_fvf.assert_called_once()
    repo.upsert_price_cache.assert_called_once()


# Scenario 6: finvizfinance raises → returns "Micro", logs WARNING, no raise
def test_finvizfinance_exception_returns_micro_and_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """finvizfinance raising → returns 'Micro', logs WARNING, does not raise."""
    repo = _make_repo(cached_row=None)

    with patch("finvizfinance.quote.finvizfinance") as mock_fvf:
        mock_fvf.side_effect = Exception("HTTP 403 Forbidden")

        with caplog.at_level(logging.WARNING, logger="influence_monitor.scoring.market_cap_resolver"):
            resolver = MarketCapResolver(repo)
            result = resolver.resolve("UNKN")

    assert result == "Micro"
    assert any("Micro" in r.message or "403" in r.message for r in caplog.records)
    repo.upsert_price_cache.assert_not_called()


# Scenario 7: ticker is case-insensitive
def test_ticker_normalised_to_upper_case() -> None:
    """Ticker is normalised to upper-case before cache lookup and storage."""
    repo = _make_repo(cached_row=None)

    with patch("finvizfinance.quote.finvizfinance") as mock_fvf:
        mock_stock = MagicMock()
        mock_fvf.return_value = mock_stock
        mock_stock.TickerFundamentals.return_value = _make_fundamentals("10.00B")

        resolver = MarketCapResolver(repo)
        resolver.resolve("tsla")

    mock_fvf.assert_called_once_with("TSLA")
    call_kwargs = repo.upsert_price_cache.call_args.kwargs
    assert call_kwargs["ticker"] == "TSLA"
