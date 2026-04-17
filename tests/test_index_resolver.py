"""Unit tests for IndexMembershipResolver — cache-first index tier resolution."""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from influence_monitor.market_data.index_resolver import (
    IndexMembershipResolver,
    _finviz_lookup,
    _is_fresh,
)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

_TODAY = date.today().isoformat()
_STALE = (date.today() - timedelta(days=10)).isoformat()
_FRESH = (date.today() - timedelta(days=2)).isoformat()

# Simulated static lists: AAPL and TSLA in SP500, DASH in NDX-only
_SP500_SET = {"AAPL", "TSLA", "MSFT", "GOOGL"}
_NDX_SET = {"AAPL", "TSLA", "MSFT", "GOOGL", "DASH", "MDB"}


def _make_repo() -> MagicMock:
    """Create a mock DatabaseRepository with async methods."""
    repo = MagicMock()
    repo.get_index_membership = AsyncMock(return_value=None)
    repo.upsert_index_membership = AsyncMock()
    repo.bulk_upsert_index_membership_if_stale = AsyncMock(return_value=0)
    repo.get_stale_index_entries = AsyncMock(return_value=[])
    return repo


def _make_resolver(repo: MagicMock | None = None) -> IndexMembershipResolver:
    """Create a resolver with pre-loaded static lists (skipping file I/O)."""
    if repo is None:
        repo = _make_repo()
    resolver = IndexMembershipResolver(repo)
    resolver._sp500_tickers = _SP500_SET.copy()
    resolver._ndx_tickers = _NDX_SET.copy()
    return resolver


# ------------------------------------------------------------------
# _is_fresh
# ------------------------------------------------------------------


class TestIsFresh:
    def test_today_is_fresh(self) -> None:
        assert _is_fresh(_TODAY, ttl_days=7) is True

    def test_two_days_ago_is_fresh(self) -> None:
        assert _is_fresh(_FRESH, ttl_days=7) is True

    def test_ten_days_ago_is_stale(self) -> None:
        assert _is_fresh(_STALE, ttl_days=7) is False

    def test_exactly_seven_days_is_stale(self) -> None:
        boundary = (date.today() - timedelta(days=7)).isoformat()
        assert _is_fresh(boundary, ttl_days=7) is False

    def test_six_days_is_fresh(self) -> None:
        boundary = (date.today() - timedelta(days=6)).isoformat()
        assert _is_fresh(boundary, ttl_days=7) is True


# ------------------------------------------------------------------
# _finviz_lookup
# ------------------------------------------------------------------


class TestFinvizLookup:
    @patch("influence_monitor.market_data.index_resolver.Finviz", create=True)
    def test_sp500_detected(self, _mock: MagicMock) -> None:
        with patch(
            "finvizfinance.quote.finvizfinance",
        ) as mock_finviz:
            mock_stock = MagicMock()
            mock_stock.ticker_fundament.return_value = {"Index": "S&P 500"}
            mock_finviz.return_value = mock_stock

            assert _finviz_lookup("AAPL") == "SP500"

    @patch("finvizfinance.quote.finvizfinance")
    def test_no_index_returns_micro(self, mock_finviz: MagicMock) -> None:
        mock_stock = MagicMock()
        mock_stock.ticker_fundament.return_value = {"Index": "-"}
        mock_finviz.return_value = mock_stock

        assert _finviz_lookup("GME") == "MICRO"

    @patch("finvizfinance.quote.finvizfinance")
    def test_empty_index_returns_micro(self, mock_finviz: MagicMock) -> None:
        mock_stock = MagicMock()
        mock_stock.ticker_fundament.return_value = {"Index": ""}
        mock_finviz.return_value = mock_stock

        assert _finviz_lookup("FNMA") == "MICRO"

    @patch(
        "finvizfinance.quote.finvizfinance",
        side_effect=Exception("connection error"),
    )
    def test_exception_returns_micro(self, _mock: MagicMock) -> None:
        assert _finviz_lookup("BADTICKER") == "MICRO"


# ------------------------------------------------------------------
# IndexMembershipResolver.resolve — cache hits
# ------------------------------------------------------------------


class TestResolveFromCache:
    @pytest.mark.asyncio
    async def test_fresh_cache_hit_returns_immediately(self) -> None:
        repo = _make_repo()
        repo.get_index_membership.return_value = {
            "ticker": "AAPL",
            "index_tier": "SP500",
            "market_cap_b": None,
            "last_updated": _FRESH,
        }

        resolver = _make_resolver(repo)
        tier = await resolver.resolve("AAPL")

        assert tier == "SP500"
        repo.upsert_index_membership.assert_not_called()

    @pytest.mark.asyncio
    async def test_stale_cache_re_resolves(self) -> None:
        repo = _make_repo()
        repo.get_index_membership.return_value = {
            "ticker": "AAPL",
            "index_tier": "SP500",
            "market_cap_b": None,
            "last_updated": _STALE,
        }

        resolver = _make_resolver(repo)
        tier = await resolver.resolve("AAPL")

        assert tier == "SP500"
        # Should have written back to cache
        repo.upsert_index_membership.assert_called_once_with("AAPL", "SP500")


# ------------------------------------------------------------------
# IndexMembershipResolver.resolve — static list fallback
# ------------------------------------------------------------------


class TestResolveFromStaticLists:
    @pytest.mark.asyncio
    async def test_aapl_resolves_sp500(self) -> None:
        """AAPL is in the SP500 set → SP500."""
        resolver = _make_resolver()
        assert await resolver.resolve("AAPL") == "SP500"

    @pytest.mark.asyncio
    async def test_tsla_resolves_sp500(self) -> None:
        """TSLA is in both SP500 and NDX → SP500 (SP500 takes priority)."""
        resolver = _make_resolver()
        assert await resolver.resolve("TSLA") == "SP500"

    @pytest.mark.asyncio
    async def test_dash_resolves_ndx(self) -> None:
        """DASH is in NDX but NOT SP500 → NDX."""
        resolver = _make_resolver()
        assert await resolver.resolve("DASH") == "NDX"

    @pytest.mark.asyncio
    async def test_case_insensitive(self) -> None:
        """Lowercase input is normalised to uppercase."""
        resolver = _make_resolver()
        assert await resolver.resolve("aapl") == "SP500"


# ------------------------------------------------------------------
# IndexMembershipResolver.resolve — finvizfinance fallback
# ------------------------------------------------------------------


class TestResolveFromFinviz:
    @pytest.mark.asyncio
    @patch("influence_monitor.market_data.index_resolver._finviz_lookup")
    async def test_gme_not_in_any_list_calls_finviz(
        self, mock_finviz: MagicMock,
    ) -> None:
        """GME is not in SP500 or NDX → calls finviz → MICRO."""
        mock_finviz.return_value = "MICRO"
        repo = _make_repo()
        resolver = _make_resolver(repo)

        tier = await resolver.resolve("GME")

        assert tier == "MICRO"
        mock_finviz.assert_called_once_with("GME")
        repo.upsert_index_membership.assert_called_once_with("GME", "MICRO")

    @pytest.mark.asyncio
    @patch("influence_monitor.market_data.index_resolver._finviz_lookup")
    async def test_fnma_resolves_micro(self, mock_finviz: MagicMock) -> None:
        """FNMA is OTC, not in any major index → MICRO."""
        mock_finviz.return_value = "MICRO"
        resolver = _make_resolver()

        assert await resolver.resolve("FNMA") == "MICRO"
        mock_finviz.assert_called_once_with("FNMA")

    @pytest.mark.asyncio
    @patch("influence_monitor.market_data.index_resolver._finviz_lookup")
    async def test_finviz_failure_defaults_micro(
        self, mock_finviz: MagicMock,
    ) -> None:
        """finvizfinance exception → MICRO safe default."""
        mock_finviz.return_value = "MICRO"
        resolver = _make_resolver()

        assert await resolver.resolve("UNKNOWN") == "MICRO"


# ------------------------------------------------------------------
# Cache-hit does NOT call finviz
# ------------------------------------------------------------------


class TestCacheSkipsFinviz:
    @pytest.mark.asyncio
    @patch("influence_monitor.market_data.index_resolver._finviz_lookup")
    async def test_fresh_cache_does_not_call_finviz(
        self, mock_finviz: MagicMock,
    ) -> None:
        repo = _make_repo()
        repo.get_index_membership.return_value = {
            "ticker": "GME",
            "index_tier": "MICRO",
            "market_cap_b": None,
            "last_updated": _FRESH,
        }

        resolver = _make_resolver(repo)
        tier = await resolver.resolve("GME")

        assert tier == "MICRO"
        mock_finviz.assert_not_called()


# ------------------------------------------------------------------
# refresh_weekly
# ------------------------------------------------------------------


class TestRefreshWeekly:
    @pytest.mark.asyncio
    @patch("influence_monitor.market_data.index_resolver._finviz_lookup")
    async def test_refreshes_stale_entries(
        self, mock_finviz: MagicMock,
    ) -> None:
        mock_finviz.return_value = "MICRO"
        repo = _make_repo()
        repo.get_stale_index_entries.return_value = [
            {"ticker": "AAPL", "index_tier": "SP500", "last_updated": _STALE},
            {"ticker": "GME", "index_tier": "MICRO", "last_updated": _STALE},
        ]

        resolver = _make_resolver(repo)
        await resolver.refresh_weekly()

        # AAPL re-resolved from static list (SP500), no finviz call
        # GME re-resolved via finviz
        assert repo.upsert_index_membership.call_count == 2
        repo.upsert_index_membership.assert_any_call("AAPL", "SP500")
        repo.upsert_index_membership.assert_any_call("GME", "MICRO")
        mock_finviz.assert_called_once_with("GME")

    @pytest.mark.asyncio
    async def test_no_stale_entries_is_noop(self) -> None:
        repo = _make_repo()
        repo.get_stale_index_entries.return_value = []

        resolver = _make_resolver(repo)
        await resolver.refresh_weekly()

        repo.upsert_index_membership.assert_not_called()


# ------------------------------------------------------------------
# initialize — populate from static
# ------------------------------------------------------------------


class TestInitialize:
    @pytest.mark.asyncio
    @patch("influence_monitor.market_data.index_resolver._load_ndx_tickers")
    @patch("influence_monitor.market_data.index_resolver._load_sp500_tickers")
    async def test_initialize_loads_lists_and_populates(
        self,
        mock_sp500: MagicMock,
        mock_ndx: MagicMock,
    ) -> None:
        mock_sp500.return_value = {"AAPL", "MSFT"}
        mock_ndx.return_value = {"AAPL", "DASH"}

        repo = _make_repo()
        resolver = IndexMembershipResolver(repo)
        await resolver.initialize()

        assert resolver._sp500_tickers == {"AAPL", "MSFT"}
        assert resolver._ndx_tickers == {"AAPL", "DASH"}

        # bulk_upsert called with SP500 entries + NDX-only entries
        repo.bulk_upsert_index_membership_if_stale.assert_called_once()
        call_args = repo.bulk_upsert_index_membership_if_stale.call_args
        entries = call_args[0][0]
        tickers_and_tiers = {(t, tier) for t, tier in entries}

        # AAPL → SP500, MSFT → SP500, DASH → NDX (NDX-only)
        assert ("AAPL", "SP500") in tickers_and_tiers
        assert ("MSFT", "SP500") in tickers_and_tiers
        assert ("DASH", "NDX") in tickers_and_tiers
        # AAPL should NOT appear as NDX (it's in SP500)
        assert ("AAPL", "NDX") not in tickers_and_tiers
