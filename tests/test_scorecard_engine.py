"""Unit tests for ScorecardEngine and its helpers.

Covers TASK-014 acceptance criteria:
- return_pct = (close - open) / open * 100, 4 decimal places.
- is_hit: LONG+positive → True; SHORT+negative → True; else False.
- NULL open_price skipped (logged, counted as error).
- Price fetch failure → signal left with NULL close_price.
- Idempotency: signals with close_price already set are not re-fetched.
- Investor accuracy updated after scoring.
- daily_summaries row written (upsert).
- Unit test for _compute_is_hit covers all four combinations.
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from influence_monitor.config import Settings
from influence_monitor.market_data.base import DataUnavailableError
from influence_monitor.scorecard.scorecard_engine import (
    ScorecardEngine,
    _compute_is_hit,
)

_TODAY = date(2026, 4, 15)


# ----------------------------------------------------------------------
# Pure helper tests
# ----------------------------------------------------------------------


class TestComputeIsHit:
    def test_long_positive_return_is_hit(self) -> None:
        assert _compute_is_hit("LONG", 3.5) is True

    def test_long_negative_return_is_miss(self) -> None:
        assert _compute_is_hit("LONG", -2.1) is False

    def test_long_zero_return_is_miss(self) -> None:
        assert _compute_is_hit("LONG", 0.0) is False

    def test_short_negative_return_is_hit(self) -> None:
        assert _compute_is_hit("SHORT", -0.6) is True

    def test_short_positive_return_is_miss(self) -> None:
        assert _compute_is_hit("SHORT", 1.8) is False

    def test_short_zero_return_is_miss(self) -> None:
        assert _compute_is_hit("SHORT", 0.0) is False

    def test_case_insensitive(self) -> None:
        assert _compute_is_hit("long", 1.0) is True
        assert _compute_is_hit("short", -1.0) is True

    def test_unknown_direction_is_miss(self) -> None:
        assert _compute_is_hit("AMBIGUOUS", 5.0) is False


# ----------------------------------------------------------------------
# Engine integration tests (all I/O mocked)
# ----------------------------------------------------------------------


def _settings() -> Settings:
    return Settings()


def _signal_row(
    signal_id: int = 1,
    ticker: str = "FNMA",
    direction: str = "LONG",
    open_price: float | None = 10.0,
    investor_id: int = 1,
    morning_rank: int = 1,
) -> dict[str, Any]:
    return {
        "id": signal_id,
        "ticker": ticker,
        "direction": direction,
        "open_price": open_price,
        "investor_id": investor_id,
        "morning_rank": morning_rank,
        "sector": None,
    }


def _ohlcv(
    open_: float = 10.0,
    high: float = 10.8,
    low: float = 9.9,
    close: float = 10.42,
    volume: int = 1_000_000,
) -> dict[str, Any]:
    return {"open": open_, "high": high, "low": low, "close": close, "volume": volume}


def _make_engine(signals: list[dict], ohlcv_map: dict[str, Any] | None = None) -> tuple[ScorecardEngine, MagicMock]:
    """Build a ScorecardEngine with fully mocked repo and market client."""
    market_client = MagicMock()
    repo = AsyncMock()

    # Repo returns configured signals for scoring
    repo.get_signals_for_scoring.return_value = signals
    # Investor stats
    repo.compute_investor_rolling_accuracy.return_value = (10, 7)
    repo.get_investor_lifetime_stats.return_value = (20, 13)
    repo.update_investor_accuracy.return_value = None
    repo.update_signal_prices.return_value = None
    repo.update_signal_market_context.return_value = None
    repo.upsert_daily_summary.return_value = 1

    # Market client — mock fetch_ohlcv and avg volume
    if ohlcv_map is not None:
        def _fetch_ohlcv(ticker, target_date):
            if ticker in ohlcv_map:
                return ohlcv_map[ticker]
            raise DataUnavailableError(f"No data for {ticker}")
        market_client.fetch_ohlcv.side_effect = _fetch_ohlcv
    else:
        market_client.fetch_ohlcv.return_value = _ohlcv()

    settings = _settings()
    engine = ScorecardEngine(market_client, repo, settings)
    return engine, repo


class TestRunEvening:
    @pytest.mark.asyncio
    async def test_long_hit_return_computed_correctly(self) -> None:
        """LONG signal: open=10.0, close=10.42 → return=+4.2% → HIT."""
        sig = _signal_row(ticker="FNMA", direction="LONG", open_price=10.0)
        engine, repo = _make_engine([sig], {"FNMA": _ohlcv(open_=10.0, close=10.42)})

        with _patch_yf():
            result = await engine.run_evening(_TODAY)

        assert result["hits"] == 1
        assert result["misses"] == 0
        assert result["signals_scored"] == 1

        # Check update_signal_prices called with correct return_pct and is_hit
        call_kwargs = repo.update_signal_prices.call_args
        assert call_kwargs.kwargs["return_pct"] == pytest.approx(4.2, abs=0.001)
        assert call_kwargs.kwargs["is_hit"] is True

    @pytest.mark.asyncio
    async def test_long_miss_computed_correctly(self) -> None:
        """LONG signal: close < open → return negative → MISS."""
        sig = _signal_row(direction="LONG", open_price=10.0)
        engine, repo = _make_engine([sig], {"FNMA": _ohlcv(open_=10.0, close=9.79)})

        with _patch_yf():
            result = await engine.run_evening(_TODAY)

        assert result["hits"] == 0
        assert result["misses"] == 1
        call_kwargs = repo.update_signal_prices.call_args
        assert call_kwargs.kwargs["is_hit"] is False
        assert call_kwargs.kwargs["return_pct"] < 0

    @pytest.mark.asyncio
    async def test_short_hit_negative_return(self) -> None:
        """SHORT signal: close < open → negative return → HIT."""
        sig = _signal_row(ticker="XYZ", direction="SHORT", open_price=50.0)
        engine, repo = _make_engine([sig], {"XYZ": _ohlcv(open_=50.0, close=48.5)})

        with _patch_yf():
            result = await engine.run_evening(_TODAY)

        assert result["hits"] == 1
        call_kwargs = repo.update_signal_prices.call_args
        assert call_kwargs.kwargs["is_hit"] is True
        assert call_kwargs.kwargs["return_pct"] == pytest.approx(-3.0, abs=0.01)

    @pytest.mark.asyncio
    async def test_short_miss_positive_return(self) -> None:
        """SHORT signal: close > open → positive return → MISS."""
        sig = _signal_row(ticker="XYZ", direction="SHORT", open_price=50.0)
        engine, repo = _make_engine([sig], {"XYZ": _ohlcv(open_=50.0, close=51.8)})

        with _patch_yf():
            result = await engine.run_evening(_TODAY)

        assert result["misses"] == 1
        call_kwargs = repo.update_signal_prices.call_args
        assert call_kwargs.kwargs["is_hit"] is False

    @pytest.mark.asyncio
    async def test_return_pct_four_decimal_places(self) -> None:
        """return_pct must be stored to 4 decimal places."""
        sig = _signal_row(open_price=7.13)
        engine, repo = _make_engine([sig], {"FNMA": _ohlcv(open_=7.13, close=7.42)})

        with _patch_yf():
            await engine.run_evening(_TODAY)

        call_kwargs = repo.update_signal_prices.call_args
        ret = call_kwargs.kwargs["return_pct"]
        # Should be rounded to 4 decimal places
        assert ret == round(ret, 4)

    @pytest.mark.asyncio
    async def test_null_open_price_falls_back_to_ohlcv(self) -> None:
        """open_price=NULL in DB falls back to ohlcv['open'] — signal is scored normally."""
        sig = _signal_row(ticker="FNMA", direction="LONG", open_price=None)
        # OHLCV provides both open and close — no separate 9:31 AM fetch needed
        engine, repo = _make_engine([sig], {"FNMA": _ohlcv(open_=10.0, close=10.42)})

        with _patch_yf():
            result = await engine.run_evening(_TODAY)

        assert result["errors"] == 0
        assert result["signals_scored"] == 1
        assert result["hits"] == 1
        call_kwargs = repo.update_signal_prices.call_args
        assert call_kwargs.kwargs["return_pct"] == pytest.approx(4.2, abs=0.001)

    @pytest.mark.asyncio
    async def test_null_open_price_and_ohlcv_open_missing_is_error(self) -> None:
        """open_price=NULL in DB and ohlcv has no 'open' field → error, skipped."""
        sig = _signal_row(open_price=None)
        # OHLCV returns but has no 'open' key
        ohlcv_no_open = {"high": 10.8, "low": 9.9, "close": 10.42, "volume": 1_000_000}
        engine, repo = _make_engine([sig], {"FNMA": ohlcv_no_open})

        with _patch_yf():
            result = await engine.run_evening(_TODAY)

        assert result["errors"] == 1
        assert result["signals_scored"] == 0
        repo.update_signal_prices.assert_not_called()

    @pytest.mark.asyncio
    async def test_price_fetch_failure_leaves_null(self) -> None:
        """Price fetch failure: update_signal_prices called with no args (NULLs)."""
        sig = _signal_row(ticker="FNMA", open_price=10.0)
        market_client = MagicMock()
        market_client.fetch_ohlcv.side_effect = DataUnavailableError("API down")
        repo = AsyncMock()
        repo.get_signals_for_scoring.return_value = [sig]
        repo.update_signal_prices.return_value = None
        repo.update_signal_market_context.return_value = None
        repo.upsert_daily_summary.return_value = 1
        engine = ScorecardEngine(market_client, repo, _settings())

        with _patch_yf():
            result = await engine.run_evening(_TODAY)

        assert result["errors"] == 1
        # update_signal_prices called with only signal_id (no price kwargs)
        repo.update_signal_prices.assert_called_once_with(sig["id"])

    @pytest.mark.asyncio
    async def test_idempotency_no_unscored_signals(self) -> None:
        """When no unscored signals remain, returns zeros without touching DB."""
        engine, repo = _make_engine([])  # empty → already scored

        with _patch_yf():
            result = await engine.run_evening(_TODAY)

        assert result["signals_scored"] == 0
        repo.update_signal_prices.assert_not_called()
        # daily_summaries still written
        repo.upsert_daily_summary.assert_called_once()

    @pytest.mark.asyncio
    async def test_investor_accuracy_updated(self) -> None:
        """Investor accuracy is updated after at least one signal is scored."""
        sig = _signal_row(investor_id=42)
        engine, repo = _make_engine([sig], {"FNMA": _ohlcv()})

        with _patch_yf():
            await engine.run_evening(_TODAY)

        repo.update_investor_accuracy.assert_called_once()
        call_args = repo.update_investor_accuracy.call_args
        # investor_id is the first positional arg
        assert call_args.args[0] == 42

    @pytest.mark.asyncio
    async def test_daily_summary_written(self) -> None:
        """daily_summaries row is always written (even on quiet night)."""
        engine, repo = _make_engine([])

        with _patch_yf():
            await engine.run_evening(_TODAY)

        repo.upsert_daily_summary.assert_called_once()
        kwargs = repo.upsert_daily_summary.call_args.kwargs
        assert kwargs["run_type"] == "evening"
        assert kwargs["summary_date"] == _TODAY.isoformat()

    @pytest.mark.asyncio
    async def test_daily_summary_hit_rate(self) -> None:
        """daily_hit_rate in summary matches computed hits/total."""
        sigs = [
            _signal_row(signal_id=1, ticker="A", direction="LONG", open_price=10.0),
            _signal_row(signal_id=2, ticker="B", direction="LONG", open_price=10.0, investor_id=2),
            _signal_row(signal_id=3, ticker="C", direction="LONG", open_price=10.0, investor_id=3),
        ]
        ohlcv_map = {
            "A": _ohlcv(open_=10.0, close=10.5),   # HIT
            "B": _ohlcv(open_=10.0, close=9.5),    # MISS
            "C": _ohlcv(open_=10.0, close=11.0),   # HIT
        }
        engine, repo = _make_engine(sigs, ohlcv_map)

        with _patch_yf():
            result = await engine.run_evening(_TODAY)

        assert result["hits"] == 2
        assert result["misses"] == 1
        kwargs = repo.upsert_daily_summary.call_args.kwargs
        assert kwargs["daily_hit_rate"] == pytest.approx(2 / 3, abs=0.001)

    @pytest.mark.asyncio
    async def test_multiple_signals_same_investor_one_update(self) -> None:
        """Investor accuracy only updated once even with multiple signals."""
        sigs = [
            _signal_row(signal_id=1, ticker="A", investor_id=7, open_price=10.0),
            _signal_row(signal_id=2, ticker="B", investor_id=7, open_price=10.0),
        ]
        ohlcv_map = {
            "A": _ohlcv(close=10.5),
            "B": _ohlcv(close=10.5),
        }
        engine, repo = _make_engine(sigs, ohlcv_map)

        with _patch_yf():
            await engine.run_evening(_TODAY)

        repo.update_investor_accuracy.assert_called_once()


# ----------------------------------------------------------------------
# Backfill tests
# ----------------------------------------------------------------------


def _backfill_signal(
    signal_id: int = 1,
    ticker: str = "FNMA",
    signal_date: str = "2026-04-08",  # 5 trading days before Apr 15
    open_price: float = 10.0,
) -> dict[str, Any]:
    return {
        "id": signal_id,
        "ticker": ticker,
        "signal_date": signal_date,
        "open_price": open_price,
    }


def _make_backfill_engine(
    pending_5d: list | None = None,
    pending_10d: list | None = None,
    pending_30d: list | None = None,
    ohlcv_map: dict | None = None,
) -> tuple[ScorecardEngine, MagicMock]:
    """Build a ScorecardEngine wired for backfill tests."""
    from influence_monitor.calendar import HolidayCalendar

    market_client = MagicMock()
    repo = AsyncMock()

    def _pending(column, cutoff_date, tenant_id=1):
        if column == "return_5d":
            return pending_5d or []
        if column == "return_10d":
            return pending_10d or []
        if column == "return_30d":
            return pending_30d or []
        return []

    repo.get_signals_pending_backfill.side_effect = _pending
    repo.update_signal_horizon_return.return_value = None

    if ohlcv_map is not None:
        def _fetch(ticker, target_date):
            if ticker in ohlcv_map:
                return ohlcv_map[ticker]
            from influence_monitor.market_data.base import DataUnavailableError
            raise DataUnavailableError(f"no data for {ticker}")
        market_client.fetch_ohlcv.side_effect = _fetch
    else:
        market_client.fetch_ohlcv.return_value = _ohlcv()

    calendar = HolidayCalendar(years=range(2026, 2027))
    engine = ScorecardEngine(market_client, repo, _settings(), calendar=calendar)
    return engine, repo


class TestBackfillReturns:
    @pytest.mark.asyncio
    async def test_signal_6_trading_days_old_gets_5d_return(self) -> None:
        """Signal from 6 trading days ago (> 5) has return_5d populated."""
        # Apr 8 is 5 trading days before Apr 15 (Wed Apr 8, 9, 10, 11(skip wknd), 14, 15)
        # Actually: Apr 8 → Apr 9 → Apr 10 → Apr 13 → Apr 14 → Apr 15 = 5 td
        # So Apr 7 is 6 trading days before Apr 15 → qualifies
        sig = _backfill_signal(signal_date="2026-04-07")
        engine, repo = _make_backfill_engine(
            pending_5d=[sig],
            ohlcv_map={"FNMA": _ohlcv(open_=10.0, close=10.5)},
        )

        with _patch_yf():
            result = await engine.backfill_returns(_TODAY)

        assert result["updated"] >= 1
        repo.update_signal_horizon_return.assert_called()
        call_args = repo.update_signal_horizon_return.call_args_list[0]
        assert call_args.args[1] == "return_5d"
        assert call_args.args[2] == pytest.approx(5.0, abs=0.01)

    @pytest.mark.asyncio
    async def test_signal_too_recent_not_backfilled(self) -> None:
        """Signal from 3 trading days ago is NOT in the 5d pending list — no update."""
        engine, repo = _make_backfill_engine(
            pending_5d=[],  # repo returns empty — signal is too recent
        )

        with _patch_yf():
            result = await engine.backfill_returns(_TODAY)

        assert result["updated"] == 0
        repo.update_signal_horizon_return.assert_not_called()

    @pytest.mark.asyncio
    async def test_signal_31_trading_days_old_gets_30d_return(self) -> None:
        """Signal from 31 trading days ago has return_30d populated."""
        sig = _backfill_signal(signal_date="2026-02-27", open_price=20.0)
        engine, repo = _make_backfill_engine(
            pending_30d=[sig],
            ohlcv_map={"FNMA": _ohlcv(open_=20.0, close=21.0)},
        )

        with _patch_yf():
            result = await engine.backfill_returns(_TODAY)

        assert result["updated"] >= 1
        call_args = repo.update_signal_horizon_return.call_args_list[0]
        assert call_args.args[1] == "return_30d"
        assert call_args.args[2] == pytest.approx(5.0, abs=0.01)

    @pytest.mark.asyncio
    async def test_backfill_fetch_failure_counted_as_error(self) -> None:
        """Price fetch failure increments errors, does not raise."""
        from influence_monitor.market_data.base import DataUnavailableError

        sig = _backfill_signal(signal_date="2026-04-07")
        engine, repo = _make_backfill_engine(
            pending_5d=[sig],
            ohlcv_map={},  # FNMA not in map → raises DataUnavailableError
        )

        with _patch_yf():
            result = await engine.backfill_returns(_TODAY)

        assert result["errors"] == 1
        assert result["updated"] == 0
        repo.update_signal_horizon_return.assert_not_called()

    @pytest.mark.asyncio
    async def test_backfill_is_idempotent(self) -> None:
        """Running backfill twice only updates signals that are still NULL."""
        sig = _backfill_signal(signal_date="2026-04-07")
        engine, repo = _make_backfill_engine(
            pending_5d=[sig],
            ohlcv_map={"FNMA": _ohlcv(close=10.5)},
        )

        with _patch_yf():
            await engine.backfill_returns(_TODAY)
            # Second run: repo returns empty list (already populated)
            repo.get_signals_pending_backfill.side_effect = lambda *a, **kw: []
            result2 = await engine.backfill_returns(_TODAY)

        assert result2["updated"] == 0

    @pytest.mark.asyncio
    async def test_return_computed_correctly(self) -> None:
        """return_5d = (close - open) / open * 100, rounded to 4dp."""
        sig = _backfill_signal(signal_date="2026-04-07", open_price=7.13)
        engine, repo = _make_backfill_engine(
            pending_5d=[sig],
            ohlcv_map={"FNMA": _ohlcv(open_=7.13, close=7.42)},
        )

        with _patch_yf():
            await engine.backfill_returns(_TODAY)

        call_args = repo.update_signal_horizon_return.call_args_list[0]
        ret = call_args.args[2]
        assert ret == round(ret, 4)
        assert ret == pytest.approx((7.42 - 7.13) / 7.13 * 100, abs=0.001)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _patch_yf():
    """Suppress live yfinance calls in unit tests."""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = MagicMock(
        empty=True, mean=MagicMock(return_value=None)
    )
    mock_ticker.info = {}
    return patch("yfinance.Ticker", return_value=mock_ticker)
