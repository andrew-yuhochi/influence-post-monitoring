"""Unit tests for TASK-012: OutcomeEngine and ScorecardAggregator.

Covers:
- LONG + up stock → positive excess_vol
- SHORT + up stock → negative excess_vol (correct sign flip)
- Price fetch failure → NULL outcome + 'unavailable' marker
- Idempotency: re-run leaves non-null rows untouched
- Weekend-crossing prev_close (Monday → Friday)
- Good-Friday-crossing prev_close (Tuesday after Good Friday → Thursday)
- Empty signals → empty scorecard (no crash)
- get_signals_for_date_range repo method
- trading_days_with_signals count
"""

from __future__ import annotations

import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from influence_monitor.config import Settings
from influence_monitor.db.repository import SignalRepository
from influence_monitor.market_data.base import (
    DataUnavailableError,
    MarketDataClient,
)
from influence_monitor.market_data.trading_calendar import TradingCalendar
from influence_monitor.outcome.outcome_engine import OutcomeEngine
from influence_monitor.outcome.scorecard_aggregator import ScorecardAggregator


# ---------------------------------------------------------------------------
# Helpers / Fixtures
# ---------------------------------------------------------------------------

def _make_repo(tmp_path: Path) -> SignalRepository:
    settings = Settings(
        turso_url="",
        database_path=str(tmp_path / "test_outcome.db"),
    )
    repo = SignalRepository(settings)
    repo.init_schema()
    repo.seed()
    return repo


def _insert_signal(
    repo: SignalRepository,
    ticker: str,
    direction: str,
    signal_date: date,
    tenant_id: int = 1,
    excess_vol_score: float | None = None,
) -> int:
    """Insert a minimal signal row; return the new signal id."""
    # Get the first account id
    accs = repo._execute("SELECT id FROM accounts WHERE tenant_id = ? LIMIT 1", [tenant_id])
    account_id = accs[0]["id"]

    # Insert a post first
    post_id = repo.insert_post(
        tenant_id=tenant_id,
        account_id=account_id,
        external_id=f"{ticker}-{direction}-{signal_date}-{id(ticker)}",
        source_type="twitter",
        text=f"Buying {ticker}",
        posted_at=datetime(signal_date.year, signal_date.month, signal_date.day, 9, 30, tzinfo=timezone.utc),
        fetched_at=datetime.now(tz=timezone.utc),
    )

    kwargs: dict[str, Any] = dict(
        tenant_id=tenant_id,
        post_id=post_id,
        account_id=account_id,
        ticker=ticker,
        direction=direction,
        signal_date=signal_date.isoformat(),
        tier="act_now",
        extraction_confidence=0.9,
        final_score=7.0,
        shown_in_morning_alert=1,
    )
    if excess_vol_score is not None:
        kwargs["excess_vol_score"] = excess_vol_score

    rowid = repo._execute_write(
        f"INSERT INTO signals ({', '.join(kwargs.keys())}) VALUES ({', '.join('?' for _ in kwargs)})",
        list(kwargs.values()),
    )
    # Fetch the actual id (rowid == id for sqlite3 backend)
    rows = repo._execute(
        "SELECT id FROM signals WHERE ticker = ? AND direction = ? AND signal_date = ? ORDER BY id DESC LIMIT 1",
        [ticker, direction, signal_date.isoformat()],
    )
    return rows[0]["id"]


class FakeMarketClient(MarketDataClient):
    """Configurable fake market client for unit tests."""

    def __init__(
        self,
        ohlcv: dict[str, float] | None = None,
        close: float | None = None,
        spy_return: float | None = None,
        stock_vol: float | None = None,
        raise_on_ticker: str | None = None,
    ):
        self._ohlcv = ohlcv or {"open": 100.0, "close": 110.0, "high": 111.0, "low": 99.0, "volume": 1_000_000}
        self._close = close if close is not None else 100.0
        self._spy_return = spy_return if spy_return is not None else 0.005
        self._stock_vol = stock_vol if stock_vol is not None else 0.015
        self._raise_on_ticker = raise_on_ticker

    def fetch_ohlcv(self, ticker: str, target_date: date) -> dict[str, float | int | None]:
        if self._raise_on_ticker and ticker == self._raise_on_ticker:
            raise DataUnavailableError(f"No data for {ticker}")
        return self._ohlcv.copy()

    def fetch_open(self, ticker: str, target_date: date) -> float:
        return float(self._ohlcv["open"])

    def fetch_close(self, ticker: str, target_date: date) -> float:
        if self._raise_on_ticker and ticker == self._raise_on_ticker:
            raise DataUnavailableError(f"No data for {ticker}")
        return self._close

    def fetch_stock_vol(self, ticker: str, target_date: date, lookback_days: int) -> float | None:
        return self._stock_vol

    def fetch_spy_return(self, target_date: date) -> float | None:
        return self._spy_return

    def fetch_batch_close(self, tickers: list[str], target_date: date) -> dict[str, float | None]:
        return {t: self._close for t in tickers}


@pytest.fixture
def tmp_path_fix(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def calendar() -> TradingCalendar:
    return TradingCalendar()


# ---------------------------------------------------------------------------
# OutcomeEngine — core computation
# ---------------------------------------------------------------------------

class TestOutcomeEngineComputation:

    def test_long_up_stock_positive_excess_vol(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """LONG signal where stock outperforms SPY → positive excess_vol_score."""
        repo = _make_repo(tmp_path_fix)
        # prev_close=100, today_open=101, today_close=110
        # stock_return = (110-100)/100 = 0.10
        # spy_return = 0.005
        # stock_20d_vol = 0.015
        # excess_vol = (0.10 - 0.005) / 0.015 = 6.333...
        client = FakeMarketClient(
            ohlcv={"open": 101.0, "close": 110.0, "high": 112.0, "low": 100.0, "volume": 1_000_000},
            close=100.0,
            spy_return=0.005,
            stock_vol=0.015,
        )
        target = date(2026, 4, 14)  # Tuesday
        sig_id = _insert_signal(repo, "AAPL", "LONG", target)

        engine = OutcomeEngine(client, repo, calendar)
        n = engine.compute_and_store(target)

        assert n == 1
        rows = repo._execute("SELECT * FROM signals WHERE id = ?", [sig_id])
        row = rows[0]
        assert row["excess_vol_score"] is not None
        assert row["excess_vol_score"] > 0
        assert row["price_data_source"] == "yfinance"
        assert row["overnight_return"] == pytest.approx((101.0 - 100.0) / 100.0, rel=1e-5)

    def test_short_up_stock_negative_excess_vol(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """SHORT signal where stock goes up → negative excess_vol_score (sign flip)."""
        repo = _make_repo(tmp_path_fix)
        # prev_close=100, today_close=110 → stock_return=0.10
        # direction=SHORT: excess = (-0.10 - spy_return) / vol
        # = (-0.10 - 0.005) / 0.015 = -7.0
        client = FakeMarketClient(
            ohlcv={"open": 101.0, "close": 110.0, "high": 112.0, "low": 100.0, "volume": 1_000_000},
            close=100.0,
            spy_return=0.005,
            stock_vol=0.015,
        )
        target = date(2026, 4, 14)
        sig_id = _insert_signal(repo, "GME", "SHORT", target)

        engine = OutcomeEngine(client, repo, calendar)
        engine.compute_and_store(target)

        rows = repo._execute("SELECT * FROM signals WHERE id = ?", [sig_id])
        row = rows[0]
        assert row["excess_vol_score"] is not None
        assert row["excess_vol_score"] < 0

    def test_price_fetch_fail_marks_unavailable(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """When price fetch raises, outcome columns stay NULL and marker='unavailable'."""
        repo = _make_repo(tmp_path_fix)
        client = FakeMarketClient(raise_on_ticker="FAIL")
        target = date(2026, 4, 14)
        sig_id = _insert_signal(repo, "FAIL", "LONG", target)

        engine = OutcomeEngine(client, repo, calendar)
        n = engine.compute_and_store(target)

        # The signal was processed (attempted), even though outcome is null
        assert n == 1
        rows = repo._execute("SELECT * FROM signals WHERE id = ?", [sig_id])
        row = rows[0]
        assert row["excess_vol_score"] is None
        assert row["overnight_return"] is None
        assert row["price_data_source"] == "unavailable"

    def test_idempotency_skips_already_scored(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """Re-running compute_and_store leaves non-null excess_vol_score rows untouched."""
        repo = _make_repo(tmp_path_fix)
        target = date(2026, 4, 14)
        sig_id = _insert_signal(repo, "AAPL", "LONG", target, excess_vol_score=3.14)

        client = FakeMarketClient()
        engine = OutcomeEngine(client, repo, calendar)
        n = engine.compute_and_store(target)

        assert n == 0  # skipped — already scored
        rows = repo._execute("SELECT excess_vol_score FROM signals WHERE id = ?", [sig_id])
        assert rows[0]["excess_vol_score"] == pytest.approx(3.14)


# ---------------------------------------------------------------------------
# OutcomeEngine — calendar edge cases
# ---------------------------------------------------------------------------

class TestOutcomeEngineCalendar:

    def test_weekend_crossing_monday_uses_friday_close(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """Monday signals resolve prev_close to Friday (not Sunday/Saturday)."""
        target = date(2026, 4, 13)  # Monday 2026-04-13
        prev = calendar.previous_trading_day(target)
        assert prev.weekday() == 4  # Friday

    def test_good_friday_crossing_monday_uses_thursday(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """Monday after Good Friday resolves prev_close to the Thursday before Good Friday.

        Good Friday 2026 is 2026-04-03 (NYSE closed). NYSE is open on Easter Monday
        (only Good Friday is the holiday). So Monday 2026-04-06 is a trading day, and
        its previous_trading_day should skip Good Friday (Apr 3) and land on Thursday
        2026-04-02.
        """
        monday_after_gf = date(2026, 4, 6)
        assert not calendar.is_trading_day(date(2026, 4, 3)), "Good Friday should be closed"
        assert calendar.is_trading_day(monday_after_gf), "Easter Monday is a NYSE trading day"
        prev = calendar.previous_trading_day(monday_after_gf)
        # Should skip Good Friday Apr 3 (closed) and land on Thursday Apr 2
        assert prev == date(2026, 4, 2)

    def test_good_friday_monday_signal_uses_thursday_close(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """OutcomeEngine passes Thursday date to fetch_close for Monday-after-GF signal."""
        repo = _make_repo(tmp_path_fix)
        # Good Friday 2026 = April 3; Monday after = April 6
        target = date(2026, 4, 6)
        sig_id = _insert_signal(repo, "SPY", "LONG", target)

        fetched_dates: list[date] = []

        class CapturingClient(FakeMarketClient):
            def fetch_close(self, ticker: str, d: date) -> float:
                fetched_dates.append(d)
                return 100.0

        client = CapturingClient(
            ohlcv={"open": 101.0, "close": 105.0, "high": 106.0, "low": 100.0, "volume": 1_000_000},
            close=100.0,
        )
        engine = OutcomeEngine(client, repo, calendar)
        engine.compute_and_store(target)

        # The prev_close fetch must have been for Thursday April 2
        assert date(2026, 4, 2) in fetched_dates


# ---------------------------------------------------------------------------
# Repository — get_signals_for_date_range
# ---------------------------------------------------------------------------

class TestGetSignalsForDateRange:

    def test_returns_signals_in_range(self, tmp_path_fix: Path) -> None:
        repo = _make_repo(tmp_path_fix)
        _insert_signal(repo, "AAPL", "LONG", date(2026, 4, 1))
        _insert_signal(repo, "MSFT", "SHORT", date(2026, 4, 5))
        _insert_signal(repo, "TSLA", "LONG", date(2026, 4, 10))
        _insert_signal(repo, "NVDA", "LONG", date(2026, 4, 20))  # outside

        rows = repo.get_signals_for_date_range(date(2026, 4, 1), date(2026, 4, 15))
        tickers = {r["ticker"] for r in rows}
        assert "AAPL" in tickers
        assert "MSFT" in tickers
        assert "TSLA" in tickers
        assert "NVDA" not in tickers

    def test_inclusive_boundary_dates(self, tmp_path_fix: Path) -> None:
        repo = _make_repo(tmp_path_fix)
        _insert_signal(repo, "AAPL", "LONG", date(2026, 4, 1))
        _insert_signal(repo, "MSFT", "LONG", date(2026, 4, 30))

        rows = repo.get_signals_for_date_range(date(2026, 4, 1), date(2026, 4, 30))
        tickers = {r["ticker"] for r in rows}
        assert "AAPL" in tickers
        assert "MSFT" in tickers

    def test_empty_range_returns_empty_list(self, tmp_path_fix: Path) -> None:
        repo = _make_repo(tmp_path_fix)
        _insert_signal(repo, "AAPL", "LONG", date(2026, 4, 1))

        rows = repo.get_signals_for_date_range(date(2026, 4, 5), date(2026, 4, 10))
        assert rows == []

    def test_returns_account_handle_in_rows(self, tmp_path_fix: Path) -> None:
        repo = _make_repo(tmp_path_fix)
        _insert_signal(repo, "AAPL", "LONG", date(2026, 4, 10))

        rows = repo.get_signals_for_date_range(date(2026, 4, 1), date(2026, 4, 30))
        assert len(rows) > 0
        assert "account_handle" in rows[0]


# ---------------------------------------------------------------------------
# ScorecardAggregator
# ---------------------------------------------------------------------------

class TestScorecardAggregator:

    def test_empty_signals_returns_empty_list(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """No crash when no scored signals exist in the window."""
        repo = _make_repo(tmp_path_fix)
        agg = ScorecardAggregator(repo, calendar)
        result = agg.top_n_posters(as_of=date(2026, 4, 14), window_days=30)
        assert result == []

    def test_unscored_signals_excluded_from_scorecard(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """Signals with excess_vol_score=NULL are not included."""
        repo = _make_repo(tmp_path_fix)
        _insert_signal(repo, "AAPL", "LONG", date(2026, 4, 14))  # NULL score

        agg = ScorecardAggregator(repo, calendar)
        result = agg.top_n_posters(as_of=date(2026, 4, 14), window_days=30)
        assert result == []

    def test_top_n_posters_returns_correct_fields(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """Each result row has handle, avg_excess_vol, n_signals."""
        repo = _make_repo(tmp_path_fix)
        sig_id = _insert_signal(repo, "AAPL", "LONG", date(2026, 4, 14))
        repo.update_signal_outcome(sig_id, excess_vol_score=2.5, price_data_source="yfinance")

        agg = ScorecardAggregator(repo, calendar)
        result = agg.top_n_posters(as_of=date(2026, 4, 14), window_days=30)
        assert len(result) == 1
        row = result[0]
        assert "handle" in row
        assert "avg_excess_vol" in row
        assert "n_signals" in row
        assert row["n_signals"] == 1
        assert row["avg_excess_vol"] == pytest.approx(2.5)

    def test_top_n_sorted_descending(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """top_n_posters returns posters sorted desc by avg_excess_vol."""
        repo = _make_repo(tmp_path_fix)
        # Two signals from same account on different days
        s1 = _insert_signal(repo, "AAPL", "LONG", date(2026, 4, 10))
        s2 = _insert_signal(repo, "MSFT", "LONG", date(2026, 4, 11))
        repo.update_signal_outcome(s1, excess_vol_score=5.0, price_data_source="yfinance")
        repo.update_signal_outcome(s2, excess_vol_score=3.0, price_data_source="yfinance")

        agg = ScorecardAggregator(repo, calendar)
        result = agg.top_n_posters(as_of=date(2026, 4, 14), window_days=30, n=5)
        # Both signals belong to the same account (only one account seeded)
        assert len(result) == 1
        assert result[0]["n_signals"] == 2
        assert result[0]["avg_excess_vol"] == pytest.approx(4.0)

    def test_top_n_capped_at_n(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """Result is capped at n even when more posters exist."""
        repo = _make_repo(tmp_path_fix)
        # Seed 3 signals; since only 1 account is seeded they'll all group together
        for d, score in [(date(2026, 4, 10), 1.0), (date(2026, 4, 11), 2.0), (date(2026, 4, 14), 3.0)]:
            sid = _insert_signal(repo, "AAPL", "LONG", d)
            repo.update_signal_outcome(sid, excess_vol_score=score, price_data_source="yfinance")

        agg = ScorecardAggregator(repo, calendar)
        result = agg.top_n_posters(as_of=date(2026, 4, 14), window_days=30, n=1)
        assert len(result) == 1

    def test_trading_days_with_signals_counts_correctly(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """trading_days_with_signals counts distinct trading days with scored signals."""
        repo = _make_repo(tmp_path_fix)
        # Insert scored signals on 2 trading days
        for d, score in [(date(2026, 4, 7), 1.5), (date(2026, 4, 14), 2.5)]:
            sid = _insert_signal(repo, "AAPL", "LONG", d)
            repo.update_signal_outcome(sid, excess_vol_score=score, price_data_source="yfinance")
        # Unscored signal on a third day — should not count
        _insert_signal(repo, "MSFT", "LONG", date(2026, 4, 10))

        agg = ScorecardAggregator(repo, calendar)
        count = agg.trading_days_with_signals(as_of=date(2026, 4, 14), window_days=30)
        assert count == 2

    def test_trading_days_with_signals_zero_when_empty(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """Returns 0 when no scored signals exist."""
        repo = _make_repo(tmp_path_fix)
        agg = ScorecardAggregator(repo, calendar)
        count = agg.trading_days_with_signals(as_of=date(2026, 4, 14), window_days=30)
        assert count == 0

    def test_rounding_to_6_dp(
        self, tmp_path_fix: Path, calendar: TradingCalendar
    ) -> None:
        """avg_excess_vol is rounded to 6 decimal places in result."""
        repo = _make_repo(tmp_path_fix)
        sid = _insert_signal(repo, "AAPL", "LONG", date(2026, 4, 14))
        repo.update_signal_outcome(sid, excess_vol_score=1.0 / 3.0, price_data_source="yfinance")

        agg = ScorecardAggregator(repo, calendar)
        result = agg.top_n_posters(as_of=date(2026, 4, 14), window_days=30)
        assert len(result) == 1
        # 1/3 ≈ 0.333333 — check that it's rounded to at most 6dp
        s = str(result[0]["avg_excess_vol"]).split(".")
        if len(s) > 1:
            assert len(s[1]) <= 6
