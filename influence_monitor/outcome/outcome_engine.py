"""Outcome engine: overnight/tradeable/excess-vol computation — implemented in TASK-012.

For each signal where excess_vol_score IS NULL, fetches market prices and computes:
  - overnight_return  = (today_open  - prev_close) / prev_close
  - tradeable_return  = (today_close - today_open)  / today_open
  - spy_return        = SPY day-over-day return
  - stock_20d_vol     = daily-return stdev over vol_lookback_days trading days
  - excess_vol_score  = (stock_return - spy_return) / stock_20d_vol
    where stock_return = (today_close - prev_close) / prev_close

Idempotent: signals with a non-null excess_vol_score are skipped on re-run.
On any price fetch failure: price_data_source='unavailable', outcome columns NULL.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from typing import Any

from influence_monitor.db.repository import SignalRepository
from influence_monitor.market_data.base import MarketDataClient
from influence_monitor.market_data.trading_calendar import TradingCalendar

logger = logging.getLogger(__name__)


class OutcomeEngine:
    """Computes and persists outcome metrics for signals on a given date.

    Args:
        market_client: Concrete MarketDataClient (YFinanceClient or chain).
        repo:           SignalRepository for reading signals and writing outcomes.
        trading_calendar: TradingCalendar for prev_close date resolution.
    """

    def __init__(
        self,
        market_client: MarketDataClient,
        repo: SignalRepository,
        trading_calendar: TradingCalendar,
    ) -> None:
        self._market = market_client
        self._repo = repo
        self._calendar = trading_calendar

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute_and_store(
        self,
        target_date: date,
        tenant_id: int = 1,
    ) -> int:
        """Compute and persist outcome metrics for all unscored signals on *target_date*.

        Skips any signal whose excess_vol_score is already non-null (idempotent).

        Args:
            target_date: The trading day the signals were generated for.
            tenant_id:   Tenant scope.

        Returns:
            Number of signals actually processed (i.e. scored, not skipped).
        """
        config = self._repo.get_scoring_config(tenant_id=tenant_id)
        vol_lookback_days = int(config.get("vol_lookback_days", 20))

        signals = self._repo.get_signals_for_date(target_date, tenant_id=tenant_id)
        if not signals:
            logger.info("compute_and_store(%s): no signals found", target_date)
            return 0

        prev_trading_day = self._calendar.previous_trading_day(target_date)
        logger.info(
            "compute_and_store(%s): prev_trading_day=%s, %d signal(s) total",
            target_date,
            prev_trading_day,
            len(signals),
        )

        processed = 0
        for sig in signals:
            if sig.get("excess_vol_score") is not None:
                logger.debug(
                    "Signal id=%s already scored — skipping (idempotent)",
                    sig["id"],
                )
                continue
            if processed > 0:
                time.sleep(1)
            self._process_signal(sig, target_date, prev_trading_day, vol_lookback_days)
            processed += 1

        logger.info(
            "compute_and_store(%s): processed %d signal(s), skipped %d",
            target_date,
            processed,
            len(signals) - processed,
        )
        return processed

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _process_signal(
        self,
        sig: dict[str, Any],
        target_date: date,
        prev_trading_day: date,
        vol_lookback_days: int,
    ) -> None:
        """Fetch market data and write outcome columns for a single signal."""
        signal_id: int = sig["id"]
        ticker: str = sig["ticker"]
        direction: str = (sig.get("direction") or "LONG").upper()

        # One attempt; retry once (30s backoff) only for transient non-rate-limit errors.
        # Yahoo Finance rate limits are typically 15-minute windows — retrying after
        # 30s or 90s rarely helps and only stalls the evening pipeline.  Mark as
        # unavailable immediately on rate-limit errors; retry once for transient failures.
        _fetch_ok = False
        today_open: float = 0.0
        today_close: float = 0.0
        prev_close: float = 0.0
        spy_return: float | None = None
        stock_20d_vol: float | None = None
        last_exc: Exception | None = None

        for attempt in range(2):  # attempt 0 = first try, attempt 1 = single retry
            if attempt > 0:
                time.sleep(30)
            try:
                ohlcv = self._market.fetch_ohlcv(ticker, target_date)
                today_open = ohlcv["open"]   # type: ignore[assignment]
                today_close = ohlcv["close"]  # type: ignore[assignment]

                prev_close = self._market.fetch_close(ticker, prev_trading_day)

                spy_return = self._market.fetch_spy_return(target_date)
                if spy_return is None:
                    raise ValueError(f"fetch_spy_return returned None for {target_date}")

                stock_20d_vol = self._market.fetch_stock_vol(
                    ticker, target_date, vol_lookback_days
                )
                if stock_20d_vol is None or stock_20d_vol == 0.0:
                    raise ValueError(
                        f"fetch_stock_vol returned unusable value={stock_20d_vol} "
                        f"for {ticker} on {target_date}"
                    )

                _fetch_ok = True
                break
            except Exception as exc:
                last_exc = exc
                exc_str = str(exc)
                is_rate_limit = (
                    "Too Many Requests" in exc_str
                    or "Rate limit" in exc_str
                    or "rate limit" in exc_str.lower()
                )
                if is_rate_limit:
                    # Yahoo rate-limit window is 15+ min — don't retry, mark unavailable.
                    logger.warning(
                        "Yahoo rate-limited for %s — marking unavailable immediately",
                        ticker,
                    )
                    break
                logger.warning(
                    "Price fetch transient error for %s (attempt %d/2): %s",
                    ticker, attempt + 1, exc,
                )

        if not _fetch_ok:
            exc = last_exc
            logger.warning(
                "Price fetch failed for signal id=%s ticker=%s: %s — marking unavailable",
                signal_id,
                ticker,
                exc,
            )
            self._repo.update_signal_outcome(
                signal_id,
                price_data_source="unavailable",
                outcome_fetched_at=datetime.now(tz=timezone.utc),
            )
            return

        # Compute returns
        overnight_return = (today_open - prev_close) / prev_close
        tradeable_return = (today_close - today_open) / today_open
        stock_return = (today_close - prev_close) / prev_close

        # Sign flip for SHORT signals: a stock rising is bad for a short thesis
        if direction == "SHORT":
            excess = ((-stock_return) - spy_return) / stock_20d_vol
        else:
            excess = (stock_return - spy_return) / stock_20d_vol

        self._repo.update_signal_outcome(
            signal_id,
            prev_close=round(prev_close, 6),
            today_open=round(today_open, 6),
            today_close=round(today_close, 6),
            overnight_return=round(overnight_return, 6),
            tradeable_return=round(tradeable_return, 6),
            spy_return=round(spy_return, 6),
            stock_20d_vol=round(stock_20d_vol, 6),
            excess_vol_score=round(excess, 6),
            price_data_source="yfinance",
            outcome_fetched_at=datetime.now(tz=timezone.utc),
        )
        logger.info(
            "Signal id=%s %s %s: overnight=%.4f tradeable=%.4f "
            "spy=%.4f vol=%.4f excess_vol=%.4f",
            signal_id,
            direction,
            ticker,
            overnight_return,
            tradeable_return,
            spy_return,
            stock_20d_vol,
            excess,
        )
