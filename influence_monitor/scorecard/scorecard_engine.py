"""Scorecard engine — evening return computation and accuracy tracking.

Runs after market close to:
1. Fetch closing prices for each ticker in the morning watchlist.
2. Compute ``return_pct`` and ``is_hit`` (SHORT-aware).
3. Populate market context (OHLCV, volume ratio, prev close).
4. Populate regime context (SP500, VIX, sector ETF returns).
5. Populate stock context (market cap, sector, industry).
6. Update per-investor rolling accuracy and lifetime call counts.
7. Write a ``daily_summaries`` row (idempotent — safe to re-run).

Idempotency guarantee: signals where ``close_price IS NOT NULL`` are
skipped, so re-running the engine for the same date is safe.

Entry point::

    engine = ScorecardEngine(market_client, repo, settings)
    await engine.run_evening(signal_date)
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import yfinance as yf

from influence_monitor.config import Settings
from influence_monitor.db.repository import DatabaseRepository
from influence_monitor.market_data.base import (
    DataFreshnessError,
    DataUnavailableError,
    MarketDataClient,
)

logger = logging.getLogger(__name__)

# Sector name → sector ETF ticker (GICS-aligned)
_SECTOR_ETF: dict[str, str] = {
    "Technology": "XLK",
    "Information Technology": "XLK",
    "Health Care": "XLV",
    "Healthcare": "XLV",
    "Financials": "XLF",
    "Financial Services": "XLF",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Energy": "XLE",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Materials": "XLB",
    "Industrials": "XLI",
    "Communication Services": "XLC",
    "Telecommunication Services": "XLC",
}

_AVG_VOLUME_LOOKBACK = "35d"  # gives ~30 trading days


class ScorecardEngine:
    """Compute returns, HIT/MISS, and update accuracy for a trading day.

    Usage::

        engine = ScorecardEngine(market_client, repo, settings)
        summary = await engine.run_evening(date(2026, 4, 15))
    """

    def __init__(
        self,
        market_client: MarketDataClient,
        repo: DatabaseRepository,
        settings: Settings,
    ) -> None:
        self._client = market_client
        self._repo = repo
        self._settings = settings

    async def run_evening(self, signal_date: date) -> dict[str, Any]:
        """Score the morning watchlist for *signal_date*.

        Fetches close prices, computes returns and HIT/MISS, updates
        market / regime / stock context fields, refreshes investor
        accuracy stats, and writes a ``daily_summaries`` row.

        Returns a summary dict with ``signals_scored``, ``hits``,
        ``misses``, ``skipped`` (already scored), ``errors``.

        Idempotent: skips signals where ``close_price IS NOT NULL``.
        """
        signals = await self._repo.get_signals_for_scoring(signal_date)

        if not signals:
            logger.info(
                "run_evening(%s): no unscored signals found (already done or quiet night)",
                signal_date,
            )
            await self._write_daily_summary(signal_date, signals_scored=0, hits=0, misses=0, errors=0)
            return {"signals_scored": 0, "hits": 0, "misses": 0, "skipped": 0, "errors": 0}

        logger.info("run_evening(%s): scoring %d signals", signal_date, len(signals))

        # Fetch regime context once for the whole day (SP500, VIX)
        regime = await self._fetch_regime_context(signal_date)

        # Score each signal; track per-investor IDs for accuracy update
        hits = misses = errors = 0
        touched_investor_ids: set[int] = set()

        for sig in signals:
            signal_id = sig["id"]
            ticker = sig["ticker"]
            direction = sig["direction"]
            sector = sig.get("sector")  # may already be populated

            # Fetch OHLCV first — provides open, high, low, close, volume in one call.
            # Both open and close prices are available from historical data after market close,
            # so no separate 9:31 AM fetch step is required.
            ohlcv = await self._fetch_ohlcv_safe(ticker, signal_date)
            if ohlcv is None:
                await self._repo.update_signal_prices(signal_id)  # keep NULLs
                errors += 1
                continue

            # Use stored open_price if present (e.g. captured intraday); fall back to OHLCV open.
            open_price = sig.get("open_price") or ohlcv.get("open")
            if open_price is None:
                logger.warning(
                    "Signal %d (%s) has no open_price in DB or OHLCV — skipping",
                    signal_id, ticker,
                )
                errors += 1
                continue

            close_price = ohlcv["close"]
            return_pct = round(
                (close_price - open_price) / open_price * 100, 4
            )
            is_hit = _compute_is_hit(direction, return_pct)

            await self._repo.update_signal_prices(
                signal_id,
                close_price=close_price,
                high_price=ohlcv.get("high"),
                low_price=ohlcv.get("low"),
                return_pct=return_pct,
                is_hit=is_hit,
            )

            if is_hit:
                hits += 1
            else:
                misses += 1

            touched_investor_ids.add(sig["investor_id"])

            # --- volume context ---
            avg_volume_30d = await self._fetch_avg_volume(ticker)
            volume = ohlcv.get("volume")
            volume_ratio = (
                round(volume / avg_volume_30d, 4)
                if volume and avg_volume_30d
                else None
            )

            # --- stock context (sector, industry, market cap) ---
            stock_info = await self._fetch_stock_info_safe(ticker)
            sector = stock_info.get("sector") if stock_info else sector
            sector_etf = _SECTOR_ETF.get(sector or "", "") if sector else None
            sector_return = (
                await self._fetch_sector_return(sector_etf, signal_date)
                if sector_etf
                else None
            )

            await self._repo.update_signal_market_context(
                signal_id,
                volume=volume,
                avg_volume_30d=int(avg_volume_30d) if avg_volume_30d else None,
                volume_ratio=volume_ratio,
                market_cap_at_signal=stock_info.get("market_cap") if stock_info else None,
                sector=sector,
                industry=stock_info.get("industry") if stock_info else None,
                sp500_return_pct=regime.get("sp500_return_pct"),
                vix_at_signal=regime.get("vix_at_signal"),
                sector_return_pct=sector_return,
            )

        # --- update per-investor accuracy ---
        for investor_id in touched_investor_ids:
            await self._update_investor_accuracy(investor_id)

        signals_scored = hits + misses
        await self._write_daily_summary(signal_date, signals_scored, hits, misses, errors)

        logger.info(
            "run_evening(%s): scored=%d hits=%d misses=%d errors=%d",
            signal_date, signals_scored, hits, misses, errors,
        )
        return {
            "signals_scored": signals_scored,
            "hits": hits,
            "misses": misses,
            "skipped": 0,
            "errors": errors,
        }

    # ------------------------------------------------------------------
    # Price and market data helpers
    # ------------------------------------------------------------------

    async def _fetch_ohlcv_safe(
        self, ticker: str, target_date: date
    ) -> dict[str, float | int | None] | None:
        """Fetch OHLCV, returning None on any failure (logs WARNING)."""
        try:
            return self._client.fetch_ohlcv(ticker, target_date)
        except (DataFreshnessError, DataUnavailableError, Exception) as exc:
            logger.warning(
                "Price fetch failed for %s on %s: %s", ticker, target_date, exc
            )
            return None

    async def _fetch_avg_volume(self, ticker: str) -> float | None:
        """Compute 30-day average volume from yfinance history."""
        try:
            hist = yf.Ticker(ticker).history(period=_AVG_VOLUME_LOOKBACK)
            if hist is None or hist.empty:
                return None
            vol_col = hist.get("Volume") or hist.get("volume")
            if vol_col is None or vol_col.empty:
                return None
            return float(vol_col.mean())
        except Exception as exc:
            logger.warning("avg_volume fetch failed for %s: %s", ticker, exc)
            return None

    async def _fetch_stock_info_safe(
        self, ticker: str
    ) -> dict[str, Any] | None:
        """Fetch sector, industry, and market cap from yfinance ticker info."""
        try:
            info = yf.Ticker(ticker).info
            if not info:
                return None
            return {
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "market_cap": info.get("marketCap"),
            }
        except Exception as exc:
            logger.warning("stock info fetch failed for %s: %s", ticker, exc)
            return None

    async def _fetch_regime_context(
        self, target_date: date
    ) -> dict[str, float | None]:
        """Fetch SP500 return and VIX close for the trading day."""
        sp500_ret = await self._fetch_index_return("^GSPC", target_date)
        vix_close = await self._fetch_index_close("^VIX", target_date)
        return {"sp500_return_pct": sp500_ret, "vix_at_signal": vix_close}

    async def _fetch_sector_return(
        self, etf_ticker: str, target_date: date
    ) -> float | None:
        """Fetch intraday return for a sector ETF."""
        return await self._fetch_index_return(etf_ticker, target_date)

    async def _fetch_index_return(
        self, ticker: str, target_date: date
    ) -> float | None:
        """Compute open-to-close return for an index/ETF."""
        try:
            ohlcv = self._client.fetch_ohlcv(ticker, target_date)
            open_px = ohlcv.get("open")
            close_px = ohlcv.get("close")
            if open_px and close_px and open_px != 0:
                return round((close_px - open_px) / open_px * 100, 4)
        except Exception as exc:
            logger.warning("index return fetch failed for %s: %s", ticker, exc)
        return None

    async def _fetch_index_close(
        self, ticker: str, target_date: date
    ) -> float | None:
        """Fetch closing price for an index (e.g., ^VIX)."""
        try:
            ohlcv = self._client.fetch_ohlcv(ticker, target_date)
            return ohlcv.get("close")
        except Exception as exc:
            logger.warning("index close fetch failed for %s: %s", ticker, exc)
        return None

    # ------------------------------------------------------------------
    # Investor accuracy update
    # ------------------------------------------------------------------

    async def _update_investor_accuracy(self, investor_id: int) -> None:
        """Recompute and store rolling and lifetime accuracy for an investor."""
        rolling_calls, rolling_hits = await self._repo.compute_investor_rolling_accuracy(
            investor_id, days=30
        )
        total_calls, total_hits = await self._repo.get_investor_lifetime_stats(investor_id)
        rolling_accuracy = (
            round(rolling_hits / rolling_calls, 4) if rolling_calls else None
        )
        await self._repo.update_investor_accuracy(
            investor_id,
            rolling_accuracy_30d=rolling_accuracy,
            total_calls=total_calls,
            total_hits=total_hits,
        )

    # ------------------------------------------------------------------
    # Daily summary
    # ------------------------------------------------------------------

    async def _write_daily_summary(
        self,
        signal_date: date,
        signals_scored: int,
        hits: int,
        misses: int,
        errors: int,
    ) -> None:
        daily_hit_rate = (
            round(hits / (hits + misses), 4) if (hits + misses) > 0 else None
        )
        await self._repo.upsert_daily_summary(
            tenant_id=1,
            summary_date=signal_date.isoformat(),
            run_type="evening",
            signals_surfaced=signals_scored,
            daily_hit_rate=daily_hit_rate,
            pipeline_status="ok" if errors == 0 else "partial",
            error_message=f"{errors} price fetch error(s)" if errors else None,
        )


# ----------------------------------------------------------------------
# Pure helpers (module-level for testability)
# ----------------------------------------------------------------------


def _compute_is_hit(direction: str, return_pct: float) -> bool:
    """Return True when the signal's directional call was correct.

    LONG is a HIT when return_pct > 0 (price went up).
    SHORT is a HIT when return_pct < 0 (price went down).
    """
    direction = direction.upper()
    if direction == "LONG":
        return return_pct > 0
    if direction == "SHORT":
        return return_pct < 0
    return False
