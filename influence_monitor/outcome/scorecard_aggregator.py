"""30-day per-poster average excess/vol scorecard aggregator — implemented in TASK-012.

Groups scored signals by poster handle over a rolling window and ranks them by
average excess_vol_score. Also exposes trading_days_with_signals() for the
'Sample still building' warning (emitted by the renderer when < 20 days).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from influence_monitor.db.repository import SignalRepository
from influence_monitor.market_data.trading_calendar import TradingCalendar

logger = logging.getLogger(__name__)


class ScorecardAggregator:
    """Aggregates per-poster excess/vol scores over a sliding window.

    Args:
        repo:             SignalRepository for reading scored signals.
        trading_calendar: TradingCalendar for trading-day counting.
    """

    def __init__(
        self,
        repo: SignalRepository,
        trading_calendar: TradingCalendar,
    ) -> None:
        self._repo = repo
        self._calendar = trading_calendar

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def top_n_posters(
        self,
        as_of: date,
        window_days: int = 30,
        n: int = 5,
        tenant_id: int = 1,
    ) -> list[dict[str, Any]]:
        """Return the top *n* posters ranked by avg excess_vol_score.

        Only signals with a non-null excess_vol_score are included.

        Args:
            as_of:       Last date of the window (inclusive).
            window_days: Number of calendar days to look back (default 30).
            n:           Number of top posters to return (default 5).
            tenant_id:   Tenant scope.

        Returns:
            List of dicts with keys ``handle``, ``avg_excess_vol``, ``n_signals``,
            sorted descending by avg_excess_vol.  Empty list when no scored signals
            exist in the window.
        """
        start_date = as_of - timedelta(days=window_days - 1)
        signals = self._repo.get_signals_for_date_range(
            start_date, as_of, tenant_id=tenant_id
        )

        # Filter to scored signals only
        scored = [s for s in signals if s.get("excess_vol_score") is not None]
        if not scored:
            logger.info(
                "top_n_posters(as_of=%s, window=%d): no scored signals",
                as_of,
                window_days,
            )
            return []

        # Group by handle
        by_handle: dict[str, list[float]] = {}
        for sig in scored:
            handle = sig.get("account_handle") or sig.get("handle") or str(sig.get("account_id"))
            by_handle.setdefault(handle, []).append(float(sig["excess_vol_score"]))

        rows = []
        for handle, scores in by_handle.items():
            avg = sum(scores) / len(scores)
            rows.append(
                {
                    "handle": handle,
                    "avg_excess_vol": round(avg, 6),
                    "n_signals": len(scores),
                }
            )

        rows.sort(key=lambda r: r["avg_excess_vol"], reverse=True)
        result = rows[:n]

        logger.info(
            "top_n_posters(as_of=%s, window=%d): returning %d of %d posters",
            as_of,
            window_days,
            len(result),
            len(rows),
        )
        return result

    def trading_days_with_signals(
        self,
        as_of: date,
        window_days: int = 30,
        tenant_id: int = 1,
    ) -> int:
        """Count distinct trading days that have at least one scored signal.

        Used by the renderer to emit the '⚠️ Sample still building' warning
        when the result is < 20.

        Args:
            as_of:       Last date of the window (inclusive).
            window_days: Number of calendar days to look back (default 30).
            tenant_id:   Tenant scope.

        Returns:
            Integer count of distinct trading days with >= 1 scored signal.
        """
        start_date = as_of - timedelta(days=window_days - 1)
        signals = self._repo.get_signals_for_date_range(
            start_date, as_of, tenant_id=tenant_id
        )

        scored_dates = {
            s["signal_date"]
            for s in signals
            if s.get("excess_vol_score") is not None
        }

        # Intersect with actual trading days to exclude any non-trading-day dates
        trading_days = set(
            d.isoformat()
            for d in self._calendar.trading_days_between(start_date, as_of)
        )
        count = len(scored_dates & trading_days)

        logger.debug(
            "trading_days_with_signals(as_of=%s, window=%d): %d day(s)",
            as_of,
            window_days,
            count,
        )
        return count
