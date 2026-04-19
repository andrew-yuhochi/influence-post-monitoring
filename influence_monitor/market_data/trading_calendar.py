# NYSE trading calendar via pandas_market_calendars — implemented in TASK-005.
#
# Purpose: Provides authoritative NYSE trading-day helpers and the collection-window
# resolver used by the ingestion pipeline, outcome engine, and vol-lookback computation.
# All date arithmetic goes through pandas_market_calendars — NO ad-hoc weekday math.
#
# Limitation (PoC): Early-close trading days (day after Thanksgiving, Christmas Eve,
# July 3rd when July 4th is a weekday) are treated as a standard 16:00 ET close.
# The small data error (~2h of missed after-hours activity on ~3 days/year) is
# acceptable for PoC. Document and address in MVP if the user finds it impactful.

from __future__ import annotations

import bisect
import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
_MARKET_CLOSE_HOUR = 16  # 16:00 ET standard close (early-close days treated the same at PoC)


class TradingCalendar:
    """NYSE trading calendar wrapper backed by pandas_market_calendars.

    All date-arithmetic methods use the authoritative NYSE schedule — no ad-hoc
    ``datetime.weekday()`` arithmetic anywhere in this class.

    PoC limitation — early-close days:
        The day after Thanksgiving, Christmas Eve (Dec 24), and July 3rd (when
        July 4th falls on a weekday) are NYSE early-close days with a 13:00 ET
        close.  This class returns 16:00 ET for those days as well, yielding a
        ~3-hour window undercount on roughly 3 days per year.  This is documented
        and accepted for PoC.  The ``collection_window`` method returns the 16:00
        timestamp in all cases; callers that need precise intraday windows should
        query ``pandas_market_calendars`` schedule data directly in MVP.
    """

    def __init__(self, start_year: int = 2020, end_year: int = 2030) -> None:
        self._calendar = mcal.get_calendar("NYSE")
        # Pre-fetch a wide schedule so individual queries are fast (no repeat IO).
        schedule = self._calendar.schedule(
            start_date=f"{start_year}-01-01",
            end_date=f"{end_year}-12-31",
        )
        # Sorted list of NYSE trading dates as plain Python date objects.
        self._trading_dates: list[date] = sorted(
            ts.date() for ts in schedule.index
        )
        # Set for O(1) membership testing.
        self._trading_date_set: set[date] = set(self._trading_dates)
        logger.debug(
            "TradingCalendar loaded %d NYSE trading days (%d–%d)",
            len(self._trading_dates),
            start_year,
            end_year,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_trading_day(self, d: date) -> bool:
        """Return True if *d* is an NYSE trading day (not weekend, not holiday)."""
        return d in self._trading_date_set

    def previous_trading_day(self, d: date) -> date:
        """Return the most recent NYSE trading day strictly before *d*.

        Works correctly whether *d* is a trading day or not (weekends, holidays).

        Raises:
            ValueError: If *d* is before or at the first trading day in the
                        pre-loaded range.
        """
        # bisect_left gives the insertion point for d; the element immediately
        # to the left is the last trading day <= d.
        idx = bisect.bisect_left(self._trading_dates, d)
        # If d is itself a trading day, idx points to d — we want idx-1 (before d).
        # If d is NOT a trading day, idx is the insertion point and idx-1 is the
        # last trading day before d.
        # Either way we subtract 1.
        if idx == 0:
            raise ValueError(f"No trading day before {d} in the pre-loaded range")
        # Edge: if the element at idx-1 equals d (d is in the list), we need
        # the element before it.
        prev_idx = idx - 1
        if self._trading_dates[prev_idx] == d:
            # d is in the list; the strictly-before result is at idx-2
            if prev_idx == 0:
                raise ValueError(f"No trading day before {d} in the pre-loaded range")
            prev_idx -= 1
        return self._trading_dates[prev_idx]

    def next_trading_day(self, d: date) -> date:
        """Return the next NYSE trading day strictly after *d*.

        Works correctly whether *d* is a trading day or not.

        Raises:
            ValueError: If *d* is at or after the last trading day in the
                        pre-loaded range.
        """
        # bisect_right gives the insertion point after any existing entry of d.
        idx = bisect.bisect_right(self._trading_dates, d)
        if idx >= len(self._trading_dates):
            raise ValueError(f"No trading day after {d} in the pre-loaded range")
        return self._trading_dates[idx]

    def trading_days_after(self, d: date, n: int) -> date:
        """Return the trading day that is *n* NYSE trading days after *d*.

        *d* itself is not counted — this returns the Nth trading day strictly
        after *d*.  Correctly skips weekends and all NYSE holidays (MLK Day,
        Good Friday, Thanksgiving, etc.).

        Args:
            d: Starting date (not counted).
            n: Number of trading days to advance; must be >= 1.

        Raises:
            ValueError: If *n* < 1 or the result falls beyond the pre-loaded range.
        """
        if n < 1:
            raise ValueError(f"n must be >= 1, got {n}")
        # Find the index of the first trading day after d and jump n steps.
        start_idx = bisect.bisect_right(self._trading_dates, d)
        target_idx = start_idx + n - 1
        if target_idx >= len(self._trading_dates):
            raise ValueError(
                f"trading_days_after({d}, {n}): result falls beyond the pre-loaded range"
            )
        return self._trading_dates[target_idx]

    def trading_days_between(self, start: date, end: date) -> list[date]:
        """Return all NYSE trading days in the closed interval [start, end].

        Includes both *start* and *end* if they are trading days.
        Returns an empty list if *start* > *end*.
        """
        if start > end:
            return []
        lo = bisect.bisect_left(self._trading_dates, start)
        hi = bisect.bisect_right(self._trading_dates, end)
        return self._trading_dates[lo:hi]

    def collection_window(self, send_time_et: datetime) -> tuple[datetime, datetime]:
        """Return the post-collection window as (previous_close_dt, send_time_et).

        The window captures all posts that appeared since the previous trading
        day's market close and up to the moment the morning alert is assembled.

        Args:
            send_time_et: The datetime at which the morning alert is being sent,
                          expressed in ET (America/New_York).  Must carry tzinfo.

        Returns:
            ``(prev_close_datetime, send_time_et)`` where *prev_close_datetime*
            is the previous NYSE trading day at 16:00 ET.

        PoC note: Early-close days (day after Thanksgiving, Christmas Eve) are
        treated as 16:00 ET, consistent with this class's documented limitation.

        Raises:
            ValueError: If *send_time_et* is timezone-naive.
        """
        if send_time_et.tzinfo is None:
            raise ValueError(
                "send_time_et must be timezone-aware (e.g. tzinfo=ET / America/New_York)"
            )

        send_date = send_time_et.date()
        prev_day = self.previous_trading_day(send_date)

        prev_close_dt = datetime(
            prev_day.year,
            prev_day.month,
            prev_day.day,
            _MARKET_CLOSE_HOUR,
            0,
            0,
            tzinfo=ET,
        )
        logger.debug(
            "collection_window: prev_close=%s send_time=%s",
            prev_close_dt.isoformat(),
            send_time_et.isoformat(),
        )
        return prev_close_dt, send_time_et
