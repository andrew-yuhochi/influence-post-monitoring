# Tests for TradingCalendar — TASK-005.
#
# Covers: weekend crossing, US market holidays (Good Friday, Memorial Day,
# Thanksgiving, Christmas), trading_days_after, collection_window, and the
# day-after-Thanksgiving early-close documentation note.

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from influence_monitor.market_data.trading_calendar import TradingCalendar, ET

# Shared calendar instance (one load covers all tests)
@pytest.fixture(scope="module")
def cal() -> TradingCalendar:
    return TradingCalendar(start_year=2020, end_year=2028)


# ---------------------------------------------------------------------------
# is_trading_day
# ---------------------------------------------------------------------------

class TestIsTradingDay:
    def test_regular_weekday(self, cal: TradingCalendar) -> None:
        # Monday 2026-04-13 is a regular trading day
        assert cal.is_trading_day(date(2026, 4, 13)) is True

    def test_saturday(self, cal: TradingCalendar) -> None:
        assert cal.is_trading_day(date(2026, 4, 18)) is False  # Saturday 2026-04-18

    def test_sunday(self, cal: TradingCalendar) -> None:
        assert cal.is_trading_day(date(2026, 4, 19)) is False

    def test_good_friday_2026(self, cal: TradingCalendar) -> None:
        # Good Friday 2026-04-03 — NYSE is closed
        assert cal.is_trading_day(date(2026, 4, 3)) is False

    def test_independence_day_2026(self, cal: TradingCalendar) -> None:
        # July 4, 2026 is a Saturday; NYSE observes it on Friday July 3
        # The actual July 4 is a Saturday so already off; let's test the observed holiday
        # July 3, 2026 (Friday) is the NYSE-observed holiday
        assert cal.is_trading_day(date(2026, 7, 3)) is False

    def test_independence_day_weekday_2023(self, cal: TradingCalendar) -> None:
        # July 4, 2023 is a Tuesday — NYSE directly closed
        assert cal.is_trading_day(date(2023, 7, 4)) is False

    def test_memorial_day_2026(self, cal: TradingCalendar) -> None:
        # Memorial Day 2026 is May 25 (last Monday of May)
        assert cal.is_trading_day(date(2026, 5, 25)) is False

    def test_thanksgiving_2026(self, cal: TradingCalendar) -> None:
        # Thanksgiving 2026 is Nov 26 (4th Thursday)
        assert cal.is_trading_day(date(2026, 11, 26)) is False

    def test_christmas_2026(self, cal: TradingCalendar) -> None:
        # Christmas 2026 is a Friday — NYSE closed
        assert cal.is_trading_day(date(2026, 12, 25)) is False


# ---------------------------------------------------------------------------
# previous_trading_day
# ---------------------------------------------------------------------------

class TestPreviousTradingDay:
    def test_weekend_crossing_with_holiday(self, cal: TradingCalendar) -> None:
        # Monday 2026-04-06 — previous trading day should cross the Good Friday holiday
        # Good Friday 2026 is April 3 (Friday). So previous trading day from Monday
        # April 6 should be Thursday April 2 (skipping Good Friday and the weekend).
        result = cal.previous_trading_day(date(2026, 4, 6))
        assert result == date(2026, 4, 2), f"Expected 2026-04-02, got {result}"

    def test_regular_monday(self, cal: TradingCalendar) -> None:
        # For a regular Monday, prev trading day is Friday
        result = cal.previous_trading_day(date(2026, 4, 27))
        assert result == date(2026, 4, 24)  # Friday

    def test_regular_wednesday(self, cal: TradingCalendar) -> None:
        result = cal.previous_trading_day(date(2026, 4, 22))
        assert result == date(2026, 4, 21)  # Tuesday

    def test_from_non_trading_day(self, cal: TradingCalendar) -> None:
        # Saturday 2026-04-25 — prev trading day is Friday 2026-04-24
        result = cal.previous_trading_day(date(2026, 4, 25))
        assert result == date(2026, 4, 24)

    def test_post_memorial_day(self, cal: TradingCalendar) -> None:
        # Tuesday after Memorial Day 2026 (May 26) — prev should be Fri May 22
        result = cal.previous_trading_day(date(2026, 5, 26))
        assert result == date(2026, 5, 22)


# ---------------------------------------------------------------------------
# next_trading_day
# ---------------------------------------------------------------------------

class TestNextTradingDay:
    def test_regular_friday(self, cal: TradingCalendar) -> None:
        result = cal.next_trading_day(date(2026, 4, 24))
        assert result == date(2026, 4, 27)  # Monday (skips weekend)

    def test_from_saturday(self, cal: TradingCalendar) -> None:
        result = cal.next_trading_day(date(2026, 4, 25))
        assert result == date(2026, 4, 27)

    def test_thursday_before_good_friday(self, cal: TradingCalendar) -> None:
        # April 1 (Wed) -> April 2 (Thu open; Good Friday 2026 is April 3)
        result = cal.next_trading_day(date(2026, 4, 1))
        assert result == date(2026, 4, 2)

    def test_day_before_good_friday_skips_to_monday(self, cal: TradingCalendar) -> None:
        # Good Friday 2026 = April 3 (Fri, NYSE closed).
        # next_trading_day(April 2 Thu) must skip Fri holiday AND weekend → Monday April 6.
        result = cal.next_trading_day(date(2026, 4, 2))
        assert result == date(2026, 4, 6)


# ---------------------------------------------------------------------------
# trading_days_after
# ---------------------------------------------------------------------------

class TestTradingDaysAfter:
    def test_five_days_from_new_year_2026(self, cal: TradingCalendar) -> None:
        """
        trading_days_after(2026-01-01, 5) must skip:
          - Jan 1 itself (New Year's Day, NYSE closed)
          - Jan 2 (Friday) → trading day 1
          - Jan 5 (Mon)    → trading day 2
          - Jan 6 (Tue)    → trading day 3
          - Jan 7 (Wed)    → trading day 4
          - Jan 8 (Thu)    → trading day 5 — but Jan 20 is MLK Day
        Actually MLK Day 2026 is Jan 19. Jan 1 is New Year (closed).
        Starting from Jan 1 (non-trading):
          next_trading_day(Jan 1) = Jan 2 (trading day 1)
          Jan 5 = trading day 2
          Jan 6 = trading day 3
          Jan 7 = trading day 4
          Jan 8 = trading day 5
        """
        result = cal.trading_days_after(date(2026, 1, 1), 5)
        assert result == date(2026, 1, 8)

    def test_five_days_skipping_mlk_day(self, cal: TradingCalendar) -> None:
        """
        MLK Day 2026 is Jan 19. Starting from Jan 15 (Thursday):
          Jan 16 (Fri)  → +1
          Jan 20 (Tue)  → +2  (Jan 19 Mon = MLK Day, skip; Jan 18 Sat, skip)
          Jan 21 (Wed)  → +3
          Jan 22 (Thu)  → +4
          Jan 23 (Fri)  → +5
        """
        result = cal.trading_days_after(date(2026, 1, 15), 5)
        assert result == date(2026, 1, 23)

    def test_raises_on_zero(self, cal: TradingCalendar) -> None:
        with pytest.raises(ValueError):
            cal.trading_days_after(date(2026, 4, 21), 0)

    def test_single_day(self, cal: TradingCalendar) -> None:
        result = cal.trading_days_after(date(2026, 4, 21), 1)
        assert result == date(2026, 4, 22)


# ---------------------------------------------------------------------------
# trading_days_between
# ---------------------------------------------------------------------------

class TestTradingDaysBetween:
    def test_week_with_holiday(self, cal: TradingCalendar) -> None:
        # Full Mon-Fri week April 13-17 2026 (no holiday — all 5 days trade)
        days = cal.trading_days_between(date(2026, 4, 13), date(2026, 4, 17))
        assert days == [
            date(2026, 4, 13),
            date(2026, 4, 14),
            date(2026, 4, 15),
            date(2026, 4, 16),
            date(2026, 4, 17),
        ]

    def test_empty_when_no_trading_days(self, cal: TradingCalendar) -> None:
        # Good Friday + weekend span: April 3-5 2026 (Fri holiday + Sat + Sun)
        days = cal.trading_days_between(date(2026, 4, 3), date(2026, 4, 5))
        assert days == []

    def test_start_after_end_returns_empty(self, cal: TradingCalendar) -> None:
        days = cal.trading_days_between(date(2026, 4, 21), date(2026, 4, 20))
        assert days == []

    def test_single_trading_day(self, cal: TradingCalendar) -> None:
        days = cal.trading_days_between(date(2026, 4, 21), date(2026, 4, 21))
        assert days == [date(2026, 4, 21)]


# ---------------------------------------------------------------------------
# collection_window
# ---------------------------------------------------------------------------

class TestCollectionWindow:
    def test_prev_close_is_correct_et(self, cal: TradingCalendar) -> None:
        """
        collection_window(2026-04-06 09:00 ET) should return prev_close = 2026-04-02 16:00 ET.
        Good Friday 2026 is April 3 (closed); April 4-5 is weekend.
        April 6 is Monday — previous trading day = Thursday April 2.
        """
        send_time = datetime(2026, 4, 6, 9, 0, 0, tzinfo=ET)
        prev_close, returned_send = cal.collection_window(send_time)

        assert returned_send == send_time
        assert prev_close.year == 2026
        assert prev_close.month == 4
        assert prev_close.day == 2
        assert prev_close.hour == 16
        assert prev_close.minute == 0
        assert prev_close.tzinfo == ET

    def test_monday_regular_week_prev_close_is_friday(self, cal: TradingCalendar) -> None:
        send_time = datetime(2026, 4, 27, 9, 0, 0, tzinfo=ET)
        prev_close, _ = cal.collection_window(send_time)
        assert prev_close.date() == date(2026, 4, 24)  # Friday
        assert prev_close.hour == 16

    def test_raises_without_timezone(self, cal: TradingCalendar) -> None:
        naive_dt = datetime(2026, 4, 21, 9, 0, 0)
        with pytest.raises(ValueError, match="timezone-aware"):
            cal.collection_window(naive_dt)

    def test_post_thanksgiving_monday(self, cal: TradingCalendar) -> None:
        """Monday after Thanksgiving 2026 (Nov 30) — prev close should be Fri Nov 27.
        Thanksgiving is Nov 26. Nov 27 (Fri, day-after-Thanksgiving) IS a trading day
        at PoC (early close only, not closed — treated as 16:00 ET).
        """
        send_time = datetime(2026, 11, 30, 9, 0, 0, tzinfo=ET)
        prev_close, _ = cal.collection_window(send_time)
        assert prev_close.date() == date(2026, 11, 27)
        # PoC: early close is treated as 16:00 ET, same as full trading day
        assert prev_close.hour == 16  # NOTE: day-after-Thanksgiving is early-close (13:00 actual) but PoC returns 16:00

    def test_christmas_week(self, cal: TradingCalendar) -> None:
        """Dec 28 (Mon) 2026 — prev close should be Dec 24 (Thu) since Dec 25 (Fri) is Christmas."""
        # Christmas 2026 is Dec 25 (Fri) — NYSE closed.
        # Dec 28 (Mon) prev trading day = Dec 24 (Thu).
        # Note: Christmas Eve Dec 24 is an early-close day but IS a trading day at PoC.
        send_time = datetime(2026, 12, 28, 9, 0, 0, tzinfo=ET)
        prev_close, _ = cal.collection_window(send_time)
        assert prev_close.date() == date(2026, 12, 24)
        assert prev_close.hour == 16  # NOTE: Dec 24 is early-close (13:00 actual) but PoC returns 16:00


# ---------------------------------------------------------------------------
# Standalone tests (use module-level cal instance)
# ---------------------------------------------------------------------------

def test_previous_trading_day_after_weekend(cal: TradingCalendar):
    """previous_trading_day(Mon Apr 20) = Fri Apr 17 (normal Friday, no holiday)"""
    assert cal.previous_trading_day(date(2026, 4, 20)) == date(2026, 4, 17)


def test_collection_window_tuesday(cal: TradingCalendar):
    """collection_window for Tue Apr 21 09:00 ET → prev_close = Mon Apr 20 16:00 ET"""
    send_time = datetime(2026, 4, 21, 9, 0, tzinfo=ZoneInfo("America/New_York"))
    prev_close, send_dt = cal.collection_window(send_time)
    assert prev_close == datetime(2026, 4, 20, 16, 0, tzinfo=ZoneInfo("America/New_York"))
    assert send_dt == send_time
