"""NYSE market holiday calendar.

Provides :class:`HolidayCalendar` for trading-day detection and counting.
All holiday rules are computed programmatically — no external dependency.
"""

from __future__ import annotations

from datetime import date, timedelta


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Return the Nth occurrence of *weekday* (0=Mon) in *year*/*month*."""
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return first + timedelta(days=delta + (n - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """Return the last occurrence of *weekday* in *year*/*month*."""
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    delta = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=delta)


def _easter(year: int) -> date:
    """Compute Easter Sunday using the Anonymous Gregorian algorithm."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _observe(d: date) -> date:
    """Apply NYSE observation rule: Sun → Mon, Sat → Fri."""
    if d.weekday() == 6:  # Sunday
        return d + timedelta(days=1)
    if d.weekday() == 5:  # Saturday
        return d - timedelta(days=1)
    return d


def _nyse_holidays(year: int) -> frozenset[date]:
    """Return the set of NYSE market holidays for *year*."""
    holidays = {
        _observe(date(year, 1, 1)),                       # New Year's Day
        _nth_weekday(year, 1, 0, 3),                      # MLK Day (3rd Mon Jan)
        _nth_weekday(year, 2, 0, 3),                      # Presidents' Day (3rd Mon Feb)
        _easter(year) - timedelta(days=2),                 # Good Friday
        _last_weekday(year, 5, 0),                        # Memorial Day (last Mon May)
        _observe(date(year, 6, 19)),                      # Juneteenth
        _observe(date(year, 7, 4)),                       # Independence Day
        _nth_weekday(year, 9, 0, 1),                      # Labor Day (1st Mon Sep)
        _nth_weekday(year, 11, 3, 4),                     # Thanksgiving (4th Thu Nov)
        _observe(date(year, 12, 25)),                     # Christmas
    }
    return frozenset(holidays)


class HolidayCalendar:
    """NYSE market holiday and trading-day calendar.

    Pre-computes holidays for a rolling window of years so that
    ``is_trading_day`` is a pure in-memory lookup with no external calls.
    """

    def __init__(self, years: range | None = None) -> None:
        if years is None:
            current_year = date.today().year
            years = range(current_year - 1, current_year + 3)
        self._holidays: frozenset[date] = frozenset(
            h for y in years for h in _nyse_holidays(y)
        )

    def is_trading_day(self, d: date) -> bool:
        """Return True for weekdays that are not NYSE holidays."""
        return d.weekday() < 5 and d not in self._holidays

    def trading_days_after(self, d: date, n: int) -> date:
        """Return the date that is *n* trading days after *d*."""
        current = d
        count = 0
        while count < n:
            current += timedelta(days=1)
            if self.is_trading_day(current):
                count += 1
        return current

    def trading_days_before(self, d: date, n: int) -> date:
        """Return the date that is *n* trading days before *d*."""
        current = d
        count = 0
        while count < n:
            current -= timedelta(days=1)
            if self.is_trading_day(current):
                count += 1
        return current
