#!/usr/bin/env python3
"""Demo script for TASK-005: TradingCalendar — 10 representative date queries.

Outputs results to stdout (captured to docs/.../TASK-005-calendar.txt).
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# Ensure the project root is on the path when run as a script
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from influence_monitor.market_data.trading_calendar import TradingCalendar, ET


def separator(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def main() -> None:
    print("TradingCalendar Demo — TASK-005")
    print("NYSE calendar backed by pandas_market_calendars")
    print(f"Date: 2026-04-19 (run date)")

    cal = TradingCalendar(start_year=2024, end_year=2027)

    # ------------------------------------------------------------------
    # 1. is_trading_day — holidays
    # ------------------------------------------------------------------
    separator("1. is_trading_day — holidays")
    queries = [
        (date(2026, 7, 4),  "Independence Day (Sat observed Fri Jul 3)"),
        (date(2026, 7, 3),  "NYSE-observed Independence Day 2026"),
        (date(2026, 4, 18), "Good Friday 2026"),
        (date(2026, 5, 25), "Memorial Day 2026"),
        (date(2026, 11, 26),"Thanksgiving 2026"),
        (date(2026, 12, 25),"Christmas 2026"),
        (date(2026, 4, 21), "Tuesday after Good Friday 2026 (open)"),
        (date(2026, 4, 25), "Saturday (always closed)"),
        (date(2026, 4, 26), "Sunday (always closed)"),
    ]
    for d, label in queries:
        result = cal.is_trading_day(d)
        print(f"  is_trading_day({d})  →  {result!s:<5}  # {label}")

    # ------------------------------------------------------------------
    # 2. previous_trading_day — holiday and weekend crossing
    # ------------------------------------------------------------------
    separator("2. previous_trading_day")
    prev_queries = [
        (date(2026, 4, 20), "Monday after Good Friday — expect 2026-04-17 (Thu)"),
        (date(2026, 4, 27), "Regular Monday — expect 2026-04-24 (Fri)"),
        (date(2026, 5, 26), "Tuesday after Memorial Day — expect 2026-05-22 (Fri)"),
        (date(2026, 11, 30),"Monday after Thanksgiving — expect 2026-11-27 (Fri)"),
        (date(2026, 12, 28),"Monday after Christmas — expect 2026-12-24 (Thu)"),
        (date(2026, 1, 1),  "New Year's Day — expect 2025-12-31 (Wed)"),
    ]
    for d, label in prev_queries:
        result = cal.previous_trading_day(d)
        print(f"  previous_trading_day({d})  →  {result}  # {label}")

    # ------------------------------------------------------------------
    # 3. trading_days_after — MLK Day crossing
    # ------------------------------------------------------------------
    separator("3. trading_days_after — skips weekends and holidays")
    after_queries = [
        (date(2026, 1, 1),  5, "New Year's Day + 5 — expect 2026-01-08 (Thu)"),
        (date(2026, 1, 15), 5, "Thu Jan 15 + 5 skipping MLK Jan 19 — expect 2026-01-23"),
        (date(2026, 4, 17), 3, "Thu Apr 17 + 3 (skips Apr 18 Good Friday) — expect 2026-04-23"),
        (date(2026, 12, 24),1, "Christmas Eve + 1 (skip Christmas) — expect 2026-12-28"),
    ]
    for d, n, label in after_queries:
        result = cal.trading_days_after(d, n)
        print(f"  trading_days_after({d}, {n})  →  {result}  # {label}")

    # ------------------------------------------------------------------
    # 4. collection_window — prev_close at various month boundaries
    # ------------------------------------------------------------------
    separator("4. collection_window — (prev_close_ET, send_time_ET)")
    window_queries = [
        datetime(2026, 4, 21, 9, 0, 0, tzinfo=ET),   # Mon after Good Friday
        datetime(2026, 5, 26, 9, 0, 0, tzinfo=ET),   # Tue after Memorial Day
        datetime(2026, 11, 30, 9, 0, 0, tzinfo=ET),  # Mon after Thanksgiving
        datetime(2026, 12, 28, 9, 0, 0, tzinfo=ET),  # Mon after Christmas
        datetime(2026, 4, 27, 9, 0, 0, tzinfo=ET),   # Regular Monday
        datetime(2026, 2, 2, 9, 0, 0, tzinfo=ET),    # Monday in Feb (month boundary)
    ]
    for send_time in window_queries:
        prev_close, _ = cal.collection_window(send_time)
        print(
            f"  send={send_time.strftime('%Y-%m-%d %H:%M ET')}  "
            f"→  prev_close={prev_close.strftime('%Y-%m-%d %H:%M ET')}"
        )

    # ------------------------------------------------------------------
    # 5. trading_days_between
    # ------------------------------------------------------------------
    separator("5. trading_days_between — span across Good Friday")
    days = cal.trading_days_between(date(2026, 4, 16), date(2026, 4, 22))
    print(f"  trading_days_between(2026-04-16, 2026-04-22)  →  {days}")
    print("  (April 18 Good Friday and April 19 Sunday excluded)")

    print("\n--- Demo complete ---\n")


if __name__ == "__main__":
    main()
