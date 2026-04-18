"""Unit tests for HolidayCalendar and PipelineOrchestrator.

Covers TASK-015 acceptance criteria:
- HolidayCalendar.is_trading_day: weekends, NYSE holidays, regular weekdays.
- HolidayCalendar.trading_days_after: skips weekends and holidays.
- run_morning: non-trading day skipped; IngestorError → failure email + no watchlist;
  unhandled exception → failure email; dry-run renders to stdout.
- run_evening: non-trading day skipped; success → ok + scorecard; exception → failure email.
- trading_days_before: skips weekends and holidays counting backwards.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from influence_monitor.calendar import (
    HolidayCalendar,
    _easter,
    _nyse_holidays,
    _observe,
)
from influence_monitor.ingestion.base import IngestorError
from influence_monitor.pipeline import (
    PipelineOrchestrator,
    _overnight_since,
)


# ======================================================================
# HolidayCalendar — pure helpers
# ======================================================================


class TestEaster:
    def test_2026(self) -> None:
        assert _easter(2026) == date(2026, 4, 5)

    def test_2025(self) -> None:
        assert _easter(2025) == date(2025, 4, 20)

    def test_2024(self) -> None:
        assert _easter(2024) == date(2024, 3, 31)


class TestObserve:
    def test_sunday_observed_monday(self) -> None:
        sunday = date(2022, 1, 2)  # Jan 2 2022 was a Sunday
        assert _observe(sunday) == date(2022, 1, 3)

    def test_saturday_observed_friday(self) -> None:
        saturday = date(2021, 7, 3)  # July 3 2021 was a Saturday
        assert _observe(saturday) == date(2021, 7, 2)

    def test_weekday_unchanged(self) -> None:
        monday = date(2026, 4, 13)
        assert _observe(monday) == monday


class TestNyseHolidays:
    """Spot-check specific NYSE holidays for 2026."""

    def test_new_years_day_2026(self) -> None:
        # Jan 1 2026 is Thursday
        assert date(2026, 1, 1) in _nyse_holidays(2026)

    def test_mlk_day_2026(self) -> None:
        # 3rd Monday January 2026 = Jan 19
        assert date(2026, 1, 19) in _nyse_holidays(2026)

    def test_presidents_day_2026(self) -> None:
        # 3rd Monday February 2026 = Feb 16
        assert date(2026, 2, 16) in _nyse_holidays(2026)

    def test_good_friday_2026(self) -> None:
        # Easter 2026 is Apr 5, Good Friday = Apr 3
        assert date(2026, 4, 3) in _nyse_holidays(2026)

    def test_memorial_day_2026(self) -> None:
        # Last Monday May 2026 = May 25
        assert date(2026, 5, 25) in _nyse_holidays(2026)

    def test_juneteenth_2026(self) -> None:
        # Jun 19 2026 is a Friday
        assert date(2026, 6, 19) in _nyse_holidays(2026)

    def test_independence_day_2026(self) -> None:
        # Jul 4 2026 is a Saturday → observed Fri Jul 3
        assert date(2026, 7, 3) in _nyse_holidays(2026)
        assert date(2026, 7, 4) not in _nyse_holidays(2026)

    def test_labor_day_2026(self) -> None:
        # 1st Monday Sep 2026 = Sep 7
        assert date(2026, 9, 7) in _nyse_holidays(2026)

    def test_thanksgiving_2026(self) -> None:
        # 4th Thursday Nov 2026 = Nov 26
        assert date(2026, 11, 26) in _nyse_holidays(2026)

    def test_christmas_2026(self) -> None:
        # Dec 25 2026 is a Friday
        assert date(2026, 12, 25) in _nyse_holidays(2026)


class TestHolidayCalendar:
    def setup_method(self) -> None:
        # Pin calendar to 2026 only to avoid year-boundary edge cases
        self.cal = HolidayCalendar(years=range(2026, 2027))

    def test_saturday_is_not_trading_day(self) -> None:
        assert self.cal.is_trading_day(date(2026, 4, 11)) is False

    def test_sunday_is_not_trading_day(self) -> None:
        assert self.cal.is_trading_day(date(2026, 4, 12)) is False

    def test_regular_monday_is_trading_day(self) -> None:
        assert self.cal.is_trading_day(date(2026, 4, 13)) is True

    def test_regular_friday_is_trading_day(self) -> None:
        assert self.cal.is_trading_day(date(2026, 4, 17)) is True

    def test_good_friday_2026_not_trading(self) -> None:
        assert self.cal.is_trading_day(date(2026, 4, 3)) is False

    def test_mlk_day_not_trading(self) -> None:
        assert self.cal.is_trading_day(date(2026, 1, 19)) is False

    def test_christmas_not_trading(self) -> None:
        assert self.cal.is_trading_day(date(2026, 12, 25)) is False

    def test_trading_days_after_skips_weekend(self) -> None:
        # Friday Apr 17 → next trading day is Monday Apr 20
        result = self.cal.trading_days_after(date(2026, 4, 17), 1)
        assert result == date(2026, 4, 20)

    def test_trading_days_after_skips_holiday(self) -> None:
        # Thursday Apr 2 → +1 trading day skips Good Friday (Apr 3) → Mon Apr 6
        result = self.cal.trading_days_after(date(2026, 4, 2), 1)
        assert result == date(2026, 4, 6)

    def test_trading_days_after_five_days(self) -> None:
        # Monday Apr 13 + 5 trading days = Monday Apr 20
        result = self.cal.trading_days_after(date(2026, 4, 13), 5)
        assert result == date(2026, 4, 20)

    def test_trading_days_before_skips_weekend(self) -> None:
        # Monday Apr 20 → 1 trading day back = Friday Apr 17
        result = self.cal.trading_days_before(date(2026, 4, 20), 1)
        assert result == date(2026, 4, 17)

    def test_trading_days_before_skips_holiday(self) -> None:
        # Monday Apr 6 → 1 trading day back skips Good Friday (Apr 3) → Thu Apr 2
        result = self.cal.trading_days_before(date(2026, 4, 6), 1)
        assert result == date(2026, 4, 2)

    def test_trading_days_before_five_days(self) -> None:
        # Monday Apr 20 − 5 trading days = Monday Apr 13
        result = self.cal.trading_days_before(date(2026, 4, 20), 5)
        assert result == date(2026, 4, 13)

    def test_trading_days_before_after_are_inverses(self) -> None:
        start = date(2026, 4, 13)
        forward = self.cal.trading_days_after(start, 7)
        back = self.cal.trading_days_before(forward, 7)
        assert back == start


# ======================================================================
# PipelineOrchestrator — all I/O mocked
# ======================================================================

_RUN_DATE = date(2026, 4, 15)  # Wednesday — regular trading day
_HOLIDAY = date(2026, 4, 3)    # Good Friday 2026 — NYSE closed


def _make_orchestrator(
    *,
    is_trading_day: bool = True,
    scorecard_summary: dict | None = None,
    ingestor_error: Exception | None = None,
    morning_error: Exception | None = None,
    evening_error: Exception | None = None,
) -> tuple[PipelineOrchestrator, MagicMock]:
    """Build a PipelineOrchestrator with every component mocked."""
    settings = MagicMock()
    settings.recipient_email = "user@example.com"
    settings.conviction_min = 3
    settings.signal_min_score = 5.0
    settings.top_n_signals = 5
    settings.corroboration_multiplier = 1.5

    repo = AsyncMock()
    repo.get_active_investors.return_value = []
    repo.upsert_daily_summary.return_value = 1

    ingestor = AsyncMock()
    if ingestor_error:
        ingestor.fetch_all_accounts.side_effect = ingestor_error
    else:
        ingestor.fetch_all_accounts.return_value = ([], 0, 0)

    scorecard_engine = AsyncMock()
    scorecard_engine.run_evening.return_value = (
        scorecard_summary or {"signals_scored": 0, "hits": 0, "misses": 0, "skipped": 0, "errors": 0}
    )
    if evening_error:
        scorecard_engine.run_evening.side_effect = evening_error

    morning_email = MagicMock()
    morning_email.subject = "Morning Watchlist"
    morning_email.html_body = "<pre>watchlist</pre>"
    morning_email.text_body = "watchlist"
    morning_renderer = AsyncMock()
    morning_renderer.render.return_value = morning_email
    # render_from_rows is synchronous — must not be an AsyncMock
    morning_renderer.render_from_rows = MagicMock(return_value=morning_email)

    evening_renderer = AsyncMock()
    evening_email = MagicMock()
    evening_email.subject = "Evening Scorecard"
    evening_email.html_body = "<pre>scorecard</pre>"
    evening_email.text_body = "scorecard"
    evening_renderer.render.return_value = evening_email

    email_provider = AsyncMock()
    email_provider.send.return_value = None

    calendar = MagicMock()
    calendar.is_trading_day.return_value = is_trading_day

    corroboration = MagicMock()
    corroboration.detect.return_value = []

    aggregator = MagicMock()
    aggregator.rank.return_value = []

    index_resolver = AsyncMock()
    index_resolver.resolve.return_value = "MICRO"

    market_client = MagicMock()

    orch = PipelineOrchestrator(
        settings=settings,
        repo=repo,
        ingestor=ingestor,
        ticker_extractor=MagicMock(),
        llm_client=MagicMock(),
        scoring_engine=MagicMock(),
        corroboration_detector=corroboration,
        aggregator=aggregator,
        index_resolver=index_resolver,
        market_client=market_client,
        scorecard_engine=scorecard_engine,
        morning_renderer=morning_renderer,
        evening_renderer=evening_renderer,
        email_provider=email_provider,
        calendar=calendar,
    )
    return orch, email_provider


class TestRunMorning:
    @pytest.mark.asyncio
    async def test_non_trading_day_skipped(self) -> None:
        orch, email = _make_orchestrator(is_trading_day=False)
        result = await orch.run_morning(_HOLIDAY)
        assert result["status"] == "skipped"
        email.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_success_returns_ok(self) -> None:
        orch, email = _make_orchestrator()
        result = await orch.run_morning(_RUN_DATE)
        assert result["status"] == "ok"

    @pytest.mark.asyncio
    async def test_success_sends_watchlist_email(self) -> None:
        orch, email = _make_orchestrator()
        await orch.run_morning(_RUN_DATE)
        email.send.assert_called_once()
        call_kwargs = email.send.call_args.kwargs
        assert call_kwargs["to"] == "user@example.com"

    @pytest.mark.asyncio
    async def test_ingestor_error_sends_failure_email(self) -> None:
        orch, email = _make_orchestrator(ingestor_error=IngestorError("API down"))
        result = await orch.run_morning(_RUN_DATE)
        assert result["status"] == "failed"
        assert "IngestorError" in result["error"]
        # Failure email sent, but NOT watchlist email
        email.send.assert_called_once()
        subject = email.send.call_args.kwargs["subject"]
        assert "FAILED" in subject

    @pytest.mark.asyncio
    async def test_ingestor_error_writes_failed_summary(self) -> None:
        orch, _ = _make_orchestrator(ingestor_error=IngestorError("API down"))
        await orch.run_morning(_RUN_DATE)
        orch._repo.upsert_daily_summary.assert_called_once()
        kwargs = orch._repo.upsert_daily_summary.call_args.kwargs
        assert kwargs["pipeline_status"] == "failed"

    @pytest.mark.asyncio
    async def test_unhandled_exception_sends_failure_email(self) -> None:
        orch, email = _make_orchestrator(ingestor_error=RuntimeError("unexpected crash"))
        result = await orch.run_morning(_RUN_DATE)
        assert result["status"] == "failed"
        email.send.assert_called_once()
        subject = email.send.call_args.kwargs["subject"]
        assert "FAILED" in subject

    @pytest.mark.asyncio
    async def test_dry_run_does_not_send_email(self, capsys) -> None:
        orch, email = _make_orchestrator()
        result = await orch.run_morning(_RUN_DATE, dry_run=True)
        assert result["status"] == "dry_run"
        email.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_dry_run_does_not_write_db(self, capsys) -> None:
        orch, _ = _make_orchestrator()
        await orch.run_morning(_RUN_DATE, dry_run=True)
        orch._repo.upsert_daily_summary.assert_not_called()

    @pytest.mark.asyncio
    async def test_success_writes_morning_daily_summary(self) -> None:
        orch, _ = _make_orchestrator()
        await orch.run_morning(_RUN_DATE)
        orch._repo.upsert_daily_summary.assert_called_once()
        kwargs = orch._repo.upsert_daily_summary.call_args.kwargs
        assert kwargs["run_type"] == "morning"
        assert kwargs["pipeline_status"] == "ok"

    @pytest.mark.asyncio
    async def test_ingestor_error_does_not_send_watchlist(self) -> None:
        """Critical: on IngestorError, NO watchlist email is sent to user."""
        orch, email = _make_orchestrator(ingestor_error=IngestorError("only 2 accounts"))
        await orch.run_morning(_RUN_DATE)
        # Exactly one email sent: the failure notification, not the watchlist
        assert email.send.call_count == 1
        subject = email.send.call_args.kwargs["subject"]
        assert "Watchlist" not in subject


class TestRunEvening:
    @pytest.mark.asyncio
    async def test_non_trading_day_skipped(self) -> None:
        orch, email = _make_orchestrator(is_trading_day=False)
        result = await orch.run_evening(_HOLIDAY)
        assert result["status"] == "skipped"
        email.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_success_returns_ok_with_summary(self) -> None:
        summary = {"signals_scored": 3, "hits": 2, "misses": 1, "skipped": 0, "errors": 0}
        orch, _ = _make_orchestrator(scorecard_summary=summary)
        result = await orch.run_evening(_RUN_DATE)
        assert result["status"] == "ok"
        assert result["hits"] == 2
        assert result["misses"] == 1
        assert result["signals_scored"] == 3

    @pytest.mark.asyncio
    async def test_success_sends_scorecard_email(self) -> None:
        orch, email = _make_orchestrator()
        await orch.run_evening(_RUN_DATE)
        email.send.assert_called_once()
        assert email.send.call_args.kwargs["to"] == "user@example.com"

    @pytest.mark.asyncio
    async def test_exception_sends_failure_email(self) -> None:
        orch, email = _make_orchestrator(evening_error=RuntimeError("DB error"))
        result = await orch.run_evening(_RUN_DATE)
        assert result["status"] == "failed"
        email.send.assert_called_once()
        assert "FAILED" in email.send.call_args.kwargs["subject"]

    @pytest.mark.asyncio
    async def test_dry_run_does_not_send_email(self, capsys) -> None:
        orch, email = _make_orchestrator()
        result = await orch.run_evening(_RUN_DATE, dry_run=True)
        assert result["status"] == "dry_run"
        email.send.assert_not_called()


# ======================================================================
# _overnight_since helper
# ======================================================================


class TestOvernightSince:
    def test_returns_datetime_in_utc(self) -> None:
        result = _overnight_since(date(2026, 4, 15))
        assert isinstance(result, datetime)
        assert result.tzinfo == timezone.utc

    def test_approximately_15_hours_before_midnight(self) -> None:
        result = _overnight_since(date(2026, 4, 15))
        # Midnight UTC of Apr 15 minus 15 hours = Apr 14 09:00 UTC
        expected = datetime(2026, 4, 14, 9, 0, tzinfo=timezone.utc)
        assert result == expected
