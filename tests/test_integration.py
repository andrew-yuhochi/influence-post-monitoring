"""Integration tests for the morning and evening pipeline.

All external APIs (twikit, Claude/Anthropic, yfinance, Resend) are mocked.
Tests run against an in-memory SQLite database with the full schema + seed data.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from influence_monitor.calendar import HolidayCalendar
from influence_monitor.config import Settings
from influence_monitor.db.repository import DatabaseRepository
from influence_monitor.email.renderer import (
    EveningScorecardRenderer,
    MorningWatchlistRenderer,
)
from influence_monitor.extraction.ticker_extractor import ExtractedTicker, TickerExtractor
from influence_monitor.ingestion.base import RawPost
from influence_monitor.pipeline import PipelineOrchestrator
from influence_monitor.scorecard.scorecard_engine import ScorecardEngine
from influence_monitor.scoring.aggregator import SignalAggregator
from influence_monitor.scoring.corroboration import CorroborationDetector
from influence_monitor.scoring.llm_client import PostScore
from influence_monitor.scoring.scoring_engine import ScoringEngine

# ---------------------------------------------------------------------------
# Test run date — confirmed trading day (Wednesday 2026-04-15)
# ---------------------------------------------------------------------------

_RUN_DATE = date(2026, 4, 15)


# ---------------------------------------------------------------------------
# Settings helper
# ---------------------------------------------------------------------------


def _test_settings() -> Settings:
    return Settings(
        database_path=":memory:",
        anthropic_api_key="test",
        resend_api_key="test",
        twitter_username="test",
        twitter_email="test@test.com",
        twitter_password="test",
        recipient_email="user@example.com",
        conviction_min=2,
        signal_min_score=1.0,
        top_n_signals=5,
        corroboration_multiplier=1.5,
    )


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def db():
    """In-memory repo with full schema + seed data."""
    repo = DatabaseRepository(_test_settings())
    await repo.connect()
    await repo.init_schema()
    await repo.seed()
    yield repo
    await repo.close()


# ---------------------------------------------------------------------------
# RawPost factory helpers
# ---------------------------------------------------------------------------


def _make_raw_post(
    handle: str,
    text: str,
    external_id: str = "1001",
    view_count: int = 5000,
    repost_count: int = 100,
) -> RawPost:
    now = datetime(2026, 4, 15, 7, 0, 0, tzinfo=timezone.utc)
    return RawPost(
        source_type="twitter",
        external_id=external_id,
        author_handle=handle,
        author_external_id=f"ext_{handle}",
        text=text,
        posted_at=now,
        fetched_at=now,
        view_count=view_count,
        repost_count=repost_count,
    )


# ---------------------------------------------------------------------------
# PostScore factory helpers
# ---------------------------------------------------------------------------


def _long_post_score(ticker: str, conviction: int = 5) -> PostScore:
    return PostScore(
        tickers=[ticker],
        direction="LONG",
        conviction_level=conviction,
        key_claim=f"Bullish on {ticker}",
        argument_quality="HIGH",
        time_horizon="weeks",
        market_moving_potential=True,
        rationale=f"Strong upside case for {ticker}",
    )


def _short_post_score(ticker: str, conviction: int = 4) -> PostScore:
    return PostScore(
        tickers=[ticker],
        direction="SHORT",
        conviction_level=conviction,
        key_claim=f"Bearish on {ticker}",
        argument_quality="HIGH",
        time_horizon="weeks",
        market_moving_potential=True,
        rationale=f"Fraud risk in {ticker}",
    )


def _neutral_post_score(ticker: str = "UNKNOWN") -> PostScore:
    return PostScore(
        tickers=[ticker],
        direction="NEUTRAL",
        conviction_level=0,
        key_claim="",
        argument_quality="LOW",
        time_horizon="unspecified",
        market_moving_potential=False,
        rationale="No actionable signal",
    )


# ---------------------------------------------------------------------------
# Orchestrator builder
# ---------------------------------------------------------------------------


async def _make_test_orchestrator(
    db: DatabaseRepository,
    ingestor_mock: Any,
    llm_mock: Any,
    market_mock: Any,
    email_mock: Any,
    index_resolver_mock: Any,
    settings: Settings | None = None,
) -> PipelineOrchestrator:
    """Build a PipelineOrchestrator wiring real and mocked components."""
    if settings is None:
        settings = _test_settings()

    # Real components
    scoring_engine = await ScoringEngine.from_db(db)
    corroboration_detector = CorroborationDetector(settings.corroboration_multiplier)
    aggregator = SignalAggregator()
    calendar = HolidayCalendar()

    # ScorecardEngine: real except market_client is mocked
    scorecard_engine = ScorecardEngine(market_mock, db, settings, calendar)

    # Renderers: real
    morning_renderer = MorningWatchlistRenderer(settings)
    evening_renderer = EveningScorecardRenderer(settings)

    # TickerExtractor: mock to avoid loading spaCy + whitelist in tests
    ticker_extractor = MagicMock(spec=TickerExtractor)

    return PipelineOrchestrator(
        settings=settings,
        repo=db,
        ingestor=ingestor_mock,
        ticker_extractor=ticker_extractor,
        llm_client=llm_mock,
        scoring_engine=scoring_engine,
        corroboration_detector=corroboration_detector,
        aggregator=aggregator,
        index_resolver=index_resolver_mock,
        market_client=market_mock,
        scorecard_engine=scorecard_engine,
        morning_renderer=morning_renderer,
        evening_renderer=evening_renderer,
        email_provider=email_mock,
        calendar=calendar,
    )


# ---------------------------------------------------------------------------
# Helper: configure the TickerExtractor mock to return a HIGH-confidence
# extraction for a given ticker.
# ---------------------------------------------------------------------------


def _configure_ticker_extractor(orchestrator: PipelineOrchestrator, ticker: str) -> None:
    """Make the orchestrator's ticker_extractor.extract() return one HIGH-confidence hit."""
    orchestrator._ticker_extractor.extract.return_value = [
        ExtractedTicker(ticker=ticker, confidence="HIGH", extraction_method="cashtag")
    ]


# ---------------------------------------------------------------------------
# Test 1: Morning pipeline scores signals and sends email
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_morning_pipeline_scores_and_sends(db: DatabaseRepository) -> None:
    """Two posts produce >= 1 signal; morning email is sent; signals are in DB."""
    post_ackman = _make_raw_post(
        handle="BillAckman",
        text="$FNMA LONG high conviction",
        external_id="2001",
        view_count=10000,
        repost_count=500,
    )
    post_citron = _make_raw_post(
        handle="CitronResearch",
        text="Short $NKLA fraud",
        external_id="2002",
        view_count=8000,
        repost_count=300,
    )

    # Ingestor mock
    ingestor = AsyncMock()
    ingestor.fetch_all_accounts.return_value = ([post_ackman, post_citron], 2, 0)

    # LLM mock — per-call routing
    score_fnma = _long_post_score("FNMA", conviction=5)
    score_nkla = _short_post_score("NKLA", conviction=4)

    llm = MagicMock()

    def _llm_side_effect(text: str, handle: str) -> PostScore:
        if handle == "BillAckman":
            return score_fnma
        return score_nkla

    llm.score_post.side_effect = _llm_side_effect
    llm.model_version.return_value = "claude-haiku-test"

    # Index resolver mock
    index_resolver = AsyncMock()
    index_resolver.initialize = AsyncMock()
    index_resolver.resolve.return_value = "MEGA"

    # Email provider mock
    email = AsyncMock()
    email.send.return_value = "msg-001"

    # Market client mock (not used in morning pipeline, but required for ScorecardEngine)
    market = MagicMock()

    orchestrator = await _make_test_orchestrator(
        db=db,
        ingestor_mock=ingestor,
        llm_mock=llm,
        market_mock=market,
        email_mock=email,
        index_resolver_mock=index_resolver,
    )

    # Configure ticker extractor to return both tickers depending on text
    def _ticker_extract_side_effect(text: str) -> list[ExtractedTicker]:
        if "FNMA" in text:
            return [ExtractedTicker(ticker="FNMA", confidence="HIGH", extraction_method="cashtag")]
        if "NKLA" in text:
            return [ExtractedTicker(ticker="NKLA", confidence="HIGH", extraction_method="cashtag")]
        return []

    orchestrator._ticker_extractor.extract.side_effect = _ticker_extract_side_effect

    result = await orchestrator.run_morning(_RUN_DATE)

    # Status OK
    assert result["status"] == "ok", f"Expected ok, got: {result}"
    assert result["signals"] >= 1, f"Expected >= 1 signals, got: {result['signals']}"

    # Email sent exactly once
    email.send.assert_called_once()

    # Subject contains one of the tickers
    subject = email.send.call_args.kwargs["subject"]
    assert "FNMA" in subject or "NKLA" in subject, f"Subject missing tickers: {subject}"

    # Signals in DB
    signals_in_db = await db.get_signals_for_date(_RUN_DATE)
    assert len(signals_in_db) > 0, "Expected signals inserted in DB"


# ---------------------------------------------------------------------------
# Test 2: Corroboration when two investors agree on same ticker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_morning_corroboration_two_investors(db: DatabaseRepository) -> None:
    """BillAckman and chamath both LONG FNMA → email body contains 'CORROBORATED'."""
    post_ackman = _make_raw_post(
        handle="BillAckman",
        text="$FNMA is massively undervalued",
        external_id="3001",
    )
    post_chamath = _make_raw_post(
        handle="chamath",
        text="$FNMA long conviction",
        external_id="3002",
    )

    ingestor = AsyncMock()
    ingestor.fetch_all_accounts.return_value = ([post_ackman, post_chamath], 2, 0)

    score_fnma = _long_post_score("FNMA", conviction=4)
    llm = MagicMock()
    llm.score_post.return_value = score_fnma
    llm.model_version.return_value = "claude-haiku-test"

    index_resolver = AsyncMock()
    index_resolver.initialize = AsyncMock()
    index_resolver.resolve.return_value = "MEGA"

    captured: dict[str, Any] = {}

    async def _capture_send(to: str, subject: str, html_body: str, text_body: str) -> str:
        captured["subject"] = subject
        captured["text_body"] = text_body
        return "msg-002"

    email = AsyncMock()
    email.send.side_effect = _capture_send

    market = MagicMock()

    orchestrator = await _make_test_orchestrator(
        db=db,
        ingestor_mock=ingestor,
        llm_mock=llm,
        market_mock=market,
        email_mock=email,
        index_resolver_mock=index_resolver,
    )

    orchestrator._ticker_extractor.extract.return_value = [
        ExtractedTicker(ticker="FNMA", confidence="HIGH", extraction_method="cashtag")
    ]

    result = await orchestrator.run_morning(_RUN_DATE)
    assert result["status"] == "ok", f"Expected ok, got: {result}"

    assert "text_body" in captured, "Email send was not called"
    assert "CORROBORATED" in captured["text_body"], (
        f"Expected 'CORROBORATED' in email body, got:\n{captured['text_body']}"
    )


# ---------------------------------------------------------------------------
# Test 3: Evening scorecard with hit and miss
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_evening_scorecard_hit_and_miss(db: DatabaseRepository) -> None:
    """Morning pipeline inserts signals; evening scorecard scores them HIT/MISS."""
    # --- Morning phase ---
    post_ackman = _make_raw_post(
        handle="BillAckman",
        text="$FNMA LONG",
        external_id="4001",
        view_count=10000,
        repost_count=500,
    )
    post_citron = _make_raw_post(
        handle="CitronResearch",
        text="Short $NKLA fraud",
        external_id="4002",
        view_count=8000,
        repost_count=300,
    )

    ingestor = AsyncMock()
    ingestor.fetch_all_accounts.return_value = ([post_ackman, post_citron], 2, 0)

    def _llm_side_effect(text: str, handle: str) -> PostScore:
        if handle == "BillAckman":
            return _long_post_score("FNMA", conviction=5)
        return _short_post_score("NKLA", conviction=4)

    llm = MagicMock()
    llm.score_post.side_effect = _llm_side_effect
    llm.model_version.return_value = "claude-haiku-test"

    index_resolver = AsyncMock()
    index_resolver.initialize = AsyncMock()
    index_resolver.resolve.return_value = "MEGA"

    morning_email = AsyncMock()
    morning_email.send.return_value = "msg-003"

    # OHLCV data: FNMA goes up (HIT for LONG), NKLA goes up (MISS for SHORT)
    # Also need to handle index tickers ^GSPC, ^VIX for regime context
    def _fetch_ohlcv(ticker: str, target_date: date) -> dict:
        if ticker == "FNMA":
            return {"open": 10.0, "high": 11.5, "low": 9.8, "close": 11.0, "volume": 1_000_000}
        if ticker == "NKLA":
            return {"open": 5.0, "high": 5.5, "low": 4.8, "close": 5.3, "volume": 500_000}
        # For index tickers (^GSPC, ^VIX, sector ETFs): return neutral data
        return {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 0}

    market = MagicMock()
    market.fetch_ohlcv.side_effect = _fetch_ohlcv

    orchestrator_morning = await _make_test_orchestrator(
        db=db,
        ingestor_mock=ingestor,
        llm_mock=llm,
        market_mock=market,
        email_mock=morning_email,
        index_resolver_mock=index_resolver,
    )

    def _ticker_extract_side_effect(text: str) -> list[ExtractedTicker]:
        if "FNMA" in text:
            return [ExtractedTicker(ticker="FNMA", confidence="HIGH", extraction_method="cashtag")]
        if "NKLA" in text:
            return [ExtractedTicker(ticker="NKLA", confidence="HIGH", extraction_method="cashtag")]
        return []

    orchestrator_morning._ticker_extractor.extract.side_effect = _ticker_extract_side_effect

    morning_result = await orchestrator_morning.run_morning(_RUN_DATE)
    assert morning_result["status"] == "ok", f"Morning pipeline failed: {morning_result}"
    assert morning_result["signals"] >= 1, "Expected at least 1 signal from morning run"

    # --- Evening phase ---
    evening_email = AsyncMock()
    captured_evening: dict[str, Any] = {}

    async def _capture_evening_send(to: str, subject: str, html_body: str, text_body: str) -> str:
        captured_evening["subject"] = subject
        captured_evening["text_body"] = text_body
        return "msg-004"

    evening_email.send.side_effect = _capture_evening_send

    # Reuse same market mock (already has correct side_effect)
    # Build a fresh orchestrator for the evening run using the same db
    ingestor2 = AsyncMock()
    ingestor2.fetch_all_accounts.return_value = ([], 0, 0)
    llm2 = MagicMock()
    llm2.model_version.return_value = "claude-haiku-test"
    index_resolver2 = AsyncMock()
    index_resolver2.initialize = AsyncMock()
    index_resolver2.resolve.return_value = "MEGA"

    orchestrator_evening = await _make_test_orchestrator(
        db=db,
        ingestor_mock=ingestor2,
        llm_mock=llm2,
        market_mock=market,
        email_mock=evening_email,
        index_resolver_mock=index_resolver2,
    )

    evening_result = await orchestrator_evening.run_evening(_RUN_DATE)

    assert evening_result["status"] == "ok", f"Evening pipeline failed: {evening_result}"
    assert evening_result.get("hits", 0) >= 1, f"Expected >= 1 hit, got: {evening_result}"

    evening_email.send.assert_called_once()

    assert "subject" in captured_evening, "Evening email send not called"
    subject = captured_evening["subject"]
    # "correct" appears in subject like "1/2 correct"
    assert "correct" in subject.lower(), f"Expected 'correct' in subject, got: {subject}"


# ---------------------------------------------------------------------------
# Test 4: Quiet night — no signals sends a "No Signals" email
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_signal_night_sends_quiet_email(db: DatabaseRepository) -> None:
    """When LLM returns zero-score (NEUTRAL), no signals are surfaced but email still sends."""
    post_ackman = _make_raw_post(
        handle="BillAckman",
        text="Just some thoughts, nothing actionable",
        external_id="5001",
    )

    ingestor = AsyncMock()
    ingestor.fetch_all_accounts.return_value = ([post_ackman], 1, 0)

    # LLM returns zero sentinel — direction AMBIGUOUS, conviction 0
    llm = MagicMock()
    llm.score_post.return_value = PostScore.zero_sentinel()
    llm.model_version.return_value = "claude-haiku-test"

    index_resolver = AsyncMock()
    index_resolver.initialize = AsyncMock()
    index_resolver.resolve.return_value = "MICRO"

    captured_morning: dict[str, Any] = {}

    async def _capture_morning_send(to: str, subject: str, html_body: str, text_body: str) -> str:
        captured_morning["subject"] = subject
        captured_morning["text_body"] = text_body
        return "msg-005"

    email = AsyncMock()
    email.send.side_effect = _capture_morning_send

    market = MagicMock()

    orchestrator = await _make_test_orchestrator(
        db=db,
        ingestor_mock=ingestor,
        llm_mock=llm,
        market_mock=market,
        email_mock=email,
        index_resolver_mock=index_resolver,
    )

    # Ticker extractor returns a ticker, but LLM zero-scores it → filtered out
    orchestrator._ticker_extractor.extract.return_value = [
        ExtractedTicker(ticker="FNMA", confidence="HIGH", extraction_method="cashtag")
    ]

    result = await orchestrator.run_morning(_RUN_DATE)

    assert result["status"] == "ok", f"Expected ok, got: {result}"
    assert result["signals"] == 0, f"Expected 0 signals, got: {result['signals']}"

    # Email was still sent (quiet night email)
    email.send.assert_called_once()

    assert "subject" in captured_morning, "Morning email send not called"
    subject = captured_morning["subject"]
    assert "No Signals" in subject, f"Expected 'No Signals' in subject, got: {subject}"
