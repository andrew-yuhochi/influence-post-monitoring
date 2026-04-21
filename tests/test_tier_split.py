"""Unit tests for the rank-first tier-split logic introduced in STEP 8 of pipeline.py.

The split is pure Python (list operations on ScoredSignal objects), so we replicate
it verbatim here as a standalone helper and test it in isolation — no DB, no API
calls, no Settings object required.

STEP 8 algorithm (from pipeline.py lines 713-730):
  1. Exclude UNSCORED signals.
  2. Sort remaining by final_score descending.
  3. Top 5 → ACT_NOW (unconditional, mutate tier in place).
  4. remainder = sorted[5:]
  5. From remainder, keep only signals whose tier == "WATCH" → top 5 of those → WATCH.

Important: SignalClassifier assigns WATCH when views_per_hour >= vel_floor (1000 default);
otherwise UNSCORED.  ACT_NOW is never assigned by ScoringEngine — only the pipeline
step does that.  So in all mock data, pre-split tiers are either "WATCH" or "UNSCORED".
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from influence_monitor.ingestion.base import RawPost
from influence_monitor.scoring.llm_client import PostScore
from influence_monitor.scoring.scoring_engine import ScoredSignal


# ---------------------------------------------------------------------------
# Helpers — replicate STEP 8 exactly as written in pipeline.py
# ---------------------------------------------------------------------------

def _apply_tier_split(
    scored_signals: list[ScoredSignal],
) -> tuple[list[ScoredSignal], list[ScoredSignal]]:
    """Replicate pipeline.py STEP 8 rank-first tier split.

    Returns (act_now_signals_raw, watch_signals_raw).
    Mutates tier on the passed-in ScoredSignal objects (same as the real pipeline).
    """
    qualifying = [s for s in scored_signals if s.tier != "UNSCORED"]
    all_sorted = sorted(qualifying, key=lambda s: s.final_score, reverse=True)

    act_now_signals_raw = all_sorted[:5]
    for s in act_now_signals_raw:
        s.tier = "ACT_NOW"

    remaining = all_sorted[5:]
    watch_signals_raw = sorted(
        [s for s in remaining if s.tier == "WATCH"],
        key=lambda s: s.final_score,
        reverse=True,
    )[:5]
    for s in watch_signals_raw:
        s.tier = "WATCH"

    return act_now_signals_raw, watch_signals_raw


# ---------------------------------------------------------------------------
# Fixture factory
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 21, 9, 0, 0, tzinfo=timezone.utc)


def _make_signal(
    ticker: str,
    final_score: float,
    tier: str,  # pre-split tier: "WATCH" or "UNSCORED"
    views_per_hour: float | None = None,
    view_count: int | None = None,
) -> ScoredSignal:
    """Build a minimal ScoredSignal suitable for tier-split tests."""
    return ScoredSignal(
        ticker=ticker,
        direction="LONG",
        conviction_level=3,
        argument_quality="HIGH",
        time_horizon="weeks",
        market_moving_potential=True,
        key_claim="Test claim",
        rationale="Test rationale",
        extraction_confidence="HIGH",
        score_credibility=0.7,
        score_virality_abs=0.5,
        score_virality_vel=None,
        score_consensus=0.8,
        score_amplifier=None,
        liquidity_modifier=None,
        conviction_score=final_score,
        final_score=final_score,
        penalty_applied=0.0,
        tier=tier,  # type: ignore[arg-type]
        direction_flip=False,
        conflict_group=None,
        engagement_views=view_count,
        engagement_reposts=None,
        views_per_hour=views_per_hour,
        account_handle="testhandle",
    )


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestTop5Unconditional:
    """TC-1: Top-5 ACT_NOW is determined purely by final_score rank."""

    def test_top_five_become_act_now_regardless_of_view_count(self) -> None:
        """8 signals; the top 5 by score include low-view posts.

        Signals 1-5 (high score) → ACT_NOW even when view_count is tiny.
        Signals 6-8 (lower score) must NOT be ACT_NOW.
        """
        signals = [
            # Rank 1 — intentionally low view count
            _make_signal("AAPL", final_score=9.5, tier="WATCH", views_per_hour=5000, view_count=100),
            # Ranks 2-5 — normal
            _make_signal("TSLA", final_score=8.8, tier="WATCH", views_per_hour=3000, view_count=50_000),
            _make_signal("NVDA", final_score=7.9, tier="WATCH", views_per_hour=2000, view_count=40_000),
            _make_signal("MSFT", final_score=7.2, tier="WATCH", views_per_hour=1500, view_count=30_000),
            _make_signal("AMZN", final_score=6.5, tier="WATCH", views_per_hour=1200, view_count=25_000),
            # Ranks 6-8 — should NOT be ACT_NOW
            _make_signal("META",  final_score=5.1, tier="WATCH", views_per_hour=1100, view_count=20_000),
            _make_signal("GOOG",  final_score=4.3, tier="WATCH", views_per_hour=1050, view_count=15_000),
            _make_signal("NFLX",  final_score=3.0, tier="WATCH", views_per_hour=1010, view_count=10_000),
        ]

        act_now, watch = _apply_tier_split(signals)

        act_now_tickers = {s.ticker for s in act_now}
        assert act_now_tickers == {"AAPL", "TSLA", "NVDA", "MSFT", "AMZN"}, (
            f"Expected top-5 tickers in ACT_NOW, got {act_now_tickers}"
        )

        for s in act_now:
            assert s.tier == "ACT_NOW", f"{s.ticker} should be ACT_NOW, got {s.tier}"

        non_act_now_tickers = {s.ticker for s in signals} - act_now_tickers
        assert non_act_now_tickers == {"META", "GOOG", "NFLX"}
        for s in signals:
            if s.ticker in non_act_now_tickers:
                assert s.tier != "ACT_NOW", (
                    f"{s.ticker} scored {s.final_score} (rank 6+) but was marked ACT_NOW"
                )


class TestLowViewInTop5:
    """TC-2: A signal with very low view_count but highest score enters ACT_NOW."""

    def test_low_view_signal_with_highest_score_is_act_now(self) -> None:
        """view_count=100 is far below any virality threshold but score=9.9 → ACT_NOW."""
        signals = [
            _make_signal("TINY", final_score=9.9, tier="WATCH", views_per_hour=1001, view_count=100),
            _make_signal("BIG1", final_score=8.0, tier="WATCH", views_per_hour=5000, view_count=500_000),
            _make_signal("BIG2", final_score=7.0, tier="WATCH", views_per_hour=4000, view_count=400_000),
            _make_signal("BIG3", final_score=6.0, tier="WATCH", views_per_hour=3000, view_count=300_000),
            _make_signal("BIG4", final_score=5.0, tier="WATCH", views_per_hour=2000, view_count=200_000),
        ]

        act_now, _ = _apply_tier_split(signals)

        act_now_tickers = {s.ticker for s in act_now}
        assert "TINY" in act_now_tickers, (
            f"Low-view signal TINY (score 9.9) must be ACT_NOW; got {act_now_tickers}"
        )
        tiny = next(s for s in signals if s.ticker == "TINY")
        assert tiny.tier == "ACT_NOW"
        assert tiny.engagement_views == 100


class TestWatchFromRemainder:
    """TC-3: After ACT_NOW selection, velocity-eligible remainder → WATCH;
    UNSCORED remainder stays excluded."""

    def test_velocity_eligible_remainder_becomes_watch(self) -> None:
        """Signals 6 and 7 have tier WATCH (vph >= floor) → appear in WATCH.
        Signal 8 has tier UNSCORED (vph < floor) → NOT in WATCH.
        """
        signals = [
            # Top-5 by score → ACT_NOW
            _make_signal("S1", final_score=9.0, tier="WATCH", views_per_hour=5000),
            _make_signal("S2", final_score=8.0, tier="WATCH", views_per_hour=4000),
            _make_signal("S3", final_score=7.0, tier="WATCH", views_per_hour=3000),
            _make_signal("S4", final_score=6.0, tier="WATCH", views_per_hour=2000),
            _make_signal("S5", final_score=5.0, tier="WATCH", views_per_hour=1500),
            # Remainder
            _make_signal("W1", final_score=4.5, tier="WATCH",    views_per_hour=1200),  # eligible
            _make_signal("W2", final_score=3.5, tier="WATCH",    views_per_hour=1100),  # eligible
            _make_signal("U1", final_score=2.0, tier="UNSCORED", views_per_hour=500),   # not eligible
        ]

        act_now, watch = _apply_tier_split(signals)

        act_now_tickers = {s.ticker for s in act_now}
        watch_tickers = {s.ticker for s in watch}

        # ACT_NOW are the top 5
        assert act_now_tickers == {"S1", "S2", "S3", "S4", "S5"}

        # WATCH contains the velocity-eligible remainder
        assert "W1" in watch_tickers, "W1 (WATCH remainder) should be in WATCH"
        assert "W2" in watch_tickers, "W2 (WATCH remainder) should be in WATCH"

        # UNSCORED remainder must not appear in WATCH
        assert "U1" not in watch_tickers, (
            "U1 has tier=UNSCORED and must not appear in WATCH"
        )

        for s in watch:
            assert s.tier == "WATCH", f"{s.ticker} in watch list has tier={s.tier}"


class TestFewerThanFiveSignals:
    """TC-4: 3 signals total → all 3 become ACT_NOW; WATCH is empty."""

    def test_three_signals_all_become_act_now(self) -> None:
        signals = [
            _make_signal("A", final_score=7.0, tier="WATCH", views_per_hour=2000),
            _make_signal("B", final_score=5.0, tier="WATCH", views_per_hour=1500),
            _make_signal("C", final_score=3.0, tier="WATCH", views_per_hour=1001),
        ]

        act_now, watch = _apply_tier_split(signals)

        assert len(act_now) == 3, f"Expected 3 ACT_NOW signals, got {len(act_now)}"
        assert len(watch) == 0, f"Expected 0 WATCH signals, got {len(watch)}"

        for s in act_now:
            assert s.tier == "ACT_NOW", f"{s.ticker} should be ACT_NOW, got {s.tier}"

    def test_one_signal_becomes_act_now(self) -> None:
        signals = [
            _make_signal("SOLO", final_score=6.0, tier="WATCH", views_per_hour=1500),
        ]

        act_now, watch = _apply_tier_split(signals)

        assert len(act_now) == 1
        assert len(watch) == 0
        assert act_now[0].ticker == "SOLO"
        assert act_now[0].tier == "ACT_NOW"


class TestActNowExcludedFromWatch:
    """TC-5: A signal assigned ACT_NOW must NOT also appear in WATCH,
    even if it has high views_per_hour."""

    def test_act_now_signals_absent_from_watch(self) -> None:
        """6 signals. Top-5 → ACT_NOW.  The 6th has high vph but lands in
        WATCH — verify no signal appears in both lists."""
        signals = [
            _make_signal("T1", final_score=9.0, tier="WATCH", views_per_hour=9000),
            _make_signal("T2", final_score=8.0, tier="WATCH", views_per_hour=8000),
            _make_signal("T3", final_score=7.0, tier="WATCH", views_per_hour=7000),
            _make_signal("T4", final_score=6.0, tier="WATCH", views_per_hour=6000),
            _make_signal("T5", final_score=5.0, tier="WATCH", views_per_hour=5000),
            # T6 has highest vph but lowest score → goes to WATCH remainder, not ACT_NOW
            _make_signal("T6", final_score=4.0, tier="WATCH", views_per_hour=50_000),
        ]

        act_now, watch = _apply_tier_split(signals)

        act_now_tickers = {s.ticker for s in act_now}
        watch_tickers = {s.ticker for s in watch}

        # No overlap between the two lists
        overlap = act_now_tickers & watch_tickers
        assert not overlap, (
            f"Signals {overlap} appear in both ACT_NOW and WATCH — must be mutually exclusive"
        )

        # T1-T5 are ACT_NOW, T6 is the only WATCH candidate
        assert act_now_tickers == {"T1", "T2", "T3", "T4", "T5"}
        assert "T6" in watch_tickers


class TestNoVelocityEligibleRemainder:
    """TC-6: 6 signals; top 5 → ACT_NOW; 6th is UNSCORED → WATCH must be empty."""

    def test_unscored_remainder_yields_empty_watch(self) -> None:
        signals = [
            _make_signal("S1", final_score=9.0, tier="WATCH",    views_per_hour=2000),
            _make_signal("S2", final_score=8.0, tier="WATCH",    views_per_hour=1800),
            _make_signal("S3", final_score=7.0, tier="WATCH",    views_per_hour=1600),
            _make_signal("S4", final_score=6.0, tier="WATCH",    views_per_hour=1400),
            _make_signal("S5", final_score=5.0, tier="WATCH",    views_per_hour=1200),
            # 6th signal: UNSCORED — not velocity-eligible
            _make_signal("U1", final_score=4.0, tier="UNSCORED", views_per_hour=500),
        ]

        act_now, watch = _apply_tier_split(signals)

        assert len(act_now) == 5, f"Expected 5 ACT_NOW, got {len(act_now)}"
        assert len(watch) == 0, (
            f"Expected empty WATCH (only UNSCORED in remainder), got {[s.ticker for s in watch]}"
        )

    def test_no_qualifying_signals_at_all(self) -> None:
        """If every signal is UNSCORED, both lists are empty."""
        signals = [
            _make_signal("U1", final_score=0.0, tier="UNSCORED", views_per_hour=100),
            _make_signal("U2", final_score=0.0, tier="UNSCORED", views_per_hour=200),
        ]

        act_now, watch = _apply_tier_split(signals)

        assert act_now == []
        assert watch == []
