"""Unit tests for SignalClassifier and ScoringEngine post-refactor behavior.

Covers:
  A. SignalClassifier.classify() — new contract: returns only WATCH or UNSCORED,
     never ACT_NOW under any input combination.
  B. ScoringEngine.score() — conviction_score, factor scores, and score bounds for
     WATCH and UNSCORED tiers.

No DB, no network. ScoringEngine is constructed with a mock repo.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from influence_monitor.ingestion.base import RawPost
from influence_monitor.scoring.llm_client import PostScore
from influence_monitor.scoring.scoring_engine import (
    ScoredSignal,
    ScoringEngine,
    ScoringInput,
    SignalClassifier,
)

# ---------------------------------------------------------------------------
# Shared config / window constants
# ---------------------------------------------------------------------------

_DEFAULT_CONFIG: dict[str, float] = {
    "weight_credibility": 0.25,
    "weight_virality_abs": 0.35,
    "weight_virality_vel": 0.15,
    "weight_consensus": 0.25,
    "weight_amplifier": 0.20,
    "virality_views_threshold": 50_000.0,
    "virality_reposts_threshold": 500.0,
    "watch_velocity_floor": 1_000.0,
    "direction_flip_penalty": 0.0,
}

# Window start: signal was posted 8.5 h before the window started, giving a
# non-trivial views-per-hour for any post with view_count > 0.
_WINDOW_START = datetime(2026, 4, 21, 6, 30, 0, tzinfo=timezone.utc)
_POSTED_AT = datetime(2026, 4, 20, 22, 0, 0, tzinfo=timezone.utc)  # 8.5 h before


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _make_repo(config: dict[str, float] | None = None) -> MagicMock:
    repo = MagicMock()
    repo.get_scoring_config.return_value = config or dict(_DEFAULT_CONFIG)
    return repo


def _make_post_score(
    direction: str = "LONG",
    conviction_level: int = 4,
) -> PostScore:
    return PostScore(
        tickers=["AAPL"],
        direction=direction,  # type: ignore[arg-type]
        conviction_level=conviction_level,
        key_claim="test claim",
        argument_quality="HIGH",
        time_horizon="months",
        market_moving_potential=True,
        rationale="test rationale",
    )


def _make_raw_post(
    views: int | None = 10_000,
    reposts: int | None = 50,
    handle: str = "poster_a",
) -> RawPost:
    return RawPost(
        source_type="twitter_twikit",
        external_id="ext_001",
        author_handle=handle,
        author_external_id="uid_001",
        text="test post",
        posted_at=_POSTED_AT,
        fetched_at=_WINDOW_START,
        view_count=views,
        repost_count=reposts,
    )


def _make_input(
    ticker: str = "AAPL",
    direction: str = "LONG",
    conviction_level: int = 4,
    views: int | None = 10_000,
    reposts: int | None = 50,
    credibility: float = 8.0,
    handle: str = "poster_a",
    distinct_same: int = 1,
    total_distinct: int = 1,
    posted_at: datetime = _POSTED_AT,
) -> ScoringInput:
    return ScoringInput(
        post_score=_make_post_score(
            direction=direction, conviction_level=conviction_level
        ),
        raw_post=_make_raw_post(views=views, reposts=reposts, handle=handle),
        account_credibility=credibility,
        posted_at=posted_at,
        collection_window_start=_WINDOW_START,
        account_handle=handle,
        distinct_same_direction_posters=distinct_same,
        total_distinct_posters_on_ticker=total_distinct,
        ticker=ticker,
    )


# ---------------------------------------------------------------------------
# Section A — SignalClassifier.classify() new behavior
# ---------------------------------------------------------------------------


class TestSignalClassifierNewBehavior:
    """SignalClassifier returns only WATCH or UNSCORED; never ACT_NOW."""

    def _clf(self, config: dict | None = None) -> SignalClassifier:
        return SignalClassifier(config or dict(_DEFAULT_CONFIG))

    # --- A1: returns WATCH when vph >= vel_floor ---

    def test_watch_when_vph_meets_floor(self) -> None:
        """vph == vel_floor (1000) and conviction >= 2, direction valid → WATCH."""
        clf = self._clf()
        result = clf.classify(
            views=10_000,
            reposts=50,
            views_per_hour=1_000.0,  # exactly at floor
            conviction_score=5.0,
            direction="LONG",
            conviction_level=3,
        )
        assert result == "WATCH", f"Expected WATCH at vel_floor, got {result}"

    def test_watch_when_vph_exceeds_floor(self) -> None:
        """vph well above vel_floor → still WATCH (not ACT_NOW)."""
        clf = self._clf()
        result = clf.classify(
            views=10_000,
            reposts=50,
            views_per_hour=5_000.0,
            conviction_score=8.0,
            direction="LONG",
            conviction_level=4,
        )
        assert result == "WATCH", f"Expected WATCH for high vph, got {result}"

    def test_watch_for_short_direction(self) -> None:
        """SHORT direction is still classified as WATCH when conditions met."""
        clf = self._clf()
        result = clf.classify(
            views=10_000,
            reposts=50,
            views_per_hour=2_000.0,
            conviction_score=6.0,
            direction="SHORT",
            conviction_level=3,
        )
        assert result == "WATCH"

    def test_watch_requires_conviction_level_2_minimum(self) -> None:
        """conviction_level == 2 is the minimum that can reach WATCH."""
        clf = self._clf()
        result = clf.classify(
            views=10_000,
            reposts=50,
            views_per_hour=2_000.0,
            conviction_score=4.0,
            direction="LONG",
            conviction_level=2,
        )
        assert result == "WATCH"

    # --- A2: returns UNSCORED when vph < vel_floor ---

    def test_unscored_when_vph_below_floor(self) -> None:
        """vph < vel_floor → UNSCORED regardless of other metrics."""
        clf = self._clf()
        result = clf.classify(
            views=5_000,
            reposts=25,
            views_per_hour=500.0,  # below 1000 floor
            conviction_score=7.0,
            direction="LONG",
            conviction_level=4,
        )
        assert result == "UNSCORED"

    def test_unscored_when_vph_none(self) -> None:
        """None vph (no view data) → UNSCORED."""
        clf = self._clf()
        result = clf.classify(
            views=None,
            reposts=50,
            views_per_hour=None,
            conviction_score=7.0,
            direction="LONG",
            conviction_level=4,
        )
        assert result == "UNSCORED"

    def test_unscored_when_vph_just_below_floor(self) -> None:
        """vph = 999.9 (one unit below floor) → UNSCORED."""
        clf = self._clf()
        result = clf.classify(
            views=5_000,
            reposts=25,
            views_per_hour=999.9,
            conviction_score=7.0,
            direction="LONG",
            conviction_level=4,
        )
        assert result == "UNSCORED"

    # --- A3: returns UNSCORED when conviction_level < 2 ---

    def test_unscored_when_conviction_level_1(self) -> None:
        clf = self._clf()
        result = clf.classify(
            views=100_000,
            reposts=1_000,
            views_per_hour=50_000.0,
            conviction_score=9.0,
            direction="LONG",
            conviction_level=1,
        )
        assert result == "UNSCORED"

    def test_unscored_when_conviction_level_0(self) -> None:
        clf = self._clf()
        result = clf.classify(
            views=100_000,
            reposts=1_000,
            views_per_hour=50_000.0,
            conviction_score=9.0,
            direction="LONG",
            conviction_level=0,
        )
        assert result == "UNSCORED"

    # --- A4: returns UNSCORED for NEUTRAL and AMBIGUOUS directions ---

    def test_unscored_for_neutral_direction(self) -> None:
        clf = self._clf()
        result = clf.classify(
            views=100_000,
            reposts=1_000,
            views_per_hour=50_000.0,
            conviction_score=9.0,
            direction="NEUTRAL",
            conviction_level=4,
        )
        assert result == "UNSCORED"

    def test_unscored_for_ambiguous_direction(self) -> None:
        clf = self._clf()
        result = clf.classify(
            views=100_000,
            reposts=1_000,
            views_per_hour=50_000.0,
            conviction_score=9.0,
            direction="AMBIGUOUS",
            conviction_level=4,
        )
        assert result == "UNSCORED"

    # --- A5: never returns ACT_NOW under ANY input combination ---

    @pytest.mark.parametrize(
        "views,reposts,vph,conviction_level,direction",
        [
            # High views + high reposts + high vph
            (500_000, 5_000, 100_000.0, 5, "LONG"),
            # At old ACT_NOW views threshold
            (50_000, 0, 5_000.0, 4, "LONG"),
            # At old ACT_NOW reposts threshold
            (0, 500, 5_000.0, 4, "LONG"),
            # Extreme velocity
            (1_000_000, 10_000, 500_000.0, 5, "SHORT"),
            # Max conviction, massive engagement
            (10_000_000, 100_000, 1_000_000.0, 5, "LONG"),
        ],
    )
    def test_never_returns_act_now(
        self,
        views: int,
        reposts: int,
        vph: float,
        conviction_level: int,
        direction: str,
    ) -> None:
        """No combination of inputs should produce ACT_NOW from classify()."""
        clf = self._clf()
        result = clf.classify(
            views=views,
            reposts=reposts,
            views_per_hour=vph,
            conviction_score=10.0,
            direction=direction,
            conviction_level=conviction_level,
        )
        assert result != "ACT_NOW", (
            f"classify() returned ACT_NOW for views={views}, reposts={reposts}, "
            f"vph={vph}, conviction_level={conviction_level}, direction={direction}. "
            f"ACT_NOW is assigned by the pipeline rank step, never by SignalClassifier."
        )


# ---------------------------------------------------------------------------
# Section B — ScoringEngine.score() conviction_score and factor scores
# ---------------------------------------------------------------------------


class TestScoringEngineNewBehavior:
    """ScoringEngine.score() tests scoped to the new WATCH/UNSCORED world.

    WATCH-eligible inputs: conviction_level >= 2, direction not NEUTRAL/AMBIGUOUS,
    and views_per_hour >= vel_floor (1000). With posted_at 8.5h before window,
    views=10_000 → vph ≈ 1176/hr → above floor.
    """

    # --- B1: WATCH-tier signal has populated scores ---

    def test_watch_signal_conviction_score_is_not_none(self) -> None:
        """A WATCH-eligible input produces a non-None, positive conviction_score."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        # views=10000, 8.5h → vph ≈ 1176 → above vel_floor 1000 → WATCH
        inp = _make_input(views=10_000, reposts=50, conviction_level=3)
        results = engine.score([inp])
        assert len(results) == 1
        sig = results[0]
        assert sig.tier == "WATCH"
        assert sig.conviction_score is not None
        assert sig.conviction_score > 0

    def test_watch_signal_score_virality_vel_is_not_none(self) -> None:
        """WATCH tier: score_virality_vel (F2b) is populated."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(views=10_000, reposts=50, conviction_level=3)
        results = engine.score([inp])
        sig = results[0]
        assert sig.tier == "WATCH"
        assert sig.score_virality_vel is not None

    def test_watch_signal_score_virality_abs_is_not_none(self) -> None:
        """WATCH tier: score_virality_abs (F2a) is recorded even though F2b drives conviction."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(views=10_000, reposts=50, conviction_level=3)
        results = engine.score([inp])
        sig = results[0]
        assert sig.tier == "WATCH"
        assert sig.score_virality_abs is not None

    def test_watch_signal_score_credibility_matches_formula(self) -> None:
        """F1 = credibility / 10; credibility=8.0 → score_credibility == 0.8."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(views=10_000, reposts=50, credibility=8.0, conviction_level=3)
        results = engine.score([inp])
        sig = results[0]
        assert sig.tier == "WATCH"
        assert sig.score_credibility == pytest.approx(0.8, abs=0.001)

    # --- B2: UNSCORED signal has zeroed scores ---

    def test_unscored_conviction_score_is_zero(self) -> None:
        """conviction_level < 2 → tier UNSCORED, conviction_score == 0.0 (not None)."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(conviction_level=1, views=100_000)
        results = engine.score([inp])
        sig = results[0]
        assert sig.tier == "UNSCORED"
        assert sig.conviction_score == 0.0

    def test_unscored_ambiguous_direction_conviction_score_is_zero(self) -> None:
        """AMBIGUOUS direction → UNSCORED, conviction_score == 0.0."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(direction="AMBIGUOUS", conviction_level=4, views=100_000)
        results = engine.score([inp])
        sig = results[0]
        assert sig.tier == "UNSCORED"
        assert sig.conviction_score == 0.0

    def test_unscored_low_vph_conviction_score_is_zero(self) -> None:
        """vph below vel_floor → UNSCORED via classifier. conviction_score == 0.0."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        # views=100 over 8.5h → vph ≈ 11.8 → below 1000 floor
        inp = _make_input(conviction_level=4, views=100)
        results = engine.score([inp])
        sig = results[0]
        assert sig.tier == "UNSCORED"
        assert sig.conviction_score == 0.0

    # --- B3: WATCH final_score > UNSCORED final_score for same inputs except vph ---

    def test_watch_final_score_exceeds_unscored(self) -> None:
        """Identical inputs except views: high-vph → WATCH; low-vph → UNSCORED.
        WATCH final_score must be strictly greater than UNSCORED final_score.
        """
        repo = _make_repo()
        engine = ScoringEngine(repo)

        inp_watch = _make_input(
            ticker="AAPL",
            views=10_000,  # vph ≈ 1176 → WATCH
            conviction_level=4,
            credibility=8.0,
            handle="poster_watch",
        )
        inp_unscored = _make_input(
            ticker="TSLA",
            views=100,  # vph ≈ 11.8 → UNSCORED
            conviction_level=4,
            credibility=8.0,
            handle="poster_unscored",
        )

        results = engine.score([inp_watch, inp_unscored])
        assert len(results) == 2

        watch_sig = next(s for s in results if s.tier == "WATCH")
        unscored_sig = next(s for s in results if s.tier == "UNSCORED")

        assert watch_sig.final_score > unscored_sig.final_score, (
            f"WATCH final_score ({watch_sig.final_score}) should exceed "
            f"UNSCORED final_score ({unscored_sig.final_score})"
        )

    # --- B4: scores are bounded in expected ranges (0–10) ---

    def test_watch_conviction_score_bounded_0_to_10(self) -> None:
        """conviction_score for WATCH must be in [0, 10]."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(views=10_000, credibility=10.0, conviction_level=5)
        results = engine.score([inp])
        sig = results[0]
        assert sig.tier == "WATCH"
        assert 0.0 <= sig.conviction_score <= 10.0, (
            f"conviction_score {sig.conviction_score} out of [0, 10]"
        )

    def test_watch_final_score_bounded_0_to_10(self) -> None:
        """final_score for WATCH must be in [0, 10]."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(views=10_000, credibility=10.0, conviction_level=5)
        results = engine.score([inp])
        sig = results[0]
        assert sig.tier == "WATCH"
        assert 0.0 <= sig.final_score <= 10.0, (
            f"final_score {sig.final_score} out of [0, 10]"
        )

    def test_score_credibility_bounded_0_to_1(self) -> None:
        """score_credibility (F1) is the normalised 0–1 sub-score (before weighting)."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        # credibility=10.0 → F1 = 1.0 (max); still in [0, 1]
        inp = _make_input(views=10_000, credibility=10.0, conviction_level=3)
        results = engine.score([inp])
        sig = results[0]
        assert sig.tier == "WATCH"
        assert 0.0 <= sig.score_credibility <= 1.0

    def test_score_virality_vel_bounded_0_to_1(self) -> None:
        """score_virality_vel (F2b) is a 0–1 normalised sub-score."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(views=10_000, conviction_level=3)
        results = engine.score([inp])
        sig = results[0]
        assert sig.tier == "WATCH"
        assert sig.score_virality_vel is not None
        assert 0.0 <= sig.score_virality_vel <= 1.0

    def test_score_virality_abs_bounded_0_to_1(self) -> None:
        """score_virality_abs (F2a) is a 0–1 normalised sub-score."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(views=10_000, conviction_level=3)
        results = engine.score([inp])
        sig = results[0]
        assert sig.tier == "WATCH"
        assert sig.score_virality_abs is not None
        assert 0.0 <= sig.score_virality_abs <= 1.0

    # --- B5: engine never returns ACT_NOW from score() directly ---

    @pytest.mark.parametrize(
        "views,reposts",
        [
            (50_000, 0),     # old ACT_NOW views threshold
            (0, 500),        # old ACT_NOW reposts threshold
            (1_000_000, 10_000),  # extreme engagement
        ],
    )
    def test_engine_never_produces_act_now_tier_directly(
        self, views: int, reposts: int
    ) -> None:
        """ScoringEngine.score() must never emit tier='ACT_NOW'.

        ACT_NOW is assigned by the pipeline STEP 8 rank-first split,
        not by the engine itself.
        """
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(views=views, reposts=reposts, conviction_level=4)
        results = engine.score([inp])
        for sig in results:
            assert sig.tier != "ACT_NOW", (
                f"ScoringEngine.score() returned tier='ACT_NOW' for views={views}, "
                f"reposts={reposts}. ACT_NOW must only be assigned by pipeline STEP 8."
            )
