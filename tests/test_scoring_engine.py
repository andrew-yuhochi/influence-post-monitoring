"""Unit tests for the new F1–F5 ScoringEngine, ConflictResolver, and SignalClassifier.

Covers TASK-008 acceptance criteria:
- Single post scoring: all sub-scores computed, weights applied
- conviction_level < 2 → UNSCORED
- direction "AMBIGUOUS" → UNSCORED
- Same-poster repeat: highest virality retained
- Same-poster flip: direction_flip=True, penalty logic
- 3-poster mixed direction: conflict_group='opposing_exists'
- ACT_NOW threshold crossing (views >= threshold)
- WATCH threshold (below views, above vel floor)
- All weights DB-driven (mock get_scoring_config)
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from influence_monitor.ingestion.base import RawPost
from influence_monitor.scoring.llm_client import PostScore
from influence_monitor.scoring.scoring_engine import (
    ConflictResolver,
    ScoredSignal,
    ScoringEngine,
    ScoringInput,
    SignalClassifier,
    _compute_f2a,
    _compute_f2b,
    _compute_f3,
)


# ---------------------------------------------------------------------------
# Test configuration and seed values matching scoring_config_seed.json
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

_WINDOW_START = datetime(2026, 4, 21, 6, 30, 0, tzinfo=timezone.utc)
_POSTED_AT = datetime(2026, 4, 20, 22, 0, 0, tzinfo=timezone.utc)  # 8.5h before window


def _make_repo(config: dict[str, float] | None = None) -> MagicMock:
    repo = MagicMock()
    repo.get_scoring_config.return_value = config or dict(_DEFAULT_CONFIG)
    return repo


def _make_post_score(
    direction: str = "LONG",
    conviction_level: int = 4,
    argument_quality: str = "HIGH",
    tickers: list[str] | None = None,
) -> PostScore:
    return PostScore(
        tickers=tickers or ["AAPL"],
        direction=direction,
        conviction_level=conviction_level,
        key_claim="test claim",
        argument_quality=argument_quality,
        time_horizon="months",
        market_moving_potential=True,
        rationale="test rationale",
    )


def _make_raw_post(
    views: int | None = 60_000,
    reposts: int | None = 600,
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
    views: int | None = 60_000,
    reposts: int | None = 600,
    credibility: float = 8.0,
    handle: str = "poster_a",
    distinct_same: int = 1,
    total_distinct: int = 1,
    posted_at: datetime = _POSTED_AT,
) -> ScoringInput:
    return ScoringInput(
        post_score=_make_post_score(direction=direction, conviction_level=conviction_level),
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
# Sub-score unit tests
# ---------------------------------------------------------------------------


class TestComputeF2a:
    def test_at_threshold(self) -> None:
        assert _compute_f2a(50_000, 0, 50_000, 500) == 1.0

    def test_above_threshold_capped(self) -> None:
        assert _compute_f2a(100_000, 0, 50_000, 500) == 1.0

    def test_half_threshold(self) -> None:
        assert abs(_compute_f2a(25_000, 0, 50_000, 500) - 0.5) < 0.001

    def test_reposts_drives_score(self) -> None:
        # reposts at threshold: score = 1.0 even if views = 0
        assert _compute_f2a(0, 500, 50_000, 500) == 1.0

    def test_zero_views_zero_reposts(self) -> None:
        assert _compute_f2a(0, 0, 50_000, 500) == 0.0


class TestComputeF2b:
    def test_above_floor(self) -> None:
        # 2000 vph / 1000 floor = 2.0 → capped at 1.0
        assert _compute_f2b(2_000.0, 1_000.0) == 1.0

    def test_exactly_floor(self) -> None:
        assert _compute_f2b(1_000.0, 1_000.0) == 1.0

    def test_below_floor(self) -> None:
        assert abs(_compute_f2b(500.0, 1_000.0) - 0.5) < 0.001

    def test_none_views_per_hour(self) -> None:
        assert _compute_f2b(None, 1_000.0) is None


class TestComputeF3:
    def test_single_poster_same_dir(self) -> None:
        assert _compute_f3(1, 1) == 1.0

    def test_two_of_three_same_dir(self) -> None:
        assert abs(_compute_f3(2, 3) - 0.6667) < 0.001

    def test_zero_total(self) -> None:
        assert _compute_f3(0, 0) == 0.0


# ---------------------------------------------------------------------------
# SignalClassifier
# ---------------------------------------------------------------------------


class TestSignalClassifier:
    def _make_classifier(self, config: dict | None = None) -> SignalClassifier:
        return SignalClassifier(config or dict(_DEFAULT_CONFIG))

    def test_act_now_views(self) -> None:
        clf = self._make_classifier()
        # Classifier no longer returns ACT_NOW — that is assigned downstream by the pipeline.
        # High views + high vph → WATCH.
        assert clf.classify(50_000, 0, 5_000, 5.0, "LONG", 4) == "WATCH"

    def test_act_now_reposts(self) -> None:
        clf = self._make_classifier()
        # Classifier no longer returns ACT_NOW — high reposts + high vph → WATCH.
        assert clf.classify(0, 500, 5_000, 5.0, "LONG", 4) == "WATCH"

    def test_watch_velocity(self) -> None:
        clf = self._make_classifier()
        # Below view threshold, above vel floor
        assert clf.classify(10_000, 50, 1_500, 4.0, "LONG", 3) == "WATCH"

    def test_unscored_low_conviction(self) -> None:
        clf = self._make_classifier()
        assert clf.classify(60_000, 600, 5_000, 0.0, "LONG", 1) == "UNSCORED"

    def test_unscored_ambiguous_direction(self) -> None:
        clf = self._make_classifier()
        assert clf.classify(60_000, 600, 5_000, 5.0, "AMBIGUOUS", 4) == "UNSCORED"

    def test_unscored_neutral_direction(self) -> None:
        clf = self._make_classifier()
        assert clf.classify(5_000, 30, 200, 2.0, "NEUTRAL", 3) == "UNSCORED"

    def test_unscored_low_virality_low_velocity(self) -> None:
        clf = self._make_classifier()
        assert clf.classify(1_000, 10, 200, 2.0, "LONG", 3) == "UNSCORED"


# ---------------------------------------------------------------------------
# ScoringEngine — single post scoring
# ---------------------------------------------------------------------------


class TestScoringEngineSinglePost:
    def test_act_now_signal_all_scores_populated(self) -> None:
        repo = _make_repo()
        engine = ScoringEngine(repo)
        # 60k views + high vph → WATCH (ACT_NOW is assigned downstream by the pipeline)
        inp = _make_input(views=60_000, reposts=600, credibility=8.0)
        results = engine.score([inp])
        assert len(results) == 1
        sig = results[0]
        assert sig.tier == "WATCH"
        assert sig.score_credibility == pytest.approx(0.8, abs=0.001)
        assert sig.score_virality_abs is not None
        assert sig.score_consensus is not None
        assert sig.direction_flip is False
        assert sig.conflict_group is None
        assert sig.conviction_score > 0

    def test_conviction_score_formula(self) -> None:
        """Verify conviction_score uses WATCH path: (w_cred*F1 + w_vir_vel*F2b + w_cons*F3) * 10."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        # views=60000 → vph = 60000/8.5h ≈ 7058 → F2b = min(7058/1000, 1.0) = 1.0
        # credibility=8.0 → F1 = 0.8
        # distinct_same=1, total=1 → F3 = 1.0
        inp = _make_input(views=60_000, reposts=0, credibility=8.0, distinct_same=1, total_distinct=1)
        results = engine.score([inp])
        sig = results[0]
        # Expected: (0.25*0.8 + 0.15*1.0 + 0.25*1.0) * 10 = (0.20 + 0.15 + 0.25) * 10 = 6.0
        assert sig.conviction_score == pytest.approx(6.0, abs=0.01)

    def test_f3_partial_consensus(self) -> None:
        """F3 with 1 of 3 posters in same direction reduces score (WATCH path)."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(views=60_000, reposts=0, credibility=8.0, distinct_same=1, total_distinct=3)
        results = engine.score([inp])
        sig = results[0]
        # F3 = 1/3 ≈ 0.333
        # WATCH path: (w_cred*F1 + w_vir_vel*F2b + w_cons*F3) * 10
        # vph ≈ 7058 → F2b = 1.0
        expected = (0.25 * 0.8 + 0.15 * 1.0 + 0.25 * (1 / 3)) * 10
        assert sig.conviction_score == pytest.approx(expected, abs=0.01)

    def test_f4_amplifier_none_until_task009(self) -> None:
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(views=60_000, reposts=600)
        results = engine.score([inp])
        assert results[0].score_amplifier is None

    def test_f5_liquidity_none_until_task009(self) -> None:
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(views=60_000, reposts=600)
        results = engine.score([inp])
        assert results[0].liquidity_modifier is None

    def test_virality_vel_populated_for_watch(self) -> None:
        """F2b (virality_vel) must be non-None for WATCH tier signals."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(views=60_000, reposts=0)  # high vph → WATCH
        results = engine.score([inp])
        assert results[0].tier == "WATCH"
        assert results[0].score_virality_vel is not None

    def test_virality_vel_populated_for_watch(self) -> None:
        """F2b must be non-None for WATCH tier signals."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        # Below view threshold (10k < 50k), above vel floor (1500 > 1000)
        inp = _make_input(views=10_000, reposts=0)
        results = engine.score([inp])
        assert results[0].tier == "WATCH"
        assert results[0].score_virality_vel is not None

    def test_empty_input(self) -> None:
        repo = _make_repo()
        engine = ScoringEngine(repo)
        assert engine.score([]) == []


# ---------------------------------------------------------------------------
# UNSCORED gate
# ---------------------------------------------------------------------------


class TestUnscoredGate:
    def test_conviction_1_unscored(self) -> None:
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(conviction_level=1, views=100_000, reposts=1_000)
        results = engine.score([inp])
        sig = results[0]
        assert sig.tier == "UNSCORED"
        assert sig.conviction_score == 0.0
        assert sig.score_credibility == 0.0

    def test_conviction_0_unscored(self) -> None:
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(conviction_level=0, views=100_000)
        results = engine.score([inp])
        assert results[0].tier == "UNSCORED"

    def test_ambiguous_direction_unscored(self) -> None:
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(direction="AMBIGUOUS", conviction_level=4, views=100_000)
        results = engine.score([inp])
        sig = results[0]
        assert sig.tier == "UNSCORED"
        assert sig.conviction_score == 0.0
        assert sig.score_credibility == 0.0

    def test_neutral_direction_unscored(self) -> None:
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(direction="NEUTRAL", conviction_level=3, views=100_000)
        results = engine.score([inp])
        assert results[0].tier == "UNSCORED"

    def test_conviction_2_passes_gate(self) -> None:
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inp = _make_input(conviction_level=2, views=60_000)
        results = engine.score([inp])
        assert results[0].tier != "UNSCORED"
        assert results[0].conviction_score > 0


# ---------------------------------------------------------------------------
# Weights are DB-driven
# ---------------------------------------------------------------------------


class TestWeightsFromDB:
    def test_different_weights_change_score(self) -> None:
        cfg_heavy_cred = dict(_DEFAULT_CONFIG)
        cfg_heavy_cred["weight_credibility"] = 0.90
        cfg_heavy_cred["weight_virality_abs"] = 0.05
        cfg_heavy_cred["weight_consensus"] = 0.05

        repo_default = _make_repo()
        repo_custom = _make_repo(cfg_heavy_cred)

        engine_default = ScoringEngine(repo_default)
        engine_custom = ScoringEngine(repo_custom)

        inp = _make_input(views=60_000, credibility=1.0, distinct_same=1, total_distinct=1)
        score_default = engine_default.score([inp])[0].conviction_score
        score_custom = engine_custom.score([inp])[0].conviction_score

        assert score_default != score_custom

    def test_zero_credibility_weight(self) -> None:
        cfg = dict(_DEFAULT_CONFIG)
        cfg["weight_credibility"] = 0.0
        cfg["weight_virality_abs"] = 0.50
        cfg["weight_consensus"] = 0.50

        repo = _make_repo(cfg)
        engine = ScoringEngine(repo)
        # Credibility is irrelevant now — 100 or 1 should give same score
        inp_high = _make_input(views=60_000, credibility=10.0, distinct_same=1, total_distinct=1)
        inp_low = _make_input(views=60_000, credibility=1.0, distinct_same=1, total_distinct=1)
        score_high = engine.score([inp_high])[0].conviction_score
        score_low = engine.score([inp_low])[0].conviction_score
        assert score_high == pytest.approx(score_low, abs=0.001)


# ---------------------------------------------------------------------------
# ConflictResolver — same-poster repeat (highest virality retained)
# ---------------------------------------------------------------------------


class TestConflictResolverSamePosterRepeat:
    def test_same_direction_keeps_highest_virality(self) -> None:
        """Two posts from same poster LONG AAPL — highest views retained."""
        resolver = ConflictResolver(direction_flip_penalty=0.0)

        inp_low = _make_input(ticker="AAPL", handle="poster_a", views=20_000)
        inp_high = _make_input(ticker="AAPL", handle="poster_a", views=60_000)

        resolved = resolver.resolve([inp_low, inp_high])
        assert len(resolved) == 1
        retained_inp, flip, penalty, cg = resolved[0]
        assert retained_inp.raw_post.view_count == 60_000
        assert flip is False
        assert penalty == 0.0

    def test_same_direction_three_posts_keeps_highest(self) -> None:
        resolver = ConflictResolver()
        inputs = [
            _make_input(ticker="AAPL", handle="poster_a", views=10_000),
            _make_input(ticker="AAPL", handle="poster_a", views=80_000),
            _make_input(ticker="AAPL", handle="poster_a", views=40_000),
        ]
        resolved = resolver.resolve(inputs)
        assert len(resolved) == 1
        assert resolved[0][0].raw_post.view_count == 80_000


# ---------------------------------------------------------------------------
# ConflictResolver — same-poster direction flip
# ---------------------------------------------------------------------------


class TestConflictResolverDirectionFlip:
    def test_flip_tagged_direction_flip_true(self) -> None:
        """Same poster LONG then SHORT on same ticker → direction_flip=True."""
        resolver = ConflictResolver(direction_flip_penalty=0.0)

        inp_long = ScoringInput(
            post_score=_make_post_score(direction="LONG"),
            raw_post=_make_raw_post(views=30_000),
            account_credibility=8.0,
            posted_at=datetime(2026, 4, 20, 18, 0, tzinfo=timezone.utc),
            collection_window_start=_WINDOW_START,
            account_handle="poster_a",
            ticker="GME",
        )
        inp_short = ScoringInput(
            post_score=_make_post_score(direction="SHORT"),
            raw_post=_make_raw_post(views=50_000),
            account_credibility=8.0,
            posted_at=datetime(2026, 4, 20, 22, 0, tzinfo=timezone.utc),  # more recent
            collection_window_start=_WINDOW_START,
            account_handle="poster_a",
            ticker="GME",
        )

        resolved = resolver.resolve([inp_long, inp_short])
        assert len(resolved) == 1
        retained, flip, penalty, cg = resolved[0]
        assert flip is True
        assert penalty == 0.0  # default penalty = 0

    def test_flip_keeps_most_recent(self) -> None:
        """Most-recent post retained on direction flip."""
        resolver = ConflictResolver(direction_flip_penalty=0.0)

        inp_old = ScoringInput(
            post_score=_make_post_score(direction="LONG"),
            raw_post=_make_raw_post(views=80_000),
            account_credibility=8.0,
            posted_at=datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc),  # older
            collection_window_start=_WINDOW_START,
            account_handle="poster_a",
            ticker="GME",
        )
        inp_new = ScoringInput(
            post_score=_make_post_score(direction="SHORT"),
            raw_post=_make_raw_post(views=20_000),
            account_credibility=8.0,
            posted_at=datetime(2026, 4, 20, 22, 0, tzinfo=timezone.utc),  # newer
            collection_window_start=_WINDOW_START,
            account_handle="poster_a",
            ticker="GME",
        )

        resolved = resolver.resolve([inp_old, inp_new])
        assert len(resolved) == 1
        retained, flip, penalty, cg = resolved[0]
        # Most recent retained even though views are lower
        assert retained.posted_at == inp_new.posted_at
        assert flip is True

    def test_flip_penalty_zero_final_score_equals_conviction(self) -> None:
        """direction_flip_penalty=0.0 → final_score == conviction_score."""
        repo = _make_repo()  # penalty=0.0 in default config
        engine = ScoringEngine(repo)

        inp_long = ScoringInput(
            post_score=_make_post_score(direction="LONG", conviction_level=4),
            raw_post=_make_raw_post(views=30_000),
            account_credibility=8.0,
            posted_at=datetime(2026, 4, 20, 18, 0, tzinfo=timezone.utc),
            collection_window_start=_WINDOW_START,
            account_handle="poster_a",
            ticker="GME",
        )
        inp_short = ScoringInput(
            post_score=_make_post_score(direction="SHORT", conviction_level=4),
            raw_post=_make_raw_post(views=60_000),
            account_credibility=8.0,
            posted_at=datetime(2026, 4, 20, 22, 0, tzinfo=timezone.utc),
            collection_window_start=_WINDOW_START,
            account_handle="poster_a",
            ticker="GME",
        )

        results = engine.score([inp_long, inp_short])
        assert len(results) == 1
        sig = results[0]
        assert sig.direction_flip is True
        assert sig.penalty_applied == 0.0
        assert sig.final_score == pytest.approx(sig.conviction_score, abs=0.001)

    def test_flip_penalty_2_deducts_from_final_score(self) -> None:
        """direction_flip_penalty=2.0 → final_score = conviction_score - 2.0."""
        cfg = dict(_DEFAULT_CONFIG)
        cfg["direction_flip_penalty"] = 2.0
        repo = _make_repo(cfg)
        engine = ScoringEngine(repo)

        inp_long = ScoringInput(
            post_score=_make_post_score(direction="LONG", conviction_level=4),
            raw_post=_make_raw_post(views=30_000),
            account_credibility=8.0,
            posted_at=datetime(2026, 4, 20, 18, 0, tzinfo=timezone.utc),
            collection_window_start=_WINDOW_START,
            account_handle="poster_a",
            ticker="GME",
        )
        inp_short = ScoringInput(
            post_score=_make_post_score(direction="SHORT", conviction_level=4),
            raw_post=_make_raw_post(views=60_000),
            account_credibility=8.0,
            posted_at=datetime(2026, 4, 20, 22, 0, tzinfo=timezone.utc),
            collection_window_start=_WINDOW_START,
            account_handle="poster_a",
            ticker="GME",
        )

        results = engine.score([inp_long, inp_short])
        sig = results[0]
        assert sig.direction_flip is True
        assert sig.penalty_applied == pytest.approx(2.0, abs=0.001)
        assert sig.final_score == pytest.approx(sig.conviction_score - 2.0, abs=0.001)


# ---------------------------------------------------------------------------
# ConflictResolver — 3+ posters mixed direction
# ---------------------------------------------------------------------------


class TestConflictResolverMixedDirection:
    def _make_input_for(
        self,
        ticker: str,
        direction: str,
        handle: str,
        views: int = 60_000,
    ) -> ScoringInput:
        return ScoringInput(
            post_score=_make_post_score(direction=direction, conviction_level=4),
            raw_post=_make_raw_post(views=views, handle=handle),
            account_credibility=8.0,
            posted_at=_POSTED_AT,
            collection_window_start=_WINDOW_START,
            account_handle=handle,
            ticker=ticker,
        )

    def test_3_posters_mixed_direction_all_tagged(self) -> None:
        """3 distinct posters on TSLA with mixed directions → conflict_group='opposing_exists'."""
        resolver = ConflictResolver()
        inputs = [
            self._make_input_for("TSLA", "LONG", "poster_a"),
            self._make_input_for("TSLA", "LONG", "poster_b"),
            self._make_input_for("TSLA", "SHORT", "poster_c"),
        ]
        resolved = resolver.resolve(inputs)
        # 3 distinct posters, mixed direction
        conflict_groups = [cg for _, _, _, cg in resolved]
        assert all(cg == "opposing_exists" for cg in conflict_groups)

    def test_3_posters_same_direction_no_conflict_tag(self) -> None:
        """3 posters all LONG → no conflict tag."""
        resolver = ConflictResolver()
        inputs = [
            self._make_input_for("TSLA", "LONG", "poster_a"),
            self._make_input_for("TSLA", "LONG", "poster_b"),
            self._make_input_for("TSLA", "LONG", "poster_c"),
        ]
        resolved = resolver.resolve(inputs)
        conflict_groups = [cg for _, _, _, cg in resolved]
        assert all(cg is None for cg in conflict_groups)

    def test_2_posters_mixed_direction_no_conflict_tag(self) -> None:
        """2 posters mixed direction (not 3+) → no opposing_exists tag."""
        resolver = ConflictResolver()
        inputs = [
            self._make_input_for("TSLA", "LONG", "poster_a"),
            self._make_input_for("TSLA", "SHORT", "poster_b"),
        ]
        resolved = resolver.resolve(inputs)
        conflict_groups = [cg for _, _, _, cg in resolved]
        # Only 2 posters — threshold not met
        assert all(cg is None for cg in conflict_groups)

    def test_3_posters_mixed_one_long_two_short_tagged(self) -> None:
        resolver = ConflictResolver()
        inputs = [
            self._make_input_for("TSLA", "LONG", "poster_a"),
            self._make_input_for("TSLA", "SHORT", "poster_b"),
            self._make_input_for("TSLA", "SHORT", "poster_c"),
        ]
        resolved = resolver.resolve(inputs)
        # All 3 tagged since mixed directions exist with 3+ distinct posters
        assert all(cg == "opposing_exists" for _, _, _, cg in resolved)

    def test_full_engine_3_poster_mixed_produces_conflict_group(self) -> None:
        """End-to-end: ScoringEngine marks conflict_group='opposing_exists' on all signals."""
        repo = _make_repo()
        engine = ScoringEngine(repo)
        inputs = [
            ScoringInput(
                post_score=_make_post_score(direction="LONG", conviction_level=4),
                raw_post=_make_raw_post(views=60_000, handle="a"),
                account_credibility=8.0,
                posted_at=_POSTED_AT,
                collection_window_start=_WINDOW_START,
                account_handle="a",
                ticker="TSLA",
            ),
            ScoringInput(
                post_score=_make_post_score(direction="LONG", conviction_level=4),
                raw_post=_make_raw_post(views=55_000, handle="b"),
                account_credibility=7.0,
                posted_at=_POSTED_AT,
                collection_window_start=_WINDOW_START,
                account_handle="b",
                ticker="TSLA",
            ),
            ScoringInput(
                post_score=_make_post_score(direction="SHORT", conviction_level=4),
                raw_post=_make_raw_post(views=65_000, handle="c"),
                account_credibility=9.0,
                posted_at=_POSTED_AT,
                collection_window_start=_WINDOW_START,
                account_handle="c",
                ticker="TSLA",
            ),
        ]
        results = engine.score(inputs)
        assert len(results) == 3
        assert all(s.conflict_group == "opposing_exists" for s in results)


# ---------------------------------------------------------------------------
# Tier threshold edge cases
# ---------------------------------------------------------------------------


class TestTierThresholds:
    def test_exactly_at_views_threshold_is_act_now(self) -> None:
        repo = _make_repo()
        engine = ScoringEngine(repo)
        # 50k views / 8.5h ≈ 5882 vph → above vel floor → WATCH
        # (ACT_NOW is assigned downstream by the pipeline after sorting)
        inp = _make_input(views=50_000, reposts=0)
        results = engine.score([inp])
        assert results[0].tier == "WATCH"

    def test_one_below_views_threshold_watch_if_vel_sufficient(self) -> None:
        repo = _make_repo()
        engine = ScoringEngine(repo)
        # 49999 views in 8.5h posted window = ~5882 vph → above 1000 vel floor
        inp = _make_input(views=49_999, reposts=0)
        results = engine.score([inp])
        # VPH = 49999 / ~8.5h ≈ 5882 → WATCH
        assert results[0].tier == "WATCH"

    def test_exactly_at_reposts_threshold_is_act_now(self) -> None:
        repo = _make_repo()
        engine = ScoringEngine(repo)
        # 100 views / 8.5h ≈ 11.8 vph → below vel floor of 1000 → UNSCORED
        # Reposts alone no longer gate WATCH; velocity (vph) is the sole classifier signal.
        inp = _make_input(views=100, reposts=500)
        results = engine.score([inp])
        assert results[0].tier == "UNSCORED"
