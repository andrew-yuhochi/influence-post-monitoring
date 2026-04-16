"""Unit tests for ScoringEngine — composite score computation."""

from __future__ import annotations

import pytest

from influence_monitor.scoring.llm_client import PostScore
from influence_monitor.scoring.scoring_engine import ScoredSignal, ScoringEngine


def _make_post_score(
    direction: str = "LONG",
    conviction_level: int = 4,
    argument_quality: str = "HIGH",
    **kwargs,
) -> PostScore:
    """Helper to create a PostScore with sensible defaults."""
    defaults = dict(
        tickers=["FNMA"],
        direction=direction,
        conviction_level=conviction_level,
        key_claim="test claim",
        argument_quality=argument_quality,
        time_horizon="months",
        market_moving_potential=True,
        rationale="test rationale",
    )
    defaults.update(kwargs)
    return PostScore(**defaults)


# Default scoring context for a credible investor
_DEFAULT_CTX = dict(
    credibility_score=8.5,
    rolling_accuracy_30d=0.65,
    view_count=50_000,
    repost_count=500,
    max_engagement_30d=100_000.0,
)


class TestZeroOutGate:
    """conviction < 2 or NEUTRAL/AMBIGUOUS → composite_score = 0.0."""

    def test_neutral_direction(self) -> None:
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(direction="NEUTRAL", conviction_level=3),
            **_DEFAULT_CTX,
        )
        assert result.composite_score == 0.0

    def test_ambiguous_direction(self) -> None:
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(direction="AMBIGUOUS", conviction_level=4),
            **_DEFAULT_CTX,
        )
        assert result.composite_score == 0.0

    def test_low_conviction(self) -> None:
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(conviction_level=1),
            **_DEFAULT_CTX,
        )
        assert result.composite_score == 0.0

    def test_zero_conviction(self) -> None:
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(conviction_level=0, direction="AMBIGUOUS"),
            **_DEFAULT_CTX,
        )
        assert result.composite_score == 0.0

    def test_conviction_2_passes_gate(self) -> None:
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(conviction_level=2),
            **_DEFAULT_CTX,
        )
        assert result.composite_score > 0.0


class TestCredibilitySubScore:
    def test_credibility_normalized(self) -> None:
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(),
            credibility_score=10.0,
            rolling_accuracy_30d=0.5,
            view_count=50_000,
            repost_count=500,
            max_engagement_30d=100_000.0,
        )
        assert result.score_credibility == 1.0

    def test_low_credibility(self) -> None:
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(),
            credibility_score=2.0,
            rolling_accuracy_30d=0.5,
            view_count=50_000,
            repost_count=500,
            max_engagement_30d=100_000.0,
        )
        assert result.score_credibility == 0.2


class TestConvictionSubScore:
    def test_max_conviction(self) -> None:
        engine = ScoringEngine()
        result = engine.score(_make_post_score(conviction_level=5), **_DEFAULT_CTX)
        assert result.score_conviction == 1.0

    def test_mid_conviction(self) -> None:
        engine = ScoringEngine()
        result = engine.score(_make_post_score(conviction_level=3), **_DEFAULT_CTX)
        assert result.score_conviction == 0.6


class TestArgumentSubScore:
    @pytest.mark.parametrize("quality,expected", [
        ("HIGH", 1.0),
        ("MEDIUM", 0.6),
        ("LOW", 0.2),
    ])
    def test_argument_mapping(self, quality: str, expected: float) -> None:
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(argument_quality=quality), **_DEFAULT_CTX,
        )
        assert result.score_argument == expected


class TestEngagementSubScore:
    def test_engagement_formula(self) -> None:
        engine = ScoringEngine()
        # (50000 + 5*500) / 100000 = 52500/100000 = 0.525
        result = engine.score(
            _make_post_score(),
            credibility_score=5.0,
            rolling_accuracy_30d=0.5,
            view_count=50_000,
            repost_count=500,
            max_engagement_30d=100_000.0,
        )
        assert result.score_engagement == 0.525

    def test_engagement_clamped_to_1(self) -> None:
        engine = ScoringEngine()
        # Viral post: engagement > max → clamped to 1.0
        result = engine.score(
            _make_post_score(),
            credibility_score=5.0,
            rolling_accuracy_30d=0.5,
            view_count=200_000,
            repost_count=10_000,
            max_engagement_30d=100_000.0,
        )
        assert result.score_engagement == 1.0

    def test_null_view_count_uses_median(self) -> None:
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(),
            credibility_score=5.0,
            rolling_accuracy_30d=0.5,
            view_count=None,
            repost_count=None,
            max_engagement_30d=100_000.0,
            median_engagement=40_000.0,
        )
        assert result.score_engagement == 0.4

    def test_null_view_count_no_median_defaults_05(self) -> None:
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(),
            credibility_score=5.0,
            rolling_accuracy_30d=0.5,
            view_count=None,
            repost_count=None,
            max_engagement_30d=100_000.0,
        )
        assert result.score_engagement == 0.5

    def test_zero_max_engagement_defaults_05(self) -> None:
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(),
            credibility_score=5.0,
            rolling_accuracy_30d=0.5,
            view_count=50_000,
            repost_count=500,
            max_engagement_30d=0.0,
        )
        assert result.score_engagement == 0.5


class TestHistoricalSubScore:
    def test_historical_accuracy(self) -> None:
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(),
            credibility_score=5.0,
            rolling_accuracy_30d=0.75,
            view_count=50_000,
            repost_count=500,
            max_engagement_30d=100_000.0,
        )
        assert result.score_historical == 0.75

    def test_null_accuracy_defaults_05(self) -> None:
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(),
            credibility_score=5.0,
            rolling_accuracy_30d=None,
            view_count=50_000,
            repost_count=500,
            max_engagement_30d=100_000.0,
        )
        assert result.score_historical == 0.5


class TestCompositeScore:
    def test_composite_in_range(self) -> None:
        engine = ScoringEngine()
        result = engine.score(_make_post_score(), **_DEFAULT_CTX)
        assert 0.0 <= result.composite_score <= 10.0

    def test_max_possible_score(self) -> None:
        """All components at max → composite ≈ 10.0."""
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(conviction_level=5, argument_quality="HIGH"),
            credibility_score=10.0,
            rolling_accuracy_30d=1.0,
            view_count=100_000,
            repost_count=0,
            max_engagement_30d=100_000.0,
        )
        assert result.composite_score == 10.0

    def test_known_calculation(self) -> None:
        """Verify exact composite with known inputs.

        credibility: 8.5/10 = 0.85  × 0.30 = 0.255
        conviction:  4/5   = 0.80  × 0.25 = 0.200
        argument:    HIGH  = 1.00  × 0.20 = 0.200
        engagement:  52500/100000 = 0.525 × 0.15 = 0.07875
        historical:  0.65         × 0.10 = 0.065
        sum = 0.79875 × 10 = 7.9875
        """
        engine = ScoringEngine()
        result = engine.score(_make_post_score(), **_DEFAULT_CTX)
        assert abs(result.composite_score - 7.9875) < 0.01


class TestWeightsFromDB:
    def test_different_weights_change_score(self) -> None:
        """Verify that weights from DB are actually used."""
        default_engine = ScoringEngine()
        result_default = default_engine.score(_make_post_score(), **_DEFAULT_CTX)

        # Heavily weight conviction (the only component that changed)
        custom_weights = {
            "credibility": 0.05,
            "conviction": 0.70,
            "argument": 0.05,
            "engagement": 0.10,
            "historical": 0.10,
        }
        custom_engine = ScoringEngine(weights=custom_weights)
        result_custom = custom_engine.score(_make_post_score(), **_DEFAULT_CTX)

        assert result_default.composite_score != result_custom.composite_score

    def test_custom_weights_applied(self) -> None:
        """With 100% conviction weight, composite = conviction * 10."""
        engine = ScoringEngine(weights={
            "credibility": 0.0,
            "conviction": 1.0,
            "argument": 0.0,
            "engagement": 0.0,
            "historical": 0.0,
        })
        result = engine.score(
            _make_post_score(conviction_level=4),
            **_DEFAULT_CTX,
        )
        # conviction = 4/5 = 0.8; composite = 0.8 * 10 = 8.0
        assert result.composite_score == 8.0


class TestShortSignals:
    def test_short_scores_normally(self) -> None:
        """SHORT direction with high conviction should score > 0."""
        engine = ScoringEngine()
        result = engine.score(
            _make_post_score(direction="SHORT", conviction_level=5),
            **_DEFAULT_CTX,
        )
        assert result.composite_score > 0.0
