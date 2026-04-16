"""Composite score computation engine.

Combines five weighted components into a single [0.0, 10.0] composite score
for each (post, ticker) pair. Weights are loaded from the ``scoring_weights``
DB table so they can be tuned without redeployment.

This module is pure computation — it does **not** call Claude, twikit, or
any external service. It receives a ``PostScore`` and investor context
already available in the database.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from influence_monitor.scoring.llm_client import PostScore

logger = logging.getLogger(__name__)

_ARGUMENT_QUALITY_MAP: dict[str, float] = {
    "HIGH": 1.0,
    "MEDIUM": 0.6,
    "LOW": 0.2,
}

_DEFAULT_WEIGHTS: dict[str, float] = {
    "credibility": 0.30,
    "conviction": 0.25,
    "argument": 0.20,
    "engagement": 0.15,
    "historical": 0.10,
}


@dataclass
class ScoredSignal:
    """All sub-scores and the final composite for a single (post, ticker) pair."""

    score_credibility: float
    score_conviction: float
    score_argument: float
    score_engagement: float
    score_historical: float
    composite_score: float


class ScoringEngine:
    """Five-component composite score engine.

    Components and their default weights:

    =========== ======== ===============================================
    Component   Weight   Source
    =========== ======== ===============================================
    Credibility  30%     ``investor_profiles.credibility_score`` [1-10]
    Conviction   25%     ``PostScore.conviction_level`` [1-5] → [0, 1]
    Argument     20%     ``PostScore.argument_quality`` HIGH/MED/LOW
    Engagement   15%     ``(views + 5*reposts) / max_30d``, clamped [0,1]
    Historical   10%     ``investor_profiles.rolling_accuracy_30d``
    =========== ======== ===============================================

    Posts with ``conviction_level < 2`` or ``direction in NEUTRAL/AMBIGUOUS``
    receive ``composite_score = 0.0``.
    """

    def __init__(self, weights: dict[str, float] | None = None) -> None:
        self._weights = weights or dict(_DEFAULT_WEIGHTS)
        total = sum(self._weights.values())
        if abs(total - 1.0) > 0.01:
            logger.warning("Scoring weights sum to %.3f (expected 1.0)", total)

    @classmethod
    async def from_db(cls, repo) -> ScoringEngine:
        """Load weights from the scoring_weights DB table."""
        weights = await repo.get_scoring_weights()
        if not weights:
            logger.warning("No scoring weights in DB — using defaults")
            weights = dict(_DEFAULT_WEIGHTS)
        return cls(weights)

    def score(
        self,
        post_score: PostScore,
        credibility_score: float,
        rolling_accuracy_30d: float | None,
        view_count: int | None,
        repost_count: int | None,
        max_engagement_30d: float,
        median_engagement: float | None = None,
    ) -> ScoredSignal:
        """Compute the composite score for a (post, ticker) pair.

        Args:
            post_score: Validated LLM output.
            credibility_score: Investor's credibility [1-10].
            rolling_accuracy_30d: Investor's 30-day hit rate [0-1], or None.
            view_count: Post view count (nullable).
            repost_count: Post repost count (nullable).
            max_engagement_30d: Rolling max engagement over 30 days.
                If zero or unavailable, engagement sub-score defaults to 0.5.
            median_engagement: Median engagement for this investor (fallback
                when view_count is None).

        Returns:
            ScoredSignal with all sub-scores and the composite.
        """
        # --- Zero-out gate ---
        if post_score.conviction_level < 2 or post_score.direction in ("NEUTRAL", "AMBIGUOUS"):
            return ScoredSignal(
                score_credibility=0.0,
                score_conviction=0.0,
                score_argument=0.0,
                score_engagement=0.0,
                score_historical=0.0,
                composite_score=0.0,
            )

        # --- Sub-score: credibility [0, 1] ---
        score_credibility = _clamp(credibility_score / 10.0, 0.0, 1.0)

        # --- Sub-score: conviction [0, 1] ---
        score_conviction = _clamp(post_score.conviction_level / 5.0, 0.0, 1.0)

        # --- Sub-score: argument quality [0, 1] ---
        score_argument = _ARGUMENT_QUALITY_MAP.get(post_score.argument_quality, 0.2)

        # --- Sub-score: engagement [0, 1] ---
        score_engagement = self._compute_engagement(
            view_count, repost_count, max_engagement_30d, median_engagement,
        )

        # --- Sub-score: historical accuracy [0, 1] ---
        score_historical = _clamp(rolling_accuracy_30d, 0.0, 1.0) if rolling_accuracy_30d is not None else 0.5

        # --- Composite ---
        composite = (
            self._weights.get("credibility", 0.30) * score_credibility
            + self._weights.get("conviction", 0.25) * score_conviction
            + self._weights.get("argument", 0.20) * score_argument
            + self._weights.get("engagement", 0.15) * score_engagement
            + self._weights.get("historical", 0.10) * score_historical
        ) * 10.0

        composite = _clamp(composite, 0.0, 10.0)

        return ScoredSignal(
            score_credibility=round(score_credibility, 4),
            score_conviction=round(score_conviction, 4),
            score_argument=round(score_argument, 4),
            score_engagement=round(score_engagement, 4),
            score_historical=round(score_historical, 4),
            composite_score=round(composite, 4),
        )

    def _compute_engagement(
        self,
        view_count: int | None,
        repost_count: int | None,
        max_engagement_30d: float,
        median_engagement: float | None,
    ) -> float:
        """Normalize engagement to [0, 1].

        Formula: (view_count + 5 * repost_count) / max_observed_30d.
        When view_count is NULL: use median_engagement for that investor.
        When max_engagement_30d is 0 or unavailable: return 0.5 (neutral).
        """
        if view_count is None:
            if median_engagement is not None and max_engagement_30d > 0:
                return _clamp(median_engagement / max_engagement_30d, 0.0, 1.0)
            return 0.5

        raw = (view_count or 0) + 5 * (repost_count or 0)

        if max_engagement_30d <= 0:
            return 0.5

        return _clamp(raw / max_engagement_30d, 0.0, 1.0)


def _clamp(value: float | None, lo: float, hi: float) -> float:
    """Clamp a numeric value to [lo, hi]. None defaults to midpoint."""
    if value is None:
        return (lo + hi) / 2.0
    return max(lo, min(hi, value))
