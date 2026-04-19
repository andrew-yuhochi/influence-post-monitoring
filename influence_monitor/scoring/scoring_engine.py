"""Five-factor conviction scoring engine — F1 credibility, F2a/b virality,
F3 consensus, F4 amplifier (TASK-009), F5 liquidity (TASK-009).

Weights and thresholds are loaded from the ``scoring_config`` DB table at
init time so they can be recalibrated without code changes.

This module is PURE — no external API calls, no network, no async.
ConflictResolver and SignalClassifier are also implemented here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from influence_monitor.ingestion.base import RawPost
from influence_monitor.scoring.llm_client import PostScore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

Tier = Literal["ACT_NOW", "WATCH", "UNSCORED"]


@dataclass
class ScoringInput:
    """All inputs needed to score a single (post, ticker) pair."""

    post_score: PostScore
    raw_post: RawPost
    account_credibility: float  # 0–10, from accounts.credibility_score
    posted_at: datetime
    collection_window_start: datetime
    # poster identity for ConflictResolver grouping
    account_handle: str = ""
    # F3 consensus: pre-computed by caller from the batch
    distinct_same_direction_posters: int = 1   # count of distinct handles on same ticker+direction
    total_distinct_posters_on_ticker: int = 1  # total distinct posters on this ticker (any direction)
    ticker: str = ""


@dataclass
class ScoredSignal:
    """New ScoredSignal — matches columns in the signals table for insert_signal()."""

    ticker: str
    direction: str
    conviction_level: int
    argument_quality: str
    time_horizon: str
    market_moving_potential: bool
    key_claim: str
    rationale: str
    extraction_confidence: str

    # F-scores
    score_credibility: float | None
    score_virality_abs: float | None
    score_virality_vel: float | None   # None for ACT_NOW
    score_consensus: float | None
    score_amplifier: float | None      # None until TASK-009
    liquidity_modifier: float | None   # None until TASK-009

    # Composite
    conviction_score: float
    final_score: float
    penalty_applied: float

    # Tier + conflict metadata
    tier: Tier
    direction_flip: bool
    conflict_group: str | None

    # Engagement snapshot (from RawPost)
    engagement_views: int | None
    engagement_reposts: int | None
    views_per_hour: float | None

    # Poster handle (for ConflictResolver grouping — not stored in signals table)
    account_handle: str = ""

    # LLM audit columns — caller fills these from LLMClient
    llm_model_version: str | None = None
    llm_raw_response: str | None = None
    llm_input_tokens: int | None = None
    llm_output_tokens: int | None = None


# ---------------------------------------------------------------------------
# Sub-score helpers
# ---------------------------------------------------------------------------

def _compute_f2a(views: int | None, reposts: int | None, threshold_views: float, threshold_reposts: float) -> float:
    """F2a — virality absolute: linear ramp to 1.0, capped.

    Uses whichever metric gives the higher score.
    """
    score_v = min((views or 0) / threshold_views, 1.0) if threshold_views > 0 else 0.0
    score_r = min((reposts or 0) / threshold_reposts, 1.0) if threshold_reposts > 0 else 0.0
    return max(score_v, score_r)


def _compute_f2b(views_per_hour: float | None, vel_floor: float) -> float | None:
    """F2b — virality velocity: views_per_hour / watch_velocity_floor.

    Only used for WATCH tier. Returns None if velocity data unavailable.
    Capped at 1.0.
    """
    if views_per_hour is None or vel_floor <= 0:
        return None
    return min(views_per_hour / vel_floor, 1.0)


def _compute_f3(distinct_same_dir: int, total_distinct: int) -> float:
    """F3 — directional consensus: fraction of posters on same direction.

    Returns a 0–1 value. Single poster on a ticker → 1/1 = 1.0.
    """
    if total_distinct <= 0:
        return 0.0
    return min(distinct_same_dir / total_distinct, 1.0)


def _compute_views_per_hour(views: int | None, posted_at: datetime, collection_window_start: datetime) -> float | None:
    """Derive views_per_hour from view count and time elapsed since posting."""
    if views is None:
        return None
    # Hours elapsed since post was published (minimum 0.5h to avoid div/0 on very fresh posts)
    elapsed_seconds = max((collection_window_start - posted_at).total_seconds(), 0)
    if elapsed_seconds <= 0:
        # Post is newer than the window start; treat as brand-new with 0.5h floor
        hours = 0.5
    else:
        hours = max(elapsed_seconds / 3600.0, 0.5)
    return views / hours


# ---------------------------------------------------------------------------
# Signal Classifier
# ---------------------------------------------------------------------------

class SignalClassifier:
    """Assigns ACT_NOW / WATCH / UNSCORED tier based on virality thresholds."""

    def __init__(self, config: dict[str, float]) -> None:
        self._views_threshold = config.get("virality_views_threshold", 50_000)
        self._reposts_threshold = config.get("virality_reposts_threshold", 500)
        self._vel_floor = config.get("watch_velocity_floor", 1_000)

    def classify(
        self,
        views: int | None,
        reposts: int | None,
        views_per_hour: float | None,
        conviction_score: float,
        direction: str,
        conviction_level: int,
    ) -> Tier:
        """Classify a single signal.

        UNSCORED gate fires first (low conviction / ambiguous direction).
        ACT_NOW: views >= threshold OR reposts >= threshold.
        WATCH: below threshold but views_per_hour >= vel_floor.
        UNSCORED: everything else.
        """
        if conviction_level < 2 or direction in ("NEUTRAL", "AMBIGUOUS"):
            return "UNSCORED"
        if (views or 0) >= self._views_threshold or (reposts or 0) >= self._reposts_threshold:
            return "ACT_NOW"
        if views_per_hour is not None and views_per_hour >= self._vel_floor:
            return "WATCH"
        return "UNSCORED"


# ---------------------------------------------------------------------------
# Conflict Resolver
# ---------------------------------------------------------------------------

@dataclass
class _ConflictState:
    """Internal state for resolving conflicts for a single poster on a single ticker."""
    inputs: list[ScoringInput] = field(default_factory=list)


class ConflictResolver:
    """Resolve same-poster repeats, direction flips, and multi-poster mixed directions.

    Rules (per TDD §3 / PRD §6.12):

    1. Same poster, same direction, same ticker → keep highest-virality post.
    2. Same poster, conflicting directions on same ticker → keep most recent,
       mark direction_flip=True, apply penalty.
    3. 3+ distinct posters on same ticker with mixed directions → emit one
       signal per direction group; set conflict_group='opposing_exists' on each.
    """

    def __init__(self, direction_flip_penalty: float = 0.0) -> None:
        self._penalty = direction_flip_penalty

    def resolve(self, inputs: list[ScoringInput]) -> list[tuple[ScoringInput, bool, float, str | None]]:
        """Resolve conflicts across a batch of ScoringInputs.

        Returns a list of (input, direction_flip, penalty_applied, conflict_group)
        tuples for the retained signals.
        """
        # Group by ticker → poster → [inputs]
        by_ticker_poster: dict[str, dict[str, list[ScoringInput]]] = {}
        for inp in inputs:
            ticker = inp.ticker.upper()
            handle = inp.account_handle.lower()
            by_ticker_poster.setdefault(ticker, {}).setdefault(handle, []).append(inp)

        # Resolve per poster per ticker
        # result: ticker → list of (input, direction_flip, penalty)
        resolved_by_ticker: dict[str, list[tuple[ScoringInput, bool, float]]] = {}
        for ticker, posters in by_ticker_poster.items():
            for handle, poster_inputs in posters.items():
                directions = {inp.post_score.direction for inp in poster_inputs}
                if len(directions) == 1:
                    # Same direction — keep highest virality
                    best = max(
                        poster_inputs,
                        key=lambda x: (x.raw_post.view_count or 0, x.posted_at.timestamp()),
                    )
                    resolved_by_ticker.setdefault(ticker, []).append((best, False, 0.0))
                else:
                    # Direction flip — keep most recent
                    most_recent = max(poster_inputs, key=lambda x: x.posted_at.timestamp())
                    resolved_by_ticker.setdefault(ticker, []).append(
                        (most_recent, True, self._penalty)
                    )

        # Detect multi-poster mixed direction (3+ distinct posters on same ticker with mixed directions)
        result: list[tuple[ScoringInput, bool, float, str | None]] = []
        for ticker, resolved in resolved_by_ticker.items():
            directions_in_batch = {inp.post_score.direction for inp, _, _ in resolved}
            distinct_posters = len(resolved)
            has_mixed = len(directions_in_batch) > 1 and distinct_posters >= 3

            for inp, flip, penalty in resolved:
                conflict_group: str | None = "opposing_exists" if has_mixed else None
                result.append((inp, flip, penalty, conflict_group))

        return result


# ---------------------------------------------------------------------------
# Scoring Engine
# ---------------------------------------------------------------------------

class ScoringEngine:
    """Five-factor conviction scoring engine.

    Instantiate once per pipeline run. Weights are loaded from the
    ``scoring_config`` DB table via ``repo.get_scoring_config(tenant_id=1)``.

    Usage::

        engine = ScoringEngine(repo)
        scored = engine.score(inputs)
    """

    def __init__(self, repo) -> None:
        cfg = repo.get_scoring_config(tenant_id=1)
        self._config = cfg
        self._w_credibility = cfg.get("weight_credibility", 0.25)
        self._w_virality_abs = cfg.get("weight_virality_abs", 0.35)
        self._w_virality_vel = cfg.get("weight_virality_vel", 0.15)
        self._w_consensus = cfg.get("weight_consensus", 0.25)
        self._w_amplifier = cfg.get("weight_amplifier", 0.20)
        self._views_threshold = cfg.get("virality_views_threshold", 50_000)
        self._reposts_threshold = cfg.get("virality_reposts_threshold", 500)
        self._vel_floor = cfg.get("watch_velocity_floor", 1_000)
        self._direction_flip_penalty = cfg.get("direction_flip_penalty", 0.0)
        self._classifier = SignalClassifier(cfg)
        self._conflict_resolver = ConflictResolver(self._direction_flip_penalty)
        logger.info(
            "ScoringEngine init: w_cred=%.2f w_vir=%.2f w_cons=%.2f flip_penalty=%.2f",
            self._w_credibility, self._w_virality_abs, self._w_consensus,
            self._direction_flip_penalty,
        )

    def score(self, inputs: list[ScoringInput]) -> list[ScoredSignal]:
        """Score a batch of ScoringInputs and return ScoredSignals.

        Steps:
          1. For each input: compute sub-scores (F1, F2a, F3).
          2. Determine views_per_hour for each.
          3. Run ConflictResolver on the batch.
          4. Classify each resolved signal (ACT_NOW/WATCH/UNSCORED).
          5. Compute F2b for WATCH signals.
          6. Build final ScoredSignal list.
        """
        if not inputs:
            return []

        # Pre-compute views_per_hour for every input
        vph_map: dict[int, float | None] = {
            id(inp): _compute_views_per_hour(
                inp.raw_post.view_count, inp.posted_at, inp.collection_window_start
            )
            for inp in inputs
        }

        # Resolve conflicts
        resolved = self._conflict_resolver.resolve(inputs)

        signals: list[ScoredSignal] = []
        for inp, direction_flip, penalty_applied, conflict_group in resolved:
            post_score = inp.post_score
            views = inp.raw_post.view_count
            reposts = inp.raw_post.repost_count
            vph = vph_map[id(inp)]

            # UNSCORED gate
            if post_score.conviction_level < 2 or post_score.direction in ("NEUTRAL", "AMBIGUOUS"):
                signals.append(ScoredSignal(
                    ticker=inp.ticker,
                    direction=post_score.direction,
                    conviction_level=post_score.conviction_level,
                    argument_quality=post_score.argument_quality,
                    time_horizon=post_score.time_horizon,
                    market_moving_potential=post_score.market_moving_potential,
                    key_claim=post_score.key_claim,
                    rationale=post_score.rationale,
                    extraction_confidence="LOW",
                    score_credibility=0.0,
                    score_virality_abs=0.0,
                    score_virality_vel=None,
                    score_consensus=0.0,
                    score_amplifier=None,
                    liquidity_modifier=None,
                    conviction_score=0.0,
                    final_score=0.0,
                    penalty_applied=0.0,
                    tier="UNSCORED",
                    direction_flip=direction_flip,
                    conflict_group=conflict_group,
                    engagement_views=views,
                    engagement_reposts=reposts,
                    views_per_hour=vph,
                    account_handle=inp.account_handle,
                ))
                continue

            # F1 — credibility (0–1, normalised from 0–10 seed)
            f1 = min(max(inp.account_credibility / 10.0, 0.0), 1.0)

            # F2a — virality absolute (0–1)
            f2a = _compute_f2a(views, reposts, self._views_threshold, self._reposts_threshold)

            # F3 — consensus (0–1)
            f3 = _compute_f3(
                inp.distinct_same_direction_posters,
                inp.total_distinct_posters_on_ticker,
            )

            # F4 — amplifier: None until TASK-009
            f4: float | None = None

            # F5 — liquidity: None until TASK-009
            f5: float | None = None

            # First pass tier determination (needed for F2b decision)
            tier = self._classifier.classify(
                views, reposts, vph,
                conviction_score=1.0,  # placeholder — we need tier to know if F2b applies
                direction=post_score.direction,
                conviction_level=post_score.conviction_level,
            )

            # Compute conviction score
            if tier == "WATCH":
                # Watch path uses F2b instead of F2a
                f2b = _compute_f2b(vph, self._vel_floor)
                conviction_score = (
                    self._w_credibility * f1
                    + self._w_virality_vel * (f2b or 0.0)
                    + self._w_consensus * f3
                ) * 10.0
                score_virality_vel = f2b
                score_virality_abs = f2a  # still record the raw value
            elif tier == "ACT_NOW":
                conviction_score = (
                    self._w_credibility * f1
                    + self._w_virality_abs * f2a
                    + self._w_consensus * f3
                    # F4 not included until TASK-009 wires it in
                ) * 10.0
                score_virality_vel = None  # NULL for ACT_NOW
                score_virality_abs = f2a
            else:
                # UNSCORED path (shouldn't reach here due to gate above, but defensive)
                conviction_score = 0.0
                score_virality_vel = None
                score_virality_abs = f2a

            conviction_score = max(0.0, min(conviction_score, 10.0))

            # Apply direction-flip penalty
            final_score = conviction_score - penalty_applied
            final_score = max(0.0, final_score)

            signals.append(ScoredSignal(
                ticker=inp.ticker,
                direction=post_score.direction,
                conviction_level=post_score.conviction_level,
                argument_quality=post_score.argument_quality,
                time_horizon=post_score.time_horizon,
                market_moving_potential=post_score.market_moving_potential,
                key_claim=post_score.key_claim,
                rationale=post_score.rationale,
                extraction_confidence="HIGH",
                score_credibility=round(f1, 4),
                score_virality_abs=round(score_virality_abs, 4),
                score_virality_vel=round(score_virality_vel, 4) if score_virality_vel is not None else None,
                score_consensus=round(f3, 4),
                score_amplifier=f4,
                liquidity_modifier=f5,
                conviction_score=round(conviction_score, 4),
                final_score=round(final_score, 4),
                penalty_applied=round(penalty_applied, 4),
                tier=tier,
                direction_flip=direction_flip,
                conflict_group=conflict_group,
                engagement_views=views,
                engagement_reposts=reposts,
                views_per_hour=round(vph, 2) if vph is not None else None,
                account_handle=inp.account_handle,
            ))

        return signals
