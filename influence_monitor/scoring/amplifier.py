"""AmplifierFetcher — retweeter fetch and F4 amplifier quality scoring (TASK-009).

For each ACT_NOW candidate, fetches up to 100 retweeters from the social media
source, persists them to the retweeters table, and computes a 0–10 amplifier
score based on follower-count tiers and monitored-account cross-reference.

Formula (TDD §2.3):
    high_tier     = retweeters with followers_count >= amplifier_high_follower_tier
    mid_tier      = retweeters with amplifier_mid_follower_tier <= followers_count < high_tier
    monitored     = retweeters whose external_id appears in accounts.external_id
    score         = min(10, monitored_count * 3 + high_tier * 1.5 + mid_tier * 0.5)
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from influence_monitor.ingestion.base import RawPost, SocialMediaSource

if TYPE_CHECKING:
    from influence_monitor.db.repository import SignalRepository

logger = logging.getLogger(__name__)

# Defaults used when scoring_config rows are absent
_DEFAULT_HIGH_FOLLOWER_TIER = 100_000
_DEFAULT_MID_FOLLOWER_TIER = 10_000


class AmplifierFetcher:
    """Fetch retweeters and compute the F4 amplifier quality score.

    Instantiate once per pipeline run (reads scoring_config thresholds at init).

    Usage::

        fetcher = AmplifierFetcher(repo)
        score = fetcher.fetch_and_score(post, source)
    """

    def __init__(self, repo: "SignalRepository", tenant_id: int = 1) -> None:
        self._repo = repo
        self._tenant_id = tenant_id
        cfg = repo.get_scoring_config(tenant_id=tenant_id)
        self._high_tier_threshold = int(
            cfg.get("amplifier_high_follower_tier", _DEFAULT_HIGH_FOLLOWER_TIER)
        )
        self._mid_tier_threshold = int(
            cfg.get("amplifier_mid_follower_tier", _DEFAULT_MID_FOLLOWER_TIER)
        )
        logger.info(
            "AmplifierFetcher init: high_tier>=%d followers, mid_tier>=%d followers",
            self._high_tier_threshold,
            self._mid_tier_threshold,
        )

    def fetch_and_score(
        self,
        post: RawPost,
        source: SocialMediaSource,
        post_db_id: int,
        tier: str = "ACT_NOW",
    ) -> float:
        """Fetch retweeters for *post*, persist them, and return the F4 score (0–10).

        Parameters
        ----------
        post:
            The RawPost whose retweeters should be fetched.
        source:
            The SocialMediaSource to call ``fetch_retweeters`` on.
        post_db_id:
            The integer primary-key of the post row in the ``posts`` table.
            Required for the ``retweeters.post_id`` FK.
        tier:
            Signal tier.  Only ACT_NOW posts are amplifier-scored; any other
            tier returns 0.0 immediately without calling fetch_retweeters.

        Returns
        -------
        float
            Amplifier score in [0.0, 10.0]. Returns 0.0 on fetch failure or
            when tier != "ACT_NOW".
        """
        if tier != "ACT_NOW":
            logger.debug(
                "AmplifierFetcher skipped: tier=%s is not ACT_NOW", tier
            )
            return 0.0

        try:
            retweeters = asyncio.run(source.fetch_retweeters(post.external_id, max_count=100))
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "fetch_retweeters failed for post_id=%s: %s — returning amplifier=0.0",
                post.external_id,
                exc,
            )
            return 0.0

        if not retweeters:
            logger.debug("No retweeters found for post_id=%s", post.external_id)
            return 0.0

        # Load monitored external_ids once (avoids per-retweeter DB round-trips)
        monitored_ids: set[str] = self._repo.get_account_external_ids(
            tenant_id=self._tenant_id
        )

        high_tier = 0
        mid_tier = 0
        monitored_count = 0

        for rt in retweeters:
            followers = rt.followers_count if rt.followers_count is not None else 0
            is_monitored = rt.external_id in monitored_ids

            if followers >= self._high_tier_threshold:
                high_tier += 1
            elif followers >= self._mid_tier_threshold:
                mid_tier += 1

            if is_monitored:
                monitored_count += 1

            # Persist every retweeter regardless of tier
            self._repo.insert_retweeter(
                post_id=post_db_id,
                retweeter_external_id=rt.external_id,
                retweeter_handle=rt.screen_name,
                followers_count=rt.followers_count,
                is_verified=rt.is_verified,
                is_monitored=is_monitored,
            )

        score = min(10.0, monitored_count * 3 + high_tier * 1.5 + mid_tier * 0.5)

        logger.info(
            "Amplifier for post %s: high=%d mid=%d monitored=%d → score=%.2f",
            post.external_id,
            high_tier,
            mid_tier,
            monitored_count,
            score,
        )
        return score
