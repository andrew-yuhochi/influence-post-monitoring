"""Twitter ingestion via official X API v2 (tweepy) — STUB.

This is the swap-path for TwitterTwikitSource (twikit). When the official
API is needed, implement this class and set SOCIAL_SOURCE=twitter_official.

Cost: $100/month (Basic tier). Volume: ~510 posts/month within 10,000 cap.
"""

from __future__ import annotations

from datetime import datetime

from influence_monitor.ingestion.base import RawPost, Retweeter, SocialMediaSource


class TwitterOfficialIngestor(SocialMediaSource):
    """Official X API v2 ingestor via tweepy — not yet implemented."""

    def __init__(self, *args, **kwargs) -> None:
        raise NotImplementedError(
            "TwitterOfficialIngestor is a stub. "
            "Set SOCIAL_SOURCE=twitter_twikit or implement this class."
        )

    async def fetch_recent_posts(
        self, author_handle: str, since: datetime, max_count: int = 20, max_pages: int = 1,
    ) -> list[RawPost]:
        raise NotImplementedError

    async def fetch_retweeters(
        self, post_external_id: str, max_count: int = 100,
    ) -> list[Retweeter]:
        raise NotImplementedError

    async def search_user(self, display_name: str) -> list[object]:
        raise NotImplementedError

    def source_type(self) -> str:
        return "twitter_official"
