"""Substack newsletter ingestion — STUB.

Future source for ingesting investor newsletters (e.g., Howard Marks memos).
Implements SocialMediaSource so it plugs into the pipeline via SOURCE_REGISTRY.
"""

from __future__ import annotations

from datetime import datetime

from influence_monitor.ingestion.base import RawPost, Retweeter, SocialMediaSource


class SubstackIngestor(SocialMediaSource):
    """Substack newsletter ingestor — not yet implemented."""

    def __init__(self, *args, **kwargs) -> None:
        raise NotImplementedError(
            "SubstackIngestor is a stub. Not implemented in PoC phase."
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
        return "substack"
