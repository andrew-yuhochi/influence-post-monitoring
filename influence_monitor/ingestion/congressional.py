"""Congressional disclosure ingestion — STUB.

Future source for ingesting congressional stock trades via Quiver Quant API.
Implements SocialMediaSource so it plugs into the pipeline via SOURCE_REGISTRY.
"""

from __future__ import annotations

from datetime import datetime

from influence_monitor.ingestion.base import RawPost, SocialMediaSource


class CongressionalIngestor(SocialMediaSource):
    """Congressional trade disclosure ingestor — not yet implemented."""

    def __init__(self, *args, **kwargs) -> None:
        raise NotImplementedError(
            "CongressionalIngestor is a stub. Not implemented in PoC phase."
        )

    async def fetch_recent_posts(
        self, author_handle: str, since: datetime, max_count: int = 20,
    ) -> list[RawPost]:
        raise NotImplementedError

    def source_type(self) -> str:
        return "congressional"
