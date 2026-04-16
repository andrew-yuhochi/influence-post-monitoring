"""Base interfaces for social media ingestion.

Defines the SocialMediaSource ABC that all ingestors implement,
the RawPost dataclass for uniform post representation, and
the IngestorError exception for pipeline-level failures.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime


class IngestorError(Exception):
    """Raised when an ingestion run fails at the pipeline level.

    Typically: fewer than settings.min_accounts_threshold accounts
    were fetched successfully.
    """


@dataclass
class RawPost:
    """Uniform representation of a social media post from any source.

    All fields are captured at fetch time and treated as immutable.
    JSON-list fields (hashtags, mentioned_users, url_links) are stored
    as Python lists here and serialised to JSON at the DB layer.
    """

    source_type: str
    external_id: str
    author_handle: str
    author_external_id: str
    text: str
    posted_at: datetime
    fetched_at: datetime
    # Engagement metrics
    view_count: int | None = None
    repost_count: int | None = None
    reply_count: int | None = None
    like_count: int | None = None
    bookmark_count: int | None = None
    # Social context (ML features)
    quote_tweet_id: str | None = None
    is_thread: bool = False
    thread_position: int | None = None
    hashtags: list[str] = field(default_factory=list)
    mentioned_users: list[str] = field(default_factory=list)
    url_links: list[str] = field(default_factory=list)
    media_type: str | None = None
    language: str = "en"
    # Poster reach snapshot
    follower_count_at_post: int | None = None
    following_count_at_post: int | None = None
    # Full platform response for replay / deletion protection
    raw_payload: dict = field(default_factory=dict)


class SocialMediaSource(ABC):
    """Abstract contract for any post-ingestion source.

    Implementations must be registered in SOURCE_REGISTRY (registry.py).
    """

    @abstractmethod
    async def fetch_recent_posts(
        self,
        author_handle: str,
        since: datetime,
        max_count: int = 20,
    ) -> list[RawPost]:
        """Fetch recent posts from *author_handle* posted after *since*.

        Returns an empty list when the account has no new posts.
        Raises on transient errors (caller decides retry policy).
        """
        ...

    @abstractmethod
    def source_type(self) -> str:
        """Return the source identifier, e.g. ``'twitter'``."""
        ...
