"""Twitter ingestion via twikit (cookie-based scraping).

Implements SocialMediaSource for fetching posts from monitored
investor accounts on X/Twitter.

WARNING: twikit violates X Terms of Service. Account suspension risk
exists. This is accepted for PoC personal use. Swap path to the
official API is TwitterOfficialIngestor (twitter_official.py).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from twikit import Client
from twikit.errors import (
    BadRequest,
    Forbidden,
    NotFound,
    TooManyRequests,
    Unauthorized,
)

from influence_monitor.config import Settings
from influence_monitor.ingestion.base import IngestorError, RawPost, SocialMediaSource

logger = logging.getLogger(__name__)


def _parse_created_at(created_at_str: str) -> datetime:
    """Parse Twitter's created_at format into a timezone-aware datetime."""
    # Twitter format: "Mon Apr 15 11:23:00 +0000 2026"
    return datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")


def _safe_int(value: Any) -> int | None:
    """Safely convert a value to int, returning None on failure."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _tweet_to_raw_payload(tweet: Any) -> dict:
    """Serialise the twikit Tweet object to a JSON-safe dict.

    Captures the internal _data dict which contains the full API
    response. This is the Burry deletion protection — once stored,
    the post text is preserved even if deleted from X.
    """
    try:
        return json.loads(json.dumps(tweet._data, default=str))
    except Exception:
        logger.warning("Failed to serialise tweet %s raw payload", getattr(tweet, "id", "?"))
        return {}


def _extract_media_type(tweet: Any) -> str | None:
    """Determine the media type from a tweet's media attachments."""
    media = getattr(tweet, "media", None)
    if not media:
        poll = getattr(tweet, "poll", None)
        if poll:
            return "poll"
        return None
    first = media[0] if isinstance(media, list) and media else None
    if first is None:
        return None
    media_type = getattr(first, "type", None)
    if media_type == "photo":
        return "image"
    if media_type in ("video", "animated_gif"):
        return "video"
    return media_type


def _extract_mentioned_users(tweet: Any) -> list[str]:
    """Extract @handles mentioned in the tweet."""
    try:
        entities = tweet._legacy.get("entities", {})
        user_mentions = entities.get("user_mentions", [])
        return [m["screen_name"] for m in user_mentions if "screen_name" in m]
    except Exception:
        return []


def _extract_url_links(tweet: Any) -> list[str]:
    """Extract expanded URLs from the tweet."""
    urls = getattr(tweet, "urls", None)
    if not urls:
        return []
    try:
        return [u.get("expanded_url") or u.get("url", "") for u in urls if isinstance(u, dict)]
    except Exception:
        return []


class TwitterIngestor(SocialMediaSource):
    """Fetches posts from monitored X/Twitter accounts via twikit.

    Authentication flow:
    1. If cookies file exists → load_cookies (no network login)
    2. Otherwise → login with credentials → save_cookies for next run

    Per-account fetch failures are logged and skipped. A pipeline-level
    IngestorError is raised only when fewer than
    settings.min_accounts_threshold accounts succeed.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._cookies_path = Path(settings.cookies_path)
        self._client = Client("en-US")
        self._authenticated = False

    async def _ensure_authenticated(self) -> None:
        """Authenticate once per session using cookies or credentials."""
        if self._authenticated:
            return

        if self._cookies_path.exists():
            logger.info("Loading Twitter cookies from %s", self._cookies_path)
            self._client.load_cookies(str(self._cookies_path))
        else:
            logger.info("No cookies found — logging in with credentials")
            await self._client.login(
                auth_info_1=self._settings.twitter_username,
                auth_info_2=self._settings.twitter_email,
                password=self._settings.twitter_password,
            )
            self._cookies_path.parent.mkdir(parents=True, exist_ok=True)
            self._client.save_cookies(str(self._cookies_path))
            logger.info("Saved Twitter cookies to %s", self._cookies_path)

        self._authenticated = True

    def source_type(self) -> str:
        return "twitter"

    async def fetch_recent_posts(
        self,
        author_handle: str,
        since: datetime,
        max_count: int = 20,
        max_pages: int = 1,
    ) -> list[RawPost]:
        """Fetch recent tweets from *author_handle* posted after *since*.

        Resolves handle → numeric user_id via get_user_by_screen_name(), then
        paginates through up to *max_pages* pages of *max_count* tweets each,
        stopping early once all tweets on a page predate *since*.
        """
        await self._ensure_authenticated()

        user = await self._client.get_user_by_screen_name(author_handle)
        user_id: str = user.id
        follower_count: int | None = getattr(user, "followers_count", None)
        following_count: int | None = getattr(user, "following_count", None)

        now = datetime.now(timezone.utc)
        posts: list[RawPost] = []
        seen_ids: set[str] = set()

        tweets = await self._client.get_user_tweets(
            user_id=user_id,
            tweet_type="Tweets",
            count=max_count,
        )

        for _page in range(max_pages):
            page_had_newer = False

            for tweet in tweets:
                posted_at = _parse_created_at(tweet.created_at)
                if posted_at <= since:
                    continue

                page_had_newer = True
                tweet_id = str(tweet.id)
                if tweet_id in seen_ids:
                    continue
                seen_ids.add(tweet_id)

                # Detect thread context
                in_reply_to = getattr(tweet, "in_reply_to", None)
                is_thread = in_reply_to is not None and str(getattr(in_reply_to, "id", "")) != ""
                thread_position: int | None = None
                if is_thread and hasattr(tweet, "thread"):
                    thread = getattr(tweet, "thread", None)
                    if thread and isinstance(thread, list):
                        for idx, t in enumerate(thread):
                            if getattr(t, "id", None) == tweet.id:
                                thread_position = idx + 1
                                break

                # Quote tweet
                quote = getattr(tweet, "quote", None)
                quote_tweet_id = str(quote.id) if quote else None

                raw_post = RawPost(
                    source_type="twitter",
                    external_id=tweet_id,
                    author_handle=author_handle,
                    author_external_id=user_id,
                    text=tweet.full_text,
                    posted_at=posted_at,
                    fetched_at=now,
                    view_count=_safe_int(tweet.view_count),
                    repost_count=_safe_int(tweet.retweet_count),
                    reply_count=_safe_int(tweet.reply_count),
                    like_count=_safe_int(tweet.favorite_count),
                    bookmark_count=_safe_int(tweet.bookmark_count),
                    quote_tweet_id=quote_tweet_id,
                    is_thread=is_thread,
                    thread_position=thread_position,
                    hashtags=tweet.hashtags or [],
                    mentioned_users=_extract_mentioned_users(tweet),
                    url_links=_extract_url_links(tweet),
                    media_type=_extract_media_type(tweet),
                    language=tweet.lang or "en",
                    follower_count_at_post=follower_count,
                    following_count_at_post=following_count,
                    raw_payload=_tweet_to_raw_payload(tweet),
                )
                posts.append(raw_post)

            # Stop paginating if this page had nothing newer than `since`
            if not page_had_newer or _page + 1 >= max_pages:
                break

            try:
                tweets = await tweets.next()
            except Exception:
                break

        logger.info(
            "Fetched %d posts from @%s (since %s)",
            len(posts), author_handle, since.isoformat(),
        )
        return posts

    async def fetch_all_accounts(
        self,
        handles: list[str],
        since: datetime,
        max_count: int = 20,
        max_pages: int = 1,
    ) -> tuple[list[RawPost], int, int]:
        """Fetch posts from multiple accounts with per-account error handling.

        Returns (all_posts, success_count, failure_count).
        Raises IngestorError if fewer than min_accounts_threshold succeed.
        """
        await self._ensure_authenticated()

        all_posts: list[RawPost] = []
        success_count = 0
        failure_count = 0

        for handle in handles:
            try:
                posts = await self.fetch_recent_posts(handle, since, max_count, max_pages)
                all_posts.extend(posts)
                success_count += 1
            except (BadRequest, Forbidden, NotFound, Unauthorized) as exc:
                logger.warning(
                    "Failed to fetch @%s: %s: %s", handle, type(exc).__name__, exc,
                )
                failure_count += 1
            except TooManyRequests as exc:
                logger.warning(
                    "Rate limited while fetching @%s: %s", handle, exc,
                )
                failure_count += 1
            except Exception as exc:
                logger.warning(
                    "Unexpected error fetching @%s: %s: %s",
                    handle, type(exc).__name__, exc,
                )
                failure_count += 1

        threshold = self._settings.min_accounts_threshold
        if success_count < threshold:
            raise IngestorError(
                f"Only {success_count}/{len(handles)} accounts fetched "
                f"(threshold: {threshold}). "
                f"Failures: {failure_count}"
            )

        logger.info(
            "Ingestion complete: %d posts from %d/%d accounts (%d failed)",
            len(all_posts), success_count, len(handles), failure_count,
        )
        return all_posts, success_count, failure_count
