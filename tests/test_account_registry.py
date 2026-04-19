"""Unit tests for AccountRegistry and TwitterTwikitSource ingestion.

All twikit network calls are mocked — no real credentials required.

Covers (per TASK-004 acceptance criteria):
  1. fetch_recent_posts returns filtered posts (posted_at >= since)
  2. Per-account transient failure → increment counter, no promotion
  3. Repeated failures → name search → rename path (handle updated, no promotion)
  4. Repeated failures → name search → no match → backup promotion
  5. All-backups-exhausted ERROR path
  6. Successful fetch resets consecutive_failures to 0
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from influence_monitor.ingestion.account_registry import (
    AccountRegistry,
    _is_credible_rename,
)
from influence_monitor.ingestion.base import RawPost, Retweeter


# ---------------------------------------------------------------------------
# Helpers for building mock objects
# ---------------------------------------------------------------------------

def _make_mock_user(
    screen_name: str,
    name: str = "",
    followers_count: int = 100_000,
    is_verified: bool = True,
) -> MagicMock:
    """Build a minimal mock twikit User object."""
    user = MagicMock()
    user.id = "999"
    user.screen_name = screen_name
    user.name = name
    user.followers_count = followers_count
    user.is_verified = is_verified
    user.is_blue_verified = is_verified
    return user


def _make_raw_post(handle: str, posted_at: datetime) -> RawPost:
    return RawPost(
        source_type="twitter_twikit",
        external_id="tweet_001",
        author_handle=handle,
        author_external_id="123",
        text="Sample post text $FNMA",
        posted_at=posted_at,
        fetched_at=datetime.now(timezone.utc),
    )


def _make_repo(
    primaries: list[dict],
    backups: list[dict] | None = None,
    config: dict | None = None,
) -> MagicMock:
    """Build a minimal SignalRepository mock."""
    repo = MagicMock()
    repo.get_scoring_config.return_value = config or {
        "max_consecutive_failures": 3,
        "retry_rest_minutes": 30,
    }

    def _get_by_status(status: str, tenant_id: int = 1) -> list[dict]:
        if status == "primary":
            return [dict(p) for p in primaries]
        if status == "backup":
            return list(backups or [])
        return []

    repo.get_accounts_by_status.side_effect = _get_by_status
    repo.update_account_failure.return_value = None
    repo.reset_account_failures.return_value = None
    repo.rename_account_handle.return_value = None
    repo.upsert_account.return_value = None
    return repo


def _make_source(
    search_results: list[Any] | None = None,
    fetch_posts_result: list[RawPost] | None = None,
    fetch_raises: Exception | None = None,
) -> MagicMock:
    """Build a minimal SocialMediaSource mock."""
    source = MagicMock()

    if fetch_raises:
        source.fetch_recent_posts = AsyncMock(side_effect=fetch_raises)
    else:
        source.fetch_recent_posts = AsyncMock(return_value=fetch_posts_result or [])

    source.search_user = AsyncMock(return_value=search_results or [])
    source.fetch_retweeters = AsyncMock(return_value=[])
    return source


# ---------------------------------------------------------------------------
# _is_credible_rename heuristic tests
# ---------------------------------------------------------------------------

class TestIsCredibleRename:
    def test_exact_match_verified_similar_followers(self):
        candidate = _make_mock_user(
            screen_name="BillAckman2",
            name="Bill Ackman",
            followers_count=1_400_000,
            is_verified=True,
        )
        assert _is_credible_rename(candidate, "Bill Ackman", 1_300_000) is True

    def test_name_mismatch_rejected(self):
        candidate = _make_mock_user(
            screen_name="SomeOtherGuy",
            name="William Ackman Jr.",  # different name
            followers_count=1_400_000,
            is_verified=True,
        )
        assert _is_credible_rename(candidate, "Bill Ackman", 1_300_000) is False

    def test_not_verified_rejected(self):
        candidate = _make_mock_user(
            screen_name="BillAckman2",
            name="Bill Ackman",
            followers_count=1_400_000,
            is_verified=False,
        )
        assert _is_credible_rename(candidate, "Bill Ackman", 1_300_000) is False

    def test_follower_count_too_different_rejected(self):
        # More than 50% divergence
        candidate = _make_mock_user(
            screen_name="BillAckman2",
            name="Bill Ackman",
            followers_count=200_000,  # far from 1.3M
            is_verified=True,
        )
        assert _is_credible_rename(candidate, "Bill Ackman", 1_300_000) is False

    def test_no_baseline_followers_skips_count_check(self):
        """When last_known_followers is None, follower count check is skipped."""
        candidate = _make_mock_user(
            screen_name="BillAckman2",
            name="Bill Ackman",
            followers_count=1,
            is_verified=True,
        )
        assert _is_credible_rename(candidate, "Bill Ackman", None) is True

    def test_case_insensitive_name_match(self):
        candidate = _make_mock_user(
            screen_name="BillAckman2",
            name="bill ackman",  # lowercase
            followers_count=1_400_000,
            is_verified=True,
        )
        assert _is_credible_rename(candidate, "Bill Ackman", 1_300_000) is True


# ---------------------------------------------------------------------------
# AccountRegistry unit tests
# ---------------------------------------------------------------------------

class TestAccountRegistry:

    def _make_primary(
        self,
        handle: str = "BillAckman",
        display_name: str = "Bill Ackman",
        consecutive_failures: int = 0,
        last_failure_at: str | None = None,
        follower_count_at_post: int | None = 1_400_000,
    ) -> dict:
        return {
            "id": 1,
            "handle": handle,
            "display_name": display_name,
            "consecutive_failures": consecutive_failures,
            "last_failure_at": last_failure_at,
            "follower_count_at_post": follower_count_at_post,
            "status": "primary",
            "angle": "Activist",
            "credibility_score": 9.0,
            "notes": "",
        }

    def _make_backup(self, handle: str = "AltaFoxCapital", backup_rank: int = 1) -> dict:
        return {
            "id": 101,
            "handle": handle,
            "display_name": "Connor Haley",
            "consecutive_failures": 0,
            "last_failure_at": None,
            "follower_count_at_post": 50_000,
            "status": "backup",
            "backup_rank": backup_rank,
            "angle": "Value",
            "credibility_score": 6.0,
            "notes": "",
        }

    # ------------------------------------------------------------------
    # Test 1: successful fetch resets counter to 0
    # ------------------------------------------------------------------

    def test_record_fetch_success_resets_counter(self):
        primary = self._make_primary(consecutive_failures=2)
        repo = _make_repo([primary])
        source = _make_source()
        registry = AccountRegistry(repo, source)

        registry.record_fetch_success(account_id=1)

        repo.reset_account_failures.assert_called_once_with(1)

    # ------------------------------------------------------------------
    # Test 2: fetch failure increments counter, no promotion
    # ------------------------------------------------------------------

    def test_record_fetch_failure_increments_counter(self):
        primary = self._make_primary(consecutive_failures=0)
        repo = _make_repo([primary])
        source = _make_source()
        registry = AccountRegistry(repo, source)

        registry.record_fetch_failure(account_id=1)

        repo.update_account_failure.assert_called_once_with(1)
        # No promotion: upsert_account not called
        repo.upsert_account.assert_not_called()

    # ------------------------------------------------------------------
    # Test 3: transient failures (below threshold) skip resolution sequence
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_validate_skips_accounts_below_threshold(self):
        primary = self._make_primary(consecutive_failures=2)  # below default max=3
        repo = _make_repo([primary])
        source = _make_source()
        registry = AccountRegistry(repo, source)

        await registry.validate_and_promote()

        # No resolution attempted — search_user never called
        source.search_user.assert_not_called()
        repo.upsert_account.assert_not_called()

    # ------------------------------------------------------------------
    # Test 4: failures at threshold + debounce elapsed → name search → rename
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_rename_path_when_credible_match_found(self):
        """Repeated failures → name search → credible rename → update handle, no promotion."""
        past_failure = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        primary = self._make_primary(
            handle="OldHandle",
            display_name="Bill Ackman",
            consecutive_failures=3,
            last_failure_at=past_failure,
        )

        # Reachability probe: search for "OldHandle" returns nothing (handle gone)
        # Name search for "Bill Ackman" returns credible match under new handle
        credible_user = _make_mock_user(
            screen_name="NewHandle",
            name="Bill Ackman",
            followers_count=1_400_000,
            is_verified=True,
        )

        call_count = {"n": 0}

        async def _mock_search(query: str):
            call_count["n"] += 1
            if query == "OldHandle":
                return []  # handle not reachable
            if query == "Bill Ackman":
                return [credible_user]
            return []

        source = _make_source()
        source.search_user = _mock_search

        repo = _make_repo([primary])
        registry = AccountRegistry(repo, source)

        await registry.validate_and_promote()

        # Handle was renamed to NewHandle
        repo.rename_account_handle.assert_called_once_with(1, "NewHandle")
        repo.reset_account_failures.assert_called_once_with(1)
        # No promotion (upsert not called with status='primary' on a different handle)
        # upsert_account may be called during marking inactive — ensure no backup promoted
        for call in repo.upsert_account.call_args_list:
            kwargs = call.kwargs if call.kwargs else {}
            args = call.args if call.args else ()
            # Ensure we didn't promote AltaFoxCapital to primary
            assert "AltaFoxCapital" not in str(call)

    # ------------------------------------------------------------------
    # Test 5: failures at threshold + no credible match → backup promotion
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_backup_promotion_when_no_credible_match(self):
        """Repeated failures → name search → no match → backup promoted."""
        past_failure = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        primary = self._make_primary(
            handle="InactiveHandle",
            display_name="Gone Investor",
            consecutive_failures=3,
            last_failure_at=past_failure,
        )
        backup = self._make_backup("AltaFoxCapital", backup_rank=1)

        # All searches return empty (no rename, no reachability)
        source = _make_source(search_results=[])
        repo = _make_repo([primary], backups=[backup])
        registry = AccountRegistry(repo, source)

        await registry.validate_and_promote()

        # Primary marked inactive
        upsert_calls = repo.upsert_account.call_args_list
        # Check inactive was set via upsert_account with handle=InactiveHandle
        found_inactive = False
        found_promotion = False
        for call in upsert_calls:
            kw = call.kwargs
            args = call.args
            handle_val = kw.get("handle", args[1] if len(args) > 1 else "")
            status_val = kw.get("status", "")
            if handle_val == "InactiveHandle" and status_val == "inactive":
                found_inactive = True
            if handle_val == "AltaFoxCapital" and status_val == "primary":
                found_promotion = True

        assert found_inactive, "Primary account was not marked inactive"
        assert found_promotion, "Backup account was not promoted to primary"

    # ------------------------------------------------------------------
    # Test 6: all backups exhausted → ERROR logged
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_all_backups_exhausted_logs_error(self, caplog):
        """When all backups exhausted after a primary goes inactive, log ERROR."""
        import logging

        past_failure = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        primary = self._make_primary(
            handle="InactiveHandle",
            display_name="Gone Investor",
            consecutive_failures=3,
            last_failure_at=past_failure,
        )

        # No backups available
        source = _make_source(search_results=[])
        repo = _make_repo([primary], backups=[])  # no backups
        registry = AccountRegistry(repo, source)

        with caplog.at_level(logging.ERROR, logger="influence_monitor.ingestion.account_registry"):
            await registry.validate_and_promote()

        assert any(
            "All backups exhausted" in r.message or "exhausted" in r.message
            for r in caplog.records
            if r.levelno >= logging.ERROR
        ), "Expected an ERROR log about exhausted backups"

    # ------------------------------------------------------------------
    # Test 7: debounce — skip resolution when within retry_rest_minutes window
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_debounce_skips_resolution_within_window(self):
        """Account with failures at threshold but recent failure → skips resolution."""
        # last_failure_at = just 5 minutes ago, retry_rest_minutes = 30
        recent_failure = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        primary = self._make_primary(
            handle="DebounceHandle",
            consecutive_failures=3,
            last_failure_at=recent_failure,
        )
        source = _make_source(search_results=[])
        repo = _make_repo([primary], config={"max_consecutive_failures": 3, "retry_rest_minutes": 30})
        registry = AccountRegistry(repo, source)

        await registry.validate_and_promote()

        # Within debounce window — no resolution triggered
        source.search_user.assert_not_called()
        repo.upsert_account.assert_not_called()


# ---------------------------------------------------------------------------
# TwitterTwikitSource fetch_recent_posts filter tests (mocked twikit)
# ---------------------------------------------------------------------------

class TestTwitterTwikitSourceFetch:
    """Test post filtering by posted_at >= since."""

    def _make_mock_tweet(self, tweet_id: str, text: str, created_at_str: str) -> MagicMock:
        tweet = MagicMock()
        tweet.id = tweet_id
        tweet.full_text = text
        tweet.created_at = created_at_str
        tweet.view_count = 1000
        tweet.retweet_count = 50
        tweet.reply_count = 10
        tweet.favorite_count = 200
        tweet.bookmark_count = 5
        tweet.hashtags = []
        tweet.lang = "en"
        tweet.quote = None
        tweet.in_reply_to = None
        tweet.media = None
        tweet.poll = None
        tweet.urls = []
        tweet._data = {"id": tweet_id, "text": text}
        tweet._legacy = {"entities": {}}
        return tweet

    @pytest.mark.asyncio
    async def test_fetch_filters_old_posts(self):
        """Only posts newer than `since` are returned."""
        from influence_monitor.ingestion.twitter_twikit import TwitterTwikitSource
        from influence_monitor.config import Settings

        settings = Settings()

        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=6)

        # Two tweets: one newer, one older
        new_tweet = self._make_mock_tweet(
            "111", "new post $FNMA",
            (now - timedelta(hours=1)).strftime("%a %b %d %H:%M:%S +0000 %Y"),
        )
        old_tweet = self._make_mock_tweet(
            "222", "old post $AAPL",
            (now - timedelta(hours=12)).strftime("%a %b %d %H:%M:%S +0000 %Y"),
        )

        mock_tweets = MagicMock()
        mock_tweets.__iter__ = MagicMock(return_value=iter([new_tweet, old_tweet]))

        mock_user = _make_mock_user("BillAckman", followers_count=1_400_000)

        with (
            patch.object(
                TwitterTwikitSource, "_ensure_authenticated", new_callable=AsyncMock
            ),
            patch.object(
                TwitterTwikitSource,
                "_client",
                create=True,
            ),
        ):
            source = TwitterTwikitSource(settings)
            source._authenticated = True
            source._client = MagicMock()
            source._client.get_user_by_screen_name = AsyncMock(return_value=mock_user)
            source._client.get_user_tweets = AsyncMock(return_value=mock_tweets)

            posts = await source.fetch_recent_posts("BillAckman", since, max_count=10)

        assert len(posts) == 1
        assert posts[0].external_id == "111"
        assert posts[0].source_type == "twitter_twikit"

    @pytest.mark.asyncio
    async def test_fetch_respects_max_count(self):
        """max_count is passed to get_user_tweets."""
        from influence_monitor.ingestion.twitter_twikit import TwitterTwikitSource
        from influence_monitor.config import Settings

        settings = Settings()
        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=6)

        mock_tweets = MagicMock()
        mock_tweets.__iter__ = MagicMock(return_value=iter([]))
        mock_user = _make_mock_user("BillAckman")

        source = TwitterTwikitSource(settings)
        source._authenticated = True
        source._client = MagicMock()
        source._client.get_user_by_screen_name = AsyncMock(return_value=mock_user)
        source._client.get_user_tweets = AsyncMock(return_value=mock_tweets)

        await source.fetch_recent_posts("BillAckman", since, max_count=5)

        source._client.get_user_tweets.assert_called_once_with(
            user_id="999",
            tweet_type="Tweets",
            count=5,
        )

    @pytest.mark.asyncio
    async def test_fetch_retweeters_returns_retweeter_objects(self):
        """fetch_retweeters maps twikit User objects to Retweeter dataclasses."""
        from influence_monitor.ingestion.twitter_twikit import TwitterTwikitSource
        from influence_monitor.config import Settings

        settings = Settings()

        rt_user = _make_mock_user("RetweeterHandle", followers_count=500_000, is_verified=True)

        source = TwitterTwikitSource(settings)
        source._authenticated = True
        source._client = MagicMock()
        source._client.get_retweeters = AsyncMock(return_value=[rt_user])

        retweeters = await source.fetch_retweeters("tweet_001", max_count=100)

        assert len(retweeters) == 1
        rt = retweeters[0]
        assert isinstance(rt, Retweeter)
        assert rt.screen_name == "RetweeterHandle"
        assert rt.followers_count == 500_000
        assert rt.is_verified is True

    @pytest.mark.asyncio
    async def test_fetch_retweeters_returns_empty_on_exception(self):
        """fetch_retweeters returns [] and logs WARNING on twikit error."""
        from influence_monitor.ingestion.twitter_twikit import TwitterTwikitSource
        from influence_monitor.config import Settings

        settings = Settings()

        source = TwitterTwikitSource(settings)
        source._authenticated = True
        source._client = MagicMock()
        source._client.get_retweeters = AsyncMock(side_effect=RuntimeError("API error"))

        retweeters = await source.fetch_retweeters("tweet_001")
        assert retweeters == []

    @pytest.mark.asyncio
    async def test_search_user_returns_results(self):
        """search_user returns twikit User objects from client.search_user."""
        from influence_monitor.ingestion.twitter_twikit import TwitterTwikitSource
        from influence_monitor.config import Settings

        settings = Settings()

        user = _make_mock_user("BillAckman", name="Bill Ackman")

        source = TwitterTwikitSource(settings)
        source._authenticated = True
        source._client = MagicMock()
        source._client.search_user = AsyncMock(return_value=[user])

        results = await source.search_user("Bill Ackman")
        assert len(results) == 1
        assert results[0].screen_name == "BillAckman"

    @pytest.mark.asyncio
    async def test_search_user_returns_empty_on_exception(self):
        """search_user returns [] on twikit error."""
        from influence_monitor.ingestion.twitter_twikit import TwitterTwikitSource
        from influence_monitor.config import Settings

        settings = Settings()

        source = TwitterTwikitSource(settings)
        source._authenticated = True
        source._client = MagicMock()
        source._client.search_user = AsyncMock(side_effect=RuntimeError("API error"))

        results = await source.search_user("Bill Ackman")
        assert results == []

    def test_source_type_returns_twitter_twikit(self):
        """source_type() returns the correct registry key."""
        from influence_monitor.ingestion.twitter_twikit import TwitterTwikitSource
        from influence_monitor.config import Settings

        settings = Settings()
        source = TwitterTwikitSource(settings)
        assert source.source_type() == "twitter_twikit"
