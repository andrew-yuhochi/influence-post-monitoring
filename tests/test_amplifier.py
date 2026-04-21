"""Unit tests for AmplifierFetcher (TASK-009).

Mocks source.fetch_retweeters and SignalRepository to keep tests pure.
"""

from __future__ import annotations

import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from influence_monitor.ingestion.base import RawPost, Retweeter
from influence_monitor.scoring.amplifier import AmplifierFetcher


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_post(external_id: str = "tweet_001") -> RawPost:
    now = datetime.utcnow()
    return RawPost(
        source_type="twitter",
        external_id=external_id,
        author_handle="BillAckman",
        author_external_id="user_123",
        text="FNMA is massively underpriced.",
        posted_at=now,
        fetched_at=now,
        view_count=100_000,
        repost_count=800,
    )


def _make_retweeter(
    external_id: str,
    followers_count: int | None,
    is_verified: bool = False,
) -> Retweeter:
    return Retweeter(
        external_id=external_id,
        screen_name=f"user_{external_id}",
        followers_count=followers_count,
        is_verified=is_verified,
    )


def _make_repo(
    monitored_ids: set[str] | None = None,
    scoring_config: dict | None = None,
) -> MagicMock:
    repo = MagicMock()
    repo.get_account_external_ids.return_value = monitored_ids or set()
    repo.get_scoring_config.return_value = scoring_config or {
        "amplifier_high_follower_tier": 100_000,
        "amplifier_mid_follower_tier": 10_000,
    }
    repo.insert_retweeter.return_value = 1
    return repo


def _make_source(retweeters: list[Retweeter]) -> MagicMock:
    source = MagicMock()
    async def _fake_fetch(*args, **kwargs):
        return retweeters
    source.fetch_retweeters = _fake_fetch
    return source


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_zero_retweeters_returns_zero() -> None:
    """0 monitored matches, 0 high/mid tier → score = 0.0."""
    repo = _make_repo()
    source = _make_source([])
    fetcher = AmplifierFetcher(repo)

    score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1)

    assert score == 0.0


def test_one_monitored_match_returns_positive() -> None:
    """1 monitored match → score = min(10, 1*3 + 0 + 0) = 3.0."""
    monitored_id = "monitored_user"
    repo = _make_repo(monitored_ids={monitored_id})
    source = _make_source([
        _make_retweeter(monitored_id, followers_count=500),  # low tier, but monitored
    ])
    fetcher = AmplifierFetcher(repo)

    score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1)

    # monitored_count=1, high=0, mid=0 → score = min(10, 1*3 + 0*1.5 + 0*0.5) = 3.0
    assert score > 0.0
    assert score == pytest.approx(3.0)


def test_high_and_mid_tier_retweeters_score_correct() -> None:
    """3 high-tier + 2 mid-tier retweeters → min(10, 3*1.5 + 2*0.5) = 5.5."""
    repo = _make_repo()
    retweeters = [
        _make_retweeter(f"high_{i}", followers_count=200_000) for i in range(3)
    ] + [
        _make_retweeter(f"mid_{i}", followers_count=50_000) for i in range(2)
    ]
    source = _make_source(retweeters)
    fetcher = AmplifierFetcher(repo)

    score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1)

    # high=3, mid=2, monitored=0 → score = min(10, 0*3 + 3*1.5 + 2*0.5) = min(10, 5.5) = 5.5
    assert score == pytest.approx(5.5)


def test_score_capped_at_ten() -> None:
    """4 monitored + 4 high-tier → min(10, 4*3 + 4*1.5) = min(10, 18) = 10.0."""
    monitored_ids = {f"mon_{i}" for i in range(4)}
    repo = _make_repo(monitored_ids=monitored_ids)
    retweeters = [
        _make_retweeter(f"mon_{i}", followers_count=5_000_000) for i in range(4)  # monitored + high
    ] + [
        _make_retweeter(f"high_{i}", followers_count=5_000_000) for i in range(4)  # high only
    ]
    source = _make_source(retweeters)
    fetcher = AmplifierFetcher(repo)

    score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1)

    # monitored=4, high=8 (all >=100k), mid=0 → score = min(10, 4*3 + 8*1.5) = min(10, 24) = 10.0
    assert score == pytest.approx(10.0)


def test_fetch_retweeters_exception_returns_zero_and_logs(caplog: pytest.LogCaptureFixture) -> None:
    """fetch_retweeters raising → returns 0.0, logs WARNING, does not re-raise."""
    repo = _make_repo()
    source = MagicMock()
    async def _fake_fetch_raises(*args, **kwargs):
        raise RuntimeError("API timeout")
    source.fetch_retweeters = _fake_fetch_raises
    fetcher = AmplifierFetcher(repo)

    with caplog.at_level(logging.WARNING, logger="influence_monitor.scoring.amplifier"):
        score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1)

    assert score == 0.0
    assert any("API timeout" in r.message for r in caplog.records)


def test_retweeters_persisted_to_repo() -> None:
    """Each retweeter must be passed to repo.insert_retweeter exactly once."""
    monitored_id = "mon_001"
    repo = _make_repo(monitored_ids={monitored_id})
    retweeters = [
        _make_retweeter(monitored_id, followers_count=150_000, is_verified=True),
        _make_retweeter("ord_001", followers_count=5_000),
    ]
    source = _make_source(retweeters)
    fetcher = AmplifierFetcher(repo)

    fetcher.fetch_and_score(_make_post(), source, post_db_id=42)

    assert repo.insert_retweeter.call_count == 2
    # Verify the monitored flag was set correctly
    calls = {call.kwargs["retweeter_external_id"]: call.kwargs for call in repo.insert_retweeter.call_args_list}
    assert calls[monitored_id]["is_monitored"] is True
    assert calls["ord_001"]["is_monitored"] is False


def test_mid_tier_threshold_boundary() -> None:
    """Retweeter exactly at mid-tier threshold counts as mid-tier, not low."""
    repo = _make_repo()
    source = _make_source([
        _make_retweeter("exact_mid", followers_count=10_000),  # == mid threshold
    ])
    fetcher = AmplifierFetcher(repo)

    score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1)

    # mid=1, high=0, monitored=0 → score = min(10, 0*3 + 0*1.5 + 1*0.5) = 0.5
    assert score == pytest.approx(0.5)


def test_high_tier_threshold_boundary() -> None:
    """Retweeter exactly at high-tier threshold counts as high-tier."""
    repo = _make_repo()
    source = _make_source([
        _make_retweeter("exact_high", followers_count=100_000),  # == high threshold
    ])
    fetcher = AmplifierFetcher(repo)

    score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1)

    # high=1, mid=0, monitored=0 → score = min(10, 0*3 + 1*1.5 + 0*0.5) = 1.5
    assert score == pytest.approx(1.5)


def test_three_monitored_matches() -> None:
    """W1: 3 monitored matches, 0 high, 0 mid → min(10, 3*3) = 9.0."""
    monitored_ids = {"mon_0", "mon_1", "mon_2"}
    repo = _make_repo(monitored_ids=monitored_ids)
    source = _make_source([
        _make_retweeter(mid, followers_count=500) for mid in monitored_ids  # low tier, monitored
    ])
    fetcher = AmplifierFetcher(repo)

    score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1)

    # monitored=3, high=0, mid=0 → score = min(10, 3*3 + 0 + 0) = 9.0
    assert score == pytest.approx(9.0)


# ---------------------------------------------------------------------------
# ACT_NOW gating (C2)
# ---------------------------------------------------------------------------

def test_skipped_for_watch_tier() -> None:
    """fetch_and_score returns 0.0 and does NOT call fetch_retweeters for WATCH tier."""
    repo = _make_repo()
    retweeters = [_make_retweeter("user_001", followers_count=500_000)]
    call_count = [0]

    source = MagicMock()
    async def _fake_fetch(*args, **kwargs):
        call_count[0] += 1
        return retweeters
    source.fetch_retweeters = _fake_fetch
    fetcher = AmplifierFetcher(repo)

    score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1, tier="WATCH")

    assert score == 0.0
    assert call_count[0] == 0, "fetch_retweeters must not be called for WATCH tier"


def test_skipped_for_unscored_tier() -> None:
    """fetch_and_score returns 0.0 and does NOT call fetch_retweeters for UNSCORED tier."""
    repo = _make_repo()
    retweeters = [_make_retweeter("user_002", followers_count=500_000)]
    call_count = [0]

    source = MagicMock()
    async def _fake_fetch(*args, **kwargs):
        call_count[0] += 1
        return retweeters
    source.fetch_retweeters = _fake_fetch
    fetcher = AmplifierFetcher(repo)

    score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1, tier="UNSCORED")

    assert score == 0.0
    assert call_count[0] == 0, "fetch_retweeters must not be called for UNSCORED tier"
