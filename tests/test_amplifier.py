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
    source.fetch_retweeters.return_value = retweeters
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
    """1 monitored match → score > 0."""
    monitored_id = "monitored_user"
    repo = _make_repo(monitored_ids={monitored_id})
    source = _make_source([
        _make_retweeter(monitored_id, followers_count=500),  # low tier, but monitored
    ])
    fetcher = AmplifierFetcher(repo)

    score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1)

    # monitored_count=1, high=0, mid=0 → raw = 5 → score = 0.5
    assert score > 0.0
    assert score == pytest.approx(0.5)


def test_high_and_mid_tier_retweeters_score_correct() -> None:
    """3 high-tier + 2 mid-tier retweeters → expected formula output (raw / 10)."""
    repo = _make_repo()
    retweeters = [
        _make_retweeter(f"high_{i}", followers_count=200_000) for i in range(3)
    ] + [
        _make_retweeter(f"mid_{i}", followers_count=50_000) for i in range(2)
    ]
    source = _make_source(retweeters)
    fetcher = AmplifierFetcher(repo)

    score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1)

    # high=3, mid=2, monitored=0 → raw = 3*3 + 2*1 = 11 → score = 11/10 = 1.1
    assert score == pytest.approx(1.1)


def test_score_capped_at_ten() -> None:
    """Many high-tier retweeters — score must not exceed 10.0."""
    repo = _make_repo()
    retweeters = [
        _make_retweeter(f"big_{i}", followers_count=5_000_000) for i in range(50)
    ]
    source = _make_source(retweeters)
    fetcher = AmplifierFetcher(repo)

    score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1)

    assert score == pytest.approx(10.0)


def test_fetch_retweeters_exception_returns_zero_and_logs(caplog: pytest.LogCaptureFixture) -> None:
    """fetch_retweeters raising → returns 0.0, logs WARNING, does not re-raise."""
    repo = _make_repo()
    source = MagicMock()
    source.fetch_retweeters.side_effect = RuntimeError("API timeout")
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

    # mid=1, high=0, monitored=0 → raw=1 → score=0.1
    assert score == pytest.approx(0.1)


def test_high_tier_threshold_boundary() -> None:
    """Retweeter exactly at high-tier threshold counts as high-tier."""
    repo = _make_repo()
    source = _make_source([
        _make_retweeter("exact_high", followers_count=100_000),  # == high threshold
    ])
    fetcher = AmplifierFetcher(repo)

    score = fetcher.fetch_and_score(_make_post(), source, post_db_id=1)

    # high=1, mid=0, monitored=0 → raw=3 → score=0.3
    assert score == pytest.approx(0.3)
