"""Integration tests for ClaudeHaikuClient.

These tests hit the real Anthropic API and require ANTHROPIC_API_KEY in the
environment. Guarded by the `integration` mark — skipped by default in CI.

Run with: pytest -m integration tests/test_claude_client_integration.py -v
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from influence_monitor.config import Settings
from influence_monitor.scoring.claude_client import ClaudeHaikuClient
from influence_monitor.scoring.llm_client import PostScore

_FIXTURES_PATH = Path(__file__).parent / "fixtures" / "sample_signals.json"

# Five representative posts from the fixture file (indices 0–4)
_FIXTURE_POSTS = [
    {
        "post_text": "Fannie Mae is absurdly underpriced. This goes 10x when conservatorship ends.",
        "account_handle": "BillAckman",
    },
    {
        "post_text": "NFLX subscriber growth is decelerating fast. Guidance miss coming.",
        "account_handle": "michaeljburry",
    },
    {
        "post_text": "Apple services revenue acceleration is deeply underappreciated by the market.",
        "account_handle": "DanielSLoeb1",
    },
    {
        "post_text": "NOVA's residential solar economics improve materially as rates come down.",
        "account_handle": "GavinSBaker",
    },
    {
        "post_text": "RIVN is burning cash at a rate that is simply not sustainable.",
        "account_handle": "WallStCynic",
    },
]


@pytest.mark.integration
def test_score_five_fixture_posts_real_api() -> None:
    """Score 5 fixture posts against the real Claude Haiku API.

    Asserts:
    - All 5 return valid PostScore instances (not zero sentinels)
    - Zero parse errors across all 5 calls
    - All directions are valid Literal values
    - All conviction_level values are in [0, 5]
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        pytest.skip("ANTHROPIC_API_KEY not set — skipping integration test")

    settings = Settings(anthropic_api_key=api_key)  # type: ignore[call-arg]
    client = ClaudeHaikuClient(settings=settings, repo=None)

    parse_errors = 0
    results: list[PostScore] = []

    for post in _FIXTURE_POSTS:
        score = client.score_post(
            post_text=post["post_text"],
            author_handle=post["account_handle"],
        )
        results.append(score)
        # Zero sentinel indicates a parse/API failure
        if score.conviction_level == 0 and score.direction == "AMBIGUOUS" and not score.tickers:
            parse_errors += 1

    assert parse_errors == 0, (
        f"{parse_errors} out of 5 posts returned zero-sentinel (parse/API failure). "
        f"Results: {[r.model_dump() for r in results]}"
    )

    valid_directions = {"LONG", "SHORT", "NEUTRAL", "AMBIGUOUS"}
    for score in results:
        assert score.direction in valid_directions
        assert 0 <= score.conviction_level <= 5
        assert isinstance(score.tickers, list)
        assert isinstance(score.key_claim, str)
        assert isinstance(score.rationale, str)
