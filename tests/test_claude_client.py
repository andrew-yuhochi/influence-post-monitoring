"""Unit tests for ClaudeHaikuClient.

Covers: valid JSON response, markdown-fenced JSON, malformed JSON (zero_sentinel),
APIError retry success, APIError twice (zero_sentinel), and api_usage logging per call.
All tests mock anthropic.Anthropic — no real API calls in this module.
"""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch, call

import anthropic
import pytest

from influence_monitor.config import Settings
from influence_monitor.scoring.claude_client import ClaudeHaikuClient, MODEL
from influence_monitor.scoring.llm_client import PostScore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_SCORE_DICT = {
    "tickers": ["FNMA"],
    "direction": "LONG",
    "conviction_level": 4,
    "key_claim": "FNMA underpriced; conservatorship release catalyst",
    "argument_quality": "HIGH",
    "time_horizon": "months",
    "market_moving_potential": True,
    "rationale": "High-conviction specific claim with policy catalyst",
}

_VALID_JSON = json.dumps(_VALID_SCORE_DICT)
_FENCED_JSON = f"```json\n{_VALID_JSON}\n```"
_MALFORMED_JSON = "not json at all"


def _make_mock_response(text: str, input_tokens: int = 50, output_tokens: int = 120) -> MagicMock:
    """Build a mock anthropic Messages response object."""
    msg = MagicMock()
    msg.content = [MagicMock(text=text)]
    msg.usage.input_tokens = input_tokens
    msg.usage.output_tokens = output_tokens
    return msg


def _make_client(mock_anthropic_cls: MagicMock) -> tuple[ClaudeHaikuClient, MagicMock, MagicMock]:
    """Return (client, mock_messages, mock_repo)."""
    mock_messages = MagicMock()
    mock_anthropic_cls.return_value.messages = mock_messages

    settings = MagicMock(spec=Settings)
    settings.anthropic_api_key = "test-key"

    mock_repo = MagicMock()

    client = ClaudeHaikuClient(settings=settings, repo=mock_repo)
    return client, mock_messages, mock_repo


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@patch("influence_monitor.scoring.claude_client.anthropic.Anthropic")
def test_valid_json_response_parses_correctly(mock_anthropic_cls: MagicMock) -> None:
    """A well-formed JSON response should produce a valid PostScore."""
    client, mock_messages, mock_repo = _make_client(mock_anthropic_cls)
    mock_messages.create.return_value = _make_mock_response(_VALID_JSON)

    result = client.score_post("Fannie Mae is underpriced.", "BillAckman")

    assert isinstance(result, PostScore)
    assert result.tickers == ["FNMA"]
    assert result.direction == "LONG"
    assert result.conviction_level == 4
    assert result.argument_quality == "HIGH"
    assert result.time_horizon == "months"
    assert result.market_moving_potential is True
    # api_usage logged exactly once
    mock_repo.log_api_usage.assert_called_once()


@patch("influence_monitor.scoring.claude_client.anthropic.Anthropic")
def test_markdown_fenced_json_unwrapped_and_parsed(mock_anthropic_cls: MagicMock) -> None:
    """Markdown-fenced JSON (```json ... ```) should be stripped and parsed."""
    client, mock_messages, mock_repo = _make_client(mock_anthropic_cls)
    mock_messages.create.return_value = _make_mock_response(_FENCED_JSON)

    result = client.score_post("Fannie Mae is underpriced.", "BillAckman")

    assert isinstance(result, PostScore)
    assert result.tickers == ["FNMA"]
    mock_repo.log_api_usage.assert_called_once()


@patch("influence_monitor.scoring.claude_client.anthropic.Anthropic")
def test_malformed_json_returns_zero_sentinel_and_logs_warning(
    mock_anthropic_cls: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Malformed JSON should return zero_sentinel and emit a WARNING log."""
    client, mock_messages, mock_repo = _make_client(mock_anthropic_cls)
    mock_messages.create.return_value = _make_mock_response(_MALFORMED_JSON)

    with caplog.at_level(logging.WARNING, logger="influence_monitor.scoring.claude_client"):
        result = client.score_post("Some post text.", "SomeHandle")

    assert result.conviction_level == 0
    assert result.direction == "AMBIGUOUS"
    assert result.tickers == []
    # WARNING must be present
    assert any("parse" in r.message.lower() or "validation" in r.message.lower()
               for r in caplog.records if r.levelno == logging.WARNING)
    # api_usage logged twice: initial attempt + retry (both fail on parse error)
    assert mock_repo.log_api_usage.call_count == 2


@patch("influence_monitor.scoring.claude_client.anthropic.Anthropic")
def test_api_error_retries_and_succeeds_on_second_attempt(mock_anthropic_cls: MagicMock) -> None:
    """APIError on first call should retry and succeed on the second attempt."""
    client, mock_messages, mock_repo = _make_client(mock_anthropic_cls)

    api_error = anthropic.APIStatusError(
        message="rate limit",
        response=MagicMock(status_code=429),
        body={},
    )
    mock_messages.create.side_effect = [
        api_error,
        _make_mock_response(_VALID_JSON),
    ]

    with patch("influence_monitor.scoring.claude_client.time.sleep") as mock_sleep:
        result = client.score_post("Some post text.", "SomeHandle")

    assert isinstance(result, PostScore)
    assert result.tickers == ["FNMA"]
    mock_sleep.assert_called_once_with(5)
    # api_usage logged once per _call_api invocation = 2 times total
    assert mock_repo.log_api_usage.call_count == 2


@patch("influence_monitor.scoring.claude_client.anthropic.Anthropic")
def test_api_error_twice_returns_zero_sentinel(mock_anthropic_cls: MagicMock) -> None:
    """Two consecutive APIErrors should return zero_sentinel."""
    client, mock_messages, mock_repo = _make_client(mock_anthropic_cls)

    api_error = anthropic.APIStatusError(
        message="service unavailable",
        response=MagicMock(status_code=503),
        body={},
    )
    mock_messages.create.side_effect = [api_error, api_error]

    with patch("influence_monitor.scoring.claude_client.time.sleep"):
        result = client.score_post("Some post text.", "SomeHandle")

    assert result.conviction_level == 0
    assert result.direction == "AMBIGUOUS"
    # api_usage logged twice (once per _call_api invocation)
    assert mock_repo.log_api_usage.call_count == 2


@patch("influence_monitor.scoring.claude_client.anthropic.Anthropic")
def test_log_api_usage_called_exactly_once_on_success(mock_anthropic_cls: MagicMock) -> None:
    """On a successful call, repo.log_api_usage must be called exactly once."""
    client, mock_messages, mock_repo = _make_client(mock_anthropic_cls)
    mock_messages.create.return_value = _make_mock_response(_VALID_JSON)

    client.score_post("Some text", "handle")

    mock_repo.log_api_usage.assert_called_once()
    kwargs = mock_repo.log_api_usage.call_args.kwargs
    assert kwargs["provider"] == "anthropic"
    assert kwargs["endpoint"] == MODEL
    assert kwargs["status"] == "ok"
    assert kwargs["error_message"] is None
    assert isinstance(kwargs["input_tokens"], int)
    assert isinstance(kwargs["output_tokens"], int)
    assert isinstance(kwargs["latency_ms"], int)


@patch("influence_monitor.scoring.claude_client.anthropic.Anthropic")
def test_model_version_returns_constant(mock_anthropic_cls: MagicMock) -> None:
    """model_version() must return the MODEL constant."""
    client, _, _ = _make_client(mock_anthropic_cls)
    assert client.model_version() == MODEL
