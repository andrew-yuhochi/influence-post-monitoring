"""Claude Haiku scoring client.

Sends investor posts to Claude Haiku for structured signal extraction.
System prompt loaded from config/prompts/scoring_prompt.txt.
Every API call is logged to the api_usage table for cost monitoring.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path

import anthropic

from influence_monitor.config import Settings
from influence_monitor.db.repository import DatabaseRepository
from influence_monitor.scoring.llm_client import LLMClient, PostScore

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent.parent
_SCORING_PROMPT_PATH = _PROJECT_ROOT / "config" / "prompts" / "scoring_prompt.txt"

_MODEL = "claude-haiku-4-5-20251001"
_MAX_TOKENS = 1024
_RETRY_DELAY_SECONDS = 5


class ClaudeHaikuClient(LLMClient):
    """Claude Haiku scoring client with retry, validation, and usage logging.

    Auth: reads ANTHROPIC_API_KEY from settings (never hardcoded).
    System prompt: loaded once from config/prompts/scoring_prompt.txt.
    Validation: every response parsed through PostScore Pydantic model.
    Failure mode: returns PostScore.zero_sentinel() — never raises.
    """

    def __init__(self, settings: Settings, repo: DatabaseRepository | None = None) -> None:
        self._client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        self._system_prompt = _SCORING_PROMPT_PATH.read_text().strip()
        self._repo = repo
        logger.info("ClaudeHaikuClient initialised (model=%s)", _MODEL)

    def model_version(self) -> str:
        return _MODEL

    def score_post(self, post_text: str, author_handle: str) -> PostScore:
        """Score a post via Claude Haiku. Returns zero sentinel on any failure."""
        user_message = f"Post by @{author_handle}:\n\n{post_text}"

        # First attempt
        result = self._call_api(user_message)
        if result is not None:
            return result

        # Retry once after delay
        logger.info("Retrying Claude API call after %ds", _RETRY_DELAY_SECONDS)
        time.sleep(_RETRY_DELAY_SECONDS)
        result = self._call_api(user_message)
        if result is not None:
            return result

        return PostScore.zero_sentinel()

    def _call_api(self, user_message: str) -> PostScore | None:
        """Single API call attempt. Returns None on failure."""
        start = time.monotonic()
        input_tokens = 0
        output_tokens = 0
        status = "ok"
        error_message = None
        raw_response = ""

        try:
            response = self._client.messages.create(
                model=_MODEL,
                max_tokens=_MAX_TOKENS,
                system=self._system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )

            input_tokens = response.usage.input_tokens
            output_tokens = response.usage.output_tokens
            raw_response = response.content[0].text

            # Parse and validate
            score = PostScore.model_validate_json(raw_response)
            return score

        except anthropic.APIError as exc:
            status = "error"
            error_message = f"{type(exc).__name__}: {exc}"
            logger.warning("Claude API error: %s", error_message)
            return None

        except (json.JSONDecodeError, Exception) as exc:
            if isinstance(exc, json.JSONDecodeError):
                status = "validation_error"
            else:
                status = "error"
            error_message = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "Claude response validation failed: %s | raw: %s",
                error_message, raw_response[:500],
            )
            return None

        finally:
            latency_ms = int((time.monotonic() - start) * 1000)
            self._log_usage(input_tokens, output_tokens, latency_ms, status, error_message)

    def _log_usage(
        self,
        input_tokens: int,
        output_tokens: int,
        latency_ms: int,
        status: str,
        error_message: str | None,
    ) -> None:
        """Log API call to the api_usage table (fire-and-forget)."""
        if self._repo is None:
            return

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(
                    self._repo.log_api_usage(
                        provider="anthropic",
                        endpoint=_MODEL,
                        input_tokens=input_tokens,
                        output_tokens=output_tokens,
                        latency_ms=latency_ms,
                        status=status,
                        error_message=error_message,
                    )
                )
            else:
                loop.run_until_complete(
                    self._repo.log_api_usage(
                        provider="anthropic",
                        endpoint=_MODEL,
                        input_tokens=input_tokens,
                        output_tokens=output_tokens,
                        latency_ms=latency_ms,
                        status=status,
                        error_message=error_message,
                    )
                )
        except Exception as exc:
            logger.debug("Failed to log API usage: %s", exc)
