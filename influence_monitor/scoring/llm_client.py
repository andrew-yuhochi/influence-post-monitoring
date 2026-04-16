"""LLM client interface and PostScore data model.

Defines the abstract LLMClient contract that all scoring implementations
must follow, and the PostScore Pydantic model that validates every LLM
response before it enters the pipeline.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Literal

from pydantic import BaseModel, Field


class PostScore(BaseModel):
    """Validated LLM scoring output for a single social media post.

    Fields match the JSON schema in config/prompts/scoring_prompt.txt.
    A zero-score sentinel (conviction_level=0, direction="AMBIGUOUS")
    is returned when the LLM response fails validation or the API errors.
    """

    tickers: list[str]
    direction: Literal["LONG", "SHORT", "NEUTRAL", "AMBIGUOUS"]
    conviction_level: int = Field(ge=0, le=5)
    key_claim: str
    argument_quality: Literal["HIGH", "MEDIUM", "LOW"]
    time_horizon: Literal["days", "weeks", "months", "years", "unspecified"]
    market_moving_potential: bool
    rationale: str

    @classmethod
    def zero_sentinel(cls) -> PostScore:
        """Return a zero-score sentinel for failed/invalid scoring."""
        return cls(
            tickers=[],
            direction="AMBIGUOUS",
            conviction_level=0,
            key_claim="",
            argument_quality="LOW",
            time_horizon="unspecified",
            market_moving_potential=False,
            rationale="Scoring failed — zero-score sentinel returned",
        )


class LLMClient(ABC):
    """Abstract contract for any LLM scoring implementation.

    Implementations must be stateless per-call: each ``score_post``
    invocation is independent. Connection setup and prompt loading
    happen at ``__init__`` time.
    """

    @abstractmethod
    def score_post(self, post_text: str, author_handle: str) -> PostScore:
        """Score a single post and return a validated PostScore.

        Must never raise — on any failure, return PostScore.zero_sentinel().
        """
        ...

    @abstractmethod
    def model_version(self) -> str:
        """Return the model identifier string for audit logging."""
        ...
