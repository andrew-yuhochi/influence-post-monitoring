"""Corroboration detector — amplifies signals when multiple investors agree.

Corroboration is a **qualitative shift**, not just a score bump.  When 2+
distinct investors post the same direction on the same ticker on the same
day, every signal in that group receives the ``CORROBORATED`` tag and a
configurable score multiplier (default 1.5×).

The detector works on a list of ``Signal`` dataclass instances and sets
three fields on each:

- ``corroboration_count`` — number of *distinct* investors in the group
- ``corroboration_bonus`` — multiplier applied (1.0 if uncorroborated)
- ``final_score`` — ``composite_score * corroboration_bonus``
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    """A scored (post, ticker) pair ready for corroboration and ranking.

    Created by the pipeline after LLM scoring and composite computation.
    The corroboration detector fills ``corroboration_count``,
    ``corroboration_bonus``, and ``final_score``.
    """

    signal_id: int | None
    post_id: int
    investor_id: int
    ticker: str
    direction: str
    signal_date: date
    composite_score: float
    # Corroboration — set by CorroborationDetector.detect()
    corroboration_count: int = 1
    corroboration_bonus: float = 1.0
    final_score: float = 0.0
    # Optional display context
    investor_name: str = ""
    extraction_confidence: str = ""


class CorroborationDetector:
    """Detect and tag corroborated signals.

    Signals are corroborated when **2+ distinct investors** post the
    **same direction** on the **same ticker** on the **same day**.

    Usage::

        detector = CorroborationDetector(corroboration_multiplier=1.5)
        signals = detector.detect(signals)
    """

    def __init__(self, corroboration_multiplier: float = 1.5) -> None:
        self._multiplier = corroboration_multiplier

    def detect(self, signals: list[Signal]) -> list[Signal]:
        """Tag corroborated signals and compute ``final_score``.

        Groups signals by ``(ticker, direction, signal_date)``.  Within
        each group, counts *distinct* ``investor_id`` values.  Groups
        with 2+ distinct investors receive the corroboration bonus.

        Mutates signals in-place and returns the same list.
        """
        # Group by (ticker, direction, date)
        groups: dict[tuple[str, str, date], list[Signal]] = defaultdict(list)
        for sig in signals:
            key = (sig.ticker.upper(), sig.direction.upper(), sig.signal_date)
            groups[key].append(sig)

        for key, group in groups.items():
            distinct_investors = {sig.investor_id for sig in group}
            count = len(distinct_investors)

            if count >= 2:
                for sig in group:
                    sig.corroboration_count = count
                    sig.corroboration_bonus = self._multiplier
                logger.info(
                    "Corroboration detected: %s %s on %s — %d distinct investors",
                    key[1], key[0], key[2], count,
                )
            else:
                for sig in group:
                    sig.corroboration_count = count
                    sig.corroboration_bonus = 1.0

        # Compute final_score for all signals
        for sig in signals:
            sig.final_score = round(sig.composite_score * sig.corroboration_bonus, 4)

        return signals
