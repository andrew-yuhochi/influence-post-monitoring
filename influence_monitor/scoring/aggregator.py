"""Signal aggregator — ranks and deduplicates signals for the morning email.

After corroboration detection, the aggregator produces the final ranked
list that goes into the morning watchlist email:

1. Sort all signals by ``final_score`` descending.
2. Deduplicate by ticker — keep only the highest-scored signal per ticker.
3. Return the top *N* signals.
"""

from __future__ import annotations

import logging

from influence_monitor.scoring.corroboration import Signal

logger = logging.getLogger(__name__)


class SignalAggregator:
    """Rank, deduplicate, and trim signals for delivery.

    Usage::

        aggregator = SignalAggregator()
        top_signals = aggregator.rank(signals, top_n=10)
    """

    def rank(self, signals: list[Signal], top_n: int = 10) -> list[Signal]:
        """Return the top *top_n* signals, deduplicated by ticker.

        For each ticker, only the signal with the highest ``final_score``
        is kept.  The returned list is sorted by ``final_score``
        descending.
        """
        # Deduplicate: keep highest final_score per ticker
        best_by_ticker: dict[str, Signal] = {}
        for sig in signals:
            ticker = sig.ticker.upper()
            if ticker not in best_by_ticker or sig.final_score > best_by_ticker[ticker].final_score:
                best_by_ticker[ticker] = sig

        # Sort by final_score descending
        ranked = sorted(
            best_by_ticker.values(),
            key=lambda s: s.final_score,
            reverse=True,
        )

        result = ranked[:top_n]

        logger.info(
            "Aggregated %d signals → %d unique tickers → top %d returned",
            len(signals),
            len(best_by_ticker),
            len(result),
        )
        return result
