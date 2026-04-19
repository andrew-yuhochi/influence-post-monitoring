"""Demo script for TASK-006 — ticker extraction on 5 representative Ackman posts.

Run from project root:
    python scripts/demo_task006_extraction.py

Output is printed to stdout AND saved to:
    docs/influence-post-monitoring/poc/demos/milestone-2/TASK-006-extraction.txt
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Ensure project root is on the path when run as a script
_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

logging.basicConfig(level=logging.WARNING)  # Suppress INFO noise in demo output

from influence_monitor.extraction.equity_whitelist import SymbolWhitelist
from influence_monitor.extraction.ticker_extractor import TickerExtractor

# ---------------------------------------------------------------------------
# 5 representative Bill Ackman / activist investor posts
# ---------------------------------------------------------------------------

SAMPLE_POSTS = [
    {
        "author": "@BillAckman",
        "text": (
            "$FNMA and $FMCC remain massively undervalued. "
            "The GSEs are trading at a fraction of book value. "
            "Conservatorship release is a matter of when, not if. "
            "This is the most asymmetric trade I've seen in years."
        ),
    },
    {
        "author": "@BillAckman",
        "text": (
            "NFLX guidance will miss next quarter. "
            "Subscriber growth is decelerating faster than consensus models. "
            "The market is pricing in 15% revenue growth — that's wrong. "
            "I remain short."
        ),
    },
    {
        "author": "@BillAckman",
        "text": (
            "Apple is the most undervalued mega-cap in the market. "
            "Services revenue alone will be $100B by 2026. "
            "The buyback engine is relentless. Long term hold."
        ),
    },
    {
        "author": "@BillAckman",
        "text": (
            "Fannie Mae will be released from conservatorship. "
            "The political and economic logic is inescapable. "
            "We have been building our position for two years."
        ),
    },
    {
        "author": "@BillAckman",
        "text": (
            "TSLA is executing on its energy storage business better than anyone realises. "
            "The CEO narrative obscures the underlying business quality. "
            "At these prices, the risk/reward is compelling. "
            "Our fund has initiated a long position."
        ),
    },
]

# ---------------------------------------------------------------------------
# Run extraction
# ---------------------------------------------------------------------------

def main() -> None:
    output_lines: list[str] = []

    def emit(line: str = "") -> None:
        print(line)
        output_lines.append(line)

    emit("=" * 68)
    emit("TASK-006 Demo: Ticker Extraction on 5 Ackman Posts")
    emit("Three-layer pipeline: cashtag → uppercase → spaCy NER + Yahoo Finance")
    emit("=" * 68)
    emit()

    # Load whitelist
    emit("Loading symbol whitelist (S&P 500 + Russell 3000 + supplement)...")
    wl = SymbolWhitelist.load()
    emit(f"  Whitelist size: {len(wl):,} symbols")
    emit()

    # Spot-check acceptance criteria
    emit("Whitelist acceptance criteria:")
    emit(f"  contains('AAPL') = {wl.contains('AAPL')}   (expected: True)")
    emit(f"  contains('CEO')  = {wl.contains('CEO')}  (expected: False)")
    emit(f"  contains('FNMA') = {wl.contains('FNMA')}   (expected: True, via supplement)")
    emit()

    # Build extractor
    emit("Loading TickerExtractor (spaCy en_core_web_sm)...")
    extractor = TickerExtractor(wl)
    emit()

    # Extract from each post
    for i, post in enumerate(SAMPLE_POSTS, 1):
        emit(f"Post {i} — {post['author']}")
        emit(f"Text: {post['text']}")
        results = extractor.extract(post["text"])
        if results:
            emit("Extracted tickers:")
            for t in results:
                name_part = f" (company: {t.company_name})" if t.company_name else ""
                emit(f"  {t.ticker:8s}  confidence={t.confidence:6s}  method={t.extraction_method}{name_part}")
        else:
            emit("  (no tickers extracted)")
        emit()

    # Save output
    output_path = (
        _PROJECT_ROOT.parent.parent
        / "docs"
        / "influence-post-monitoring"
        / "poc"
        / "demos"
        / "milestone-2"
        / "TASK-006-extraction.txt"
    )
    # Try the docs path relative to project root
    docs_path = _PROJECT_ROOT.parent / "docs" / "influence-post-monitoring" / "poc" / "demos" / "milestone-2" / "TASK-006-extraction.txt"
    if not docs_path.parent.exists():
        # Try one level up
        docs_path = _PROJECT_ROOT.parent.parent / "docs" / "influence-post-monitoring" / "poc" / "demos" / "milestone-2" / "TASK-006-extraction.txt"

    docs_path.parent.mkdir(parents=True, exist_ok=True)
    docs_path.write_text("\n".join(output_lines) + "\n")
    print(f"Demo artifact saved to: {docs_path}")


if __name__ == "__main__":
    main()
