"""Three-layer ticker extraction pipeline.

Layers (executed in order, results merged and deduplicated):
  1. Cashtag regex  — ``$TICKER`` mentions  → confidence HIGH
  2. Uppercase regex — standalone ``TICKER`` → confidence MEDIUM
  3. spaCy NER       — company names → Yahoo Finance resolver → confidence LOW

All candidates are validated against SymbolWhitelist before being returned.
"""

from __future__ import annotations

import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Literal

import spacy
import yfinance as yf
from pydantic import BaseModel

from influence_monitor.extraction.equity_whitelist import SymbolWhitelist

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent.parent
_FP_FILTER_PATH = _PROJECT_ROOT / "config" / "false_positive_filter.json"

# --- Regex patterns (from TDD Section 2.2) ---
_CASHTAG_RE = re.compile(r"\$([A-Z]{1,5}(?:\.[A-Z]{1,2})?)")
_STANDALONE_RE = re.compile(r"\b([A-Z]{2,5})\b")


class ExtractedTicker(BaseModel):
    """A ticker extracted from post text with provenance metadata."""

    ticker: str
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    extraction_method: Literal["cashtag", "uppercase", "ner"]
    company_name: str | None = None


class TickerExtractor:
    """Three-layer ticker extraction pipeline.

    Loads the spaCy model and false-positive filter once at init time.
    The Yahoo Finance name→ticker resolver uses an LRU cache to avoid
    repeated API calls for the same company name.

    Usage::

        extractor = TickerExtractor(whitelist)
        tickers = extractor.extract("$FNMA is massively undervalued")
    """

    def __init__(self, whitelist: SymbolWhitelist) -> None:
        self._whitelist = whitelist
        self._false_positives = _load_false_positives()
        self._nlp = spacy.load("en_core_web_sm")
        logger.info(
            "TickerExtractor initialised (whitelist=%d, false_positives=%d)",
            len(whitelist), len(self._false_positives),
        )

    def extract(self, text: str) -> list[ExtractedTicker]:
        """Extract tickers from *text* using all three layers.

        Returns deduplicated list ordered by confidence (HIGH → MEDIUM → LOW).
        Higher-confidence extractions take precedence when the same ticker
        is found by multiple layers.
        """
        seen: dict[str, ExtractedTicker] = {}

        # Layer 1: cashtag regex (HIGH confidence)
        for ticker in self._extract_cashtags(text):
            if ticker not in seen:
                seen[ticker] = ExtractedTicker(
                    ticker=ticker,
                    confidence="HIGH",
                    extraction_method="cashtag",
                )

        # Layer 2: standalone uppercase (MEDIUM confidence)
        for ticker in self._extract_uppercase(text):
            if ticker not in seen:
                seen[ticker] = ExtractedTicker(
                    ticker=ticker,
                    confidence="MEDIUM",
                    extraction_method="uppercase",
                )

        # Layer 3: spaCy NER → Yahoo Finance resolver (LOW confidence)
        for ticker, company_name in self._extract_ner(text):
            if ticker not in seen:
                seen[ticker] = ExtractedTicker(
                    ticker=ticker,
                    confidence="LOW",
                    extraction_method="ner",
                    company_name=company_name,
                )

        # Sort by confidence priority
        priority = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        results = sorted(seen.values(), key=lambda t: priority[t.confidence])
        return results

    # ------------------------------------------------------------------
    # Layer 1: Cashtag regex
    # ------------------------------------------------------------------

    def _extract_cashtags(self, text: str) -> list[str]:
        """Extract tickers from ``$TICKER`` patterns."""
        matches = _CASHTAG_RE.findall(text)
        return [m for m in matches if self._whitelist.contains(m)]

    # ------------------------------------------------------------------
    # Layer 2: Standalone uppercase
    # ------------------------------------------------------------------

    def _extract_uppercase(self, text: str) -> list[str]:
        """Extract uppercase words that look like tickers.

        Filters out false positives (common acronyms, English words)
        and validates against the whitelist.
        """
        matches = _STANDALONE_RE.findall(text)
        results: list[str] = []
        for m in matches:
            if m in self._false_positives:
                continue
            if self._whitelist.contains(m):
                results.append(m)
        return results

    # ------------------------------------------------------------------
    # Layer 3: spaCy NER → Yahoo Finance resolver
    # ------------------------------------------------------------------

    def _extract_ner(self, text: str) -> list[tuple[str, str]]:
        """Extract ORG entities and resolve to tickers via Yahoo Finance.

        Returns list of (ticker, company_name) tuples.
        """
        doc = self._nlp(text)
        results: list[tuple[str, str]] = []

        for ent in doc.ents:
            if ent.label_ != "ORG":
                continue

            company_name = ent.text.strip()
            if len(company_name) < 2:
                continue

            # Skip if the entity text is already a known ticker (handled by L1/L2)
            if company_name.isupper() and len(company_name) <= 5:
                continue

            ticker = _resolve_company_to_ticker(company_name)
            if ticker and self._whitelist.contains(ticker):
                results.append((ticker, company_name))

        return results


# ------------------------------------------------------------------
# Yahoo Finance resolver (cached)
# ------------------------------------------------------------------

@lru_cache(maxsize=512)
def _resolve_company_to_ticker(company_name: str) -> str | None:
    """Resolve a company name to a ticker via yfinance search.

    Cached to avoid repeated API calls for the same name.
    Returns None if resolution fails.
    """
    try:
        results = yf.Search(company_name)
        quotes = getattr(results, "quotes", [])
        if not quotes:
            logger.debug("Yahoo Finance: no results for '%s'", company_name)
            return None

        # Take the first equity result
        for quote in quotes:
            quote_type = quote.get("quoteType", "")
            if quote_type == "EQUITY":
                ticker = quote.get("symbol", "")
                if ticker:
                    logger.debug(
                        "Yahoo Finance: '%s' → %s", company_name, ticker,
                    )
                    return ticker.upper()

        # Fallback: take the first result regardless of type
        first_symbol = quotes[0].get("symbol", "")
        if first_symbol:
            return first_symbol.upper()

    except Exception as exc:
        logger.warning(
            "Yahoo Finance resolver failed for '%s': %s", company_name, exc,
        )

    return None


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _load_false_positives() -> frozenset[str]:
    """Load false-positive filter from config JSON."""
    if not _FP_FILTER_PATH.exists():
        logger.warning("False positive filter not found at %s", _FP_FILTER_PATH)
        return frozenset()

    data = json.loads(_FP_FILTER_PATH.read_text())
    fps = frozenset(w.upper() for w in data.get("false_positives", []))
    logger.info("Loaded %d false-positive filter words", len(fps))
    return fps
