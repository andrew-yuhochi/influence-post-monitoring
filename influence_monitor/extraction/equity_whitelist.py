"""US equity symbol whitelist — final gate against false-positive tickers.

Loads symbols from three sources (in order):
1. S&P 500 constituents CSV (local cache or GitHub download)
2. Manual supplement file (hand-curated OTC/pink-sheet tickers)
3. (Optional) Russell 3000 symbol list

All ticker extraction results must pass ``SymbolWhitelist.contains()``
before entering the scoring pipeline.
"""

from __future__ import annotations

import csv
import io
import logging
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent.parent
_DATA_DIR = _PROJECT_ROOT / "data"
_SP500_CSV = _DATA_DIR / "sp500_constituents.csv"
_MANUAL_SUPPLEMENT = _DATA_DIR / "manual_supplement.txt"
_SP500_URL = (
    "https://raw.githubusercontent.com/datasets/"
    "s-and-p-500-companies/main/data/constituents.csv"
)


class SymbolWhitelist:
    """Set of valid US equity ticker symbols.

    Usage::

        wl = SymbolWhitelist.load()
        wl.contains("AAPL")   # True
        wl.contains("CEO")    # False
    """

    def __init__(self, symbols: set[str]) -> None:
        self._symbols = symbols

    def contains(self, ticker: str) -> bool:
        """Return True if *ticker* is in the whitelist."""
        return ticker.upper() in self._symbols

    def __len__(self) -> int:
        return len(self._symbols)

    @classmethod
    def load(cls) -> SymbolWhitelist:
        """Load whitelist from local CSV + manual supplement.

        Downloads the S&P 500 CSV from GitHub if no local copy exists.
        """
        symbols: set[str] = set()

        # --- S&P 500 ---
        symbols.update(_load_sp500())

        # --- Manual supplement (OTC, pink sheets, tracked-but-not-indexed) ---
        symbols.update(_load_manual_supplement())

        logger.info("Symbol whitelist loaded: %d tickers", len(symbols))
        return cls(symbols)


def _load_sp500() -> set[str]:
    """Load S&P 500 tickers from local CSV, downloading if absent."""
    if _SP500_CSV.exists():
        logger.info("Reading S&P 500 symbols from %s", _SP500_CSV)
        return _parse_sp500_csv(_SP500_CSV.read_text())

    logger.info("Downloading S&P 500 CSV from GitHub")
    try:
        resp = httpx.get(_SP500_URL, timeout=15.0, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("Failed to download S&P 500 CSV: %s", exc)
        return set()

    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    _SP500_CSV.write_text(resp.text)
    logger.info("Saved S&P 500 CSV to %s", _SP500_CSV)
    return _parse_sp500_csv(resp.text)


def _parse_sp500_csv(text: str) -> set[str]:
    """Parse Symbol column from the constituents CSV."""
    symbols: set[str] = set()
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        sym = row.get("Symbol", "").strip().upper()
        if sym:
            symbols.add(sym)
    return symbols


def _load_manual_supplement() -> set[str]:
    """Load hand-curated tickers from manual_supplement.txt.

    Format: one ticker per line, blank lines and ``#`` comments ignored.
    """
    if not _MANUAL_SUPPLEMENT.exists():
        logger.info("No manual supplement file at %s", _MANUAL_SUPPLEMENT)
        return set()

    symbols: set[str] = set()
    for line in _MANUAL_SUPPLEMENT.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        symbols.add(line.upper())

    logger.info("Loaded %d manual supplement tickers", len(symbols))
    return symbols
