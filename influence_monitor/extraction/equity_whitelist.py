"""US equity symbol whitelist — final gate against false-positive tickers.

Loads symbols from three sources (in order):
1. S&P 500 constituents CSV  → data/sp500.csv  (downloads from GitHub if absent)
2. Russell 3000 CSV          → data/russell3000.csv (downloads from GitHub if absent)
3. Supplement file           → data/supplement.txt  (hand-curated OTC/pink-sheet tickers)

Legacy filenames (sp500_constituents.csv, manual_supplement.txt) are also checked so
existing data directories continue to work without a rename.

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

# Canonical filenames (spec / TDD)
_SP500_CSV = _DATA_DIR / "sp500.csv"
_RUSSELL_CSV = _DATA_DIR / "russell3000.csv"
_SUPPLEMENT_TXT = _DATA_DIR / "supplement.txt"

# Legacy filenames (backward-compat for existing data directories)
_SP500_CSV_LEGACY = _DATA_DIR / "sp500_constituents.csv"
_SUPPLEMENT_TXT_LEGACY = _DATA_DIR / "manual_supplement.txt"

_SP500_URL = (
    "https://raw.githubusercontent.com/datasets/"
    "s-and-p-500-companies/main/data/constituents.csv"
)
# rreichel3/US-Stock-Symbols — all_symbols.csv contains both NYSE + NASDAQ listings
# which covers most of the Russell 3000 universe
_RUSSELL_URL = (
    "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse_full_tickers.csv"
)
_NASDAQ_URL = (
    "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.csv"
)


class SymbolWhitelist:
    """Set of valid US equity ticker symbols.

    Usage::

        wl = SymbolWhitelist.load()
        wl.contains("AAPL")   # True
        wl.contains("CEO")    # False
        wl.contains("FNMA")   # True  (via supplement)
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
        """Load whitelist from S&P 500 + Russell 3000 CSVs + supplement file.

        Download strategy:
        - S&P 500: GitHub datasets/s-and-p-500-companies
        - Russell 3000: rreichel3/US-Stock-Symbols (NYSE + NASDAQ listings)
        - Supplement: data/supplement.txt (hand-curated OTC names)

        Falls back to legacy filenames when canonical files are absent.
        """
        symbols: set[str] = set()

        # --- S&P 500 ---
        symbols.update(_load_sp500())

        # --- Russell 3000 (NYSE + NASDAQ universe) ---
        symbols.update(_load_russell3000())

        # --- Supplement (OTC, pink sheets, tracked-but-not-indexed) ---
        symbols.update(_load_supplement())

        logger.info("Symbol whitelist loaded: %d tickers", len(symbols))
        return cls(symbols)


# ---------------------------------------------------------------------------
# S&P 500 loader
# ---------------------------------------------------------------------------

def _load_sp500() -> set[str]:
    """Load S&P 500 tickers from local CSV, trying canonical then legacy path.

    Downloads from GitHub and saves to the canonical path if neither exists.
    """
    # 1. Canonical path
    if _SP500_CSV.exists():
        logger.info("Reading S&P 500 symbols from %s", _SP500_CSV)
        return _parse_sp500_csv(_SP500_CSV.read_text())

    # 2. Legacy path (backward-compat)
    if _SP500_CSV_LEGACY.exists():
        logger.info("Reading S&P 500 symbols from legacy path %s", _SP500_CSV_LEGACY)
        return _parse_sp500_csv(_SP500_CSV_LEGACY.read_text())

    # 3. Download
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
    """Parse Symbol column from the S&P 500 constituents CSV."""
    symbols: set[str] = set()
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        sym = row.get("Symbol", "").strip().upper()
        if sym:
            symbols.add(sym)
    return symbols


# ---------------------------------------------------------------------------
# Russell 3000 loader
# ---------------------------------------------------------------------------

def _load_russell3000() -> set[str]:
    """Load Russell 3000 approximation from local CSV or GitHub download.

    Uses rreichel3/US-Stock-Symbols (NYSE + NASDAQ) as a proxy.
    Saves to data/russell3000.csv on first download.
    """
    if _RUSSELL_CSV.exists():
        logger.info("Reading Russell 3000 symbols from %s", _RUSSELL_CSV)
        return _parse_russell_csv(_RUSSELL_CSV.read_text())

    logger.info("Downloading Russell 3000 symbol list from GitHub")
    symbols: set[str] = set()
    lines: list[str] = []

    for url, exchange in [(_RUSSELL_URL, "NYSE"), (_NASDAQ_URL, "NASDAQ")]:
        try:
            resp = httpx.get(url, timeout=5.0, follow_redirects=True)
            resp.raise_for_status()
            syms = _parse_exchange_csv(resp.text)
            symbols.update(syms)
            lines.extend(resp.text.splitlines(keepends=True))
            logger.info("Downloaded %d %s symbols", len(syms), exchange)
        except httpx.HTTPError as exc:
            logger.warning("Failed to download %s symbol list: %s", exchange, exc)

    if symbols:
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        # Write a simple one-ticker-per-line CSV for caching
        _RUSSELL_CSV.write_text("\n".join(sorted(symbols)) + "\n")
        logger.info(
            "Saved %d Russell 3000 symbols to %s", len(symbols), _RUSSELL_CSV,
        )

    return symbols


def _parse_russell_csv(text: str) -> set[str]:
    """Parse cached Russell 3000 CSV — one symbol per line."""
    symbols: set[str] = set()
    for line in text.splitlines():
        sym = line.strip().upper()
        # Skip header-like lines and empty lines
        if sym and not sym.startswith("#") and sym not in ("SYMBOL", "TICKER"):
            symbols.add(sym)
    return symbols


def _parse_exchange_csv(text: str) -> set[str]:
    """Parse Symbol/Ticker column from exchange symbol CSV files."""
    symbols: set[str] = set()
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        # Try common column names
        sym = (
            row.get("Symbol", "")
            or row.get("Ticker", "")
            or row.get("ticker", "")
            or row.get("symbol", "")
        ).strip().upper()
        if sym and 1 <= len(sym) <= 5:
            symbols.add(sym)
    return symbols


# ---------------------------------------------------------------------------
# Supplement loader
# ---------------------------------------------------------------------------

def _load_supplement() -> set[str]:
    """Load hand-curated tickers from supplement.txt (or legacy manual_supplement.txt).

    Format: one ticker per line, blank lines and ``#`` comments ignored.
    """
    # 1. Canonical path
    if _SUPPLEMENT_TXT.exists():
        logger.info("Reading supplement from %s", _SUPPLEMENT_TXT)
        return _parse_supplement(_SUPPLEMENT_TXT.read_text())

    # 2. Legacy path
    if _SUPPLEMENT_TXT_LEGACY.exists():
        logger.info("Reading supplement from legacy path %s", _SUPPLEMENT_TXT_LEGACY)
        return _parse_supplement(_SUPPLEMENT_TXT_LEGACY.read_text())

    logger.info("No supplement file found (checked %s and %s)", _SUPPLEMENT_TXT, _SUPPLEMENT_TXT_LEGACY)
    return set()


def _parse_supplement(text: str) -> set[str]:
    """Parse a supplement file — one ticker per line, # comments ignored."""
    symbols: set[str] = set()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        symbols.add(line.upper())
    return symbols
