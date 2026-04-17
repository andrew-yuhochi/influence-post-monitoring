"""Index membership resolver with DB-cache-first strategy.

Determines whether a ticker belongs to S&P 500, Nasdaq-100, Russell 2000,
or is a micro/OTC stock.  Resolution order:

1. ``index_membership`` table (fresh within 7 days)
2. Static lists — S&P 500 CSV, NDX-100 symbols file
3. finvizfinance live lookup
4. Default: ``"MICRO"``

The static lists are loaded at ``initialize()`` and bulk-written to the DB
cache so that subsequent ``resolve()`` calls are pure cache hits.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import date
from pathlib import Path

from influence_monitor.db.repository import DatabaseRepository

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent.parent
_DATA_DIR = _PROJECT_ROOT / "data"
_SP500_CSV = _DATA_DIR / "sp500_constituents.csv"
_NDX100_FILE = _DATA_DIR / "ndx100_symbols.txt"

_VALID_TIERS = frozenset({"SP500", "NDX", "RUT", "MICRO"})


class IndexMembershipResolver:
    """Cache-first index membership resolver.

    Usage::

        resolver = IndexMembershipResolver(repo)
        await resolver.initialize()          # loads static lists + populates DB
        tier = await resolver.resolve("AAPL")  # "SP500"
    """

    _CACHE_TTL_DAYS = 7

    def __init__(self, repo: DatabaseRepository) -> None:
        self._repo = repo
        self._sp500_tickers: set[str] = set()
        self._ndx_tickers: set[str] = set()

    async def initialize(self) -> None:
        """Load static lists and populate DB cache for known index members."""
        self._sp500_tickers = _load_sp500_tickers()
        self._ndx_tickers = _load_ndx_tickers()
        await self._populate_from_static()

    async def resolve(self, ticker: str) -> str:
        """Resolve index tier for *ticker*: ``SP500``, ``NDX``, ``RUT``, or ``MICRO``.

        Cache-first: DB cache → static lists → finvizfinance → MICRO.
        """
        ticker = ticker.upper()

        # 1. DB cache (fast path)
        cached = await self._repo.get_index_membership(ticker)
        if cached and _is_fresh(cached["last_updated"], self._CACHE_TTL_DAYS):
            return cached["index_tier"]

        # 2. Static lists (covers SP500 + NDX members)
        if ticker in self._sp500_tickers:
            tier = "SP500"
        elif ticker in self._ndx_tickers:
            tier = "NDX"
        else:
            # 3. finvizfinance live lookup
            tier = _finviz_lookup(ticker)

        # Persist to cache
        await self._repo.upsert_index_membership(ticker, tier)
        return tier

    async def refresh_weekly(self) -> None:
        """Re-resolve all cached entries older than 7 days."""
        stale_entries = await self._repo.get_stale_index_entries(
            self._CACHE_TTL_DAYS,
        )
        if not stale_entries:
            logger.info("Weekly refresh: no stale index entries")
            return

        refreshed = 0
        for entry in stale_entries:
            ticker = entry["ticker"]
            if ticker in self._sp500_tickers:
                tier = "SP500"
            elif ticker in self._ndx_tickers:
                tier = "NDX"
            else:
                tier = _finviz_lookup(ticker)

            await self._repo.upsert_index_membership(ticker, tier)
            refreshed += 1

        logger.info("Weekly refresh: updated %d stale index entries", refreshed)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _populate_from_static(self) -> None:
        """Bulk-write static-list entries to the DB cache.

        Existing fresh entries are left untouched (the SQL ``WHERE``
        clause skips rows whose ``last_updated`` is within TTL).
        """
        entries: list[tuple[str, str]] = []

        for ticker in sorted(self._sp500_tickers):
            entries.append((ticker, "SP500"))

        # NDX-only: tickers in NDX that are NOT in SP500
        ndx_only = self._ndx_tickers - self._sp500_tickers
        for ticker in sorted(ndx_only):
            entries.append((ticker, "NDX"))

        if entries:
            count = await self._repo.bulk_upsert_index_membership_if_stale(
                entries, self._CACHE_TTL_DAYS,
            )
            logger.info(
                "Index cache populated: %d entries processed "
                "(%d SP500, %d NDX-only)",
                count,
                len(self._sp500_tickers),
                len(ndx_only),
            )


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _is_fresh(last_updated: str, ttl_days: int) -> bool:
    """Return True if *last_updated* (ISO date string) is within *ttl_days*."""
    last = date.fromisoformat(last_updated)
    return (date.today() - last).days < ttl_days


def _finviz_lookup(ticker: str) -> str:
    """Look up index membership via finvizfinance.

    Returns one of ``SP500``, ``NDX``, ``RUT``, ``MICRO``.
    Any exception → ``MICRO`` (logged at WARNING).
    """
    try:
        from finvizfinance.quote import finvizfinance as Finviz

        stock = Finviz(ticker)
        fundament = stock.ticker_fundament()
        index_val = fundament.get("Index", "")

        if "S&P 500" in index_val:
            return "SP500"
        # finviz does not reliably distinguish NDX / RUT in the Index
        # field, so tickers not in our static lists default to MICRO.
        return "MICRO"
    except Exception as exc:
        logger.warning("finvizfinance lookup failed for %s: %s", ticker, exc)
        return "MICRO"


def _load_sp500_tickers() -> set[str]:
    """Load S&P 500 ticker symbols from the local CSV."""
    if not _SP500_CSV.exists():
        logger.warning("S&P 500 CSV not found at %s", _SP500_CSV)
        return set()

    symbols: set[str] = set()
    reader = csv.DictReader(io.StringIO(_SP500_CSV.read_text()))
    for row in reader:
        sym = row.get("Symbol", "").strip().upper()
        if sym:
            symbols.add(sym)

    logger.info("Loaded %d S&P 500 tickers from CSV", len(symbols))
    return symbols


def _load_ndx_tickers() -> set[str]:
    """Load Nasdaq-100 ticker symbols from the manual seed file.

    Format: one ticker per line, blank lines and ``#`` comments ignored.
    """
    if not _NDX100_FILE.exists():
        logger.warning("NDX-100 symbols file not found at %s", _NDX100_FILE)
        return set()

    symbols: set[str] = set()
    for line in _NDX100_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        symbols.add(line.upper())

    logger.info("Loaded %d NDX-100 tickers from file", len(symbols))
    return symbols
