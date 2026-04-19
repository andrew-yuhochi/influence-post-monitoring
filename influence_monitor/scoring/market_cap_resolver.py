"""MarketCapResolver — market-cap class lookup with 7-day DB cache (TASK-009).

Resolves a ticker to one of five market-cap classes:
    Mega  (≥ $200 000M / $200B)
    Large (≥ $10 000M / $10B)
    Mid   (≥  $2 000M / $2B)
    Small (≥    $300M)
    Micro (<    $300M)

Data source: finvizfinance (scrapes Finviz fundamentals page).
Cache: price_cache table in SQLite/Turso; TTL = 7 days (enforced by DB query).
On any finvizfinance exception the resolver logs a WARNING and returns "Micro"
so that scoring continues gracefully.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from influence_monitor.db.repository import SignalRepository

_DEFAULT_TENANT_ID = 1

logger = logging.getLogger(__name__)

MarketCapClass = Literal["Mega", "Large", "Mid", "Small", "Micro"]

# Market-cap class boundaries in millions of USD
_MEGA_THRESHOLD_M = 200_000   # >= $200B
_LARGE_THRESHOLD_M = 10_000   # >= $10B
_MID_THRESHOLD_M = 2_000      # >= $2B
_SMALL_THRESHOLD_M = 300      # >= $300M
# < $300M → Micro

# Pattern: "3911.50B", "498.22M", "1.23T"
_MARKET_CAP_RE = re.compile(r"^([\d,]+(?:\.\d+)?)\s*([TMBKtmbk])$")


def _parse_market_cap_to_millions(raw: str) -> float | None:
    """Parse a Finviz market-cap string to millions of USD.

    Examples:
        "3911.50B" → 3_911_500.0
        "498.22M"  → 498.22
        "1.23T"    → 1_230_000.0
        ""         → None
        "N/A"      → None
    """
    if not raw or raw.strip() in ("", "-", "N/A", "n/a"):
        return None

    cleaned = raw.strip().replace(",", "")
    m = _MARKET_CAP_RE.match(cleaned)
    if not m:
        logger.debug("Cannot parse market cap string: %r", raw)
        return None

    value = float(m.group(1))
    suffix = m.group(2).upper()

    if suffix == "T":
        return value * 1_000_000.0   # trillions → millions
    elif suffix == "B":
        return value * 1_000.0       # billions → millions
    elif suffix == "M":
        return value                  # already millions
    elif suffix == "K":
        return value / 1_000.0       # thousands → millions
    return None


def _classify(market_cap_m: float | None) -> MarketCapClass:
    """Map a market-cap value (in millions) to a cap class string."""
    if market_cap_m is None:
        return "Micro"
    if market_cap_m >= _MEGA_THRESHOLD_M:
        return "Mega"
    if market_cap_m >= _LARGE_THRESHOLD_M:
        return "Large"
    if market_cap_m >= _MID_THRESHOLD_M:
        return "Mid"
    if market_cap_m >= _SMALL_THRESHOLD_M:
        return "Small"
    return "Micro"


class MarketCapResolver:
    """Resolve a ticker symbol to its market-cap class and liquidity modifier.

    Cache-first: checks price_cache (7-day TTL) before calling finvizfinance.
    Falls back to "Micro" on any finvizfinance exception.

    Usage::

        resolver = MarketCapResolver(repo)
        cap_class, modifier = resolver.resolve("AAPL")   # → ("Mega", 0.8)
    """

    def __init__(self, repo: "SignalRepository", tenant_id: int = _DEFAULT_TENANT_ID) -> None:
        self._repo = repo
        self._tenant_id = tenant_id
        cfg = repo.get_scoring_config(tenant_id=tenant_id)
        self._liq_modifiers: dict[str, float] = {
            "mega": float(cfg.get("liq_mega", 0.8)),
            "large": float(cfg.get("liq_large", 0.9)),
            "mid": float(cfg.get("liq_mid", 1.0)),
            "small": float(cfg.get("liq_small", 1.15)),
            "micro": float(cfg.get("liq_micro", 1.3)),
        }

    def _get_modifier(self, cap_class: MarketCapClass) -> float:
        """Return the liquidity modifier for *cap_class* (from scoring_config)."""
        return self._liq_modifiers.get(cap_class.lower(), 1.0)

    def resolve(self, ticker: str) -> tuple[MarketCapClass, float]:
        """Return the market-cap class and liquidity modifier for *ticker*.

        Checks the 7-day price_cache first. On a miss, calls finvizfinance,
        parses the "Market Cap" field, classifies it, and upserts the cache.

        Parameters
        ----------
        ticker:
            Ticker symbol (case-insensitive; normalised to upper-case internally).

        Returns
        -------
        tuple[str, float]
            ``(cap_class, liquidity_modifier)`` where cap_class is one of
            "Mega", "Large", "Mid", "Small", "Micro" and liquidity_modifier
            is the corresponding value from scoring_config (e.g. 0.8 for Mega).
        """
        ticker_upper = ticker.upper()

        # Cache hit path
        cached = self._repo.get_cached_market_cap(ticker_upper)
        if cached is not None:
            cap_class: MarketCapClass = cached["market_cap_class"]  # type: ignore[assignment]
            modifier = self._get_modifier(cap_class)
            logger.debug(
                "MarketCapResolver cache hit: %s → %s (modifier=%.2f)",
                ticker_upper, cap_class, modifier,
            )
            return cap_class, modifier

        # Cache miss — call finvizfinance
        try:
            from finvizfinance.quote import finvizfinance  # type: ignore[import]

            stock = finvizfinance(ticker_upper)
            fundamentals: dict = stock.TickerFundamentals()
            raw_cap: str = fundamentals.get("Market Cap", "")
            sector: str | None = fundamentals.get("Sector") or None
            industry: str | None = fundamentals.get("Industry") or None
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "finvizfinance failed for ticker %s: %s — defaulting to Micro",
                ticker_upper,
                exc,
            )
            return "Micro", self._get_modifier("Micro")

        market_cap_m = _parse_market_cap_to_millions(raw_cap)
        cap_class = _classify(market_cap_m)

        # Convert millions back to billions for storage (schema column is market_cap_b)
        market_cap_b: float | None = (market_cap_m / 1_000.0) if market_cap_m is not None else None

        self._repo.upsert_price_cache(
            ticker=ticker_upper,
            market_cap_b=market_cap_b,
            market_cap_class=cap_class,
            sector=sector,
            industry=industry,
        )

        modifier = self._get_modifier(cap_class)
        logger.info(
            "MarketCapResolver fetched %s: raw=%r → %.1fM → %s modifier=%.2f (cached)",
            ticker_upper,
            raw_cap,
            market_cap_m or 0.0,
            cap_class,
            modifier,
        )
        return cap_class, modifier
