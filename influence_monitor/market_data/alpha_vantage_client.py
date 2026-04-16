"""Alpha Vantage market data client — fallback for yfinance.

Uses the GLOBAL_QUOTE endpoint (free tier: 25 requests/day).
Only used when yfinance fails freshness assertion after retry.
"""

from __future__ import annotations

import logging
from datetime import date

import httpx

from influence_monitor.config import Settings
from influence_monitor.market_data.base import (
    DataFreshnessError,
    DataUnavailableError,
    MarketDataClient,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.alphavantage.co/query"


class AlphaVantageClient(MarketDataClient):
    """Alpha Vantage GLOBAL_QUOTE client.

    Free tier: 25 requests/day — use only as yfinance fallback.
    """

    def __init__(self, settings: Settings) -> None:
        self._api_key = settings.alpha_vantage_api_key
        if not self._api_key:
            logger.warning("ALPHA_VANTAGE_API_KEY not set — fallback will fail")

    def fetch_open(self, ticker: str, target_date: date) -> float:
        ohlcv = self.fetch_ohlcv(ticker, target_date)
        return ohlcv["open"]  # type: ignore[return-value]

    def fetch_close(self, ticker: str, target_date: date) -> float:
        ohlcv = self.fetch_ohlcv(ticker, target_date)
        return ohlcv["close"]  # type: ignore[return-value]

    def fetch_ohlcv(self, ticker: str, target_date: date) -> dict[str, float | int | None]:
        """Fetch OHLCV from Alpha Vantage GLOBAL_QUOTE endpoint.

        Raises:
            DataUnavailableError: No data or API error.
            DataFreshnessError: Data date does not match target_date.
        """
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": ticker,
            "apikey": self._api_key,
        }

        try:
            resp = httpx.get(_BASE_URL, params=params, timeout=15.0)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise DataUnavailableError(
                f"Alpha Vantage HTTP error for {ticker}: {exc}"
            ) from exc

        data = resp.json()
        quote = data.get("Global Quote", {})

        if not quote:
            error_msg = data.get("Note", data.get("Information", "empty response"))
            raise DataUnavailableError(
                f"Alpha Vantage: no data for {ticker} — {error_msg}"
            )

        # Freshness check
        latest_day = quote.get("07. latest trading day", "")
        if latest_day and latest_day != target_date.isoformat():
            raise DataFreshnessError(
                f"Alpha Vantage returned data for {latest_day}, "
                f"expected {target_date} (ticker: {ticker})"
            )

        try:
            return {
                "open": float(quote.get("02. open", 0)),
                "high": float(quote.get("03. high", 0)),
                "low": float(quote.get("04. low", 0)),
                "close": float(quote.get("05. price", 0)),
                "volume": int(quote.get("06. volume", 0)),
            }
        except (ValueError, TypeError) as exc:
            raise DataUnavailableError(
                f"Alpha Vantage: failed to parse response for {ticker}: {exc}"
            ) from exc
