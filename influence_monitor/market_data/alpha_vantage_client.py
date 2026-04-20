"""Alpha Vantage market data client — fallback for yfinance.

Uses the GLOBAL_QUOTE endpoint (free tier: 25 requests/day).
Only used when yfinance fails freshness assertion after retry.

Extended in TASK-011: adds fetch_stock_vol (via TIME_SERIES_DAILY) and
fetch_spy_return (via GLOBAL_QUOTE on SPY) to match the MarketDataClient ABC.
"""

from __future__ import annotations

import logging
import statistics
from datetime import date

import httpx

from influence_monitor.config import Settings
from influence_monitor.market_data.base import (
    DataFreshnessError,
    DataUnavailableError,
    MarketDataClient,
)
from influence_monitor.market_data.trading_calendar import TradingCalendar

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
        self._calendar = TradingCalendar()

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

    def fetch_stock_vol(
        self, ticker: str, target_date: date, lookback_days: int,
    ) -> float | None:
        """Compute daily-return stdev via Alpha Vantage TIME_SERIES_DAILY.

        Free-tier note: TIME_SERIES_DAILY defaults to 100 data points (compact),
        which covers up to ~lookback_days=60 without needing outputsize=full.

        Args:
            ticker: Equity ticker symbol.
            target_date: Last day of the lookback window (inclusive).
            lookback_days: Number of prior trading days; required parameter.

        Returns:
            Daily stdev of close-to-close returns, or None on fetch failure.
        """
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": ticker,
            "outputsize": "compact",
            "apikey": self._api_key,
        }

        try:
            resp = httpx.get(_BASE_URL, params=params, timeout=15.0)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("AlphaVantage fetch_stock_vol HTTP error for %s: %s", ticker, exc)
            return None

        data = resp.json()
        time_series = data.get("Time Series (Daily)", {})

        if not time_series:
            error_msg = data.get("Note", data.get("Information", "empty time series"))
            logger.warning(
                "AlphaVantage fetch_stock_vol: no time series for %s — %s", ticker, error_msg,
            )
            return None

        # Collect trading days in the window using the authoritative calendar.
        trading_days = self._calendar.trading_days_between(
            date.fromisoformat(min(time_series.keys())),
            target_date,
        )
        window_trading_days = trading_days[-lookback_days:] if len(trading_days) >= lookback_days else trading_days

        # Extract closes for days present in the API response.
        closes: list[float] = []
        for d in window_trading_days:
            day_str = d.isoformat()
            if day_str in time_series:
                try:
                    closes.append(float(time_series[day_str]["4. close"]))
                except (KeyError, ValueError):
                    pass

        if len(closes) < 2:
            logger.warning(
                "AlphaVantage fetch_stock_vol: insufficient data for %s "
                "(got %d prices, need >=2) target=%s lookback=%d",
                ticker, len(closes), target_date, lookback_days,
            )
            return None

        returns = [
            (closes[i] - closes[i - 1]) / closes[i - 1]
            for i in range(1, len(closes))
        ]
        vol = statistics.stdev(returns)
        logger.debug(
            "AlphaVantage fetch_stock_vol: %s target=%s lookback=%d observations=%d vol=%.6f",
            ticker, target_date, lookback_days, len(closes), vol,
        )
        return vol

    def fetch_spy_return(self, target_date: date) -> float | None:
        """Compute the SPY day-over-day return via Alpha Vantage GLOBAL_QUOTE.

        Fetches the latest GLOBAL_QUOTE for SPY and computes the change percent
        directly from the API's '10. change percent' field when target_date matches
        the latest trading day.  Falls back to None on any fetch failure.

        Returns:
            Fractional return (e.g. 0.012 for +1.2 %), or None on fetch failure.
        """
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": "SPY",
            "apikey": self._api_key,
        }

        try:
            resp = httpx.get(_BASE_URL, params=params, timeout=15.0)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("AlphaVantage fetch_spy_return HTTP error: %s", exc)
            return None

        data = resp.json()
        quote = data.get("Global Quote", {})

        if not quote:
            error_msg = data.get("Note", data.get("Information", "empty response"))
            logger.warning("AlphaVantage fetch_spy_return: no SPY data — %s", error_msg)
            return None

        # Freshness check: verify this is data for our target_date.
        latest_day = quote.get("07. latest trading day", "")
        if latest_day and latest_day != target_date.isoformat():
            logger.warning(
                "AlphaVantage fetch_spy_return: stale SPY data (got %s, expected %s)",
                latest_day, target_date,
            )
            return None

        # Use the pre-computed change percent from the API to avoid a second call.
        try:
            change_pct_str = quote.get("10. change percent", "").replace("%", "")
            spy_return = float(change_pct_str) / 100.0
        except (ValueError, AttributeError) as exc:
            logger.warning("AlphaVantage fetch_spy_return: cannot parse change percent: %s", exc)
            return None

        logger.debug(
            "AlphaVantage fetch_spy_return: target=%s spy_return=%.6f", target_date, spy_return,
        )
        return spy_return
