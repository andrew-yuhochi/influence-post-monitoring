"""yfinance market data client with freshness assertion.

Primary OHLC data source. Every fetch asserts that the returned data
matches the requested date — yfinance's most dangerous failure mode is
returning stale data silently without raising an error.

On DataFreshnessError: the pipeline retries once, then falls back to
AlphaVantageClient.
"""

from __future__ import annotations

import logging
import time
from datetime import date

import yfinance as yf

from influence_monitor.market_data.base import (
    DataFreshnessError,
    DataUnavailableError,
    MarketDataClient,
)

logger = logging.getLogger(__name__)

_RETRY_DELAY_SECONDS = 60


class YFinanceClient(MarketDataClient):
    """yfinance-backed market data client with freshness assertion.

    Usage::

        client = YFinanceClient()
        price = client.fetch_close("AAPL", date.today())
    """

    def fetch_open(self, ticker: str, target_date: date) -> float:
        ohlcv = self.fetch_ohlcv(ticker, target_date)
        return ohlcv["open"]  # type: ignore[return-value]

    def fetch_close(self, ticker: str, target_date: date) -> float:
        ohlcv = self.fetch_ohlcv(ticker, target_date)
        return ohlcv["close"]  # type: ignore[return-value]

    def fetch_ohlcv(self, ticker: str, target_date: date) -> dict[str, float | int | None]:
        """Fetch OHLCV with freshness assertion.

        Raises:
            DataUnavailableError: Empty response from yfinance.
            DataFreshnessError: Data date does not match target_date.
        """
        hist = yf.Ticker(ticker).history(period="5d")

        if hist is None or hist.empty:
            raise DataUnavailableError(
                f"yfinance returned empty data for {ticker}"
            )

        last_date = hist.index[-1].date()
        if last_date != target_date:
            raise DataFreshnessError(
                f"yfinance returned data for {last_date}, "
                f"expected {target_date} (ticker: {ticker})"
            )

        row = hist.iloc[-1]
        return {
            "open": float(row.get("Open", row.get("open", 0))),
            "high": float(row.get("High", row.get("high", 0))),
            "low": float(row.get("Low", row.get("low", 0))),
            "close": float(row.get("Close", row.get("close", 0))),
            "volume": int(row.get("Volume", row.get("volume", 0))),
        }

    def fetch_batch_close(
        self, tickers: list[str], target_date: date,
    ) -> dict[str, float]:
        """Batch-fetch closing prices using yf.download() for efficiency.

        Returns a dict of {ticker: close_price} for tickers where data
        is available and fresh. Tickers with stale or missing data are
        omitted (logged at WARNING).
        """
        if not tickers:
            return {}

        data = yf.download(tickers, period="5d", group_by="ticker", progress=False)

        if data is None or data.empty:
            logger.warning("yf.download returned empty for %s", tickers)
            return {}

        results: dict[str, float] = {}

        for ticker in tickers:
            try:
                if len(tickers) == 1:
                    ticker_data = data
                else:
                    ticker_data = data[ticker]

                if ticker_data.empty:
                    logger.warning("No data for %s in batch download", ticker)
                    continue

                last_date = ticker_data.index[-1].date()
                if last_date != target_date:
                    logger.warning(
                        "Stale data for %s: got %s, expected %s",
                        ticker, last_date, target_date,
                    )
                    continue

                close_col = (
                    ticker_data.get("Close")
                    if "Close" in ticker_data.columns
                    else ticker_data.get("close")
                )
                if close_col is not None and not close_col.empty:
                    results[ticker] = float(close_col.iloc[-1])

            except (KeyError, IndexError, TypeError) as exc:
                logger.warning("Error extracting %s from batch: %s", ticker, exc)

        return results

    def fetch_with_retry(
        self,
        ticker: str,
        target_date: date,
        fallback: MarketDataClient | None = None,
        repo=None,
    ) -> dict[str, float | int | None]:
        """Fetch OHLCV with one retry and optional fallback.

        On DataFreshnessError: retry once after delay, then try fallback.
        Logs fallback usage to api_usage table if repo is provided.
        """
        try:
            return self.fetch_ohlcv(ticker, target_date)
        except (DataFreshnessError, DataUnavailableError) as first_exc:
            logger.info(
                "yfinance failed for %s (%s) — retrying in %ds",
                ticker, first_exc, _RETRY_DELAY_SECONDS,
            )

        time.sleep(_RETRY_DELAY_SECONDS)

        try:
            return self.fetch_ohlcv(ticker, target_date)
        except (DataFreshnessError, DataUnavailableError) as exc:
            if fallback is None:
                raise
            logger.warning(
                "yfinance retry failed for %s (%s) — falling back to %s",
                ticker, exc, type(fallback).__name__,
            )
            return fallback.fetch_ohlcv(ticker, target_date)
