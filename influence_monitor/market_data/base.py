"""Market data client interface and custom exceptions.

Defines the MarketDataClient ABC for fetching OHLC price data and the
error types that drive the fallback chain (yfinance → Alpha Vantage).

Extended in TASK-011 to add fetch_stock_vol and fetch_spy_return abstract
methods, required for the OutcomeEngine excess-return computation (TASK-012).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date


class DataFreshnessError(Exception):
    """Raised when fetched price data is stale (wrong date)."""


class DataUnavailableError(Exception):
    """Raised when no price data is returned at all (empty response)."""


class MarketDataClient(ABC):
    """Abstract contract for daily OHLC price fetches.

    Implementations: YFinanceClient (primary), AlphaVantageClient (fallback).
    """

    @abstractmethod
    def fetch_open(self, ticker: str, target_date: date) -> float:
        """Fetch the opening price for *ticker* on *target_date*."""
        ...

    @abstractmethod
    def fetch_close(self, ticker: str, target_date: date) -> float:
        """Fetch the closing price for *ticker* on *target_date*."""
        ...

    @abstractmethod
    def fetch_ohlcv(self, ticker: str, target_date: date) -> dict[str, float | int | None]:
        """Fetch full OHLCV data for *ticker* on *target_date*.

        Returns dict with keys: open, high, low, close, volume.
        """
        ...

    @abstractmethod
    def fetch_stock_vol(
        self, ticker: str, target_date: date, lookback_days: int,
    ) -> float | None:
        """Compute daily-return stdev over *lookback_days* trading days ending at *target_date*.

        Uses TradingCalendar.trading_days_between for date alignment so weekends
        and NYSE holidays are excluded.  The result is a daily (not annualised) stdev.

        Args:
            ticker: Equity ticker symbol.
            target_date: The last day of the lookback window (inclusive).
            lookback_days: Number of prior trading days to include. This is a
                required parameter — callers read scoring_config.vol_lookback_days
                and pass it in; no default is applied by this interface.

        Returns:
            Daily stdev of close-to-close returns, or None when fewer than 2
            observations are available (logged at WARNING by the implementation).
        """
        ...

    @abstractmethod
    def fetch_spy_return(self, target_date: date) -> float | None:
        """Compute the SPY day-over-day return for *target_date*.

        Formula: (today_close - prev_close) / prev_close where prev_close is
        SPY's close on the previous NYSE trading day (resolved via TradingCalendar).

        Returns:
            Fractional return (e.g. 0.012 for +1.2 %), or None on fetch failure
            (logged at WARNING by the implementation).
        """
        ...
