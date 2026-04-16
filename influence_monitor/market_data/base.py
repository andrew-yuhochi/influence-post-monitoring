"""Market data client interface and custom exceptions.

Defines the MarketDataClient ABC for fetching OHLC price data and the
error types that drive the fallback chain (yfinance → Alpha Vantage).
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
