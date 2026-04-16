"""Unit tests for market data clients — YFinanceClient and AlphaVantageClient."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from influence_monitor.config import Settings
from influence_monitor.market_data.alpha_vantage_client import AlphaVantageClient
from influence_monitor.market_data.base import (
    DataFreshnessError,
    DataUnavailableError,
    MarketDataClient,
)
from influence_monitor.market_data.yfinance_client import YFinanceClient


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

_TODAY = date(2026, 4, 16)
_YESTERDAY = date(2026, 4, 15)


def _make_hist_df(as_of: date, open_: float = 100.0, close: float = 105.0) -> pd.DataFrame:
    """Create a mock yfinance history DataFrame."""
    idx = pd.DatetimeIndex([pd.Timestamp(as_of)])
    return pd.DataFrame(
        {"Open": [open_], "High": [107.0], "Low": [99.0], "Close": [close], "Volume": [1_000_000]},
        index=idx,
    )


def _make_empty_df() -> pd.DataFrame:
    return pd.DataFrame()


# ------------------------------------------------------------------
# YFinanceClient — freshness assertion
# ------------------------------------------------------------------

class TestYFinanceFreshness:
    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_fresh_data_passes(self, mock_ticker_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = _make_hist_df(_TODAY, open_=100.0, close=105.0)

        client = YFinanceClient()
        result = client.fetch_ohlcv("AAPL", _TODAY)

        assert result["open"] == 100.0
        assert result["close"] == 105.0
        assert result["high"] == 107.0
        assert result["low"] == 99.0
        assert result["volume"] == 1_000_000

    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_stale_data_raises_freshness_error(self, mock_ticker_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = _make_hist_df(_YESTERDAY)

        client = YFinanceClient()
        with pytest.raises(DataFreshnessError, match="expected 2026-04-16"):
            client.fetch_ohlcv("AAPL", _TODAY)

    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_empty_response_raises_unavailable(self, mock_ticker_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = _make_empty_df()

        client = YFinanceClient()
        with pytest.raises(DataUnavailableError, match="empty"):
            client.fetch_ohlcv("AAPL", _TODAY)


class TestYFinanceFetchOpenClose:
    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_fetch_open(self, mock_ticker_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = _make_hist_df(_TODAY, open_=250.5)

        client = YFinanceClient()
        assert client.fetch_open("AAPL", _TODAY) == 250.5

    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_fetch_close(self, mock_ticker_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = _make_hist_df(_TODAY, close=260.3)

        client = YFinanceClient()
        assert client.fetch_close("AAPL", _TODAY) == 260.3


# ------------------------------------------------------------------
# YFinanceClient — retry and fallback
# ------------------------------------------------------------------

class TestYFinanceRetryFallback:
    @patch("influence_monitor.market_data.yfinance_client.time.sleep")
    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_stale_then_fresh_on_retry(
        self, mock_ticker_cls: MagicMock, mock_sleep: MagicMock,
    ) -> None:
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        # First call: stale. Second call: fresh.
        mock_ticker.history.side_effect = [
            _make_hist_df(_YESTERDAY),
            _make_hist_df(_TODAY, close=105.0),
        ]

        client = YFinanceClient()
        result = client.fetch_with_retry("AAPL", _TODAY)

        assert result["close"] == 105.0
        mock_sleep.assert_called_once_with(60)

    @patch("influence_monitor.market_data.yfinance_client.time.sleep")
    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_stale_twice_falls_back(
        self, mock_ticker_cls: MagicMock, mock_sleep: MagicMock,
    ) -> None:
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = _make_hist_df(_YESTERDAY)

        mock_fallback = MagicMock(spec=MarketDataClient)
        mock_fallback.fetch_ohlcv.return_value = {
            "open": 100.0, "high": 107.0, "low": 99.0, "close": 104.0, "volume": 500_000,
        }

        client = YFinanceClient()
        result = client.fetch_with_retry("AAPL", _TODAY, fallback=mock_fallback)

        assert result["close"] == 104.0
        mock_fallback.fetch_ohlcv.assert_called_once_with("AAPL", _TODAY)

    @patch("influence_monitor.market_data.yfinance_client.time.sleep")
    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_stale_twice_no_fallback_raises(
        self, mock_ticker_cls: MagicMock, mock_sleep: MagicMock,
    ) -> None:
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = _make_hist_df(_YESTERDAY)

        client = YFinanceClient()
        with pytest.raises(DataFreshnessError):
            client.fetch_with_retry("AAPL", _TODAY, fallback=None)

    @patch("influence_monitor.market_data.yfinance_client.time.sleep")
    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_empty_then_fallback(
        self, mock_ticker_cls: MagicMock, mock_sleep: MagicMock,
    ) -> None:
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = _make_empty_df()

        mock_fallback = MagicMock(spec=MarketDataClient)
        mock_fallback.fetch_ohlcv.return_value = {
            "open": 100.0, "high": 107.0, "low": 99.0, "close": 103.0, "volume": 400_000,
        }

        client = YFinanceClient()
        result = client.fetch_with_retry("AAPL", _TODAY, fallback=mock_fallback)
        assert result["close"] == 103.0


# ------------------------------------------------------------------
# YFinanceClient — batch fetch
# ------------------------------------------------------------------

class TestYFinanceBatchFetch:
    @patch("influence_monitor.market_data.yfinance_client.yf.download")
    def test_batch_close_single_ticker(self, mock_download: MagicMock) -> None:
        idx = pd.DatetimeIndex([pd.Timestamp(_TODAY)])
        mock_download.return_value = pd.DataFrame(
            {"Close": [260.0], "Open": [258.0]}, index=idx,
        )

        client = YFinanceClient()
        result = client.fetch_batch_close(["AAPL"], _TODAY)
        assert result == {"AAPL": 260.0}

    def test_batch_close_empty_list(self) -> None:
        client = YFinanceClient()
        assert client.fetch_batch_close([], _TODAY) == {}


# ------------------------------------------------------------------
# AlphaVantageClient
# ------------------------------------------------------------------

class TestAlphaVantageClient:
    @patch("influence_monitor.market_data.alpha_vantage_client.httpx.get")
    def test_successful_fetch(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "Global Quote": {
                "01. symbol": "AAPL",
                "02. open": "258.10",
                "03. high": "266.50",
                "04. low": "257.00",
                "05. price": "266.43",
                "06. volume": "5000000",
                "07. latest trading day": "2026-04-16",
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        settings = Settings(alpha_vantage_api_key="test-key")
        client = AlphaVantageClient(settings)
        result = client.fetch_ohlcv("AAPL", _TODAY)

        assert result["open"] == 258.10
        assert result["close"] == 266.43
        assert result["high"] == 266.50
        assert result["low"] == 257.00
        assert result["volume"] == 5_000_000

    @patch("influence_monitor.market_data.alpha_vantage_client.httpx.get")
    def test_stale_data_raises(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "Global Quote": {
                "02. open": "258.10",
                "05. price": "266.43",
                "07. latest trading day": "2026-04-15",
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        settings = Settings(alpha_vantage_api_key="test-key")
        client = AlphaVantageClient(settings)
        with pytest.raises(DataFreshnessError, match="2026-04-15"):
            client.fetch_ohlcv("AAPL", _TODAY)

    @patch("influence_monitor.market_data.alpha_vantage_client.httpx.get")
    def test_empty_quote_raises(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Global Quote": {}}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        settings = Settings(alpha_vantage_api_key="test-key")
        client = AlphaVantageClient(settings)
        with pytest.raises(DataUnavailableError, match="no data"):
            client.fetch_ohlcv("AAPL", _TODAY)

    @patch("influence_monitor.market_data.alpha_vantage_client.httpx.get")
    def test_fetch_open_and_close(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "Global Quote": {
                "02. open": "100.50",
                "03. high": "105.00",
                "04. low": "99.00",
                "05. price": "103.75",
                "06. volume": "1000000",
                "07. latest trading day": "2026-04-16",
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        settings = Settings(alpha_vantage_api_key="test-key")
        client = AlphaVantageClient(settings)
        assert client.fetch_open("AAPL", _TODAY) == 100.50
        assert client.fetch_close("AAPL", _TODAY) == 103.75

    def test_api_key_from_settings(self) -> None:
        settings = Settings(alpha_vantage_api_key="av-test-key-123")
        client = AlphaVantageClient(settings)
        assert client._api_key == "av-test-key-123"


# ------------------------------------------------------------------
# ABC contract
# ------------------------------------------------------------------

class TestMarketDataClientABC:
    def test_cannot_instantiate(self) -> None:
        with pytest.raises(TypeError):
            MarketDataClient()

    def test_yfinance_is_subclass(self) -> None:
        assert issubclass(YFinanceClient, MarketDataClient)

    def test_alpha_vantage_is_subclass(self) -> None:
        assert issubclass(AlphaVantageClient, MarketDataClient)
