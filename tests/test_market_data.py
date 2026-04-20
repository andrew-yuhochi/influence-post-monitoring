"""Unit tests for market data clients — YFinanceClient and AlphaVantageClient.

Extended in TASK-011 to cover fetch_stock_vol and fetch_spy_return.
"""

from __future__ import annotations

import statistics
from datetime import date, timedelta
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

_TODAY = date(2026, 4, 16)   # Wednesday — real NYSE trading day
_YESTERDAY = date(2026, 4, 15)  # Tuesday — real NYSE trading day


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
    def test_fallback_logs_api_usage(
        self, mock_ticker_cls: MagicMock, mock_sleep: MagicMock,
    ) -> None:
        """When stale data triggers fallback, repo.log_api_usage is called with provider='yfinance_fallback'."""
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = _make_hist_df(_YESTERDAY)  # always stale

        mock_fallback = MagicMock(spec=MarketDataClient)
        mock_fallback.fetch_ohlcv.return_value = {
            "open": 100.0, "high": 107.0, "low": 99.0, "close": 104.0, "volume": 500_000,
        }

        mock_repo = MagicMock()

        client = YFinanceClient()
        client.fetch_with_retry("AAPL", _TODAY, fallback=mock_fallback, repo=mock_repo)

        mock_repo.log_api_usage.assert_called_once_with(
            provider="yfinance_fallback", endpoint="AAPL"
        )

    @patch("influence_monitor.market_data.yfinance_client.time.sleep")
    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_fallback_no_repo_does_not_raise(
        self, mock_ticker_cls: MagicMock, mock_sleep: MagicMock,
    ) -> None:
        """When repo=None and fallback fires, no AttributeError is raised."""
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = _make_hist_df(_YESTERDAY)

        mock_fallback = MagicMock(spec=MarketDataClient)
        mock_fallback.fetch_ohlcv.return_value = {
            "open": 100.0, "high": 107.0, "low": 99.0, "close": 104.0, "volume": 500_000,
        }

        client = YFinanceClient()
        result = client.fetch_with_retry("AAPL", _TODAY, fallback=mock_fallback, repo=None)
        assert result["close"] == 104.0

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
            MarketDataClient()  # type: ignore[abstract]

    def test_yfinance_is_subclass(self) -> None:
        assert issubclass(YFinanceClient, MarketDataClient)

    def test_alpha_vantage_is_subclass(self) -> None:
        assert issubclass(AlphaVantageClient, MarketDataClient)

    def test_abc_has_fetch_stock_vol(self) -> None:
        assert hasattr(MarketDataClient, "fetch_stock_vol")

    def test_abc_has_fetch_spy_return(self) -> None:
        assert hasattr(MarketDataClient, "fetch_spy_return")


# ------------------------------------------------------------------
# YFinanceClient — fetch_stock_vol
# ------------------------------------------------------------------

def _make_vol_hist_df(dates: list[date], closes: list[float]) -> pd.DataFrame:
    """Create a multi-row history DataFrame for vol tests."""
    idx = pd.DatetimeIndex([pd.Timestamp(d) for d in dates])
    opens = [c * 0.99 for c in closes]
    return pd.DataFrame(
        {
            "Open": opens,
            "High": [c * 1.01 for c in closes],
            "Low": [c * 0.98 for c in closes],
            "Close": closes,
            "Volume": [1_000_000] * len(closes),
        },
        index=idx,
    )


class TestYFinanceFetchStockVol:
    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_vol_returns_stdev_for_lookback_20(self, mock_ticker_cls: MagicMock) -> None:
        """fetch_stock_vol with lookback_days=20 returns a positive daily-return stdev."""
        # Generate 21 consecutive weekday dates starting 2026-01-02.
        # Jan 1 is NYSE holiday; 2 Jan is the first trading day.
        # MLK Day is Jan 19 — we go past it so we have 21 dates regardless.
        target = date(2026, 1, 30)
        trading_days: list[date] = []
        d = date(2026, 1, 2)
        while len(trading_days) < 21:
            if d.weekday() < 5:
                trading_days.append(d)
            d += timedelta(days=1)
        # Prices: start at 100, vary slightly so stdev is non-trivial.
        closes = [100.0 + i + (i % 3) * 0.5 for i in range(len(trading_days))]
        hist_df = _make_vol_hist_df(trading_days, closes)

        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = hist_df

        client = YFinanceClient()
        result = client.fetch_stock_vol("AAPL", target, lookback_days=20)

        assert result is not None
        # Expected stdev computed by replicating the implementation's window selection:
        # TradingCalendar excludes MLK Day (Jan 19) and uses Jan 20 instead.
        # The 20-day window drawn from available mock dates yields stdev = 0.006238130914401764.
        expected_stdev = 0.006238130914401764
        assert abs(result - expected_stdev) < 1e-9

    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_vol_respects_non_default_lookback(self, mock_ticker_cls: MagicMock) -> None:
        """fetch_stock_vol with lookback_days=10 uses only the 10 most recent days.

        Price series: first 6 closes are highly volatile, last 10 are very stable.
        This ensures lookback_days=10 (late, low-vol window) and lookback_days=15
        (spanning into the high-vol early period) produce measurably different stdevs.
        """
        target = date(2026, 1, 30)
        trading_days: list[date] = []
        d = date(2026, 1, 2)
        while len(trading_days) < 16:
            if d.weekday() < 5:
                trading_days.append(d)
            d += timedelta(days=1)
        # First 6: high volatility. Last 10: very stable (near-flat).
        closes = (
            [100.0, 115.0, 95.0, 120.0, 90.0, 110.0]
            + [100.0, 100.2, 99.8, 100.1, 100.0, 99.9, 100.1, 100.0, 99.9, 100.1]
        )
        hist_df = _make_vol_hist_df(trading_days, closes)

        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = hist_df

        client = YFinanceClient()
        result_10 = client.fetch_stock_vol("AAPL", target, lookback_days=10)
        result_15 = client.fetch_stock_vol("AAPL", target, lookback_days=15)

        assert result_10 is not None
        assert result_15 is not None
        assert result_10 > 0.0
        assert result_15 > 0.0
        # lookback=10 covers only the stable tail; lookback=15 reaches into the
        # high-vol early period — the two stdevs must be distinctly different.
        assert result_10 != result_15

    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_vol_returns_none_on_empty_response(self, mock_ticker_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = pd.DataFrame()

        client = YFinanceClient()
        result = client.fetch_stock_vol("AAPL", date(2026, 1, 30), lookback_days=20)
        assert result is None

    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_vol_returns_none_on_single_row(self, mock_ticker_cls: MagicMock) -> None:
        """Only 1 close price → cannot compute stdev → return None."""
        single_day_df = _make_vol_hist_df([date(2026, 1, 30)], [200.0])
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = single_day_df

        client = YFinanceClient()
        result = client.fetch_stock_vol("AAPL", date(2026, 1, 30), lookback_days=20)
        assert result is None


# ------------------------------------------------------------------
# YFinanceClient — fetch_spy_return
# ------------------------------------------------------------------

class TestYFinanceFetchSpyReturn:
    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_spy_return_positive(self, mock_ticker_cls: MagicMock) -> None:
        """fetch_spy_return returns (today - prev) / prev correctly."""
        # target_date = 2026-04-16 (Wednesday); prev trading day = 2026-04-15.
        target = date(2026, 4, 16)
        prev_day = date(2026, 4, 15)
        hist_df = _make_vol_hist_df(
            [prev_day, target],
            [500.0, 510.0],
        )
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = hist_df

        client = YFinanceClient()
        result = client.fetch_spy_return(target)

        assert result is not None
        assert abs(result - (510.0 - 500.0) / 500.0) < 1e-9

    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_spy_return_negative(self, mock_ticker_cls: MagicMock) -> None:
        target = date(2026, 4, 16)
        prev_day = date(2026, 4, 15)
        hist_df = _make_vol_hist_df([prev_day, target], [500.0, 490.0])

        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = hist_df

        client = YFinanceClient()
        result = client.fetch_spy_return(target)

        assert result is not None
        assert abs(result - (490.0 - 500.0) / 500.0) < 1e-9
        assert result < 0.0

    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_spy_return_none_on_empty(self, mock_ticker_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = pd.DataFrame()

        client = YFinanceClient()
        result = client.fetch_spy_return(date(2026, 4, 16))
        assert result is None

    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_spy_return_weekend_crossing(self, mock_ticker_cls: MagicMock) -> None:
        """Monday target date → prev trading day is the preceding Friday."""
        # 2026-04-13 is Monday; 2026-04-10 is Friday.
        target = date(2026, 4, 13)
        friday = date(2026, 4, 10)
        hist_df = _make_vol_hist_df([friday, target], [502.0, 507.0])

        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = hist_df

        client = YFinanceClient()
        result = client.fetch_spy_return(target)

        assert result is not None
        assert abs(result - (507.0 - 502.0) / 502.0) < 1e-9

    @patch("influence_monitor.market_data.yfinance_client.yf.Ticker")
    def test_spy_return_good_friday_crossing(self, mock_ticker_cls: MagicMock) -> None:
        """Good Friday 2026 is April 3; the next trading day is April 6 (Monday).
        SPY return for April 6 should use April 2 (Thursday) close as prev."""
        good_friday = date(2026, 4, 3)   # NYSE closed
        target = date(2026, 4, 6)         # Monday after Good Friday
        thursday = date(2026, 4, 2)       # Last trading day before Good Friday
        # The calendar skips Good Friday and Saturday — prev of April 6 is April 2.
        hist_df = _make_vol_hist_df([thursday, target], [490.0, 495.0])

        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.history.return_value = hist_df

        client = YFinanceClient()
        result = client.fetch_spy_return(target)

        assert result is not None
        assert abs(result - (495.0 - 490.0) / 490.0) < 1e-9


# ------------------------------------------------------------------
# AlphaVantageClient — fetch_stock_vol
# ------------------------------------------------------------------

class TestAlphaVantageFetchStockVol:
    @patch("influence_monitor.market_data.alpha_vantage_client.httpx.get")
    def test_vol_computed_from_time_series(self, mock_get: MagicMock) -> None:
        """AlphaVantageClient.fetch_stock_vol computes stdev from TIME_SERIES_DAILY."""
        # Build a mock response with 5 consecutive trading days (Mon-Fri, Jan 5-9 2026).
        closes = [100.0, 102.0, 101.0, 103.5, 104.0]
        trading_days = [
            date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7),
            date(2026, 1, 8), date(2026, 1, 9),
        ]
        time_series = {
            d.isoformat(): {
                "1. open": str(c * 0.99),
                "2. high": str(c * 1.01),
                "3. low": str(c * 0.98),
                "4. close": str(c),
                "5. volume": "1000000",
            }
            for d, c in zip(trading_days, closes)
        }

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Time Series (Daily)": time_series}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        settings = Settings(alpha_vantage_api_key="test-key")
        client = AlphaVantageClient(settings)
        result = client.fetch_stock_vol("AAPL", date(2026, 1, 9), lookback_days=4)

        assert result is not None
        assert result > 0.0

    @patch("influence_monitor.market_data.alpha_vantage_client.httpx.get")
    def test_vol_returns_none_on_empty_series(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Time Series (Daily)": {}}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        settings = Settings(alpha_vantage_api_key="test-key")
        client = AlphaVantageClient(settings)
        result = client.fetch_stock_vol("AAPL", date(2026, 1, 9), lookback_days=20)
        assert result is None


# ------------------------------------------------------------------
# AlphaVantageClient — fetch_spy_return
# ------------------------------------------------------------------

class TestAlphaVantageFetchSpyReturn:
    @patch("influence_monitor.market_data.alpha_vantage_client.httpx.get")
    def test_spy_return_from_change_percent(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "Global Quote": {
                "01. symbol": "SPY",
                "05. price": "510.00",
                "07. latest trading day": "2026-04-16",
                "09. change": "10.00",
                "10. change percent": "2.0000%",
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        settings = Settings(alpha_vantage_api_key="test-key")
        client = AlphaVantageClient(settings)
        result = client.fetch_spy_return(date(2026, 4, 16))

        assert result is not None
        assert abs(result - 0.02) < 1e-9

    @patch("influence_monitor.market_data.alpha_vantage_client.httpx.get")
    def test_spy_return_none_on_stale_data(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "Global Quote": {
                "05. price": "500.00",
                "07. latest trading day": "2026-04-15",  # yesterday
                "10. change percent": "1.0000%",
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        settings = Settings(alpha_vantage_api_key="test-key")
        client = AlphaVantageClient(settings)
        result = client.fetch_spy_return(date(2026, 4, 16))
        assert result is None

    @patch("influence_monitor.market_data.alpha_vantage_client.httpx.get")
    def test_spy_return_none_on_empty_quote(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Global Quote": {}}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        settings = Settings(alpha_vantage_api_key="test-key")
        client = AlphaVantageClient(settings)
        result = client.fetch_spy_return(date(2026, 4, 16))
        assert result is None
