"""yfinance market data client with freshness assertion.

Primary OHLC data source. Every fetch asserts that the returned data
matches the requested date — yfinance's most dangerous failure mode is
returning stale data silently without raising an error.

On DataFreshnessError: the pipeline retries once, then falls back to
AlphaVantageClient.

Extended in TASK-011: adds fetch_stock_vol (daily-return stdev over a
configurable lookback window) and fetch_spy_return (SPY day-over-day return).
"""

from __future__ import annotations

import logging
import statistics
import time
from datetime import date, timedelta

import yfinance as yf

from influence_monitor.market_data.base import (
    DataFreshnessError,
    DataUnavailableError,
    MarketDataClient,
)
from influence_monitor.market_data.trading_calendar import TradingCalendar

logger = logging.getLogger(__name__)

_RETRY_DELAY_SECONDS = 60
_SPY_TICKER = "SPY"

# Module-level TradingCalendar instance (shared across calls, one-time load cost).
_trading_calendar = TradingCalendar()


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

    def fetch_stock_vol(
        self, ticker: str, target_date: date, lookback_days: int,
    ) -> float | None:
        """Compute daily-return stdev over *lookback_days* trading days ending at *target_date*.

        Uses TradingCalendar.trading_days_between to resolve the exact trading-day
        window so weekends and NYSE holidays are excluded from the count.

        The stdev is daily (not annualised).  The excess-return computation in
        OutcomeEngine divides an overnight daily return by this daily vol, yielding
        a dimensionless signal (daily_return / daily_vol).

        Args:
            ticker: Equity ticker symbol.
            target_date: Last day of the lookback window (inclusive).
            lookback_days: Number of prior trading days; required parameter —
                           the caller reads scoring_config.vol_lookback_days.

        Returns:
            Daily stdev of close-to-close returns, or None when fewer than 2
            observations are available (e.g. brand-new ticker, API outage).
        """
        # Build a wide download window: lookback_days trading days before target_date.
        # We over-fetch by asking yfinance for (lookback_days + 10) calendar days,
        # then trim to the exact trading days using TradingCalendar.
        buffer_days = lookback_days + 20  # extra calendar days to cover holidays/weekends
        fetch_start = target_date - timedelta(days=buffer_days)

        try:
            hist = yf.Ticker(ticker).history(
                start=fetch_start.isoformat(),
                end=(target_date + timedelta(days=1)).isoformat(),  # end is exclusive
            )
        except Exception as exc:
            logger.warning(
                "fetch_stock_vol: yfinance download failed for %s target=%s: %s",
                ticker, target_date, exc,
            )
            return None

        if hist is None or hist.empty:
            logger.warning(
                "fetch_stock_vol: empty response for %s target=%s lookback=%d",
                ticker, target_date, lookback_days,
            )
            return None

        # Determine the trading-day window using the authoritative calendar.
        # We want *lookback_days* trading days ending at target_date (inclusive).
        trading_days_in_range = _trading_calendar.trading_days_between(
            fetch_start, target_date,
        )
        window_trading_days = trading_days_in_range[-lookback_days:] if len(trading_days_in_range) >= lookback_days else trading_days_in_range

        if len(window_trading_days) < 2:
            logger.warning(
                "fetch_stock_vol: insufficient trading days for %s (got %d, need >=2); lookback=%d target=%s",
                ticker, len(window_trading_days), lookback_days, target_date,
            )
            return None

        # Align hist to only the rows within our window.
        hist_dates = {ts.date() for ts in hist.index}
        available_days = [d for d in window_trading_days if d in hist_dates]

        if len(available_days) < 2:
            logger.warning(
                "fetch_stock_vol: fewer than 2 data points available for %s "
                "(available=%d window=%d) target=%s",
                ticker, len(available_days), len(window_trading_days), target_date,
            )
            return None

        # Extract closing prices in chronological order.
        closes: list[float] = []
        for d in available_days:
            # Match the timestamp whose date equals d.
            mask = [ts.date() == d for ts in hist.index]
            row_idx = [i for i, m in enumerate(mask) if m]
            if row_idx:
                close_val = hist.iloc[row_idx[0]].get("Close", hist.iloc[row_idx[0]].get("close"))
                if close_val is not None:
                    closes.append(float(close_val))

        if len(closes) < 2:
            logger.warning(
                "fetch_stock_vol: fewer than 2 valid close prices for %s target=%s",
                ticker, target_date,
            )
            return None

        # Compute daily close-to-close returns then their stdev.
        returns = [
            (closes[i] - closes[i - 1]) / closes[i - 1]
            for i in range(1, len(closes))
        ]
        vol = statistics.stdev(returns)
        logger.debug(
            "fetch_stock_vol: %s target=%s lookback=%d observations=%d vol=%.6f",
            ticker, target_date, lookback_days, len(closes), vol,
        )
        return vol

    def fetch_spy_return(self, target_date: date) -> float | None:
        """Compute the SPY day-over-day return for *target_date*.

        Formula: (today_close - prev_close) / prev_close
        where prev_close is the SPY close on the previous NYSE trading day,
        resolved via TradingCalendar.previous_trading_day.

        Returns:
            Fractional return (e.g. 0.012 for +1.2 %), or None on fetch failure.
        """
        try:
            prev_day = _trading_calendar.previous_trading_day(target_date)
        except ValueError as exc:
            logger.warning("fetch_spy_return: cannot resolve previous trading day for %s: %s", target_date, exc)
            return None

        # Fetch 10 calendar days to cover weekends + a holiday or two.
        fetch_start = prev_day - timedelta(days=5)
        try:
            hist = yf.Ticker(_SPY_TICKER).history(
                start=fetch_start.isoformat(),
                end=(target_date + timedelta(days=1)).isoformat(),
            )
        except Exception as exc:
            logger.warning("fetch_spy_return: yfinance failed for SPY: %s", exc)
            return None

        if hist is None or hist.empty:
            logger.warning("fetch_spy_return: empty SPY response for target=%s", target_date)
            return None

        # Extract closes keyed by date.
        close_by_date: dict[date, float] = {}
        for ts, row in hist.iterrows():
            d = ts.date()
            close_val = row.get("Close", row.get("close"))
            if close_val is not None:
                close_by_date[d] = float(close_val)

        today_close = close_by_date.get(target_date)
        prev_close = close_by_date.get(prev_day)

        if today_close is None:
            logger.warning(
                "fetch_spy_return: no SPY close for target_date=%s", target_date,
            )
            return None
        if prev_close is None:
            logger.warning(
                "fetch_spy_return: no SPY close for prev_day=%s", prev_day,
            )
            return None
        if prev_close == 0.0:
            logger.warning("fetch_spy_return: SPY prev_close is zero for %s", prev_day)
            return None

        spy_return = (today_close - prev_close) / prev_close
        logger.debug(
            "fetch_spy_return: target=%s prev_day=%s today_close=%.4f prev_close=%.4f return=%.6f",
            target_date, prev_day, today_close, prev_close, spy_return,
        )
        return spy_return
