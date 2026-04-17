"""Unit tests for CorroborationDetector and SignalAggregator."""

from __future__ import annotations

from datetime import date

import pytest

from influence_monitor.scoring.aggregator import SignalAggregator
from influence_monitor.scoring.corroboration import (
    CorroborationDetector,
    Signal,
)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

_TODAY = date(2026, 4, 17)

_INVESTOR_ACKMAN = 1
_INVESTOR_BURRY = 2
_INVESTOR_ICAHN = 3


def _sig(
    ticker: str = "FNMA",
    direction: str = "LONG",
    investor_id: int = _INVESTOR_ACKMAN,
    composite_score: float = 7.0,
    signal_date: date = _TODAY,
    signal_id: int | None = None,
    post_id: int = 100,
    investor_name: str = "",
) -> Signal:
    """Create a test Signal with sensible defaults."""
    return Signal(
        signal_id=signal_id,
        post_id=post_id,
        investor_id=investor_id,
        ticker=ticker,
        direction=direction,
        signal_date=signal_date,
        composite_score=composite_score,
        investor_name=investor_name,
    )


# ------------------------------------------------------------------
# CorroborationDetector — 3 investors LONG FNMA same day
# ------------------------------------------------------------------


class TestThreeInvestorsCorroborated:
    def test_all_three_have_count_3(self) -> None:
        """3 investors posting LONG FNMA on same day → corroboration_count=3."""
        signals = [
            _sig(investor_id=_INVESTOR_ACKMAN, composite_score=8.0, post_id=1),
            _sig(investor_id=_INVESTOR_BURRY, composite_score=6.0, post_id=2),
            _sig(investor_id=_INVESTOR_ICAHN, composite_score=7.0, post_id=3),
        ]

        detector = CorroborationDetector(corroboration_multiplier=1.5)
        result = detector.detect(signals)

        assert len(result) == 3
        for sig in result:
            assert sig.corroboration_count == 3
            assert sig.corroboration_bonus == 1.5

    def test_final_score_includes_bonus(self) -> None:
        """final_score = composite_score * corroboration_bonus."""
        signals = [
            _sig(investor_id=_INVESTOR_ACKMAN, composite_score=8.0, post_id=1),
            _sig(investor_id=_INVESTOR_BURRY, composite_score=6.0, post_id=2),
        ]

        detector = CorroborationDetector(corroboration_multiplier=1.5)
        detector.detect(signals)

        assert signals[0].final_score == pytest.approx(12.0, abs=0.01)
        assert signals[1].final_score == pytest.approx(9.0, abs=0.01)


# ------------------------------------------------------------------
# CorroborationDetector — opposing directions NOT corroborated
# ------------------------------------------------------------------


class TestOpposingDirections:
    def test_long_and_short_not_corroborated(self) -> None:
        """1 LONG FNMA + 1 SHORT FNMA → no corroboration (directions differ)."""
        signals = [
            _sig(investor_id=_INVESTOR_ACKMAN, direction="LONG", post_id=1),
            _sig(investor_id=_INVESTOR_BURRY, direction="SHORT", post_id=2),
        ]

        detector = CorroborationDetector(corroboration_multiplier=1.5)
        detector.detect(signals)

        for sig in signals:
            assert sig.corroboration_count == 1
            assert sig.corroboration_bonus == 1.0

    def test_final_score_no_bonus_on_opposing(self) -> None:
        signals = [
            _sig(investor_id=_INVESTOR_ACKMAN, direction="LONG",
                 composite_score=7.0, post_id=1),
            _sig(investor_id=_INVESTOR_BURRY, direction="SHORT",
                 composite_score=6.0, post_id=2),
        ]

        detector = CorroborationDetector(corroboration_multiplier=1.5)
        detector.detect(signals)

        assert signals[0].final_score == pytest.approx(7.0, abs=0.01)
        assert signals[1].final_score == pytest.approx(6.0, abs=0.01)


# ------------------------------------------------------------------
# CorroborationDetector — same investor posts twice
# ------------------------------------------------------------------


class TestSameInvestorTwice:
    def test_duplicate_investor_not_corroborated(self) -> None:
        """Same investor posts FNMA twice → corroboration_count=1 (distinct investors only)."""
        signals = [
            _sig(investor_id=_INVESTOR_ACKMAN, composite_score=8.0, post_id=1),
            _sig(investor_id=_INVESTOR_ACKMAN, composite_score=7.5, post_id=2),
        ]

        detector = CorroborationDetector(corroboration_multiplier=1.5)
        detector.detect(signals)

        for sig in signals:
            assert sig.corroboration_count == 1
            assert sig.corroboration_bonus == 1.0


# ------------------------------------------------------------------
# CorroborationDetector — edge cases
# ------------------------------------------------------------------


class TestCorroborationEdgeCases:
    def test_different_dates_not_corroborated(self) -> None:
        """Two investors, same ticker/direction, different dates → no corroboration."""
        signals = [
            _sig(investor_id=_INVESTOR_ACKMAN, signal_date=date(2026, 4, 17), post_id=1),
            _sig(investor_id=_INVESTOR_BURRY, signal_date=date(2026, 4, 18), post_id=2),
        ]

        detector = CorroborationDetector(corroboration_multiplier=1.5)
        detector.detect(signals)

        for sig in signals:
            assert sig.corroboration_count == 1

    def test_different_tickers_not_corroborated(self) -> None:
        """Two investors, same direction/date, different tickers → no corroboration."""
        signals = [
            _sig(investor_id=_INVESTOR_ACKMAN, ticker="FNMA", post_id=1),
            _sig(investor_id=_INVESTOR_BURRY, ticker="AAPL", post_id=2),
        ]

        detector = CorroborationDetector(corroboration_multiplier=1.5)
        detector.detect(signals)

        for sig in signals:
            assert sig.corroboration_count == 1

    def test_empty_signal_list(self) -> None:
        """Empty input returns empty output."""
        detector = CorroborationDetector()
        assert detector.detect([]) == []

    def test_single_signal_no_corroboration(self) -> None:
        """A lone signal gets count=1, bonus=1.0."""
        signals = [_sig()]
        detector = CorroborationDetector()
        detector.detect(signals)

        assert signals[0].corroboration_count == 1
        assert signals[0].corroboration_bonus == 1.0
        assert signals[0].final_score == signals[0].composite_score

    def test_custom_multiplier(self) -> None:
        """Non-default multiplier is applied correctly."""
        signals = [
            _sig(investor_id=_INVESTOR_ACKMAN, composite_score=10.0, post_id=1),
            _sig(investor_id=_INVESTOR_BURRY, composite_score=10.0, post_id=2),
        ]

        detector = CorroborationDetector(corroboration_multiplier=2.0)
        detector.detect(signals)

        assert signals[0].corroboration_bonus == 2.0
        assert signals[0].final_score == pytest.approx(20.0, abs=0.01)

    def test_case_insensitive_ticker_and_direction(self) -> None:
        """Ticker and direction grouping is case-insensitive."""
        signals = [
            _sig(investor_id=_INVESTOR_ACKMAN, ticker="fnma",
                 direction="long", post_id=1),
            _sig(investor_id=_INVESTOR_BURRY, ticker="FNMA",
                 direction="LONG", post_id=2),
        ]

        detector = CorroborationDetector(corroboration_multiplier=1.5)
        detector.detect(signals)

        for sig in signals:
            assert sig.corroboration_count == 2

    def test_mixed_corroborated_and_solo(self) -> None:
        """Some tickers corroborated, others solo, in the same batch."""
        signals = [
            # FNMA corroborated (2 investors)
            _sig(investor_id=_INVESTOR_ACKMAN, ticker="FNMA",
                 composite_score=8.0, post_id=1),
            _sig(investor_id=_INVESTOR_BURRY, ticker="FNMA",
                 composite_score=6.0, post_id=2),
            # AAPL solo
            _sig(investor_id=_INVESTOR_ICAHN, ticker="AAPL",
                 composite_score=7.0, post_id=3),
        ]

        detector = CorroborationDetector(corroboration_multiplier=1.5)
        detector.detect(signals)

        # FNMA signals corroborated
        assert signals[0].corroboration_count == 2
        assert signals[0].corroboration_bonus == 1.5
        assert signals[1].corroboration_count == 2

        # AAPL solo
        assert signals[2].corroboration_count == 1
        assert signals[2].corroboration_bonus == 1.0
        assert signals[2].final_score == pytest.approx(7.0, abs=0.01)


# ------------------------------------------------------------------
# SignalAggregator.rank — basic ranking
# ------------------------------------------------------------------


class TestAggregatorRanking:
    def test_returns_sorted_by_final_score_desc(self) -> None:
        """Signals returned in descending final_score order."""
        signals = [
            _sig(ticker="A", composite_score=3.0, investor_id=1, post_id=1),
            _sig(ticker="B", composite_score=9.0, investor_id=2, post_id=2),
            _sig(ticker="C", composite_score=6.0, investor_id=3, post_id=3),
        ]
        # Set final_score (normally done by detector)
        for s in signals:
            s.final_score = s.composite_score

        agg = SignalAggregator()
        result = agg.rank(signals, top_n=10)

        scores = [s.final_score for s in result]
        assert scores == sorted(scores, reverse=True)
        assert result[0].ticker == "B"
        assert result[1].ticker == "C"
        assert result[2].ticker == "A"

    def test_top_n_limits_output(self) -> None:
        """top_n=10 with 15 signals → returns 10 highest-scored."""
        signals = [
            _sig(ticker=f"T{i:02d}", composite_score=float(i),
                 investor_id=i, post_id=i)
            for i in range(1, 16)
        ]
        for s in signals:
            s.final_score = s.composite_score

        agg = SignalAggregator()
        result = agg.rank(signals, top_n=10)

        assert len(result) == 10
        # Highest score should be T15 (score=15.0)
        assert result[0].ticker == "T15"
        assert result[0].final_score == 15.0
        # Lowest in top-10 should be T06 (score=6.0)
        assert result[-1].ticker == "T06"
        assert result[-1].final_score == 6.0

    def test_fewer_than_top_n_returns_all(self) -> None:
        """3 signals with top_n=10 → returns all 3."""
        signals = [
            _sig(ticker=f"T{i}", composite_score=float(i),
                 investor_id=i, post_id=i)
            for i in range(1, 4)
        ]
        for s in signals:
            s.final_score = s.composite_score

        agg = SignalAggregator()
        result = agg.rank(signals, top_n=10)
        assert len(result) == 3


# ------------------------------------------------------------------
# SignalAggregator.rank — deduplication
# ------------------------------------------------------------------


class TestAggregatorDeduplication:
    def test_keeps_highest_score_per_ticker(self) -> None:
        """Two signals for same ticker → only the highest-scored kept."""
        signals = [
            _sig(ticker="FNMA", composite_score=8.0, investor_id=1, post_id=1),
            _sig(ticker="FNMA", composite_score=6.0, investor_id=2, post_id=2),
            _sig(ticker="AAPL", composite_score=5.0, investor_id=3, post_id=3),
        ]
        for s in signals:
            s.final_score = s.composite_score

        agg = SignalAggregator()
        result = agg.rank(signals, top_n=10)

        tickers = [s.ticker for s in result]
        assert tickers.count("FNMA") == 1
        fnma_sig = [s for s in result if s.ticker == "FNMA"][0]
        assert fnma_sig.final_score == 8.0

    def test_dedup_then_top_n(self) -> None:
        """Deduplication happens before top_n truncation."""
        # 6 signals across 3 tickers (2 signals each)
        signals = [
            _sig(ticker="A", composite_score=10.0, investor_id=1, post_id=1),
            _sig(ticker="A", composite_score=5.0, investor_id=2, post_id=2),
            _sig(ticker="B", composite_score=8.0, investor_id=3, post_id=3),
            _sig(ticker="B", composite_score=3.0, investor_id=4, post_id=4),
            _sig(ticker="C", composite_score=6.0, investor_id=5, post_id=5),
            _sig(ticker="C", composite_score=2.0, investor_id=6, post_id=6),
        ]
        for s in signals:
            s.final_score = s.composite_score

        agg = SignalAggregator()
        result = agg.rank(signals, top_n=2)

        # 3 unique tickers → top 2 by score: A(10), B(8)
        assert len(result) == 2
        assert result[0].ticker == "A"
        assert result[1].ticker == "B"

    def test_case_insensitive_dedup(self) -> None:
        """Ticker deduplication is case-insensitive."""
        signals = [
            _sig(ticker="fnma", composite_score=8.0, investor_id=1, post_id=1),
            _sig(ticker="FNMA", composite_score=6.0, investor_id=2, post_id=2),
        ]
        for s in signals:
            s.final_score = s.composite_score

        agg = SignalAggregator()
        result = agg.rank(signals, top_n=10)

        assert len(result) == 1
        assert result[0].final_score == 8.0

    def test_empty_list(self) -> None:
        agg = SignalAggregator()
        assert agg.rank([], top_n=10) == []


# ------------------------------------------------------------------
# End-to-end: detect → rank
# ------------------------------------------------------------------


class TestDetectThenRank:
    def test_corroborated_signals_rank_higher(self) -> None:
        """Corroborated signals' boosted final_score elevates their rank."""
        signals = [
            # FNMA: 2 investors, will be corroborated (bonus 1.5×)
            _sig(ticker="FNMA", investor_id=_INVESTOR_ACKMAN,
                 composite_score=5.0, post_id=1),
            _sig(ticker="FNMA", investor_id=_INVESTOR_BURRY,
                 composite_score=4.0, post_id=2),
            # AAPL: solo, higher base score but no bonus
            _sig(ticker="AAPL", investor_id=_INVESTOR_ICAHN,
                 composite_score=7.0, post_id=3),
        ]

        detector = CorroborationDetector(corroboration_multiplier=1.5)
        detector.detect(signals)

        agg = SignalAggregator()
        result = agg.rank(signals, top_n=10)

        # FNMA best: 5.0 * 1.5 = 7.5 > AAPL: 7.0 * 1.0 = 7.0
        assert result[0].ticker == "FNMA"
        assert result[0].final_score == pytest.approx(7.5, abs=0.01)
        assert result[1].ticker == "AAPL"
        assert result[1].final_score == pytest.approx(7.0, abs=0.01)
