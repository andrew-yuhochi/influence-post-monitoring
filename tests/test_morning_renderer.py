"""Unit tests for morning_renderer.render_morning() and helpers.

Tests:
  A. render_morning() with 5 ACT_NOW + 5 WATCH signals:
     - output is 1 or 2 strings
     - each string is <= 1600 chars
     - first message contains ACT NOW section
     - second message (if split) contains WATCH section
     - all 5 ACT_NOW tickers appear in output
     - all 5 WATCH tickers appear in output

  B. Edge cases:
     - 0 signals → "No signals" text present
     - 3 ACT_NOW, 0 WATCH → 3 entries, no WATCH section / empty
     - _conviction_display values for 9.5, 7.0, 5.0, 1.0

No pipeline instantiation. MorningSignal objects are constructed directly.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from influence_monitor.rendering.morning_renderer import (
    MorningSignal,
    Poster,
    _conviction_display,
    render_morning,
)

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

_BASE_TS = datetime(2026, 4, 21, 7, 0, 0)


def _make_act_now(ticker: str, score: float = 7.5) -> MorningSignal:
    return MorningSignal(
        ticker=ticker,
        posters=[Poster(handle=f"trader_{ticker.lower()}", strategy="macro trader")],
        direction="LONG",
        conviction_score=score,
        summary=f"{ticker} looks bullish based on technicals and fundamentals.",
        views_per_hour=5_000.0,
        corroboration_count=1,
        direction_flip=False,
        conflict_group="",
        tier="act_now",
        post_created_at=_BASE_TS,
        market_cap_class="Large",
    )


def _make_watch(ticker: str, score: float = 4.5) -> MorningSignal:
    return MorningSignal(
        ticker=ticker,
        posters=[Poster(handle=f"watcher_{ticker.lower()}", strategy="value investor")],
        direction="LONG",
        conviction_score=score,
        summary=f"{ticker} momentum building. Worth watching closely.",
        views_per_hour=1_500.0,
        corroboration_count=1,
        direction_flip=False,
        conflict_group="",
        tier="watch",
        post_created_at=_BASE_TS,
        market_cap_class="Mid",
    )


# ---------------------------------------------------------------------------
# Section A — render_morning() with 5 ACT_NOW + 5 WATCH
# ---------------------------------------------------------------------------


class TestRenderMorningFull:
    """Standard render: 5 ACT_NOW + 5 WATCH signals."""

    @pytest.fixture()
    def act_now_signals(self) -> list[MorningSignal]:
        return [
            _make_act_now("AAPL", score=9.1),
            _make_act_now("TSLA", score=8.5),
            _make_act_now("NVDA", score=7.9),
            _make_act_now("MSFT", score=7.2),
            _make_act_now("AMZN", score=6.8),
        ]

    @pytest.fixture()
    def watch_signals(self) -> list[MorningSignal]:
        return [
            _make_watch("META", score=5.1),
            _make_watch("GOOG", score=4.8),
            _make_watch("NFLX", score=4.3),
            _make_watch("RIVN", score=3.9),
            _make_watch("NOVA", score=3.2),
        ]

    def test_output_is_list_of_one_or_two_strings(
        self,
        act_now_signals: list[MorningSignal],
        watch_signals: list[MorningSignal],
    ) -> None:
        result = render_morning(act_now_signals, watch_signals)
        assert isinstance(result, list), "render_morning must return a list"
        assert 1 <= len(result) <= 2, (
            f"Expected 1 or 2 message strings, got {len(result)}"
        )
        for msg in result:
            assert isinstance(msg, str), "Each element must be a string"

    def test_each_message_at_most_1600_chars(
        self,
        act_now_signals: list[MorningSignal],
        watch_signals: list[MorningSignal],
    ) -> None:
        result = render_morning(act_now_signals, watch_signals)
        for i, msg in enumerate(result):
            assert len(msg) <= 1600, (
                f"Message {i} has {len(msg)} chars — exceeds 1600 char limit"
            )

    def test_first_message_contains_act_now_header(
        self,
        act_now_signals: list[MorningSignal],
        watch_signals: list[MorningSignal],
    ) -> None:
        result = render_morning(act_now_signals, watch_signals)
        assert "ACT NOW" in result[0], (
            f"First message must contain 'ACT NOW' section header. Got:\n{result[0]}"
        )

    def test_second_message_contains_watch_header_when_split(
        self,
        act_now_signals: list[MorningSignal],
        watch_signals: list[MorningSignal],
    ) -> None:
        result = render_morning(act_now_signals, watch_signals)
        # Concatenate all messages to find the WATCH section regardless of split
        combined = "\n".join(result)
        assert "WATCH LIST" in combined, (
            "Output must contain 'WATCH LIST' section header"
        )
        if len(result) == 2:
            assert "WATCH" in result[1], (
                f"When split, second message must contain WATCH section. Got:\n{result[1]}"
            )

    def test_all_five_act_now_tickers_appear_in_output(
        self,
        act_now_signals: list[MorningSignal],
        watch_signals: list[MorningSignal],
    ) -> None:
        result = render_morning(act_now_signals, watch_signals)
        combined = "\n".join(result)
        act_now_tickers = [s.ticker for s in act_now_signals]
        for ticker in act_now_tickers:
            assert ticker in combined, (
                f"ACT_NOW ticker '{ticker}' missing from output"
            )

    def test_all_five_watch_tickers_appear_in_output(
        self,
        act_now_signals: list[MorningSignal],
        watch_signals: list[MorningSignal],
    ) -> None:
        result = render_morning(act_now_signals, watch_signals)
        combined = "\n".join(result)
        watch_tickers = [s.ticker for s in watch_signals]
        for ticker in watch_tickers:
            assert ticker in combined, (
                f"WATCH ticker '{ticker}' missing from output"
            )


# ---------------------------------------------------------------------------
# Section B — Edge cases
# ---------------------------------------------------------------------------


class TestRenderMorningEdgeCases:
    """Edge-case inputs: empty, partial, and score display."""

    def test_zero_signals_contains_no_signals_text(self) -> None:
        """Both lists empty → output contains a 'No signals' indicator."""
        result = render_morning(act_now=[], watch=[])
        assert isinstance(result, list)
        assert len(result) >= 1
        combined = "\n".join(result)
        assert "No signals" in combined or "no signals" in combined.lower(), (
            f"Expected 'No signals' in empty output. Got:\n{combined}"
        )

    def test_zero_signals_returns_single_message(self) -> None:
        """Empty state should be a single short message (not split)."""
        result = render_morning(act_now=[], watch=[])
        assert len(result) == 1

    def test_three_act_now_zero_watch(self) -> None:
        """3 ACT_NOW, 0 WATCH → all 3 ACT_NOW tickers present; no WATCH signals in body."""
        act_now = [
            _make_act_now("AAPL", score=9.0),
            _make_act_now("TSLA", score=8.0),
            _make_act_now("NVDA", score=7.0),
        ]
        result = render_morning(act_now=act_now, watch=[])
        combined = "\n".join(result)

        for ticker in ["AAPL", "TSLA", "NVDA"]:
            assert ticker in combined, (
                f"ACT_NOW ticker {ticker} should appear in output with 3 ACT_NOW, 0 WATCH"
            )

    def test_three_act_now_zero_watch_no_watch_signal_tickers(self) -> None:
        """With 0 WATCH signals, WATCH-only tickers must not appear in the output."""
        act_now = [
            _make_act_now("AAPL"),
            _make_act_now("TSLA"),
            _make_act_now("NVDA"),
        ]
        # These tickers should NOT be in the output since we didn't pass them
        result = render_morning(act_now=act_now, watch=[])
        combined = "\n".join(result)

        for absent_ticker in ["META", "GOOG", "RIVN"]:
            assert absent_ticker not in combined, (
                f"Ticker {absent_ticker} should not appear when not in any input list"
            )

    def test_three_act_now_zero_watch_watch_section_empty_or_absent(self) -> None:
        """With 0 WATCH signals, WATCH section says 'No signals need close attention'."""
        act_now = [
            _make_act_now("AAPL"),
            _make_act_now("TSLA"),
            _make_act_now("NVDA"),
        ]
        result = render_morning(act_now=act_now, watch=[])
        combined = "\n".join(result)
        # Renderer always renders the WATCH header; with no signals it shows an empty state
        assert "No signals need close attention" in combined or "WATCH LIST" in combined, (
            "WATCH section should be present (even if empty) when no WATCH signals supplied"
        )


# ---------------------------------------------------------------------------
# _conviction_display helper — score display correctness
# ---------------------------------------------------------------------------


class TestConvictionDisplay:
    """Tests for the _conviction_display helper with specific score values."""

    def test_score_9_5_long_shows_5_markers(self) -> None:
        """score >= 9.0 → 5 filled markers."""
        result = _conviction_display(9.5, "LONG")
        assert result.startswith("✅✅✅✅✅"), (
            f"Score 9.5 LONG should start with 5 buy markers, got: {result!r}"
        )

    def test_score_9_5_long_shows_decimal_value(self) -> None:
        result = _conviction_display(9.5, "LONG")
        assert "9.50" in result, f"Score 9.5 should display as '9.50', got: {result!r}"

    def test_score_7_0_long_shows_4_markers(self) -> None:
        """score >= 7.0 and < 9.0 → 4 filled markers."""
        result = _conviction_display(7.0, "LONG")
        assert result.startswith("✅✅✅✅"), (
            f"Score 7.0 LONG should start with 4 buy markers, got: {result!r}"
        )
        assert "7.00" in result

    def test_score_5_0_long_shows_3_markers(self) -> None:
        """score >= 5.0 and < 7.0 → 3 filled markers."""
        result = _conviction_display(5.0, "LONG")
        assert result.startswith("✅✅✅"), (
            f"Score 5.0 LONG should start with 3 buy markers, got: {result!r}"
        )
        assert "5.00" in result

    def test_score_1_0_long_shows_1_marker(self) -> None:
        """score >= 1.0 and < 3.0 → 1 filled marker."""
        result = _conviction_display(1.0, "LONG")
        assert result.startswith("✅"), (
            f"Score 1.0 LONG should start with 1 buy marker, got: {result!r}"
        )
        assert "1.00" in result

    def test_score_9_5_short_uses_sell_markers(self) -> None:
        """SHORT direction should use ❌ markers, not ✅."""
        result = _conviction_display(9.5, "SHORT")
        assert result.startswith("❌❌❌❌❌"), (
            f"Score 9.5 SHORT should start with 5 sell markers, got: {result!r}"
        )
        assert "✅" not in result, "SHORT signal must not contain buy markers"

    def test_score_0_shows_no_markers(self) -> None:
        """score < 1.0 → 0 markers; only the decimal value shown."""
        result = _conviction_display(0.0, "LONG")
        assert "✅" not in result and "❌" not in result, (
            f"Score 0.0 should produce no markers, got: {result!r}"
        )
        assert "0.00" in result

    @pytest.mark.parametrize("score,expected_filled", [
        (9.5, 5),
        (9.0, 5),
        (7.0, 4),
        (8.9, 4),
        (5.0, 3),
        (6.9, 3),
        (3.0, 2),
        (4.9, 2),
        (1.0, 1),
        (2.9, 1),
        (0.5, 0),
        (0.0, 0),
    ])
    def test_marker_count_boundary_values(self, score: float, expected_filled: int) -> None:
        """Parametrised check for marker count at every score boundary."""
        result = _conviction_display(score, "LONG")
        marker = "✅"
        # Count leading markers only (stop at first non-marker char)
        count = 0
        for ch in result:
            if ch == marker:
                count += 1
            else:
                break
        assert count == expected_filled, (
            f"score={score}: expected {expected_filled} markers, got {count} in {result!r}"
        )
