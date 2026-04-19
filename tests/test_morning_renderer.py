"""Unit tests for render_morning in morning_renderer.py — TASK-002.

Covers five acceptance-criteria scenarios:
1. Full alert (Act Now + Watch both populated)
2. Act Now empty (Watch List populated)
3. Both sections empty
4. Direction flip flag renders ⚠️ Direction changed
5. Conflict flag renders ⚠️ Conflicted — opposing view exists

No real WhatsApp calls are made; Twilio is fully mocked.
"""

from __future__ import annotations

import pytest

from influence_monitor.rendering.morning_renderer import (
    DISCLAIMER,
    MorningSignal,
    _conviction_dots,
    render_morning,
)


def _make_signal(
    ticker: str = "AAPL",
    poster_handle: str = "TestPoster",
    direction: str = "LONG",
    conviction_score: float = 7.5,
    summary: str = "Test summary.",
    views_per_hour: float = 5000.0,
    corroboration_count: int = 1,
    direction_flip: bool = False,
    conflict_group: str = "",
    tier: str = "act_now",
) -> MorningSignal:
    return MorningSignal(
        ticker=ticker,
        poster_handle=poster_handle,
        direction=direction,
        conviction_score=conviction_score,
        summary=summary,
        views_per_hour=views_per_hour,
        corroboration_count=corroboration_count,
        direction_flip=direction_flip,
        conflict_group=conflict_group,
        tier=tier,
    )


# ---------------------------------------------------------------------------
# Conviction dots helper
# ---------------------------------------------------------------------------


class TestConvictionDots:
    def test_zero_score(self) -> None:
        assert _conviction_dots(0.0) == "○○○○○"

    def test_ge_9_five_filled(self) -> None:
        assert _conviction_dots(9.0) == "●●●●●"
        assert _conviction_dots(10.0) == "●●●●●"

    def test_ge_7_four_filled(self) -> None:
        assert _conviction_dots(7.0) == "●●●●○"
        assert _conviction_dots(8.9) == "●●●●○"

    def test_ge_5_three_filled(self) -> None:
        assert _conviction_dots(5.0) == "●●●○○"
        assert _conviction_dots(6.9) == "●●●○○"

    def test_ge_3_two_filled(self) -> None:
        assert _conviction_dots(3.0) == "●●○○○"
        assert _conviction_dots(4.9) == "●●○○○"

    def test_ge_1_one_filled(self) -> None:
        assert _conviction_dots(1.0) == "●○○○○"
        assert _conviction_dots(2.9) == "●○○○○"


# ---------------------------------------------------------------------------
# Scenario 1: Full alert — Act Now + Watch both populated
# ---------------------------------------------------------------------------


class TestFullAlert:
    def test_both_sections_present(self) -> None:
        act = [_make_signal(ticker="FNMA", tier="act_now", conviction_score=9.2)]
        watch = [_make_signal(ticker="RIVN", tier="watch", views_per_hour=3000.0)]
        result = render_morning(act, watch)

        assert "ACT NOW" in result
        assert "WATCH LIST" in result
        assert "$FNMA" in result
        assert "$RIVN" in result

    def test_act_now_ordered_by_conviction_desc(self) -> None:
        act = [
            _make_signal(ticker="LOW", conviction_score=5.0),
            _make_signal(ticker="HIGH", conviction_score=9.0),
            _make_signal(ticker="MID", conviction_score=7.0),
        ]
        result = render_morning(act, [])
        high_pos = result.index("$HIGH")
        mid_pos = result.index("$MID")
        low_pos = result.index("$LOW")
        assert high_pos < mid_pos < low_pos

    def test_watch_ordered_by_views_per_hour_desc(self) -> None:
        watch = [
            _make_signal(ticker="SLOW", tier="watch", views_per_hour=1000.0),
            _make_signal(ticker="FAST", tier="watch", views_per_hour=9000.0),
        ]
        result = render_morning([], watch)
        fast_pos = result.index("$FAST")
        slow_pos = result.index("$SLOW")
        assert fast_pos < slow_pos

    def test_act_now_capped_at_five(self) -> None:
        act = [_make_signal(ticker=f"TK{i}", conviction_score=float(10 - i)) for i in range(8)]
        result = render_morning(act, [])
        # Only first 5 tickers should appear
        for i in range(5):
            assert f"$TK{i}" in result
        for i in range(5, 8):
            assert f"$TK{i}" not in result

    def test_watch_capped_at_five(self) -> None:
        watch = [
            _make_signal(ticker=f"WK{i}", tier="watch", views_per_hour=float(9000 - i * 100))
            for i in range(8)
        ]
        result = render_morning([], watch)
        for i in range(5):
            assert f"$WK{i}" in result
        for i in range(5, 8):
            assert f"$WK{i}" not in result

    def test_disclaimer_present(self) -> None:
        act = [_make_signal()]
        result = render_morning(act, [])
        assert DISCLAIMER in result

    def test_corroboration_tag_when_count_ge_2(self) -> None:
        act = [_make_signal(corroboration_count=3)]
        result = render_morning(act, [])
        assert "CORROBORATED — 3 posters" in result

    def test_no_corroboration_tag_when_count_1(self) -> None:
        act = [_make_signal(corroboration_count=1)]
        result = render_morning(act, [])
        assert "CORROBORATED" not in result

    def test_watch_shows_velocity(self) -> None:
        watch = [_make_signal(tier="watch", views_per_hour=4200.0)]
        result = render_morning([], watch)
        assert "4,200 views/hr" in result

    def test_output_under_4000_chars_with_10_signals(self) -> None:
        long_summary = "X" * 200
        act = [
            _make_signal(ticker=f"AC{i}", summary=long_summary, conviction_score=float(9 - i))
            for i in range(5)
        ]
        watch = [
            _make_signal(ticker=f"WC{i}", tier="watch", summary=long_summary, views_per_hour=float(5000 - i * 100))
            for i in range(5)
        ]
        result = render_morning(act, watch)
        assert len(result) <= 4000


# ---------------------------------------------------------------------------
# Scenario 2: Act Now empty, Watch populated
# ---------------------------------------------------------------------------


class TestActNowEmpty:
    def test_no_high_conviction_message_shown(self) -> None:
        watch = [_make_signal(tier="watch")]
        result = render_morning([], watch)
        assert "No high-conviction signals" in result

    def test_watch_section_still_renders(self) -> None:
        watch = [_make_signal(ticker="NOVA", tier="watch")]
        result = render_morning([], watch)
        assert "$NOVA" in result
        assert "WATCH LIST" in result

    def test_disclaimer_still_present(self) -> None:
        watch = [_make_signal(tier="watch")]
        result = render_morning([], watch)
        assert DISCLAIMER in result


# ---------------------------------------------------------------------------
# Scenario 3: Both sections empty
# ---------------------------------------------------------------------------


class TestBothEmpty:
    def test_no_signals_message(self) -> None:
        result = render_morning([], [])
        assert "No signals above threshold" in result
        assert "Nothing actionable overnight" in result

    def test_disclaimer_present_on_empty(self) -> None:
        result = render_morning([], [])
        assert DISCLAIMER in result

    def test_no_section_headers_on_empty(self) -> None:
        result = render_morning([], [])
        assert "ACT NOW" not in result
        assert "WATCH LIST" not in result


# ---------------------------------------------------------------------------
# Scenario 4: Direction flip renders ⚠️ Direction changed
# ---------------------------------------------------------------------------


class TestDirectionFlip:
    def test_direction_flip_tag_rendered(self) -> None:
        act = [_make_signal(direction_flip=True)]
        result = render_morning(act, [])
        assert "⚠️ Direction changed" in result

    def test_no_flip_tag_when_false(self) -> None:
        act = [_make_signal(direction_flip=False)]
        result = render_morning(act, [])
        assert "Direction changed" not in result


# ---------------------------------------------------------------------------
# Scenario 5: Conflict flag renders ⚠️ Conflicted
# ---------------------------------------------------------------------------


class TestConflictFlag:
    def test_conflicted_tag_rendered(self) -> None:
        act = [_make_signal(conflict_group="opposing_exists")]
        result = render_morning(act, [])
        assert "⚠️ Conflicted — opposing view exists" in result

    def test_no_conflicted_tag_when_empty(self) -> None:
        act = [_make_signal(conflict_group="")]
        result = render_morning(act, [])
        assert "Conflicted" not in result
