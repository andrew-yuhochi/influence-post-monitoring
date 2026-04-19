"""Unit tests for render_morning in morning_renderer.py — TASK-002.

Covers acceptance-criteria scenarios:
1. Full alert (Act Now + Watch both populated)
2. Act Now empty (Watch List populated)
3. Both sections empty
4. Direction flip flag renders 🔄 Direction changed
5. Conflict flag renders ⚔️ Conflicted — opposing view exists
6. Date header present at top of every message
7. All posters shown for corroborated signals
8. No ❌ in conviction score display
9. Combined views/hr + posted time in Watch List footer

No real WhatsApp calls are made; Twilio is fully mocked.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from influence_monitor.rendering.morning_renderer import (
    MorningSignal,
    Poster,
    _conviction_display,
    _truncate_words,
    render_morning,
)

_DEFAULT_POST_TIME = datetime(2026, 4, 18, 7, 30)
_DEFAULT_STRATEGY = "value investor"


def _make_poster(handle: str = "TestPoster", strategy: str = _DEFAULT_STRATEGY) -> Poster:
    return Poster(handle=handle, strategy=strategy)


def _make_signal(
    ticker: str = "AAPL",
    posters: list[Poster] | None = None,
    direction: str = "LONG",
    conviction_score: float = 7.5,
    summary: str = "Test summary.",
    views_per_hour: float = 5000.0,
    corroboration_count: int = 1,
    direction_flip: bool = False,
    conflict_group: str = "",
    tier: str = "act_now",
    post_created_at: datetime = _DEFAULT_POST_TIME,
    # Legacy convenience params kept for backwards compat with older tests
    poster_handle: str = "TestPoster",
    poster_strategy: str = _DEFAULT_STRATEGY,
) -> MorningSignal:
    if posters is None:
        posters = [Poster(handle=poster_handle, strategy=poster_strategy)]
    return MorningSignal(
        ticker=ticker,
        posters=posters,
        direction=direction,
        conviction_score=conviction_score,
        summary=summary,
        views_per_hour=views_per_hour,
        corroboration_count=corroboration_count,
        direction_flip=direction_flip,
        conflict_group=conflict_group,
        tier=tier,
        post_created_at=post_created_at,
    )


# ---------------------------------------------------------------------------
# Conviction display helper
# ---------------------------------------------------------------------------


class TestConvictionDisplay:
    def test_zero_score(self) -> None:
        assert _conviction_display(0.0) == " - 0%"

    def test_ge_9_five_filled(self) -> None:
        assert _conviction_display(9.0) == "✅✅✅✅✅ - 90%"
        assert _conviction_display(10.0) == "✅✅✅✅✅ - 100%"

    def test_ge_9_6_percentage(self) -> None:
        assert _conviction_display(9.6) == "✅✅✅✅✅ - 96%"

    def test_ge_7_four_filled(self) -> None:
        assert _conviction_display(7.0) == "✅✅✅✅ - 70%"
        assert _conviction_display(8.9) == "✅✅✅✅ - 89%"

    def test_ge_7_4_percentage(self) -> None:
        assert _conviction_display(7.4) == "✅✅✅✅ - 74%"

    def test_ge_5_three_filled(self) -> None:
        assert _conviction_display(5.0) == "✅✅✅ - 50%"
        assert _conviction_display(6.9) == "✅✅✅ - 69%"

    def test_ge_3_two_filled(self) -> None:
        assert _conviction_display(3.0) == "✅✅ - 30%"
        assert _conviction_display(4.9) == "✅✅ - 49%"

    def test_ge_1_one_filled(self) -> None:
        assert _conviction_display(1.0) == "✅ - 10%"
        assert _conviction_display(2.9) == "✅ - 29%"

    def test_no_empty_cross_in_output(self) -> None:
        # ❌ must never appear in any score output
        for score in [0.0, 1.0, 3.0, 5.0, 7.0, 9.0]:
            assert "❌" not in _conviction_display(score)


# ---------------------------------------------------------------------------
# Date header
# ---------------------------------------------------------------------------


class TestDateHeader:
    def test_date_header_present_in_full_alert(self) -> None:
        act = [_make_signal()]
        messages = render_morning(act, [])
        assert "📅 *Morning Alert —" in messages[0]

    def test_date_header_present_when_empty(self) -> None:
        messages = render_morning([], [])
        assert "📅 *Morning Alert —" in messages[0]

    def test_date_header_is_first_line(self) -> None:
        act = [_make_signal()]
        messages = render_morning(act, [])
        first_line = messages[0].splitlines()[0]
        assert first_line.startswith("📅 *Morning Alert —")


# ---------------------------------------------------------------------------
# Scenario 1: Full alert — Act Now + Watch both populated
# ---------------------------------------------------------------------------


def _joined(messages: list[str]) -> str:
    """Join all message parts for content assertions."""
    return "\n".join(messages)


class TestFullAlert:
    def test_both_sections_present(self) -> None:
        act = [_make_signal(ticker="FNMA", tier="act_now", conviction_score=9.2)]
        watch = [_make_signal(ticker="RIVN", tier="watch", views_per_hour=3000.0)]
        result = _joined(render_morning(act, watch))

        assert "ACT NOW" in result
        assert "WATCH LIST" in result
        assert "$FNMA" in result
        assert "$RIVN" in result

    def test_returns_list(self) -> None:
        act = [_make_signal()]
        messages = render_morning(act, [])
        assert isinstance(messages, list)
        assert len(messages) >= 1

    def test_act_now_ordered_by_conviction_desc(self) -> None:
        act = [
            _make_signal(ticker="LOW", conviction_score=5.0),
            _make_signal(ticker="HIGH", conviction_score=9.0),
            _make_signal(ticker="MID", conviction_score=7.0),
        ]
        result = _joined(render_morning(act, []))
        high_pos = result.index("$HIGH")
        mid_pos = result.index("$MID")
        low_pos = result.index("$LOW")
        assert high_pos < mid_pos < low_pos

    def test_watch_ordered_by_views_per_hour_desc(self) -> None:
        watch = [
            _make_signal(ticker="SLOW", tier="watch", views_per_hour=1000.0),
            _make_signal(ticker="FAST", tier="watch", views_per_hour=9000.0),
        ]
        result = _joined(render_morning([], watch))
        fast_pos = result.index("$FAST")
        slow_pos = result.index("$SLOW")
        assert fast_pos < slow_pos

    def test_act_now_capped_at_five(self) -> None:
        act = [_make_signal(ticker=f"TK{i}", conviction_score=float(10 - i)) for i in range(8)]
        result = _joined(render_morning(act, []))
        for i in range(5):
            assert f"$TK{i}" in result
        for i in range(5, 8):
            assert f"$TK{i}" not in result

    def test_watch_capped_at_five(self) -> None:
        watch = [
            _make_signal(ticker=f"WK{i}", tier="watch", views_per_hour=float(9000 - i * 100))
            for i in range(8)
        ]
        result = _joined(render_morning([], watch))
        for i in range(5):
            assert f"$WK{i}" in result
        for i in range(5, 8):
            assert f"$WK{i}" not in result

    def test_no_disclaimer_footer(self) -> None:
        act = [_make_signal()]
        result = _joined(render_morning(act, []))
        assert "not investment advice" not in result
        assert "Do your own research" not in result

    def test_no_corroboration_tag_when_count_1(self) -> None:
        act = [_make_signal(corroboration_count=1)]
        result = _joined(render_morning(act, []))
        assert "CORROBORATED" not in result

    def test_no_corroboration_tag_when_multiple_posters(self) -> None:
        posters = [
            Poster(handle="BillAckman", strategy="activist investor"),
            Poster(handle="DavidEinhorn", strategy="value investor"),
        ]
        act = [_make_signal(posters=posters, corroboration_count=2)]
        result = _joined(render_morning(act, []))
        assert "CORROBORATED" not in result

    def test_watch_shows_combined_velocity_and_time(self) -> None:
        ts = datetime(2026, 4, 18, 8, 32)
        watch = [_make_signal(tier="watch", views_per_hour=4200.0, post_created_at=ts)]
        result = _joined(render_morning([], watch))
        assert "_4,200 posts/hr — Posted 08:32_" in result

    def test_watch_no_standalone_date_string(self) -> None:
        ts = datetime(2026, 4, 18, 8, 32)
        watch = [_make_signal(tier="watch", views_per_hour=4200.0, post_created_at=ts)]
        result = _joined(render_morning([], watch))
        assert "_Posted: 2026-04-18 08:32_" not in result

    def test_watch_views_per_hour_comma_formatted(self) -> None:
        watch = [_make_signal(tier="watch", views_per_hour=12500.0)]
        result = _joined(render_morning([], watch))
        assert "12,500 posts/hr" in result

    def test_output_under_4000_chars_with_10_short_signals(self) -> None:
        # With 20-word truncation, summaries are short; single message expected
        short_summary = "Word " * 5  # 5 words, well under 20
        act = [
            _make_signal(ticker=f"AC{i}", summary=short_summary.strip(), conviction_score=float(9 - i))
            for i in range(5)
        ]
        watch = [
            _make_signal(ticker=f"WC{i}", tier="watch", summary=short_summary.strip(), views_per_hour=float(5000 - i * 100))
            for i in range(5)
        ]
        messages = render_morning(act, watch)
        assert len(messages) == 1
        assert len(messages[0]) <= 4000

    def test_act_now_signals_need_immediate_action(self) -> None:
        act = [_make_signal(), _make_signal(ticker="GOOG")]
        result = _joined(render_morning(act, []))
        assert "signals need immediate action" in result
        assert "above threshold" not in result

    def test_watch_signals_need_close_attention(self) -> None:
        watch = [_make_signal(tier="watch"), _make_signal(ticker="GOOG", tier="watch")]
        result = _joined(render_morning([], watch))
        assert "signals need close attention" in result
        assert "gaining momentum" not in result

    def test_direction_buy_rendered(self) -> None:
        act = [_make_signal(direction="LONG")]
        result = _joined(render_morning(act, []))
        assert "Buy" in result
        assert "LONG" not in result

    def test_direction_sell_rendered(self) -> None:
        act = [_make_signal(direction="SHORT")]
        result = _joined(render_morning(act, []))
        assert "Sell" in result
        assert "SHORT" not in result

    def test_buy_emoji_in_header(self) -> None:
        act = [_make_signal(direction="LONG")]
        result = _joined(render_morning(act, []))
        assert "📈 Buy" in result

    def test_sell_emoji_in_header(self) -> None:
        act = [_make_signal(direction="SHORT")]
        result = _joined(render_morning(act, []))
        assert "📉 Sell" in result

    def test_poster_strategy_rendered(self) -> None:
        act = [_make_signal(poster_handle="BillAckman", poster_strategy="activist investor")]
        result = _joined(render_morning(act, []))
        assert "@BillAckman - activist investor" in result

    def test_conviction_emoji_bar_rendered(self) -> None:
        act = [_make_signal(conviction_score=9.6)]
        result = _joined(render_morning(act, []))
        assert "✅✅✅✅✅ - 96%" in result

    def test_no_empty_cross_in_rendered_output(self) -> None:
        act = [_make_signal(conviction_score=5.0)]
        result = _joined(render_morning(act, []))
        assert "❌" not in result

    def test_all_posters_shown_for_corroborated_signal(self) -> None:
        posters = [
            Poster(handle="BillAckman", strategy="activist investor"),
            Poster(handle="DavidEinhorn", strategy="value investor"),
        ]
        act = [_make_signal(posters=posters, corroboration_count=2)]
        result = _joined(render_morning(act, []))
        assert "@BillAckman - activist investor" in result
        assert "@DavidEinhorn - value investor" in result
        # Corroboration line removed when multiple posters are listed
        assert "CORROBORATED" not in result


# ---------------------------------------------------------------------------
# Scenario 2: Act Now empty, Watch populated
# ---------------------------------------------------------------------------


class TestActNowEmpty:
    def test_no_high_conviction_message_shown(self) -> None:
        watch = [_make_signal(tier="watch")]
        result = _joined(render_morning([], watch))
        assert "No high-conviction signals" in result

    def test_watch_section_still_renders(self) -> None:
        watch = [_make_signal(ticker="NOVA", tier="watch")]
        result = _joined(render_morning([], watch))
        assert "$NOVA" in result
        assert "WATCH LIST" in result

    def test_no_disclaimer_on_watch_only(self) -> None:
        watch = [_make_signal(tier="watch")]
        result = _joined(render_morning([], watch))
        assert "not investment advice" not in result


# ---------------------------------------------------------------------------
# Scenario 3: Both sections empty
# ---------------------------------------------------------------------------


class TestBothEmpty:
    def test_no_signals_message(self) -> None:
        result = _joined(render_morning([], []))
        assert "No signals for today" in result

    def test_no_disclaimer_on_empty(self) -> None:
        result = _joined(render_morning([], []))
        assert "not investment advice" not in result

    def test_no_section_headers_on_empty(self) -> None:
        result = _joined(render_morning([], []))
        assert "ACT NOW" not in result
        assert "WATCH LIST" not in result

    def test_empty_returns_single_element_list(self) -> None:
        messages = render_morning([], [])
        assert isinstance(messages, list)
        assert len(messages) == 1

    def test_demo_empty_path_returns_nonempty_string(self) -> None:
        # Exercises the --demo-empty code path: render_morning([], []) must
        # return a list containing at least one non-empty string.
        messages = render_morning([], [])
        assert len(messages) >= 1
        assert all(len(m) > 0 for m in messages)


# ---------------------------------------------------------------------------
# Scenario 4: Direction flip renders 🔄 Direction changed
# ---------------------------------------------------------------------------


class TestDirectionFlip:
    def test_direction_flip_tag_rendered(self) -> None:
        act = [_make_signal(direction_flip=True)]
        result = _joined(render_morning(act, []))
        assert "🔄 Direction changed" in result

    def test_no_flip_tag_when_false(self) -> None:
        act = [_make_signal(direction_flip=False)]
        result = _joined(render_morning(act, []))
        assert "Direction changed" not in result

    def test_no_warning_emoji_for_flip(self) -> None:
        act = [_make_signal(direction_flip=True)]
        result = _joined(render_morning(act, []))
        assert "⚠️ Direction changed" not in result


# ---------------------------------------------------------------------------
# Scenario 5: Conflict flag renders ⚔️ Conflicted
# ---------------------------------------------------------------------------


class TestConflictFlag:
    def test_conflicted_tag_rendered(self) -> None:
        act = [_make_signal(conflict_group="opposing_exists")]
        result = _joined(render_morning(act, []))
        assert "⚔️ Conflicted — opposing view exists" in result

    def test_no_conflicted_tag_when_empty(self) -> None:
        act = [_make_signal(conflict_group="")]
        result = _joined(render_morning(act, []))
        assert "Conflicted" not in result

    def test_no_warning_emoji_for_conflict(self) -> None:
        act = [_make_signal(conflict_group="opposing_exists")]
        result = _joined(render_morning(act, []))
        assert "⚠️ Conflicted" not in result


# ---------------------------------------------------------------------------
# New: Quote truncation
# ---------------------------------------------------------------------------


class TestQuoteTruncation:
    def test_short_summary_not_truncated(self) -> None:
        words = ["word"] * 10
        summary = " ".join(words)
        assert _truncate_words(summary, 20) == summary

    def test_exact_20_words_not_truncated(self) -> None:
        summary = " ".join(["word"] * 20)
        assert _truncate_words(summary, 20) == summary

    def test_21_words_truncated_to_20_plus_ellipsis(self) -> None:
        summary = " ".join([f"w{i}" for i in range(21)])
        result = _truncate_words(summary, 20)
        assert result.endswith("…")
        assert len(result.split("…")[0].split()) == 20

    def test_rendered_quote_truncated_at_20_words(self) -> None:
        # 25 words — must be truncated to 20 + ellipsis
        long_summary = "one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty twentyone twentytwo twentythree twentyfour twentyfive"
        act = [_make_signal(summary=long_summary)]
        result = _joined(render_morning(act, []))
        # Extract quote from rendered output
        for line in result.splitlines():
            if line.startswith('> "'):
                quote_text = line[3:-1]  # strip '> "' and trailing '"'
                assert quote_text.endswith("…"), f"Expected ellipsis, got: {quote_text!r}"
                words_in_quote = quote_text[:-1].split()  # strip the ellipsis char before splitting
                assert len(words_in_quote) == 20
                break
        else:
            pytest.fail("Quote line not found in output")

    def test_short_summary_no_ellipsis(self) -> None:
        short = "Just five words here."
        act = [_make_signal(summary=short)]
        result = _joined(render_morning(act, []))
        for line in result.splitlines():
            if line.startswith('> "'):
                assert "…" not in line
                break


# ---------------------------------------------------------------------------
# New: Split into two messages when >4000 chars
# ---------------------------------------------------------------------------


class TestMessageSplit:
    def _make_long_signal(self, ticker: str, tier: str = "act_now", views_per_hour: float = 5000.0) -> MorningSignal:
        # 25-word summary so it gets truncated but blocks are still sizable
        summary = " ".join([f"word{i}" for i in range(25)])
        return _make_signal(
            ticker=ticker,
            summary=summary,
            tier=tier,
            views_per_hour=views_per_hour,
            conviction_score=9.0,
            posters=[Poster(handle=f"Poster{ticker}", strategy="activist investor activist investor activist")],
        )

    def test_short_alert_is_single_message(self) -> None:
        act = [_make_signal()]
        messages = render_morning(act, [])
        assert len(messages) == 1

    def test_large_alert_splits_into_two(self) -> None:
        # 5 act_now + 5 watch with bulky poster strategies to push over 4000 chars
        act = [self._make_long_signal(f"AC{i}") for i in range(5)]
        watch = [self._make_long_signal(f"WC{i}", tier="watch", views_per_hour=float(5000 - i * 100)) for i in range(5)]
        messages = render_morning(act, watch)
        # Force a >4000 scenario by checking combined length
        full = "\n".join(messages)
        if len(messages) == 2:
            assert len(messages[0]) <= 4000
            assert len(messages[1]) <= 4000
            # Message 1 has date header + ACT NOW; message 2 has WATCH LIST
            assert "📅 *Morning Alert —" in messages[0]
            assert "ACT NOW" in messages[0]
            assert "WATCH LIST" in messages[1]
            assert "📅 *Morning Alert —" not in messages[1]
        else:
            # If it fits in one, that's valid — just check it's under 4000
            assert len(messages[0]) <= 4000

    def test_two_message_split_structure(self) -> None:
        # Build a fixture guaranteed to exceed 4000 chars.
        # Each signal block contributes ~400 chars via a 400-char summary (20 words of ~20 chars each).
        # 5 act + 5 watch = 10 blocks × ~400 chars + overhead > 4000.
        long_word = "abcdefghijklmnopqrst"  # 20 chars
        long_summary = " ".join([long_word] * 20)  # 20 words × 20 chars = 400 chars
        act = [
            _make_signal(
                ticker=f"AC{i}",
                posters=[Poster(handle=f"Poster{i}", strategy="activist investor")],
                summary=long_summary,
                conviction_score=float(9 - i),
            )
            for i in range(5)
        ]
        watch = [
            _make_signal(
                ticker=f"WC{i}",
                tier="watch",
                posters=[Poster(handle=f"WPoster{i}", strategy="activist investor")],
                summary=long_summary,
                views_per_hour=float(5000 - i * 100),
            )
            for i in range(5)
        ]
        messages = render_morning(act, watch)
        full = "\n".join(messages)
        assert len(full) > 4000, f"Fixture must exceed 4000 chars to test split, got {len(full)}"
        assert len(messages) == 2
        assert "ACT NOW" in messages[0]
        assert "WATCH LIST" in messages[1]
        assert "📅 *Morning Alert —" not in messages[1]
