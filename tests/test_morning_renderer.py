"""Unit tests for MorningWatchlistRenderer.

Covers the acceptance criteria for TASK-012:
- Signal card format (rank, direction, ticker, index tier, investor,
  quote, track record, strength bars, corroboration flag).
- Track record warm-up (<5 calls → ``building...``).
- Strength bars mapped from composite_score to 5 Unicode levels.
- Corroboration tag rendered only when count >= 2.
- Edge cases: 0 signals, <10 signals, deleted post.
- Subject line convention from UX-SPEC.md.
- Plain-text version stands alone without HTML.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from influence_monitor.config import Settings
from influence_monitor.email.renderer import (
    MorningWatchlistRenderer,
    _format_quote,
    _format_track_record,
    _strength_bars,
)

_TODAY = date(2026, 4, 15)


def _settings() -> Settings:
    return Settings(track_record_min_calls=5)


def _signal_row(
    rank: int = 1,
    ticker: str = "FNMA",
    direction: str = "LONG",
    composite_score: float = 8.1,
    corroboration_count: int = 1,
    index_tier: str = "SP500",
    investor_name: str = "Bill Ackman",
    x_handle: str = "BillAckman",
    total_calls: int = 20,
    total_hits: int = 14,
    post_text: str = "FNMA is egregiously underpriced. Government could monetize its stake.",
    post_deleted: int = 0,
) -> dict[str, Any]:
    return {
        "morning_rank": rank,
        "ticker": ticker,
        "direction": direction,
        "composite_score": composite_score,
        "corroboration_count": corroboration_count,
        "index_tier": index_tier,
        "investor_name": investor_name,
        "x_handle": x_handle,
        "total_calls": total_calls,
        "total_hits": total_hits,
        "post_text": post_text,
        "post_deleted": post_deleted,
    }


# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------


class TestStrengthBars:
    def test_score_zero_shows_one_filled(self) -> None:
        assert _strength_bars(0.0) == "●○○○○"

    def test_score_ten_shows_full(self) -> None:
        assert _strength_bars(10.0) == "●●●●●"

    def test_midrange_score(self) -> None:
        assert _strength_bars(5.5) == "●●●○○"

    def test_scores_map_to_five_levels(self) -> None:
        assert _strength_bars(1.5) == "●○○○○"
        assert _strength_bars(3.5) == "●●○○○"
        assert _strength_bars(5.5) == "●●●○○"
        assert _strength_bars(7.5) == "●●●●○"
        assert _strength_bars(9.5) == "●●●●●"

    def test_above_ten_clamps(self) -> None:
        assert _strength_bars(99.0) == "●●●●●"


class TestFormatTrackRecord:
    def test_warmup_below_threshold(self) -> None:
        assert _format_track_record(total_calls=3, total_hits=2, min_calls=5) == "building..."

    def test_at_threshold_shows_percent(self) -> None:
        assert _format_track_record(total_calls=5, total_hits=3, min_calls=5) == "3/5 (60%)"

    def test_well_above_threshold(self) -> None:
        assert _format_track_record(total_calls=20, total_hits=14, min_calls=5) == "14/20 (70%)"

    def test_zero_calls_shows_warmup(self) -> None:
        assert _format_track_record(0, 0, 5) == "building..."


class TestFormatQuote:
    def test_normal_text(self) -> None:
        text = "FNMA is cheap."
        assert _format_quote(text, post_deleted=0) == '"FNMA is cheap."'

    def test_deleted_post_graceful(self) -> None:
        assert _format_quote("original text", post_deleted=1) == '"[Post no longer available]"'

    def test_deleted_takes_precedence_over_text(self) -> None:
        assert _format_quote("", post_deleted=True) == '"[Post no longer available]"'

    def test_empty_text_placeholder(self) -> None:
        assert _format_quote("", post_deleted=0) == '"[No text available]"'

    def test_long_text_truncates(self) -> None:
        text = "A" * 500
        result = _format_quote(text, post_deleted=0)
        assert result.endswith('..."')
        assert len(result) < 220

    def test_collapses_whitespace(self) -> None:
        text = "FNMA\n\nis    cheap.\n  Really."
        assert _format_quote(text, post_deleted=0) == '"FNMA is cheap. Really."'


# ----------------------------------------------------------------------
# End-to-end rendering
# ----------------------------------------------------------------------


class TestRenderFromRows:
    def test_ten_signal_watchlist(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        signals = [
            _signal_row(
                rank=i + 1,
                ticker=f"TK{i}",
                composite_score=9.0 - i * 0.5,
            )
            for i in range(10)
        ]
        email = renderer.render_from_rows(
            signals, signal_date=_TODAY, accounts_monitored=17
        )

        assert "10 Signals Today" in email.subject
        assert "[Apr 15]" in email.subject
        assert "Top: TK0 (LONG" in email.subject

        assert "10 signals detected" in email.text_body
        assert "0 corroborated" in email.text_body
        assert "10 LONG  0 SHORT" in email.text_body
        assert "#1" in email.text_body and "#10" in email.text_body
        # Footer fewer-than-10 note not shown
        assert "Only" not in email.text_body

    def test_three_signal_fewer_than_ten(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        signals = [
            _signal_row(rank=1, ticker="AAA"),
            _signal_row(rank=2, ticker="BBB", direction="SHORT"),
            _signal_row(rank=3, ticker="CCC"),
        ]
        email = renderer.render_from_rows(
            signals, signal_date=_TODAY, accounts_monitored=17
        )

        assert "3 Signals Today" in email.subject
        assert "3 signals detected" in email.text_body
        assert "2 LONG  1 SHORT" in email.text_body
        assert "Only 3 signals met the threshold overnight." in email.text_body

    def test_zero_signals_no_signal_email(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        email = renderer.render_from_rows(
            [], signal_date=_TODAY, accounts_monitored=17
        )

        assert email.subject == "Influence Monitor — No Signals Overnight [Apr 15]"
        assert "No significant overnight signals" in email.text_body
        assert "Monitored 17 accounts" in email.text_body
        assert "Evening Scorecard" in email.text_body

    def test_corroboration_tag_rendered(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        signals = [_signal_row(corroboration_count=3)]
        email = renderer.render_from_rows(
            signals, signal_date=_TODAY, accounts_monitored=17
        )
        assert "CORROBORATED — 3 posters" in email.text_body

    def test_no_corroboration_tag_when_count_is_one(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        signals = [_signal_row(corroboration_count=1)]
        email = renderer.render_from_rows(
            signals, signal_date=_TODAY, accounts_monitored=17
        )
        assert "CORROBORATED" not in email.text_body

    def test_deleted_post_renders_placeholder(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        signals = [_signal_row(post_deleted=1, post_text="original but deleted")]
        email = renderer.render_from_rows(
            signals, signal_date=_TODAY, accounts_monitored=17
        )
        assert "[Post no longer available]" in email.text_body
        # Original text not leaked
        assert "original but deleted" not in email.text_body

    def test_short_signal_direction_shown(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        signals = [_signal_row(ticker="XYZ", direction="SHORT")]
        email = renderer.render_from_rows(
            signals, signal_date=_TODAY, accounts_monitored=17
        )
        assert "[SHORT]" in email.text_body
        assert "Top: XYZ (SHORT" in email.subject

    def test_track_record_warmup_shown(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        signals = [_signal_row(total_calls=2, total_hits=1)]
        email = renderer.render_from_rows(
            signals, signal_date=_TODAY, accounts_monitored=17
        )
        assert "Track record: building..." in email.text_body

    def test_strength_dots_in_card(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        signals = [_signal_row(composite_score=8.1)]
        email = renderer.render_from_rows(
            signals, signal_date=_TODAY, accounts_monitored=17
        )
        # score 8.1 → ceil(4.05) = 5 filled
        assert "●●●●●" in email.text_body

    def test_index_tier_displayed(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        signals = [_signal_row(index_tier="SP500")]
        email = renderer.render_from_rows(
            signals, signal_date=_TODAY, accounts_monitored=17
        )
        assert "[SP500]" in email.text_body

    def test_html_wraps_text_body(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        signals = [_signal_row()]
        email = renderer.render_from_rows(
            signals, signal_date=_TODAY, accounts_monitored=17
        )
        assert "<pre" in email.html_body
        assert "</pre>" in email.html_body
        assert "FNMA" in email.html_body

    def test_html_escapes_user_content(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        # Inject HTML into quote text — must be escaped in HTML output
        signals = [_signal_row(post_text="<script>alert('xss')</script>")]
        email = renderer.render_from_rows(
            signals, signal_date=_TODAY, accounts_monitored=17
        )
        assert "<script>" not in email.html_body
        assert "&lt;script&gt;" in email.html_body

    def test_plain_text_has_full_information(self) -> None:
        """Plain text must stand alone — no information loss vs. HTML."""
        renderer = MorningWatchlistRenderer(_settings())
        signals = [
            _signal_row(
                ticker="FNMA",
                direction="LONG",
                index_tier="SP500",
                corroboration_count=2,
                composite_score=8.5,
                total_calls=20,
                total_hits=14,
            )
        ]
        email = renderer.render_from_rows(
            signals, signal_date=_TODAY, accounts_monitored=17
        )
        body = email.text_body
        # Every required field present
        assert "#1" in body
        assert "[LONG]" in body
        assert "FNMA" in body
        assert "[SP500]" in body
        assert "Bill Ackman" in body
        assert "@BillAckman" in body
        assert "14/20 (70%)" in body
        assert "CORROBORATED" in body
        # 5 filled dots for score 8.5
        assert "●●●●●" in body


# ----------------------------------------------------------------------
# DB-integrated render (uses get_morning_watchlist)
# ----------------------------------------------------------------------


class _FakeRepo:
    """Lightweight stand-in for DatabaseRepository in render() tests."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    async def get_morning_watchlist(
        self, signal_date: date, tenant_id: int = 1
    ) -> list[dict[str, Any]]:
        return list(self._rows)


class TestRenderFromRepo:
    @pytest.mark.asyncio
    async def test_render_reads_from_repo(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        repo = _FakeRepo([_signal_row()])
        email = await renderer.render(
            signal_date=_TODAY, repo=repo, accounts_monitored=17
        )
        assert "FNMA" in email.text_body

    @pytest.mark.asyncio
    async def test_render_empty_repo_returns_no_signals(self) -> None:
        renderer = MorningWatchlistRenderer(_settings())
        repo = _FakeRepo([])
        email = await renderer.render(
            signal_date=_TODAY, repo=repo, accounts_monitored=17
        )
        assert "No Signals Overnight" in email.subject
