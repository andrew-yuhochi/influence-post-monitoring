"""Unit tests for EveningScorecardRenderer.

Covers TASK-013 acceptance criteria:
- Performance table ranked by SHORT-corrected return (best → worst).
- Separator line between hits and misses.
- SHORT HIT label: "✓ HIT  (short = gain)".
- SHORT MISS label: "✗ MISS  (short went up)".
- LONG HIT/MISS labels.
- Halted stocks: "HALTED — no price data", excluded from hit counts.
- Running track record section.
- Track record age caveat when < 20 trading days.
- No-watchlist quiet-night state.
- Subject line convention from UX-SPEC.md.
- Plain-text complete (no information loss vs HTML).
- HTML wraps plain-text in <pre>.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from influence_monitor.config import Settings
from influence_monitor.email.renderer import (
    EveningScorecardRenderer,
    _adjusted_return,
    _avg_adjusted_return,
    _fmt_return,
    _hit_miss_label,
)

_TODAY = date(2026, 4, 15)

_EMPTY_STATS: dict[str, Any] = {
    "total_scored": 0,
    "total_hits": 0,
    "avg_gain_correct": None,
    "avg_loss_incorrect": None,
    "corroborated_total": 0,
    "corroborated_hits": 0,
}


def _settings() -> Settings:
    return Settings(track_record_min_calls=5)


def _signal(
    morning_rank: int = 1,
    ticker: str = "FNMA",
    direction: str = "LONG",
    return_pct: float | None = 4.2,
    is_hit: bool | None = True,
    close_price: float | None = 10.4,
    open_price: float | None = 10.0,
    corroboration_count: int = 1,
    investor_name: str = "Bill Ackman",
    x_handle: str = "BillAckman",
    total_calls: int = 20,
    total_hits: int = 14,
) -> dict[str, Any]:
    return {
        "morning_rank": morning_rank,
        "ticker": ticker,
        "direction": direction,
        "return_pct": return_pct,
        "is_hit": is_hit,
        "close_price": close_price,
        "open_price": open_price,
        "corroboration_count": corroboration_count,
        "investor_name": investor_name,
        "x_handle": x_handle,
        "total_calls": total_calls,
        "total_hits": total_hits,
    }


def _full_stats(
    total_scored: int = 312,
    total_hits: int = 200,
    avg_gain: float = 1.8,
    avg_loss: float = -0.9,
    corroborated_total: int = 48,
    corroborated_hits: int = 34,
) -> dict[str, Any]:
    return {
        "total_scored": total_scored,
        "total_hits": total_hits,
        "avg_gain_correct": avg_gain,
        "avg_loss_incorrect": avg_loss,
        "corroborated_total": corroborated_total,
        "corroborated_hits": corroborated_hits,
    }


# ----------------------------------------------------------------------
# Helper function unit tests
# ----------------------------------------------------------------------


class TestAdjustedReturn:
    def test_long_positive_is_positive(self) -> None:
        sig = _signal(direction="LONG", return_pct=3.5)
        assert _adjusted_return(sig) == pytest.approx(3.5)

    def test_long_negative_is_negative(self) -> None:
        sig = _signal(direction="LONG", return_pct=-2.0)
        assert _adjusted_return(sig) == pytest.approx(-2.0)

    def test_short_negative_is_positive_adjusted(self) -> None:
        # SHORT -2% = good; adjusted return should be +2
        sig = _signal(direction="SHORT", return_pct=-2.0)
        assert _adjusted_return(sig) == pytest.approx(2.0)

    def test_short_positive_is_negative_adjusted(self) -> None:
        # SHORT +1.8% = bad; adjusted return should be -1.8
        sig = _signal(direction="SHORT", return_pct=1.8)
        assert _adjusted_return(sig) == pytest.approx(-1.8)


class TestHitMissLabel:
    def test_long_hit(self) -> None:
        assert _hit_miss_label(_signal(direction="LONG", is_hit=True)) == "✓ HIT"

    def test_long_miss(self) -> None:
        assert _hit_miss_label(_signal(direction="LONG", is_hit=False)) == "✗ MISS"

    def test_short_hit(self) -> None:
        label = _hit_miss_label(_signal(direction="SHORT", is_hit=True))
        assert label == "✓ HIT  (short = gain)"

    def test_short_miss(self) -> None:
        label = _hit_miss_label(_signal(direction="SHORT", is_hit=False))
        assert label == "✗ MISS  (short went up)"

    def test_halted_none_is_hit(self) -> None:
        label = _hit_miss_label(_signal(is_hit=None))
        assert "HALTED" in label


class TestFmtReturn:
    def test_positive_has_plus_prefix(self) -> None:
        assert _fmt_return(4.2) == "+4.2%"

    def test_negative_has_minus(self) -> None:
        assert _fmt_return(-0.6) == "-0.6%"

    def test_none_returns_na(self) -> None:
        assert _fmt_return(None) == "N/A"

    def test_zero_has_plus(self) -> None:
        assert _fmt_return(0.0) == "+0.0%"


# ----------------------------------------------------------------------
# End-to-end rendering
# ----------------------------------------------------------------------


class TestAllHitsDay:
    def test_all_hits_subject(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [
            _signal(morning_rank=1, ticker="FNMA", direction="LONG", return_pct=4.2, is_hit=True),
            _signal(morning_rank=2, ticker="XYZ", direction="LONG", return_pct=2.8, is_hit=True),
            _signal(morning_rank=3, ticker="ABC", direction="LONG", return_pct=1.9, is_hit=True),
        ]
        email = _render(renderer, signals)
        assert "3/3 correct" in email.subject
        # Best performer = FNMA +4.2% (was #1)
        assert "Best: FNMA +4.2%" in email.subject
        assert "was #1" in email.subject

    def test_all_hits_body(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [_signal(morning_rank=i + 1, ticker=f"T{i}", return_pct=3.0 - i * 0.5, is_hit=True) for i in range(3)]
        email = _render(renderer, signals)
        assert "✓ HIT" in email.text_body
        assert "✗ MISS" not in email.text_body
        assert "3 correct" in email.text_body
        assert "0 incorrect" in email.text_body


class TestAllMissesDay:
    def test_all_misses_subject(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [
            _signal(morning_rank=1, ticker="FNMA", direction="LONG", return_pct=-2.1, is_hit=False),
            _signal(morning_rank=2, ticker="XYZ", direction="SHORT", return_pct=1.8, is_hit=False),
        ]
        email = _render(renderer, signals)
        assert "0/2 correct" in email.subject

    def test_all_misses_body(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [_signal(morning_rank=1, return_pct=-1.5, is_hit=False)]
        email = _render(renderer, signals)
        assert "✗ MISS" in email.text_body
        assert "0 correct" in email.text_body
        assert "1 incorrect" in email.text_body


class TestMixedDay:
    def test_separator_between_hits_and_misses(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [
            _signal(morning_rank=1, ticker="FNMA", direction="LONG", return_pct=4.2, is_hit=True),
            _signal(morning_rank=2, ticker="XYZ", direction="LONG", return_pct=-2.1, is_hit=False),
        ]
        email = _render(renderer, signals)
        assert "✓ HIT" in email.text_body
        assert "✗ MISS" in email.text_body
        # Separator row present (uses "─ " * 27 pattern)
        assert "─ ─ ─" in email.text_body

    def test_performance_ranking_respects_short_correction(self) -> None:
        """SHORT signal with -3% return should rank above LONG signal with +1%."""
        renderer = EveningScorecardRenderer(_settings())
        signals = [
            _signal(morning_rank=1, ticker="LONG1", direction="LONG", return_pct=1.0, is_hit=True),
            _signal(morning_rank=2, ticker="SHORT1", direction="SHORT", return_pct=-3.0, is_hit=True),
        ]
        email = _render(renderer, signals)
        body = email.text_body
        # SHORT1 adjusted return = +3.0 > LONG1 adjusted return = +1.0
        # So SHORT1 should appear before LONG1 in the perf table
        short1_pos = body.find("SHORT1")
        long1_pos = body.find("LONG1")
        assert short1_pos < long1_pos, "SHORT1 (better adjusted return) should rank higher"

    def test_short_hit_label_has_parenthetical(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [_signal(ticker="XYZ", direction="SHORT", return_pct=-0.6, is_hit=True)]
        email = _render(renderer, signals)
        assert "✓ HIT  (short = gain)" in email.text_body

    def test_short_miss_label_has_parenthetical(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [_signal(ticker="XYZ", direction="SHORT", return_pct=1.8, is_hit=False)]
        email = _render(renderer, signals)
        assert "✗ MISS  (short went up)" in email.text_body


class TestHaltedStock:
    def test_halted_excluded_from_hit_count(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [
            _signal(morning_rank=1, ticker="FNMA", return_pct=4.2, is_hit=True, close_price=10.4),
            # Halted: no close_price
            _signal(morning_rank=2, ticker="HALT", return_pct=None, is_hit=None, close_price=None, open_price=None),
        ]
        email = _render(renderer, signals)
        # Only 1 scoreable signal → 1/1 correct
        assert "1/1 correct" in email.subject
        assert "HALTED" in email.text_body
        assert "no price data" in email.text_body

    def test_halted_row_appears_in_table(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [
            _signal(morning_rank=1, ticker="GOOD", return_pct=2.0, is_hit=True, close_price=10.0),
            _signal(morning_rank=2, ticker="STOP", return_pct=None, is_hit=None, close_price=None, open_price=None),
        ]
        email = _render(renderer, signals)
        assert "STOP" in email.text_body


class TestQuietNight:
    def test_no_watchlist_subject(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        email = _render(renderer, [])
        assert "No watchlist today" in email.subject
        assert "[Apr 15]" in email.subject

    def test_no_watchlist_body(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        email = _render(renderer, [])
        assert "no overnight signals" in email.text_body.lower()

    def test_no_watchlist_still_has_track_record(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        email = _render(renderer, [], trading_days=30, stats=_full_stats())
        assert "RUNNING TRACK RECORD" in email.text_body


class TestTrackRecord:
    def test_caveat_shown_under_20_days(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [_signal()]
        email = _render(renderer, signals, trading_days=5, stats=_full_stats(total_scored=15, total_hits=10))
        assert "still building" in email.text_body

    def test_caveat_absent_at_20_days(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [_signal()]
        email = _render(renderer, signals, trading_days=20, stats=_full_stats())
        assert "still building" not in email.text_body

    def test_corroborated_line_shown_when_nonzero(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [_signal()]
        email = _render(renderer, signals, stats=_full_stats(corroborated_total=48, corroborated_hits=34))
        assert "Corroborated signals" in email.text_body

    def test_no_corroborated_line_when_zero(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [_signal()]
        email = _render(renderer, signals, stats=_full_stats(corroborated_total=0, corroborated_hits=0))
        assert "Corroborated signals" not in email.text_body

    def test_empty_stats_renders_gracefully(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [_signal()]
        email = _render(renderer, signals, stats=_EMPTY_STATS)
        assert "No scored signals yet" in email.text_body

    def test_top_performer_shown(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [_signal()]
        top = {"investor_name": "Bill Ackman", "x_handle": "BillAckman", "calls": 23, "hits": 18}
        email = _render(renderer, signals, stats=_full_stats(), top_performer=top)
        assert "Bill Ackman" in email.text_body
        assert "78%" in email.text_body  # 18/23 = 78%


class TestHtmlRendering:
    def test_html_has_pre_tag(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        email = _render(renderer, [_signal()])
        assert "<pre" in email.html_body
        assert "</pre>" in email.html_body

    def test_html_escapes_user_content(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        signals = [_signal()]
        # Inject HTML via top_performer name (displayed in track record section)
        top = {"investor_name": "<script>evil</script>", "x_handle": "evil", "calls": 5, "hits": 4}
        email = _render(renderer, signals, stats=_full_stats(), top_performer=top)
        assert "<script>" not in email.html_body
        assert "&lt;script&gt;" in email.html_body


class TestPlainTextCompleteness:
    def test_all_required_fields_present(self) -> None:
        """Plain-text must stand alone — all required fields visible."""
        renderer = EveningScorecardRenderer(_settings())
        signals = [
            _signal(morning_rank=1, ticker="FNMA", direction="LONG", return_pct=4.2, is_hit=True),
            _signal(morning_rank=2, ticker="STU", direction="SHORT", return_pct=-0.6, is_hit=True),
            _signal(morning_rank=3, ticker="DEF", direction="LONG", return_pct=-2.1, is_hit=False),
        ]
        email = _render(renderer, signals, trading_days=32, stats=_full_stats())
        body = email.text_body
        assert "Apr 15" in body            # date
        assert "FNMA" in body              # top ticker
        assert "+4.2%" in body             # return
        assert "#1" in body                # morning rank
        assert "✓ HIT" in body             # hit label
        assert "✗ MISS" in body            # miss label
        assert "short = gain" in body      # short hit parenthetical
        assert "2/3 correct" in body       # today summary
        assert "RUNNING TRACK RECORD" in body  # track record header
        assert "64%" in body               # running hit rate (200/312)


# ----------------------------------------------------------------------
# DB-integrated render
# ----------------------------------------------------------------------


class _FakeRepo:
    def __init__(
        self,
        rows: list[dict[str, Any]],
        stats: dict[str, Any] | None = None,
        trading_days: int = 0,
        first_date: date | None = None,
        top_performer: dict[str, Any] | None = None,
    ) -> None:
        self._rows = rows
        self._stats = stats or _EMPTY_STATS
        self._trading_days = trading_days
        self._first_date = first_date
        self._top_performer = top_performer

    async def get_evening_scorecard_signals(
        self, signal_date: date, tenant_id: int = 1
    ) -> list[dict[str, Any]]:
        return list(self._rows)

    async def get_running_stats(self, tenant_id: int = 1) -> dict[str, Any]:
        return self._stats

    async def get_trading_days_scored(self, tenant_id: int = 1) -> int:
        return self._trading_days

    async def get_first_scored_date(self, tenant_id: int = 1) -> date | None:
        return self._first_date

    async def get_top_performer_month(
        self, tenant_id: int = 1, min_calls: int = 3
    ) -> dict[str, Any] | None:
        return self._top_performer


class TestRenderFromRepo:
    @pytest.mark.asyncio
    async def test_render_reads_from_repo(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        repo = _FakeRepo([_signal()])
        email = await renderer.render(signal_date=_TODAY, repo=repo)
        assert "FNMA" in email.text_body

    @pytest.mark.asyncio
    async def test_render_empty_returns_no_watchlist(self) -> None:
        renderer = EveningScorecardRenderer(_settings())
        repo = _FakeRepo([])
        email = await renderer.render(signal_date=_TODAY, repo=repo)
        assert "No watchlist today" in email.subject


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _render(
    renderer: EveningScorecardRenderer,
    signals: list[dict[str, Any]],
    trading_days: int = 0,
    stats: dict[str, Any] | None = None,
    first_date: date | None = None,
    top_performer: dict[str, Any] | None = None,
) -> Any:
    return renderer.render_from_data(
        signals=signals,
        signal_date=_TODAY,
        running_stats=stats or _EMPTY_STATS,
        trading_days=trading_days,
        first_scored_date=first_date,
        top_performer=top_performer,
    )
