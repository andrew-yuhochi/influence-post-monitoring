"""Email renderers for morning watchlist and evening scorecard.

Both renderers read from the DB so they can be re-run for any date
(dry-run, QA, replay) without live API calls.  HTML bodies wrap the
plain-text wireframe in ``<pre>`` so no CSS is load-bearing.

MorningWatchlistRenderer (TASK-012):
- Signal cards: rank, direction badge, ticker, index tier, poster name,
  quote fragment, track record, strength bars, corroboration flag.
- Track-record warm-up when total_calls < settings.track_record_min_calls.
- Edge cases: 0 signals (no-signals), N<10 (footer note), deleted posts.

EveningScorecardRenderer (TASK-013):
- Performance table ranked by SHORT-corrected return.
- HIT/MISS labels with SHORT-aware parenthetical.
- Halted stocks flagged and excluded from stats.
- Running track record footer with age caveat (<20 trading days).
- No-watchlist quiet-night state.
"""

from __future__ import annotations

import html as html_lib
import logging
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from influence_monitor.config import Settings
from influence_monitor.db.repository import DatabaseRepository

logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).parent / "templates"

QUOTE_MAX_CHARS = 200


@dataclass
class RenderedEmail:
    """The three pieces of a rendered email ready to send."""

    subject: str
    text_body: str
    html_body: str


class MorningWatchlistRenderer:
    """Render the 7:00 AM morning watchlist email.

    Usage::

        renderer = MorningWatchlistRenderer(settings)
        email = await renderer.render(signal_date, repo, accounts_monitored=17)
        await email_provider.send(to, email.subject, email.html_body, email.text_body)
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._min_calls = settings.track_record_min_calls
        self._watchlist_txt = (_TEMPLATES_DIR / "morning_watchlist.txt").read_text()
        self._watchlist_html = (_TEMPLATES_DIR / "morning_watchlist.html").read_text()
        self._signal_card_txt = (_TEMPLATES_DIR / "signal_card.txt").read_text()
        self._no_signals_txt = (_TEMPLATES_DIR / "no_signals.txt").read_text()

    async def render(
        self,
        signal_date: date,
        repo: DatabaseRepository,
        accounts_monitored: int,
    ) -> RenderedEmail:
        """Fetch signals for *signal_date* and produce the email.

        Falls back to the no-signals template when the watchlist is empty.
        """
        signals = await repo.get_morning_watchlist(signal_date)
        return self.render_from_rows(signals, signal_date, accounts_monitored)

    def render_from_rows(
        self,
        signals: list[dict[str, Any]],
        signal_date: date,
        accounts_monitored: int,
    ) -> RenderedEmail:
        """Render an email from already-fetched signal rows.

        Exposed for unit testing and dry-run modes where the DB layer is
        mocked.  *signals* is a list of dicts with the shape produced by
        ``DatabaseRepository.get_morning_watchlist``.
        """
        date_long = signal_date.strftime("%a, %b %d %Y")
        date_short = signal_date.strftime("%b %d")

        if not signals:
            return self._render_no_signals(date_long, date_short, accounts_monitored)

        subject = self._render_subject(signals, date_short)
        text_body = self._render_text_body(
            signals, date_long, accounts_monitored
        )
        html_body = self._render_html_body(text_body)
        return RenderedEmail(subject=subject, text_body=text_body, html_body=html_body)

    # ------------------------------------------------------------------
    # Subject line
    # ------------------------------------------------------------------

    def _render_subject(self, signals: list[dict[str, Any]], date_short: str) -> str:
        top = signals[0]
        top_bars = _strength_bars(top.get("composite_score") or 0.0)
        return (
            f"Influence Monitor — {len(signals)} Signals Today [{date_short}] | "
            f"Top: {top['ticker']} ({top['direction']}, {top_bars})"
        )

    # ------------------------------------------------------------------
    # No-signals state
    # ------------------------------------------------------------------

    def _render_no_signals(
        self, date_long: str, date_short: str, accounts_monitored: int
    ) -> RenderedEmail:
        subject = f"Influence Monitor — No Signals Overnight [{date_short}]"
        text_body = self._no_signals_txt.format(
            date_long=date_long,
            accounts_monitored=accounts_monitored,
        )
        html_body = self._render_html_body(text_body)
        return RenderedEmail(subject=subject, text_body=text_body, html_body=html_body)

    # ------------------------------------------------------------------
    # Plain-text body
    # ------------------------------------------------------------------

    def _render_text_body(
        self,
        signals: list[dict[str, Any]],
        date_long: str,
        accounts_monitored: int,
    ) -> str:
        total = len(signals)
        corroborated = sum(1 for s in signals if (s.get("corroboration_count") or 0) >= 2)
        longs = sum(1 for s in signals if (s.get("direction") or "").upper() == "LONG")
        shorts = sum(1 for s in signals if (s.get("direction") or "").upper() == "SHORT")
        headline_stats = (
            f"{total} signals detected   |   "
            f"{corroborated} corroborated   |   "
            f"{longs} LONG  {shorts} SHORT"
        )

        cards = "\n".join(self._render_card_text(s) for s in signals)

        footer_note = ""
        if total < 10:
            footer_note = f"\nOnly {total} signals met the threshold overnight."

        return self._watchlist_txt.format(
            date_long=date_long,
            headline_stats=headline_stats,
            signal_cards=cards,
            accounts_monitored=accounts_monitored,
            footer_note=footer_note,
        )

    def _render_card_text(self, signal: dict[str, Any]) -> str:
        rank = signal.get("morning_rank") or 0
        direction = (signal.get("direction") or "").upper()
        ticker = signal.get("ticker") or "?"
        index_tier = signal.get("index_tier") or "MICRO"
        composite = signal.get("composite_score") or 0.0
        strength_bars = _strength_bars(composite)

        corroboration_count = signal.get("corroboration_count") or 1
        if corroboration_count >= 2:
            corroboration_line = (
                f"\n│  CORROBORATED — {corroboration_count} posters"
            )
        else:
            corroboration_line = ""

        investor_name = signal.get("investor_name") or "Unknown"
        handle_raw = signal.get("x_handle") or ""
        handle = f"@{handle_raw}" if handle_raw and not handle_raw.startswith("@") else handle_raw or "@unknown"
        track_record = _format_track_record(
            signal.get("total_calls") or 0,
            signal.get("total_hits") or 0,
            self._min_calls,
        )
        quote_fragment = _format_quote(signal.get("post_text") or "", signal.get("post_deleted"))

        return self._signal_card_txt.format(
            rank=rank,
            direction=direction,
            ticker=ticker,
            index_tier=index_tier,
            strength_bars=strength_bars,
            corroboration_line=corroboration_line,
            investor_name=investor_name,
            handle=handle,
            track_record=track_record,
            quote_fragment=quote_fragment,
        )

    # ------------------------------------------------------------------
    # HTML body
    # ------------------------------------------------------------------

    def _render_html_body(self, text_body: str) -> str:
        """Wrap plain text in a <pre> block for email clients.

        HTML-escape the text so any user-controlled content (quote
        fragments, investor names) cannot inject markup.  The plain-text
        wireframe is preserved verbatim — all Unicode circles and box
        drawing characters render without CSS.
        """
        escaped = html_lib.escape(text_body)
        return self._watchlist_html.format(body=escaped)


# ----------------------------------------------------------------------
# Formatting helpers (module-level for testability)
# ----------------------------------------------------------------------


def _strength_bars(composite_score: float) -> str:
    """Map composite score (0–10) to a 5-level Unicode dot bar.

    ``●○○○○`` (weak) → ``●●●●●`` (strongest).  Uses ``ceil(score/2)``
    clamped to [1, 5] so any surfaced signal shows at least one filled
    dot.
    """
    filled = max(1, min(5, math.ceil(composite_score / 2.0)))
    return "●" * filled + "○" * (5 - filled)


def _format_track_record(total_calls: int, total_hits: int, min_calls: int) -> str:
    """Return the track record badge string.

    Shows ``building...`` when the investor has fewer than *min_calls*
    scored signals; otherwise ``{hits}/{calls} ({pct}%)``.
    """
    if total_calls < min_calls:
        return "building..."
    pct = round(100.0 * total_hits / total_calls) if total_calls else 0
    return f"{total_hits}/{total_calls} ({pct}%)"


def _format_quote(post_text: str, post_deleted: Any) -> str:
    """Trim post text to a 2–3 line quote fragment.

    Gracefully handles deleted posts (``post_deleted`` truthy) — shows a
    short placeholder rather than a live fetch.  All text is read from
    the DB, so a deleted upstream post does not break rendering.
    """
    if post_deleted:
        return '"[Post no longer available]"'
    if not post_text:
        return '"[No text available]"'
    cleaned = " ".join(post_text.split())
    if len(cleaned) > QUOTE_MAX_CHARS:
        cleaned = cleaned[: QUOTE_MAX_CHARS - 3].rstrip() + "..."
    return f'"{cleaned}"'


# ======================================================================
# Evening Scorecard Renderer (TASK-013)
# ======================================================================

_TRACK_RECORD_AGE_THRESHOLD = 20  # trading days before caveat is removed


class EveningScorecardRenderer:
    """Render the ~5:00 PM evening scorecard email.

    The scorecard shows how the morning watchlist performed intraday and
    updates the running track record.  This is both the daily feedback
    loop and the commercial signal instrument (UX-SPEC.md).

    Usage::

        renderer = EveningScorecardRenderer(settings)
        email = await renderer.render(signal_date, repo)
        await email_provider.send(to, email.subject, email.html_body, email.text_body)
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._scorecard_txt = (_TEMPLATES_DIR / "scorecard.txt").read_text()
        self._no_watchlist_txt = (_TEMPLATES_DIR / "no_watchlist.txt").read_text()
        self._watchlist_html = (_TEMPLATES_DIR / "morning_watchlist.html").read_text()

    async def render(
        self,
        signal_date: date,
        repo: DatabaseRepository,
    ) -> RenderedEmail:
        """Fetch scorecard data for *signal_date* and produce the email."""
        signals = await repo.get_evening_scorecard_signals(signal_date)
        running_stats = await repo.get_running_stats()
        trading_days = await repo.get_trading_days_scored()
        first_date = await repo.get_first_scored_date()
        top_performer = await repo.get_top_performer_month()
        return self.render_from_data(
            signals=signals,
            signal_date=signal_date,
            running_stats=running_stats,
            trading_days=trading_days,
            first_scored_date=first_date,
            top_performer=top_performer,
        )

    def render_from_data(
        self,
        signals: list[dict[str, Any]],
        signal_date: date,
        running_stats: dict[str, Any],
        trading_days: int,
        first_scored_date: date | None,
        top_performer: dict[str, Any] | None,
    ) -> RenderedEmail:
        """Render the scorecard from pre-fetched data rows.

        Exposed for unit testing without a real database.
        """
        date_long = signal_date.strftime("%a, %b %d %Y")
        date_short = signal_date.strftime("%b %d")
        send_time = "05:00 PM"

        track_record_section = self._render_track_record(
            running_stats=running_stats,
            trading_days=trading_days,
            first_scored_date=first_scored_date,
            top_performer=top_performer,
        )

        if not signals:
            return self._render_no_watchlist(
                date_long=date_long,
                date_short=date_short,
                send_time=send_time,
                track_record_section=track_record_section,
            )

        subject = self._render_subject(signals, date_short)
        text_body = self._render_text_body(
            signals=signals,
            date_long=date_long,
            send_time=send_time,
            track_record_section=track_record_section,
        )
        html_body = self._render_html_body(text_body)
        return RenderedEmail(subject=subject, text_body=text_body, html_body=html_body)

    # ------------------------------------------------------------------
    # No-watchlist state (quiet night)
    # ------------------------------------------------------------------

    def _render_no_watchlist(
        self,
        date_long: str,
        date_short: str,
        send_time: str,
        track_record_section: str,
    ) -> RenderedEmail:
        subject = f"Scorecard [{date_short}] — No watchlist today (no overnight signals)"
        text_body = self._no_watchlist_txt.format(
            date_long=date_long,
            send_time=send_time,
            track_record_section=track_record_section,
        )
        return RenderedEmail(
            subject=subject,
            text_body=text_body,
            html_body=self._render_html_body(text_body),
        )

    # ------------------------------------------------------------------
    # Subject line
    # ------------------------------------------------------------------

    def _render_subject(
        self, signals: list[dict[str, Any]], date_short: str
    ) -> str:
        # Exclude halted signals from hit count
        scoreable = [s for s in signals if s.get("close_price") is not None]
        correct = sum(1 for s in scoreable if s.get("is_hit"))
        total = len(scoreable)

        # Best performer: highest adjusted return among scored signals
        best = _best_performer(scoreable)
        if best:
            ticker = best["ticker"]
            ret = best.get("return_pct") or 0.0
            ret_str = _fmt_return(ret)
            morning_rank = best.get("morning_rank") or 0
            direction = (best.get("direction") or "").upper()
            suffix = " short" if direction == "SHORT" else ""
            best_part = f" | Best: {ticker} {ret_str}{suffix} (was #{morning_rank})"
        else:
            best_part = ""

        return f"Scorecard [{date_short}] — {correct}/{total} correct{best_part}"

    # ------------------------------------------------------------------
    # Plain-text body
    # ------------------------------------------------------------------

    def _render_text_body(
        self,
        signals: list[dict[str, Any]],
        date_long: str,
        send_time: str,
        track_record_section: str,
    ) -> str:
        scoreable = [s for s in signals if s.get("close_price") is not None]
        halted = [s for s in signals if s.get("close_price") is None]

        correct = sum(1 for s in scoreable if s.get("is_hit"))
        incorrect = sum(1 for s in scoreable if s.get("is_hit") is not None and not s.get("is_hit"))

        if scoreable:
            hit_rate_pct = round(100.0 * correct / len(scoreable))
            # Show fraction when < 20 days of data; always show fraction when total < 20
            hit_rate_display = f"{correct}/{len(scoreable)} correct"
            avg_gain_val = _avg_adjusted_return(scoreable, hit=True)
            avg_loss_val = _avg_adjusted_return(scoreable, hit=False)
            avg_gain = _fmt_return(avg_gain_val) if avg_gain_val is not None else "N/A"
            avg_loss = _fmt_return(avg_loss_val) if avg_loss_val is not None else "N/A"
        else:
            hit_rate_display = "no price data"
            avg_gain = "N/A"
            avg_loss = "N/A"

        perf_table = self._render_performance_table(signals)

        return self._scorecard_txt.format(
            date_long=date_long,
            send_time=send_time,
            correct=correct,
            incorrect=incorrect,
            hit_rate_display=hit_rate_display,
            avg_gain=avg_gain,
            avg_loss=avg_loss,
            perf_table=perf_table,
            track_record_section=track_record_section,
        )

    def _render_performance_table(self, signals: list[dict[str, Any]]) -> str:
        """Build the fixed-width performance table.

        Sorted by SHORT-corrected return descending (best performer first).
        Halted signals appear at the bottom with a special marker.
        Separator line between hits and misses.
        """
        scoreable = [s for s in signals if s.get("close_price") is not None]
        halted = [s for s in signals if s.get("close_price") is None]

        # Sort scoreable: hits first (descending adjusted return), then misses
        ranked = sorted(
            scoreable,
            key=lambda s: _adjusted_return(s),
            reverse=True,
        )

        header = (
            "Rank  Ticker  Dir   Open→Close   Morning Rank   Result\n"
            "────  ──────  ────  ──────────   ────────────   ──────"
        )

        hits = [s for s in ranked if s.get("is_hit")]
        misses = [s for s in ranked if not s.get("is_hit")]

        rows: list[str] = []
        for perf_rank, sig in enumerate(hits + misses, start=1):
            rows.append(_format_perf_row(perf_rank, sig))
            # Separator after last hit, before first miss
            if perf_rank == len(hits) and misses:
                rows.append("─ " * 27)

        for sig in halted:
            rows.append(_format_halted_row(sig))

        return header + "\n" + "\n".join(rows)

    # ------------------------------------------------------------------
    # Running track record section
    # ------------------------------------------------------------------

    def _render_track_record(
        self,
        running_stats: dict[str, Any],
        trading_days: int,
        first_scored_date: date | None,
        top_performer: dict[str, Any] | None,
    ) -> str:
        total_scored = running_stats.get("total_scored") or 0
        total_hits = running_stats.get("total_hits") or 0
        avg_gain = running_stats.get("avg_gain_correct")
        avg_loss = running_stats.get("avg_loss_incorrect")
        corroborated_total = running_stats.get("corroborated_total") or 0
        corroborated_hits = running_stats.get("corroborated_hits") or 0

        if total_scored == 0:
            return (
                "RUNNING TRACK RECORD\n"
                "─────────────────────────────────────────────────────────────\n"
                "  No scored signals yet. Track record will build over time.\n"
            )

        hit_rate_pct = round(100.0 * total_hits / total_scored) if total_scored else 0
        avg_gain_str = _fmt_return(avg_gain) if avg_gain is not None else "N/A"
        avg_loss_str = _fmt_return(avg_loss) if avg_loss is not None else "N/A"

        since_str = first_scored_date.strftime("%Y-%m-%d") if first_scored_date else "?"
        header = (
            f"RUNNING TRACK RECORD  (since {since_str} — {trading_days} trading days)\n"
            "─────────────────────────────────────────────────────────────"
        )

        lines = [
            header,
            f"Total signals scored:     {total_scored}",
            f"Directional hit rate:     {hit_rate_pct}%   ({total_hits}/{total_scored})",
            f"Avg gain on correct:      {avg_gain_str}",
            f"Avg loss on incorrect:    {avg_loss_str}",
        ]

        if corroborated_total > 0:
            corr_pct = round(100.0 * corroborated_hits / corroborated_total)
            lines.append(
                f"Corroborated signals:     {corr_pct}%   "
                f"({corroborated_hits}/{corroborated_total})   "
                "<- higher accuracy subset"
            )

        if top_performer:
            tp_name = top_performer.get("investor_name") or "Unknown"
            tp_calls = top_performer.get("calls") or 0
            tp_hits = top_performer.get("hits") or 0
            tp_pct = round(100.0 * tp_hits / tp_calls) if tp_calls else 0
            lines.append(
                f"Top performer this month: {tp_name}  ({tp_pct}% hit rate, {tp_calls} calls)"
            )

        lines.append(f"\n  Track record based on {trading_days} trading days of data.")

        if trading_days < _TRACK_RECORD_AGE_THRESHOLD:
            lines.append(
                "  Track record still building — use this as watchlist only."
            )

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # HTML body
    # ------------------------------------------------------------------

    def _render_html_body(self, text_body: str) -> str:
        escaped = html_lib.escape(text_body)
        return self._watchlist_html.format(body=escaped)


# ----------------------------------------------------------------------
# Scorecard formatting helpers (module-level for testability)
# ----------------------------------------------------------------------


def _adjusted_return(signal: dict[str, Any]) -> float:
    """Return SHORT-corrected return for ranking (higher = better performance).

    For LONG: positive return is good → use return_pct as-is.
    For SHORT: negative return is good → negate return_pct.
    """
    ret = signal.get("return_pct") or 0.0
    direction = (signal.get("direction") or "").upper()
    return -ret if direction == "SHORT" else ret


def _best_performer(scoreable: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the signal with the highest adjusted return."""
    if not scoreable:
        return None
    return max(scoreable, key=_adjusted_return)


def _avg_adjusted_return(
    scoreable: list[dict[str, Any]], hit: bool
) -> float | None:
    """Average adjusted return for hit (hit=True) or miss (hit=False) signals."""
    subset = [s for s in scoreable if bool(s.get("is_hit")) is hit]
    if not subset:
        return None
    return sum(_adjusted_return(s) for s in subset) / len(subset)


def _fmt_return(value: float | None) -> str:
    """Format a return percentage with a leading sign and one decimal."""
    if value is None:
        return "N/A"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.1f}%"


def _format_perf_row(perf_rank: int, signal: dict[str, Any]) -> str:
    """Format one row of the performance table."""
    ticker = (signal.get("ticker") or "?").ljust(6)
    direction = (signal.get("direction") or "?").ljust(4)
    ret = signal.get("return_pct") or 0.0
    ret_str = _fmt_return(ret).rjust(9)
    morning_rank = f"#{signal.get('morning_rank') or 0}".rjust(11)
    result = _hit_miss_label(signal)
    return f"{str(perf_rank).rjust(4)}  {ticker}  {direction}  {ret_str}   {morning_rank}   {result}"


def _format_halted_row(signal: dict[str, Any]) -> str:
    """Format the row for a halted / price-unavailable stock."""
    ticker = (signal.get("ticker") or "?").ljust(6)
    direction = (signal.get("direction") or "?").ljust(4)
    morning_rank = f"#{signal.get('morning_rank') or 0}".rjust(11)
    return f"   -  {ticker}  {direction}       N/A   {morning_rank}   HALTED — no price data"


def _hit_miss_label(signal: dict[str, Any]) -> str:
    """Return the HIT/MISS label with SHORT-aware parenthetical."""
    is_hit = signal.get("is_hit")
    direction = (signal.get("direction") or "").upper()
    if is_hit is None:
        return "HALTED — no price data"
    if is_hit:
        if direction == "SHORT":
            return "✓ HIT  (short = gain)"
        return "✓ HIT"
    else:
        if direction == "SHORT":
            return "✗ MISS  (short went up)"
        return "✗ MISS"
