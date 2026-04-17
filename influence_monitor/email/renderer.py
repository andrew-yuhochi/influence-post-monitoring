"""Morning watchlist email renderer.

Consumes ranked signals from the database and produces both plain-text
and HTML email bodies in the format specified in UX-SPEC.md.  The
renderer reads from the DB rather than in-memory Signal objects so it
can be re-run for any date (e.g., dry-run, QA, replay).

Acceptance criteria mapped to code:
- Signal cards use the wireframe format (rank, direction, ticker, index
  tier, investor name, quote fragment, track record, strength bars,
  corroboration flag).
- Track record warm-up: shows ``building...`` when ``total_calls``
  falls below ``settings.track_record_min_calls``.
- Signal strength: 5 Unicode-circle levels mapped from composite_score.
- Corroboration: ``CORROBORATED — N posters`` when count >= 2.
- Edge cases: 0 signals (no-signals template), N<10 (footer note),
  deleted posts (graceful — DB text used, never a live fetch).
- HTML wraps plain-text in ``<pre>`` so no CSS is load-bearing.
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
