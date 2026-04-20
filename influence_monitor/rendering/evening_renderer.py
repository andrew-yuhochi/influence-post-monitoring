"""Evening summary composer for WhatsApp delivery — TASK-013.

Renders per-stock outcome blocks (overnight / tradeable / excess-vol) and the
30-day per-poster scorecard from ScorecardAggregator output.

Entry point: render_evening(signals, scorecard, trading_days_scored, as_of_date)
Returns a list[str] — one element normally; two if the message exceeds 4,000 chars.

Format per outcome block (ACT NOW):
  $TICKER +X.X% overnight / +X.X% tradeable / +X.XX excess-vol (SPY: +X.X% | vol: X.X%)
  _(short = gain)_ or _(short went up)_   [SHORT only]

Watch List outcomes appear in a separate section.
No disclaimer footer (PRD §8 override — personal-use PoC).
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

_CHAR_LIMIT = 4_000


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _pct(value: float, decimals: int = 1) -> str:
    """Format a decimal fraction as a signed percentage string.

    Examples:
        0.086387 → '+8.6%'
        -0.047018 → '-4.7%'
    """
    pct = value * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.{decimals}f}%"


def _evol(value: float) -> str:
    """Format excess-vol as a signed two-decimal number.

    Examples:
        1.878282 → '+1.88'
        -0.993297 → '-0.99'
    """
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}"


def _render_outcome_block(sig: dict[str, Any]) -> str | None:
    """Render a single signal outcome block.

    Returns None if price data is unavailable or excess_vol_score is None.
    """
    overnight = sig.get("overnight_return")
    tradeable = sig.get("tradeable_return")
    spy_ret = sig.get("spy_return")
    stock_vol = sig.get("stock_20d_vol")
    excess_vol = sig.get("excess_vol_score")
    direction = (sig.get("direction") or "LONG").upper()
    ticker = sig.get("ticker", "???")
    price_src = sig.get("price_data_source") or ""

    # Price data unavailable
    if price_src == "unavailable" or any(
        v is None for v in (overnight, tradeable, spy_ret, stock_vol, excess_vol)
    ):
        return f"*${ticker}* — price data unavailable"

    lines: list[str] = []

    # Main metrics line
    spy_str = _pct(spy_ret, 1)
    vol_str = f"{stock_vol * 100:.1f}%"
    metrics = (
        f"*${ticker}* "
        f"{_pct(overnight)} overnight / "
        f"{_pct(tradeable)} tradeable / "
        f"{_evol(excess_vol)} excess-vol "
        f"(SPY: {spy_str} | vol: {vol_str})"
    )
    lines.append(metrics)

    # SHORT annotation
    if direction == "SHORT":
        # Stock went down overnight → gain for short
        if overnight < 0:
            lines.append("_(short = gain)_")
        else:
            lines.append("_(short went up)_")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main render function
# ---------------------------------------------------------------------------

def render_evening(
    signals: list[dict[str, Any]],
    scorecard: list[dict[str, Any]],
    trading_days_scored: int,
    as_of_date: date | None = None,
) -> list[str]:
    """Render the evening WhatsApp summary.

    Args:
        signals:              All signals for the date (ACT_NOW + WATCH + UNSCORED).
                              Must include outcome columns (overnight_return, etc.).
        scorecard:            Top-N poster rows from ScorecardAggregator.top_n_posters().
                              Each dict has: handle, avg_excess_vol, n_signals.
        trading_days_scored:  Count from ScorecardAggregator.trading_days_with_signals().
        as_of_date:           The date of this evening run (used in the header).

    Returns:
        list[str] — one message normally; two if combined length exceeds 4,000 chars.
        Always returns at least one message (always-send rule).
    """
    date_str = as_of_date.strftime("%-d %b %Y") if as_of_date else "Today"
    header = f"📊 *Evening Summary — {date_str}*"

    # Split signals into ACT_NOW and WATCH tiers
    act_now_sigs = [s for s in signals if (s.get("tier") or "").upper() == "ACT_NOW"]
    watch_sigs = [s for s in signals if (s.get("tier") or "").upper() == "WATCH"]

    # Build ACT NOW outcomes section
    act_lines: list[str] = [header, "", "━━━ ACT NOW OUTCOMES ━━━"]

    act_blocks: list[str] = []
    for sig in act_now_sigs:
        block = _render_outcome_block(sig)
        if block:
            act_blocks.append(block)

    if act_blocks:
        for block in act_blocks:
            act_lines.append(block)
            act_lines.append("")
    else:
        act_lines.append("No outcomes to report today.")
        act_lines.append("")

    # Build WATCH LIST outcomes section
    watch_lines: list[str] = ["━━━ WATCH LIST (monitored only) ━━━"]

    watch_blocks: list[str] = []
    for sig in watch_sigs:
        block = _render_outcome_block(sig)
        if block:
            watch_blocks.append(block)

    if watch_blocks:
        for block in watch_blocks:
            watch_lines.append(block)
            watch_lines.append("")
    else:
        watch_lines.append("No watch-list outcomes today.")
        watch_lines.append("")

    # Build scorecard section
    scorecard_lines: list[str] = ["━━━ 30-DAY SCORECARD ━━━"]

    if trading_days_scored < 20:
        scorecard_lines.append(
            f"⚠️ Sample still building — treat as watchlist only (< 20 days)"
        )

    if scorecard:
        for row in scorecard:
            handle = row.get("handle", "unknown")
            avg_ev = float(row.get("avg_excess_vol", 0.0))
            n = int(row.get("n_signals", 0))
            sign = "+" if avg_ev >= 0 else ""
            scorecard_lines.append(
                f"@{handle} — avg excess-vol {sign}{avg_ev:.2f} ({n} {'signal' if n == 1 else 'signals'})"
            )
    else:
        scorecard_lines.append("No scorecard data yet.")

    scorecard_lines.append("")

    # Combine all sections
    section1 = "\n".join(act_lines)
    section2 = "\n".join(watch_lines)
    section3 = "\n".join(scorecard_lines)

    full_msg = section1 + "\n" + section2 + "\n" + section3

    if len(full_msg) <= _CHAR_LIMIT:
        return [full_msg]

    logger.info(
        "Evening summary exceeds %d chars (%d); splitting into two messages.",
        _CHAR_LIMIT,
        len(full_msg),
    )
    msg1 = section1 + "\n" + section2
    msg2 = section3
    return [msg1, msg2]
