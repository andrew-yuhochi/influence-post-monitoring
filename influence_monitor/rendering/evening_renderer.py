"""Evening summary composer for WhatsApp delivery — TASK-013 / TASK-013b overhaul.

Renders per-stock outcome blocks and the 30-day per-poster scorecard from
ScorecardAggregator output.

Entry point: render_evening(signals, scorecard, trading_days_scored, as_of_date)
Returns a list[str] — one element normally; two if the message exceeds 4,000 chars.

Format per outcome block (ACT NOW):
  1. 📈 $FNMA
  D2D Return: +8.6%
  O2C Return: +6.4%
  BUY @BillAckman - 82%
  Excess-vol: 1.88 (vol: 4.2%) ✅

Conflict block (two opposing signals for same ticker):
  5. 📈📉 $TSLA
  D2D Return: +3.9%
  O2C Return: +2.7%
  BUY @CathieWood - 76%
  Excess-vol: 1.07 (vol: 3.0%) ✅
  SELL @CarsonBlock - 68%
  Excess-vol: -1.07 (vol: 3.0%) ❌

Watch List outcomes appear in a separate section (no "(monitored only)" suffix).
ACT NOW section is capped at 5 slots (mirrors morning alert cap).
No disclaimer footer (PRD §8 override — personal-use PoC).
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

_CHAR_LIMIT = 4_000

# Opposing-direction pairs (mirrors morning_renderer._OPPOSING)
_OPPOSING = {("LONG", "SHORT"), ("SHORT", "LONG"), ("BUY", "SELL"), ("SELL", "BUY")}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _are_opposing(dir_a: str, dir_b: str) -> bool:
    return (dir_a.upper(), dir_b.upper()) in _OPPOSING


def _pct(value: float, decimals: int = 1) -> str:
    """Format a decimal fraction as a signed percentage string.

    Examples:
        0.086387 → '+8.6%'
        -0.047018 → '-4.7%'
    """
    pct = value * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.{decimals}f}%"


def _direction_label(direction: str) -> str:
    d = direction.upper()
    if d in ("LONG", "BUY"):
        return "BUY"
    if d in ("SHORT", "SELL"):
        return "SELL"
    return direction.upper()


def _direction_emoji(direction: str) -> str:
    d = direction.upper()
    if d in ("LONG", "BUY"):
        return "📈"
    if d in ("SHORT", "SELL"):
        return "📉"
    return ""


def _score_pct(final_score: float | None) -> str:
    """Convert final_score (0.0–10.0 or 0.0–1.0) to a percentage string like '82%'."""
    if final_score is None:
        return "?%"
    # Scores stored as 0–10 scale; normalise to 0–100
    if final_score > 1.0:
        return f"{round(final_score / 10 * 100)}%"
    return f"{round(final_score * 100)}%"


def _excess_vol_line(
    excess_vol: float | None,
    stock_vol: float | None,
    price_unavailable: bool,
) -> str:
    """Render the Excess-vol line.

    Rules:
    - price unavailable → 'Excess-vol: — (price unavailable)'
    - abs(excess_vol) >= 2 → wrap with ⭐ stars
    - excess_vol >= 0 → trailing ✅
    - excess_vol < 0  → trailing ❌
    """
    if price_unavailable or excess_vol is None:
        return "Excess-vol: — (price unavailable)"

    vol_str = f"{stock_vol * 100:.1f}%" if stock_vol is not None else "?%"
    ev_val = f"{excess_vol:.2f}"
    tail = "✅" if excess_vol >= 0 else "❌"

    if abs(excess_vol) >= 2:
        return f"Excess-vol: ⭐ {ev_val} (vol: {vol_str}) ⭐"
    return f"Excess-vol: {ev_val} (vol: {vol_str}) {tail}"


def _render_single_block(sig: dict[str, Any]) -> list[str]:
    """Return lines (without numbering) for a single signal block.

    Always returns lines — never returns None.  Signals with missing price data
    are shown with 'Excess-vol: — (price unavailable)'.
    """
    ticker = sig.get("ticker", "???")
    direction = (sig.get("direction") or "LONG").upper()
    final_score = sig.get("final_score")
    overnight = sig.get("overnight_return")
    tradeable = sig.get("tradeable_return")
    stock_vol = sig.get("stock_20d_vol")
    excess_vol = sig.get("excess_vol_score")
    price_src = (sig.get("price_data_source") or "").lower()
    handle = sig.get("account_handle") or sig.get("handle") or ""

    price_unavailable = (
        price_src == "unavailable"
        or overnight is None
        or tradeable is None
        or excess_vol is None
    )

    label = _direction_label(direction)
    emoji = _direction_emoji(direction)
    score_str = _score_pct(final_score)

    lines: list[str] = []

    # Header: emoji $TICKER (stock first)
    lines.append(f"{emoji} ${ticker}")

    # Returns second
    if price_unavailable:
        lines.append("D2D Return: —")
        lines.append("O2C Return: —")
    else:
        lines.append(f"D2D Return: {_pct(overnight)}")
        lines.append(f"O2C Return: {_pct(tradeable)}")

    # Account + score third
    if handle:
        lines.append(f"{label} @{handle} - {score_str}")
    else:
        lines.append(f"{label} - {score_str}")

    # Excess-vol last
    lines.append(_excess_vol_line(excess_vol, stock_vol, price_unavailable))

    # SHORT annotation (only when NOT price unavailable, only _(short = gain)_ kept)
    if not price_unavailable and direction in ("SHORT", "SELL") and overnight is not None and overnight < 0:
        lines.append("_(short = gain)_")

    return lines


def _render_conflict_block_evening(
    sig_a: dict[str, Any],
    sig_b: dict[str, Any],
) -> list[str]:
    """Return lines (without numbering) for a conflict block.

    sig_a should have the higher conviction/final_score (sorted by caller).

    New format:
      📈📉 $TICKER
      D2D Return: +X.X%   (shared — same stock move for both sides)
      O2C Return: +X.X%
      BUY @handle - XX%
      Excess-vol: X.XX (vol: X.X%) ✅
      SELL @handle - XX%
      Excess-vol: -X.XX (vol: X.X%) ❌
    """
    ticker = sig_a.get("ticker", "???")
    lines: list[str] = [f"📈📉 ${ticker}"]

    # Use sig_a's price data for the shared D2D / O2C lines (both sides have the
    # same underlying stock movement).
    overnight_a = sig_a.get("overnight_return")
    tradeable_a = sig_a.get("tradeable_return")
    price_src_a = (sig_a.get("price_data_source") or "").lower()
    excess_vol_a = sig_a.get("excess_vol_score")

    shared_price_unavailable = (
        price_src_a == "unavailable"
        or overnight_a is None
        or tradeable_a is None
        or excess_vol_a is None
    )

    if shared_price_unavailable:
        lines.append("D2D Return: —")
        lines.append("O2C Return: —")
    else:
        lines.append(f"D2D Return: {_pct(overnight_a)}")
        lines.append(f"O2C Return: {_pct(tradeable_a)}")

    # Per-account: label+score then excess-vol (no blank line between sides)
    for sig in (sig_a, sig_b):
        direction = (sig.get("direction") or "LONG").upper()
        label = _direction_label(direction)
        final_score = sig.get("final_score")
        score_str = _score_pct(final_score)
        handle = sig.get("account_handle") or sig.get("handle") or ""
        stock_vol = sig.get("stock_20d_vol")
        excess_vol = sig.get("excess_vol_score")
        overnight = sig.get("overnight_return")
        price_src = (sig.get("price_data_source") or "").lower()
        tradeable = sig.get("tradeable_return")

        price_unavailable = (
            price_src == "unavailable"
            or overnight is None
            or tradeable is None
            or excess_vol is None
        )

        # Account + score line
        if handle:
            lines.append(f"{label} @{handle} - {score_str}")
        else:
            lines.append(f"{label} - {score_str}")

        # Per-account excess-vol line
        lines.append(_excess_vol_line(excess_vol, stock_vol, price_unavailable))

        # SHORT annotation — only _(short = gain)_ kept
        if not price_unavailable and direction in ("SHORT", "SELL") and overnight is not None and overnight < 0:
            lines.append("_(short = gain)_")

    return lines


def _group_signals(
    signals: list[dict[str, Any]],
) -> list[dict[str, Any] | tuple[dict[str, Any], dict[str, Any]]]:
    """Group signals by ticker.

    When exactly 2 signals share a ticker and have opposing directions, merge
    them into a (high_score, low_score) tuple that renders as one conflict block.
    All other signals remain as individual slots.
    """
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for sig in signals:
        by_ticker[sig.get("ticker", "???")] .append(sig)

    slots: list[dict[str, Any] | tuple[dict[str, Any], dict[str, Any]]] = []
    for _ticker, group in by_ticker.items():
        if len(group) == 2:
            d0 = (group[0].get("direction") or "LONG").upper()
            d1 = (group[1].get("direction") or "LONG").upper()
            if _are_opposing(d0, d1):
                ordered = sorted(group, key=lambda s: float(s.get("final_score") or 0), reverse=True)
                slots.append((ordered[0], ordered[1]))
                continue
        slots.extend(group)

    return slots


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
    header_lines: list[str] = [f"📊 *Evening Summary — {date_str}*"]

    # Market return header — use spy_return from any signal (all share the same value)
    spy_ret: float | None = None
    for sig in signals:
        v = sig.get("spy_return")
        if v is not None:
            spy_ret = float(v)
            break
    if spy_ret is not None:
        header_lines.append(f"Today Market Return: {_pct(spy_ret)}")

    # Split signals into ACT_NOW and WATCH tiers
    act_now_sigs = [s for s in signals if (s.get("tier") or "").upper() == "ACT_NOW"]
    watch_sigs = [s for s in signals if (s.get("tier") or "").upper() == "WATCH"]

    # Group opposing-direction pairs into conflict slots; cap ACT NOW at 5 (mirrors morning)
    act_slots = _group_signals(act_now_sigs)[:5]
    watch_slots = _group_signals(watch_sigs)

    # Build ACT NOW section
    act_lines: list[str] = header_lines + ["", "━━━ ACT NOW ━━━"]

    if act_slots:
        counter = 1
        for slot in act_slots:
            if isinstance(slot, tuple):
                block_lines = _render_conflict_block_evening(slot[0], slot[1])
            else:
                block_lines = _render_single_block(slot)

            # Prepend number to first line
            block_lines[0] = f"{counter}. {block_lines[0]}"
            counter += 1

            act_lines.extend(block_lines)
            act_lines.append("")
    else:
        act_lines.append("No outcomes to report today.")
        act_lines.append("")

    # Build WATCH LIST section
    watch_lines: list[str] = ["━━━ WATCH LIST ━━━"]

    if watch_slots:
        counter_w = len(act_slots) + 1
        for slot in watch_slots:
            if isinstance(slot, tuple):
                block_lines = _render_conflict_block_evening(slot[0], slot[1])
            else:
                block_lines = _render_single_block(slot)

            block_lines[0] = f"{counter_w}. {block_lines[0]}"
            counter_w += 1

            watch_lines.extend(block_lines)
            watch_lines.append("")
    else:
        watch_lines.append("No watch-list outcomes today.")
        watch_lines.append("")

    # Build scorecard section
    scorecard_lines: list[str] = ["━━━ 30-DAY SCORECARD ━━━"]

    if trading_days_scored < 20:
        scorecard_lines.append(
            "⚠️ Sample still building — treat as watchlist only (< 20 days)"
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
