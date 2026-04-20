# Morning alert composer for WhatsApp delivery — TASK-002.
# Renders Act Now and Watch List sections from MorningSignal dataclasses.
# Run with: python -m influence_monitor.rendering.morning_renderer --demo

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)

_FILLED_BUY = "✅"
_FILLED_SELL = "❌"


@dataclass
class Poster:
    handle: str
    strategy: str


@dataclass
class MorningSignal:
    ticker: str
    posters: list[Poster]
    direction: str              # "LONG" | "SHORT"
    conviction_score: float     # 0.0–10.0
    summary: str
    views_per_hour: float
    corroboration_count: int
    direction_flip: bool
    conflict_group: str         # "" | "opposing_exists"
    tier: str                   # "act_now" | "watch"
    post_created_at: datetime   # time the original post was published
    market_cap_class: str = ""  # "Mega" | "Large" | "Mid" | "Small" | "" (not shown)


def _direction_label(direction: str) -> str:
    if direction == "LONG":
        return "Buy"
    if direction == "SHORT":
        return "Sell"
    return direction


def _direction_emoji(direction: str) -> str:
    if direction == "LONG":
        return "📈"
    if direction == "SHORT":
        return "📉"
    return ""


def _conviction_display(score: float, direction: str = "LONG") -> str:
    """Return emoji bar + percentage score (filled markers only, no empty markers).

    BUY/LONG signals use ✅; SELL/SHORT signals use ❌.
    """
    if score >= 9.0:
        filled = 5
    elif score >= 7.0:
        filled = 4
    elif score >= 5.0:
        filled = 3
    elif score >= 3.0:
        filled = 2
    elif score >= 1.0:
        filled = 1
    else:
        filled = 0
    marker = _FILLED_SELL if direction == "SHORT" else _FILLED_BUY
    bar = marker * filled
    pct = round(score / 10 * 100)
    return f"{bar} - {pct}%"


def _truncate_chars(text: str, max_chars: int = 150) -> str:
    """Return text truncated to max_chars characters, appending … if cut.

    Truncation always happens at a whitespace boundary so words are not split.
    """
    if len(text) <= max_chars:
        return text
    # Walk back from max_chars to find the last whitespace boundary.
    cut = text.rfind(" ", 0, max_chars)
    if cut == -1:
        # No whitespace found — hard-cut at max_chars.
        cut = max_chars
    return text[:cut] + "…"


def _render_signal_block(signal: MorningSignal, include_velocity: bool = False) -> str:
    label = _direction_label(signal.direction)
    emoji = _direction_emoji(signal.direction)
    lines: list[str] = []
    cap_suffix = f" ({signal.market_cap_class})" if signal.market_cap_class else ""
    lines.append(f"*{emoji} {label} ${signal.ticker}{cap_suffix}*")
    lines.append(f"Score: {_conviction_display(signal.conviction_score, signal.direction)}")

    for poster in signal.posters:
        lines.append(f"@{poster.handle} - {poster.strategy}")

    if signal.direction_flip:
        lines.append("🔄 Direction changed")
    if signal.conflict_group == "opposing_exists":
        lines.append("⚔️ Conflicted — opposing view exists")

    quote = _truncate_chars(signal.summary)
    lines.append(f'> "{quote}"')

    if include_velocity:
        vph = f"{int(signal.views_per_hour):,}"
        posted_time = signal.post_created_at.strftime("%H:%M")
        lines.append(f"_{vph} posts/hr — Posted {posted_time}_")

    return "\n".join(lines)


def render_morning(act_now: list[MorningSignal], watch: list[MorningSignal]) -> list[str]:
    date_header = "📅 *Morning Alert — " + datetime.now().strftime("%-d %b %Y") + "*"

    if not act_now and not watch:
        parts = [
            date_header,
            "❌ No signals for today.",
        ]
        return ["\n".join(parts)]

    top_act = sorted(act_now, key=lambda s: s.conviction_score, reverse=True)
    top_watch = sorted(watch, key=lambda s: s.views_per_hour, reverse=True)[:5]

    # Build ACT NOW section
    act_sections: list[str] = [date_header, ""]
    act_sections.append("━━━ ACT NOW ━━━")
    if top_act:
        n = len(top_act)
        act_sections.append(f"*{n}* signal{'s' if n != 1 else ''} need immediate action")
        act_sections.append("")
        for sig in top_act:
            act_sections.append(_render_signal_block(sig, include_velocity=False))
            act_sections.append("")
    else:
        act_sections.append("No high-conviction signals")
        act_sections.append("")

    # Build WATCH LIST section
    watch_sections: list[str] = []
    watch_sections.append("━━━ WATCH LIST ━━━")
    if top_watch:
        n = len(top_watch)
        watch_sections.append(f"*{n}* signal{'s' if n != 1 else ''} need close attention")
        watch_sections.append("")
        for sig in top_watch:
            watch_sections.append(_render_signal_block(sig, include_velocity=True))
            watch_sections.append("")
    else:
        watch_sections.append("No signals need close attention")
        watch_sections.append("")

    msg1 = "\n".join(act_sections)
    msg2 = "\n".join(watch_sections)
    full = msg1 + "\n" + msg2

    if len(full) <= 4000:
        return [full]

    logger.info("Morning alert exceeds 4000 chars (%d); splitting into two messages.", len(full))
    return [msg1, msg2]


# ---------------------------------------------------------------------------
# Hardcoded fixture data
# ---------------------------------------------------------------------------

DEMO_FIXTURE: list[MorningSignal] = [
    MorningSignal(
        ticker="FNMA",
        posters=[
            Poster(handle="BillAckman", strategy="activist investor"),
            Poster(handle="DavidEinhorn", strategy="value investor"),
        ],
        direction="LONG",
        conviction_score=9.2,
        summary="Fannie Mae is absurdly underpriced. Regulatory unlock is imminent and this goes 10x from here.",
        views_per_hour=12500.0,
        corroboration_count=2,
        direction_flip=False,
        conflict_group="",
        tier="act_now",
        post_created_at=datetime(2026, 4, 18, 6, 47),
        market_cap_class="Small",
    ),
    MorningSignal(
        ticker="NFLX",
        posters=[Poster(handle="WallStCynic", strategy="short seller")],
        direction="SHORT",
        conviction_score=7.4,
        summary="Netflix subscriber growth story is over. Ad tier economics do not pencil out at scale.",
        views_per_hour=8300.0,
        corroboration_count=1,
        direction_flip=True,
        conflict_group="",
        tier="act_now",
        post_created_at=datetime(2026, 4, 18, 7, 12),
        market_cap_class="Large",
    ),
    MorningSignal(
        ticker="TSLA",
        posters=[Poster(handle="ValueInvestor99", strategy="value investor")],
        direction="LONG",
        conviction_score=6.1,
        summary="TSLA energy business is worth more than the car business alone. Market ignoring it.",
        views_per_hour=5100.0,
        corroboration_count=1,
        direction_flip=False,
        conflict_group="opposing_exists",
        tier="act_now",
        post_created_at=datetime(2026, 4, 18, 7, 32),
        market_cap_class="Large",
    ),
    MorningSignal(
        ticker="NOVA",
        posters=[Poster(handle="hkuppy", strategy="macro trader")],
        direction="LONG",
        conviction_score=4.2,
        summary="Solar permitting reform is the real catalyst here. Consensus underestimates the timing.",
        views_per_hour=4200.0,
        corroboration_count=1,
        direction_flip=False,
        conflict_group="",
        tier="watch",
        post_created_at=datetime(2026, 4, 18, 5, 58),
        market_cap_class="Small",
    ),
    MorningSignal(
        ticker="RIVN",
        posters=[Poster(handle="StockJabber", strategy="short seller")],
        direction="SHORT",
        conviction_score=3.8,
        summary="RIVN cash burn rate means dilution is coming faster than bulls expect. Run the math.",
        views_per_hour=2800.0,
        corroboration_count=1,
        direction_flip=False,
        conflict_group="",
        tier="watch",
        post_created_at=datetime(2026, 4, 18, 8, 15),
        market_cap_class="Small",
    ),
]


# ---------------------------------------------------------------------------
# __main__ entrypoint
# ---------------------------------------------------------------------------

def _main() -> None:
    parser = argparse.ArgumentParser(description="Morning alert renderer")
    parser.add_argument("--demo", action="store_true", help="Render and send the hardcoded fixture")
    parser.add_argument("--demo-empty", action="store_true", help="Render and send the no-signals state")
    args = parser.parse_args()

    if not args.demo and not args.demo_empty:
        parser.print_help()
        sys.exit(0)

    if args.demo_empty:
        messages = render_morning(act_now=[], watch=[])
        for msg in messages:
            sys.stdout.write(msg + "\n")

        from influence_monitor.delivery.registry import DELIVERY_REGISTRY
        try:
            provider = DELIVERY_REGISTRY["twilio"]()
        except Exception as exc:
            logger.error("Delivery instantiation failed: %s", exc)
            sys.exit(1)

        for msg in messages:
            success = provider.send(msg)
            if not success:
                logger.error("Morning alert delivery failed.")
                sys.exit(1)

        logger.info("Morning alert (empty) sent successfully.")
        sys.exit(0)

    act_now = [s for s in DEMO_FIXTURE if s.tier == "act_now"]
    watch = [s for s in DEMO_FIXTURE if s.tier == "watch"]

    messages = render_morning(act_now, watch)
    for msg in messages:
        sys.stdout.write(msg + "\n")

    from influence_monitor.delivery.registry import DELIVERY_REGISTRY
    try:
        provider = DELIVERY_REGISTRY["twilio"]()
    except Exception as exc:
        logger.error("Delivery instantiation failed: %s", exc)
        sys.exit(1)

    for msg in messages:
        success = provider.send(msg)
        if not success:
            logger.error("Morning alert delivery failed.")
            sys.exit(1)

    logger.info("Morning alert sent successfully.")
    sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    _main()
