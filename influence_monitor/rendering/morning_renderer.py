# Morning alert composer for WhatsApp delivery — TASK-002.
# Renders Act Now and Watch List sections from MorningSignal dataclasses.
# Run with: python -m influence_monitor.rendering.morning_renderer --demo

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass

logger = logging.getLogger(__name__)

DISCLAIMER = "_This is information about public posts, not investment advice. Do your own research._"

_DOTS = ["○○○○○", "●○○○○", "●●○○○", "●●●○○", "●●●●○", "●●●●●"]


@dataclass
class MorningSignal:
    ticker: str
    poster_handle: str
    direction: str              # "LONG" | "SHORT"
    conviction_score: float     # 0.0–10.0
    summary: str
    views_per_hour: float
    corroboration_count: int
    direction_flip: bool
    conflict_group: str         # "" | "opposing_exists"
    tier: str                   # "act_now" | "watch"


def _conviction_dots(score: float) -> str:
    if score >= 9.0:
        return _DOTS[5]
    if score >= 7.0:
        return _DOTS[4]
    if score >= 5.0:
        return _DOTS[3]
    if score >= 3.0:
        return _DOTS[2]
    if score >= 1.0:
        return _DOTS[1]
    return _DOTS[0]


def _render_signal_block(signal: MorningSignal, include_velocity: bool = False) -> str:
    lines: list[str] = []
    lines.append(f"*{signal.direction} ${signal.ticker}*")
    lines.append(f"Score: {_conviction_dots(signal.conviction_score)}")
    lines.append(f"@{signal.poster_handle}")

    if signal.corroboration_count >= 2:
        lines.append(f"CORROBORATED — {signal.corroboration_count} posters")
    if signal.direction_flip:
        lines.append("⚠️ Direction changed")
    if signal.conflict_group == "opposing_exists":
        lines.append("⚠️ Conflicted — opposing view exists")

    summary_fragment = signal.summary[:150]
    if len(signal.summary) > 150:
        summary_fragment += "..."
    lines.append(f'> "{summary_fragment}"')

    if include_velocity:
        lines.append(f"_{int(signal.views_per_hour):,} views/hr — watch if it keeps spreading_")

    return "\n".join(lines)


def render_morning(act_now: list[MorningSignal], watch: list[MorningSignal]) -> str:
    if not act_now and not watch:
        parts = [
            "No signals above threshold. No emerging velocity detected.",
            "_Nothing actionable overnight — check back this evening._",
            "",
            DISCLAIMER,
        ]
        return "\n".join(parts)

    top_act = sorted(act_now, key=lambda s: s.conviction_score, reverse=True)[:5]
    top_watch = sorted(watch, key=lambda s: s.views_per_hour, reverse=True)[:5]

    sections: list[str] = []

    sections.append("━━━ ACT NOW ━━━")
    if top_act:
        sections.append(f"{len(top_act)} signal{'s' if len(top_act) != 1 else ''} above threshold")
        sections.append("")
        for sig in top_act:
            sections.append(_render_signal_block(sig, include_velocity=False))
            sections.append("")
    else:
        sections.append("No high-conviction signals")
        sections.append("")

    sections.append("━━━ WATCH LIST ━━━")
    if top_watch:
        sections.append(f"{len(top_watch)} signal{'s' if len(top_watch) != 1 else ''} gaining momentum")
        sections.append("")
        for sig in top_watch:
            sections.append(_render_signal_block(sig, include_velocity=True))
            sections.append("")
    else:
        sections.append("No signals gaining momentum")
        sections.append("")

    sections.append(DISCLAIMER)

    result = "\n".join(sections)

    if len(result) > 4000:
        logger.warning("Rendered morning alert exceeds 4000 chars (%d); truncating.", len(result))
        result = result[:3990] + "\n[truncated]"

    return result


# ---------------------------------------------------------------------------
# Hardcoded fixture data
# ---------------------------------------------------------------------------

DEMO_FIXTURE: list[MorningSignal] = [
    MorningSignal(
        ticker="FNMA",
        poster_handle="BillAckman",
        direction="LONG",
        conviction_score=9.2,
        summary="Fannie Mae is absurdly underpriced. Regulatory unlock is imminent and this goes 10x from here.",
        views_per_hour=12500.0,
        corroboration_count=2,
        direction_flip=False,
        conflict_group="",
        tier="act_now",
    ),
    MorningSignal(
        ticker="NFLX",
        poster_handle="WallStCynic",
        direction="SHORT",
        conviction_score=7.4,
        summary="Netflix subscriber growth story is over. Ad tier economics do not pencil out at scale.",
        views_per_hour=8300.0,
        corroboration_count=1,
        direction_flip=True,
        conflict_group="",
        tier="act_now",
    ),
    MorningSignal(
        ticker="TSLA",
        poster_handle="ValueInvestor99",
        direction="LONG",
        conviction_score=6.1,
        summary="TSLA energy business is worth more than the car business alone. Market ignoring it.",
        views_per_hour=5100.0,
        corroboration_count=1,
        direction_flip=False,
        conflict_group="opposing_exists",
        tier="act_now",
    ),
    MorningSignal(
        ticker="NOVA",
        poster_handle="hkuppy",
        direction="LONG",
        conviction_score=4.2,
        summary="Solar permitting reform is the real catalyst here. Consensus underestimates the timing.",
        views_per_hour=4200.0,
        corroboration_count=1,
        direction_flip=False,
        conflict_group="",
        tier="watch",
    ),
    MorningSignal(
        ticker="RIVN",
        poster_handle="StockJabber",
        direction="SHORT",
        conviction_score=3.8,
        summary="RIVN cash burn rate means dilution is coming faster than bulls expect. Run the math.",
        views_per_hour=2800.0,
        corroboration_count=1,
        direction_flip=False,
        conflict_group="",
        tier="watch",
    ),
]


# ---------------------------------------------------------------------------
# __main__ entrypoint
# ---------------------------------------------------------------------------

def _main() -> None:
    parser = argparse.ArgumentParser(description="Morning alert renderer")
    parser.add_argument("--demo", action="store_true", help="Render and send the hardcoded fixture")
    args = parser.parse_args()

    if not args.demo:
        parser.print_help()
        sys.exit(0)

    act_now = [s for s in DEMO_FIXTURE if s.tier == "act_now"]
    watch = [s for s in DEMO_FIXTURE if s.tier == "watch"]

    rendered = render_morning(act_now, watch)
    sys.stdout.write(rendered + "\n")

    from influence_monitor.delivery.registry import DELIVERY_REGISTRY
    try:
        provider = DELIVERY_REGISTRY["twilio"]()
        success = provider.send(rendered)
    except Exception as exc:
        logger.error("Delivery instantiation failed: %s", exc)
        sys.exit(1)

    if not success:
        logger.error("Morning alert delivery failed.")
        sys.exit(1)

    logger.info("Morning alert sent successfully.")
    sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    _main()
