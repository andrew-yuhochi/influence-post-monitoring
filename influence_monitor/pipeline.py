"""Pipeline orchestrator — TASK-010b (morning) + TASK-013 (evening) + TASK-014 (poll).

Top-level synchronous entry point for the influence-post-monitoring pipeline.
Wires together all Milestone 2, 3, and 4 components.

CLI:
    python -m influence_monitor.pipeline morning [--dry-run] [--account-limit N] [--use-fixtures]
    python -m influence_monitor.pipeline evening [--dry-run] [--use-fixtures]
    python -m influence_monitor.pipeline poll [--dry-run]

The orchestrator is fully synchronous from the outside; async twikit calls
are wrapped with asyncio.run() at the ingestion call-site only.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any

from influence_monitor.config import Settings
from influence_monitor.db.repository import SignalRepository
from influence_monitor.delivery.registry import DELIVERY_REGISTRY
from influence_monitor.extraction.equity_whitelist import SymbolWhitelist
from influence_monitor.extraction.ticker_extractor import TickerExtractor
from influence_monitor.ingestion.account_registry import AccountRegistry
from influence_monitor.ingestion.base import RawPost
from influence_monitor.ingestion.registry import SOURCE_REGISTRY
from influence_monitor.market_data.trading_calendar import TradingCalendar
from influence_monitor.market_data.yfinance_client import YFinanceClient
from influence_monitor.outcome.outcome_engine import OutcomeEngine
from influence_monitor.outcome.scorecard_aggregator import ScorecardAggregator
from influence_monitor.rendering.evening_renderer import render_evening
from influence_monitor.rendering.morning_renderer import (
    MorningSignal,
    Poster,
    render_morning,
)
from influence_monitor.scoring.amplifier import AmplifierFetcher
from influence_monitor.scoring.claude_client import ClaudeHaikuClient
from influence_monitor.scoring.market_cap_resolver import MarketCapResolver
from influence_monitor.scoring.scoring_engine import ScoringEngine, ScoringInput

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent
_FIXTURES_PATH = _PROJECT_ROOT / "tests" / "fixtures" / "sample_signals.json"
_OUTCOMES_FIXTURES_PATH = _PROJECT_ROOT / "tests" / "fixtures" / "sample_outcomes.json"

# Non-signal columns to strip before inserting into the signals table
_FIXTURE_META_FIELDS = {
    "_comment", "post_text", "posted_at", "account_handle", "corroboration_count"
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _send_operational_message(
    settings: Settings,
    text: str,
    dry_run: bool = False,
) -> None:
    """Send an operational WhatsApp alert (pipeline failure notification).

    Tries the primary delivery provider first; falls back to callmebot.
    Errors are logged but never re-raised — operational messages must not
    compound a failure.
    """
    if dry_run:
        logger.info("[DRY-RUN] Operational message: %s", text)
        return

    for provider_name in [settings.delivery_primary, settings.delivery_fallback]:
        try:
            delivery_cls = DELIVERY_REGISTRY.get(provider_name)
            if delivery_cls is None:
                continue
            provider = delivery_cls()  # may raise if env vars missing
            ok = provider.send(text)
            if ok:
                logger.info("Operational message sent via %s", provider_name)
                return
        except Exception as exc:  # noqa: BLE001
            logger.error("Operational send via %s failed: %s", provider_name, exc)

    logger.error("All delivery providers failed for operational message")


def _build_morning_signals(
    signals_rows: list[dict[str, Any]],
    accounts_by_id: dict[int, dict[str, Any]],
    repo: SignalRepository,
) -> tuple[list[MorningSignal], list[MorningSignal]]:
    """Convert scored signal rows (from DB) into MorningSignal objects for rendering.

    Returns (act_now_list, watch_list).
    """
    act_now: list[MorningSignal] = []
    watch: list[MorningSignal] = []

    for row in signals_rows:
        tier = row.get("tier", "UNSCORED")
        if tier not in ("ACT_NOW", "WATCH"):
            continue

        account_id = row.get("account_id")
        account = accounts_by_id.get(account_id) if account_id else {}
        handle = (
            row.get("account_handle")
            or (account.get("handle") if account else None)
            or "unknown"
        )
        strategy = (account.get("angle") or "investor") if account else "investor"
        posted_at_raw = row.get("posted_at") or row.get("created_at") or ""
        try:
            posted_at = datetime.fromisoformat(str(posted_at_raw).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            posted_at = datetime.now(tz=timezone.utc)

        signal = MorningSignal(
            ticker=row["ticker"],
            posters=[Poster(handle=handle, strategy=strategy)],
            direction=row.get("direction", "LONG"),
            conviction_score=float(row.get("final_score") or row.get("conviction_score") or 0.0),
            summary=row.get("key_claim") or row.get("rationale") or "",
            views_per_hour=float(row.get("views_per_hour") or 0.0),
            corroboration_count=1,
            direction_flip=bool(row.get("direction_flip")),
            conflict_group=row.get("conflict_group") or "",
            tier="act_now" if tier == "ACT_NOW" else "watch",
            post_created_at=posted_at,
            market_cap_class=row.get("market_cap_class") or "",
        )

        if tier == "ACT_NOW":
            act_now.append(signal)
        else:
            watch.append(signal)

    return act_now, watch


# ---------------------------------------------------------------------------
# PipelineOrchestrator
# ---------------------------------------------------------------------------

class PipelineOrchestrator:
    """Synchronous morning pipeline orchestrator.

    Wires all Milestone 2 components. Call ``run_morning(run_date)`` to
    execute the full 13-step morning pipeline.
    """

    def __init__(self, settings: Settings, repo: SignalRepository) -> None:
        self._settings = settings
        self._repo = repo

        # Trading calendar — used for is_trading_day and collection window
        self._calendar = TradingCalendar()

        # Social source (twikit by default)
        source_cls = SOURCE_REGISTRY.get(settings.social_source)
        if source_cls is None:
            raise ValueError(f"Unknown social source: {settings.social_source!r}")
        self._source = source_cls(settings)

        # Extraction
        self._whitelist = SymbolWhitelist.load()
        self._extractor = TickerExtractor(self._whitelist)

        # Scoring
        self._llm_client = ClaudeHaikuClient(settings, repo)
        self._scoring_engine = ScoringEngine(repo)
        self._amplifier = AmplifierFetcher(repo)
        self._market_cap_resolver = MarketCapResolver(repo)

        # Delivery — lazy: classes stored, instances created on first send
        # This avoids env-var errors in dry-run / use-fixtures mode
        self._primary_delivery_cls = DELIVERY_REGISTRY.get(settings.delivery_primary)
        self._fallback_delivery_cls = DELIVERY_REGISTRY.get(settings.delivery_fallback)

        # Registry for accounts
        self._account_registry = AccountRegistry(
            repo=repo,
            source=self._source,
            tenant_id=1,
        )

        # Market data client for outcome computation
        self._market_client = YFinanceClient()

        # Evening pipeline components
        self._outcome_engine = OutcomeEngine(
            market_client=self._market_client,
            repo=repo,
            trading_calendar=self._calendar,
        )
        self._scorecard_aggregator = ScorecardAggregator(
            repo=repo,
            trading_calendar=self._calendar,
        )

    # ------------------------------------------------------------------
    # Step helpers
    # ------------------------------------------------------------------

    def _deliver(self, text: str, kind: str, dry_run: bool) -> None:
        """Send a WhatsApp message through primary → fallback chain.

        In dry_run mode, writes to stdout only.
        """
        if dry_run:
            print(text)
            return

        for delivery_cls, name in [
            (self._primary_delivery_cls, self._settings.delivery_primary),
            (self._fallback_delivery_cls, self._settings.delivery_fallback),
        ]:
            if delivery_cls is None:
                continue
            try:
                delivery = delivery_cls()
                ok = delivery.send(text)
                if ok:
                    self._repo.log_message_sent(
                        kind=kind,
                        delivery=name,
                        status="ok",
                        body_preview=text[:200],
                    )
                    logger.info("Message delivered via %s", name)
                    return
                logger.warning("Delivery via %s returned False — trying fallback", name)
            except Exception as exc:  # noqa: BLE001
                logger.error("Delivery via %s raised: %s", name, exc)

        # Both failed — log failed row
        self._repo.log_message_sent(
            kind=kind,
            delivery="all_failed",
            status="failed",
            body_preview=text[:200],
            error_message="All providers failed",
        )
        logger.error("All delivery providers failed for %s message", kind)

    # ------------------------------------------------------------------
    # Fixture mode
    # ------------------------------------------------------------------

    def _run_fixtures_mode(
        self,
        run_date: date,
        dry_run: bool,
    ) -> None:
        """Load sample_signals.json, insert posts + signals, then render and deliver.

        Bypasses steps 1–9 (no twikit, no Claude). Executes steps 10–13.
        """
        logger.info("STEP [fixtures] START — loading sample_signals.json")

        raw_signals = json.loads(_FIXTURES_PATH.read_text())
        today_str = run_date.isoformat()

        # Ensure DB is initialised
        self._repo.init_schema()
        self._repo.seed(phone_e164=self._settings.recipient_phone_e164)

        # Delete any existing signals for today to avoid duplicate renders
        if not dry_run:
            self._repo._execute_write(
                "DELETE FROM signals WHERE signal_date = ? AND tenant_id = 1",
                [today_str],
            )

        scoring_cfg = self._repo.get_scoring_config(tenant_id=1)
        accounts = self._repo.get_accounts_by_status("primary", tenant_id=1)
        accounts_by_handle: dict[str, dict[str, Any]] = {
            a["handle"].lower(): a for a in accounts
        }

        inserted_signal_ids: list[int | None] = []

        for sig in raw_signals:
            handle = sig.get("account_handle", "BillAckman")
            account = accounts_by_handle.get(handle.lower())
            if account is None:
                # Insert a placeholder account so FK constraint is satisfied
                self._repo.upsert_account(
                    tenant_id=1,
                    handle=handle,
                    display_name=handle,
                    credibility_score=7.0,
                    status="primary",
                )
                # Reload
                accounts = self._repo.get_accounts_by_status("primary", tenant_id=1)
                accounts_by_handle = {a["handle"].lower(): a for a in accounts}
                account = accounts_by_handle.get(handle.lower())

            account_id = account["id"] if account else 1

            # Insert a post row for FK satisfaction
            posted_at_str = sig.get("posted_at", f"{today_str}T09:00:00Z")
            try:
                posted_at = datetime.fromisoformat(posted_at_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                posted_at = datetime.now(tz=timezone.utc)

            post_id = self._repo.insert_post(
                tenant_id=1,
                account_id=account_id,
                external_id=f"fixture-{uuid.uuid4().hex[:8]}",
                source_type="fixture",
                text=sig.get("post_text", ""),
                posted_at=posted_at,
                fetched_at=datetime.now(tz=timezone.utc),
                view_count=sig.get("engagement_views"),
                repost_count=sig.get("engagement_reposts"),
            )
            if post_id is None:
                # Fallback: query for existing post (duplicate key path)
                post_id = 1

            # Build signal kwargs, stripping non-column meta fields
            signal_kwargs: dict[str, Any] = {
                k: v for k, v in sig.items()
                if k not in _FIXTURE_META_FIELDS
            }
            signal_kwargs["tenant_id"] = 1
            signal_kwargs["user_id"] = 1
            signal_kwargs["post_id"] = post_id
            signal_kwargs["account_id"] = account_id
            signal_kwargs["signal_date"] = today_str

            # Insert signal (ignore if already exists)
            sid = self._repo.insert_signal(**signal_kwargs)
            inserted_signal_ids.append(sid)

        logger.info("STEP [fixtures] DONE — inserted %d signals", len(inserted_signal_ids))

        # Steps 10–13: classify → render → deliver
        # Build in-memory signal rows from fixture data (avoids querying stale DB rows)
        signal_rows = [
            {
                **{k: v for k, v in sig.items() if k not in _FIXTURE_META_FIELDS},
                "account_handle": sig.get("account_handle", ""),
                "posted_at": sig.get("posted_at", f"{today_str}T09:00:00Z"),
                "signal_date": today_str,
                "account_id": 1,
            }
            for sig in raw_signals
        ]

        # Build lookup for accounts
        accounts_by_id = {a["id"]: a for a in self._repo.get_accounts_by_status("primary", tenant_id=1)}

        act_now_signals, watch_signals = _build_morning_signals(
            signal_rows, accounts_by_id, self._repo
        )

        logger.info(
            "STEP [fixtures] Rendering: %d ACT_NOW, %d WATCH signals",
            len(act_now_signals), len(watch_signals),
        )

        messages = render_morning(act_now_signals, watch_signals)
        for msg in messages:
            self._deliver(msg, kind="morning", dry_run=dry_run)

        if not dry_run:
            self._repo.upsert_daily_summary(
                tenant_id=1,
                summary_date=today_str,
                run_type="morning",
                accounts_fetched=3,
                signals_scored=len(raw_signals),
                signals_act_now=len(act_now_signals),
                signals_watch=len(watch_signals),
                pipeline_status="ok",
                duration_seconds=0.0,
            )

        logger.info("STEP [fixtures] Pipeline complete — fixtures mode")

    # ------------------------------------------------------------------
    # Main entry: run_morning
    # ------------------------------------------------------------------

    def run_morning(
        self,
        run_date: date,
        account_limit: int | None = None,
        dry_run: bool = False,
        use_fixtures: bool = False,
    ) -> None:
        """Execute the 13-step morning alert pipeline.

        Parameters
        ----------
        run_date:
            The date for which to run the morning pipeline (typically today).
        account_limit:
            If set, only process the first N active accounts.
        dry_run:
            Render to stdout; do not write to signals/messages_sent/daily_summaries.
        use_fixtures:
            Bypass twikit + Claude; load pre-scored signals from
            tests/fixtures/sample_signals.json and run steps 10–13 only.
        """
        start_ts = monotonic()
        logger.info(
            "run_morning START — date=%s dry_run=%s use_fixtures=%s account_limit=%s",
            run_date, dry_run, use_fixtures, account_limit,
        )

        # Fixtures short-circuit
        if use_fixtures:
            self._run_fixtures_mode(run_date, dry_run=dry_run)
            return

        accounts_fetched = 0
        signals_scored = 0
        signals_act_now = 0
        signals_watch = 0

        try:
            # ------------------------------------------------------------------
            # STEP 1 — Validate accounts via AccountRegistry
            # ------------------------------------------------------------------
            logger.info("STEP 1 START — AccountRegistry.validate_and_promote")
            try:
                active_accounts = asyncio.run(self._account_registry.validate_and_promote())
            except Exception as exc:  # noqa: BLE001
                logger.warning("validate_and_promote raised: %s — using get_active_accounts()", exc)
                active_accounts = self._account_registry.get_active_accounts()

            if not active_accounts:
                logger.error("All accounts inactive — aborting pipeline")
                _send_operational_message(
                    self._settings,
                    f"⚠️ Pipeline FAILED — {run_date} morning\n"
                    f"Component: AccountRegistry\n"
                    f"Error: All primary accounts inactive\n"
                    f"Check accounts table.",
                    dry_run=dry_run,
                )
                if not dry_run:
                    self._repo.upsert_daily_summary(
                        tenant_id=1,
                        summary_date=run_date.isoformat(),
                        run_type="morning",
                        accounts_fetched=0,
                        signals_scored=0,
                        signals_act_now=0,
                        signals_watch=0,
                        pipeline_status="failed",
                        error_message="All primary accounts inactive",
                        duration_seconds=monotonic() - start_ts,
                    )
                return

            if account_limit is not None:
                active_accounts = active_accounts[:account_limit]

            logger.info("STEP 1 DONE — %d active accounts (limit=%s)", len(active_accounts), account_limit)

            # ------------------------------------------------------------------
            # STEP 2 — Determine collection window via TradingCalendar
            # ------------------------------------------------------------------
            logger.info("STEP 2 START — TradingCalendar.is_trading_day + collection_window")

            if not self._calendar.is_trading_day(run_date):
                logger.info(
                    "STEP 2 DONE — %s is not a trading day — short-circuiting", run_date
                )
                return

            now_et = datetime.now(tz=self._calendar._calendar.tz if hasattr(self._calendar._calendar, 'tz') else timezone.utc)
            from zoneinfo import ZoneInfo
            et = ZoneInfo("America/New_York")
            now_et = datetime.now(tz=et)
            try:
                prev_close_dt, send_time_dt = self._calendar.collection_window(now_et)
            except Exception as exc:  # noqa: BLE001
                logger.warning("collection_window failed (%s) — using 24h lookback", exc)
                from datetime import timedelta
                prev_close_dt = now_et - timedelta(hours=24)
                send_time_dt = now_et

            logger.info(
                "STEP 2 DONE — collection window: %s → %s",
                prev_close_dt, send_time_dt,
            )

            # ------------------------------------------------------------------
            # STEP 3 — Fetch posts for each account
            # ------------------------------------------------------------------
            logger.info("STEP 3 START — fetch posts for %d accounts", len(active_accounts))

            all_posts: list[RawPost] = []
            success_count = 0
            failure_count = 0

            try:
                all_posts, success_count, failure_count = asyncio.run(
                    self._account_registry.fetch_all_accounts(
                        since=prev_close_dt,
                        max_count=self._settings.max_posts_per_account,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.error("fetch_all_accounts raised: %s", exc)

            accounts_fetched = success_count
            logger.info(
                "STEP 3 DONE — %d posts from %d/%d accounts (%d failed)",
                len(all_posts), success_count, len(active_accounts), failure_count,
            )

            # ------------------------------------------------------------------
            # STEP 4 — Extract tickers per post
            # ------------------------------------------------------------------
            logger.info("STEP 4 START — ticker extraction for %d posts", len(all_posts))

            post_tickers: dict[str, list] = {}  # external_id → [ExtractedTicker]
            for post in all_posts:
                try:
                    tickers = self._extractor.extract(post.text)
                    post_tickers[post.external_id] = tickers
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "Ticker extraction failed for post %s: %s", post.external_id, exc
                    )
                    post_tickers[post.external_id] = []

            logger.info(
                "STEP 4 DONE — extracted tickers from %d posts",
                sum(1 for t in post_tickers.values() if t),
            )

            # ------------------------------------------------------------------
            # STEP 5 — Claude-score each post
            # ------------------------------------------------------------------
            logger.info("STEP 5 START — LLM scoring for posts with extracted tickers")

            post_scores: dict[str, Any] = {}  # external_id → PostScore
            for post in all_posts:
                tickers = post_tickers.get(post.external_id, [])
                if not tickers:
                    continue
                try:
                    score = self._llm_client.score_post(post.text, post.author_handle)
                    post_scores[post.external_id] = score
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "LLM scoring failed for post %s: %s", post.external_id, exc
                    )

            logger.info("STEP 5 DONE — scored %d posts", len(post_scores))

            # ------------------------------------------------------------------
            # STEP 6 — Build ScoringInputs + compute F1/F2a/F2b/F3/F5
            # ------------------------------------------------------------------
            logger.info("STEP 6 START — ScoringEngine (F1/F2a/F2b/F3/F5)")

            # Build account lookup
            accounts_by_handle: dict[str, dict[str, Any]] = {
                acc["handle"].lower(): acc for acc in active_accounts
            }

            # Build F3 consensus counts: ticker → direction → set of handles
            ticker_dir_posters: dict[str, dict[str, set[str]]] = {}
            for post in all_posts:
                ps = post_scores.get(post.external_id)
                if ps is None:
                    continue
                tickers = post_tickers.get(post.external_id, [])
                for t in tickers:
                    if t.confidence == "LOW":
                        continue
                    ticker_dir_posters.setdefault(t.ticker, {}).setdefault(
                        ps.direction, set()
                    ).add(post.author_handle.lower())

            scoring_inputs: list[ScoringInput] = []
            for post in all_posts:
                ps = post_scores.get(post.external_id)
                if ps is None:
                    continue

                tickers = post_tickers.get(post.external_id, [])
                account = accounts_by_handle.get(post.author_handle.lower(), {})
                cred = float(account.get("credibility_score") or 5.0)

                for t in tickers:
                    if t.confidence == "LOW":
                        continue
                    ticker_dirs = ticker_dir_posters.get(t.ticker, {})
                    same_dir_count = len(ticker_dirs.get(ps.direction, set()))
                    total_distinct = sum(len(v) for v in ticker_dirs.values())

                    # Apply MarketCapResolver for F5
                    try:
                        cap_class, _ = self._market_cap_resolver.resolve(t.ticker)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("MarketCapResolver failed for %s: %s", t.ticker, exc)
                        cap_class = "Micro"

                    inp = ScoringInput(
                        post_score=ps,
                        raw_post=post,
                        account_credibility=cred,
                        posted_at=post.posted_at,
                        collection_window_start=prev_close_dt,
                        account_handle=post.author_handle,
                        distinct_same_direction_posters=same_dir_count,
                        total_distinct_posters_on_ticker=total_distinct,
                        ticker=t.ticker,
                    )
                    scoring_inputs.append(inp)

            scored_signals = self._scoring_engine.score(scoring_inputs)
            logger.info("STEP 6 DONE — %d scored signals", len(scored_signals))

            # ------------------------------------------------------------------
            # STEP 7 — ConflictResolver (already applied inside ScoringEngine.score)
            # ------------------------------------------------------------------
            logger.info("STEP 7 START — ConflictResolver (embedded in ScoringEngine)")
            # ConflictResolver.resolve() is called inside ScoringEngine.score(),
            # so its results are already reflected in scored_signals.
            logger.info("STEP 7 DONE — conflict resolution applied")

            # ------------------------------------------------------------------
            # STEP 8 — Classify into ACT_NOW / WATCH / UNSCORED
            # ------------------------------------------------------------------
            logger.info("STEP 8 START — tier classification (embedded in ScoringEngine)")
            # Tier is set in ScoringEngine.score(); no separate step needed here.
            act_now_signals_raw = [s for s in scored_signals if s.tier == "ACT_NOW"]
            watch_signals_raw = [s for s in scored_signals if s.tier == "WATCH"]
            logger.info(
                "STEP 8 DONE — ACT_NOW=%d WATCH=%d UNSCORED=%d",
                len(act_now_signals_raw),
                len(watch_signals_raw),
                sum(1 for s in scored_signals if s.tier == "UNSCORED"),
            )

            # ------------------------------------------------------------------
            # STEP 9 — AmplifierFetcher for ACT_NOW signals only
            # ------------------------------------------------------------------
            logger.info("STEP 9 START — AmplifierFetcher for %d ACT_NOW signals", len(act_now_signals_raw))

            # Map external_id → DB post_id (needed for AmplifierFetcher FK)
            post_ext_to_dbid: dict[str, int] = {}
            for post in all_posts:
                rows = self._repo._execute(
                    "SELECT id FROM posts WHERE source_type = ? AND external_id = ?",
                    [post.source_type, post.external_id],
                )
                if rows:
                    post_ext_to_dbid[post.external_id] = rows[0]["id"]

            # Map external_id → RawPost
            posts_by_ext: dict[str, RawPost] = {p.external_id: p for p in all_posts}

            for signal in act_now_signals_raw:
                # Find the RawPost that generated this signal
                matching_post: RawPost | None = None
                for post in all_posts:
                    if (
                        post.author_handle.lower() == signal.account_handle.lower()
                        and signal.ticker in [t.ticker for t in post_tickers.get(post.external_id, [])]
                    ):
                        matching_post = post
                        break

                if matching_post is None:
                    logger.debug("No matching post for ACT_NOW signal %s", signal.ticker)
                    continue

                db_post_id = post_ext_to_dbid.get(matching_post.external_id)
                if db_post_id is None:
                    continue

                try:
                    f4_score = self._amplifier.fetch_and_score(
                        post=matching_post,
                        source=self._source,
                        post_db_id=db_post_id,
                        tier="ACT_NOW",
                    )
                    signal.score_amplifier = f4_score
                except Exception as exc:  # noqa: BLE001
                    logger.warning("AmplifierFetcher failed for %s: %s", signal.ticker, exc)

            logger.info("STEP 9 DONE — amplifier scores applied")

            # ------------------------------------------------------------------
            # STEP 10 — Persist signals to DB
            # ------------------------------------------------------------------
            logger.info("STEP 10 START — persisting %d signals", len(scored_signals))

            if not dry_run:
                accounts_by_handle_lower = {
                    acc["handle"].lower(): acc for acc in active_accounts
                }
                post_ext_to_account: dict[str, dict[str, Any]] = {}
                for post in all_posts:
                    acc = accounts_by_handle_lower.get(post.author_handle.lower(), {})
                    post_ext_to_account[post.external_id] = acc

                for signal in scored_signals:
                    # Find the matching post for this signal
                    matching_post = None
                    for post in all_posts:
                        tickers_for_post = [t.ticker for t in post_tickers.get(post.external_id, [])]
                        if (
                            post.author_handle.lower() == signal.account_handle.lower()
                            and signal.ticker in tickers_for_post
                        ):
                            matching_post = post
                            break

                    if matching_post is None:
                        continue

                    db_post_id = post_ext_to_dbid.get(matching_post.external_id, 1)
                    acc = post_ext_to_account.get(matching_post.external_id, {})
                    account_id = acc.get("id", 1)

                    # Get market cap for this ticker
                    try:
                        cap_class, liq_mod = self._market_cap_resolver.resolve(signal.ticker)
                    except Exception:  # noqa: BLE001
                        cap_class = "Micro"
                        liq_mod = 1.3

                    self._repo.insert_signal(
                        tenant_id=1,
                        user_id=1,
                        post_id=db_post_id,
                        account_id=account_id,
                        signal_date=run_date.isoformat(),
                        ticker=signal.ticker,
                        extraction_confidence=signal.extraction_confidence,
                        market_cap_class=cap_class,
                        direction=signal.direction,
                        conviction_level=signal.conviction_level,
                        argument_quality=signal.argument_quality,
                        time_horizon=signal.time_horizon,
                        market_moving_potential=signal.market_moving_potential,
                        key_claim=signal.key_claim,
                        rationale=signal.rationale,
                        llm_model_version=signal.llm_model_version,
                        llm_raw_response=signal.llm_raw_response,
                        llm_input_tokens=signal.llm_input_tokens,
                        llm_output_tokens=signal.llm_output_tokens,
                        score_credibility=signal.score_credibility,
                        score_virality_abs=signal.score_virality_abs,
                        score_virality_vel=signal.score_virality_vel,
                        score_consensus=signal.score_consensus,
                        score_amplifier=signal.score_amplifier,
                        liquidity_modifier=liq_mod,
                        conviction_score=signal.conviction_score,
                        direction_flip=int(signal.direction_flip),
                        conflict_group=signal.conflict_group,
                        penalty_applied=signal.penalty_applied,
                        final_score=signal.final_score,
                        tier=signal.tier,
                        engagement_views=signal.engagement_views,
                        engagement_reposts=signal.engagement_reposts,
                        views_per_hour=signal.views_per_hour,
                    )

            signals_scored = len(scored_signals)
            signals_act_now = len(act_now_signals_raw)
            signals_watch = len(watch_signals_raw)
            logger.info("STEP 10 DONE — persisted %d signals", signals_scored)

            # ------------------------------------------------------------------
            # STEP 11 — Render morning alert
            # ------------------------------------------------------------------
            logger.info("STEP 11 START — rendering morning alert")

            # Load accounts by ID for renderer
            all_active = self._repo.get_accounts_by_status("primary", tenant_id=1)
            accounts_by_id: dict[int, dict[str, Any]] = {a["id"]: a for a in all_active}

            # Build MorningSignal objects from scored results
            act_now_morning: list[MorningSignal] = []
            watch_morning: list[MorningSignal] = []

            for signal in sorted(act_now_signals_raw, key=lambda s: s.final_score, reverse=True)[:5]:
                acc = accounts_by_handle.get(signal.account_handle.lower(), {})
                try:
                    cap_class, _ = self._market_cap_resolver.resolve(signal.ticker)
                except Exception:  # noqa: BLE001
                    cap_class = "Micro"
                # Find posted_at from raw post
                posted_at = datetime.now(tz=timezone.utc)
                for post in all_posts:
                    if post.author_handle.lower() == signal.account_handle.lower():
                        posted_at = post.posted_at
                        break
                act_now_morning.append(MorningSignal(
                    ticker=signal.ticker,
                    posters=[Poster(
                        handle=signal.account_handle,
                        strategy=acc.get("angle") or "investor",
                    )],
                    direction=signal.direction,
                    conviction_score=signal.final_score,
                    summary=signal.key_claim or signal.rationale or "",
                    views_per_hour=signal.views_per_hour or 0.0,
                    corroboration_count=signal.distinct_same_direction_posters
                        if hasattr(signal, "distinct_same_direction_posters") else 1,
                    direction_flip=signal.direction_flip,
                    conflict_group=signal.conflict_group or "",
                    tier="act_now",
                    post_created_at=posted_at,
                    market_cap_class=cap_class,
                ))

            for signal in sorted(watch_signals_raw, key=lambda s: s.views_per_hour or 0.0, reverse=True)[:5]:
                acc = accounts_by_handle.get(signal.account_handle.lower(), {})
                try:
                    cap_class, _ = self._market_cap_resolver.resolve(signal.ticker)
                except Exception:  # noqa: BLE001
                    cap_class = "Micro"
                posted_at = datetime.now(tz=timezone.utc)
                for post in all_posts:
                    if post.author_handle.lower() == signal.account_handle.lower():
                        posted_at = post.posted_at
                        break
                watch_morning.append(MorningSignal(
                    ticker=signal.ticker,
                    posters=[Poster(
                        handle=signal.account_handle,
                        strategy=acc.get("angle") or "investor",
                    )],
                    direction=signal.direction,
                    conviction_score=signal.final_score,
                    summary=signal.key_claim or signal.rationale or "",
                    views_per_hour=signal.views_per_hour or 0.0,
                    corroboration_count=1,
                    direction_flip=signal.direction_flip,
                    conflict_group=signal.conflict_group or "",
                    tier="watch",
                    post_created_at=posted_at,
                    market_cap_class=cap_class,
                ))

            messages = render_morning(act_now_morning, watch_morning)
            logger.info(
                "STEP 11 DONE — %d message parts rendered", len(messages)
            )

            # ------------------------------------------------------------------
            # STEP 12 — Send via WhatsApp delivery chain
            # ------------------------------------------------------------------
            logger.info("STEP 12 START — WhatsApp delivery")
            for msg in messages:
                self._deliver(msg, kind="morning", dry_run=dry_run)
            logger.info("STEP 12 DONE — delivery complete")

            # ------------------------------------------------------------------
            # STEP 13 — Write daily_summaries and messages_sent
            # ------------------------------------------------------------------
            logger.info("STEP 13 START — writing daily_summaries")
            duration = monotonic() - start_ts

            if not dry_run:
                self._repo.upsert_daily_summary(
                    tenant_id=1,
                    summary_date=run_date.isoformat(),
                    run_type="morning",
                    accounts_active=len(active_accounts),
                    accounts_fetched=accounts_fetched,
                    signals_scored=signals_scored,
                    signals_act_now=signals_act_now,
                    signals_watch=signals_watch,
                    pipeline_status="ok",
                    duration_seconds=round(duration, 2),
                )

            logger.info(
                "STEP 13 DONE — pipeline complete in %.1fs | ACT_NOW=%d WATCH=%d",
                duration, signals_act_now, signals_watch,
            )

        except Exception as exc:  # noqa: BLE001
            logger.exception("Unhandled pipeline exception: %s", exc)
            _send_operational_message(
                self._settings,
                f"⚠️ Pipeline FAILED — {run_date} morning\n"
                f"Error: {type(exc).__name__}: {str(exc)[:200]}\n"
                f"Check Actions logs.",
                dry_run=dry_run,
            )
            if not dry_run:
                self._repo.upsert_daily_summary(
                    tenant_id=1,
                    summary_date=run_date.isoformat(),
                    run_type="morning",
                    accounts_fetched=accounts_fetched,
                    signals_scored=signals_scored,
                    signals_act_now=signals_act_now,
                    signals_watch=signals_watch,
                    pipeline_status="failed",
                    error_message=f"{type(exc).__name__}: {str(exc)[:500]}",
                    duration_seconds=round(monotonic() - start_ts, 2),
                )
            raise


    # ------------------------------------------------------------------
    # Evening fixtures mode
    # ------------------------------------------------------------------

    def _run_evening_fixtures_mode(
        self,
        run_date: date,
        dry_run: bool,
    ) -> None:
        """Load sample_outcomes.json, insert posts + signals (with outcome data), then
        run ScorecardAggregator → render_evening → deliver.

        Bypasses OutcomeEngine (outcomes are already populated in the fixture JSON).
        """
        logger.info("STEP [evening-fixtures] START — loading sample_outcomes.json")

        raw_outcomes = json.loads(_OUTCOMES_FIXTURES_PATH.read_text())
        today_str = run_date.isoformat()

        # Ensure DB is initialised
        self._repo.init_schema()
        self._repo.seed(phone_e164=self._settings.recipient_phone_e164)

        # Clear existing signals for today unconditionally — fixture inserts always
        # write to DB regardless of dry_run, so the delete must also always run to
        # keep each fixture run idempotent.
        self._repo.delete_signals_for_date(run_date, tenant_id=1)

        accounts = self._repo.get_accounts_by_status("primary", tenant_id=1)
        accounts_by_handle: dict[str, dict[str, Any]] = {
            a["handle"].lower(): a for a in accounts
        }

        # Non-signal / non-outcome meta fields to strip before inserting signals
        _OUTCOME_META_FIELDS = {
            "_comment", "post_text", "posted_at", "account_handle",
            "corroboration_count",
        }

        for sig in raw_outcomes:
            handle = sig.get("account_handle", "BillAckman")
            account = accounts_by_handle.get(handle.lower())
            if account is None:
                self._repo.upsert_account(
                    tenant_id=1,
                    handle=handle,
                    display_name=handle,
                    credibility_score=7.0,
                    status="primary",
                )
                accounts = self._repo.get_accounts_by_status("primary", tenant_id=1)
                accounts_by_handle = {a["handle"].lower(): a for a in accounts}
                account = accounts_by_handle.get(handle.lower())

            account_id = account["id"] if account else 1

            posted_at_str = sig.get("posted_at", f"{today_str}T16:00:00Z")
            try:
                posted_at = datetime.fromisoformat(posted_at_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                posted_at = datetime.now(tz=timezone.utc)

            post_id = self._repo.insert_post(
                tenant_id=1,
                account_id=account_id,
                external_id=f"outcome-fixture-{uuid.uuid4().hex[:8]}",
                source_type="fixture",
                text=sig.get("post_text", ""),
                posted_at=posted_at,
                fetched_at=datetime.now(tz=timezone.utc),
                view_count=sig.get("engagement_views"),
                repost_count=sig.get("engagement_reposts"),
            )
            if post_id is None:
                post_id = 1

            signal_kwargs: dict[str, Any] = {
                k: v for k, v in sig.items()
                if k not in _OUTCOME_META_FIELDS
            }
            signal_kwargs["tenant_id"] = 1
            signal_kwargs["user_id"] = 1
            signal_kwargs["post_id"] = post_id
            signal_kwargs["account_id"] = account_id
            signal_kwargs["signal_date"] = today_str

            self._repo.insert_signal(**signal_kwargs)

        logger.info(
            "STEP [evening-fixtures] DONE — inserted %d outcome signals",
            len(raw_outcomes),
        )

    # ------------------------------------------------------------------
    # Main entry: run_evening
    # ------------------------------------------------------------------

    def run_evening(
        self,
        run_date: date,
        dry_run: bool = False,
        use_fixtures: bool = False,
    ) -> None:
        """Execute the evening summary pipeline.

        Steps:
          1. Check trading day — skip entirely if not a trading day.
          2. (If use_fixtures) Insert sample_outcomes.json into DB.
          3. (If not use_fixtures) OutcomeEngine.compute_and_store(run_date).
          4. ScorecardAggregator.top_n_posters + trading_days_with_signals.
          5. render_evening.
          6. Deliver via WhatsApp.
          7. Upsert daily_summaries with run_type='evening'.

        Parameters
        ----------
        run_date:
            The date for which to run the evening pipeline (typically today).
        dry_run:
            Render to stdout; do not send WhatsApp or write DB.
        use_fixtures:
            Bypass OutcomeEngine; insert pre-computed outcomes from
            tests/fixtures/sample_outcomes.json and run from step 4 onward.
        """
        start_ts = monotonic()
        logger.info(
            "run_evening START — date=%s dry_run=%s use_fixtures=%s",
            run_date, dry_run, use_fixtures,
        )

        try:
            # ------------------------------------------------------------------
            # STEP 0 — Fixtures short-circuit (bypasses trading-day gate)
            # ------------------------------------------------------------------
            if use_fixtures:
                self._run_evening_fixtures_mode(run_date, dry_run=dry_run)
                # Skip the trading-day check and OutcomeEngine; continue from
                # STEP 3 onward so rendering + delivery still happen.
            else:
                # ------------------------------------------------------------------
                # STEP 1 — Check trading day
                # ------------------------------------------------------------------
                if not self._calendar.is_trading_day(run_date):
                    logger.info(
                        "run_evening: %s is not a trading day — skipping", run_date
                    )
                    return

                # ------------------------------------------------------------------
                # STEP 2 — Real OutcomeEngine
                # ------------------------------------------------------------------
                logger.info("STEP 2 START — OutcomeEngine.compute_and_store")
                self._outcome_engine.compute_and_store(run_date)
                logger.info("STEP 2 DONE — outcome computation complete")

            # ------------------------------------------------------------------
            # STEP 3 — ScorecardAggregator
            # ------------------------------------------------------------------
            logger.info("STEP 3 START — ScorecardAggregator")
            scorecard = self._scorecard_aggregator.top_n_posters(
                as_of=run_date,
                window_days=30,
                n=5,
                tenant_id=1,
            )
            trading_days_scored = self._scorecard_aggregator.trading_days_with_signals(
                as_of=run_date,
                window_days=30,
                tenant_id=1,
            )
            logger.info(
                "STEP 3 DONE — %d scorecard rows, %d trading days scored",
                len(scorecard), trading_days_scored,
            )

            # ------------------------------------------------------------------
            # STEP 4 — Load signals for today
            # ------------------------------------------------------------------
            logger.info("STEP 4 START — loading signals for %s", run_date)
            signals = self._repo.get_signals_for_date(
                signal_date=run_date,
                tenant_id=1,
            )
            logger.info("STEP 4 DONE — %d signals loaded", len(signals))

            # ------------------------------------------------------------------
            # STEP 5 — render_evening
            # ------------------------------------------------------------------
            logger.info("STEP 5 START — rendering evening summary")
            messages = render_evening(
                signals=signals,
                scorecard=scorecard,
                trading_days_scored=trading_days_scored,
                as_of_date=run_date,
            )
            logger.info("STEP 5 DONE — %d message part(s) rendered", len(messages))

            # ------------------------------------------------------------------
            # STEP 6 — Deliver
            # ------------------------------------------------------------------
            logger.info("STEP 6 START — WhatsApp delivery")
            for msg in messages:
                self._deliver(msg, kind="evening", dry_run=dry_run)
            logger.info("STEP 6 DONE — delivery complete")

            # ------------------------------------------------------------------
            # STEP 7 — Write daily_summaries
            # ------------------------------------------------------------------
            duration = monotonic() - start_ts

            # Compute avg_excess_vol across today's scored signals
            scored = [
                s for s in signals
                if s.get("excess_vol_score") is not None
            ]
            avg_excess_vol: float | None = None
            if scored:
                avg_excess_vol = round(
                    sum(float(s["excess_vol_score"]) for s in scored) / len(scored),
                    6,
                )

            if not dry_run:
                self._repo.upsert_daily_summary(
                    tenant_id=1,
                    summary_date=run_date.isoformat(),
                    run_type="evening",
                    signals_scored=len(signals),
                    avg_excess_vol=avg_excess_vol,
                    pipeline_status="ok",
                    duration_seconds=round(duration, 2),
                )

            logger.info(
                "run_evening DONE — %.1fs | signals=%d avg_excess_vol=%s",
                duration, len(signals), avg_excess_vol,
            )

        except Exception as exc:  # noqa: BLE001
            logger.exception("Unhandled evening pipeline exception: %s", exc)
            _send_operational_message(
                self._settings,
                f"⚠️ Evening Pipeline FAILED — {run_date}\n"
                f"Error: {type(exc).__name__}: {str(exc)[:200]}\n"
                f"Check Actions logs.",
                dry_run=dry_run,
            )
            if not dry_run:
                self._repo.upsert_daily_summary(
                    tenant_id=1,
                    summary_date=run_date.isoformat(),
                    run_type="evening",
                    pipeline_status="failed",
                    error_message=f"{type(exc).__name__}: {str(exc)[:500]}",
                    duration_seconds=round(monotonic() - start_ts, 2),
                )
            raise


    # ------------------------------------------------------------------
    # run_poll — TASK-014: intra-day engagement snapshot (no scoring/alerts)
    # ------------------------------------------------------------------

    def run_poll(self, dry_run: bool = False) -> None:
        """Fetch current posts and write engagement_snapshots rows.

        Does NOT re-score signals, does NOT send alerts.
        Intended for the market_hours_poll GitHub Actions workflow which fires
        every 2 hours during 9 AM–5 PM ET.  An ET-local-time guard exits early
        outside that window (DST-safe, using zoneinfo).

        Parameters
        ----------
        dry_run:
            Log what would be written; do not touch the DB or send anything.
        """
        from zoneinfo import ZoneInfo

        et_tz = ZoneInfo("America/New_York")
        now_et = datetime.now(tz=et_tz)
        et_hour = now_et.hour  # 0-23 in ET local time

        if et_hour < 9 or et_hour >= 17:
            logger.info(
                "run_poll: ET local time is %02d:%02d — outside 09:00–17:00 ET window; exiting early",
                et_hour, now_et.minute,
            )
            return

        run_date = now_et.date()
        start_ts = monotonic()
        logger.info(
            "run_poll START — ET time=%02d:%02d run_date=%s dry_run=%s",
            et_hour, now_et.minute, run_date, dry_run,
        )

        try:
            # Ensure schema + seed exist
            if not dry_run:
                self._repo.init_schema()
                self._repo.seed(phone_e164=self._settings.recipient_phone_e164)

            # Validate + load active accounts
            try:
                active_accounts = asyncio.run(self._account_registry.validate_and_promote())
            except Exception as exc:  # noqa: BLE001
                logger.warning("validate_and_promote raised: %s — using get_active_accounts()", exc)
                active_accounts = self._account_registry.get_active_accounts()

            if not active_accounts:
                logger.warning("run_poll: no active accounts — nothing to poll")
                return

            logger.info("run_poll: %d active accounts", len(active_accounts))

            # Determine collection window: last 2h (matches poll cadence)
            from datetime import timedelta
            window_start = datetime.now(tz=et_tz) - timedelta(hours=2)

            # Fetch posts
            try:
                all_posts, success_count, failure_count = asyncio.run(
                    self._account_registry.fetch_all_accounts(
                        since=window_start,
                        max_count=self._settings.max_posts_per_account,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.error("run_poll: fetch_all_accounts raised: %s", exc)
                all_posts = []
                success_count = 0
                failure_count = 0

            logger.info(
                "run_poll: %d posts fetched from %d/%d accounts (%d failed)",
                len(all_posts), success_count, len(active_accounts), failure_count,
            )

            if not all_posts:
                logger.info("run_poll: no posts in window — nothing to snapshot")
                return

            if dry_run:
                logger.info(
                    "[DRY-RUN] Would write %d engagement_snapshots rows", len(all_posts)
                )
                return

            # Look up DB post_id for each fetched post; insert snapshot row
            snaps_written = 0
            accounts_by_handle_lower = {
                acc["handle"].lower(): acc for acc in active_accounts
            }

            for post in all_posts:
                acc = accounts_by_handle_lower.get(post.author_handle.lower(), {})
                account_id = acc.get("id", 1)

                # Upsert the post row (insert if not already present from morning run)
                existing_rows = self._repo._execute(
                    "SELECT id FROM posts WHERE source_type = ? AND external_id = ?",
                    [post.source_type, post.external_id],
                )
                if existing_rows:
                    db_post_id = existing_rows[0]["id"]
                else:
                    db_post_id = self._repo.insert_post(
                        tenant_id=1,
                        account_id=account_id,
                        external_id=post.external_id,
                        source_type=post.source_type,
                        text=post.text,
                        posted_at=post.posted_at,
                        fetched_at=datetime.now(tz=timezone.utc),
                        view_count=post.view_count,
                        repost_count=post.repost_count,
                    )

                if db_post_id is None:
                    logger.warning("run_poll: could not get post_id for %s", post.external_id)
                    continue

                snap_id = self._repo.insert_engagement_snapshot(
                    post_id=db_post_id,
                    view_count=post.view_count,
                    repost_count=post.repost_count,
                )
                if snap_id is not None:
                    snaps_written += 1

            duration = monotonic() - start_ts
            logger.info(
                "run_poll DONE — %d snapshots written in %.1fs",
                snaps_written, duration,
            )

        except Exception as exc:  # noqa: BLE001
            logger.exception("run_poll unhandled exception: %s", exc)
            raise


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="influence_monitor.pipeline",
        description="Influence Post Monitoring pipeline CLI",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ------------------------------------------------------------------
    # morning subcommand
    # ------------------------------------------------------------------
    morning_parser = subparsers.add_parser("morning", help="Run the morning alert pipeline")
    morning_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Render to stdout only; do not write to DB or send WhatsApp",
    )
    morning_parser.add_argument(
        "--account-limit",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N active accounts (default: all)",
    )
    morning_parser.add_argument(
        "--use-fixtures",
        action="store_true",
        help=(
            "Load pre-scored signals from tests/fixtures/sample_signals.json. "
            "Bypasses twikit and Claude; runs rendering + delivery only. "
            "Works on any day without live credentials."
        ),
    )

    # ------------------------------------------------------------------
    # evening subcommand
    # ------------------------------------------------------------------
    evening_parser = subparsers.add_parser("evening", help="Run the evening summary pipeline")
    evening_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Render to stdout only; do not write to DB or send WhatsApp",
    )
    evening_parser.add_argument(
        "--account-limit",
        type=int,
        default=None,
        metavar="N",
        help="Accepted for CLI parity with morning; not used by evening pipeline",
    )
    evening_parser.add_argument(
        "--use-fixtures",
        action="store_true",
        help=(
            "Insert pre-computed outcomes from tests/fixtures/sample_outcomes.json "
            "into DB, then run ScorecardAggregator → render → deliver. "
            "Works on any day without live market data."
        ),
    )

    # ------------------------------------------------------------------
    # poll subcommand — TASK-014: intra-day engagement snapshot
    # ------------------------------------------------------------------
    poll_parser = subparsers.add_parser(
        "poll",
        help=(
            "Fetch current posts and write engagement_snapshots rows. "
            "Does NOT re-score or send alerts. "
            "Includes an ET-local-time guard: exits 0 outside 09:00–17:00 ET."
        ),
    )
    poll_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be written; do not touch the DB",
    )

    return parser


def main() -> None:
    """CLI entry point: python -m influence_monitor.pipeline morning [flags]."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = _build_parser()
    args = parser.parse_args()

    settings = Settings()

    # Bootstrap cookies from TWITTER_COOKIES_JSON env var when present.
    # This supports GitHub Actions Strategy A (cookie secret) so the ephemeral
    # runner has valid session cookies before twikit initialises.
    if settings.twitter_cookies_json:
        cookies_path = Path(settings.cookies_path)
        cookies_path.parent.mkdir(parents=True, exist_ok=True)
        cookies_path.write_text(settings.twitter_cookies_json)
        logger.info(
            "Wrote TWITTER_COOKIES_JSON to %s (%d bytes)",
            cookies_path, len(settings.twitter_cookies_json),
        )

    repo = SignalRepository(settings)

    # Ensure schema + seed data exist before any run
    try:
        repo.init_schema()
        repo.seed(phone_e164=settings.recipient_phone_e164)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Schema init/seed raised: %s (may already exist)", exc)

    orchestrator = PipelineOrchestrator(settings=settings, repo=repo)

    try:
        if args.command == "morning":
            orchestrator.run_morning(
                run_date=date.today(),
                account_limit=args.account_limit,
                dry_run=args.dry_run,
                use_fixtures=args.use_fixtures,
            )
        elif args.command == "evening":
            orchestrator.run_evening(
                run_date=date.today(),
                dry_run=args.dry_run,
                use_fixtures=args.use_fixtures,
            )
        elif args.command == "poll":
            orchestrator.run_poll(dry_run=args.dry_run)
        else:
            logger.error("Unknown command: %s", args.command)
            sys.exit(1)
    finally:
        repo.close()


if __name__ == "__main__":
    main()
