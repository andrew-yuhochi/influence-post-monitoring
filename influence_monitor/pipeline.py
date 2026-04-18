"""Pipeline orchestrator — morning watchlist and evening scorecard.

Wires together every component in the correct order and handles all
failure modes from TDD.md Section 6.  The two public entry points are:

  ``run_morning(run_date)``   — fetch → extract → score → rank → email
  ``run_evening(run_date)``   — score returns → render scorecard → email

Failure contract: a pipeline failure NEVER sends a partial or misleading
email to the user.  On IngestorError or any unhandled exception the
orchestrator sends an operational failure email and aborts.

Open and close prices are fetched together in ``run_evening`` using a
single historical OHLCV call — no separate 9:31 AM open-price step needed.

CLI entry points::

    python -m influence_monitor.pipeline morning [--dry-run]
    python -m influence_monitor.pipeline evening [--dry-run]

``--dry-run`` renders the email to stdout and skips DB writes + sending.
"""

from __future__ import annotations

import asyncio
import logging
import traceback
from datetime import date, datetime, timedelta, timezone
from typing import Any

from influence_monitor.calendar import (  # re-exported; tests import HolidayCalendar from here
    HolidayCalendar,
    _easter,
    _nyse_holidays,
    _observe,
)
from influence_monitor.config import Settings
from influence_monitor.db.repository import DatabaseRepository
from influence_monitor.email.base import EmailProvider
from influence_monitor.email.renderer import (
    EveningScorecardRenderer,
    MorningWatchlistRenderer,
)
from influence_monitor.extraction.ticker_extractor import TickerExtractor
from influence_monitor.ingestion.base import IngestorError, RawPost
from influence_monitor.ingestion.base import SocialMediaSource
from influence_monitor.market_data.base import MarketDataClient
from influence_monitor.market_data.index_resolver import IndexMembershipResolver
from influence_monitor.scorecard.scorecard_engine import ScorecardEngine
from influence_monitor.scoring.corroboration import CorroborationDetector, Signal
from influence_monitor.scoring.aggregator import SignalAggregator
from influence_monitor.scoring.llm_client import LLMClient
from influence_monitor.scoring.scoring_engine import ScoringEngine

logger = logging.getLogger(__name__)

# Overnight window: yesterday 4 PM ET to today 6:30 AM ET (in UTC)
_OVERNIGHT_HOURS_BACK = 15  # hours before pipeline run
_ET_OFFSET = timedelta(hours=4)   # EST offset from UTC (DST ignored for PoC)


# HolidayCalendar, _easter, _nyse_holidays, _observe are imported from
# influence_monitor.calendar at the top of this file.


# ======================================================================
# Pipeline Orchestrator
# ======================================================================

class PipelineOrchestrator:
    """Wire together all pipeline components for morning and evening runs.

    Constructed via ``build_orchestrator(settings, repo)`` for production
    use.  Constructor accepts injected components to allow unit testing.
    """

    def __init__(
        self,
        settings: Settings,
        repo: DatabaseRepository,
        ingestor: SocialMediaSource,
        ticker_extractor: TickerExtractor,
        llm_client: LLMClient,
        scoring_engine: ScoringEngine,
        corroboration_detector: CorroborationDetector,
        aggregator: SignalAggregator,
        index_resolver: IndexMembershipResolver,
        market_client: MarketDataClient,
        scorecard_engine: ScorecardEngine,
        morning_renderer: MorningWatchlistRenderer,
        evening_renderer: EveningScorecardRenderer,
        email_provider: EmailProvider,
        calendar: HolidayCalendar,
    ) -> None:
        self._settings = settings
        self._repo = repo
        self._ingestor = ingestor
        self._ticker_extractor = ticker_extractor
        self._llm_client = llm_client
        self._scoring_engine = scoring_engine
        self._corroboration_detector = corroboration_detector
        self._aggregator = aggregator
        self._index_resolver = index_resolver
        self._market_client = market_client
        self._scorecard_engine = scorecard_engine
        self._morning_renderer = morning_renderer
        self._evening_renderer = evening_renderer
        self._email_provider = email_provider
        self._calendar = calendar

    # ------------------------------------------------------------------
    # Morning pipeline
    # ------------------------------------------------------------------

    async def run_morning(
        self,
        run_date: date,
        dry_run: bool = False,
        since_override: datetime | None = None,
        max_pages: int = 1,
    ) -> dict[str, Any]:
        """Run the full morning watchlist pipeline for *run_date*.

        *since_override*: if provided, use this datetime instead of the
        default overnight window. Useful for historical backfill tests.
        *max_pages*: number of pagination pages per account (default 1 = ~20
        tweets). Increase for longer lookback windows.

        Returns a summary dict. On failure, sends an operational alert
        and returns a summary with ``status='failed'``.

        ``dry_run=True`` renders to stdout and skips DB writes + sending.
        """
        if not self._calendar.is_trading_day(run_date) and since_override is None:
            logger.info("run_morning(%s): not a trading day — skipping", run_date)
            return {"status": "skipped", "reason": "non-trading day"}

        try:
            return await self._run_morning_inner(run_date, dry_run, since_override, max_pages)
        except IngestorError as exc:
            msg = f"IngestorError: {exc}"
            logger.error("Morning pipeline IngestorError: %s", exc)
            if not dry_run:
                await self._send_failure_email(run_date, "IngestorError", msg)
                await self._write_failed_summary(run_date, "morning", msg)
            return {"status": "failed", "error": msg}
        except Exception as exc:
            tb = traceback.format_exc()
            msg = f"{type(exc).__name__}: {exc}"
            logger.exception("Morning pipeline unhandled exception")
            if not dry_run:
                await self._send_failure_email(run_date, type(exc).__name__, f"{msg}\n\n{tb}")
                await self._write_failed_summary(run_date, "morning", msg)
            return {"status": "failed", "error": msg}

    async def _run_morning_inner(
        self,
        run_date: date,
        dry_run: bool,
        since_override: datetime | None = None,
        max_pages: int = 1,
    ) -> dict[str, Any]:
        since = since_override if since_override is not None else _overnight_since(run_date)
        investors = await self._repo.get_active_investors()
        handles = [inv["x_handle"] for inv in investors if inv.get("x_handle")]
        investor_map = {inv["x_handle"]: inv for inv in investors}

        logger.info("Fetching posts from %d accounts since %s", len(handles), since)
        posts, success_count, _ = await self._ingestor.fetch_all_accounts(
            handles, since=since, max_pages=max_pages
        )
        logger.info("Fetched %d posts from %d accounts", len(posts), success_count)

        all_signals: list[Signal] = []
        signal_id_map: dict[int, int] = {}  # signal_list_index → db_id

        for post in posts:
            investor = investor_map.get(post.author_handle)
            if investor is None:
                logger.warning("Unknown handle %s — skipping post", post.author_handle)
                continue

            if not dry_run:
                post_id = await self._save_post(post, investor["id"])
                if post_id is None:
                    continue  # duplicate
            else:
                post_id = -1  # sentinel for dry-run

            tickers = self._ticker_extractor.extract(post.text)
            # Only HIGH and MEDIUM confidence tickers enter scoring
            tickers = [t for t in tickers if t.confidence != "LOW"]

            for extracted in tickers:
                post_score = self._llm_client.score_post(
                    post.text, post.author_handle
                )
                # Zero-sentinel or NEUTRAL/AMBIGUOUS direction → skip
                if post_score.conviction_level < self._settings.conviction_min:
                    continue
                if post_score.direction in ("NEUTRAL", "AMBIGUOUS"):
                    continue
                # Confirm ticker matches LLM extraction (best-effort check)
                if extracted.ticker not in (post_score.tickers or [extracted.ticker]):
                    post_score = post_score.model_copy(
                        update={"tickers": [extracted.ticker]}
                    )

                scored = self._scoring_engine.score(
                    post_score=post_score,
                    credibility_score=investor.get("credibility_score", 5.0),
                    rolling_accuracy_30d=investor.get("rolling_accuracy_30d"),
                    view_count=post.view_count,
                    repost_count=post.repost_count,
                    max_engagement_30d=1000.0,  # placeholder; real value from DB
                    median_engagement=None,
                )
                if scored.composite_score < self._settings.signal_min_score:
                    continue

                sig = Signal(
                    signal_id=None,
                    post_id=post_id,
                    investor_id=investor["id"],
                    ticker=extracted.ticker,
                    direction=post_score.direction,
                    signal_date=run_date,
                    composite_score=scored.composite_score,
                    investor_name=investor.get("name", ""),
                    extraction_confidence=extracted.confidence,
                )

                if not dry_run:
                    db_id = await self._repo.insert_signal(
                        tenant_id=1,
                        post_id=post_id,
                        investor_id=investor["id"],
                        ticker=extracted.ticker,
                        extraction_confidence=extracted.confidence,
                        extraction_method="three_layer",
                        direction=post_score.direction,
                        conviction_level=post_score.conviction_level,
                        argument_quality=post_score.argument_quality,
                        time_horizon=post_score.time_horizon,
                        market_moving_potential=post_score.market_moving_potential,
                        key_claim=post_score.key_claim,
                        rationale=post_score.rationale,
                        score_credibility=scored.score_credibility,
                        score_conviction=scored.score_conviction,
                        score_argument=scored.score_argument,
                        score_engagement=scored.score_engagement,
                        score_historical=scored.score_historical,
                        composite_score=scored.composite_score,
                        signal_date=run_date.isoformat(),
                        llm_model_version=self._llm_client.model_version()
                        if hasattr(self._llm_client, "model_version") else None,
                    )
                    sig = Signal(
                        signal_id=db_id,
                        post_id=post_id,
                        investor_id=investor["id"],
                        ticker=extracted.ticker,
                        direction=post_score.direction,
                        signal_date=run_date,
                        composite_score=scored.composite_score,
                        investor_name=investor.get("name", ""),
                        extraction_confidence=extracted.confidence,
                    )

                all_signals.append(sig)

        # Corroboration + ranking
        self._corroboration_detector.detect(all_signals)
        ranked = self._aggregator.rank(all_signals, top_n=self._settings.top_n_signals)

        # Persist morning rank, corroboration, and index tier
        if not dry_run:
            for rank, sig in enumerate(ranked, start=1):
                if sig.signal_id is not None:
                    await self._repo.update_signal_morning_rank(
                        sig.signal_id, rank,
                        sig.corroboration_count,
                        sig.corroboration_bonus,
                    )
                    tier = await self._index_resolver.resolve(sig.ticker)
                    await self._repo.update_signal_index_tier(sig.signal_id, tier)

        # Render
        if dry_run:
            email = self._morning_renderer.render_from_rows(
                _signals_to_render_rows(ranked, run_date),
                signal_date=run_date,
                accounts_monitored=len(handles),
            )
            print(f"SUBJECT: {email.subject}\n")
            print(email.text_body)
            return {"status": "dry_run", "signals": len(ranked)}

        email = await self._morning_renderer.render(
            run_date, self._repo, accounts_monitored=success_count
        )
        await self._email_provider.send(
            to=self._settings.recipient_email,
            subject=email.subject,
            html_body=email.html_body,
            text_body=email.text_body,
        )
        await self._repo.upsert_daily_summary(
            tenant_id=1,
            summary_date=run_date.isoformat(),
            run_type="morning",
            accounts_fetched=success_count,
            posts_fetched=len(posts),
            signals_scored=len(all_signals),
            signals_surfaced=len(ranked),
            corroborated_signals=sum(1 for s in ranked if s.corroboration_count >= 2),
            pipeline_status="ok",
        )
        logger.info(
            "run_morning(%s): %d signals surfaced, email sent",
            run_date, len(ranked),
        )
        return {"status": "ok", "signals": len(ranked), "posts": len(posts)}

    # ------------------------------------------------------------------
    # Evening pipeline
    # ------------------------------------------------------------------

    async def run_evening(
        self, run_date: date, dry_run: bool = False
    ) -> dict[str, Any]:
        """Run the evening scorecard pipeline for *run_date*."""
        if not self._calendar.is_trading_day(run_date):
            logger.info("run_evening(%s): not a trading day — skipping", run_date)
            return {"status": "skipped"}

        try:
            return await self._run_evening_inner(run_date, dry_run)
        except Exception as exc:
            tb = traceback.format_exc()
            msg = f"{type(exc).__name__}: {exc}"
            logger.exception("Evening pipeline unhandled exception")
            if not dry_run:
                await self._send_failure_email(run_date, type(exc).__name__, f"{msg}\n\n{tb}")
            return {"status": "failed", "error": msg}

    async def _run_evening_inner(
        self, run_date: date, dry_run: bool
    ) -> dict[str, Any]:
        summary = await self._scorecard_engine.run_evening(run_date)
        backfill = await self._scorecard_engine.backfill_returns(run_date)
        summary["backfill_updated"] = backfill["updated"]

        if dry_run:
            email = await self._evening_renderer.render(run_date, self._repo)
            print(f"SUBJECT: {email.subject}\n")
            print(email.text_body)
            return {"status": "dry_run", **summary}

        email = await self._evening_renderer.render(run_date, self._repo)
        await self._email_provider.send(
            to=self._settings.recipient_email,
            subject=email.subject,
            html_body=email.html_body,
            text_body=email.text_body,
        )
        logger.info("run_evening(%s): scorecard email sent", run_date)
        return {"status": "ok", **summary}

    # ------------------------------------------------------------------
    # Operational failure helpers
    # ------------------------------------------------------------------

    async def _send_failure_email(
        self, run_date: date, error_type: str, detail: str
    ) -> None:
        subject = f"[Influence Monitor] Pipeline FAILED — {error_type} on {run_date}"
        body = (
            f"Pipeline failure on {run_date}\n"
            f"Error type: {error_type}\n\n"
            f"{detail}\n\n"
            "No watchlist email was sent to protect against partial data."
        )
        try:
            await self._email_provider.send(
                to=self._settings.recipient_email,
                subject=subject,
                html_body=f"<pre>{body}</pre>",
                text_body=body,
            )
        except Exception as send_exc:
            logger.error("Failed to send operational failure email: %s", send_exc)

    async def _write_failed_summary(
        self, run_date: date, run_type: str, error_msg: str
    ) -> None:
        try:
            await self._repo.upsert_daily_summary(
                tenant_id=1,
                summary_date=run_date.isoformat(),
                run_type=run_type,
                pipeline_status="failed",
                error_message=error_msg[:500],
            )
        except Exception as exc:
            logger.error("Failed to write failed daily summary: %s", exc)

    # ------------------------------------------------------------------
    # Post persistence helper
    # ------------------------------------------------------------------

    async def _save_post(
        self, post: RawPost, investor_id: int
    ) -> int | None:
        return await self._repo.insert_post(
            tenant_id=1,
            investor_id=investor_id,
            external_id=post.external_id,
            source_type=post.source_type,
            text=post.text,
            posted_at=post.posted_at,
            fetched_at=post.fetched_at,
            view_count=post.view_count,
            repost_count=post.repost_count,
            reply_count=post.reply_count,
            like_count=post.like_count,
            bookmark_count=post.bookmark_count,
            quote_tweet_id=post.quote_tweet_id,
            is_thread=post.is_thread,
            thread_position=post.thread_position,
            hashtags=post.hashtags,
            mentioned_users=post.mentioned_users,
            url_links=post.url_links,
            media_type=post.media_type,
            language=post.language,
            follower_count_at_post=post.follower_count_at_post,
            following_count_at_post=post.following_count_at_post,
            raw_payload=post.raw_payload,
        )


# ======================================================================
# Factory
# ======================================================================

async def build_orchestrator(settings: Settings, repo: DatabaseRepository) -> PipelineOrchestrator:
    """Assemble a PipelineOrchestrator with all real production components."""
    from influence_monitor.email.registry import EMAIL_REGISTRY
    from influence_monitor.extraction.equity_whitelist import SymbolWhitelist
    from influence_monitor.ingestion.registry import SOURCE_REGISTRY
    from influence_monitor.market_data.alpha_vantage_client import AlphaVantageClient
    from influence_monitor.market_data.yfinance_client import YFinanceClient
    from influence_monitor.scoring.claude_client import ClaudeHaikuClient

    ingestor = SOURCE_REGISTRY[settings.twitter_source](settings)
    ticker_extractor = TickerExtractor(SymbolWhitelist.load())
    llm_client = ClaudeHaikuClient(settings, repo)
    scoring_engine = await ScoringEngine.from_db(repo)
    corroboration_detector = CorroborationDetector(settings.corroboration_multiplier)
    aggregator = SignalAggregator()
    index_resolver = IndexMembershipResolver(repo)
    await index_resolver.initialize()
    yf_client = YFinanceClient()
    av_client = AlphaVantageClient(settings)
    scorecard_engine = ScorecardEngine(yf_client, repo, settings)
    morning_renderer = MorningWatchlistRenderer(settings)
    evening_renderer = EveningScorecardRenderer(settings)
    email_provider = EMAIL_REGISTRY[settings.email_provider](settings, repo)
    calendar = HolidayCalendar()

    return PipelineOrchestrator(
        settings=settings,
        repo=repo,
        ingestor=ingestor,
        ticker_extractor=ticker_extractor,
        llm_client=llm_client,
        scoring_engine=scoring_engine,
        corroboration_detector=corroboration_detector,
        aggregator=aggregator,
        index_resolver=index_resolver,
        market_client=yf_client,
        scorecard_engine=scorecard_engine,
        morning_renderer=morning_renderer,
        evening_renderer=evening_renderer,
        email_provider=email_provider,
        calendar=calendar,
    )


# ======================================================================
# Helpers
# ======================================================================

def _overnight_since(run_date: date) -> datetime:
    """Return the start of the overnight window for *run_date*.

    Approximately yesterday 4 PM ET, expressed in UTC.
    """
    yesterday_4pm_et = datetime(
        run_date.year, run_date.month, run_date.day,
        tzinfo=timezone.utc,
    ) - timedelta(hours=_OVERNIGHT_HOURS_BACK)
    return yesterday_4pm_et


def _signals_to_render_rows(
    signals: list[Signal], signal_date: date
) -> list[dict[str, Any]]:
    """Convert Signal objects to the dict shape expected by render_from_rows."""
    return [
        {
            "morning_rank": rank,
            "ticker": sig.ticker,
            "direction": sig.direction,
            "composite_score": sig.composite_score,
            "corroboration_count": sig.corroboration_count,
            "index_tier": "MICRO",  # best-effort for dry-run (not yet resolved)
            "investor_name": sig.investor_name,
            "x_handle": "",
            "total_calls": 0,
            "total_hits": 0,
            "post_text": "",
            "post_deleted": 0,
        }
        for rank, sig in enumerate(signals, start=1)
    ]


# ======================================================================
# CLI entry point
# ======================================================================

async def _async_main(command: str, dry_run: bool, since_str: str | None = None) -> None:
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    settings = Settings()
    repo = DatabaseRepository(settings)
    await repo.connect()

    try:
        orchestrator = await build_orchestrator(settings, repo)
        run_date = date.today()

        since_override: datetime | None = None
        max_pages = 1
        if since_str:
            since_override = datetime.fromisoformat(since_str).replace(tzinfo=timezone.utc)
            # Estimate pages needed: ~20 tweets/page, ~5 tweets/day/account
            days = (datetime.now(timezone.utc) - since_override).days
            max_pages = max(1, min(days // 4, 15))  # cap at 15 pages
            logger.info("Historical run: since=%s, max_pages=%d per account", since_str, max_pages)

        if command == "morning":
            result = await orchestrator.run_morning(
                run_date, dry_run=dry_run,
                since_override=since_override, max_pages=max_pages,
            )
        elif command == "evening":
            result = await orchestrator.run_evening(run_date, dry_run=dry_run)
        elif command == "auth":
            from influence_monitor.ingestion.twitter_twikit import TwitterIngestor
            ingestor = TwitterIngestor(settings)
            await ingestor._ensure_authenticated()
            print("Authentication complete. Cookies saved.")
            result = {"status": "ok"}
        else:
            print(f"Unknown command: {command}", file=sys.stderr)
            print("Usage: python -m influence_monitor.pipeline [morning|evening|auth] [--dry-run]")
            sys.exit(1)

        logger.info("Pipeline result: %s", result)
    finally:
        await repo.close()


def main() -> None:
    import sys

    args = sys.argv[1:]
    if not args:
        print("Usage: python -m influence_monitor.pipeline [morning|evening|auth] [--dry-run] [--since YYYY-MM-DD]")
        sys.exit(1)

    command = args[0]
    dry_run = "--dry-run" in args

    since_str: str | None = None
    if "--since" in args:
        idx = args.index("--since")
        if idx + 1 < len(args):
            since_str = args[idx + 1]

    asyncio.run(_async_main(command, dry_run, since_str))


if __name__ == "__main__":
    main()
