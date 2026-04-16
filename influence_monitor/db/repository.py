"""Database repository — single access point to SQLite.

All read/write operations go through DatabaseRepository.
Uses aiosqlite for async operations and raw SQL (no ORM).

CLI entry point:
    python -m influence_monitor.db.repository --init
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

import aiosqlite

from influence_monitor.config import Settings

logger = logging.getLogger(__name__)

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"
_PROJECT_ROOT = Path(__file__).parent.parent.parent
_INVESTORS_SEED = _PROJECT_ROOT / "config" / "investors_seed.json"
_WEIGHTS_SEED = _PROJECT_ROOT / "config" / "scoring_weights_seed.json"


class DatabaseRepository:
    """Async SQLite repository for all pipeline data operations."""

    def __init__(self, settings: Settings) -> None:
        self._db_path = settings.database_path_resolved
        self._conn: aiosqlite.Connection | None = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open a connection and enable WAL mode + foreign keys."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(str(self._db_path))
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._conn

    # ------------------------------------------------------------------
    # Schema initialisation and seeding
    # ------------------------------------------------------------------

    async def init_schema(self) -> None:
        """Create all tables from schema.sql."""
        schema_sql = _SCHEMA_PATH.read_text()
        await self.conn.executescript(schema_sql)
        await self.conn.commit()
        logger.info("Database schema initialised")

    async def seed(self) -> None:
        """Seed default tenant, investor profiles, and scoring weights from config files."""
        await self._seed_tenant()
        await self._seed_investor_profiles()
        await self._seed_scoring_weights()
        await self.conn.commit()
        logger.info("Database seeding complete")

    async def _seed_tenant(self) -> None:
        await self.conn.execute(
            "INSERT OR IGNORE INTO tenants (id, name) VALUES (1, 'default')"
        )

    async def _seed_investor_profiles(self) -> None:
        investors = json.loads(_INVESTORS_SEED.read_text())
        for inv in investors:
            await self.conn.execute(
                """INSERT INTO investor_profiles
                   (tenant_id, name, x_handle, source_type, investor_type,
                    credibility_score, is_active, notes)
                   VALUES (1, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(tenant_id, x_handle) DO UPDATE SET
                     name=excluded.name,
                     investor_type=excluded.investor_type,
                     credibility_score=excluded.credibility_score,
                     is_active=excluded.is_active,
                     notes=excluded.notes,
                     updated_at=CURRENT_TIMESTAMP""",
                (
                    inv["name"],
                    inv["x_handle"],
                    inv["source_type"],
                    inv["investor_type"],
                    inv["credibility_score"],
                    inv["is_active"],
                    inv.get("notes", ""),
                ),
            )
        logger.info("Seeded %d investor profiles", len(investors))

    async def _seed_scoring_weights(self) -> None:
        seed_data = json.loads(_WEIGHTS_SEED.read_text())
        for w in seed_data["weights"]:
            await self.conn.execute(
                """INSERT OR IGNORE INTO scoring_weights
                   (component, weight, description)
                   VALUES (?, ?, ?)""",
                (w["component"], w["weight"], w["description"]),
            )
        logger.info("Seeded %d scoring weights", len(seed_data["weights"]))

    # ------------------------------------------------------------------
    # Investor profiles
    # ------------------------------------------------------------------

    async def get_investor_by_handle(self, handle: str) -> dict[str, Any] | None:
        """Look up an active investor profile by X handle."""
        cursor = await self.conn.execute(
            "SELECT * FROM investor_profiles WHERE x_handle = ? AND is_active = 1",
            (handle,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def upsert_investor_profile(
        self,
        tenant_id: int,
        name: str,
        x_handle: str,
        source_type: str,
        investor_type: str,
        credibility_score: float,
        is_active: bool = True,
        notes: str = "",
    ) -> int:
        """Insert or update an investor profile. Returns the row id."""
        cursor = await self.conn.execute(
            """INSERT INTO investor_profiles
               (tenant_id, name, x_handle, source_type, investor_type,
                credibility_score, is_active, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 name=excluded.name,
                 x_handle=excluded.x_handle,
                 credibility_score=excluded.credibility_score,
                 is_active=excluded.is_active,
                 notes=excluded.notes,
                 updated_at=CURRENT_TIMESTAMP""",
            (tenant_id, name, x_handle, source_type, investor_type,
             credibility_score, is_active, notes),
        )
        await self.conn.commit()
        return cursor.lastrowid  # type: ignore[return-value]

    async def update_investor_accuracy(
        self,
        investor_id: int,
        rolling_accuracy_30d: float | None,
        total_calls: int,
        total_hits: int,
    ) -> None:
        """Update an investor's track record stats."""
        await self.conn.execute(
            """UPDATE investor_profiles
               SET rolling_accuracy_30d = ?,
                   total_calls = ?,
                   total_hits = ?,
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (rolling_accuracy_30d, total_calls, total_hits, investor_id),
        )
        await self.conn.commit()

    async def get_active_investors(self, tenant_id: int = 1) -> list[dict[str, Any]]:
        """Return all active investor profiles for a tenant."""
        cursor = await self.conn.execute(
            "SELECT * FROM investor_profiles WHERE tenant_id = ? AND is_active = 1",
            (tenant_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Posts
    # ------------------------------------------------------------------

    async def insert_post(
        self,
        tenant_id: int,
        investor_id: int,
        external_id: str,
        source_type: str,
        text: str,
        posted_at: datetime,
        fetched_at: datetime,
        view_count: int | None = None,
        repost_count: int | None = None,
        reply_count: int | None = None,
        like_count: int | None = None,
        bookmark_count: int | None = None,
        quote_tweet_id: str | None = None,
        is_thread: bool = False,
        thread_position: int | None = None,
        hashtags: list[str] | None = None,
        mentioned_users: list[str] | None = None,
        url_links: list[str] | None = None,
        media_type: str | None = None,
        language: str = "en",
        follower_count_at_post: int | None = None,
        following_count_at_post: int | None = None,
        raw_payload: dict | None = None,
    ) -> int | None:
        """Insert a post. Uses INSERT OR IGNORE for deduplication on external_id.

        Returns the row id on insert, or None if the post already exists.
        """
        cursor = await self.conn.execute(
            """INSERT OR IGNORE INTO posts
               (tenant_id, investor_id, external_id, source_type, text,
                posted_at, fetched_at, view_count, repost_count, reply_count,
                like_count, bookmark_count, quote_tweet_id, is_thread,
                thread_position, hashtags, mentioned_users, url_links,
                media_type, language, follower_count_at_post,
                following_count_at_post, raw_payload)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                tenant_id, investor_id, external_id, source_type, text,
                posted_at.isoformat(), fetched_at.isoformat(),
                view_count, repost_count, reply_count, like_count,
                bookmark_count, quote_tweet_id, is_thread, thread_position,
                json.dumps(hashtags) if hashtags else None,
                json.dumps(mentioned_users) if mentioned_users else None,
                json.dumps(url_links) if url_links else None,
                media_type, language, follower_count_at_post,
                following_count_at_post,
                json.dumps(raw_payload) if raw_payload else None,
            ),
        )
        await self.conn.commit()
        if cursor.lastrowid and cursor.rowcount > 0:
            return cursor.lastrowid
        return None

    # ------------------------------------------------------------------
    # Signals
    # ------------------------------------------------------------------

    async def insert_signal(self, **kwargs: Any) -> int:
        """Insert a signal row. Accepts all signals columns as keyword args.

        Required: tenant_id, post_id, investor_id, ticker,
                  extraction_confidence, direction, signal_date.
        """
        columns = list(kwargs.keys())
        placeholders = ", ".join("?" for _ in columns)
        col_names = ", ".join(columns)
        values = list(kwargs.values())

        cursor = await self.conn.execute(
            f"INSERT INTO signals ({col_names}) VALUES ({placeholders})",
            values,
        )
        await self.conn.commit()
        return cursor.lastrowid  # type: ignore[return-value]

    async def get_signals_for_date(
        self, signal_date: date, tenant_id: int = 1
    ) -> list[dict[str, Any]]:
        """Return all signals for a given date and tenant."""
        cursor = await self.conn.execute(
            """SELECT s.*, p.text AS post_text, ip.name AS investor_name,
                      ip.x_handle, ip.credibility_score,
                      ip.rolling_accuracy_30d, ip.total_calls, ip.total_hits
               FROM signals s
               JOIN posts p ON s.post_id = p.id
               JOIN investor_profiles ip ON s.investor_id = ip.id
               WHERE s.signal_date = ? AND s.tenant_id = ?
               ORDER BY s.final_score DESC NULLS LAST""",
            (signal_date.isoformat(), tenant_id),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def update_signal_prices(
        self,
        signal_id: int,
        open_price: float | None = None,
        close_price: float | None = None,
        high_price: float | None = None,
        low_price: float | None = None,
        prev_close_price: float | None = None,
        return_pct: float | None = None,
        is_hit: bool | None = None,
    ) -> None:
        """Update price-related fields on a signal."""
        updates: list[str] = []
        values: list[Any] = []
        for col, val in [
            ("open_price", open_price),
            ("close_price", close_price),
            ("high_price", high_price),
            ("low_price", low_price),
            ("prev_close_price", prev_close_price),
            ("return_pct", return_pct),
            ("is_hit", is_hit),
        ]:
            if val is not None:
                updates.append(f"{col} = ?")
                values.append(val)

        if not updates:
            return

        values.append(signal_id)
        await self.conn.execute(
            f"UPDATE signals SET {', '.join(updates)} WHERE id = ?",
            values,
        )
        await self.conn.commit()

    async def update_signal_market_context(
        self,
        signal_id: int,
        volume: int | None = None,
        avg_volume_30d: int | None = None,
        volume_ratio: float | None = None,
        market_cap_at_signal: float | None = None,
        sector: str | None = None,
        industry: str | None = None,
        sp500_return_pct: float | None = None,
        vix_at_signal: float | None = None,
        sector_return_pct: float | None = None,
    ) -> None:
        """Update market context fields on a signal."""
        updates: list[str] = []
        values: list[Any] = []
        for col, val in [
            ("volume", volume),
            ("avg_volume_30d", avg_volume_30d),
            ("volume_ratio", volume_ratio),
            ("market_cap_at_signal", market_cap_at_signal),
            ("sector", sector),
            ("industry", industry),
            ("sp500_return_pct", sp500_return_pct),
            ("vix_at_signal", vix_at_signal),
            ("sector_return_pct", sector_return_pct),
        ]:
            if val is not None:
                updates.append(f"{col} = ?")
                values.append(val)

        if not updates:
            return

        values.append(signal_id)
        await self.conn.execute(
            f"UPDATE signals SET {', '.join(updates)} WHERE id = ?",
            values,
        )
        await self.conn.commit()

    # ------------------------------------------------------------------
    # Engagement snapshots
    # ------------------------------------------------------------------

    async def insert_engagement_snapshot(
        self,
        post_id: int,
        view_count: int | None = None,
        repost_count: int | None = None,
        reply_count: int | None = None,
        like_count: int | None = None,
        bookmark_count: int | None = None,
    ) -> int:
        """Insert an engagement snapshot for a post."""
        cursor = await self.conn.execute(
            """INSERT INTO engagement_snapshots
               (post_id, view_count, repost_count, reply_count, like_count, bookmark_count)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (post_id, view_count, repost_count, reply_count, like_count, bookmark_count),
        )
        await self.conn.commit()
        return cursor.lastrowid  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Daily summaries
    # ------------------------------------------------------------------

    async def insert_daily_summary(self, **kwargs: Any) -> int:
        """Insert a daily pipeline run summary."""
        columns = list(kwargs.keys())
        placeholders = ", ".join("?" for _ in columns)
        col_names = ", ".join(columns)
        values = list(kwargs.values())

        cursor = await self.conn.execute(
            f"INSERT INTO daily_summaries ({col_names}) VALUES ({placeholders})",
            values,
        )
        await self.conn.commit()
        return cursor.lastrowid  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # API usage logging
    # ------------------------------------------------------------------

    async def log_api_usage(
        self,
        provider: str,
        endpoint: str | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        latency_ms: int | None = None,
        status: str = "ok",
        error_message: str | None = None,
    ) -> None:
        """Log an external API call for cost monitoring."""
        await self.conn.execute(
            """INSERT INTO api_usage
               (provider, endpoint, input_tokens, output_tokens,
                latency_ms, status, error_message)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (provider, endpoint, input_tokens, output_tokens,
             latency_ms, status, error_message),
        )
        await self.conn.commit()

    # ------------------------------------------------------------------
    # Scoring weights
    # ------------------------------------------------------------------

    async def get_scoring_weights(self) -> dict[str, float]:
        """Return scoring weights as {component: weight}."""
        cursor = await self.conn.execute(
            "SELECT component, weight FROM scoring_weights"
        )
        rows = await cursor.fetchall()
        return {row["component"]: row["weight"] for row in rows}


# ------------------------------------------------------------------
# CLI entry point: python -m influence_monitor.db.repository --init
# ------------------------------------------------------------------

async def _async_main() -> None:
    import sys

    if "--init" not in sys.argv:
        print("Usage: python -m influence_monitor.db.repository --init")
        sys.exit(1)

    settings = Settings()
    repo = DatabaseRepository(settings)
    await repo.connect()
    try:
        await repo.init_schema()
        await repo.seed()

        # Verify seeding
        cursor = await repo.conn.execute("SELECT COUNT(*) AS cnt FROM investor_profiles")
        row = await cursor.fetchone()
        investor_count = row["cnt"] if row else 0

        cursor = await repo.conn.execute("SELECT COUNT(*) AS cnt FROM scoring_weights")
        row = await cursor.fetchone()
        weight_count = row["cnt"] if row else 0

        logger.info(
            "Verification: %d investor profiles, %d scoring weights",
            investor_count,
            weight_count,
        )
        print(f"Database initialised at {settings.database_path}")
        print(f"  investor_profiles: {investor_count}")
        print(f"  scoring_weights:   {weight_count}")
    finally:
        await repo.close()


def main() -> None:
    import asyncio

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
