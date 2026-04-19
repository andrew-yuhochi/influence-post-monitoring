"""Database repository — all read/write operations go through SignalRepository.

Uses libsql_client (Turso) when TURSO_URL is set; falls back to stdlib sqlite3
on a local data/signals.db when TURSO_URL is empty (dev mode).

CLI entry point:
    python -m influence_monitor.db.repository --init
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any

from influence_monitor.config import Settings

logger = logging.getLogger(__name__)

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"
_PROJECT_ROOT = Path(__file__).parent.parent.parent
_ACCOUNTS_SEED = _PROJECT_ROOT / "config" / "accounts.json"
_SCORING_CONFIG_SEED = _PROJECT_ROOT / "config" / "scoring_config_seed.json"


# ---------------------------------------------------------------------------
# Row helper — normalise libsql_client ResultSet rows and sqlite3 Row objects
# ---------------------------------------------------------------------------

def _row_to_dict(row: Any, columns: tuple[str, ...] | None) -> dict[str, Any]:
    """Convert a libsql_client Row or sqlite3.Row to a plain dict."""
    if isinstance(row, dict):
        return row
    if hasattr(row, "keys"):
        # sqlite3.Row supports .keys()
        return dict(row)
    # libsql_client Row: a tuple, use columns list
    if columns is not None:
        return dict(zip(columns, row))
    raise TypeError(f"Cannot convert row of type {type(row)} to dict")


# ---------------------------------------------------------------------------
# Connection abstraction — wraps both backends behind a thin interface
# ---------------------------------------------------------------------------

class _LibsqlBackend:
    """Thin wrapper around libsql_client.ClientSync."""

    def __init__(self, url: str, auth_token: str | None) -> None:
        import libsql_client  # type: ignore[import]

        kwargs: dict[str, Any] = {"url": url}
        if auth_token:
            kwargs["auth_token"] = auth_token
        self._client = libsql_client.create_client_sync(**kwargs)

    def execute(
        self,
        sql: str,
        params: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        result = self._client.execute(sql, params or [])
        cols = result.columns
        return [_row_to_dict(r, cols) for r in result.rows]

    def executemany(self, sql: str, param_list: list[list[Any]]) -> None:
        for params in param_list:
            self._client.execute(sql, params)

    def executescript(self, script: str) -> None:
        """Execute a multi-statement SQL script (split on ';')."""
        statements = [
            s.strip() for s in script.split(";") if s.strip()
        ]
        self._client.batch(statements)

    def close(self) -> None:
        self._client.close()

    @property
    def lastrowid(self) -> int | None:
        return None  # not available via batch/execute on libsql


class _Sqlite3Backend:
    """Thin wrapper around stdlib sqlite3."""

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")

    def execute(
        self,
        sql: str,
        params: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        cursor = self._conn.execute(sql, params or [])
        rows = cursor.fetchall()
        return [dict(r) for r in rows]

    def execute_returning_lastrowid(
        self,
        sql: str,
        params: list[Any] | None = None,
    ) -> int | None:
        cursor = self._conn.execute(sql, params or [])
        self._conn.commit()
        return cursor.lastrowid

    def executemany(self, sql: str, param_list: list[list[Any]]) -> None:
        self._conn.executemany(sql, param_list)
        self._conn.commit()

    def executescript(self, script: str) -> None:
        self._conn.executescript(script)

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


# ---------------------------------------------------------------------------
# SignalRepository
# ---------------------------------------------------------------------------

class SignalRepository:
    """Single access point for all Influence Monitor DB operations.

    Uses Turso (libsql_client) when TURSO_URL is configured; falls back to
    a local SQLite file at ``settings.database_path_resolved`` otherwise.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._backend: _LibsqlBackend | _Sqlite3Backend
        if settings.turso_url:
            logger.info("Connecting to Turso at %s", settings.turso_url)
            self._backend = _LibsqlBackend(
                url=settings.turso_url,
                auth_token=settings.turso_token or None,
            )
            self._is_libsql = True
        else:
            db_path = settings.database_path_resolved
            logger.info("Using local SQLite at %s", db_path)
            self._backend = _Sqlite3Backend(db_path)
            self._is_libsql = False

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _execute(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        return self._backend.execute(sql, params)

    def _execute_write(self, sql: str, params: list[Any] | None = None) -> int | None:
        """Execute a write statement; return lastrowid when available."""
        if self._is_libsql:
            self._backend.execute(sql, params)
            return None
        else:
            assert isinstance(self._backend, _Sqlite3Backend)
            return self._backend.execute_returning_lastrowid(sql, params)

    def _executemany(self, sql: str, param_list: list[list[Any]]) -> None:
        self._backend.executemany(sql, param_list)

    def close(self) -> None:
        self._backend.close()

    # ------------------------------------------------------------------
    # Schema initialisation
    # ------------------------------------------------------------------

    def init_schema(self) -> None:
        """Create all tables from schema.sql."""
        schema_sql = _SCHEMA_PATH.read_text()
        self._backend.executescript(schema_sql)
        if not self._is_libsql:
            assert isinstance(self._backend, _Sqlite3Backend)
            self._backend.commit()
        logger.info("Schema initialised")

    # ------------------------------------------------------------------
    # Seeding
    # ------------------------------------------------------------------

    def seed(
        self,
        phone_e164: str = "",
        tenant_id: int = 1,
    ) -> None:
        """Seed default tenant, user, accounts, and scoring_config."""
        self._seed_tenant(tenant_id)
        self._seed_user(phone_e164, tenant_id)
        self._seed_accounts(tenant_id)
        self._seed_scoring_config(tenant_id)
        if not self._is_libsql:
            assert isinstance(self._backend, _Sqlite3Backend)
            self._backend.commit()
        logger.info("Seeding complete")

    def _seed_tenant(self, tenant_id: int) -> None:
        self._execute_write(
            "INSERT OR IGNORE INTO tenants (id, name) VALUES (?, 'default')",
            [tenant_id],
        )

    def _seed_user(self, phone_e164: str, tenant_id: int) -> None:
        self._execute_write(
            """INSERT OR IGNORE INTO users (id, tenant_id, phone_e164)
               VALUES (1, ?, ?)""",
            [tenant_id, phone_e164],
        )

    def _seed_accounts(self, tenant_id: int) -> None:
        accounts = json.loads(_ACCOUNTS_SEED.read_text())
        for acc in accounts:
            self._execute_write(
                """INSERT OR IGNORE INTO accounts
                   (tenant_id, user_id, handle, display_name, angle,
                    credibility_score, status, backup_rank, notes)
                   VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    tenant_id,
                    acc["handle"],
                    acc["display_name"],
                    acc["angle"],
                    acc["credibility_score"],
                    acc["status"],
                    acc.get("backup_rank"),
                    acc.get("notes", ""),
                ],
            )
        logger.info("Seeded %d accounts", len(accounts))

    def _seed_scoring_config(self, tenant_id: int) -> None:
        config_rows = json.loads(_SCORING_CONFIG_SEED.read_text())
        for row in config_rows:
            self._execute_write(
                """INSERT OR IGNORE INTO scoring_config
                   (tenant_id, key, value, description)
                   VALUES (?, ?, ?, ?)""",
                [tenant_id, row["key"], row["value"], row.get("description", "")],
            )
        logger.info("Seeded %d scoring_config rows", len(config_rows))

    # ------------------------------------------------------------------
    # Accounts
    # ------------------------------------------------------------------

    def upsert_account(
        self,
        tenant_id: int,
        handle: str,
        display_name: str | None = None,
        angle: str | None = None,
        credibility_score: float = 5.0,
        status: str = "primary",
        backup_rank: int | None = None,
        notes: str = "",
    ) -> int | None:
        """Insert or update an account row. Returns rowid on sqlite3 backend."""
        return self._execute_write(
            """INSERT INTO accounts
               (tenant_id, user_id, handle, display_name, angle,
                credibility_score, status, backup_rank, notes)
               VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, handle) DO UPDATE SET
                 display_name=excluded.display_name,
                 angle=excluded.angle,
                 credibility_score=excluded.credibility_score,
                 status=excluded.status,
                 backup_rank=excluded.backup_rank,
                 notes=excluded.notes""",
            [tenant_id, handle, display_name, angle,
             credibility_score, status, backup_rank, notes],
        )

    def update_account_failure(
        self,
        account_id: int,
        last_failure_at: datetime | None = None,
    ) -> None:
        """Increment consecutive_failures and record last_failure_at."""
        ts = (last_failure_at or datetime.utcnow()).isoformat()
        self._execute_write(
            """UPDATE accounts
               SET consecutive_failures = consecutive_failures + 1,
                   last_failure_at = ?,
                   last_fetch_status = 'error'
               WHERE id = ?""",
            [ts, account_id],
        )

    def reset_account_failures(self, account_id: int) -> None:
        """Reset consecutive_failures to 0 after a successful fetch."""
        self._execute_write(
            """UPDATE accounts
               SET consecutive_failures = 0,
                   last_fetch_status = 'ok'
               WHERE id = ?""",
            [account_id],
        )

    def rename_account_handle(
        self,
        account_id: int,
        new_handle: str,
    ) -> None:
        """Update the handle in place (handle-resolution rename path)."""
        self._execute_write(
            "UPDATE accounts SET handle = ? WHERE id = ?",
            [new_handle, account_id],
        )
        logger.info("Renamed account id=%d to handle=%s", account_id, new_handle)

    def get_accounts_by_status(
        self, status: str, tenant_id: int = 1
    ) -> list[dict[str, Any]]:
        """Return all accounts with the given status for a tenant."""
        return self._execute(
            "SELECT * FROM accounts WHERE status = ? AND tenant_id = ? ORDER BY backup_rank ASC NULLS LAST, id ASC",
            [status, tenant_id],
        )

    # ------------------------------------------------------------------
    # Posts
    # ------------------------------------------------------------------

    def insert_post(
        self,
        tenant_id: int,
        account_id: int,
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
        raw_payload: dict | None = None,
        user_id: int = 1,
    ) -> int | None:
        """Insert a post using INSERT OR IGNORE for dedup on (source_type, external_id).

        Returns the rowid on insert, or None if the post already existed (sqlite3 only).
        """
        return self._execute_write(
            """INSERT OR IGNORE INTO posts
               (tenant_id, user_id, account_id, external_id, source_type, text,
                posted_at, fetched_at, view_count, repost_count, reply_count,
                like_count, bookmark_count, raw_payload)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                tenant_id, user_id, account_id, external_id, source_type, text,
                posted_at.isoformat(), fetched_at.isoformat(),
                view_count, repost_count, reply_count, like_count, bookmark_count,
                json.dumps(raw_payload) if raw_payload else None,
            ],
        )

    # ------------------------------------------------------------------
    # Engagement snapshots
    # ------------------------------------------------------------------

    def insert_engagement_snapshot(
        self,
        post_id: int,
        view_count: int | None = None,
        repost_count: int | None = None,
        reply_count: int | None = None,
        like_count: int | None = None,
    ) -> int | None:
        """Insert an engagement snapshot for a post."""
        return self._execute_write(
            """INSERT INTO engagement_snapshots
               (post_id, view_count, repost_count, reply_count, like_count)
               VALUES (?, ?, ?, ?, ?)""",
            [post_id, view_count, repost_count, reply_count, like_count],
        )

    # ------------------------------------------------------------------
    # Retweeters
    # ------------------------------------------------------------------

    def insert_retweeter(
        self,
        post_id: int,
        retweeter_external_id: str,
        retweeter_handle: str | None = None,
        followers_count: int | None = None,
        is_verified: bool = False,
        is_monitored: bool = False,
    ) -> int | None:
        """Insert a retweeter row (INSERT OR IGNORE for dedup)."""
        return self._execute_write(
            """INSERT OR IGNORE INTO retweeters
               (post_id, retweeter_external_id, retweeter_handle,
                followers_count, is_verified, is_monitored)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [post_id, retweeter_external_id, retweeter_handle,
             followers_count, is_verified, is_monitored],
        )

    # ------------------------------------------------------------------
    # Signals
    # ------------------------------------------------------------------

    def insert_signal(self, **kwargs: Any) -> int | None:
        """Insert a signal row. Accepts all signals columns as keyword args.

        Required: tenant_id, post_id, account_id, ticker,
                  extraction_confidence, direction, signal_date, tier.
        """
        columns = list(kwargs.keys())
        placeholders = ", ".join("?" for _ in columns)
        col_names = ", ".join(columns)
        values = list(kwargs.values())
        return self._execute_write(
            f"INSERT INTO signals ({col_names}) VALUES ({placeholders})",
            values,
        )

    def update_signal_outcome(
        self,
        signal_id: int,
        prev_close: float | None = None,
        today_open: float | None = None,
        today_close: float | None = None,
        overnight_return: float | None = None,
        tradeable_return: float | None = None,
        spy_return: float | None = None,
        stock_20d_vol: float | None = None,
        excess_vol_score: float | None = None,
        price_data_source: str | None = None,
        outcome_fetched_at: datetime | None = None,
    ) -> None:
        """Populate outcome columns on an existing signal row."""
        updates: list[str] = []
        values: list[Any] = []
        field_map = [
            ("prev_close", prev_close),
            ("today_open", today_open),
            ("today_close", today_close),
            ("overnight_return", overnight_return),
            ("tradeable_return", tradeable_return),
            ("spy_return", spy_return),
            ("stock_20d_vol", stock_20d_vol),
            ("excess_vol_score", excess_vol_score),
            ("price_data_source", price_data_source),
            ("outcome_fetched_at", outcome_fetched_at.isoformat() if outcome_fetched_at else None),
        ]
        for col, val in field_map:
            if val is not None:
                updates.append(f"{col} = ?")
                values.append(val)
        if not updates:
            return
        values.append(signal_id)
        self._execute_write(
            f"UPDATE signals SET {', '.join(updates)} WHERE id = ?",
            values,
        )

    def get_signals_for_date(
        self,
        signal_date: date,
        tenant_id: int = 1,
    ) -> list[dict[str, Any]]:
        """Return all signals for a given date and tenant."""
        return self._execute(
            """SELECT s.*, a.handle AS account_handle, a.display_name AS account_display_name,
                      a.credibility_score AS account_credibility
               FROM signals s
               JOIN accounts a ON s.account_id = a.id
               WHERE s.signal_date = ? AND s.tenant_id = ?
               ORDER BY s.final_score DESC""",
            [signal_date.isoformat(), tenant_id],
        )

    # ------------------------------------------------------------------
    # Scoring config
    # ------------------------------------------------------------------

    def get_scoring_config(self, tenant_id: int = 1) -> dict[str, float]:
        """Return all scoring_config rows as {key: value}."""
        rows = self._execute(
            "SELECT key, value FROM scoring_config WHERE tenant_id = ?",
            [tenant_id],
        )
        return {r["key"]: r["value"] for r in rows}

    # ------------------------------------------------------------------
    # Messages sent
    # ------------------------------------------------------------------

    def log_message_sent(
        self,
        kind: str,
        delivery: str,
        status: str,
        body_preview: str | None = None,
        provider_id: str | None = None,
        error_message: str | None = None,
        tenant_id: int = 1,
        user_id: int = 1,
        sent_at: datetime | None = None,
    ) -> int | None:
        """Log a WhatsApp delivery attempt."""
        ts = (sent_at or datetime.utcnow()).isoformat()
        return self._execute_write(
            """INSERT INTO messages_sent
               (tenant_id, user_id, kind, sent_at, delivery, status,
                body_preview, provider_id, error_message)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [tenant_id, user_id, kind, ts, delivery, status,
             body_preview, provider_id, error_message],
        )

    # ------------------------------------------------------------------
    # API usage
    # ------------------------------------------------------------------

    def log_api_usage(
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
        self._execute_write(
            """INSERT INTO api_usage
               (provider, endpoint, input_tokens, output_tokens,
                latency_ms, status, error_message)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [provider, endpoint, input_tokens, output_tokens,
             latency_ms, status, error_message],
        )

    # ------------------------------------------------------------------
    # Daily summaries
    # ------------------------------------------------------------------

    def upsert_daily_summary(self, **kwargs: Any) -> int | None:
        """Insert or update a daily pipeline run summary (idempotent).

        Unique constraint is (tenant_id, summary_date, run_type).
        """
        columns = list(kwargs.keys())
        placeholders = ", ".join("?" for _ in columns)
        col_names = ", ".join(columns)
        values = list(kwargs.values())
        update_cols = [c for c in columns if c not in ("tenant_id", "summary_date", "run_type")]
        update_clause = ", ".join(f"{c}=excluded.{c}" for c in update_cols)
        return self._execute_write(
            f"""INSERT INTO daily_summaries ({col_names}) VALUES ({placeholders})
                ON CONFLICT(tenant_id, summary_date, run_type) DO UPDATE SET {update_clause}""",
            values,
        )


# ---------------------------------------------------------------------------
# CLI: python -m influence_monitor.db.repository --init
# ---------------------------------------------------------------------------

def main() -> None:
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    if "--init" not in sys.argv:
        print("Usage: python -m influence_monitor.db.repository --init")
        sys.exit(1)

    settings = Settings()
    repo = SignalRepository(settings)
    try:
        repo.init_schema()
        repo.seed(phone_e164=settings.recipient_phone_e164)

        # Verification
        rows = repo._execute(
            "SELECT status, COUNT(*) AS cnt FROM accounts GROUP BY status"
        )
        for r in rows:
            print(f"  accounts[status={r['status']}]: {r['cnt']}")

        cfg_count = repo._execute("SELECT COUNT(*) AS cnt FROM scoring_config")
        print(f"  scoring_config rows: {cfg_count[0]['cnt']}")

        print(f"\nDatabase initialised at: {settings.database_path}")
    finally:
        repo.close()


if __name__ == "__main__":
    main()
