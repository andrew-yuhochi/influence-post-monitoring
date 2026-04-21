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
from datetime import date, datetime, timezone
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
    """Direct HTTP client for Turso's v2 pipeline API.

    Replaces the unmaintained libsql_client package which failed with
    KeyError: 'result' against Turso's current response format.
    Uses httpx (already a project dependency) to POST to /v2/pipeline.
    """

    def __init__(self, url: str, auth_token: str | None) -> None:
        import httpx  # already in requirements.txt

        # Normalise libsql:// scheme to https:// for the HTTP API.
        if url.startswith("libsql://"):
            url = url.replace("libsql://", "https://", 1)

        self._pipeline_url = f"{url}/v2/pipeline"
        self._headers: dict[str, str] = {
            "Content-Type": "application/json",
        }
        if auth_token:
            self._headers["Authorization"] = f"Bearer {auth_token}"
        self._client = httpx.Client(timeout=30.0)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_turso_value(v: Any) -> dict[str, str]:
        """Convert a Python value to a Turso v2 typed value object."""
        if v is None:
            return {"type": "null", "value": "null"}
        if isinstance(v, bool):
            return {"type": "integer", "value": "1" if v else "0"}
        if isinstance(v, int):
            return {"type": "integer", "value": str(v)}
        if isinstance(v, float):
            return {"type": "float", "value": float(v)}
        if isinstance(v, bytes):
            import base64
            return {"type": "blob", "value": base64.b64encode(v).decode()}
        return {"type": "text", "value": str(v)}

    def _post_pipeline(self, requests: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """POST a batch of pipeline requests and return the results list."""
        body = {"requests": requests}
        response = self._client.post(self._pipeline_url, headers=self._headers, json=body)
        try:
            response.raise_for_status()
        except Exception as exc:
            logger.error(
                "Turso pipeline request failed: status=%s body=%s",
                response.status_code,
                response.text[:500],
            )
            raise
        data = response.json()
        return data["results"]

    def _build_execute_request(self, sql: str, params: list[Any] | None) -> dict[str, Any]:
        """Build a single execute request object for the pipeline body."""
        stmt: dict[str, Any] = {"sql": sql}
        if params:
            stmt["args"] = [self._to_turso_value(p) for p in params]
        return {"type": "execute", "stmt": stmt}

    @staticmethod
    def _parse_result_to_dicts(result: dict[str, Any]) -> list[dict[str, Any]]:
        """Convert a Turso v2 execute result into a list of plain dicts."""
        cols = [c["name"] for c in result.get("cols", [])]
        rows_out: list[dict[str, Any]] = []
        for row in result.get("rows", []):
            row_dict: dict[str, Any] = {}
            for col_name, cell in zip(cols, row):
                cell_type = cell.get("type", "null")
                raw = cell.get("value")
                if cell_type == "null" or raw is None:
                    row_dict[col_name] = None
                elif cell_type == "integer":
                    row_dict[col_name] = int(raw)
                elif cell_type == "real":
                    row_dict[col_name] = float(raw)
                else:
                    row_dict[col_name] = raw
            rows_out.append(row_dict)
        return rows_out

    # ------------------------------------------------------------------
    # Public interface (mirrors _Sqlite3Backend)
    # ------------------------------------------------------------------

    def execute(
        self,
        sql: str,
        params: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        requests = [
            self._build_execute_request(sql, params),
            {"type": "close"},
        ]
        results = self._post_pipeline(requests)
        # results[0] is the execute response; results[1] is close
        execute_result = results[0]
        if execute_result.get("type") == "error":
            raise RuntimeError(f"Turso execute error: {execute_result.get('error')}")
        inner = execute_result["response"]["result"]
        return self._parse_result_to_dicts(inner)

    def executemany(self, sql: str, param_list: list[list[Any]]) -> None:
        if not param_list:
            return
        requests: list[dict[str, Any]] = [
            self._build_execute_request(sql, params) for params in param_list
        ]
        requests.append({"type": "close"})
        results = self._post_pipeline(requests)
        for i, res in enumerate(results[:-1]):  # skip the close result
            if res.get("type") == "error":
                raise RuntimeError(
                    f"Turso executemany error at index {i}: {res.get('error')}"
                )

    def executescript(self, script: str) -> None:
        """Execute a multi-statement SQL script (split on ';')."""

        def _has_sql(stmt: str) -> bool:
            return any(
                line.strip() and not line.strip().startswith("--")
                for line in stmt.splitlines()
            )

        statements = [s.strip() for s in script.split(";")]
        statements = [s for s in statements if _has_sql(s)]
        if not statements:
            return
        requests: list[dict[str, Any]] = [
            {"type": "execute", "stmt": {"sql": s}} for s in statements
        ]
        requests.append({"type": "close"})
        results = self._post_pipeline(requests)
        for i, res in enumerate(results[:-1]):  # skip the close result
            if res.get("type") == "error":
                raise RuntimeError(
                    f"Turso executescript error at statement {i}: {res.get('error')}"
                )

    def execute_returning_lastrowid(
        self,
        sql: str,
        params: list[Any] | None = None,
    ) -> int | None:
        """Execute a write statement and return last_insert_rowid from the response."""
        requests = [
            self._build_execute_request(sql, params),
            {"type": "close"},
        ]
        results = self._post_pipeline(requests)
        execute_result = results[0]
        if execute_result.get("type") == "error":
            raise RuntimeError(f"Turso execute error: {execute_result.get('error')}")
        inner = execute_result["response"]["result"]
        rowid = inner.get("last_insert_rowid")
        if rowid is not None:
            return int(rowid)
        return None

    def close(self) -> None:
        self._client.close()

    @property
    def lastrowid(self) -> int | None:
        return None  # not available via Turso HTTP API


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
            assert isinstance(self._backend, _LibsqlBackend)
            return self._backend.execute_returning_lastrowid(sql, params)
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
        # Migration guard: add shown_in_morning_alert to existing DBs.
        # The CREATE TABLE in schema.sql already includes the column for fresh DBs;
        # this ALTER TABLE is a no-op on new DBs but adds the column on older ones.
        try:
            self._execute_write(
                "ALTER TABLE signals ADD COLUMN shown_in_morning_alert INTEGER NOT NULL DEFAULT 0"
            )
        except Exception:
            pass  # column already exists
        try:
            self._execute_write(
                "CREATE INDEX IF NOT EXISTS idx_signals_shown"
                " ON signals(signal_date, tenant_id, shown_in_morning_alert)"
            )
        except Exception:
            pass  # index already exists
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
        # Backfill new normalization keys into existing DBs where INSERT OR IGNORE
        # above is a no-op (the row already exists from an older seed run).
        for key, value in [
            ("virality_views_normalization", 1_000_000),
            ("virality_reposts_normalization", 5_000),
        ]:
            self._execute_write(
                "INSERT OR IGNORE INTO scoring_config (tenant_id, key, value) VALUES (?, ?, ?)",
                [tenant_id, key, str(value)],
            )

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
        follower_count_at_post: int | None = None,
    ) -> int | None:
        """Insert or update an account row. Returns rowid on sqlite3 backend."""
        return self._execute_write(
            """INSERT INTO accounts
               (tenant_id, user_id, handle, display_name, angle,
                credibility_score, status, backup_rank, notes,
                follower_count_at_post)
               VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, handle) DO UPDATE SET
                 display_name=excluded.display_name,
                 angle=excluded.angle,
                 credibility_score=excluded.credibility_score,
                 status=excluded.status,
                 backup_rank=excluded.backup_rank,
                 notes=excluded.notes,
                 follower_count_at_post=COALESCE(excluded.follower_count_at_post,
                                                  accounts.follower_count_at_post)""",
            [tenant_id, handle, display_name, angle,
             credibility_score, status, backup_rank, notes,
             follower_count_at_post],
        )

    def update_account_failure(
        self,
        account_id: int,
        last_failure_at: datetime | None = None,
    ) -> None:
        """Increment consecutive_failures and record last_failure_at."""
        ts = (last_failure_at or datetime.now(tz=timezone.utc)).isoformat()
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
    # Price cache (market-cap resolver — weekly TTL)
    # ------------------------------------------------------------------

    def get_cached_market_cap(self, ticker: str) -> dict | None:
        """Return a price_cache row if it exists and was updated within 7 days.

        Returns the full row as a dict (keys: ticker, market_cap_b, market_cap_class,
        sector, industry, last_updated), or None on cache miss / stale entry.
        """
        rows = self._execute(
            """SELECT ticker, market_cap_b, market_cap_class, sector, industry, last_updated
               FROM price_cache
               WHERE ticker = ?
                 AND last_updated >= datetime('now', '-7 days')""",
            [ticker.upper()],
        )
        return rows[0] if rows else None

    def upsert_price_cache(
        self,
        ticker: str,
        market_cap_b: float | None,
        market_cap_class: str,
        sector: str | None,
        industry: str | None,
    ) -> None:
        """Insert or replace a price_cache row, stamping last_updated to now."""
        self._execute_write(
            """INSERT INTO price_cache
               (ticker, market_cap_b, market_cap_class, sector, industry, last_updated)
               VALUES (?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(ticker) DO UPDATE SET
                 market_cap_b=excluded.market_cap_b,
                 market_cap_class=excluded.market_cap_class,
                 sector=excluded.sector,
                 industry=excluded.industry,
                 last_updated=excluded.last_updated""",
            [ticker.upper(), market_cap_b, market_cap_class, sector, industry],
        )

    # ------------------------------------------------------------------
    # Accounts — helper for amplifier cross-reference
    # ------------------------------------------------------------------

    def get_account_external_ids(self, tenant_id: int = 1) -> set[str]:
        """Return the set of external_ids for all accounts (any status) in a tenant.

        Used by AmplifierFetcher to flag retweeters that are monitored accounts.
        Only non-NULL external_ids are returned.
        """
        rows = self._execute(
            "SELECT external_id FROM accounts WHERE tenant_id = ? AND external_id IS NOT NULL",
            [tenant_id],
        )
        return {r["external_id"] for r in rows}

    # ------------------------------------------------------------------
    # Signals
    # ------------------------------------------------------------------

    def delete_signals_for_date(
        self,
        signal_date: date,
        tenant_id: int = 1,
    ) -> None:
        """Delete all signals (and their fixture posts) for a given date and tenant.

        Used by fixture modes to ensure idempotent re-runs — each fixture run
        starts from a clean slate for the target date.
        """
        date_str = signal_date.isoformat()
        # Collect fixture post IDs before deleting signals (FK: signals → posts)
        post_id_rows = self._execute(
            "SELECT DISTINCT post_id FROM signals WHERE signal_date = ? AND tenant_id = ?",
            [date_str, tenant_id],
        )
        fixture_post_ids = [r["post_id"] for r in post_id_rows]

        # Delete signals first (they reference posts via FK)
        self._execute_write(
            "DELETE FROM signals WHERE signal_date = ? AND tenant_id = ?",
            [date_str, tenant_id],
        )

        # Now delete the fixture posts (no signals referencing them any more)
        for post_id in fixture_post_ids:
            self._execute_write(
                "DELETE FROM posts WHERE id = ? AND source_type = 'fixture'",
                [post_id],
            )
        logger.debug(
            "delete_signals_for_date: cleared signals for %s (tenant_id=%d)",
            date_str,
            tenant_id,
        )

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

    def mark_signals_shown_in_morning(self, signal_ids: list[int]) -> None:
        """Mark the given signal IDs as shown in the morning alert."""
        for sid in signal_ids:
            self._execute_write(
                "UPDATE signals SET shown_in_morning_alert = 1 WHERE id = ?",
                [sid],
            )

    def get_signals_for_date(
        self,
        signal_date: date,
        tenant_id: int = 1,
        shown_only: bool = False,
    ) -> list[dict[str, Any]]:
        """Return signals for a given date and tenant.

        Args:
            signal_date: The date to query.
            tenant_id:   Tenant scope (default 1).
            shown_only:  When True, only return signals that were shown in the
                         morning alert (shown_in_morning_alert = 1).  Defaults to
                         False so that the morning pipeline and any admin queries
                         still see all scored signals.
        """
        where_extra = " AND s.shown_in_morning_alert = 1" if shown_only else ""
        return self._execute(
            f"""SELECT s.*, a.handle AS account_handle, a.display_name AS account_display_name,
                      a.credibility_score AS account_credibility
               FROM signals s
               JOIN accounts a ON s.account_id = a.id
               WHERE s.signal_date = ? AND s.tenant_id = ?{where_extra}
               ORDER BY s.final_score DESC""",
            [signal_date.isoformat(), tenant_id],
        )

    def get_signals_for_date_range(
        self,
        start_date: date,
        end_date: date,
        tenant_id: int = 1,
    ) -> list[dict[str, Any]]:
        """Return all signals in the inclusive [start_date, end_date] range for a tenant.

        Joins accounts so callers receive ``account_handle`` and
        ``account_display_name`` on every row (mirrors get_signals_for_date).

        Args:
            start_date: First day of the range (inclusive).
            end_date:   Last day of the range (inclusive).
            tenant_id:  Tenant scope (default 1).

        Returns:
            List of signal rows as dicts, ordered by signal_date DESC then
            final_score DESC so the most-recent highest-conviction signals
            come first.
        """
        return self._execute(
            """SELECT s.*, a.handle AS account_handle, a.display_name AS account_display_name,
                      a.credibility_score AS account_credibility
               FROM signals s
               JOIN accounts a ON s.account_id = a.id
               WHERE s.signal_date >= ? AND s.signal_date <= ? AND s.tenant_id = ?
               ORDER BY s.signal_date DESC, s.final_score DESC""",
            [start_date.isoformat(), end_date.isoformat(), tenant_id],
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
        ts = (sent_at or datetime.now(tz=timezone.utc)).isoformat()
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


# backward-compat alias
DatabaseRepository = SignalRepository


if __name__ == "__main__":
    main()
