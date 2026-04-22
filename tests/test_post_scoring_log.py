"""Tests for the post_scoring_log diagnostic table.

Verifies:
- Table is created by init_schema()
- All required columns are present
- log_post_scoring() inserts a row with correct values
- pipeline_stage='failed' rows populate error_message
- tickers_extracted is stored as a JSON array
- processed_at is always populated
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from influence_monitor.config import Settings
from influence_monitor.db.repository import SignalRepository, _Sqlite3Backend

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent


def _make_repo(tmp_path: Path) -> SignalRepository:
    settings = Settings(
        turso_url="",
        database_path=str(tmp_path / "test_scoring_log.db"),
    )
    repo = SignalRepository(settings)
    repo.init_schema()
    return repo


def _column_names(db_path: Path, table: str) -> set[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        return {row[1] for row in cur.fetchall()}
    finally:
        conn.close()


def _table_exists(db_path: Path, table: str) -> bool:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", [table]
        ).fetchone()
        return row[0] == 1
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Table existence and structure
# ---------------------------------------------------------------------------

def test_post_scoring_log_table_exists(tmp_path: Path) -> None:
    """init_schema() must create the post_scoring_log table."""
    repo = _make_repo(tmp_path)
    try:
        db_path = tmp_path / "test_scoring_log.db"
        assert _table_exists(db_path, "post_scoring_log"), (
            "post_scoring_log table not found after init_schema()"
        )
    finally:
        repo.close()


REQUIRED_COLUMNS = [
    "id",
    "user_id",
    "tenant_id",
    "post_id",
    "account_handle",
    "posted_at",
    "fetched_at",
    "post_text",
    "tickers_extracted",
    "extraction_confidence",
    "direction",
    "argument_quality",
    "conviction_score",
    "tier",
    "pipeline_stage",
    "error_message",
    "processed_at",
]


@pytest.mark.parametrize("col", REQUIRED_COLUMNS)
def test_post_scoring_log_has_required_column(col: str, tmp_path: Path) -> None:
    """post_scoring_log must have every column specified in the schema."""
    repo = _make_repo(tmp_path)
    try:
        db_path = tmp_path / "test_scoring_log.db"
        cols = _column_names(db_path, "post_scoring_log")
        assert col in cols, f"post_scoring_log missing column '{col}'"
    finally:
        repo.close()


# ---------------------------------------------------------------------------
# log_post_scoring — basic insert
# ---------------------------------------------------------------------------

def test_log_post_scoring_inserts_row(tmp_path: Path) -> None:
    """log_post_scoring() returns a non-None rowid and the row is queryable."""
    repo = _make_repo(tmp_path)
    try:
        now = datetime.now(tz=timezone.utc).isoformat()
        rowid = repo.log_post_scoring(
            post_id="tweet_abc123",
            pipeline_stage="ingested",
            account_handle="BillAckman",
            posted_at=now,
            fetched_at=now,
            post_text="$FNMA is massively underpriced and the govt knows it.",
        )
        assert rowid is not None, "log_post_scoring should return a rowid"
        assert rowid > 0

        rows = repo._execute(
            "SELECT * FROM post_scoring_log WHERE post_id = ?", ["tweet_abc123"]
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["pipeline_stage"] == "ingested"
        assert row["account_handle"] == "BillAckman"
        assert row["processed_at"] is not None
    finally:
        repo.close()


def test_log_post_scoring_extracted_stage(tmp_path: Path) -> None:
    """Stage 'extracted' stores tickers_extracted as a JSON array."""
    repo = _make_repo(tmp_path)
    try:
        now = datetime.now(tz=timezone.utc).isoformat()
        repo.log_post_scoring(
            post_id="tweet_extract",
            pipeline_stage="extracted",
            account_handle="burryofficialtwit",
            posted_at=now,
            fetched_at=now,
            post_text="Long $FNMA and $FMCC here.",
            tickers_extracted=["FNMA", "FMCC"],
            extraction_confidence=3.0,
        )
        rows = repo._execute(
            "SELECT tickers_extracted, extraction_confidence FROM post_scoring_log"
            " WHERE post_id = 'tweet_extract'"
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["extraction_confidence"] == pytest.approx(3.0)
        tickers = json.loads(row["tickers_extracted"])
        assert set(tickers) == {"FNMA", "FMCC"}
    finally:
        repo.close()


def test_log_post_scoring_scored_stage(tmp_path: Path) -> None:
    """Stage 'scored' stores direction, argument_quality, conviction_score, and tier."""
    repo = _make_repo(tmp_path)
    try:
        now = datetime.now(tz=timezone.utc).isoformat()
        repo.log_post_scoring(
            post_id="tweet_scored",
            pipeline_stage="scored",
            account_handle="BillAckman",
            posted_at=now,
            fetched_at=now,
            post_text="Going long $AAPL.",
            tickers_extracted=["AAPL"],
            direction="LONG",
            argument_quality="HIGH",
            conviction_score=7.5,
            tier="ACT_NOW",
        )
        rows = repo._execute(
            "SELECT direction, argument_quality, conviction_score, tier"
            " FROM post_scoring_log WHERE post_id = 'tweet_scored'"
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["direction"] == "LONG"
        assert row["argument_quality"] == "HIGH"
        assert row["conviction_score"] == pytest.approx(7.5)
        assert row["tier"] == "ACT_NOW"
    finally:
        repo.close()


def test_log_post_scoring_failed_stage(tmp_path: Path) -> None:
    """Stage 'failed' stores the error_message."""
    repo = _make_repo(tmp_path)
    try:
        now = datetime.now(tz=timezone.utc).isoformat()
        repo.log_post_scoring(
            post_id="tweet_failed",
            pipeline_stage="failed",
            account_handle="CathieDWood",
            posted_at=now,
            fetched_at=now,
            post_text="Some post text here.",
            error_message="TickerExtractor: ValueError: spacy model not found",
        )
        rows = repo._execute(
            "SELECT pipeline_stage, error_message FROM post_scoring_log"
            " WHERE post_id = 'tweet_failed'"
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["pipeline_stage"] == "failed"
        assert "TickerExtractor" in row["error_message"]
    finally:
        repo.close()


def test_log_post_scoring_multiple_rows_same_post(tmp_path: Path) -> None:
    """Multiple rows for the same post_id across different stages are allowed (no dedup)."""
    repo = _make_repo(tmp_path)
    try:
        now = datetime.now(tz=timezone.utc).isoformat()
        for stage in ("ingested", "extracted", "scored"):
            repo.log_post_scoring(
                post_id="tweet_multi",
                pipeline_stage=stage,
                account_handle="BillAckman",
                posted_at=now,
                fetched_at=now,
                post_text="Multi stage post.",
            )
        rows = repo._execute(
            "SELECT pipeline_stage FROM post_scoring_log WHERE post_id = 'tweet_multi'"
            " ORDER BY id"
        )
        assert len(rows) == 3
        stages = [r["pipeline_stage"] for r in rows]
        assert stages == ["ingested", "extracted", "scored"]
    finally:
        repo.close()


def test_log_post_scoring_post_text_truncated_to_500(tmp_path: Path) -> None:
    """post_text is stored as first 500 chars only."""
    repo = _make_repo(tmp_path)
    try:
        long_text = "A" * 1000
        now = datetime.now(tz=timezone.utc).isoformat()
        repo.log_post_scoring(
            post_id="tweet_long",
            pipeline_stage="ingested",
            account_handle="tester",
            post_text=long_text,
            posted_at=now,
            fetched_at=now,
        )
        rows = repo._execute(
            "SELECT post_text FROM post_scoring_log WHERE post_id = 'tweet_long'"
        )
        assert len(rows) == 1
        stored_text = rows[0]["post_text"]
        assert len(stored_text) == 500, (
            f"Expected 500 chars, got {len(stored_text)}"
        )
    finally:
        repo.close()


def test_log_post_scoring_null_optional_fields(tmp_path: Path) -> None:
    """All optional fields may be omitted — only post_id and pipeline_stage are required."""
    repo = _make_repo(tmp_path)
    try:
        rowid = repo.log_post_scoring(
            post_id="tweet_minimal",
            pipeline_stage="ingested",
        )
        assert rowid is not None
        rows = repo._execute(
            "SELECT * FROM post_scoring_log WHERE post_id = 'tweet_minimal'"
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["tickers_extracted"] is None
        assert row["conviction_score"] is None
        assert row["error_message"] is None
        assert row["processed_at"] is not None  # always stamped
    finally:
        repo.close()


# ---------------------------------------------------------------------------
# Idempotency of table creation (init_schema called twice)
# ---------------------------------------------------------------------------

def test_init_schema_idempotent_post_scoring_log(tmp_path: Path) -> None:
    """Calling init_schema() twice must not raise (CREATE TABLE IF NOT EXISTS)."""
    repo = _make_repo(tmp_path)
    try:
        repo.init_schema()  # second call
        db_path = tmp_path / "test_scoring_log.db"
        assert _table_exists(db_path, "post_scoring_log")
    finally:
        repo.close()
