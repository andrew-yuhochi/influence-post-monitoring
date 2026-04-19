"""Tests for TASK-003: schema.sql + SignalRepository.

Validates:
- Schema creates all 11 required tables with correct columns/constraints
- Multi-tenancy: user_id + tenant_id on row-producing tables
- accounts table has consecutive_failures and last_failure_at columns
- posts UNIQUE constraint on (source_type, external_id)
- signals has every factor column and outcome column
- Seeding: 30 primaries, 15 backups, 19 scoring_config rows
- Repository CRUD methods
- Backend falls back to sqlite3 when TURSO_URL is unset
- Fixture files exist and have the required shape
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from datetime import date, datetime
from pathlib import Path

import pytest

from influence_monitor.config import Settings
from influence_monitor.db.repository import SignalRepository, _Sqlite3Backend

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"
CONFIG_DIR = PROJECT_ROOT / "config"


def _make_repo(tmp_path: Path) -> SignalRepository:
    """Create an in-process SQLite-backed SignalRepository in a temp dir."""
    settings = Settings(
        turso_url="",
        database_path=str(tmp_path / "test_signals.db"),
    )
    repo = SignalRepository(settings)
    repo.init_schema()
    return repo


def _table_info(db_path: Path, table: str) -> list[dict]:
    """Return PRAGMA table_info rows as dicts for the named table."""
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _column_names(db_path: Path, table: str) -> set[str]:
    return {row["name"] for row in _table_info(db_path, table)}


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", [name]
    ).fetchone()
    return row[0] == 1


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    return tmp_path / "test_signals.db"


@pytest.fixture
def repo(tmp_db: Path) -> SignalRepository:
    repo = _make_repo(tmp_db.parent)
    yield repo
    repo.close()


@pytest.fixture
def seeded_repo(tmp_db: Path) -> SignalRepository:
    repo = _make_repo(tmp_db.parent)
    repo.seed(phone_e164="+14161234567", tenant_id=1)
    yield repo
    repo.close()


# ---------------------------------------------------------------------------
# Schema: table existence
# ---------------------------------------------------------------------------

REQUIRED_TABLES = [
    "tenants",
    "users",
    "accounts",
    "posts",
    "engagement_snapshots",
    "retweeters",
    "price_cache",
    "scoring_config",
    "signals",
    "messages_sent",
    "daily_summaries",
    "api_usage",
]


@pytest.mark.parametrize("table_name", REQUIRED_TABLES)
def test_schema_creates_all_required_tables(repo: SignalRepository, tmp_db: Path, table_name: str) -> None:
    """All 11 tables (plus api_usage = 12) must exist after init_schema."""
    conn = sqlite3.connect(str(tmp_db.parent / "test_signals.db"))
    try:
        assert _table_exists(conn, table_name), f"Table '{table_name}' not found in schema"
    finally:
        conn.close()


def test_schema_creates_exactly_required_tables(repo: SignalRepository, tmp_db: Path) -> None:
    """Acceptance criterion: schema.sql creates all 11 tables from TDD §2.4.

    (The spec lists 11 explicitly; api_usage is implicit from the code comments.)
    """
    conn = sqlite3.connect(str(tmp_db.parent / "test_signals.db"))
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        created = {r[0] for r in rows}
        required = set(REQUIRED_TABLES)
        missing = required - created
        assert not missing, f"Tables missing from schema: {missing}"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema: multi-tenancy columns
# ---------------------------------------------------------------------------

MULTI_TENANT_TABLES = [
    "accounts",
    "posts",
    "signals",
    "messages_sent",
    "daily_summaries",
    "scoring_config",
    "users",
]


@pytest.mark.parametrize("table_name", MULTI_TENANT_TABLES)
def test_tenant_id_column_present(repo: SignalRepository, tmp_db: Path, table_name: str) -> None:
    """Every row-producing table must have tenant_id."""
    db_path = tmp_db.parent / "test_signals.db"
    cols = _column_names(db_path, table_name)
    assert "tenant_id" in cols, f"tenant_id missing from {table_name}"


# user_id is required on a subset of tables per spec
USER_ID_TABLES = ["accounts", "posts", "signals", "messages_sent", "users"]


@pytest.mark.parametrize("table_name", USER_ID_TABLES)
def test_user_id_column_present(repo: SignalRepository, tmp_db: Path, table_name: str) -> None:
    """Row-producing tables must have user_id."""
    db_path = tmp_db.parent / "test_signals.db"
    cols = _column_names(db_path, table_name)
    assert "user_id" in cols, f"user_id missing from {table_name}"


# ---------------------------------------------------------------------------
# Schema: accounts debounce columns
# ---------------------------------------------------------------------------

def test_accounts_consecutive_failures_column(repo: SignalRepository, tmp_db: Path) -> None:
    """accounts table must have consecutive_failures INTEGER NOT NULL DEFAULT 0."""
    db_path = tmp_db.parent / "test_signals.db"
    info = {row["name"]: row for row in _table_info(db_path, "accounts")}
    assert "consecutive_failures" in info, "consecutive_failures column missing from accounts"
    row = info["consecutive_failures"]
    assert row["dflt_value"] == "0", (
        f"consecutive_failures default should be 0, got {row['dflt_value']!r}"
    )
    assert row["notnull"] == 1, "consecutive_failures should be NOT NULL"


def test_accounts_last_failure_at_column(repo: SignalRepository, tmp_db: Path) -> None:
    """accounts table must have last_failure_at DATETIME."""
    db_path = tmp_db.parent / "test_signals.db"
    cols = _column_names(db_path, "accounts")
    assert "last_failure_at" in cols, "last_failure_at missing from accounts"


# ---------------------------------------------------------------------------
# Schema: posts UNIQUE constraint on (source_type, external_id)
# ---------------------------------------------------------------------------

def test_posts_unique_source_type_external_id(repo: SignalRepository, tmp_db: Path) -> None:
    """posts must enforce UNIQUE (source_type, external_id)."""
    db_path = tmp_db.parent / "test_signals.db"
    conn = sqlite3.connect(str(db_path))
    try:
        # Need a parent account row first (FK)
        conn.execute(
            "INSERT OR IGNORE INTO tenants (id, name) VALUES (1, 'default')"
        )
        conn.execute(
            "INSERT INTO accounts (tenant_id, user_id, handle, credibility_score, status) "
            "VALUES (1, 1, 'TestHandle', 5.0, 'primary')"
        )
        aid = conn.execute("SELECT id FROM accounts WHERE handle='TestHandle'").fetchone()[0]
        now_iso = datetime.utcnow().isoformat()

        conn.execute(
            "INSERT INTO posts (tenant_id, user_id, account_id, external_id, source_type, text, posted_at, fetched_at) "
            "VALUES (1, 1, ?, 'tweet123', 'twitter', 'hello', ?, ?)",
            [aid, now_iso, now_iso],
        )
        conn.commit()

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO posts (tenant_id, user_id, account_id, external_id, source_type, text, posted_at, fetched_at) "
                "VALUES (1, 1, ?, 'tweet123', 'twitter', 'duplicate', ?, ?)",
                [aid, now_iso, now_iso],
            )
            conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema: signals columns — factor scores + outcome columns
# ---------------------------------------------------------------------------

FACTOR_COLUMNS = [
    "score_credibility",
    "score_virality_abs",
    "score_virality_vel",
    "score_consensus",
    "score_amplifier",
    "liquidity_modifier",
]

OUTCOME_COLUMNS = [
    "prev_close",
    "today_open",
    "today_close",
    "overnight_return",
    "tradeable_return",
    "spy_return",
    "stock_20d_vol",
    "excess_vol_score",
    "price_data_source",
]

SIGNAL_REQUIRED_COLUMNS = [
    "direction_flip",
    "conflict_group",
    "penalty_applied",
    "final_score",
    "tier",
    "morning_rank",
]


@pytest.mark.parametrize("col", FACTOR_COLUMNS + OUTCOME_COLUMNS + SIGNAL_REQUIRED_COLUMNS)
def test_signals_has_required_column(repo: SignalRepository, tmp_db: Path, col: str) -> None:
    """signals table must have every factor, outcome, and metadata column."""
    db_path = tmp_db.parent / "test_signals.db"
    cols = _column_names(db_path, "signals")
    assert col in cols, f"signals column '{col}' missing"


# ---------------------------------------------------------------------------
# Seeding: 30 primaries, 15 backups, 17 scoring_config rows
# ---------------------------------------------------------------------------

def test_seed_accounts_primary_count(seeded_repo: SignalRepository) -> None:
    """After seeding: SELECT COUNT(*) FROM accounts WHERE status='primary' = 30."""
    rows = seeded_repo._execute(
        "SELECT COUNT(*) AS cnt FROM accounts WHERE status='primary'"
    )
    assert rows[0]["cnt"] == 30, f"Expected 30 primary accounts, got {rows[0]['cnt']}"


def test_seed_accounts_backup_count(seeded_repo: SignalRepository) -> None:
    """After seeding: SELECT COUNT(*) FROM accounts WHERE status='backup' = 15."""
    rows = seeded_repo._execute(
        "SELECT COUNT(*) AS cnt FROM accounts WHERE status='backup'"
    )
    assert rows[0]["cnt"] == 15, f"Expected 15 backup accounts, got {rows[0]['cnt']}"


def test_seed_scoring_config_count(seeded_repo: SignalRepository) -> None:
    """After seeding: SELECT COUNT(*) FROM scoring_config = 19."""
    rows = seeded_repo._execute(
        "SELECT COUNT(*) AS cnt FROM scoring_config"
    )
    assert rows[0]["cnt"] == 19, f"Expected 19 scoring_config rows, got {rows[0]['cnt']}"


def test_seed_total_accounts_is_45(seeded_repo: SignalRepository) -> None:
    """Total accounts seeded from config/accounts.json must be 45."""
    rows = seeded_repo._execute("SELECT COUNT(*) AS cnt FROM accounts")
    assert rows[0]["cnt"] == 45, f"Expected 45 total accounts, got {rows[0]['cnt']}"


# ---------------------------------------------------------------------------
# Seeding: scoring_config values match DATA-SOURCES.md spec
# ---------------------------------------------------------------------------

EXPECTED_SCORING_CONFIG = {
    "direction_flip_penalty": 0.0,
    "vol_lookback_days": 20.0,
    "max_consecutive_failures": 3.0,
    "retry_rest_minutes": 30.0,
    "watch_velocity_floor": 1000.0,
    "virality_views_threshold": 50000.0,
    "virality_reposts_threshold": 500.0,
    "liq_mega": 0.8,
    "liq_large": 0.9,
    "liq_mid": 1.0,
    "liq_small": 1.15,
    "liq_micro": 1.3,
}


@pytest.mark.parametrize("key,expected_value", EXPECTED_SCORING_CONFIG.items())
def test_scoring_config_seed_values(seeded_repo: SignalRepository, key: str, expected_value: float) -> None:
    """scoring_config seed values must match DATA-SOURCES.md spec exactly."""
    config = seeded_repo.get_scoring_config(tenant_id=1)
    assert key in config, f"scoring_config key '{key}' missing after seed"
    assert config[key] == pytest.approx(expected_value, abs=1e-9), (
        f"scoring_config[{key}]: expected {expected_value}, got {config[key]}"
    )


def test_scoring_config_has_all_weight_keys(seeded_repo: SignalRepository) -> None:
    """All 5 weight keys must be present in scoring_config."""
    config = seeded_repo.get_scoring_config(tenant_id=1)
    weight_keys = [
        "weight_credibility",
        "weight_virality_abs",
        "weight_virality_vel",
        "weight_consensus",
        "weight_amplifier",
    ]
    for key in weight_keys:
        assert key in config, f"weight key '{key}' missing from scoring_config"


def test_scoring_config_has_b14_b15_accounts(seeded_repo: SignalRepository) -> None:
    """B14 Josh Wolfe and B15 Mark Yusko must be seeded as backup accounts."""
    rows = seeded_repo._execute(
        "SELECT handle, backup_rank FROM accounts WHERE handle IN ('wolfejosh', 'MarkYusko') "
        "ORDER BY backup_rank"
    )
    handles = [r["handle"] for r in rows]
    assert "wolfejosh" in handles, "B14 Josh Wolfe (wolfejosh) missing from accounts"
    assert "MarkYusko" in handles, "B15 Mark Yusko (MarkYusko) missing from accounts"
    ranks = {r["handle"]: r["backup_rank"] for r in rows}
    assert ranks["wolfejosh"] == 14, f"wolfejosh backup_rank={ranks['wolfejosh']}, expected 14"
    assert ranks["MarkYusko"] == 15, f"MarkYusko backup_rank={ranks['MarkYusko']}, expected 15"


# ---------------------------------------------------------------------------
# Repository: CRUD methods
# ---------------------------------------------------------------------------

def _seeded_repo_with_account(tmp_path: Path) -> tuple[SignalRepository, int]:
    """Create a seeded repo and return (repo, account_id) for BillAckman."""
    repo = _make_repo(tmp_path)
    repo.seed(phone_e164="+14161234567", tenant_id=1)
    rows = repo._execute("SELECT id FROM accounts WHERE handle='BillAckman'")
    return repo, rows[0]["id"]


def test_insert_post_returns_rowid(tmp_path: Path) -> None:
    """insert_post returns a non-None rowid on first insert."""
    repo, account_id = _seeded_repo_with_account(tmp_path)
    try:
        now = datetime.utcnow()
        rowid = repo.insert_post(
            tenant_id=1,
            account_id=account_id,
            external_id="tweet_001",
            source_type="twitter",
            text="FNMA is massively underpriced.",
            posted_at=now,
            fetched_at=now,
            view_count=100000,
        )
        assert rowid is not None
        assert rowid > 0
    finally:
        repo.close()


def test_insert_post_dedup_on_source_external_id(tmp_path: Path) -> None:
    """Duplicate (source_type, external_id) returns None (INSERT OR IGNORE)."""
    repo, account_id = _seeded_repo_with_account(tmp_path)
    try:
        now = datetime.utcnow()
        kwargs = dict(
            tenant_id=1,
            account_id=account_id,
            external_id="tweet_dedup",
            source_type="twitter",
            text="First insert.",
            posted_at=now,
            fetched_at=now,
        )
        first = repo.insert_post(**kwargs)
        second = repo.insert_post(**kwargs)
        assert first is not None
        # On sqlite3, INSERT OR IGNORE on duplicate → rowid 0 or None
        # The important thing is no exception is raised
        rows = repo._execute(
            "SELECT COUNT(*) AS cnt FROM posts WHERE external_id='tweet_dedup'"
        )
        assert rows[0]["cnt"] == 1, "Duplicate post was inserted — dedup failed"
    finally:
        repo.close()


def test_insert_engagement_snapshot(tmp_path: Path) -> None:
    """insert_engagement_snapshot inserts a row linked to a post."""
    repo, account_id = _seeded_repo_with_account(tmp_path)
    try:
        now = datetime.utcnow()
        post_id = repo.insert_post(
            tenant_id=1, account_id=account_id, external_id="snap_post",
            source_type="twitter", text="snapshot test", posted_at=now, fetched_at=now,
        )
        snap_id = repo.insert_engagement_snapshot(
            post_id=post_id, view_count=5000, repost_count=50
        )
        assert snap_id is not None
        rows = repo._execute(
            "SELECT view_count FROM engagement_snapshots WHERE post_id=?", [post_id]
        )
        assert rows[0]["view_count"] == 5000
    finally:
        repo.close()


def test_insert_retweeter(tmp_path: Path) -> None:
    """insert_retweeter inserts with dedup on (post_id, retweeter_external_id)."""
    repo, account_id = _seeded_repo_with_account(tmp_path)
    try:
        now = datetime.utcnow()
        post_id = repo.insert_post(
            tenant_id=1, account_id=account_id, external_id="rt_post",
            source_type="twitter", text="retweeter test", posted_at=now, fetched_at=now,
        )
        r1 = repo.insert_retweeter(
            post_id=post_id, retweeter_external_id="user_001",
            retweeter_handle="john_doe", followers_count=50000,
            is_verified=True, is_monitored=False,
        )
        # Duplicate insert
        r2 = repo.insert_retweeter(
            post_id=post_id, retweeter_external_id="user_001",
            retweeter_handle="john_doe", followers_count=50000,
        )
        assert r1 is not None
        rows = repo._execute(
            "SELECT COUNT(*) AS cnt FROM retweeters WHERE post_id=?", [post_id]
        )
        assert rows[0]["cnt"] == 1, "Duplicate retweeter was inserted"
    finally:
        repo.close()


def test_update_account_failure_increments_counter(tmp_path: Path) -> None:
    """update_account_failure increments consecutive_failures by 1."""
    repo, account_id = _seeded_repo_with_account(tmp_path)
    try:
        repo.update_account_failure(account_id=account_id)
        rows = repo._execute(
            "SELECT consecutive_failures FROM accounts WHERE id=?", [account_id]
        )
        assert rows[0]["consecutive_failures"] == 1

        repo.update_account_failure(account_id=account_id)
        rows = repo._execute(
            "SELECT consecutive_failures FROM accounts WHERE id=?", [account_id]
        )
        assert rows[0]["consecutive_failures"] == 2
    finally:
        repo.close()


def test_reset_account_failures_sets_zero(tmp_path: Path) -> None:
    """reset_account_failures sets consecutive_failures = 0."""
    repo, account_id = _seeded_repo_with_account(tmp_path)
    try:
        repo.update_account_failure(account_id=account_id)
        repo.update_account_failure(account_id=account_id)
        repo.reset_account_failures(account_id=account_id)
        rows = repo._execute(
            "SELECT consecutive_failures FROM accounts WHERE id=?", [account_id]
        )
        assert rows[0]["consecutive_failures"] == 0
    finally:
        repo.close()


def test_rename_account_handle(tmp_path: Path) -> None:
    """rename_account_handle updates the handle in place."""
    repo, account_id = _seeded_repo_with_account(tmp_path)
    try:
        repo.rename_account_handle(account_id=account_id, new_handle="BillAckman_NEW")
        rows = repo._execute(
            "SELECT handle FROM accounts WHERE id=?", [account_id]
        )
        assert rows[0]["handle"] == "BillAckman_NEW"
    finally:
        repo.close()


def test_insert_signal_and_get_signals_for_date(tmp_path: Path) -> None:
    """insert_signal persists a row; get_signals_for_date retrieves it."""
    repo, account_id = _seeded_repo_with_account(tmp_path)
    try:
        now = datetime.utcnow()
        post_id = repo.insert_post(
            tenant_id=1, account_id=account_id, external_id="sig_post",
            source_type="twitter", text="signal test", posted_at=now, fetched_at=now,
        )
        sig_date = date(2026, 4, 21)
        repo.insert_signal(
            tenant_id=1, user_id=1, post_id=post_id, account_id=account_id,
            signal_date=sig_date.isoformat(), ticker="FNMA",
            extraction_confidence="HIGH", direction="LONG", tier="ACT_NOW",
            conviction_score=8.2, final_score=8.2,
        )
        signals = repo.get_signals_for_date(sig_date, tenant_id=1)
        assert len(signals) == 1
        assert signals[0]["ticker"] == "FNMA"
    finally:
        repo.close()


def test_update_signal_outcome(tmp_path: Path) -> None:
    """update_signal_outcome populates outcome columns on an existing signal."""
    repo, account_id = _seeded_repo_with_account(tmp_path)
    try:
        now = datetime.utcnow()
        post_id = repo.insert_post(
            tenant_id=1, account_id=account_id, external_id="outcome_post",
            source_type="twitter", text="outcome test", posted_at=now, fetched_at=now,
        )
        sig_date = date(2026, 4, 21)
        signal_id = repo.insert_signal(
            tenant_id=1, user_id=1, post_id=post_id, account_id=account_id,
            signal_date=sig_date.isoformat(), ticker="FNMA",
            extraction_confidence="HIGH", direction="LONG", tier="ACT_NOW",
            conviction_score=8.2, final_score=8.2,
        )
        repo.update_signal_outcome(
            signal_id=signal_id,
            prev_close=3.82,
            today_open=3.90,
            today_close=4.15,
            overnight_return=0.086387,
            tradeable_return=0.064103,
            spy_return=0.0067,
            stock_20d_vol=0.0425,
            excess_vol_score=1.878282,
            price_data_source="yfinance",
            outcome_fetched_at=now,
        )
        rows = repo._execute(
            "SELECT overnight_return, excess_vol_score, price_data_source FROM signals WHERE id=?",
            [signal_id],
        )
        assert rows[0]["overnight_return"] == pytest.approx(0.086387, abs=1e-6)
        assert rows[0]["excess_vol_score"] == pytest.approx(1.878282, abs=1e-6)
        assert rows[0]["price_data_source"] == "yfinance"
    finally:
        repo.close()


def test_get_scoring_config_returns_dict(seeded_repo: SignalRepository) -> None:
    """get_scoring_config returns a {key: float} dict with at least 19 entries."""
    config = seeded_repo.get_scoring_config(tenant_id=1)
    assert isinstance(config, dict)
    assert len(config) >= 19


def test_log_message_sent(tmp_path: Path) -> None:
    """log_message_sent inserts a row into messages_sent."""
    repo = _make_repo(tmp_path)
    repo.seed(tenant_id=1)
    try:
        repo.log_message_sent(
            kind="morning", delivery="twilio", status="delivered",
            body_preview="FNMA LONG...", tenant_id=1,
        )
        rows = repo._execute("SELECT kind, status FROM messages_sent WHERE kind='morning'")
        assert len(rows) == 1
        assert rows[0]["status"] == "delivered"
    finally:
        repo.close()


def test_log_api_usage(tmp_path: Path) -> None:
    """log_api_usage inserts a row into api_usage."""
    repo = _make_repo(tmp_path)
    try:
        repo.log_api_usage(
            provider="anthropic", endpoint="messages",
            input_tokens=200, output_tokens=80,
            latency_ms=450, status="ok",
        )
        rows = repo._execute("SELECT provider, status FROM api_usage")
        assert len(rows) == 1
        assert rows[0]["provider"] == "anthropic"
    finally:
        repo.close()


def test_upsert_daily_summary(tmp_path: Path) -> None:
    """upsert_daily_summary is idempotent on (tenant_id, summary_date, run_type)."""
    repo = _make_repo(tmp_path)
    repo.seed(tenant_id=1)
    try:
        kwargs = dict(
            tenant_id=1,
            summary_date="2026-04-21",
            run_type="morning",
            pipeline_status="ok",
            accounts_fetched=3,
            signals_scored=5,
            signals_act_now=2,
            signals_watch=3,
        )
        repo.upsert_daily_summary(**kwargs)
        repo.upsert_daily_summary(**{**kwargs, "signals_scored": 7})  # idempotent upsert

        rows = repo._execute(
            "SELECT COUNT(*) AS cnt FROM daily_summaries WHERE summary_date='2026-04-21'"
        )
        assert rows[0]["cnt"] == 1, "upsert_daily_summary created duplicate rows"

        rows = repo._execute(
            "SELECT signals_scored FROM daily_summaries WHERE summary_date='2026-04-21'"
        )
        assert rows[0]["signals_scored"] == 7, "upsert did not update existing row"
    finally:
        repo.close()


def test_upsert_account(tmp_path: Path) -> None:
    """upsert_account inserts on first call and updates on second call."""
    repo = _make_repo(tmp_path)
    repo.seed(tenant_id=1)
    try:
        repo.upsert_account(
            tenant_id=1, handle="NewHandle",
            display_name="New Person", credibility_score=7.0, status="primary",
        )
        rows = repo._execute(
            "SELECT credibility_score FROM accounts WHERE handle='NewHandle'"
        )
        assert rows[0]["credibility_score"] == pytest.approx(7.0)

        repo.upsert_account(
            tenant_id=1, handle="NewHandle",
            display_name="New Person", credibility_score=8.5, status="primary",
        )
        rows = repo._execute(
            "SELECT credibility_score FROM accounts WHERE handle='NewHandle'"
        )
        assert rows[0]["credibility_score"] == pytest.approx(8.5), "upsert did not update"
    finally:
        repo.close()


# ---------------------------------------------------------------------------
# Backend fallback: no TURSO_URL → sqlite3 backend
# ---------------------------------------------------------------------------

def test_sqlite3_backend_used_when_no_turso_url(tmp_path: Path) -> None:
    """When TURSO_URL is empty, _Sqlite3Backend should be used."""
    settings = Settings(turso_url="", database_path=str(tmp_path / "fallback.db"))
    repo = SignalRepository(settings)
    try:
        assert isinstance(repo._backend, _Sqlite3Backend), (
            "Expected _Sqlite3Backend when TURSO_URL is unset"
        )
        assert repo._is_libsql is False
    finally:
        repo.close()


# ---------------------------------------------------------------------------
# Fixture file validation
# ---------------------------------------------------------------------------

def test_sample_signals_fixture_exists() -> None:
    """tests/fixtures/sample_signals.json must exist."""
    path = FIXTURES_DIR / "sample_signals.json"
    assert path.exists(), f"sample_signals.json not found at {path}"


def test_sample_outcomes_fixture_exists() -> None:
    """tests/fixtures/sample_outcomes.json must exist."""
    path = FIXTURES_DIR / "sample_outcomes.json"
    assert path.exists(), f"sample_outcomes.json not found at {path}"


def test_sample_signals_has_11_records() -> None:
    """sample_signals.json must have exactly 11 records (TSLA conflict scenario requires 2 rows)."""
    data = json.loads((FIXTURES_DIR / "sample_signals.json").read_text())
    assert len(data) == 11, f"Expected 11 signals, got {len(data)}"


def test_sample_outcomes_has_11_records() -> None:
    """sample_outcomes.json must have exactly 11 records (TSLA conflict scenario requires 2 rows)."""
    data = json.loads((FIXTURES_DIR / "sample_outcomes.json").read_text())
    assert len(data) == 11, f"Expected 11 outcome records, got {len(data)}"


REQUIRED_SIGNAL_FIELDS = [
    "ticker", "direction", "conviction_level", "extraction_confidence",
    "tier", "final_score", "direction_flip", "conflict_group",
    "account_handle", "signal_date",
]


@pytest.mark.parametrize("field", REQUIRED_SIGNAL_FIELDS)
def test_sample_signals_all_records_have_required_fields(field: str) -> None:
    """Every record in sample_signals.json must have the required field."""
    data = json.loads((FIXTURES_DIR / "sample_signals.json").read_text())
    for i, rec in enumerate(data):
        assert field in rec, f"sample_signals[{i}] missing field '{field}'"


def test_sample_signals_covers_act_now_tier() -> None:
    """sample_signals.json must include at least one ACT_NOW signal."""
    data = json.loads((FIXTURES_DIR / "sample_signals.json").read_text())
    tiers = [r["tier"] for r in data]
    assert "ACT_NOW" in tiers, "No ACT_NOW signal in sample_signals.json"


def test_sample_signals_covers_watch_tier() -> None:
    """sample_signals.json must include at least one WATCH signal."""
    data = json.loads((FIXTURES_DIR / "sample_signals.json").read_text())
    tiers = [r["tier"] for r in data]
    assert "WATCH" in tiers, "No WATCH signal in sample_signals.json"


def test_sample_signals_covers_direction_flip() -> None:
    """sample_signals.json must include at least one signal with direction_flip=True."""
    data = json.loads((FIXTURES_DIR / "sample_signals.json").read_text())
    flips = [r for r in data if r.get("direction_flip") is True]
    assert len(flips) >= 1, "No direction_flip signal in sample_signals.json"


def test_sample_signals_covers_opposing_conflict() -> None:
    """sample_signals.json must include at least one signal with conflict_group='opposing_exists'."""
    data = json.loads((FIXTURES_DIR / "sample_signals.json").read_text())
    conflicts = [r for r in data if r.get("conflict_group") == "opposing_exists"]
    assert len(conflicts) >= 1, "No opposing_exists conflict signal in sample_signals.json"


def test_sample_signals_covers_unscored() -> None:
    """sample_signals.json must include at least one UNSCORED signal."""
    data = json.loads((FIXTURES_DIR / "sample_signals.json").read_text())
    unscored = [r for r in data if r.get("tier") == "UNSCORED"]
    assert len(unscored) >= 1, "No UNSCORED signal in sample_signals.json"


REQUIRED_OUTCOME_FIELDS = [
    "ticker", "direction", "tier",
    "overnight_return", "tradeable_return", "spy_return",
    "stock_20d_vol", "excess_vol_score", "price_data_source",
]


@pytest.mark.parametrize("field", REQUIRED_OUTCOME_FIELDS)
def test_sample_outcomes_all_records_have_required_fields(field: str) -> None:
    """Every record in sample_outcomes.json must have the outcome field (even if null)."""
    data = json.loads((FIXTURES_DIR / "sample_outcomes.json").read_text())
    for i, rec in enumerate(data):
        assert field in rec, f"sample_outcomes[{i}] missing field '{field}'"


def test_sample_outcomes_scored_signals_have_outcome_data() -> None:
    """ACT_NOW and WATCH signals in sample_outcomes.json must have non-null overnight_return."""
    data = json.loads((FIXTURES_DIR / "sample_outcomes.json").read_text())
    for rec in data:
        if rec["tier"] in ("ACT_NOW", "WATCH") and rec.get("price_data_source") != "unavailable":
            assert rec["overnight_return"] is not None, (
                f"ACT_NOW/WATCH signal for {rec['ticker']} missing overnight_return"
            )
            assert rec["excess_vol_score"] is not None, (
                f"ACT_NOW/WATCH signal for {rec['ticker']} missing excess_vol_score"
            )


def test_sample_outcomes_covers_price_data_unavailable() -> None:
    """sample_outcomes.json must include at least one record with price_data_source='unavailable'."""
    data = json.loads((FIXTURES_DIR / "sample_outcomes.json").read_text())
    unavailable = [r for r in data if r.get("price_data_source") == "unavailable"]
    assert len(unavailable) >= 1, "No unavailable price_data_source scenario in sample_outcomes.json"


def test_sample_outcomes_short_signal_has_negative_excess_vol() -> None:
    """SHORT signal where stock went up should have negative excess_vol_score."""
    data = json.loads((FIXTURES_DIR / "sample_outcomes.json").read_text())
    # NFLX SHORT: stock went up (positive overnight_return), excess_vol_score should be negative
    nflx = next((r for r in data if r["ticker"] == "NFLX" and r["direction"] == "SHORT"), None)
    assert nflx is not None, "No NFLX SHORT signal in sample_outcomes.json"
    if nflx.get("overnight_return") is not None and nflx["overnight_return"] > 0:
        assert nflx["excess_vol_score"] < 0, (
            f"SHORT signal where stock went up should have negative excess_vol_score, "
            f"got {nflx['excess_vol_score']}"
        )


# ---------------------------------------------------------------------------
# Config seed files
# ---------------------------------------------------------------------------

def test_accounts_json_exists() -> None:
    """config/accounts.json must exist."""
    path = CONFIG_DIR / "accounts.json"
    assert path.exists(), f"accounts.json not found at {path}"


def test_accounts_json_has_45_entries() -> None:
    """config/accounts.json must have exactly 45 entries."""
    data = json.loads((CONFIG_DIR / "accounts.json").read_text())
    assert len(data) == 45, f"Expected 45 accounts, got {len(data)}"


def test_scoring_config_seed_exists() -> None:
    """config/scoring_config_seed.json must exist."""
    path = CONFIG_DIR / "scoring_config_seed.json"
    assert path.exists(), f"scoring_config_seed.json not found at {path}"


def test_scoring_config_seed_has_19_rows() -> None:
    """config/scoring_config_seed.json must have exactly 19 rows."""
    data = json.loads((CONFIG_DIR / "scoring_config_seed.json").read_text())
    assert len(data) == 19, f"Expected 19 scoring_config rows, got {len(data)}"


def test_scoring_config_seed_all_have_key_and_value() -> None:
    """Every row in scoring_config_seed.json must have 'key' and 'value' fields."""
    data = json.loads((CONFIG_DIR / "scoring_config_seed.json").read_text())
    for i, row in enumerate(data):
        assert "key" in row, f"scoring_config_seed.json row {i} missing 'key'"
        assert "value" in row, f"scoring_config_seed.json row {i} missing 'value'"
