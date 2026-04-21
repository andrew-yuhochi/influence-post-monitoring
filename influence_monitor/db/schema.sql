-- Influence Post Monitoring — Database Schema (TDD §2.4)
-- All row-producing tables carry tenant_id + user_id (multi-tenancy from day one).
-- Pure SQLite/Turso dialect. Types used:
--   INTEGER PRIMARY KEY  — rowid alias (auto-increment)
--   INTEGER              — int values and booleans (0/1)
--   REAL                 — floating-point
--   TEXT                 — strings, ISO8601 dates/timestamps, JSON
-- BOOLEAN, DATE, DATETIME are replaced with INTEGER/TEXT to avoid
-- PostgreSQL-dialect parse errors in Turso's sqld.
-- PoC: all tenant_id / user_id default to 1.

-- Tenant registry
CREATE TABLE IF NOT EXISTS tenants (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- User registry (single user at PoC — ready for multi-tenant Beta)
CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY,
    user_id     INTEGER NOT NULL DEFAULT 1,
    tenant_id   INTEGER NOT NULL DEFAULT 1,
    phone_e164  TEXT NOT NULL,
    timezone    TEXT NOT NULL DEFAULT 'America/Toronto',
    created_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);

-- Curated investor accounts (30 primaries + 15 backups)
CREATE TABLE IF NOT EXISTS accounts (
    id                      INTEGER PRIMARY KEY,
    user_id                 INTEGER NOT NULL DEFAULT 1,
    tenant_id               INTEGER NOT NULL DEFAULT 1,
    handle                  TEXT NOT NULL,
    external_id             TEXT,
    display_name            TEXT,
    angle                   TEXT,
    credibility_score       REAL NOT NULL DEFAULT 5.0,
    status                  TEXT NOT NULL DEFAULT 'primary',
    backup_rank             INTEGER,
    last_fetch_status       TEXT,
    consecutive_failures    INTEGER NOT NULL DEFAULT 0,
    last_validated_at       TEXT,
    last_failure_at         TEXT,
    follower_count_at_post  INTEGER,
    notes                   TEXT,
    created_at              TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    UNIQUE (tenant_id, handle)
);
CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status, tenant_id);

-- Raw posts (immutable after first fetch — Burry-deletion-resistant)
CREATE TABLE IF NOT EXISTS posts (
    id            INTEGER PRIMARY KEY,
    user_id       INTEGER NOT NULL DEFAULT 1,
    tenant_id     INTEGER NOT NULL DEFAULT 1,
    account_id    INTEGER NOT NULL,
    external_id   TEXT NOT NULL,
    source_type   TEXT NOT NULL,
    text          TEXT NOT NULL,
    posted_at     TEXT NOT NULL,
    fetched_at    TEXT NOT NULL,
    view_count    INTEGER,
    repost_count  INTEGER,
    reply_count   INTEGER,
    like_count    INTEGER,
    bookmark_count INTEGER,
    deleted       INTEGER DEFAULT 0,
    raw_payload   TEXT,
    FOREIGN KEY (account_id) REFERENCES accounts(id),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    UNIQUE (source_type, external_id)
);

-- Engagement snapshots for velocity computation + MVP regression
CREATE TABLE IF NOT EXISTS engagement_snapshots (
    id           INTEGER PRIMARY KEY,
    post_id      INTEGER NOT NULL,
    snapshot_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    view_count   INTEGER,
    repost_count INTEGER,
    reply_count  INTEGER,
    like_count   INTEGER,
    FOREIGN KEY (post_id) REFERENCES posts(id)
);
CREATE INDEX IF NOT EXISTS idx_engagement_post ON engagement_snapshots(post_id, snapshot_at);

-- Retweeters (amplifier regression dataset)
CREATE TABLE IF NOT EXISTS retweeters (
    id                    INTEGER PRIMARY KEY,
    post_id               INTEGER NOT NULL,
    retweeter_external_id TEXT NOT NULL,
    retweeter_handle      TEXT,
    followers_count       INTEGER,
    is_verified           INTEGER,
    is_monitored          INTEGER DEFAULT 0,
    fetched_at            TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (post_id) REFERENCES posts(id),
    UNIQUE (post_id, retweeter_external_id)
);
CREATE INDEX IF NOT EXISTS idx_retweeters_post ON retweeters(post_id);

-- Market-cap cache (weekly refresh via finvizfinance)
CREATE TABLE IF NOT EXISTS price_cache (
    id               INTEGER PRIMARY KEY,
    ticker           TEXT NOT NULL UNIQUE,
    market_cap_b     REAL,
    market_cap_class TEXT,
    sector           TEXT,
    industry         TEXT,
    last_updated     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- Scoring config (factor weights + thresholds + penalties — DB-driven, not hardcoded)
CREATE TABLE IF NOT EXISTS scoring_config (
    id          INTEGER PRIMARY KEY,
    tenant_id   INTEGER NOT NULL DEFAULT 1,
    key         TEXT NOT NULL,
    value       REAL NOT NULL,
    description TEXT,
    updated_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE (tenant_id, key)
);
-- Seeded rows (17 total):
--   weight_credibility, weight_virality_abs, weight_virality_vel,
--   weight_consensus, weight_amplifier,
--   virality_views_threshold, virality_reposts_threshold,
--   watch_velocity_floor, direction_flip_penalty, vol_lookback_days,
--   max_consecutive_failures, retry_rest_minutes,
--   liq_mega, liq_large, liq_mid, liq_small, liq_micro

-- SIGNALS — the commercial asset
CREATE TABLE IF NOT EXISTS signals (
    id                      INTEGER PRIMARY KEY,
    user_id                 INTEGER NOT NULL DEFAULT 1,
    tenant_id               INTEGER NOT NULL DEFAULT 1,
    -- Provenance
    post_id                 INTEGER NOT NULL,
    account_id              INTEGER NOT NULL,
    signal_date             TEXT NOT NULL,
    -- Ticker
    ticker                  TEXT NOT NULL,
    extraction_confidence   TEXT NOT NULL,
    market_cap_class        TEXT,
    -- LLM scoring
    direction               TEXT NOT NULL,
    conviction_level        INTEGER,
    argument_quality        TEXT,
    time_horizon            TEXT,
    market_moving_potential INTEGER,
    key_claim               TEXT,
    rationale               TEXT,
    llm_model_version       TEXT,
    llm_raw_response        TEXT,
    llm_input_tokens        INTEGER,
    llm_output_tokens       INTEGER,
    -- Five factor scores
    score_credibility       REAL,
    score_virality_abs      REAL,
    score_virality_vel      REAL,
    score_consensus         REAL,
    score_amplifier         REAL,
    liquidity_modifier      REAL,
    conviction_score        REAL,
    direction_flip          INTEGER DEFAULT 0,
    conflict_group          TEXT,
    penalty_applied         REAL DEFAULT 0,
    final_score             REAL,
    tier                    TEXT NOT NULL,
    shown_in_morning_alert  INTEGER NOT NULL DEFAULT 0,
    morning_rank            INTEGER,
    -- Outcome (evening pass — NULL until then)
    prev_close              REAL,
    today_open              REAL,
    today_close             REAL,
    overnight_return        REAL,
    tradeable_return        REAL,
    spy_return              REAL,
    stock_20d_vol           REAL,
    excess_vol_score        REAL,
    price_data_source       TEXT,
    outcome_fetched_at      TEXT,
    -- Engagement views
    engagement_views        INTEGER,
    engagement_reposts      INTEGER,
    views_per_hour          REAL,
    -- Metadata
    created_at              TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (post_id) REFERENCES posts(id),
    FOREIGN KEY (account_id) REFERENCES accounts(id),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
CREATE INDEX IF NOT EXISTS idx_signals_date ON signals(signal_date, tenant_id);
CREATE INDEX IF NOT EXISTS idx_signals_ticker ON signals(ticker, signal_date);
CREATE INDEX IF NOT EXISTS idx_signals_account ON signals(account_id, signal_date);
CREATE INDEX IF NOT EXISTS idx_signals_shown ON signals(signal_date, tenant_id, shown_in_morning_alert);

-- Message delivery log
CREATE TABLE IF NOT EXISTS messages_sent (
    id           INTEGER PRIMARY KEY,
    user_id      INTEGER NOT NULL DEFAULT 1,
    tenant_id    INTEGER NOT NULL DEFAULT 1,
    kind         TEXT NOT NULL,
    sent_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    delivery     TEXT NOT NULL,
    status       TEXT NOT NULL,
    body_preview TEXT,
    provider_id  TEXT,
    error_message TEXT
);

-- Daily pipeline summaries (for ops)
CREATE TABLE IF NOT EXISTS daily_summaries (
    id               INTEGER PRIMARY KEY,
    tenant_id        INTEGER NOT NULL DEFAULT 1,
    summary_date     TEXT NOT NULL,
    run_type         TEXT NOT NULL,
    accounts_active  INTEGER,
    accounts_fetched INTEGER,
    posts_fetched    INTEGER,
    signals_scored   INTEGER,
    signals_act_now  INTEGER,
    signals_watch    INTEGER,
    avg_excess_vol   REAL,
    pipeline_status  TEXT NOT NULL,
    error_message    TEXT,
    duration_seconds REAL,
    created_at       TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE (tenant_id, summary_date, run_type)
);

-- API usage tracking (cost + rate-limit monitoring)
CREATE TABLE IF NOT EXISTS api_usage (
    id            INTEGER PRIMARY KEY,
    called_at     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    provider      TEXT NOT NULL,
    endpoint      TEXT,
    input_tokens  INTEGER,
    output_tokens INTEGER,
    latency_ms    INTEGER,
    status        TEXT,
    error_message TEXT
);
