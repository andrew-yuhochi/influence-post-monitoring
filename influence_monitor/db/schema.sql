-- Influence Post Monitoring — Database Schema
-- All tables carry tenant_id where applicable (multi-tenancy from day one).
-- SQLite PoC; PostgreSQL-compatible by design.

-- Tenant registry (multi-tenancy foundation)
CREATE TABLE IF NOT EXISTS tenants (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Curated investor/source profiles
CREATE TABLE IF NOT EXISTS investor_profiles (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id               INTEGER NOT NULL DEFAULT 1,
    name                    TEXT NOT NULL,
    x_handle                TEXT,
    x_numeric_id            TEXT,
    source_type             TEXT NOT NULL DEFAULT 'twitter',
    investor_type           TEXT NOT NULL,
    credibility_score       REAL NOT NULL DEFAULT 5.0,
    rolling_accuracy_30d    REAL,
    total_calls             INTEGER NOT NULL DEFAULT 0,
    total_hits              INTEGER NOT NULL DEFAULT 0,
    is_active               BOOLEAN NOT NULL DEFAULT 1,
    last_fetch_status       TEXT,
    notes                   TEXT,
    created_at              DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_investor_handle ON investor_profiles(tenant_id, x_handle);

-- Raw posts fetched from social media (stored at fetch time, immutable)
CREATE TABLE IF NOT EXISTS posts (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id               INTEGER NOT NULL DEFAULT 1,
    investor_id             INTEGER NOT NULL,
    external_id             TEXT NOT NULL UNIQUE,
    source_type             TEXT NOT NULL,
    text                    TEXT NOT NULL,
    posted_at               DATETIME NOT NULL,
    fetched_at              DATETIME NOT NULL,
    -- Engagement metrics (snapshot at fetch time)
    view_count              INTEGER,
    repost_count            INTEGER,
    reply_count             INTEGER,
    like_count              INTEGER,
    bookmark_count          INTEGER,
    -- Social context (ML features)
    quote_tweet_id          TEXT,
    is_thread               BOOLEAN NOT NULL DEFAULT 0,
    thread_position         INTEGER,
    hashtags                TEXT,
    mentioned_users         TEXT,
    url_links               TEXT,
    media_type              TEXT,
    language                TEXT DEFAULT 'en',
    -- Poster reach at time of post
    follower_count_at_post  INTEGER,
    following_count_at_post INTEGER,
    -- Deletion tracking
    deleted                 BOOLEAN NOT NULL DEFAULT 0,
    raw_payload             TEXT,
    FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    FOREIGN KEY (investor_id) REFERENCES investor_profiles(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_posts_external ON posts(source_type, external_id);

-- Signals: one row per (post, ticker) pair
CREATE TABLE IF NOT EXISTS signals (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id               INTEGER NOT NULL DEFAULT 1,
    post_id                 INTEGER NOT NULL,
    investor_id             INTEGER NOT NULL,
    ticker                  TEXT NOT NULL,
    extraction_confidence   TEXT NOT NULL,
    extraction_method       TEXT,
    direction               TEXT NOT NULL,
    conviction_level        INTEGER,
    argument_quality        TEXT,
    time_horizon            TEXT,
    market_moving_potential  BOOLEAN,
    key_claim               TEXT,
    rationale               TEXT,
    -- Composite score and sub-components
    score_credibility       REAL,
    score_conviction        REAL,
    score_argument          REAL,
    score_engagement        REAL,
    score_historical        REAL,
    composite_score         REAL,
    corroboration_count     INTEGER NOT NULL DEFAULT 1,
    corroboration_bonus     REAL NOT NULL DEFAULT 1.0,
    final_score             REAL,
    -- Morning email rank
    morning_rank            INTEGER,
    -- Market data (filled in multiple passes)
    signal_date             DATE NOT NULL,
    prev_close_price        REAL,
    open_price              REAL,
    close_price             REAL,
    high_price              REAL,
    low_price               REAL,
    return_pct              REAL,
    is_hit                  BOOLEAN,
    -- Return windows for multi-horizon analysis
    return_5d               REAL,
    return_10d              REAL,
    return_30d              REAL,
    -- Volume context
    volume                  INTEGER,
    avg_volume_30d          INTEGER,
    volume_ratio            REAL,
    -- Stock context at time of signal
    index_tier              TEXT,
    market_cap_at_signal    REAL,
    sector                  TEXT,
    industry                TEXT,
    -- Market regime context
    sp500_return_pct        REAL,
    vix_at_signal           REAL,
    sector_return_pct       REAL,
    -- LLM metadata
    llm_model_version       TEXT,
    llm_raw_response        TEXT,
    llm_input_tokens        INTEGER,
    llm_output_tokens       INTEGER,
    created_at              DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    FOREIGN KEY (post_id) REFERENCES posts(id),
    FOREIGN KEY (investor_id) REFERENCES investor_profiles(id)
);
CREATE INDEX IF NOT EXISTS idx_signals_date ON signals(signal_date, tenant_id);
CREATE INDEX IF NOT EXISTS idx_signals_ticker ON signals(ticker, signal_date);

-- Engagement snapshots: track how post engagement grows over time
CREATE TABLE IF NOT EXISTS engagement_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id         INTEGER NOT NULL,
    snapshot_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    view_count      INTEGER,
    repost_count    INTEGER,
    reply_count     INTEGER,
    like_count      INTEGER,
    bookmark_count  INTEGER,
    FOREIGN KEY (post_id) REFERENCES posts(id)
);
CREATE INDEX IF NOT EXISTS idx_engagement_post ON engagement_snapshots(post_id, snapshot_at);

-- Index membership cache (refreshed weekly)
CREATE TABLE IF NOT EXISTS index_membership (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL UNIQUE,
    index_tier      TEXT NOT NULL,
    market_cap_b    REAL,
    last_updated    DATE NOT NULL
);

-- Scoring model weights (configurable without redeployment)
CREATE TABLE IF NOT EXISTS scoring_weights (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    component       TEXT NOT NULL UNIQUE,
    weight          REAL NOT NULL,
    description     TEXT,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Daily pipeline run summaries
CREATE TABLE IF NOT EXISTS daily_summaries (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id               INTEGER NOT NULL DEFAULT 1,
    summary_date            DATE NOT NULL,
    run_type                TEXT NOT NULL,
    accounts_fetched        INTEGER,
    posts_fetched           INTEGER,
    signals_scored          INTEGER,
    signals_surfaced        INTEGER,
    corroborated_signals    INTEGER,
    daily_hit_rate          REAL,
    pipeline_status         TEXT NOT NULL,
    error_message           TEXT,
    run_duration_seconds    REAL,
    created_at              DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- API usage tracking for cost monitoring
CREATE TABLE IF NOT EXISTS api_usage (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    called_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    provider        TEXT NOT NULL,
    endpoint        TEXT,
    input_tokens    INTEGER,
    output_tokens   INTEGER,
    latency_ms      INTEGER,
    status          TEXT,
    error_message   TEXT
);
