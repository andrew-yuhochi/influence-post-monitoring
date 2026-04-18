# Influence Post Monitoring

Automated pipeline to surface influential investor social media posts as actionable trading signals, delivered via morning watchlist and evening scorecard emails.

> Built with [Claude Code](https://claude.ai/code)

**Phase**: PoC (personal use)

---

## What it does

The system monitors 17 curated fund manager and investor accounts on X/Twitter throughout each trading day. Before market open it sends an email ranking the overnight posts most likely to move equity prices. After close it scores how each signal performed and emails a scorecard.

The core loop:

1. **Ingest** — twikit fetches tweets from overnight (approx. yesterday 4 PM ET to today 6:30 AM ET) using cookie-based scraping
2. **Extract** — a three-layer NLP pipeline pulls equity tickers from post text: `$CASHTAG` regex (HIGH confidence), standalone uppercase regex (MEDIUM), and spaCy NER with a Yahoo Finance name resolver (LOW)
3. **Filter** — tickers are validated against a whitelist of S&P 500 + Nasdaq-100 members
4. **Score** — Claude Haiku assigns a direction (LONG / SHORT / NEUTRAL), conviction (1–5), argument quality (HIGH / MEDIUM / LOW), and time horizon per post
5. **Rank** — a five-component composite score combines investor credibility, conviction, argument quality, engagement, and 30-day directional accuracy; corroboration (≥2 investors posting the same ticker) adds a 1.5× bonus
6. **Email** — morning watchlist (top 10 signals) sent via Resend; evening scorecard compares open vs. close price for each signal and tracks the running track record

Signals and pipeline runs are stored in a SQLite database at `data/signals.db`. GitHub Actions automate the daily schedule.

---

## Project structure

```
influence-post-monitoring/
├── influence_monitor/
│   ├── ingestion/          # twikit Twitter ingestor + base interface
│   ├── extraction/         # Three-layer ticker extractor + S&P/NDX whitelist
│   ├── scoring/            # Claude Haiku LLM client + five-component scoring engine
│   ├── scorecard/          # Evening return calculation and scorecard engine
│   ├── market_data/        # yfinance + Alpha Vantage OHLCV clients
│   ├── email/              # Resend provider + HTML/text email renderers
│   ├── db/                 # Async SQLite repository (aiosqlite, raw SQL)
│   ├── calendar.py         # NYSE trading-day and holiday calendar
│   ├── config.py           # pydantic-settings: all settings loaded from .env
│   └── pipeline.py         # CLI entry point: morning / evening / auth
├── scripts/
│   ├── import_cookies.py   # Convert Cookie-Editor JSON export to twikit format
│   └── patch_twikit.py     # Patch twikit 2.3.3 for X API compatibility
├── tests/
│   ├── test_scorecard_engine.py
│   ├── test_pipeline.py
│   └── test_integration.py
├── config/
│   ├── investors_seed.json          # 17 monitored accounts with credibility scores
│   ├── scoring_weights_seed.json    # Default composite score weights
│   └── prompts/scoring_prompt.txt   # Claude Haiku system prompt
├── .github/workflows/
│   ├── morning_pipeline.yml   # Runs at 12:00 UTC (7 AM EST / 8 AM EDT), weekdays
│   └── evening_pipeline.yml   # Runs at 22:30 UTC (5:30 PM EST / 6:30 PM EDT), weekdays
├── data/                    # gitignored — signals.db, twitter_cookies.json
├── .env.example
└── requirements.txt
```

---

## Setup

### Prerequisites

- Python 3.11+
- An X/Twitter account (for cookie export or credential-based login)
- Anthropic API key — [console.anthropic.com](https://console.anthropic.com)
- Resend API key — [resend.com](https://resend.com) (free tier is sufficient)

### 1. Clone and install

```bash
git clone https://github.com/andrew-yuhochi/influence-post-monitoring.git
cd influence-post-monitoring

python3.11 -m venv venv
source venv/bin/activate       # Windows: venv\Scripts\activate

pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

### 2. Patch twikit

X changed their anti-bot JavaScript after twikit 2.3.3 was released. Run this once after install — it applies two patches to the installed package:

```bash
python scripts/patch_twikit.py
```

Expected output:
```
Found twikit at: /path/to/venv/lib/python3.11/site-packages/twikit
Patch 1 applied: KEY_BYTE indices fallback
Patch 2 applied: 3/3 User field(s) made optional
Done — twikit patches applied.
```

If you upgrade twikit later, re-run this script.

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your values. See the [Configuration](#configuration) section for a full reference.

### 4. Authenticate with X/Twitter

**Option A — Cookie export (recommended, more reliable)**

1. Log in to X in your browser
2. Install the [Cookie-Editor](https://cookie-editor.com) extension
3. On x.com, open Cookie-Editor and export cookies as JSON
4. Run:

```bash
python scripts/import_cookies.py /path/to/cookies_export.json
```

Expected output:
```
Wrote 32 cookies to data/twitter_cookies.json
Key cookies present: ['ct0', 'auth_token']
OK — ready to run the pipeline
```

**Option B — Credential login (less reliable)**

Set `TWITTER_USERNAME`, `TWITTER_EMAIL`, and `TWITTER_PASSWORD` in `.env`, then run:

```bash
python -m influence_monitor.pipeline auth
```

This triggers twikit's login flow and saves cookies to `data/twitter_cookies.json`. X's login flow is fragile and may require solving a CAPTCHA manually.

### 5. Initialize the database

```bash
python -m influence_monitor.db.repository --init
```

This creates `data/signals.db`, applies the full schema, and seeds the 17 investor profiles and default scoring weights.

### 6. Test with a dry run

Before the first real run, verify the setup end-to-end:

```bash
# Renders the morning watchlist to stdout — no DB writes, no email sent
python -m influence_monitor.pipeline morning --dry-run

# Renders the evening scorecard to stdout
python -m influence_monitor.pipeline evening --dry-run
```

---

## Running the pipeline

```bash
# Morning watchlist — run around 9 AM ET on a weekday
python -m influence_monitor.pipeline morning

# Evening scorecard — run after 4 PM ET on the same weekday
python -m influence_monitor.pipeline evening
```

The pipeline skips automatically on weekends and NYSE holidays. A failure email is sent to `RECIPIENT_EMAIL` if the pipeline errors — no partial watchlist is ever delivered.

---

## Configuration

All settings are read from `.env` via pydantic-settings. Copy `.env.example` to `.env` to get started.

### Required

| Variable | Description |
|---|---|
| `ANTHROPIC_API_KEY` | Claude API key for LLM scoring. Get one at [console.anthropic.com](https://console.anthropic.com). |
| `RESEND_API_KEY` | Resend API key for email delivery. Get one at [resend.com](https://resend.com). |
| `RECIPIENT_EMAIL` | Email address that receives the morning watchlist and evening scorecard. |
| `TWITTER_USERNAME` | X account username. Required for Option B (credential login). |
| `TWITTER_EMAIL` | X account email. Required for Option B. |
| `TWITTER_PASSWORD` | X account password. Required for Option B. |

### Optional (with defaults)

| Variable | Default | Description |
|---|---|---|
| `SENDER_EMAIL` | `Influence Monitor <onboarding@resend.dev>` | From address. On Resend's free tier, only `onboarding@resend.dev` works without a verified domain. |
| `TWITTER_SOURCE` | `twitter_twikit` | Ingestor plugin key. Only `twitter_twikit` is implemented. |
| `EMAIL_PROVIDER` | `resend` | Email provider plugin key. Only `resend` is implemented. |
| `DATABASE_PATH` | `data/signals.db` | Path to the SQLite database file. |
| `COOKIES_PATH` | `data/twitter_cookies.json` | Path to the twikit cookie file. |
| `MIN_ACCOUNTS_THRESHOLD` | `13` | Minimum number of accounts that must respond before the pipeline is considered healthy. |
| `SIGNAL_MIN_SCORE` | `2.0` | Composite score floor (0–10 scale) — signals below this are dropped before ranking. |
| `TOP_N_SIGNALS` | `10` | Maximum signals in each morning watchlist email. |
| `CONVICTION_MIN` | `2` | Minimum Claude-assigned conviction level (1–5). Posts below this threshold are dropped. |
| `CORROBORATION_MULTIPLIER` | `1.5` | Score multiplier applied when ≥2 investors post about the same ticker. |
| `TRACK_RECORD_MIN_CALLS` | `5` | Minimum signals required before an investor's track record is shown in the scorecard. |
| `LOG_LEVEL` | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |
| `TIMEZONE` | `America/New_York` | Timezone for display and scheduling logic. |
| `ALPHA_VANTAGE_API_KEY` | _(empty)_ | Optional. Alpha Vantage is a fallback market data source; yfinance is the primary. |

---

## Scoring model

Each (post, ticker) pair receives a composite score on a 0–10 scale built from five weighted components:

| Component | Default weight | Source |
|---|---|---|
| Credibility | 30% | Investor's `credibility_score` in `investors_seed.json` (1–10) |
| Conviction | 25% | Claude's `conviction_level` output (1–5) |
| Argument quality | 20% | Claude's `argument_quality` output (HIGH / MEDIUM / LOW) |
| Engagement | 15% | `(views + 5 × reposts) / 30-day max`, clamped to [0, 1] |
| Historical accuracy | 10% | Investor's `rolling_accuracy_30d` (updated nightly by scorecard engine) |

Weights are stored in the database (`scoring_weights` table) and can be tuned without code changes by editing `config/scoring_weights_seed.json` and re-running `--init`, or by updating the DB rows directly.

**Corroboration**: when ≥2 monitored investors post about the same ticker on the same day, the composite scores of all matching signals are multiplied by `CORROBORATION_MULTIPLIER` (default 1.5×).

**Gates**: signals with `conviction_level < CONVICTION_MIN` or direction `NEUTRAL`/`AMBIGUOUS` are dropped before scoring.

---

## Investors monitored

17 accounts across activist, growth, macro, value, short-seller, quant, and data categories. Configured in `config/investors_seed.json` — add or remove entries there to change the monitored set.

| Handle | Name | Type | Credibility |
|---|---|---|---|
| BillAckman | Bill Ackman | Activist | 8.5 |
| chamath | Chamath Palihapitiya | Growth | 7.0 |
| michaeljburry | Michael Burry | Contrarian | 9.0 |
| Nouriel | Nouriel Roubini | Macro | 7.5 |
| CathieDWood | Cathie Wood | Growth | 6.5 |
| elerianm | Mohamed El-Erian | Macro | 7.5 |
| TruthGundlach | Jeff Gundlach | Macro | 8.0 |
| DanIvesWedbush | Dan Ives | Analyst | 7.0 |
| CitronResearch | Citron Research | Short seller | 7.0 |
| HowardMarksBook | Howard Marks | Value | 8.5 |
| AswathDamodaran | Aswath Damodaran | Valuation | 8.0 |
| AQRCliff | Cliff Asness | Quant | 8.0 |
| MuddyWatersRsrch | Carson Block | Short seller | 8.5 |
| WallStCynic | Jim Chanos | Short seller | 7.5 |
| Carl_C_Icahn | Carl Icahn | Activist | 8.0 |
| whitneytilson | Whitney Tilson | Value | 6.5 |
| QuiverQuant | Quiver Quantitative | Data | 6.0 |

---

## Automation with GitHub Actions

Two workflows handle the daily schedule. The database is persisted between runs as a GitHub Actions artifact (90-day retention). Each run downloads the latest artifact before executing and re-uploads it after.

| Workflow | Cron (UTC) | ET equivalent | Command |
|---|---|---|---|
| Morning Pipeline | `0 12 * * 1-5` | 7:00 AM EST / 8:00 AM EDT | `morning` |
| Evening Pipeline | `30 22 * * 1-5` | 5:30 PM EST / 6:30 PM EDT | `evening` |

Both workflows support `workflow_dispatch` for manual triggering from the GitHub Actions UI.

### Required GitHub secrets

Go to **Settings → Secrets and variables → Actions** in your fork and add:

| Secret | Description |
|---|---|
| `ANTHROPIC_API_KEY` | Claude API key |
| `RESEND_API_KEY` | Resend API key |
| `RECIPIENT_EMAIL` | Where to deliver emails |
| `TWITTER_USERNAME` | X account username (used by Option B login) |
| `TWITTER_EMAIL` | X account email (used by Option B login) |
| `TWITTER_PASSWORD` | X account password (used by Option B login) |

If you authenticate via cookie export (Option A), you will still need to provide placeholder values for the `TWITTER_*` secrets — the workflow passes them as env vars even when cookies are used.

> **Longer retention**: GitHub artifact retention maxes out at 90 days. For indefinite persistence, swap `DATABASE_PATH` for a [Turso](https://turso.tech) `libsql://` URL (free tier: 8 GB, 1B row reads/month).

---

## Running tests

```bash
pytest tests/ -v
```

The test suite covers the scorecard engine, pipeline orchestrator, and a full integration path. pytest-asyncio is required for async test support and is included in `requirements.txt`.

---

## Known limitations

**twikit compatibility**: twikit 2.3.3 requires the patch script after every fresh install. If X changes their anti-bot JavaScript again, the patch target may not match and `patch_twikit.py` will print a warning — the fix requires updating the patch targets in the script manually.

**UserNotFound on some accounts**: If a monitored investor has deleted or renamed their X account since the seed data was written, twikit returns a `UserNotFound` error for that handle. The pipeline logs a warning and continues with the remaining accounts.

**EST/EDT DST handling**: The overnight fetch window uses a fixed UTC offset (`-4h`) rather than proper DST-aware conversion. During EDT (summer), the window shifts by one hour. This is a known PoC simplification.

**Engagement data gaps**: X does not always return view counts. When `view_count` is NULL, the engagement sub-score falls back to the investor's median engagement or a neutral 0.5. This reduces scoring precision for accounts that suppress analytics.

**GitHub artifact expiry**: Signals older than 90 days will be lost when the artifact expires unless you migrate to an external database before that point.

**X Terms of Service**: twikit uses cookie-based scraping which violates X's ToS. This tool is built for personal PoC use only and should not be run at commercial scale.

---

## License

Private — not licensed for redistribution.
