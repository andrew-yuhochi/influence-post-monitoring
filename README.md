# Influence Post Monitoring

> Built with [Claude Code](https://claude.ai/code)

Automated pipeline that monitors curated fund manager and investor accounts on X/Twitter, extracts equity signals, scores them through a five-factor conviction model, and delivers a morning WhatsApp alert plus an evening outcome summary.

**Phase**: PoC (personal use)

---

## What it does

The system monitors 30 primary investor accounts on X/Twitter throughout each trading day. Before market open it sends a WhatsApp message ranking the overnight posts most likely to move equity prices (Act Now + Watch List). After close it scores how each signal performed and sends an evening WhatsApp summary with returns and a 30-day per-poster scorecard.

The core loop:

1. **Ingest** — twikit fetches posts from the overnight window (prev close → 9 AM ET) using cookie-based scraping
2. **Extract** — three-layer NLP pipeline pulls equity tickers: `$CASHTAG` regex (HIGH), standalone uppercase (MEDIUM), spaCy NER with Yahoo Finance resolver (LOW); whitelist-validated against S&P 500 + Russell 3000
3. **Score** — Claude Haiku assigns direction (LONG/SHORT/NEUTRAL/AMBIGUOUS), conviction (0–5), argument quality, and time horizon
4. **Rank** — five-factor composite: credibility (F1), virality absolute (F2a), virality velocity (F2b), directional consensus (F3), amplifier quality (F4), liquidity modifier (F5); all weights DB-driven via `scoring_config`
5. **Classify** — ACT_NOW (virality threshold crossed), WATCH (velocity floor met), UNSCORED otherwise
6. **Deliver** — morning WhatsApp: top 5 Act Now + top 5 Watch; evening WhatsApp: per-stock returns (overnight / tradeable / excess-vol) + 30-day per-poster scorecard

All signals, factor scores, and outcome metrics persist in Turso-hosted SQLite from day one. This database is the commercial asset.

---

## Project structure

```
influence-post-monitoring/
├── influence_monitor/
│   ├── config.py                    # Settings (pydantic-settings)
│   ├── pipeline.py                  # PipelineOrchestrator entry points
│   ├── ingestion/                   # SocialMediaSource ABC + twikit impl + AccountRegistry
│   ├── extraction/                  # 3-layer ticker extractor + S&P/Russell whitelist
│   ├── scoring/                     # LLMClient ABC + ClaudeHaikuClient + ScoringEngine
│   ├── market_data/                 # MarketDataClient ABC + yfinance + Alpha Vantage + TradingCalendar
│   ├── outcome/                     # OutcomeEngine + ScorecardAggregator
│   ├── delivery/                    # MessageDelivery ABC + TwilioWhatsAppDelivery + CallMeBotDelivery
│   ├── rendering/                   # Morning alert + evening summary composers
│   └── db/                          # SignalRepository + schema.sql
├── config/
│   ├── accounts.json                # 30 primary + 15 backup accounts
│   ├── scoring_config_seed.json     # Factor weights and thresholds seed
│   ├── false_positive_filter.json
│   └── prompts/scoring_prompt.txt
├── data/                            # gitignored — signals.db, twitter_cookies.json
├── tests/
├── .github/workflows/
├── .env.example
└── requirements.txt
```

---

## Setup

### Prerequisites

- Python 3.11+
- Twilio account with WhatsApp Sandbox activated ([console.twilio.com](https://console.twilio.com))
- CallMeBot API key (WhatsApp fallback — send bootstrap message from your phone)
- Anthropic API key — [console.anthropic.com](https://console.anthropic.com)
- A throwaway X/Twitter account for twikit cookie-based scraping

### 1. Clone and install

```bash
git clone https://github.com/andrew-yuhochi/influence-post-monitoring.git
cd influence-post-monitoring

python3.11 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

### 2. Patch twikit

X changed their anti-bot JavaScript after twikit 2.3.3 was released. Run once after install:

```bash
python scripts/patch_twikit.py
```

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your values. See `.env.example` for all variables.

### 4. Activate Twilio WhatsApp Sandbox

1. Go to [console.twilio.com](https://console.twilio.com) → Messaging → Try it out → Send a WhatsApp message
2. From your personal WhatsApp, send `join <sandbox-code>` to `+1 (415) 523-8886`
3. Capture `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, and your sandbox sender number into `.env`

### 5. Get a CallMeBot API key

Send `I allow callmebot to send me messages` to `+34 644 79 98 65` on WhatsApp. You'll receive your API key in reply.

### 6. Authenticate with X/Twitter

```bash
python scripts/import_cookies.py /path/to/cookies_export.json
```

Or use credential login:

```bash
python -m influence_monitor.pipeline auth
```

### 7. Initialize the database

```bash
python -m influence_monitor.db.repository --init
```

Creates `data/signals.db` (or connects to Turso if `TURSO_URL` is set), applies schema, and seeds accounts and scoring config.

### 8. Test WhatsApp delivery

```bash
python -m influence_monitor.delivery.twilio_whatsapp --test-message "hello from influence monitor"
```

---

## Running the pipeline

```bash
# Morning alert — run around 9 AM ET on a trading day
python -m influence_monitor.pipeline morning

# Evening summary — run after 4:30 PM ET
python -m influence_monitor.pipeline evening

# Intra-day engagement poll — fetches current posts and writes engagement_snapshots rows;
# no re-scoring, no alerts sent.  ET-local-time guard: exits 0 outside 09:00–17:00 ET.
python -m influence_monitor.pipeline poll

# Dry run (renders to stdout, no DB writes, no WhatsApp sent)
python -m influence_monitor.pipeline morning --dry-run
python -m influence_monitor.pipeline evening --dry-run
python -m influence_monitor.pipeline poll --dry-run
```

---

## GitHub Actions scheduling

Three workflows run automatically on a Mon–Fri schedule. All require the secrets below to be registered in the repository Settings → Secrets and variables → Actions.

| Workflow file | Schedule (UTC) | Purpose |
|---|---|---|
| `morning_alert.yml` | `0 13 * * 1-5` | Morning alert — 9 AM ET |
| `evening_summary.yml` | `45 20 * * 1-5` | Evening summary — 4:45 PM ET |
| `market_hours_poll.yml` | `0 13,15,17,19,21 * * 1-5` | Engagement snapshot every 2h during market hours |

All three workflows support `workflow_dispatch` for manual one-off runs.

### Required GitHub Actions secrets

| Secret | Description |
|---|---|
| `TWIKIT_USERNAME` | X/Twitter login username for twikit |
| `TWIKIT_EMAIL` | X/Twitter login email |
| `TWIKIT_PASSWORD` | X/Twitter login password |
| `ANTHROPIC_API_KEY` | Claude API key for LLM scoring |
| `TWILIO_ACCOUNT_SID` | Twilio account SID |
| `TWILIO_AUTH_TOKEN` | Twilio auth token |
| `TWILIO_SANDBOX_NUMBER` | WhatsApp sandbox sender (`whatsapp:+14155238886`) |
| `CALLMEBOT_API_KEY` | CallMeBot API key (WhatsApp fallback) |
| `RECIPIENT_PHONE_E164` | Your WhatsApp number in E.164 format |
| `TURSO_URL` | Turso database URL (`libsql://...`) |
| `TURSO_TOKEN` | Turso auth token |
| `ALPHA_VANTAGE_API_KEY` | Alpha Vantage API key (market data fallback) |
| `TWITTER_COOKIES_JSON` | (Optional) twikit cookie JSON — see below |

### Twikit cookie persistence strategy

twikit authenticates via browser cookies stored in `data/twitter_cookies.json`. GitHub Actions runners are ephemeral — this file is destroyed after each run. Two strategies are supported:

**Strategy A — Commit cookies as a multi-line secret (recommended for PoC):**

1. Run `python -m influence_monitor.pipeline auth` locally to generate `data/twitter_cookies.json`.
2. Copy the full JSON content.
3. Add it as a GitHub Actions secret named `TWITTER_COOKIES_JSON` (multi-line secrets are supported).
4. The pipeline reads `TWITTER_COOKIES_JSON` from the environment at startup and writes it to `data/twitter_cookies.json` before twikit initialises.

**Strategy B — One-time `workflow_dispatch` auth run:**

1. Trigger a manual run of any workflow with the `--auth-only` flag (not yet implemented — backlog item).
2. The auth run logs in with username/password, writes cookies, and uploads them as an artifact.
3. Subsequent runs download the artifact. Not implemented at PoC — use Strategy A.

The pipeline's `Settings` class reads `TWITTER_COOKIES_JSON` from the environment and writes it to `COOKIES_PATH` before twikit is initialised. If the env var is unset, the pipeline falls back to username/password login using `TWIKIT_USERNAME` / `TWIKIT_EMAIL` / `TWIKIT_PASSWORD`.

---

## Configuration

All settings are loaded from `.env` via pydantic-settings. See `.env.example` for the full reference.

Key variables:

| Variable | Description |
|---|---|
| `TWILIO_ACCOUNT_SID` | Twilio account SID |
| `TWILIO_AUTH_TOKEN` | Twilio auth token |
| `TWILIO_WHATSAPP_FROM` | Sandbox sender number (E.164, e.g. `+14155238886`) |
| `RECIPIENT_PHONE_E164` | Your WhatsApp number (E.164) |
| `CALLMEBOT_PHONE` | Same recipient phone for CallMeBot fallback |
| `CALLMEBOT_API_KEY` | CallMeBot API key |
| `ANTHROPIC_API_KEY` | Claude API key for LLM scoring |
| `TURSO_URL` | Turso DB URL (`libsql://...`); empty = local `data/signals.db` |
| `TURSO_TOKEN` | Turso auth token |

---

## Scoring model

Each (post, ticker) pair is scored through five factors:

| Factor | Description |
|---|---|
| F1 — Credibility | Manual seed score [1–10] per account |
| F2a — Virality absolute | Views + reposts mapped to [1–10] |
| F2b — Virality velocity | views/hour (Watch List tier) |
| F3 — Directional consensus | Count of distinct posters on same ticker + direction |
| F4 — Amplifier quality | Retweeter profile quality (ACT_NOW only) |
| F5 — Liquidity modifier | Market-cap class multiplier (Mega 0.8× → Micro 1.3×) |

All weights and thresholds live in the `scoring_config` DB table — tune without code changes.

---

## Known limitations

**twikit compatibility**: requires the patch script after every fresh install. If X changes their anti-bot JavaScript, re-run `patch_twikit.py`.

**twikit ToS**: cookie-based scraping violates X's Terms of Service. Built for personal PoC use only. MVP uses the official tweepy API.

**EST/EDT DST handling**: GitHub Actions cron runs in UTC. On the two DST-transition Sundays each year (March spring-forward, November fall-back), the UTC cron fire times drift ±1 hour relative to ET. For `morning_alert.yml` and `evening_summary.yml` this means the alert fires 1 hour early or late on those days — acceptable for PoC. For `market_hours_poll.yml` the pipeline's `poll` subcommand contains a DST-safe ET-local-time guard (using `zoneinfo.ZoneInfo("America/New_York")`) that exits 0 if the current ET hour is outside the 09:00–17:00 window, so on transition days the out-of-window cron fire is silently skipped rather than polluting the DB with off-hours snapshots.

**yfinance staleness**: freshness assertions on every fetch; Alpha Vantage is the fallback. Stale data from both sources marks the signal `price_data_source = 'unavailable'`.

**Burry deletion pattern**: full tweet JSON is stored in `raw_payload` at fetch time — deleted posts remain in the DB.

> This is information about public posts, not investment advice. Do your own research.

---

## License

Private — not licensed for redistribution.
