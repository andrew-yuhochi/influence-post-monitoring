# Influence Post Monitoring

Automated pipeline to surface influential investor social media posts as actionable trading signals, delivered via morning watchlist and evening scorecard emails.

> Built with [Claude Code](https://claude.ai/code)

## Status

**Phase**: PoC (personal use)

## Overview

This system monitors 17 curated investor/analyst accounts on X (Twitter), extracts ticker mentions using a three-layer NLP pipeline, scores each post for market-moving potential using Claude Haiku, and delivers a ranked daily watchlist by email before market open. An evening scorecard tracks directional accuracy to build a verified track record.

## Setup

See `docs/influence-post-monitoring/poc/` for full project documentation (PRD, TDD, data sources, task breakdown).

### Quick Start

```bash
cd projects/influence-post-monitoring
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm
cp .env.example .env
# Edit .env with your API keys
```

### Running manually

```bash
# Morning watchlist (dry-run — renders to stdout, no DB writes)
python -m influence_monitor.pipeline morning --dry-run

# Evening scorecard (dry-run)
python -m influence_monitor.pipeline evening --dry-run

# Authenticate twikit (run once, saves cookies to data/twitter_cookies.json)
python -m influence_monitor.pipeline auth
```

### GitHub Actions Scheduling

The pipeline runs automatically via two GitHub Actions workflows:

| Workflow | Schedule (UTC) | Local time | Command |
|----------|---------------|------------|---------|
| Morning Pipeline | `0 12 * * 1-5` | 7:00 AM EST / 8:00 AM EDT | `morning` |
| Evening Pipeline | `30 22 * * 1-5` | 5:30 PM EST / 6:30 PM EDT | `evening` |

Both workflows download/upload the SQLite database as a GitHub Actions artifact (90-day retention) to persist state between runs.

**Required GitHub repository secrets** (Settings → Secrets and variables → Actions):

| Secret | Description |
|--------|-------------|
| `ANTHROPIC_API_KEY` | Claude API key for LLM scoring |
| `RESEND_API_KEY` | Resend API key for email delivery |
| `TWITTER_USERNAME` | X/Twitter account username |
| `TWITTER_EMAIL` | X/Twitter account email |
| `TWITTER_PASSWORD` | X/Twitter account password |
| `RECIPIENT_EMAIL` | Email address to receive watchlist and scorecard |

> **Database persistence**: The SQLite file is stored as a GitHub Actions artifact with 90-day retention. Each workflow run downloads the latest artifact before executing and re-uploads it after. For longer retention, migrate to [Turso](https://turso.tech) (free tier: 8 GB, 1B row reads/month) by updating `DATABASE_PATH` to a `libsql://` URL.

## License

Private — not licensed for redistribution.
