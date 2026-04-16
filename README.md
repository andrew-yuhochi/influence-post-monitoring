# Influence Post Monitoring

Automated pipeline to surface influential investor social media posts as actionable trading signals, delivered via morning watchlist and evening scorecard emails.

> Built with [Claude Code](https://claude.ai/code)

## Status

**Phase**: PoC (personal use)

## Overview

This system monitors 17 curated investor/analyst accounts on X (Twitter), extracts ticker mentions using a three-layer NLP pipeline, scores each post for market-moving potential using Claude Haiku, and delivers a ranked daily watchlist by email before market open. An evening scorecard tracks directional accuracy to build a verified track record.

## Setup

See `docs/pocs/influence-post-monitoring/` for full project documentation (PRD, TDD, data sources, task breakdown).

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

## License

Private — not licensed for redistribution.
