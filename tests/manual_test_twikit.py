"""Manual integration test for TwitterIngestor.

Run with real Twitter credentials configured in .env:
    python tests/manual_test_twikit.py

Tests against 3 accounts to verify fetch works without triggering
rate limits. Do NOT run against all 17 during development.
"""

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone

from influence_monitor.config import Settings
from influence_monitor.ingestion.twitter_twikit import TwitterIngestor

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Test with 3 active, high-volume accounts
TEST_HANDLES = ["BillAckman", "chamath", "CathieDWood"]


async def main() -> None:
    settings = Settings()
    if not settings.twitter_username:
        print("ERROR: Twitter credentials not configured in .env")
        sys.exit(1)

    ingestor = TwitterIngestor(settings)
    since = datetime.now(timezone.utc) - timedelta(days=7)

    print(f"\nFetching posts since {since.isoformat()} from {TEST_HANDLES}\n")

    for handle in TEST_HANDLES:
        try:
            posts = await ingestor.fetch_recent_posts(handle, since=since, max_count=5)
            print(f"@{handle}: {len(posts)} posts")
            for p in posts[:2]:
                assert p.text, f"Post {p.external_id} has empty text"
                assert p.external_id, "Post has no external_id"
                assert p.posted_at, "Post has no posted_at"
                assert p.posted_at > since, (
                    f"Post {p.external_id} posted_at {p.posted_at} is before since {since}"
                )
                print(f"  [{p.external_id}] {p.posted_at.isoformat()}")
                print(f"    views={p.view_count} reposts={p.repost_count} "
                      f"likes={p.like_count} bookmarks={p.bookmark_count}")
                print(f"    text={p.text[:100]}...")
                print(f"    raw_payload keys: {list(p.raw_payload.keys())[:5]}")
                print()
        except Exception as exc:
            print(f"@{handle}: FAILED — {type(exc).__name__}: {exc}")

    print("Manual integration test complete.")


if __name__ == "__main__":
    asyncio.run(main())
