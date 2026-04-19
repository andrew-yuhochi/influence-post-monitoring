"""AccountRegistry — account validation, handle-resolution, and backup promotion.

Manages the 30-primary + 15-backup account list. On startup, validates every
primary account and runs the handle-resolution sequence for any account that
has accumulated enough consecutive failures to trigger investigation.

Handle-resolution sequence (TDD §2.1):
  1. Check if the handle still exists (get_user_by_screen_name).
  2. If reachable: record success, reset counter.
  3. If unreachable (404/suspended): search by display_name.
  4. If a credible match is found (exact display_name + verified + follower
     count within 50% of last-known): UPDATE accounts.handle — no backup
     promotion needed.
  5. If no credible match: mark status='inactive', promote next backup by
     backup_rank, log WARNING. If all backups exhausted: log ERROR.

Consecutive-failure debounce (TDD §2.1):
  - Any fetch failure increments consecutive_failures via
    repo.update_account_failure().
  - Any successful fetch calls repo.reset_account_failures().
  - When consecutive_failures >= max_consecutive_failures (default 3),
    the resolution sequence runs.
  - retry_rest_minutes (default 30) is enforced between successive retries
    by comparing datetime.now() with last_failure_at before re-attempting.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from influence_monitor.db.repository import SignalRepository
from influence_monitor.ingestion.base import SocialMediaSource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Credible-rename heuristic
# ---------------------------------------------------------------------------
# A search result is considered a credible rename match when ALL three
# conditions hold:
#   1. Exact display_name match (case-insensitive) against the stored
#      display_name of the account we are trying to resolve.
#   2. The candidate has a verified badge (is_verified or is_blue_verified).
#   3. The candidate's follower count is within 50% of the last-known
#      follower count stored in the accounts row. If last-known is 0 or
#      NULL, this condition is skipped (cannot apply the heuristic).
#
# Rationale: handles are user-chosen and change freely; display names are
# the real-world identity and rarely change on rename. The follower-count
# guard prevents false positives from unrelated accounts sharing a similar
# name. The verified badge is a strong additional signal that the account
# represents the same institutional identity.


def _is_credible_rename(
    candidate: Any,
    expected_display_name: str,
    last_known_followers: int | None,
) -> bool:
    """Return True when *candidate* is a credible rename of the account.

    Args:
        candidate: A twikit User object returned by search_user.
        expected_display_name: The display_name stored in the accounts table.
        last_known_followers: The followers_count from the last successful
            fetch, or None if never fetched.
    """
    # Condition 1: exact display_name match (case-insensitive)
    candidate_name = getattr(candidate, "name", None) or ""
    if candidate_name.strip().lower() != expected_display_name.strip().lower():
        return False

    # Condition 2: verified badge
    is_verified = bool(
        getattr(candidate, "is_verified", False)
        or getattr(candidate, "is_blue_verified", False)
    )
    if not is_verified:
        return False

    # Condition 3: follower count within 50% (skip if no baseline)
    if last_known_followers and last_known_followers > 0:
        candidate_followers = getattr(candidate, "followers_count", None)
        if candidate_followers is not None:
            ratio = abs(candidate_followers - last_known_followers) / last_known_followers
            if ratio > 0.5:
                return False

    return True


# ---------------------------------------------------------------------------
# AccountRegistry
# ---------------------------------------------------------------------------

class AccountRegistry:
    """Loads and validates the monitored account list.

    Call ``validate_and_promote()`` at pipeline startup to ensure exactly
    30 primaries are active before fetching begins.
    """

    def __init__(
        self,
        repo: SignalRepository,
        source: SocialMediaSource,
        tenant_id: int = 1,
    ) -> None:
        self._repo = repo
        self._source = source
        self._tenant_id = tenant_id
        self._config: dict[str, float] = {}

    def _get_config(self) -> dict[str, float]:
        """Load scoring_config once per registry lifetime."""
        if not self._config:
            self._config = self._repo.get_scoring_config(self._tenant_id)
        return self._config

    def _max_consecutive_failures(self) -> int:
        return int(self._get_config().get("max_consecutive_failures", 3))

    def _retry_rest_minutes(self) -> float:
        return float(self._get_config().get("retry_rest_minutes", 30))

    def get_active_accounts(self) -> list[dict[str, Any]]:
        """Return all primary accounts for the current tenant."""
        return self._repo.get_accounts_by_status("primary", self._tenant_id)

    def record_fetch_success(self, account_id: int) -> None:
        """Reset consecutive_failures to 0 after a successful fetch."""
        self._repo.reset_account_failures(account_id)

    def record_fetch_failure(self, account_id: int) -> None:
        """Increment consecutive_failures and record last_failure_at."""
        self._repo.update_account_failure(account_id)

    def should_retry(self, account: dict[str, Any]) -> bool:
        """True if the debounce window has elapsed for a failing account.

        Compares datetime.now() with last_failure_at + retry_rest_minutes.
        Returns True if no last_failure_at is recorded (first failure).
        """
        last_failure_raw = account.get("last_failure_at")
        if not last_failure_raw:
            return True

        try:
            last_failure = datetime.fromisoformat(str(last_failure_raw))
            if last_failure.tzinfo is None:
                last_failure = last_failure.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return True

        rest = timedelta(minutes=self._retry_rest_minutes())
        return datetime.now(timezone.utc) >= last_failure + rest

    async def validate_and_promote(self) -> list[dict[str, Any]]:
        """Run the handle-resolution sequence for all failing primaries.

        Called at pipeline startup. Iterates every primary account; if
        consecutive_failures >= max_consecutive_failures and the debounce
        window has elapsed, runs the five-step resolution sequence.

        Returns the refreshed list of active primary accounts.
        """
        primaries = self._repo.get_accounts_by_status("primary", self._tenant_id)
        max_failures = self._max_consecutive_failures()

        for account in primaries:
            failures = int(account.get("consecutive_failures") or 0)
            if failures < max_failures:
                continue  # still within debounce tolerance

            if not self.should_retry(account):
                logger.info(
                    "Account @%s in debounce window (%d failures) — skipping",
                    account["handle"], failures,
                )
                continue

            logger.info(
                "Account @%s has %d consecutive failures — running resolution sequence",
                account["handle"], failures,
            )
            await self._resolve_account(account)

        return self._repo.get_accounts_by_status("primary", self._tenant_id)

    async def _resolve_account(self, account: dict[str, Any]) -> None:
        """Run the five-step handle-resolution sequence for a single account.

        Step 1: Check if handle still reachable via get_user_by_screen_name.
        Step 2: If reachable, reset failures; done.
        Step 3: If unreachable, search by display_name.
        Step 4: If credible rename found, update handle; reset failures; done.
        Step 5: No credible match → mark inactive, promote next backup; if
                no backups remain, log ERROR.
        """
        account_id = account["id"]
        handle = account["handle"]
        display_name = account.get("display_name") or ""

        # Step 1 + 2: check if handle still exists
        try:
            _ = await self._source.search_user(handle)  # lightweight reachability probe
            # If we got here without exception, try the actual user lookup
            # (some search_user implementations return empty list for no-match)
        except Exception:
            pass  # fall through to name search

        # More reliable: attempt get_user_by_screen_name via fetch_recent_posts
        # is expensive; instead we use search_user as a reachability probe and
        # check if the first result matches the exact handle.
        reachable = await self._check_handle_reachable(handle)

        if reachable:
            # Step 2: handle alive — reset failures
            logger.info("Account @%s is reachable — resetting failure counter", handle)
            self._repo.reset_account_failures(account_id)
            return

        # Step 3: handle not reachable — search by display_name
        logger.info(
            "Account @%s not reachable — searching by display_name=%r",
            handle, display_name,
        )
        candidates = await self._source.search_user(display_name)

        # Step 4: check for credible rename
        last_followers = account.get("follower_count_at_post") or None
        for candidate in candidates:
            if _is_credible_rename(candidate, display_name, last_followers):
                new_handle = getattr(candidate, "screen_name", None) or ""
                if new_handle and new_handle.lower() != handle.lower():
                    logger.info(
                        "Credible rename detected: @%s → @%s (display_name=%r)",
                        handle, new_handle, display_name,
                    )
                    self._repo.rename_account_handle(account_id, new_handle)
                    self._repo.reset_account_failures(account_id)
                    return

        # Step 5: no credible match — mark inactive + promote backup
        logger.warning(
            "No credible rename match for @%s (display_name=%r) — marking inactive",
            handle, display_name,
        )
        self._mark_inactive_and_promote(account_id, handle)

    async def _check_handle_reachable(self, handle: str) -> bool:
        """Return True if the handle is reachable on X.

        Uses search_user as a lightweight probe — looks for an exact
        screen_name match in the top results.  Falls back to False on
        any exception.
        """
        try:
            results = await self._source.search_user(handle)
            for user in results:
                screen_name = getattr(user, "screen_name", "") or ""
                if screen_name.lower() == handle.lower():
                    return True
        except Exception as exc:
            logger.warning(
                "_check_handle_reachable failed for @%s: %s", handle, exc,
            )
        return False

    def _mark_inactive_and_promote(self, account_id: int, handle: str) -> None:
        """Mark *account_id* inactive and promote the next backup."""
        # Mark current primary inactive
        self._repo.upsert_account(
            tenant_id=self._tenant_id,
            handle=handle,
            status="inactive",
        )

        # Find next backup ordered by backup_rank
        backups = self._repo.get_accounts_by_status("backup", self._tenant_id)
        if not backups:
            logger.error(
                "All backups exhausted — no replacement for @%s. "
                "Fewer than 30 primaries are now active.",
                handle,
            )
            return

        next_backup = backups[0]  # already ordered by backup_rank ASC
        self._repo.upsert_account(
            tenant_id=self._tenant_id,
            handle=next_backup["handle"],
            display_name=next_backup.get("display_name"),
            angle=next_backup.get("angle"),
            credibility_score=float(next_backup.get("credibility_score", 5.0)),
            status="primary",
            backup_rank=None,
            notes=next_backup.get("notes", ""),
        )
        logger.warning(
            "Promoted backup @%s to primary (replaced inactive @%s)",
            next_backup["handle"], handle,
        )
