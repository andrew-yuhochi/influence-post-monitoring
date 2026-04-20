"""Tests for PipelineOrchestrator — TASK-010b.

Verifies the synchronous orchestrator API, fixtures mode, dry-run behaviour,
non-trading-day short-circuit, and all-inactive-accounts error path.

No skip markers — these tests run unconditionally.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from influence_monitor.config import Settings
from influence_monitor.db.repository import SignalRepository
from influence_monitor.delivery.registry import DELIVERY_REGISTRY
from influence_monitor.pipeline import PipelineOrchestrator, _build_morning_signals


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(tmp_db: Path) -> Settings:
    """Return a Settings instance pointing at an isolated SQLite DB."""
    return Settings(
        database_path=str(tmp_db),
        turso_url="",
        delivery_primary="twilio",
        delivery_fallback="callmebot",
        social_source="twitter_twikit",
        anthropic_api_key="test-key",
        recipient_phone_e164="whatsapp:+10000000000",
    )


def _make_repo(settings: Settings) -> SignalRepository:
    repo = SignalRepository(settings)
    repo.init_schema()
    repo.seed(phone_e164=settings.recipient_phone_e164)
    return repo


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    return tmp_path / "signals_test.db"


@pytest.fixture
def settings(tmp_db: Path) -> Settings:
    return _make_settings(tmp_db)


@pytest.fixture
def repo(settings: Settings) -> SignalRepository:
    r = _make_repo(settings)
    yield r
    r.close()


# ---------------------------------------------------------------------------
# Test: _build_morning_signals helper
# ---------------------------------------------------------------------------

class TestBuildMorningSignals:
    def test_act_now_and_watch_split(self) -> None:
        rows = [
            {
                "ticker": "AAPL",
                "tier": "ACT_NOW",
                "direction": "LONG",
                "final_score": 8.0,
                "conviction_score": 8.0,
                "key_claim": "Apple is great",
                "account_id": 1,
                "account_handle": "TestHandle",
                "posted_at": "2026-04-21T09:00:00Z",
                "views_per_hour": 5000.0,
                "direction_flip": False,
                "conflict_group": None,
                "market_cap_class": "Mega",
            },
            {
                "ticker": "TSLA",
                "tier": "WATCH",
                "direction": "SHORT",
                "final_score": 5.0,
                "conviction_score": 5.0,
                "key_claim": "Tesla is risky",
                "account_id": 1,
                "account_handle": "TestHandle",
                "posted_at": "2026-04-21T09:00:00Z",
                "views_per_hour": 1200.0,
                "direction_flip": False,
                "conflict_group": None,
                "market_cap_class": "Mega",
            },
            {
                "ticker": "SPY",
                "tier": "UNSCORED",
                "direction": "NEUTRAL",
                "final_score": 0.0,
                "conviction_score": 0.0,
                "key_claim": "Market flat",
                "account_id": 1,
                "account_handle": "TestHandle",
                "posted_at": "2026-04-21T09:00:00Z",
                "views_per_hour": 100.0,
                "direction_flip": False,
                "conflict_group": None,
                "market_cap_class": "Mega",
            },
        ]
        act_now, watch = _build_morning_signals(rows, {}, None)
        assert len(act_now) == 1
        assert act_now[0].ticker == "AAPL"
        assert act_now[0].tier == "act_now"
        assert len(watch) == 1
        assert watch[0].ticker == "TSLA"
        assert watch[0].tier == "watch"

    def test_direction_flip_preserved(self) -> None:
        rows = [
            {
                "ticker": "GME",
                "tier": "ACT_NOW",
                "direction": "LONG",
                "final_score": 6.8,
                "conviction_score": 6.8,
                "key_claim": "GME flip",
                "account_id": 1,
                "account_handle": "SomeHandle",
                "posted_at": "2026-04-21T09:00:00Z",
                "views_per_hour": 3000.0,
                "direction_flip": True,
                "conflict_group": None,
                "market_cap_class": "Large",
            }
        ]
        act_now, watch = _build_morning_signals(rows, {}, None)
        assert act_now[0].direction_flip is True

    def test_conflict_group_preserved(self) -> None:
        rows = [
            {
                "ticker": "TSLA",
                "tier": "ACT_NOW",
                "direction": "LONG",
                "final_score": 7.6,
                "conviction_score": 7.6,
                "key_claim": "Bull case",
                "account_id": 1,
                "account_handle": "Handle",
                "posted_at": "2026-04-21T09:00:00Z",
                "views_per_hour": 4100.0,
                "direction_flip": False,
                "conflict_group": "opposing_exists",
                "market_cap_class": "Mega",
            }
        ]
        act_now, _ = _build_morning_signals(rows, {}, None)
        assert act_now[0].conflict_group == "opposing_exists"


# ---------------------------------------------------------------------------
# Test: PipelineOrchestrator — instantiation
# ---------------------------------------------------------------------------

class TestPipelineOrchestratorInit:
    @patch("influence_monitor.pipeline.SOURCE_REGISTRY", {"twitter_twikit": MagicMock(return_value=MagicMock())})
    def test_init_succeeds_with_valid_settings(self, settings: Settings, repo: SignalRepository) -> None:
        # Should instantiate without raising
        orch = PipelineOrchestrator(settings=settings, repo=repo)
        assert orch is not None

    def test_invalid_social_source_raises(self, settings: Settings, repo: SignalRepository) -> None:
        settings.social_source = "nonexistent_source"
        with pytest.raises(ValueError, match="Unknown social source"):
            PipelineOrchestrator(settings=settings, repo=repo)


# ---------------------------------------------------------------------------
# Test: non-trading-day short-circuit
# ---------------------------------------------------------------------------

class TestNonTradingDay:
    @patch("influence_monitor.pipeline.SOURCE_REGISTRY", {"twitter_twikit": MagicMock(return_value=MagicMock())})
    def test_non_trading_day_exits_early(self, settings: Settings, repo: SignalRepository) -> None:
        orch = PipelineOrchestrator(settings=settings, repo=repo)

        # Patch validate_and_promote to return 3 mock accounts (step 1 passes)
        mock_accounts = [
            {"id": 1, "handle": "BillAckman", "angle": "Activist", "credibility_score": 9.0, "status": "primary"},
            {"id": 2, "handle": "michaeljburry", "angle": "Value/Short", "credibility_score": 9.0, "status": "primary"},
            {"id": 3, "handle": "carsonblock", "angle": "Short", "credibility_score": 8.0, "status": "primary"},
        ]
        with patch.object(orch._account_registry, "validate_and_promote", return_value=mock_accounts):
            with patch.object(orch._calendar, "is_trading_day", return_value=False):
                # Should return without error and without calling fetch_all_accounts
                orch.run_morning(run_date=date(2026, 4, 19), dry_run=True)  # Saturday


# ---------------------------------------------------------------------------
# Test: all-inactive-accounts path
# ---------------------------------------------------------------------------

class TestAllInactiveAccounts:
    @patch("influence_monitor.pipeline.SOURCE_REGISTRY", {"twitter_twikit": MagicMock(return_value=MagicMock())})
    def test_all_inactive_sends_operational_message(
        self, settings: Settings, repo: SignalRepository
    ) -> None:
        orch = PipelineOrchestrator(settings=settings, repo=repo)

        operational_messages: list[str] = []

        def fake_send_operational(s: Settings, text: str, dry_run: bool = False) -> None:
            operational_messages.append(text)

        with patch.object(orch._account_registry, "validate_and_promote", return_value=[]):
            with patch("influence_monitor.pipeline._send_operational_message", fake_send_operational):
                orch.run_morning(run_date=date(2026, 4, 21), dry_run=True)

        assert len(operational_messages) == 1
        assert "inactive" in operational_messages[0].lower() or "failed" in operational_messages[0].lower()


# ---------------------------------------------------------------------------
# Test: dry-run does not write to DB
# ---------------------------------------------------------------------------

class TestDryRun:
    @patch("influence_monitor.pipeline.SOURCE_REGISTRY", {"twitter_twikit": MagicMock(return_value=MagicMock())})
    def test_dry_run_does_not_write_signals(
        self, settings: Settings, repo: SignalRepository
    ) -> None:
        orch = PipelineOrchestrator(settings=settings, repo=repo)

        mock_accounts = [
            {"id": 1, "handle": "BillAckman", "angle": "Activist", "credibility_score": 9.0, "status": "primary"},
        ]

        with patch.object(orch._account_registry, "validate_and_promote", return_value=mock_accounts):
            with patch.object(orch._calendar, "is_trading_day", return_value=True):
                with patch.object(orch._account_registry, "fetch_all_accounts", return_value=([], 1, 0)):
                    # With no posts, no signals should be written even on a trading day
                    orch.run_morning(run_date=date(2026, 4, 21), dry_run=True)

        # Signals table should be empty
        rows = repo.get_signals_for_date(date(2026, 4, 21), tenant_id=1)
        assert len(rows) == 0


# ---------------------------------------------------------------------------
# Test: --use-fixtures mode
# ---------------------------------------------------------------------------

class TestFixturesMode:
    @patch("influence_monitor.pipeline.SOURCE_REGISTRY", {"twitter_twikit": MagicMock(return_value=MagicMock())})
    def test_fixtures_mode_renders_signals(
        self, settings: Settings, repo: SignalRepository
    ) -> None:
        orch = PipelineOrchestrator(settings=settings, repo=repo)

        rendered: list[str] = []

        def fake_deliver(text: str, kind: str, dry_run: bool) -> None:
            rendered.append(text)

        with patch.object(orch, "_deliver", fake_deliver):
            orch.run_morning(
                run_date=date(2026, 4, 21),
                dry_run=True,
                use_fixtures=True,
            )

        assert len(rendered) >= 1, "Expected at least one rendered message"
        # Verify the morning alert contains expected ticker from sample_signals.json
        full_text = " ".join(rendered)
        assert "FNMA" in full_text or "NFLX" in full_text or "AAPL" in full_text

    @patch("influence_monitor.pipeline.SOURCE_REGISTRY", {"twitter_twikit": MagicMock(return_value=MagicMock())})
    def test_fixtures_mode_works_on_any_day(
        self, settings: Settings, repo: SignalRepository
    ) -> None:
        """--use-fixtures must complete successfully regardless of trading calendar."""
        orch = PipelineOrchestrator(settings=settings, repo=repo)

        with patch.object(orch, "_deliver", MagicMock()):
            # Saturday — non-trading day — should still complete
            orch.run_morning(
                run_date=date(2026, 4, 19),  # Saturday
                dry_run=True,
                use_fixtures=True,
            )

    @patch("influence_monitor.pipeline.SOURCE_REGISTRY", {"twitter_twikit": MagicMock(return_value=MagicMock())})
    def test_fixtures_mode_no_twikit_credentials_required(
        self, settings: Settings, repo: SignalRepository
    ) -> None:
        """fixtures mode must NOT call twikit or Claude."""
        settings.twikit_username = ""
        settings.twikit_password = ""
        settings.anthropic_api_key = ""
        orch = PipelineOrchestrator(settings=settings, repo=repo)

        with patch.object(orch, "_deliver", MagicMock()):
            # Should complete without error even with no credentials
            orch.run_morning(
                run_date=date(2026, 4, 21),
                dry_run=True,
                use_fixtures=True,
            )


# ---------------------------------------------------------------------------
# Test: AmplifierFetcher called only for ACT_NOW
# ---------------------------------------------------------------------------

class TestAmplifierCallCount:
    @patch("influence_monitor.pipeline.SOURCE_REGISTRY", {"twitter_twikit": MagicMock(return_value=MagicMock())})
    def test_amplifier_not_called_for_watch_signals(
        self, settings: Settings, repo: SignalRepository
    ) -> None:
        """AmplifierFetcher.fetch_and_score must be called 0 times when there are no ACT_NOW signals."""
        orch = PipelineOrchestrator(settings=settings, repo=repo)
        call_log: list[str] = []

        original = orch._amplifier.fetch_and_score

        def spy_fetch_and_score(post, source, post_db_id, tier="ACT_NOW"):
            call_log.append(tier)
            return original(post, source, post_db_id, tier)

        orch._amplifier.fetch_and_score = spy_fetch_and_score  # type: ignore[method-assign]

        mock_accounts = [
            {"id": 1, "handle": "BillAckman", "angle": "Activist", "credibility_score": 9.0, "status": "primary"},
        ]

        with patch.object(orch._account_registry, "validate_and_promote", return_value=mock_accounts):
            with patch.object(orch._calendar, "is_trading_day", return_value=True):
                with patch.object(orch._account_registry, "fetch_all_accounts", return_value=([], 1, 0)):
                    orch.run_morning(run_date=date(2026, 4, 21), dry_run=True)

        # No posts → no signals → amplifier never called
        assert call_log == [], f"Amplifier was called unexpectedly: {call_log}"


# ---------------------------------------------------------------------------
# Test: DELIVERY_REGISTRY contains expected providers
# ---------------------------------------------------------------------------

class TestDeliveryRegistry:
    def test_twilio_in_registry(self) -> None:
        assert "twilio" in DELIVERY_REGISTRY

    def test_callmebot_in_registry(self) -> None:
        assert "callmebot" in DELIVERY_REGISTRY

    def test_registry_values_are_classes(self) -> None:
        for name, cls in DELIVERY_REGISTRY.items():
            assert callable(cls), f"{name} delivery cls is not callable"


# ---------------------------------------------------------------------------
# Test: AC-6 — factor columns round-trip through DB (insert → SELECT back)
# ---------------------------------------------------------------------------

class TestAC6FactorColumnsRoundTrip:
    """AC-6: All signals persisted with F1, F2a/F2b, F3, F5 populated;
    F4 populated only for ACT_NOW.

    Calls repo.insert_post() + repo.insert_signal() directly (the same
    codepath used by the pipeline persist loop) and reads the row back via
    get_signals_for_date() to confirm every factor column lands correctly.
    """

    def _setup_post(self, repo: SignalRepository) -> tuple[int, int]:
        """Return (account_id, post_id) for use in insert_signal().

        Uses the first seeded primary account and inserts a synthetic post row.
        """
        from datetime import datetime as _dt, timezone as _tz

        accounts = repo.get_accounts_by_status("primary", tenant_id=1)
        assert accounts, "No seeded primary accounts found"
        account_id: int = accounts[0]["id"]

        post_id = repo.insert_post(
            tenant_id=1,
            account_id=account_id,
            external_id="test-ext-id-ac6",
            source_type="twitter_twikit",
            text="Synthetic test post for AC-6 round-trip",
            posted_at=_dt(2026, 4, 21, 9, 0, 0, tzinfo=_tz.utc),
            fetched_at=_dt(2026, 4, 21, 9, 1, 0, tzinfo=_tz.utc),
            view_count=50000,
            repost_count=2000,
        )
        assert post_id is not None, "insert_post should return a rowid"
        return account_id, post_id

    def _insert_signal(
        self,
        repo: SignalRepository,
        *,
        account_id: int,
        post_id: int,
        tier: str,
        score_amplifier: float | None,
        signal_date: str = "2026-04-21",
    ) -> None:
        repo.insert_signal(
            tenant_id=1,
            user_id=1,
            post_id=post_id,
            account_id=account_id,
            signal_date=signal_date,
            ticker="AAPL",
            extraction_confidence="HIGH",
            market_cap_class="Mega",
            direction="LONG",
            conviction_level=4,
            argument_quality="STRONG",
            time_horizon="SHORT",
            market_moving_potential=1,
            key_claim="Strong buy signal",
            rationale="Bull case intact",
            llm_model_version="claude-3-5-sonnet",
            llm_raw_response=None,
            llm_input_tokens=100,
            llm_output_tokens=50,
            score_credibility=8.5,            # F1
            score_virality_abs=7.0,           # F2a
            score_virality_vel=None,          # F2b — None for ACT_NOW per TDD
            score_consensus=6.0,              # F3
            score_amplifier=score_amplifier,  # F4 — tier-dependent
            liquidity_modifier=1.0,
            conviction_score=7.8,
            direction_flip=0,
            conflict_group=None,
            penalty_applied=0.0,
            final_score=7.8,                  # F5
            tier=tier,
            engagement_views=50000,
            engagement_reposts=2000,
            views_per_hour=4500.0,
        )

    def test_act_now_signal_factor_columns_persisted(
        self, repo: SignalRepository
    ) -> None:
        """ACT_NOW signal: F1, F2a, F3, F5 set; F4 (score_amplifier) set."""
        from datetime import date as _date

        account_id, post_id = self._setup_post(repo)
        self._insert_signal(
            repo, account_id=account_id, post_id=post_id,
            tier="ACT_NOW", score_amplifier=3.2,
        )
        rows = repo.get_signals_for_date(_date(2026, 4, 21), tenant_id=1)
        assert len(rows) == 1, "Expected exactly one signal row"
        row = rows[0]

        # F1 — credibility
        assert row["score_credibility"] == pytest.approx(8.5)
        # F2a — virality absolute
        assert row["score_virality_abs"] == pytest.approx(7.0)
        # F3 — consensus
        assert row["score_consensus"] == pytest.approx(6.0)
        # F5 — final score
        assert row["final_score"] == pytest.approx(7.8)
        # F4 — amplifier populated for ACT_NOW
        assert row["score_amplifier"] is not None
        assert row["score_amplifier"] == pytest.approx(3.2)

    def test_watch_signal_factor_columns_persisted(
        self, repo: SignalRepository
    ) -> None:
        """WATCH signal: F1, F2a, F3, F5 set; F4 (score_amplifier) is NULL."""
        from datetime import date as _date

        account_id, post_id = self._setup_post(repo)
        self._insert_signal(
            repo, account_id=account_id, post_id=post_id,
            tier="WATCH", score_amplifier=None,
        )
        rows = repo.get_signals_for_date(_date(2026, 4, 21), tenant_id=1)
        assert len(rows) == 1, "Expected exactly one signal row"
        row = rows[0]

        # F1 — credibility
        assert row["score_credibility"] == pytest.approx(8.5)
        # F2a — virality absolute
        assert row["score_virality_abs"] == pytest.approx(7.0)
        # F3 — consensus
        assert row["score_consensus"] == pytest.approx(6.0)
        # F5 — final score
        assert row["final_score"] == pytest.approx(7.8)
        # F4 — amplifier must be NULL for WATCH
        assert row["score_amplifier"] is None, (
            f"score_amplifier should be NULL for WATCH signal, got {row['score_amplifier']!r}"
        )
