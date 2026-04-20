"""Integration tests — TASK-010b.

End-to-end integration tests against the new synchronous PipelineOrchestrator.
Uses an isolated SQLite DB per test (no Turso connection required).

No skip markers — these tests run unconditionally.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from influence_monitor.config import Settings
from influence_monitor.db.repository import SignalRepository
from influence_monitor.delivery.registry import DELIVERY_REGISTRY
from influence_monitor.pipeline import PipelineOrchestrator

_FIXTURES_PATH = Path(__file__).parent / "fixtures" / "sample_signals.json"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    return tmp_path / "integration_test.db"


@pytest.fixture
def settings(tmp_db: Path) -> Settings:
    return Settings(
        database_path=str(tmp_db),
        turso_url="",
        delivery_primary="twilio",
        delivery_fallback="callmebot",
        social_source="twitter_twikit",
        anthropic_api_key="test-key",
        recipient_phone_e164="whatsapp:+10000000000",
    )


@pytest.fixture
def repo(settings: Settings) -> SignalRepository:
    r = SignalRepository(settings)
    r.init_schema()
    r.seed(phone_e164=settings.recipient_phone_e164)
    yield r
    r.close()


@pytest.fixture
def orchestrator(settings: Settings, repo: SignalRepository) -> PipelineOrchestrator:
    with patch("influence_monitor.pipeline.SOURCE_REGISTRY", {
        "twitter_twikit": MagicMock(return_value=MagicMock())
    }):
        return PipelineOrchestrator(settings=settings, repo=repo)


# ---------------------------------------------------------------------------
# Integration: fixtures mode end-to-end
# ---------------------------------------------------------------------------

class TestFixturesModeIntegration:
    def test_fixtures_pipeline_completes(
        self, orchestrator: PipelineOrchestrator
    ) -> None:
        """fixtures mode must complete without raising on any day."""
        delivered: list[str] = []

        with patch.object(orchestrator, "_deliver", lambda text, kind, dry_run: delivered.append(text)):
            orchestrator.run_morning(
                run_date=date(2026, 4, 21),
                dry_run=True,
                use_fixtures=True,
            )

        assert len(delivered) >= 1

    def test_fixtures_pipeline_contains_act_now_section(
        self, orchestrator: PipelineOrchestrator
    ) -> None:
        """Rendered output should contain the ACT NOW section header."""
        delivered: list[str] = []

        with patch.object(orchestrator, "_deliver", lambda text, kind, dry_run: delivered.append(text)):
            orchestrator.run_morning(
                run_date=date(2026, 4, 21),
                dry_now=False,
                use_fixtures=True,
            ) if False else orchestrator.run_morning(
                run_date=date(2026, 4, 21),
                dry_run=True,
                use_fixtures=True,
            )

        full_text = "\n".join(delivered)
        assert "ACT NOW" in full_text

    def test_fixtures_pipeline_contains_watch_section(
        self, orchestrator: PipelineOrchestrator
    ) -> None:
        """Rendered output should contain the WATCH LIST section header."""
        delivered: list[str] = []

        with patch.object(orchestrator, "_deliver", lambda text, kind, dry_run: delivered.append(text)):
            orchestrator.run_morning(
                run_date=date(2026, 4, 21),
                dry_run=True,
                use_fixtures=True,
            )

        full_text = "\n".join(delivered)
        assert "WATCH" in full_text

    def test_fixtures_pipeline_on_saturday(
        self, orchestrator: PipelineOrchestrator
    ) -> None:
        """--use-fixtures must work on Saturday (non-trading day)."""
        delivered: list[str] = []

        with patch.object(orchestrator, "_deliver", lambda text, kind, dry_run: delivered.append(text)):
            orchestrator.run_morning(
                run_date=date(2026, 4, 19),  # Saturday
                dry_run=True,
                use_fixtures=True,
            )

        assert len(delivered) >= 1, "Expected at least one message even on Saturday in fixtures mode"


# ---------------------------------------------------------------------------
# Integration: sample_signals.json structure
# ---------------------------------------------------------------------------

class TestSampleSignalsJson:
    def test_fixture_file_exists(self) -> None:
        assert _FIXTURES_PATH.exists(), f"Fixture file not found at {_FIXTURES_PATH}"

    def test_fixture_has_required_fields(self) -> None:
        raw = json.loads(_FIXTURES_PATH.read_text())
        assert len(raw) >= 3, "Expected at least 3 signals in sample_signals.json"

        required = {"ticker", "direction", "tier", "final_score", "account_handle"}
        for i, sig in enumerate(raw):
            missing = required - set(sig.keys())
            assert not missing, f"Signal {i} missing fields: {missing}"

    def test_fixture_has_ackman_burry_signals(self) -> None:
        """Fixture should contain signals from Ackman and Burry."""
        raw = json.loads(_FIXTURES_PATH.read_text())
        handles = {s["account_handle"].lower() for s in raw}
        assert "billackman" in handles, "Expected BillAckman signal in fixture"
        assert "michaeljburry" in handles, "Expected michaeljburry signal in fixture"

    def test_fixture_has_act_now_and_watch_tiers(self) -> None:
        raw = json.loads(_FIXTURES_PATH.read_text())
        tiers = {s["tier"] for s in raw}
        assert "ACT_NOW" in tiers, "Expected ACT_NOW signals in fixture"
        assert "WATCH" in tiers, "Expected WATCH signals in fixture"


# ---------------------------------------------------------------------------
# Integration: non-trading-day short-circuit
# ---------------------------------------------------------------------------

class TestNonTradingDayShortCircuit:
    def test_non_trading_day_does_not_deliver(
        self, orchestrator: PipelineOrchestrator
    ) -> None:
        """Non-trading day must short-circuit without delivering a message."""
        delivered: list[str] = []
        mock_accounts = [
            {"id": 1, "handle": "BillAckman", "angle": "Activist", "credibility_score": 9.0, "status": "primary"},
        ]

        with patch.object(orchestrator._account_registry, "validate_and_promote", return_value=mock_accounts):
            with patch.object(orchestrator._calendar, "is_trading_day", return_value=False):
                with patch.object(orchestrator, "_deliver", lambda t, k, d: delivered.append(t)):
                    orchestrator.run_morning(
                        run_date=date(2026, 4, 19),  # Saturday
                        dry_run=True,
                    )

        # No morning message should have been delivered
        morning_messages = [m for m in delivered if "Morning Alert" in m]
        assert len(morning_messages) == 0


# ---------------------------------------------------------------------------
# Integration: DB writes in non-dry-run mode
# ---------------------------------------------------------------------------

class TestDbWrites:
    def test_daily_summary_written_after_fixtures_run(
        self, orchestrator: PipelineOrchestrator, repo: SignalRepository
    ) -> None:
        """daily_summaries must be upserted after a non-dry-run fixtures run."""
        with patch.object(orchestrator, "_deliver", MagicMock()):
            orchestrator.run_morning(
                run_date=date(2026, 4, 21),
                dry_run=False,
                use_fixtures=True,
            )

        rows = repo._execute(
            "SELECT * FROM daily_summaries WHERE summary_date = ? AND run_type = 'morning'",
            ["2026-04-21"],
        )
        assert len(rows) == 1, "Expected one daily_summaries row after pipeline run"
        assert rows[0]["pipeline_status"] == "ok"

    def test_signals_written_after_fixtures_run(
        self, orchestrator: PipelineOrchestrator, repo: SignalRepository
    ) -> None:
        """signals table must be populated after a non-dry-run fixtures run."""
        # Delete pre-existing signals for the date
        repo._execute_write("DELETE FROM signals WHERE signal_date = '2026-04-21'")

        with patch.object(orchestrator, "_deliver", MagicMock()):
            orchestrator.run_morning(
                run_date=date(2026, 4, 21),
                dry_run=False,
                use_fixtures=True,
            )

        fixture_data = json.loads(_FIXTURES_PATH.read_text())
        expected_count = len(fixture_data)
        rows = repo._execute(
            "SELECT COUNT(*) AS cnt FROM signals WHERE signal_date = '2026-04-21'",
        )
        actual_count = rows[0]["cnt"]
        assert actual_count == expected_count, (
            f"Expected {expected_count} signals, got {actual_count}"
        )


# ---------------------------------------------------------------------------
# Integration: DELIVERY_REGISTRY paths
# ---------------------------------------------------------------------------

class TestDeliveryRegistryPaths:
    def test_twilio_cls_in_registry(self) -> None:
        assert "twilio" in DELIVERY_REGISTRY

    def test_callmebot_cls_in_registry(self) -> None:
        assert "callmebot" in DELIVERY_REGISTRY

    def test_orchestrator_uses_registry(
        self, orchestrator: PipelineOrchestrator
    ) -> None:
        """PipelineOrchestrator must hold references to DELIVERY_REGISTRY classes."""
        primary_cls = orchestrator._primary_delivery_cls
        fallback_cls = orchestrator._fallback_delivery_cls
        assert primary_cls is not None or fallback_cls is not None
