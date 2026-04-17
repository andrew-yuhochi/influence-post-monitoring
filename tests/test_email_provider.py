"""Unit tests for email provider layer — ABC, Resend, SendGrid stub, registry."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from influence_monitor.config import Settings
from influence_monitor.email.base import EmailDeliveryError, EmailProvider
from influence_monitor.email.registry import EMAIL_REGISTRY
from influence_monitor.email.resend_provider import ResendEmailProvider
from influence_monitor.email.sendgrid_provider import SendGridEmailProvider


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

_SUBJECT = "Morning Watchlist — 2026-04-17"
_HTML = "<h1>Top Signals</h1>"
_TEXT = "Top Signals"
_TO = "user@example.com"
_MSG_ID = "4ef9a417-02d9-4cac-8b0f-0b2e1e032a53"


def _settings(**overrides) -> Settings:
    return Settings(
        resend_api_key="re_test_key",
        sender_email="Monitor <monitor@test.com>",
        **overrides,
    )


def _make_repo() -> MagicMock:
    repo = MagicMock()
    repo.log_api_usage = AsyncMock()
    return repo


# ------------------------------------------------------------------
# EmailProvider ABC
# ------------------------------------------------------------------


class TestEmailProviderABC:
    def test_cannot_instantiate(self) -> None:
        with pytest.raises(TypeError):
            EmailProvider()

    def test_resend_is_subclass(self) -> None:
        assert issubclass(ResendEmailProvider, EmailProvider)

    def test_sendgrid_is_subclass(self) -> None:
        assert issubclass(SendGridEmailProvider, EmailProvider)


# ------------------------------------------------------------------
# ResendEmailProvider — successful send
# ------------------------------------------------------------------


class TestResendSuccess:
    @pytest.mark.asyncio
    @patch("influence_monitor.email.resend_provider.resend")
    async def test_send_returns_message_id(self, mock_resend: MagicMock) -> None:
        mock_resend.Emails.send.return_value = {"id": _MSG_ID}

        provider = ResendEmailProvider(_settings())
        msg_id = await provider.send(_TO, _SUBJECT, _HTML, _TEXT)

        assert msg_id == _MSG_ID

    @pytest.mark.asyncio
    @patch("influence_monitor.email.resend_provider.resend")
    async def test_send_calls_resend_with_correct_params(
        self, mock_resend: MagicMock,
    ) -> None:
        mock_resend.Emails.send.return_value = {"id": _MSG_ID}

        provider = ResendEmailProvider(_settings())
        await provider.send(_TO, _SUBJECT, _HTML, _TEXT)

        mock_resend.Emails.send.assert_called_once_with(
            {
                "from": "Monitor <monitor@test.com>",
                "to": [_TO],
                "subject": _SUBJECT,
                "html": _HTML,
                "text": _TEXT,
            }
        )

    @pytest.mark.asyncio
    @patch("influence_monitor.email.resend_provider.resend")
    async def test_send_sets_api_key(self, mock_resend: MagicMock) -> None:
        mock_resend.Emails.send.return_value = {"id": _MSG_ID}

        ResendEmailProvider(_settings())
        assert mock_resend.api_key == "re_test_key"


# ------------------------------------------------------------------
# ResendEmailProvider — retry logic
# ------------------------------------------------------------------


class TestResendRetry:
    @pytest.mark.asyncio
    @patch("influence_monitor.email.resend_provider.time.sleep")
    @patch("influence_monitor.email.resend_provider.resend")
    async def test_retries_once_on_failure_then_succeeds(
        self, mock_resend: MagicMock, mock_sleep: MagicMock,
    ) -> None:
        """First call fails, second succeeds → returns message ID."""
        mock_resend.Emails.send.side_effect = [
            Exception("network error"),
            {"id": _MSG_ID},
        ]

        provider = ResendEmailProvider(_settings())
        msg_id = await provider.send(_TO, _SUBJECT, _HTML, _TEXT)

        assert msg_id == _MSG_ID
        assert mock_resend.Emails.send.call_count == 2
        mock_sleep.assert_called_once_with(30)

    @pytest.mark.asyncio
    @patch("influence_monitor.email.resend_provider.time.sleep")
    @patch("influence_monitor.email.resend_provider.resend")
    async def test_raises_after_two_failures(
        self, mock_resend: MagicMock, mock_sleep: MagicMock,
    ) -> None:
        """Both attempts fail → raises EmailDeliveryError."""
        mock_resend.Emails.send.side_effect = Exception("persistent failure")

        provider = ResendEmailProvider(_settings())
        with pytest.raises(EmailDeliveryError, match="2 attempts"):
            await provider.send(_TO, _SUBJECT, _HTML, _TEXT)

        assert mock_resend.Emails.send.call_count == 2
        mock_sleep.assert_called_once_with(30)

    @pytest.mark.asyncio
    @patch("influence_monitor.email.resend_provider.resend")
    async def test_no_retry_on_first_success(
        self, mock_resend: MagicMock,
    ) -> None:
        """First call succeeds → no retry, no sleep."""
        mock_resend.Emails.send.return_value = {"id": _MSG_ID}

        provider = ResendEmailProvider(_settings())
        await provider.send(_TO, _SUBJECT, _HTML, _TEXT)

        assert mock_resend.Emails.send.call_count == 1


# ------------------------------------------------------------------
# ResendEmailProvider — API usage logging
# ------------------------------------------------------------------


class TestResendAPIUsageLogging:
    @pytest.mark.asyncio
    @patch("influence_monitor.email.resend_provider.resend")
    async def test_logs_usage_on_success(self, mock_resend: MagicMock) -> None:
        mock_resend.Emails.send.return_value = {"id": _MSG_ID}
        repo = _make_repo()

        provider = ResendEmailProvider(_settings(), repo=repo)
        await provider.send(_TO, _SUBJECT, _HTML, _TEXT)

        repo.log_api_usage.assert_called_once()
        call_kwargs = repo.log_api_usage.call_args
        assert call_kwargs[1]["provider"] == "resend"
        assert call_kwargs[1]["endpoint"] == "Emails.send"
        assert call_kwargs[1]["status"] == "ok"
        assert isinstance(call_kwargs[1]["latency_ms"], int)

    @pytest.mark.asyncio
    @patch("influence_monitor.email.resend_provider.time.sleep")
    @patch("influence_monitor.email.resend_provider.resend")
    async def test_logs_error_on_failure(
        self, mock_resend: MagicMock, mock_sleep: MagicMock,
    ) -> None:
        mock_resend.Emails.send.side_effect = Exception("API down")
        repo = _make_repo()

        provider = ResendEmailProvider(_settings(), repo=repo)
        with pytest.raises(EmailDeliveryError):
            await provider.send(_TO, _SUBJECT, _HTML, _TEXT)

        # Both attempts logged
        assert repo.log_api_usage.call_count == 2
        for call in repo.log_api_usage.call_args_list:
            assert call[1]["status"] == "error"
            assert "API down" in call[1]["error_message"]

    @pytest.mark.asyncio
    @patch("influence_monitor.email.resend_provider.resend")
    async def test_no_logging_without_repo(self, mock_resend: MagicMock) -> None:
        """No repo provided → send still works, no logging error."""
        mock_resend.Emails.send.return_value = {"id": _MSG_ID}

        provider = ResendEmailProvider(_settings(), repo=None)
        msg_id = await provider.send(_TO, _SUBJECT, _HTML, _TEXT)

        assert msg_id == _MSG_ID


# ------------------------------------------------------------------
# SendGrid stub
# ------------------------------------------------------------------


class TestSendGridStub:
    @pytest.mark.asyncio
    async def test_raises_not_implemented(self) -> None:
        provider = SendGridEmailProvider()
        with pytest.raises(NotImplementedError, match="stub"):
            await provider.send(_TO, _SUBJECT, _HTML, _TEXT)


# ------------------------------------------------------------------
# EMAIL_REGISTRY
# ------------------------------------------------------------------


class TestEmailRegistry:
    def test_resend_registered(self) -> None:
        assert "resend" in EMAIL_REGISTRY
        assert EMAIL_REGISTRY["resend"] is ResendEmailProvider

    def test_sendgrid_registered(self) -> None:
        assert "sendgrid" in EMAIL_REGISTRY
        assert EMAIL_REGISTRY["sendgrid"] is SendGridEmailProvider
