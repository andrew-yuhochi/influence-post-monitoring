"""Resend email provider — primary email delivery for the pipeline.

Uses the Resend Python SDK to send HTML + plain-text emails.  On failure,
retries exactly once after 30 seconds, then raises ``EmailDeliveryError``
to avoid duplicate emails from over-retrying.

Every send attempt (success or failure) is logged to the ``api_usage``
table when a ``DatabaseRepository`` is provided.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import resend

from influence_monitor.config import Settings
from influence_monitor.email.base import EmailDeliveryError, EmailProvider

if TYPE_CHECKING:
    from influence_monitor.db.repository import DatabaseRepository

logger = logging.getLogger(__name__)

_RETRY_DELAY_SECONDS = 30


class ResendEmailProvider(EmailProvider):
    """Resend-backed email provider with single-retry and usage logging.

    Usage::

        provider = ResendEmailProvider(settings, repo=repo)
        message_id = await provider.send(
            to="user@example.com",
            subject="Morning Watchlist",
            html_body="<h1>Signals</h1>",
            text_body="Signals...",
        )
    """

    def __init__(
        self,
        settings: Settings,
        repo: DatabaseRepository | None = None,
    ) -> None:
        self._sender_email = settings.sender_email
        self._repo = repo
        resend.api_key = settings.resend_api_key

    async def send(
        self,
        to: str,
        subject: str,
        html_body: str,
        text_body: str,
    ) -> str:
        """Send an email via Resend.  Retries once on failure.

        Returns the Resend message ID on success.

        Raises:
            EmailDeliveryError: Both attempts failed.
        """
        last_error: Exception | None = None

        for attempt in range(2):
            if attempt == 1:
                logger.info(
                    "Resend send failed — retrying in %ds", _RETRY_DELAY_SECONDS,
                )
                time.sleep(_RETRY_DELAY_SECONDS)

            start = time.monotonic()
            try:
                resp = resend.Emails.send(
                    {
                        "from": self._sender_email,
                        "to": [to],
                        "subject": subject,
                        "html": html_body,
                        "text": text_body,
                    }
                )
                elapsed_ms = int((time.monotonic() - start) * 1000)

                message_id: str = resp.get("id", "") if isinstance(resp, dict) else getattr(resp, "id", "")

                await self._log_usage(
                    endpoint="Emails.send",
                    latency_ms=elapsed_ms,
                    status="ok",
                )
                logger.info(
                    "Email sent via Resend: to=%s subject=%r message_id=%s (%dms)",
                    to, subject, message_id, elapsed_ms,
                )
                return message_id

            except Exception as exc:
                elapsed_ms = int((time.monotonic() - start) * 1000)
                last_error = exc
                await self._log_usage(
                    endpoint="Emails.send",
                    latency_ms=elapsed_ms,
                    status="error",
                    error_message=str(exc),
                )
                logger.warning(
                    "Resend send attempt %d failed (%dms): %s",
                    attempt + 1, elapsed_ms, exc,
                )

        raise EmailDeliveryError(
            f"Resend delivery failed after 2 attempts: {last_error}"
        )

    async def _log_usage(
        self,
        endpoint: str,
        latency_ms: int,
        status: str,
        error_message: str | None = None,
    ) -> None:
        """Log the API call to the api_usage table if a repo is available."""
        if self._repo is None:
            return
        try:
            await self._repo.log_api_usage(
                provider="resend",
                endpoint=endpoint,
                latency_ms=latency_ms,
                status=status,
                error_message=error_message,
            )
        except Exception as exc:
            logger.warning("Failed to log Resend API usage: %s", exc)
