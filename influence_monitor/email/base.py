"""Email provider interface and shared exceptions.

All email delivery implementations (Resend, SendGrid, SES) implement
the ``EmailProvider`` ABC.  The pipeline selects the active provider
via ``settings.email_provider`` and the ``EMAIL_REGISTRY``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class EmailDeliveryError(Exception):
    """Raised when email delivery fails after all retries are exhausted."""


class EmailProvider(ABC):
    """Abstract contract for email delivery.

    Implementations: ResendEmailProvider (primary), SendGridEmailProvider (stub).
    """

    @abstractmethod
    async def send(
        self,
        to: str,
        subject: str,
        html_body: str,
        text_body: str,
    ) -> str:
        """Send an email and return the provider's message ID.

        Args:
            to: Recipient email address.
            subject: Email subject line.
            html_body: HTML-formatted email body.
            text_body: Plain-text fallback body.

        Returns:
            Message ID string from the provider.

        Raises:
            EmailDeliveryError: Delivery failed after retry.
        """
        ...
