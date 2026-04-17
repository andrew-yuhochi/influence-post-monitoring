"""SendGrid email provider stub — fallback for when Resend is unavailable.

Implements ``EmailProvider`` so it plugs into the pipeline via
``EMAIL_REGISTRY``.  Raises ``NotImplementedError`` until Phase 2
when a real SendGrid integration may be needed.
"""

from __future__ import annotations

from influence_monitor.email.base import EmailProvider


class SendGridEmailProvider(EmailProvider):
    """Stub — not yet implemented.  Swap to ``EMAIL_PROVIDER=sendgrid`` to activate."""

    async def send(
        self,
        to: str,
        subject: str,
        html_body: str,
        text_body: str,
    ) -> str:
        raise NotImplementedError(
            "SendGridEmailProvider is a stub. "
            "Install sendgrid SDK and implement before use."
        )
