"""Email provider registry — maps config keys to EmailProvider implementations.

The pipeline orchestrator selects the active provider via
``settings.email_provider`` (default: ``"resend"``).
"""

from __future__ import annotations

from influence_monitor.email.base import EmailProvider
from influence_monitor.email.resend_provider import ResendEmailProvider
from influence_monitor.email.sendgrid_provider import SendGridEmailProvider

EMAIL_REGISTRY: dict[str, type[EmailProvider]] = {
    "resend": ResendEmailProvider,
    "sendgrid": SendGridEmailProvider,
}
