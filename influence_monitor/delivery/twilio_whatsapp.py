# Primary WhatsApp delivery via Twilio REST API.
# Run as __main__ with --test-message <text> to send a real message.
import argparse
import logging

from twilio.base.exceptions import TwilioRestException
from twilio.rest import Client

from influence_monitor.config import Settings
from influence_monitor.delivery.base import MessageDelivery

logger = logging.getLogger(__name__)


class TwilioWhatsAppDelivery(MessageDelivery):
    def __init__(self) -> None:
        settings = Settings()
        self._sid = settings.twilio_account_sid
        self._token = settings.twilio_auth_token
        self._from = settings.twilio_whatsapp_from
        self._to = settings.recipient_phone_e164

    def send(self, text: str) -> bool:
        if not text or not text.strip():
            logger.warning("Skipping empty message body")
            return True
        try:
            client = Client(self._sid, self._token)
            message = client.messages.create(
                from_=f"whatsapp:{self._from}",
                to=f"whatsapp:{self._to}",
                body=text,
            )
            logger.info("Twilio message sent", extra={"sid": message.sid})
            return True
        except TwilioRestException as exc:
            logger.error(
                "Twilio API error: status=%s code=%s detail=%s body_len=%d",
                exc.status, exc.code, exc.msg, len(text),
            )
            return False
        except Exception as exc:
            logger.error("Twilio unexpected error: %s body_len=%d", exc, len(text))
            return False


if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Load .env when running as __main__
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).parents[2] / ".env")

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    parser = argparse.ArgumentParser(description="Send a test WhatsApp via Twilio")
    parser.add_argument("--test-message", required=True, help="Message body to send")
    args = parser.parse_args()

    delivery = TwilioWhatsAppDelivery()
    success = delivery.send(args.test_message)
    sys.exit(0 if success else 1)
