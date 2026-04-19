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
                "Twilio API error",
                extra={"status": exc.status, "code": exc.code, "detail": exc.msg},
            )
            return False
        except Exception as exc:
            logger.error("Twilio unexpected error", extra={"error": str(exc)})
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
