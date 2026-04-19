# Fallback WhatsApp delivery via CallMeBot (no SLA; best effort only).
import logging
import os
from urllib.parse import quote

import httpx

from influence_monitor.delivery.base import MessageDelivery

logger = logging.getLogger(__name__)

_CALLMEBOT_ENDPOINT = "https://api.callmebot.com/whatsapp.php"


class CallMeBotDelivery(MessageDelivery):
    def __init__(self) -> None:
        self._phone = os.environ["CALLMEBOT_PHONE"]
        self._api_key = os.environ["CALLMEBOT_API_KEY"]

    def send(self, text: str) -> bool:
        url = (
            f"{_CALLMEBOT_ENDPOINT}"
            f"?phone={self._phone}"
            f"&text={quote(text)}"
            f"&apikey={self._api_key}"
        )
        try:
            response = httpx.get(url, timeout=15)
            if response.is_success:
                logger.info("CallMeBot message sent", extra={"status": response.status_code})
                return True
            logger.error(
                "CallMeBot non-success response",
                extra={"status": response.status_code, "body": response.text[:200]},
            )
            return False
        except httpx.RequestError as exc:
            logger.error("CallMeBot network error", extra={"error": str(exc)})
            return False
