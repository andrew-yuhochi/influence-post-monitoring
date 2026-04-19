# Unit tests for delivery/: TwilioWhatsAppDelivery and CallMeBotDelivery.
# All external HTTP and Twilio SDK calls are mocked — no credentials required.
import os
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers — set required env vars before importing delivery classes
# ---------------------------------------------------------------------------

_TWILIO_ENV = {
    "TWILIO_ACCOUNT_SID": "ACtest000",
    "TWILIO_AUTH_TOKEN": "token000",
    "TWILIO_WHATSAPP_FROM": "+14155238886",
    "RECIPIENT_PHONE_E164": "+16471234567",
}
_CALLMEBOT_ENV = {
    "CALLMEBOT_PHONE": "+16471234567",
    "CALLMEBOT_API_KEY": "testkey123",
}


# ---------------------------------------------------------------------------
# TwilioWhatsAppDelivery
# ---------------------------------------------------------------------------


class TestTwilioWhatsAppDelivery:
    def _make_delivery(self) -> "TwilioWhatsAppDelivery":
        from influence_monitor.delivery.twilio_whatsapp import TwilioWhatsAppDelivery

        return TwilioWhatsAppDelivery()

    def test_send_success_returns_true(self) -> None:
        with patch.dict(os.environ, _TWILIO_ENV):
            with patch("influence_monitor.delivery.twilio_whatsapp.Client") as mock_client_cls:
                mock_message = MagicMock()
                mock_message.sid = "SM123"
                mock_client_cls.return_value.messages.create.return_value = mock_message

                delivery = self._make_delivery()
                result = delivery.send("hello world")

        assert result is True
        mock_client_cls.return_value.messages.create.assert_called_once_with(
            from_="whatsapp:+14155238886",
            to="whatsapp:+16471234567",
            body="hello world",
        )

    def test_send_twilio_4xx_returns_false(self) -> None:
        from twilio.base.exceptions import TwilioRestException

        with patch.dict(os.environ, _TWILIO_ENV):
            with patch("influence_monitor.delivery.twilio_whatsapp.Client") as mock_client_cls:
                mock_client_cls.return_value.messages.create.side_effect = TwilioRestException(
                    status=400,
                    uri="/Messages",
                    msg="Bad request",
                )

                delivery = self._make_delivery()
                result = delivery.send("hello")

        assert result is False

    def test_send_twilio_5xx_returns_false(self) -> None:
        from twilio.base.exceptions import TwilioRestException

        with patch.dict(os.environ, _TWILIO_ENV):
            with patch("influence_monitor.delivery.twilio_whatsapp.Client") as mock_client_cls:
                mock_client_cls.return_value.messages.create.side_effect = TwilioRestException(
                    status=503,
                    uri="/Messages",
                    msg="Service unavailable",
                )

                delivery = self._make_delivery()
                result = delivery.send("hello")

        assert result is False

    def test_send_unexpected_exception_returns_false(self) -> None:
        with patch.dict(os.environ, _TWILIO_ENV):
            with patch("influence_monitor.delivery.twilio_whatsapp.Client") as mock_client_cls:
                mock_client_cls.return_value.messages.create.side_effect = RuntimeError("network")

                delivery = self._make_delivery()
                result = delivery.send("hello")

        assert result is False


# ---------------------------------------------------------------------------
# CallMeBotDelivery
# ---------------------------------------------------------------------------


class TestCallMeBotDelivery:
    def _make_delivery(self) -> "CallMeBotDelivery":
        from influence_monitor.delivery.callmebot import CallMeBotDelivery

        return CallMeBotDelivery()

    def test_send_success_returns_true(self) -> None:
        with patch.dict(os.environ, _CALLMEBOT_ENV):
            with patch("influence_monitor.delivery.callmebot.httpx.get") as mock_get:
                mock_response = MagicMock()
                mock_response.is_success = True
                mock_response.status_code = 200
                mock_get.return_value = mock_response

                delivery = self._make_delivery()
                result = delivery.send("test message")

        assert result is True
        called_url = mock_get.call_args[0][0]
        assert "phone=%2B16471234567" in called_url or "phone=+16471234567" in called_url
        assert "apikey=testkey123" in called_url
        assert "text=" in called_url

    def test_send_non_success_returns_false(self) -> None:
        with patch.dict(os.environ, _CALLMEBOT_ENV):
            with patch("influence_monitor.delivery.callmebot.httpx.get") as mock_get:
                mock_response = MagicMock()
                mock_response.is_success = False
                mock_response.status_code = 500
                mock_response.text = "Internal Server Error"
                mock_get.return_value = mock_response

                delivery = self._make_delivery()
                result = delivery.send("test")

        assert result is False

    @pytest.mark.skip(
        reason="CALLMEBOT_API_KEY not yet captured — requires the user to complete PRE-001 "
        "and add CALLMEBOT_API_KEY to .env before this integration path can be exercised live."
    )
    def test_send_integration_live(self) -> None:
        """Integration test: sends a real CallMeBot message using .env credentials."""
        from influence_monitor.delivery.callmebot import CallMeBotDelivery

        delivery = CallMeBotDelivery()
        assert delivery.send("Integration test from influence-post-monitoring") is True


# ---------------------------------------------------------------------------
# DELIVERY_REGISTRY
# ---------------------------------------------------------------------------


class TestDeliveryRegistry:
    def test_registry_keys(self) -> None:
        from influence_monitor.delivery.registry import DELIVERY_REGISTRY
        from influence_monitor.delivery.callmebot import CallMeBotDelivery
        from influence_monitor.delivery.twilio_whatsapp import TwilioWhatsAppDelivery

        assert DELIVERY_REGISTRY["twilio"] is TwilioWhatsAppDelivery
        assert DELIVERY_REGISTRY["callmebot"] is CallMeBotDelivery
