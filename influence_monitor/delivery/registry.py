# Registry mapping delivery provider names to their implementation classes.
from influence_monitor.delivery.base import MessageDelivery
from influence_monitor.delivery.callmebot import CallMeBotDelivery
from influence_monitor.delivery.twilio_whatsapp import TwilioWhatsAppDelivery

DELIVERY_REGISTRY: dict[str, type[MessageDelivery]] = {
    "twilio": TwilioWhatsAppDelivery,
    "callmebot": CallMeBotDelivery,
}
