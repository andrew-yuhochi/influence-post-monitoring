# Abstract base for all WhatsApp delivery providers.
from abc import ABC, abstractmethod


class MessageDelivery(ABC):
    @abstractmethod
    def send(self, text: str) -> bool: ...
