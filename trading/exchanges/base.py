"""Base interfaces for exchange integrations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ExchangeAdapter(ABC):
    """Small abstraction layer for strategy and dashboard code."""

    def __init__(self, config: dict[str, Any]):
        self.config = config

    @property
    @abstractmethod
    def name(self) -> str:
        """Human readable exchange name."""

    def start(self) -> None:
        """Start background services such as WebSocket listeners."""

    def close(self) -> None:
        """Close background services and network clients."""

    def ws_status(self) -> dict[str, Any]:
        """Return transport status for UI/debugging."""
        return {"enabled": False, "transport": "rest"}

