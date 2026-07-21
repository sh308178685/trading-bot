"""Factory helpers for exchange adapters."""

from __future__ import annotations

from .base import ExchangeAdapter
from .bitget import BitgetExchangeAdapter
from .weex import WeexExchangeAdapter


def create_exchange_adapter(config: dict[str, object]) -> ExchangeAdapter:
    exchange_name = str(config.get("exchange", "bitget")).strip().lower()
    if exchange_name == "bitget":
        return BitgetExchangeAdapter(config)
    if exchange_name == "weex":
        return WeexExchangeAdapter(config)
    raise ValueError(f"Unsupported exchange: {exchange_name}")
