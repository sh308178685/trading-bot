"""Factory helpers for exchange adapters."""

from __future__ import annotations

from .bitget import BitgetExchangeAdapter
from .gate import GateExchangeAdapter


def create_exchange_adapter(config: dict[str, object]) -> BitgetExchangeAdapter | GateExchangeAdapter:
    exchange_name = str(config.get("exchange", "bitget")).strip().lower()
    if exchange_name == "bitget":
        return BitgetExchangeAdapter(config)
    if exchange_name in {"gate", "gateio", "gate.io"}:
        return GateExchangeAdapter(config)
    raise ValueError(f"Unsupported exchange: {exchange_name}")
