#!/usr/bin/env python3
"""Runtime entrypoint that adds exchange-specific safety extensions."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from trading.gate_order_trade_safety import GateOrderTradeSafetyMixin

CORE_PATH = ROOT / "scripts" / "martin-bot.py"
spec = importlib.util.spec_from_file_location("martin_bot_core", CORE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError(f"Unable to load martingale core from {CORE_PATH}")
core = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = core
spec.loader.exec_module(core)


class MartinBot(GateOrderTradeSafetyMixin, core.MartinBot):
    """Core bot plus Gate order-scoped fill verification."""


if __name__ == "__main__":
    core.configure_runtime_logging()
    MartinBot().main()
