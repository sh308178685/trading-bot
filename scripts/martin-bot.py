#!/usr/bin/env python3
"""Stable CLI: Gate runs the risk-managed strategy; Bitget keeps legacy behavior."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from trading.martin_core import *  # Preserve the existing module/test interface.
from trading.martin_core import MartinBot as LegacyMartinBot
from trading.risk_guard import GateRiskMixin


class MartinBot(GateRiskMixin, LegacyMartinBot):
    pass


if __name__ == '__main__':
    configure_runtime_logging()
    MartinBot().main()
