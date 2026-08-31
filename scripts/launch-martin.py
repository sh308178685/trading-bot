#!/usr/bin/env python3
"""Convenience launcher for the martingale bot."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DEPS_DIR = ROOT / ".deps-local"
LEGACY_DEPS_DIR = ROOT / ".deps"

from trading.runtime_config import load_runtime_config

CONFIG_FILE = ROOT / "config" / "config.json"
BOT_SCRIPT = ROOT / "scripts" / "martin-runner.py"
LOG_DIR = ROOT / "data" / "logs"


def load_config() -> dict:
    return load_runtime_config(CONFIG_FILE, default={})


def choose_python() -> list[str]:
    if sys.platform.startswith("win") and shutil.which("py"):
        return ["py", "-3"]
    return [sys.executable]


def build_parser():
    parser = argparse.ArgumentParser(description="Start the martingale bot with one command.")
    parser.add_argument(
        "--action",
        choices=["run", "balance", "position", "trend", "sync", "help"],
        default="run",
        help="What to run. Default: run",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Print launch information and exit without starting the bot.",
    )
    return parser


def action_to_args(action: str) -> list[str]:
    mapping = {
        "run": ["--run"],
        "balance": ["--balance"],
        "position": ["--position"],
        "trend": ["--trend"],
        "sync": ["--sync"],
        "help": ["--help"],
    }
    return mapping[action]


def build_log_file() -> Path:
    return LOG_DIR / f"martin-{time.strftime('%Y%m%d-%H%M%S')}.log"


def main():
    args = build_parser().parse_args()
    config = load_config()

    symbol = config.get("symbol", "ETH/USDT:USDT")
    leverage = config.get("leverage", 3)
    timeframe = config.get("timeframe", "5m")
    exchange = config.get("exchange", "bitget")
    sandbox = bool(config.get("sandbox", True))
    ws_enabled = bool(config.get("wsEnabled", True))

    print("=" * 68)
    print("Martin Bot Launcher")
    print(f"Workspace : {ROOT}")
    print(f"Script    : {BOT_SCRIPT}")
    print(f"Exchange  : {exchange}")
    print(f"Mode      : {'sandbox' if sandbox else 'live'}")
    print(f"Transport : {'websocket+rest' if ws_enabled else 'rest only'}")
    print(f"Symbol    : {symbol}")
    print(f"Leverage  : {leverage}x")
    print(f"Timeframe : {timeframe}")
    print(f"Action    : {args.action}")
    print(f"Log Dir   : {LOG_DIR}")
    print("=" * 68)

    if args.check:
        return 0

    log_file = build_log_file()
    log_file.parent.mkdir(parents=True, exist_ok=True)
    print(f"Log File  : {log_file}")

    command = choose_python() + [str(BOT_SCRIPT)] + action_to_args(args.action)
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env["MARTIN_LOG_FILE"] = str(log_file)
    deps_paths = [path for path in (DEPS_DIR, LEGACY_DEPS_DIR) if path.exists()]
    if deps_paths:
        existing_pythonpath = env.get("PYTHONPATH", "")
        joined = os.pathsep.join(str(path) for path in deps_paths)
        env["PYTHONPATH"] = joined if not existing_pythonpath else os.pathsep.join([joined, existing_pythonpath])

    completed = subprocess.run(command, cwd=str(ROOT), env=env)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
