#!/usr/bin/env python3
"""Convenience launcher for the martingale dashboard."""

from __future__ import annotations

import argparse
import os
import socket
import sys
import threading
import time
import webbrowser
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from trading.runtime_config import load_runtime_config

CONFIG_FILE = ROOT / "config" / "config.json"


def load_config() -> dict:
    return load_runtime_config(CONFIG_FILE, default={})


def apply_dashboard_env(config: dict) -> tuple[str, int]:
    env_mapping = {
        "dashboardUsername": "MARTIN_DASHBOARD_USERNAME",
        "dashboardPassword": "MARTIN_DASHBOARD_PASSWORD",
        "dashboardSecret": "MARTIN_DASHBOARD_SECRET",
    }
    for config_key, env_key in env_mapping.items():
        value = config.get(config_key)
        if value and not os.getenv(env_key):
            os.environ[env_key] = str(value)

    host = str(config.get("dashboardHost", "0.0.0.0"))
    port = int(config.get("dashboardPort", 8765))
    return host, port


def detect_lan_ip() -> str:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    except OSError:
        try:
            return socket.gethostbyname(socket.gethostname())
        except OSError:
            return "127.0.0.1"
    finally:
        sock.close()


def schedule_browser_open(url: str, delay_sec: float) -> None:
    def _open():
        time.sleep(delay_sec)
        webbrowser.open(url)

    thread = threading.Thread(target=_open, daemon=True)
    thread.start()


def build_parser():
    parser = argparse.ArgumentParser(description="Start the martingale dashboard with one command.")
    parser.add_argument("--host", help="Override host from config.")
    parser.add_argument("--port", type=int, help="Override port from config.")
    parser.add_argument("--debug", action="store_true", help="Run Flask in debug mode.")
    parser.add_argument("--no-browser", action="store_true", help="Do not auto-open the browser.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Print launch information and exit without starting the server.",
    )
    return parser


def main():
    args = build_parser().parse_args()
    config = load_config()
    default_host, default_port = apply_dashboard_env(config)

    host = args.host or default_host
    port = args.port or default_port

    local_url = f"http://127.0.0.1:{port}"
    lan_url = f"http://{detect_lan_ip()}:{port}"
    auth_enabled = bool(os.getenv("MARTIN_DASHBOARD_PASSWORD"))
    username = os.getenv("MARTIN_DASHBOARD_USERNAME") or "admin"

    print("=" * 68)
    print("Martin Command Deck Launcher")
    print(f"Workspace : {ROOT}")
    print(f"Local URL : {local_url}")
    print(f"LAN URL   : {lan_url}")
    print(f"Bind      : {host}:{port}")
    print(f"Auth      : {'enabled' if auth_enabled else 'disabled'}")
    if auth_enabled:
        print(f"Username  : {username}")
    else:
        print("Warning   : dashboard password not set, access is open.")
    print("=" * 68)

    if args.check:
        return 0

    if not args.no_browser:
        schedule_browser_open(local_url, delay_sec=1.2)

    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    from dashboard.app import app

    app.run(host=host, port=port, debug=args.debug, use_reloader=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
