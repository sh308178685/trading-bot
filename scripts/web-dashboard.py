#!/usr/bin/env python3
"""Launch the martingale web dashboard."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.app import app


def build_parser():
    parser = argparse.ArgumentParser(description="Run the martingale strategy web dashboard.")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind. Default: 0.0.0.0")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind. Default: 8765")
    parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode.")
    return parser


def main():
    args = build_parser().parse_args()
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
