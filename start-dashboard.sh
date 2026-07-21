#!/usr/bin/env sh
set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$PROJECT_ROOT"

if [ ! -x .venv/bin/python ]; then
    PYTHON_BIN=$(command -v python3 || command -v python || true)
    if [ -z "$PYTHON_BIN" ]; then
        echo "Python 3 was not found in PATH." >&2
        exit 1
    fi
    echo "Creating virtual environment in .venv..."
    "$PYTHON_BIN" -m venv .venv
    .venv/bin/python -m pip install --upgrade pip
    .venv/bin/python -m pip install -r requirements.txt
fi

exec .venv/bin/python scripts/launch-dashboard.py "$@"
