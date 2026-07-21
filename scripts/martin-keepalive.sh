#!/usr/bin/env bash
# 马丁机器人 + 面板 保活脚本
set -u

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
PIDFILE="$PROJECT_ROOT/data/martin.pid"
DASH_PIDFILE="$PROJECT_ROOT/data/martin-dashboard.pid"
LOGDIR="$PROJECT_ROOT/data/logs"
VENV_PYTHON="$PROJECT_ROOT/.venv/bin/python"

if [[ ! -x "$VENV_PYTHON" ]]; then
    echo "Virtual environment is missing. Run ./start-martin.sh --check first." >&2
    exit 1
fi

mkdir -p "$LOGDIR"

# --- Function: ensure process running ---
ensure_running() {
    local pidfile="$1"
    local name="$2"
    shift 2

    if [ -f "$pidfile" ]; then
        local pid=$(cat "$pidfile")
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "[$(date)] $name already running (PID=$pid)"
            return 0
        else
            echo "[$(date)] $name PID stale (PID=$pid), cleaning up"
            rm -f "$pidfile"
        fi
    fi

    cd "$PROJECT_ROOT"
    echo "[$(date)] Starting $name..."
    # Use setsid to create a new session, completely independent of caller's process group
    setsid "$@" > "$LOGDIR/${name}.log" 2>&1 &
    echo $! > "$pidfile"
    sleep 2
    if kill -0 $(cat "$pidfile") 2>/dev/null; then
        echo "[$(date)] $name started (PID=$(cat "$pidfile"))"
    else
        echo "[$(date)] ERROR: $name failed to start!"
        rm -f "$pidfile"
        return 1
    fi
}

# --- Start both ---
ensure_running "$PIDFILE" "martin-bot" "$VENV_PYTHON" scripts/launch-martin.py
ensure_running "$DASH_PIDFILE" "martin-dashboard" "$VENV_PYTHON" scripts/launch-dashboard.py --no-browser
