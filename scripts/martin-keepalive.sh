#!/bin/bash
# 马丁机器人 + 面板 保活脚本
PIDFILE="/root/clawd/skills/bitget-pro-trader/data/martin.pid"
DASH_PIDFILE="/root/clawd/skills/bitget-pro-trader/data/martin-dashboard.pid"
LOGDIR="/root/clawd/skills/bitget-pro-trader/logs"
HOMEDIR="/root/clawd/skills/bitget-pro-trader"

mkdir -p "$LOGDIR"

# --- Function: ensure process running ---
ensure_running() {
    local pidfile="$1"
    local name="$2"
    local cmd="$3"

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

    cd "$HOMEDIR"
    echo "[$(date)] Starting $name..."
    # Use setsid to create a new session, completely independent of caller's process group
    setsid $cmd > "$LOGDIR/${name}.log" 2>&1 &
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
ensure_running "$PIDFILE" "martin-bot" "python3 scripts/martin-runner.py --run"
ensure_running "$DASH_PIDFILE" "martin-dashboard" "python3 scripts/launch-dashboard.py --no-browser"
