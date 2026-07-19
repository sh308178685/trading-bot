#!/usr/bin/env bash
# 马丁策略状态检测并通知（推送到微信）
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR" || exit 1
LOGDIR="${MARTIN_NOTIFY_LOG_DIR:-$ROOT_DIR/logs}"
mkdir -p "$LOGDIR"
CRON_LOG="$LOGDIR/cron.log"
ERROR_LOG="$LOGDIR/cron-error.log"

# 防止 cron 上一轮仍在运行时并发发送重复通知。
if command -v flock >/dev/null 2>&1; then
    exec 9>"$LOGDIR/check-and-notify.lock"
    flock -n 9 || exit 0
fi

rotate_log() {
    local file="$1"
    local max_bytes="${MARTIN_NOTIFY_LOG_MAX_BYTES:-5242880}"
    if [ -f "$file" ] && [ "$(wc -c < "$file")" -ge "$max_bytes" ]; then
        mv -f "$file" "$file.1"
    fi
}
rotate_log "$CRON_LOG"
rotate_log "$ERROR_LOG"

# 获取状态报告（stderr 单独记录日志，不混入报告）
REPORT=$(python3 scripts/check-martin-status.py --events-only 2>>"$ERROR_LOG")
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "[$(date --iso-8601=seconds)] Status check failed (exit=$EXIT_CODE)" >> "$CRON_LOG"
    exit 0
fi

if [ -z "$REPORT" ]; then
    echo "[$(date --iso-8601=seconds)] No events to report" >> "$CRON_LOG"
    exit 0
fi

echo "[$(date --iso-8601=seconds)] Event detected, sending to WeChat..." >> "$CRON_LOG"

# 发送到微信
OPENCLAW_BIN="${OPENCLAW_BIN:-$(command -v openclaw 2>/dev/null || true)}"
NOTIFY_CHANNEL="${MARTIN_NOTIFY_CHANNEL:-openclaw-weixin}"
NOTIFY_TARGET="${MARTIN_NOTIFY_TARGET:-o9cq8049v2_BFONUYfRp1yafZG7E@im.wechat}"
if [ -z "$OPENCLAW_BIN" ] || [ ! -x "$OPENCLAW_BIN" ]; then
    echo "[$(date --iso-8601=seconds)] Notification failed: openclaw executable not found" >> "$CRON_LOG"
    exit 0
fi

"$OPENCLAW_BIN" message send \
    --channel "$NOTIFY_CHANNEL" \
    --target "$NOTIFY_TARGET" \
    --message "$REPORT" >> "$CRON_LOG" 2>&1
