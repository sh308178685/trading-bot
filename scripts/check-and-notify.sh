#!/bin/bash
# 马丁策略状态检测并通知（推送到微信）
cd /root/clawd/skills/bitget-pro-trader
LOGDIR="/root/clawd/skills/bitget-pro-trader/logs"
mkdir -p "$LOGDIR"

# 获取状态报告（stderr 单独记录日志，不混入报告）
REPORT=$(python3 scripts/check-martin-status.py --events-only 2>>"$LOGDIR/cron-error.log")
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "[$(date)] Status check failed (exit=$EXIT_CODE)" >> "$LOGDIR/cron.log"
    exit 0
fi

if [ -z "$REPORT" ]; then
    echo "[$(date)] No events to report" >> "$LOGDIR/cron.log"
    exit 0
fi

echo "[$(date)] Event detected, sending to WeChat..." >> "$LOGDIR/cron.log"

# 发送到微信
/root/.nvm/versions/node/v22.22.0/bin/openclaw message send \
    --channel openclaw-weixin \
    --target o9cq8049v2_BFONUYfRp1yafZG7E@im.wechat \
    --message "$REPORT" 2>&1 >> "$LOGDIR/cron.log"
