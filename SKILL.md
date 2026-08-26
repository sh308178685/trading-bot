---
name: bitget-pro-trader
description: Bitget and Gate.io martingale strategy. Includes the running bot and a status checker.
version: 1.1.0
author: Codex
tags: [trading, crypto, bitget, gateio, martingale]
---

# Bitget Martin Bot

This project supports Bitget and Gate.io USDT perpetual martingale strategies.

## What remains

- `scripts/martin-bot.py`: main strategy runner
- `scripts/check-martin-status.py`: current status inspector, supports `--json`
- `config/config.json`: live config
- `config/config.example.json`: Bitget martingale template
- `config/config.gate.example.json`: Gate.io USDT perpetual template
- `data/martin-runtime.json`: runtime snapshot
- `data/martin-state.json`: last status snapshot for event detection

## Quick start

Run the bot:

```bash
python scripts/martin-bot.py --run
```

One-click Windows launcher for the bot:

```text
Double-click start-martin.bat
```

Check current status:

```bash
python scripts/check-martin-status.py
```

Get JSON output for dashboards or a future web UI:

```bash
python scripts/check-martin-status.py --json
```

Run the web dashboard:

```bash
python scripts/web-dashboard.py --host 0.0.0.0 --port 8765
```

One-click Windows launcher:

```text
Double-click start-dashboard.bat
```

Recommended secure launch:

```bash
set MARTIN_DASHBOARD_USERNAME=admin
set MARTIN_DASHBOARD_PASSWORD=your_password
set MARTIN_DASHBOARD_SECRET=your_random_secret
python scripts/web-dashboard.py --host 0.0.0.0 --port 8765
```

Then open:

```text
http://127.0.0.1:8765
```

To open it from a phone on the same network, use your computer LAN IP:

```text
http://YOUR_PC_IP:8765
```

## Notes

- Set `exchange` to `bitget` or `gate`. Gate requires an API v4 key with futures permissions.
- `sandbox: true` means the selected exchange's demo/testnet mode.
- `sandbox: false` means live trading.
- Start with demo mode before touching live capital.
