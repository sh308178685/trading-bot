---
name: bitget-pro-trader
description: Bitget and WEEX martingale strategy. Includes the running bot and a status checker.
version: 1.2.0
author: Codex
tags: [trading, crypto, bitget, weex, martingale]
---

# Multi-exchange Martin Bot

This project supports the martingale strategy on Bitget and WEEX USDT futures.

## What remains

- `scripts/martin-bot.py`: main strategy runner
- `scripts/check-martin-status.py`: current status inspector, supports `--json`
- `config/config.json`: live config
- `config/config.example.json`: martingale config template
- `data/martin-runtime.json`: runtime snapshot
- `data/martin-state.json`: last status snapshot for event detection

## Quick start

Install isolated runtime dependencies once (recommended):

```bash
python -m pip install --upgrade --target .deps-local -r requirements.txt
```

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

To use WEEX, set `exchange` to `weex`, `sandbox` to `false`, and use a unified
USDT futures symbol such as `BTC/USDT:USDT` in `config/config.json`. Keep the
credentials out of that file when possible:

```text
set WEEX_API_KEY=your_key
set WEEX_SECRET_KEY=your_secret
set WEEX_PASSPHRASE=your_passphrase
```

WEEX uses the same `wsEnabled`, `wsPublicEnabled`, `wsPrivateEnabled`,
`wsFreshSeconds`, and `wsReconnectDelay` switches as Bitget. Its V3 public
WebSocket supplies ticker/depth/candles and the authenticated private socket
supplies account/position/fill/order updates; REST remains the authoritative
fallback when a cache is missing or stale.

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

- Bitget supports `sandbox: true` demo mode and `sandbox: false` live mode.
- WEEX currently supports only `sandbox: false`: WEEX's official V3 demo API does not expose the cancel-order, conditional-order, or TP/SL endpoints needed to manage this strategy safely, so the adapter fails closed in demo mode.
- WEEX currently uses REST polling; the dashboard reports `rest` transport.
- WEEX startup checks the official API-trading-symbol list and refuses to run when the configured contract is not API-enabled.
- Start Bitget changes in demo mode. For WEEX, use a dedicated low-risk API key and manually verify the account and symbol before enabling the bot.
- Keep exchange credentials in environment variables where possible; `config/config.json` is ignored and must never be committed. Bitget uses `BITGET_*`; WEEX uses `WEEX_API_KEY`, `WEEX_SECRET_KEY`, and `WEEX_PASSPHRASE` (or the documented `MARTIN_*` aliases).
- The repository's pre-push hook rejects any ref whose history contains `config/config.json`. Rotate exposed keys and rewrite that local history before publishing it.
