# Gate risk-managed strategy upgrade

This is a conservative change of strategy, not an optimized-return claim or a promise that losses cannot occur. It intentionally removes martingale averaging. All changes are on the review branch; no exchange account has been accessed and no live deployment has been performed.

## Entry point and scope

Keep using `python scripts/martin-bot.py --run` or the existing launcher. The previous 310,703-byte engine is preserved byte-for-byte as `trading/martin_core.py`; the public entry point composes it with `GateRiskMixin`. Do not run the internal core module as a strategy entry point. Gate defaults to `risk_enabled: true`, including existing configurations that omit the new key. Bitget is unchanged. Explicitly setting `risk_enabled: false` restores the old high-risk behavior and is not a risk fix.

The overlay targets the existing Gate USDT linear perpetual adapter and its base-asset quantity convention. Use a dedicated futures account, a single bot process and a separate directory for demo testing. Do not combine manual trades, multiple symbols/bots, deposits or withdrawals with the equity-based risk ledger. Unknown account schemas block new entries rather than guessing equity. Other account products such as portfolio/unified margin have not been validated.

## Default behavior

- No additional martingale layers, including phase-2 rescue orders. Existing owned entry/add orders are canceled and their fill identities retained for reconciliation. Legacy layer settings remain for compatibility but do not override the risk gate.
- First-order margin is capped at the smaller of the existing setting and 2.5% of equity. The submission check also limits notional exposure. With 3x leverage the first-order notional cap is 7.5% of equity, not 2.5%. Inherited positions above 50% notional/equity trigger a full-exit request. Orders below exchange/bot minima are skipped, never rounded upward to meet a risk budget.
- The 1-hour trend filter uses the last two CLOSED bars with aligned fast/slow EMAs and ADX >= 25 to block opposite entries. Missing, nonfinite or stale indicators block entries. This is a lagging filter, not a prediction guarantee.
- Initial price-stop distance is 3 times 1-hour ATR, clamped to 0.6%-2% of entry price; missing ATR falls back to 2%. This anchor is recorded once per cycle and is not widened after an average-price change or restart. Existing tighter protection is retained.
- A separate per-cycle equity loss trigger is 2%, including a nominal 0.2% notional fee/slippage reserve in stop sizing. These are trigger budgets, not guaranteed maximum realized losses. Gate equity is raw futures `total + unrealised_pnl`; the generic wallet-total field is not silently substituted.
- UTC daily loss of 4% pauses entries for at least 24 hours and requests exit of an active cycle. The daily reference is the first valid observation that UTC day, not reconstructed midnight equity. An 8% decline from the persisted equity high-water mark latches a manual-review halt even after recovery.
- Losing cycles cool down for 4 hours; other completed cycles cool down for 15 minutes. Three consecutive losing cycles pause for at least 24 hours. Holding time beyond 24 hours requests exit. Where an existing position lacks a reliable opening timestamp, the age starts from adoption; old holding duration is not invented.

## Execution changes

Protection is requested as soon as an authoritative fill/position is observed, without waiting for profitability. The post-entry check handles immediately visible fills, and the normal loop sleep is capped at 5 seconds. This is NOT an atomic entry-plus-stop bracket: delayed position publication, API latency and the time before stop acknowledgement still create an exposure window. An acknowledged native stop can operate without the local bot, subject to exchange behavior.

Trailing protection no longer requires an extra 0.001 leveraged-return buffer. It tightens from the best return using the existing drawdown/lock ratio and tick precision. If price has already crossed the target, the bot requests an exit instead of submitting an invalid trigger or waiting for recovery. A missing known stop also latches an exit.

Stop replacements are submitted before old stops are canceled. All are reduce-only. Cancellation failures retain old identities for cleanup; they are not forgotten. Stop-creation identities are persisted before the request. An uncertain creation response is reconciled by identity instead of blindly duplicated; protection failure requests exit.

Software exits use reduce-only market orders instead of waiting five seconds for a limit close. Exit intent survives partial fills, API errors and process restarts. A retry requires terminal-order evidence plus a consistent remaining position. An unresolved request may therefore require operator intervention; the bot does not trade through uncertainty. Native protection is not deliberately canceled before closing is confirmed. Foreign entry orders are not canceled and force a review halt.

The native Gate adapter uses quantity-based stop orders, so protection is refreshed when observed position size increases. A fill can still occur between REST observations. Stop prices and market orders can slip, be rejected, hit exchange limits or only partly fill. Funding and actual fees may differ from the reserve. No settings eliminate liquidation or exchange/outage risk.

## Persistent state and observability

The overlay records `data/martin-risk-gate-{demo|live}-{symbol}.json`, separately from the legacy cycle state. Atomic writes preserve stop/exit identities, cooldown, loss streak and equity baselines. Corrupt/unsupported state aborts initialization rather than silently clearing history. Read-only construction does not rewrite this file. Do not delete it to bypass a halt. Review the exchange's positions and orders, preserve an audit backup, and resolve the recorded reason before any deliberate reset.

The live snapshot strategy section includes `risk_enabled`, `risk_adds_disabled`, `risk_halted`, `risk_paused_until`, `risk_exit_reason` and `risk_cycle`. The existing dashboard rendering itself has not been redesigned.

## Validation and rollout

The new `tests/test_risk_guard.py` contains 40 offline regression cases covering long/short adverse paths, protection before profit, the buffer gap, base units, fixed stops, entry limits, missing data, cooldown, daily/high-water breakers, persisted state, partial/uncertain executions and stop replacement ordering. These are safety tests, NOT a historical profit backtest or a Gate testnet end-to-end execution test. The unchanged existing regression suite is run alongside them in GitHub Actions.

Before live use: review the diff and CI results; back up configuration/runtime data; validate in a separate Gate demo directory/account with fresh demo credentials; check that reduce-only mark-price stop orders are visible on Gate; exercise cancellation, partial-fill and restart handling. Do not overwrite a live config with the example. The example remains `sandbox: true`; actual local credentials/account configuration are not modified by this change.

Deploying this version onto an existing losing or oversized position may request an immediate full close. It does not promise to recover that position's previous loss. Prefer a controlled, flat-state migration instead of an unattended hot update. The 2%, 4% and 8% values are conservative starting policies, not empirically optimal parameters; forward testing and execution review are still required.

## Credential incident

The previous public Gate example contained non-placeholder credential strings. They are blanked in the example in this branch and were not used. File cleanup does not revoke a key or erase Git history. If those keys were ever valid, revoke and replace them at Gate and review account activity; do not paste replacements into chat or commit them. Historical cleanup is a separate action and has not been performed.

## References

Gate API v4 perpetual-futures documentation: https://www.gate.com/docs/developers/apiv4/en/futures/

CCXT manual (OHLCV incomplete candles and exchange-specific order semantics): https://github.com/ccxt/ccxt/wiki/Manual
