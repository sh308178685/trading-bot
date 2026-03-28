"""Data collection and caching for the martingale dashboard."""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Callable

from trading.runtime_config import load_runtime_config


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
CONFIG_FILE = ROOT / "config" / "config.json"
RUNTIME_FILE = DATA_DIR / "martin-runtime.json"
LIVE_FILE = DATA_DIR / "martin-live.json"
HISTORY_FILE = DATA_DIR / "dashboard-history.json"
EVENTS_FILE = DATA_DIR / "dashboard-events.json"
CACHE_FILE = DATA_DIR / "dashboard-cache.json"

def load_json(path: Path, default: Any) -> Any:
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except (json.JSONDecodeError, OSError):
            return default
    return default


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_div(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return numerator / denominator


def format_timestamp(timestamp_ms: Any) -> str | None:
    if not timestamp_ms:
        return None
    try:
        return datetime.fromtimestamp(float(timestamp_ms) / 1000).isoformat(timespec="seconds")
    except (TypeError, ValueError, OSError):
        return None


def age_seconds(timestamp: Any) -> float | None:
    if not timestamp:
        return None
    try:
        return max(time.time() - datetime.fromisoformat(str(timestamp)).timestamp(), 0.0)
    except (TypeError, ValueError, OSError):
        return None


CONFIG = load_runtime_config(CONFIG_FILE, default={})


class DashboardBotProfile:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.symbol = config.get("symbol", "ETH/USDT:USDT")
        self.timeframe = config.get("timeframe", "5m")
        self.max_layers = int(config.get("max_layers", 5) or 5)
        self.leverage = safe_float(config.get("leverage", 3), 3)
        self.first_order_ratio = safe_float(config.get("first_order_ratio", 0.1), 0.1)
        self.layer_multipliers = list(config.get("layer_multipliers", [1, 1.5, 2, 2.5, 3]))
        self.level_offset_pct = safe_float(config.get("level_offset_pct", 0.001), 0.001)
        self.add_layer_base_offset_pct = safe_float(config.get("add_layer_base_offset_pct", 0.005), 0.005)
        self.rsi_threshold = safe_float(config.get("rsi_threshold", 50), 50)
        self.adx_threshold = safe_float(config.get("adx_threshold", 16.5), 16.5)
        self.trend_follow_enabled = bool(config.get("trend_follow_enabled", True))
        self.trend_follow_adx_threshold = safe_float(
            config.get("trend_follow_adx_threshold", max(self.adx_threshold + 8.0, 28.0)),
            max(self.adx_threshold + 8.0, 28.0),
        )
        self.mean_reversion_adx_max = safe_float(
            config.get("mean_reversion_adx_max", max(self.adx_threshold, 20.0)),
            max(self.adx_threshold, 20.0),
        )
        self.mean_reversion_long_rsi = safe_float(config.get("mean_reversion_long_rsi", 35.0), 35.0)
        self.mean_reversion_short_rsi = safe_float(config.get("mean_reversion_short_rsi", 65.0), 65.0)
        self.mean_reversion_entry_atr_ratio = safe_float(
            config.get("mean_reversion_entry_atr_ratio", 0.35),
            0.35,
        )
        self.exchange_name = str(config.get("exchange", "bitget")).strip().capitalize() or "Bitget"
        self.mode = "sandbox" if config.get("sandbox", True) else "live"
        self.transport = "rest"
        self.price_precision: Any = None
        self.amount_precision: Any = None
        self.latest_atr = 0.0

    def update_from_live(self, snapshot: dict[str, Any]) -> None:
        strategy = snapshot.get("strategy") or {}
        market = snapshot.get("market") or {}
        indicators = market.get("indicators") or {}
        self.exchange_name = str(strategy.get("exchange") or self.exchange_name)
        self.mode = str(strategy.get("mode") or self.mode)
        self.symbol = str(strategy.get("symbol") or self.symbol)
        self.timeframe = str(strategy.get("timeframe") or self.timeframe)
        self.max_layers = int(strategy.get("max_layers") or self.max_layers)
        self.leverage = safe_float(strategy.get("leverage", self.leverage), self.leverage)
        self.first_order_ratio = safe_float(strategy.get("first_order_ratio", self.first_order_ratio), self.first_order_ratio)
        self.layer_multipliers = list(strategy.get("layer_multipliers") or self.layer_multipliers)
        self.transport = str(strategy.get("transport") or self.transport)
        self.price_precision = strategy.get("price_precision")
        self.amount_precision = strategy.get("amount_precision")
        self.latest_atr = safe_float(indicators.get("atr", self.latest_atr))

    def strategy_payload(self) -> dict[str, Any]:
        payload = {
            "name": "Bitget 马丁策略机器人",
            "exchange": self.exchange_name,
            "mode": self.mode,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "max_layers": self.max_layers,
            "leverage": self.leverage,
            "first_order_ratio": self.first_order_ratio,
            "layer_multipliers": self.layer_multipliers,
            "trend_follow_adx_threshold": self.trend_follow_adx_threshold,
            "mean_reversion_adx_max": self.mean_reversion_adx_max,
            "transport": self.transport,
        }
        if self.price_precision is not None:
            payload["price_precision"] = self.price_precision
        if self.amount_precision is not None:
            payload["amount_precision"] = self.amount_precision
        return payload

    def _price_to_precision(self, numeric: float) -> float:
        precision = self.price_precision
        if precision in (None, ""):
            return numeric
        try:
            if isinstance(precision, int) or (
                isinstance(precision, float) and float(precision).is_integer() and 0 <= int(precision) <= 12
            ):
                return float(f"{numeric:.{int(precision)}f}")

            step = Decimal(str(precision))
            if step <= 0:
                return numeric
            value = Decimal(str(numeric))
            units = (value / step).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            return float(units * step)
        except Exception:
            return numeric

    def _select_atr_entry_price(self, side: str, layer_num: int, current_price: float, avg_price: float):
        atr = safe_float(self.latest_atr, current_price * 0.02)
        atr_mult = 1.0 + max(layer_num - 2, 0) * 0.5
        distance = atr * atr_mult
        if side == "long":
            return min(avg_price, current_price) - distance
        return max(avg_price, current_price) + distance

    def _select_fallback_entry_price(self, side: str, layer_num: int, current_price: float, avg_price: float):
        offset_pct = self.add_layer_base_offset_pct * layer_num
        if side == "long":
            return avg_price * (1 - offset_pct)
        return avg_price * (1 + offset_pct)


class DashboardService:
    def __init__(self, refresh_ttl: int = 5):
        self.refresh_ttl = refresh_ttl
        self._lock = threading.Lock()
        self._cached_snapshot: dict[str, Any] | None = load_json(CACHE_FILE, None)
        self._cached_at = 0.0
        self.bot = DashboardBotProfile(CONFIG)

    def get_snapshot(self, force: bool = False) -> dict[str, Any]:
        with self._lock:
            now = time.time()
            if (
                not force
                and self._cached_snapshot is not None
                and (now - self._cached_at) < self.refresh_ttl
            ):
                return self._cached_snapshot

            try:
                snapshot = self._build_snapshot()
            except Exception as exc:  # pragma: no cover - fallback path
                snapshot = load_json(CACHE_FILE, None)
                if snapshot is None:
                    raise
                snapshot.setdefault("server", {})
                snapshot["server"]["stale"] = True
                snapshot["server"].setdefault("warnings", [])
                snapshot["server"]["warnings"].append(f"Refresh failed: {exc}")
            else:
                save_json(CACHE_FILE, snapshot)
                self._cached_snapshot = snapshot
                self._cached_at = now

            return snapshot

    def _build_snapshot(self) -> dict[str, Any]:
        warnings: list[str] = []
        live_snapshot = load_json(LIVE_FILE, {})
        runtime = live_snapshot.get("runtime") or load_json(RUNTIME_FILE, {})
        stream = live_snapshot.get("stream") or {"enabled": False, "transport": "rest"}
        balance = live_snapshot.get("account") or self._default_balance()
        position = live_snapshot.get("position")
        open_orders = live_snapshot.get("open_orders") or []
        trades = live_snapshot.get("recent_trades") or []
        ledger = live_snapshot.get("ledger") or []
        market = live_snapshot.get("market") or self._default_market_bundle()

        self.bot.update_from_live(live_snapshot)
        live_warnings = live_snapshot.get("warnings") or []
        warnings.extend(str(item) for item in live_warnings if item)

        snapshot_age = age_seconds(live_snapshot.get("timestamp"))
        stale = False
        if not live_snapshot:
            warnings.append("尚未发现本地实时快照，请先启动马丁机器人。")
            stale = True
        elif snapshot_age is not None and snapshot_age > max(self.refresh_ttl * 3, 15):
            warnings.append(f"本地快照已过期 {snapshot_age:.1f} 秒，当前展示的是最后一次机器人状态。")
            stale = True

        current_price = safe_float(
            market["indicators"].get("price")
            or market.get("ticker", {}).get("last")
            or market.get("support_resistance", {}).get("current_price")
        )
        performance = self._build_performance(balance, position, trades, ledger, current_price)
        trade_analytics = self._build_trade_analytics(trades)
        order_book = self._build_orders(open_orders, current_price)
        ladder = self._build_ladder(runtime, balance, position, current_price, market)
        next_action = self._build_next_action(runtime, position, current_price, market)

        snapshot = {
            "timestamp": live_snapshot.get("timestamp") or datetime.now().isoformat(timespec="seconds"),
            "server": {
                "refresh_ttl_sec": self.refresh_ttl,
                "stale": stale,
                "source": "local_snapshot",
                "warnings": warnings,
            },
            "strategy": {**self.bot.strategy_payload(), **(live_snapshot.get("strategy") or {})},
            "runtime": {
                "bot_state": runtime.get("bot_state", "IDLE"),
                "layer": int(runtime.get("layer", 0) or 0),
                "pending_layer": int(runtime.get("pending_layer", runtime.get("layer", 0)) or 0),
                "position_side": runtime.get("position_side"),
                "best_profit_pct": safe_float(runtime.get("best_profit_pct", 0)) * 100,
                "last_known_contracts": safe_float(runtime.get("last_known_contracts", 0)),
                "entry_price": safe_float(runtime.get("entry_price", 0)),
                "partial_tp_1_done": bool(runtime.get("partial_tp_1_done", False)),
                "partial_tp_2_done": bool(runtime.get("partial_tp_2_done", False)),
                "activated": bool(runtime.get("activated", False)),
                "last_update": runtime.get("last_update"),
            },
            "account": balance,
            "stream": stream,
            "position": position,
            "performance": performance,
            "trade_analytics": trade_analytics,
            "market": market,
            "orders": order_book,
            "ladder": ladder,
            "next_action": next_action,
            "recent_trades": list(reversed(trades[-18:])),
        }

        history = self._record_history(snapshot)
        events = self._update_event_log(snapshot)
        snapshot["history"] = history
        snapshot["events"] = events
        return snapshot

    def _guard(
        self,
        label: str,
        fn: Callable[[], Any],
        fallback: Any,
        warnings: list[str],
    ) -> Any:
        try:
            return fn()
        except Exception as exc:  # pragma: no cover - network dependent
            warnings.append(f"{label}: {exc}")
            return fallback

    def _fetch_balance(self) -> dict[str, Any]:
        return load_json(LIVE_FILE, {}).get("account") or self._default_balance()

    def _fetch_stream_status(self) -> dict[str, Any]:
        return load_json(LIVE_FILE, {}).get("stream") or {"enabled": False, "transport": "rest"}

    def _fetch_position(self) -> dict[str, Any] | None:
        return load_json(LIVE_FILE, {}).get("position")

    def _fetch_open_orders(self) -> list[dict[str, Any]]:
        return load_json(LIVE_FILE, {}).get("open_orders") or []

    def _fetch_recent_trades(self) -> list[dict[str, Any]]:
        return load_json(LIVE_FILE, {}).get("recent_trades") or []

    def _fetch_ledger(self) -> list[dict[str, Any]]:
        return load_json(LIVE_FILE, {}).get("ledger") or []

    def _fetch_market_bundle(self) -> dict[str, Any]:
        return load_json(LIVE_FILE, {}).get("market") or self._default_market_bundle()

    def _build_price_series(self, frame) -> dict[str, Any]:
        points = []
        for _, row in frame.iterrows():
            points.append(
                {
                    "timestamp": format_timestamp(row["timestamp"]),
                    "close": safe_float(row["close"]),
                    "ema_fast": safe_float(row["ema_fast"]),
                    "ema_slow": safe_float(row["ema_slow"]),
                    "volume": safe_float(row["volume"]),
                }
            )
        return {"points": points}

    def _build_performance(
        self,
        balance: dict[str, Any],
        position: dict[str, Any] | None,
        trades: list[dict[str, Any]],
        ledger: list[dict[str, Any]],
        current_price: float,
    ) -> dict[str, Any]:
        unrealized = safe_float(position["unrealized_pnl"]) if position else 0.0
        realized = 0.0
        fees = 0.0
        turnover = 0.0
        now = datetime.now()
        cutoff = now.timestamp() - 24 * 60 * 60

        for trade in trades:
            turnover += safe_float(trade.get("cost", 0))
            fees += safe_float(trade.get("fee", 0))

        for entry in ledger:
            ts = entry.get("timestamp")
            if ts:
                try:
                    if datetime.fromisoformat(ts).timestamp() < cutoff:
                        continue
                except ValueError:
                    pass
            entry_type = str(entry.get("type", "")).lower()
            amount = safe_float(entry.get("amount", 0))
            if any(token in entry_type for token in ("pnl", "profit", "realized", "settle")):
                realized += amount

        equity_estimate = safe_float(balance.get("total", 0)) + unrealized
        roi = safe_float(position["percentage"]) if position else 0.0
        return {
            "equity_estimate": equity_estimate,
            "unrealized_pnl": unrealized,
            "realized_pnl_24h": realized,
            "fees_recent": fees,
            "turnover_recent": turnover,
            "roi_pct": roi,
            "mark_price": current_price,
        }

    def _build_trade_analytics(self, trades: list[dict[str, Any]]) -> dict[str, Any]:
        entries = [trade for trade in trades if not trade.get("reduce_only")]
        exits = [trade for trade in trades if trade.get("reduce_only")]

        inventory_qty = 0.0
        inventory_avg_price = 0.0
        inventory_direction = 0
        current_cycle: dict[str, Any] | None = None
        closed_cycles: list[dict[str, Any]] = []
        realized_gross = 0.0
        realized_net = 0.0
        winning_cycles = 0
        losing_cycles = 0
        largest_fill_cost = 0.0

        for trade in trades:
            side = str(trade.get("side") or "").lower()
            qty = safe_float(trade.get("amount", 0))
            price = safe_float(trade.get("price", 0))
            cost = safe_float(trade.get("cost", 0)) or (qty * price)
            fee = safe_float(trade.get("fee", 0))
            timestamp = trade.get("timestamp")
            largest_fill_cost = max(largest_fill_cost, cost)

            if qty <= 0 or price <= 0 or side not in {"buy", "sell"}:
                continue

            trade_direction = 1 if side == "buy" else -1
            reduce_only = bool(trade.get("reduce_only"))

            if not reduce_only:
                if inventory_direction == 0:
                    inventory_direction = trade_direction
                    inventory_qty = qty
                    inventory_avg_price = price
                    current_cycle = self._start_cycle(trade, inventory_direction)
                elif inventory_direction == trade_direction:
                    total_cost = (inventory_avg_price * inventory_qty) + (price * qty)
                    inventory_qty += qty
                    inventory_avg_price = safe_div(total_cost, inventory_qty)
                    if current_cycle is None:
                        current_cycle = self._start_cycle(trade, inventory_direction)
                    current_cycle["entry_qty"] += qty
                    current_cycle["entry_notional"] += cost
                    current_cycle["fills"] += 1
                    current_cycle["fees"] += fee
                    current_cycle["latest_fill_at"] = timestamp
                else:
                    close_qty = min(inventory_qty, qty)
                    gross = self._calculate_realized_gross(
                        inventory_direction,
                        inventory_avg_price,
                        price,
                        close_qty,
                    )
                    realized_gross += gross
                    if current_cycle is not None:
                        current_cycle["exit_qty"] += close_qty
                        current_cycle["exit_notional"] += close_qty * price
                        current_cycle["gross_pnl"] += gross
                        current_cycle["fills"] += 1
                        current_cycle["fees"] += fee
                        current_cycle["latest_fill_at"] = timestamp

                    inventory_qty -= close_qty
                    if inventory_qty <= 1e-12:
                        inventory_qty = 0.0
                        inventory_avg_price = 0.0
                        inventory_direction = 0
                        if current_cycle is not None:
                            cycle = self._finalize_cycle(current_cycle, timestamp)
                            realized_net += cycle["net_pnl"]
                            if cycle["net_pnl"] >= 0:
                                winning_cycles += 1
                            else:
                                losing_cycles += 1
                            closed_cycles.append(cycle)
                            current_cycle = None

                    remaining_qty = qty - close_qty
                    if remaining_qty > 1e-12:
                        inventory_direction = trade_direction
                        inventory_qty = remaining_qty
                        inventory_avg_price = price
                        current_cycle = self._start_cycle(
                            {
                                **trade,
                                "amount": remaining_qty,
                                "cost": remaining_qty * price,
                            },
                            inventory_direction,
                        )
                continue

            if inventory_direction == 0 or inventory_qty <= 0:
                continue

            close_qty = min(inventory_qty, qty)
            gross = self._calculate_realized_gross(
                inventory_direction,
                inventory_avg_price,
                price,
                close_qty,
            )
            realized_gross += gross
            if current_cycle is None:
                current_cycle = self._start_cycle(trade, inventory_direction)
                current_cycle["entry_qty"] = inventory_qty
                current_cycle["entry_notional"] = inventory_qty * inventory_avg_price
            current_cycle["exit_qty"] += close_qty
            current_cycle["exit_notional"] += close_qty * price
            current_cycle["gross_pnl"] += gross
            current_cycle["fills"] += 1
            current_cycle["fees"] += fee
            current_cycle["latest_fill_at"] = timestamp

            inventory_qty -= close_qty
            if inventory_qty <= 1e-12:
                inventory_qty = 0.0
                inventory_avg_price = 0.0
                inventory_direction = 0
                cycle = self._finalize_cycle(current_cycle, timestamp)
                realized_net += cycle["net_pnl"]
                if cycle["net_pnl"] >= 0:
                    winning_cycles += 1
                else:
                    losing_cycles += 1
                closed_cycles.append(cycle)
                current_cycle = None

        open_cycle = None
        if current_cycle is not None and inventory_qty > 0:
            open_cycle = {
                "opened_at": current_cycle.get("opened_at"),
                "direction": current_cycle.get("direction"),
                "entry_qty": current_cycle.get("entry_qty", inventory_qty),
                "entry_avg_price": inventory_avg_price,
                "fills": current_cycle.get("fills", 0),
                "fees": current_cycle.get("fees", 0.0),
            }

        closed_cycles = list(reversed(closed_cycles[-8:]))
        total_cycles = winning_cycles + losing_cycles
        return {
            "entry_fills": len(entries),
            "exit_fills": len(exits),
            "total_fills": len(trades),
            "largest_fill_cost": largest_fill_cost,
            "realized_gross_recent": realized_gross,
            "realized_net_recent": realized_net,
            "win_rate_pct": safe_div(winning_cycles, total_cycles) * 100,
            "winning_cycles": winning_cycles,
            "losing_cycles": losing_cycles,
            "closed_cycle_count": total_cycles,
            "avg_cycle_net": safe_div(realized_net, total_cycles),
            "avg_entry_fill": safe_div(
                sum(safe_float(trade.get("cost", 0)) for trade in entries),
                max(len(entries), 1),
            ),
            "avg_exit_fill": safe_div(
                sum(safe_float(trade.get("cost", 0)) for trade in exits),
                max(len(exits), 1),
            ),
            "latest_fill_at": trades[-1].get("timestamp") if trades else None,
            "open_cycle": open_cycle,
            "closed_cycles": closed_cycles,
        }

    def _start_cycle(self, trade: dict[str, Any], direction: int) -> dict[str, Any]:
        amount = safe_float(trade.get("amount", 0))
        cost = safe_float(trade.get("cost", 0))
        fee = safe_float(trade.get("fee", 0))
        return {
            "opened_at": trade.get("timestamp"),
            "closed_at": None,
            "direction": "做多" if direction > 0 else "做空",
            "entry_qty": amount,
            "entry_notional": cost,
            "exit_qty": 0.0,
            "exit_notional": 0.0,
            "gross_pnl": 0.0,
            "net_pnl": 0.0,
            "fills": 1,
            "fees": fee,
            "latest_fill_at": trade.get("timestamp"),
        }

    def _finalize_cycle(self, cycle: dict[str, Any], closed_at: str | None) -> dict[str, Any]:
        cycle = dict(cycle)
        cycle["closed_at"] = closed_at
        cycle["entry_avg_price"] = safe_div(cycle["entry_notional"], cycle["entry_qty"])
        cycle["exit_avg_price"] = safe_div(cycle["exit_notional"], cycle["exit_qty"])
        cycle["net_pnl"] = cycle["gross_pnl"] - cycle["fees"]
        cycle["duration_minutes"] = self._duration_minutes(cycle.get("opened_at"), closed_at)
        return cycle

    def _duration_minutes(self, started_at: str | None, ended_at: str | None) -> float:
        if not started_at or not ended_at:
            return 0.0
        try:
            delta = datetime.fromisoformat(ended_at) - datetime.fromisoformat(started_at)
        except ValueError:
            return 0.0
        return delta.total_seconds() / 60

    def _calculate_realized_gross(
        self,
        inventory_direction: int,
        entry_price: float,
        exit_price: float,
        quantity: float,
    ) -> float:
        if inventory_direction > 0:
            return (exit_price - entry_price) * quantity
        return (entry_price - exit_price) * quantity

    def _build_orders(self, open_orders: list[dict[str, Any]], current_price: float) -> dict[str, Any]:
        rows = []
        add_count = 0
        reduce_count = 0
        for order in sorted(open_orders, key=lambda item: safe_float(item.get("price", 0))):
            reduce_only = bool(order.get("reduceOnly", False))
            price = safe_float(order.get("price", 0))
            if reduce_only:
                reduce_count += 1
            else:
                add_count += 1
            distance_pct = 0.0
            if current_price and price:
                distance_pct = abs(price - current_price) / current_price * 100
            rows.append(
                {
                    "id": order.get("id"),
                    "side": order.get("side"),
                    "type": order.get("type"),
                    "price": price,
                    "amount": safe_float(order.get("amount", 0)),
                    "filled": safe_float(order.get("filled", 0)),
                    "status": order.get("status"),
                    "reduce_only": reduce_only,
                    "distance_pct": distance_pct,
                }
            )
        return {
            "count": len(rows),
            "add_count": add_count,
            "reduce_count": reduce_count,
            "items": rows,
        }

    def _build_ladder(
        self,
        runtime: dict[str, Any],
        balance: dict[str, Any],
        position: dict[str, Any] | None,
        current_price: float,
        market: dict[str, Any],
    ) -> list[dict[str, Any]]:
        ladder = []
        runtime_layer = int(runtime.get("layer", 0) or 0)
        side = runtime.get("position_side") or (position.get("side") if position else None)
        balance_total = safe_float(balance.get("total", 0))
        reference_price = safe_float(position.get("entry_price", 0)) if position else current_price

        for layer in range(1, self.bot.max_layers + 1):
            margin = balance_total * self.bot.first_order_ratio * self.bot.layer_multipliers[layer - 1]
            estimated_amount = 0.0
            if reference_price > 0:
                estimated_amount = (margin * self.bot.leverage) / reference_price

            state = "standby"
            if runtime_layer >= layer and side:
                state = "filled"
            elif runtime_layer + 1 == layer and side:
                state = "next"
            elif runtime_layer == 0 and layer == 1:
                state = "next"

            plan: dict[str, Any] | None = None
            if side and state in {"next", "standby"} and current_price:
                plan = self._build_layer_order_plan(
                    layer,
                    side,
                    current_price,
                    safe_float(position.get("entry_price", current_price)) if position else current_price,
                    market.get("support_resistance") or {},
                )
            elif layer == 1 and current_price:
                seed_side = self._signal_to_side(market["indicators"].get("signal"))
                if seed_side:
                    plan = self._build_first_order_plan(
                        seed_side,
                        current_price,
                        market.get("support_resistance") or {},
                    )

            ladder.append(
                {
                    "layer": layer,
                    "state": state,
                    "multiplier": self.bot.layer_multipliers[layer - 1],
                    "margin_estimate": margin,
                    "amount_estimate": estimated_amount,
                    "projected_price": safe_float((plan or {}).get("final_price", 0)),
                    "price_source": (plan or {}).get("source"),
                    "pricing": plan,
                }
            )
        return ladder

    def _build_next_action(
        self,
        runtime: dict[str, Any],
        position: dict[str, Any] | None,
        current_price: float,
        market: dict[str, Any],
    ) -> dict[str, Any]:
        signal = market["indicators"].get("signal")
        bot_state = runtime.get("bot_state", "IDLE")
        side = runtime.get("position_side") or (position.get("side") if position else None)
        current_layer = int(runtime.get("layer", 0) or 0)

        if bot_state == "IDLE":
            seed_side = self._signal_to_side(signal)
            if seed_side:
                plan = self._build_first_order_plan(
                    seed_side,
                    current_price,
                    market.get("support_resistance") or {},
                )
                return {
                    "title": "准备首层挂单",
                    "detail": f"当前触发 {signal} 信号，可准备第一层 {seed_side} 方向挂单。",
                    "side": seed_side,
                    "layer": 1,
                    "projected_price": safe_float(plan.get("final_price", 0)),
                    "source": plan.get("source"),
                    "pricing": plan,
                }
            return {
                "title": "继续等待",
                "detail": "当前趋势过滤为中性，等待新的方向触发。",
                "side": None,
                "layer": None,
                "projected_price": 0.0,
                "source": None,
                "pricing": None,
            }

        next_layer = current_layer + 1
        if side and next_layer <= self.bot.max_layers:
            plan = self._build_layer_order_plan(
                next_layer,
                side,
                current_price,
                safe_float(position.get("entry_price", current_price)) if position else current_price,
                market.get("support_resistance") or {},
            )
            return {
                "title": f"准备第 {next_layer} 层",
                "detail": f"机器人处于策略执行中，仍可继续挂出下一层 {side} 仓位。",
                "side": side,
                "layer": next_layer,
                "projected_price": safe_float(plan.get("final_price", 0)),
                "source": plan.get("source"),
                "pricing": plan,
            }

        return {
            "title": "以风控和止盈为主",
            "detail": "当前已接近或达到最大层级，重点关注移动止盈和回撤保护。",
            "side": side,
            "layer": current_layer,
            "projected_price": 0.0,
            "source": "RISK",
            "pricing": None,
        }

    def _price_to_snapshot_value(self, value: Any) -> float:
        numeric = safe_float(value, 0.0)
        if numeric <= 0:
            return 0.0
        return safe_float(self.bot._price_to_precision(numeric), numeric)

    def _build_first_order_plan(
        self,
        side: str,
        current_price: float,
        support_resistance: dict[str, Any],
    ) -> dict[str, Any]:
        offset_pct = safe_float(getattr(self.bot, "level_offset_pct", 0.0))
        if side == "short":
            resistance = [safe_float(level, 0.0) for level in support_resistance.get("resistance") or []]
            resistance = [level for level in resistance if level > 0]
            if resistance:
                reference_price = resistance[0]
                preferred_price = reference_price * (1 - offset_pct)
                guard_price = current_price * (1 + offset_pct)
                final_price = max(preferred_price, guard_price)
                return {
                    "source": "STRUCTURE",
                    "reference_type": "resistance",
                    "reference_label": "阻力位",
                    "reference_price": self._price_to_snapshot_value(reference_price),
                    "preferred_price": self._price_to_snapshot_value(preferred_price),
                    "guard_price": self._price_to_snapshot_value(guard_price),
                    "final_price": self._price_to_snapshot_value(final_price),
                    "offset_pct": offset_pct * 100,
                    "candidate_index": 1,
                    "candidate_count": len(resistance),
                    "rule_label": "空单挂在阻力位下方，并保持高于现价保护线。",
                }

            fallback_price = current_price * (1 + offset_pct)
            return {
                "source": "OFFSET",
                "reference_type": "market",
                "reference_label": "现价",
                "reference_price": self._price_to_snapshot_value(current_price),
                "preferred_price": self._price_to_snapshot_value(fallback_price),
                "guard_price": self._price_to_snapshot_value(fallback_price),
                "final_price": self._price_to_snapshot_value(fallback_price),
                "offset_pct": offset_pct * 100,
                "candidate_index": None,
                "candidate_count": 0,
                "rule_label": "暂无可用阻力位，按现价上方偏移挂出空单首仓。",
            }

        support = [safe_float(level, 0.0) for level in support_resistance.get("support") or []]
        support = [level for level in support if level > 0]
        if support:
            reference_price = support[0]
            preferred_price = reference_price * (1 + offset_pct)
            guard_price = current_price * (1 - offset_pct)
            final_price = min(preferred_price, guard_price)
            return {
                "source": "STRUCTURE",
                "reference_type": "support",
                "reference_label": "支撑位",
                "reference_price": self._price_to_snapshot_value(reference_price),
                "preferred_price": self._price_to_snapshot_value(preferred_price),
                "guard_price": self._price_to_snapshot_value(guard_price),
                "final_price": self._price_to_snapshot_value(final_price),
                "offset_pct": offset_pct * 100,
                "candidate_index": 1,
                "candidate_count": len(support),
                "rule_label": "多单挂在支撑位上方，并保持低于现价保护线。",
            }

        fallback_price = current_price * (1 - offset_pct)
        return {
            "source": "OFFSET",
            "reference_type": "market",
            "reference_label": "现价",
            "reference_price": self._price_to_snapshot_value(current_price),
            "preferred_price": self._price_to_snapshot_value(fallback_price),
            "guard_price": self._price_to_snapshot_value(fallback_price),
            "final_price": self._price_to_snapshot_value(fallback_price),
            "offset_pct": offset_pct * 100,
            "candidate_index": None,
            "candidate_count": 0,
            "rule_label": "暂无可用支撑位，按现价下方偏移挂出多单首仓。",
        }

    def _build_layer_order_plan(
        self,
        layer: int,
        side: str,
        current_price: float,
        avg_price: float,
        support_resistance: dict[str, Any],
    ) -> dict[str, Any]:
        offset_pct = safe_float(getattr(self.bot, "level_offset_pct", 0.0))
        support = [safe_float(level, 0.0) for level in support_resistance.get("support") or []]
        resistance = [safe_float(level, 0.0) for level in support_resistance.get("resistance") or []]

        if side == "long":
            candidates = [level for level in support if 0 < level < current_price and level < avg_price]
            if candidates:
                idx = min(max(layer - 2, 0), len(candidates) - 1)
                reference_price = candidates[idx]
                preferred_price = reference_price * (1 + offset_pct)
                guard_price = min(current_price, avg_price) * (1 - offset_pct)
                final_price = min(preferred_price, guard_price)
                return {
                    "source": "STRUCTURE",
                    "reference_type": "support",
                    "reference_label": "支撑位",
                    "reference_price": self._price_to_snapshot_value(reference_price),
                    "preferred_price": self._price_to_snapshot_value(preferred_price),
                    "guard_price": self._price_to_snapshot_value(guard_price),
                    "final_price": self._price_to_snapshot_value(final_price),
                    "offset_pct": offset_pct * 100,
                    "candidate_index": idx + 1,
                    "candidate_count": len(candidates),
                    "rule_label": "多单按更深一档支撑位上方偏移，并限制在现价/均价下方。",
                }
        else:
            candidates = [level for level in resistance if level > current_price and level > avg_price]
            if candidates:
                idx = min(max(layer - 2, 0), len(candidates) - 1)
                reference_price = candidates[idx]
                preferred_price = reference_price * (1 - offset_pct)
                guard_price = max(current_price, avg_price) * (1 + offset_pct)
                final_price = max(preferred_price, guard_price)
                return {
                    "source": "STRUCTURE",
                    "reference_type": "resistance",
                    "reference_label": "阻力位",
                    "reference_price": self._price_to_snapshot_value(reference_price),
                    "preferred_price": self._price_to_snapshot_value(preferred_price),
                    "guard_price": self._price_to_snapshot_value(guard_price),
                    "final_price": self._price_to_snapshot_value(final_price),
                    "offset_pct": offset_pct * 100,
                    "candidate_index": idx + 1,
                    "candidate_count": len(candidates),
                    "rule_label": "空单按更深一档阻力位下方偏移，并限制在现价/均价上方。",
                }

        atr_price = self._quiet(
            self.bot._select_atr_entry_price,
            side,
            layer,
            current_price,
            avg_price,
        )
        if atr_price is not None:
            return {
                "source": "ATR",
                "reference_type": "atr",
                "reference_label": "ATR 距离",
                "reference_price": self._price_to_snapshot_value(avg_price),
                "preferred_price": self._price_to_snapshot_value(atr_price),
                "guard_price": 0.0,
                "final_price": self._price_to_snapshot_value(atr_price),
                "offset_pct": 0.0,
                "candidate_index": None,
                "candidate_count": 0,
                "rule_label": "当前没有合适结构位，改用 ATR 距离估算下一层挂单价。",
            }

        fallback = self._quiet(
            self.bot._select_fallback_entry_price,
            side,
            layer,
            current_price,
            avg_price,
        )
        if fallback is None:
            return {
                "source": None,
                "reference_type": None,
                "reference_label": None,
                "reference_price": 0.0,
                "preferred_price": 0.0,
                "guard_price": 0.0,
                "final_price": 0.0,
                "offset_pct": 0.0,
                "candidate_index": None,
                "candidate_count": 0,
                "rule_label": "当前无法计算下一层挂单价。",
            }

        fallback_offset_pct = safe_float(getattr(self.bot, "add_layer_base_offset_pct", 0.0)) * layer * 100
        return {
            "source": "FALLBACK",
            "reference_type": "avg_price",
            "reference_label": "均价偏移",
            "reference_price": self._price_to_snapshot_value(avg_price),
            "preferred_price": self._price_to_snapshot_value(fallback),
            "guard_price": 0.0,
            "final_price": self._price_to_snapshot_value(fallback),
            "offset_pct": fallback_offset_pct,
            "candidate_index": None,
            "candidate_count": 0,
            "rule_label": "结构位和 ATR 都不可用，回退到均价百分比偏移。",
        }

    def _project_first_order_price(
        self,
        side: str,
        current_price: float,
        support_resistance: dict[str, Any],
    ) -> tuple[float, str | None]:
        plan = self._build_first_order_plan(side, current_price, support_resistance)
        return safe_float(plan.get("final_price", 0)), plan.get("source")

    def _project_layer_price(
        self,
        layer: int,
        side: str,
        current_price: float,
        avg_price: float,
    ) -> tuple[float, str | None]:
        plan = self._build_layer_order_plan(layer, side, current_price, avg_price, {})
        return safe_float(plan.get("final_price", 0)), plan.get("source")

    def _record_history(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        history = load_json(HISTORY_FILE, [])
        point = {
            "timestamp": snapshot["timestamp"],
            "price": safe_float(snapshot["market"]["indicators"].get("price", 0)),
            "equity_estimate": safe_float(snapshot["performance"].get("equity_estimate", 0)),
            "unrealized_pnl": safe_float(snapshot["performance"].get("unrealized_pnl", 0)),
            "layer": int(snapshot["runtime"].get("layer", 0) or 0),
            "orders": int(snapshot["orders"].get("count", 0)),
        }
        if not history or history[-1]["timestamp"] != point["timestamp"]:
            history.append(point)
        history = history[-480:]
        save_json(HISTORY_FILE, history)
        return {"points": history}

    def _update_event_log(self, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        events = load_json(EVENTS_FILE, [])
        previous = load_json(CACHE_FILE, None) or {}

        prev_position = previous.get("position")
        curr_position = snapshot.get("position")
        prev_layer = int(previous.get("runtime", {}).get("layer", 0) or 0)
        curr_layer = int(snapshot.get("runtime", {}).get("layer", 0) or 0)
        prev_signal = previous.get("market", {}).get("indicators", {}).get("signal")
        curr_signal = snapshot.get("market", {}).get("indicators", {}).get("signal")
        prev_orders = int(previous.get("orders", {}).get("count", 0) or 0)
        curr_orders = int(snapshot.get("orders", {}).get("count", 0) or 0)

        fresh_events = []
        if prev_position is None and curr_position is not None:
            fresh_events.append(
                self._event(
                    snapshot["timestamp"],
                    "OPEN",
                    "开仓完成",
                    f"{curr_position['side']} {curr_position['contracts']:.4f} @ {curr_position['entry_price']:.2f}",
                )
            )
        elif prev_position is not None and curr_position is None:
            fresh_events.append(
                self._event(
                    snapshot["timestamp"],
                    "CLOSE",
                    "平仓完成",
                    f"{prev_position['side']} 仓位已经全部平掉。",
                )
            )
        elif prev_position is not None and curr_position is not None:
            prev_contracts = safe_float(prev_position.get("contracts", 0))
            curr_contracts = safe_float(curr_position.get("contracts", 0))
            if curr_contracts > prev_contracts:
                fresh_events.append(
                    self._event(
                        snapshot["timestamp"],
                        "ADD",
                        "加仓成交",
                        f"仓位数量从 {prev_contracts:.4f} 增长到 {curr_contracts:.4f}。",
                    )
                )
            elif curr_contracts < prev_contracts:
                fresh_events.append(
                    self._event(
                        snapshot["timestamp"],
                        "REDUCE",
                        "减仓成交",
                        f"仓位数量从 {prev_contracts:.4f} 降到 {curr_contracts:.4f}。",
                    )
                )

        if prev_layer != curr_layer:
            fresh_events.append(
                self._event(
                    snapshot["timestamp"],
                    "LAYER",
                    "层级发生变化",
                    f"运行层级从 {prev_layer} 变化为 {curr_layer}。",
                )
            )
        if prev_signal and prev_signal != curr_signal:
            fresh_events.append(
                self._event(
                    snapshot["timestamp"],
                    "SIGNAL",
                    "信号发生变化",
                    f"信号从 {prev_signal} 切换到 {curr_signal}。",
                )
            )
        if prev_orders != curr_orders:
            fresh_events.append(
                self._event(
                    snapshot["timestamp"],
                    "ORDERS",
                    "挂单栈更新",
                    f"当前挂单数量从 {prev_orders} 变为 {curr_orders}。",
                )
            )

        if fresh_events:
            events.extend(fresh_events)
            events = events[-120:]
            save_json(EVENTS_FILE, events)

        return list(reversed(events[-18:]))

    def _event(self, timestamp: str, event_type: str, title: str, detail: str) -> dict[str, Any]:
        return {
            "timestamp": timestamp,
            "type": event_type,
            "title": title,
            "detail": detail,
        }

    def _infer_signal(self, row) -> tuple[str, str]:
        price = safe_float(row.get("close", 0) if hasattr(row, "get") else 0)
        ema_fast = safe_float(row["ema_fast"])
        ema_slow = safe_float(row["ema_slow"])
        rsi = safe_float(row["rsi"])
        adx = safe_float(row["adx"])
        atr = safe_float(row.get("atr", self.bot.latest_atr if hasattr(self.bot, "latest_atr") else 0))
        atr_band = atr * safe_float(getattr(self.bot, "mean_reversion_entry_atr_ratio", 0.35), 0.35)

        is_bullish = ema_fast > ema_slow and rsi > safe_float(self.bot.rsi_threshold)
        is_bearish = ema_fast < ema_slow and rsi < safe_float(self.bot.rsi_threshold)

        if bool(getattr(self.bot, "trend_follow_enabled", True)) and adx >= safe_float(
            getattr(self.bot, "trend_follow_adx_threshold", max(self.bot.adx_threshold + 8.0, 28.0))
        ):
            if is_bullish:
                return "TREND_UP", "LONG"
            if is_bearish:
                return "TREND_DOWN", "SHORT"

        if adx <= safe_float(getattr(self.bot, "mean_reversion_adx_max", max(self.bot.adx_threshold, 20.0))):
            if rsi <= safe_float(getattr(self.bot, "mean_reversion_long_rsi", 35.0)) and price <= (ema_fast - atr_band):
                return "RANGE_MEAN_REVERSION", "LONG"
            if rsi >= safe_float(getattr(self.bot, "mean_reversion_short_rsi", 65.0)) and price >= (ema_fast + atr_band):
                return "RANGE_MEAN_REVERSION", "SHORT"

        if is_bullish:
            return "BULLISH_BUT_STRETCHED", "WAIT"
        if is_bearish:
            return "BEARISH_BUT_STRETCHED", "WAIT"
        return "NEUTRAL", "WAIT"

    def _signal_to_side(self, signal: str | None) -> str | None:
        if signal == "SHORT":
            return "short"
        if signal == "LONG":
            return "long"
        return None

    def _quiet(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return fn(*args, **kwargs)

    def _default_balance(self) -> dict[str, Any]:
        return {
            "currency": "USDT",
            "free": 0.0,
            "used": 0.0,
            "total": 0.0,
            "utilization_pct": 0.0,
            "tradable_margin": 0.0,
            "safe_tradable_margin": 0.0,
        }

    def _default_market_bundle(self) -> dict[str, Any]:
        return {
            "ticker": {
                "last": 0.0,
                "bid": 0.0,
                "ask": 0.0,
                "high": 0.0,
                "low": 0.0,
                "change_pct": 0.0,
                "base_volume": 0.0,
                "quote_volume": 0.0,
            },
            "indicators": {
                "price": 0.0,
                "ema_fast": 0.0,
                "ema_slow": 0.0,
                "rsi": 0.0,
                "adx": 0.0,
                "atr": 0.0,
                "trend": "未知",
                "signal": "WAIT",
                "dynamic_tp_activate_pct": 0.0,
                "dynamic_tp_trail_ratio": 0.0,
            },
            "support_resistance": {"current_price": 0.0, "support": [], "resistance": []},
            "price_series": {"points": []},
        }
