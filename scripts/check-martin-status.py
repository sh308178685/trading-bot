#!/usr/bin/env python3
"""Inspect the current martingale strategy status."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
for dependency_dir in (ROOT / ".deps-local", ROOT / ".deps"):
    if dependency_dir.exists() and str(dependency_dir) not in sys.path:
        sys.path.insert(0, str(dependency_dir))

import pandas as pd

from trading.exchanges import create_exchange_adapter
from trading.runtime_config import load_runtime_config

CONFIG_FILE = ROOT / "config" / "config.json"
RUNTIME_FILE = ROOT / "data" / "martin-runtime.json"
STATE_FILE = ROOT / "data" / "martin-state.json"


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def load_json(path, default):
    if path.exists():
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    return default


def save_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


class MartinStatusChecker:
    def __init__(self):
        self.config = load_runtime_config(CONFIG_FILE, default={})
        self.runtime_state = load_json(RUNTIME_FILE, {})
        self.symbol = self.config.get("symbol", "ETH/USDT:USDT")
        self.timeframe = self.config.get("timeframe", "5m")
        self.fast_ema_period = int(self.config.get("fast_ema_period", 20))
        self.slow_ema_period = int(self.config.get("slow_ema_period", 50))
        self.rsi_period = int(self.config.get("rsi_period", 14))
        self.adx_period = int(self.config.get("adx_period", 14))
        self.atr_period = int(self.config.get("atr_period", 14))
        self.adx_threshold = safe_float(self.config.get("adx_threshold", 16.5))
        self.rsi_threshold = safe_float(self.config.get("rsi_threshold", 50))
        self.trend_follow_enabled = bool(self.config.get("trend_follow_enabled", True))
        self.trend_follow_adx_threshold = safe_float(
            self.config.get("trend_follow_adx_threshold", max(self.adx_threshold + 8.0, 28.0)),
            max(self.adx_threshold + 8.0, 28.0),
        )
        self.mean_reversion_adx_max = safe_float(
            self.config.get("mean_reversion_adx_max", max(self.adx_threshold, 20.0)),
            max(self.adx_threshold, 20.0),
        )
        self.mean_reversion_long_rsi = safe_float(self.config.get("mean_reversion_long_rsi", 35.0), 35.0)
        self.mean_reversion_short_rsi = safe_float(self.config.get("mean_reversion_short_rsi", 65.0), 65.0)
        self.mean_reversion_entry_atr_ratio = safe_float(
            self.config.get("mean_reversion_entry_atr_ratio", 0.35),
            0.35,
        )
        self.exchange = self._init_exchange()

    def _init_exchange(self):
        return create_exchange_adapter(self.config)

    def _fetch_balance(self):
        balance = self.exchange.fetch_balance({"type": "swap"})
        usdt = balance.get("USDT", {})
        return {
            "currency": "USDT",
            "free": safe_float(usdt.get("free", 0)),
            "used": safe_float(usdt.get("used", 0)),
            "total": safe_float(usdt.get("total", 0)),
        }

    def _fetch_position(self):
        positions = self.exchange.fetch_positions([self.symbol])
        for position in positions:
            if safe_float(position.get("contracts", 0)) > 0:
                return {
                    "side": str(position.get("side", "")).lower() or None,
                    "contracts": safe_float(position.get("contracts", 0)),
                    "entry_price": safe_float(position.get("entryPrice", 0)),
                    "mark_price": safe_float(position.get("markPrice", 0)),
                    "unrealized_pnl": safe_float(position.get("unrealizedPnl", 0)),
                    "percentage": safe_float(position.get("percentage", 0)),
                    "liquidation_price": safe_float(position.get("liquidationPrice", 0)),
                }
        return None

    def _fetch_open_orders(self):
        orders = self.exchange.fetch_open_orders(self.symbol)
        result = []
        for order in orders:
            result.append(
                {
                    "id": order.get("id"),
                    "side": order.get("side"),
                    "type": order.get("type"),
                    "price": safe_float(order.get("price", 0)),
                    "amount": safe_float(order.get("amount", 0)),
                    "filled": safe_float(order.get("filled", 0)),
                    "reduce_only": bool(order.get("reduceOnly", False)),
                    "status": order.get("status"),
                }
            )
        return result

    def _fetch_indicators(self):
        limit = max(self.slow_ema_period + self.adx_period + 20, 100)
        ohlcv = self.exchange.fetch_ohlcv(self.symbol, self.timeframe, limit=limit)
        if len(ohlcv) < self.slow_ema_period + self.adx_period:
            raise RuntimeError("Not enough OHLCV data to calculate indicators.")

        frame = pd.DataFrame(
            ohlcv,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float64")

        # 检查K线数据是否有效（模拟盘可能返回全相同价格）
        price_range = frame["high"].max() - frame["low"].min()
        if price_range == 0:
            raise RuntimeError(
                f"K线数据无效: 价格无波动 ({frame['close'].iloc[0]:.2f})，"
                f"可能是模拟盘数据异常"
            )

        frame["ema_fast"] = frame["close"].ewm(span=self.fast_ema_period, adjust=False).mean()
        frame["ema_slow"] = frame["close"].ewm(span=self.slow_ema_period, adjust=False).mean()

        delta = frame["close"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(self.rsi_period).mean()
        avg_loss = loss.rolling(self.rsi_period).mean()
        rs = avg_gain / avg_loss.where(avg_loss.ne(0))
        frame["rsi"] = 100 - (100 / (1 + rs))

        prev_close = frame["close"].shift(1)
        tr1 = frame["high"] - frame["low"]
        tr2 = (frame["high"] - prev_close).abs()
        tr3 = (frame["low"] - prev_close).abs()
        frame["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        frame["atr"] = frame["tr"].rolling(self.atr_period).mean().fillna(0)

        up_move = frame["high"].diff()
        down_move = -frame["low"].diff()
        plus_dm = pd.Series(0.0, index=frame.index)
        minus_dm = pd.Series(0.0, index=frame.index)
        plus_dm[(up_move > down_move) & (up_move > 0)] = up_move
        minus_dm[(down_move > up_move) & (down_move > 0)] = down_move

        atr_safe = frame["atr"].replace(0, float("nan"))
        plus_di = 100 * (plus_dm.rolling(self.adx_period).mean().fillna(0) / atr_safe.fillna(1))
        minus_di = 100 * (minus_dm.rolling(self.adx_period).mean().fillna(0) / atr_safe.fillna(1))
        di_sum = plus_di + minus_di
        di_sum = di_sum.replace(0, float("nan"))
        dx = 100 * (plus_di - minus_di).abs() / di_sum
        frame["adx"] = dx.rolling(self.adx_period).mean().fillna(0)

        latest = frame.iloc[-1]
        trend, signal = self._infer_signal(latest)
        return {
            "price": safe_float(latest["close"]),
            "ema_fast": safe_float(latest["ema_fast"]),
            "ema_slow": safe_float(latest["ema_slow"]),
            "rsi": safe_float(latest["rsi"]),
            "adx": safe_float(latest["adx"]),
            "atr": safe_float(latest["atr"]),
            "trend": trend,
            "signal": signal,
        }

    def _infer_signal(self, row):
        price = safe_float(row.get("close", 0))
        ema_fast = safe_float(row["ema_fast"])
        ema_slow = safe_float(row["ema_slow"])
        rsi = safe_float(row["rsi"])
        adx = safe_float(row["adx"])
        atr = safe_float(row.get("atr", 0))
        atr_band = atr * self.mean_reversion_entry_atr_ratio

        is_bullish = ema_fast > ema_slow and rsi > self.rsi_threshold
        is_bearish = ema_fast < ema_slow and rsi < self.rsi_threshold

        if self.trend_follow_enabled and adx >= self.trend_follow_adx_threshold:
            if is_bullish:
                return "TREND_UP", "LONG"
            if is_bearish:
                return "TREND_DOWN", "SHORT"

        if adx <= self.mean_reversion_adx_max:
            if rsi <= self.mean_reversion_long_rsi and price <= (ema_fast - atr_band):
                return "RANGE_MEAN_REVERSION", "LONG"
            if rsi >= self.mean_reversion_short_rsi and price >= (ema_fast + atr_band):
                return "RANGE_MEAN_REVERSION", "SHORT"

        if is_bullish:
            return "BULLISH_BUT_STRETCHED", "WAIT"
        if is_bearish:
            return "BEARISH_BUT_STRETCHED", "WAIT"
        return "NEUTRAL", "WAIT"

    def collect(self):
        balance = self._fetch_balance()
        position = self._fetch_position()
        open_orders = self._fetch_open_orders()
        indicators = self._fetch_indicators()

        return {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "exchange": getattr(self.exchange, "name", str(self.config.get("exchange", "bitget"))),
            "mode": "sandbox" if self.config.get("sandbox", True) else "live",
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "balance": balance,
            "runtime": {
                "bot_state": self.runtime_state.get("bot_state", "IDLE"),
                "layer": self.runtime_state.get("layer", 0),
                "pending_layer": self.runtime_state.get(
                    "pending_layer",
                    self.runtime_state.get("layer", 0),
                ),
                "pending_entry_submission_state": self.runtime_state.get(
                    "pending_entry_submission_state",
                    "",
                ),
                "pending_entry_client_oid": self.runtime_state.get(
                    "pending_entry_client_oid",
                    "",
                ),
                "pending_entry_order_id": self.runtime_state.get(
                    "pending_entry_order_id",
                    "",
                ),
                "pending_entry_cancel_requested": bool(
                    self.runtime_state.get("pending_entry_cancel_requested", False)
                ),
                "partial_tp_pending_flag": self.runtime_state.get(
                    "partial_tp_pending_flag",
                    "",
                ),
                "cycle_id": self.runtime_state.get("cycle_id", ""),
                "exit_state": self.runtime_state.get("exit_state", "IDLE"),
                "exit_reason": self.runtime_state.get("exit_reason", ""),
                "exit_client_oid": self.runtime_state.get("exit_client_oid", ""),
                "exit_order_id": self.runtime_state.get("exit_order_id", ""),
                "protective_stop_submission_state": self.runtime_state.get(
                    "protective_stop_submission_state",
                    "IDLE",
                ),
                "protective_stop_client_oid": self.runtime_state.get(
                    "protective_stop_client_oid",
                    "",
                ),
                "position_side": self.runtime_state.get("position_side"),
                "best_profit_pct": safe_float(self.runtime_state.get("best_profit_pct", 0)),
                "protective_stop_active": bool(
                    self.runtime_state.get("protective_stop_active", False)
                ),
                "protective_stop_price": safe_float(
                    self.runtime_state.get("protective_stop_price", 0)
                ),
                "last_known_contracts": safe_float(
                    self.runtime_state.get("last_known_contracts", 0)
                ),
                "entry_price": safe_float(self.runtime_state.get("entry_price", 0)),
                "last_update": self.runtime_state.get("last_update"),
            },
            "position": position,
            "orders": {
                "count": len(open_orders),
                "items": open_orders,
            },
            "indicators": indicators,
        }


def detect_trade_events(current_state, previous_state):
    events = []
    previous_position = previous_state.get("last_position")
    current_position = current_state.get("position")

    if previous_position is None and current_position is not None:
        events.append(
            {
                "type": "OPEN",
                "detail": (
                    f"开仓 {_side_cn(current_position['side'])} "
                    f"{current_position['contracts']:.4f} @ {current_position['entry_price']:.2f}"
                ),
            }
        )
    elif previous_position is not None and current_position is None:
        events.append(
            {
                "type": "CLOSE",
                "detail": (
                    f"平仓 {_side_cn(previous_position['side'])} "
                    f"{safe_float(previous_position['contracts']):.4f}"
                ),
            }
        )
    elif previous_position is not None and current_position is not None:
        previous_contracts = safe_float(previous_position.get("contracts", 0))
        current_contracts = safe_float(current_position.get("contracts", 0))
        if current_contracts > previous_contracts:
            events.append(
                {
                    "type": "ADD",
                    "detail": f"加仓 {previous_contracts:.4f} → {current_contracts:.4f}",
                }
            )
        elif current_contracts < previous_contracts:
            events.append(
                {
                    "type": "REDUCE",
                    "detail": f"减仓 {previous_contracts:.4f} → {current_contracts:.4f}",
                }
            )

    return events


def _side_cn(side):
    return {"long": "做多", "short": "做空"}.get(str(side).lower(), str(side))

def _trend_cn(trend):
    return {
        "TREND_UP": "强趋势上行",
        "TREND_DOWN": "强趋势下行",
        "RANGE_MEAN_REVERSION": "震荡均值回归",
        "BULLISH_BUT_STRETCHED": "偏多但不追",
        "BEARISH_BUT_STRETCHED": "偏空但不追",
        "NEUTRAL": "中性等待",
    }.get(trend, trend)

def _signal_cn(signal):
    return {"LONG": "做多", "SHORT": "做空", "WAIT": "观望"}.get(signal, signal)

def _bot_state_cn(state):
    return {
        "IDLE": "空闲", "RUNNING": "运行中", "STOPPED": "已停止", "PAUSED": "已暂停"
    }.get(state, state)

def format_text_report(status, events=None):
    runtime = status["runtime"]
    indicators = status["indicators"]
    position = status["position"]

    mode_cn = "模拟盘" if status["mode"] == "sandbox" else "实盘"

    lines = [
        f"📊 {status.get('exchange', '交易所')} 马丁策略状态",
        f"⏰ 时间: {status['timestamp']}",
        f"🎮 模式: {mode_cn}",
        f"📈 交易对: {status['symbol']} ({status['timeframe']})",
        (
            f"🤖 机器人: {_bot_state_cn(runtime['bot_state'])} | "
            f"层级: {runtime['layer']} | "
            f"方向: {_side_cn(runtime['position_side']) if runtime['position_side'] else '无'}"
        ),
        (
            f"💰 余额: {status['balance']['total']:.2f} USDT "
            f"(可用 {status['balance']['free']:.2f})"
        ),
        (
            f"📡 信号: {_signal_cn(indicators['signal'])} | "
            f"趋势: {_trend_cn(indicators['trend'])} | "
            f"价格: {indicators['price']:.2f}"
        ),
        (
            f"📊 EMA快: {indicators['ema_fast']:.2f} | "
            f"EMA慢: {indicators['ema_slow']:.2f} | "
            f"RSI: {indicators['rsi']:.2f} | ADX: {indicators['adx']:.2f}"
        ),
        f"📋 挂单数: {status['orders']['count']}",
    ]

    if position:
        lines.append(
            f"📍 持仓: {_side_cn(position['side'])} "
            f"{position['contracts']:.4f} @ {position['entry_price']:.2f} | "
            f"盈亏 {position['unrealized_pnl']:+.2f} ({position['percentage']:+.2f}%)"
        )
    else:
        lines.append("📍 持仓: 无")

    if runtime.get("pending_entry_submission_state") not in {"", "IDLE"}:
        lines.append(
            "⏳ 开仓/加仓待确认: "
            f"{runtime['pending_entry_submission_state']} | "
            f"clientOid={runtime.get('pending_entry_client_oid') or '--'}"
        )
    if runtime.get("partial_tp_pending_flag"):
        lines.append(f"⏳ 分批止盈待确认: TP{runtime['partial_tp_pending_flag']}")
    if runtime.get("exit_state") not in {"", "IDLE", "CONFIRMED"}:
        lines.append(
            "⏳ 整仓退出待确认: "
            f"{runtime['exit_state']} | 原因={runtime.get('exit_reason') or '--'} | "
            f"clientOid={runtime.get('exit_client_oid') or '--'}"
        )
    if runtime.get("protective_stop_submission_state") not in {"", "IDLE", "CONFIRMED"}:
        lines.append(
            "⏳ 保护止损待确认: "
            f"{runtime['protective_stop_submission_state']} | "
            f"clientOid={runtime.get('protective_stop_client_oid') or '--'}"
        )

    if events:
        lines.append("🔔 事件:")
        event_type_cn = {"OPEN": "开仓", "CLOSE": "平仓", "ADD": "加仓", "REDUCE": "减仓"}
        for event in events:
            lines.append(f"  - {event_type_cn.get(event['type'], event['type'])}: {event['detail']}")

    return "\n".join(lines)


def build_parser():
    parser = argparse.ArgumentParser(description="Check the current martingale strategy status.")
    parser.add_argument("--json", action="store_true", help="Output the full status as JSON.")
    parser.add_argument(
        "--events-only",
        action="store_true",
        help="Only print output when a position event is detected.",
    )
    return parser


def main():
    args = build_parser().parse_args()
    checker = MartinStatusChecker()
    previous_state = load_json(
        STATE_FILE,
        {"last_position": None, "last_orders": 0, "last_check": None},
    )

    try:
        current_state = checker.collect()
        events = detect_trade_events(current_state, previous_state)
    except Exception as exc:
        error_payload = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "error": str(exc),
        }
        if args.json:
            print(json.dumps(error_payload, indent=2, ensure_ascii=False))
        else:
            print(f"Status check failed: {exc}", file=sys.stderr)
        return 1

    save_json(
        STATE_FILE,
        {
            "last_position": current_state.get("position"),
            "last_orders": current_state.get("orders", {}).get("count", 0),
            "last_check": current_state["timestamp"],
        },
    )

    if args.events_only and not events:
        return 0

    if args.json:
        print(json.dumps(current_state, indent=2, ensure_ascii=False))
    else:
        print(format_text_report(current_state, events))
    return 0


if __name__ == "__main__":
    sys.exit(main())
