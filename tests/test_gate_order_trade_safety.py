import importlib.util
import sys
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
module_path = ROOT / "trading" / "gate_order_trade_safety.py"
spec = importlib.util.spec_from_file_location("gate_order_trade_safety_under_test", module_path)
patch = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = patch
spec.loader.exec_module(patch)


class FakeExchange:
    name = "Gate.io"

    def __init__(self, order_trades=None, recent_trades=None):
        self.order_trades = dict(order_trades or {})
        self.recent_trades = list(recent_trades or [])
        self.calls = []

    def fetch_my_trades(self, symbol, since=None, limit=None, params=None):
        params = dict(params or {})
        self.calls.append((symbol, limit, params))
        if "order" in params:
            rows = list(self.order_trades.get(str(params["order"]), []))
            offset = int(params.get("offset", 0))
            size = limit or len(rows)
            return rows[offset : offset + size]
        size = limit or len(self.recent_trades)
        return self.recent_trades[-size:]


class BaseBot:
    def __init__(self, exchange):
        self.exchange = exchange
        self.symbol = "BTC/USDT:USDT"
        self.order_status = {}

    @staticmethod
    def _safe_float(value, default=0.0):
        try:
            return float(value)
        except Exception:
            return default

    def _fetch_authoritative_my_trades(self, limit):
        return self.exchange.fetch_my_trades(self.symbol, limit=limit)

    def _query_latest_entry_fill(
        self,
        position,
        preferred_order_id=None,
        preferred_client_oid=None,
    ):
        trades = self._fetch_authoritative_my_trades(60)
        target_id = str(preferred_order_id or "")
        target = [t for t in trades if str(t.get("order") or "") == target_id]
        if not target:
            return (False, None) if len(trades) >= 60 else (True, None)
        amount = sum(float(t["amount"]) for t in target)
        price = sum(float(t["price"]) * float(t["amount"]) for t in target) / amount
        return True, {
            "order_id": target_id,
            "amount": amount,
            "price": price,
            "timestamp_ms": 1,
        }

    def _entry_order_fill_barrier(self, order_ids):
        trades = self._fetch_authoritative_my_trades(60)
        expected = set(map(str, order_ids))
        for trade in trades:
            if (
                str(trade.get("order")) in expected
                and float(trade.get("amount", 0)) > 0
            ):
                return str(trade.get("order"))
        if len(trades) >= 60:
            return None
        for order_id in order_ids:
            if self.order_status.get(str(order_id)) != "canceled":
                return None
        return ""


class Bot(patch.GateOrderTradeSafetyMixin, BaseBot):
    pass


class ProtectiveStopBaseBot:
    def __init__(self, exchange_name="Gate.io", config=None):
        self.exchange = types.SimpleNamespace(name=exchange_name)
        self.config = dict(config or {})
        self.protective_stop_min_profit_pct = 0.005
        self.protective_stop_profit_lock_ratio = 0.75
        self.state = types.SimpleNamespace(
            best_profit_pct=0.005,
            active_trailing_drawdown_ratio=0.25,
            layer=1,
        )

    @staticmethod
    def _safe_float(value, default=0.0):
        try:
            return float(value)
        except Exception:
            return default

    def _fallback_trailing_values_for_layer(self, _layer):
        return 0.005, 0.25

    def _protective_stop_target(
        self,
        position,
        current_profit_pct=None,
        best_profit_pct=None,
        trail_ratio=None,
    ):
        entry_price = self._safe_float(position.get("entryPrice"), 0.0)
        if entry_price <= 0:
            return None
        side = str(position.get("side") or "long").lower()
        leverage = max(self._safe_float(position.get("leverage"), 3.0), 1.0)
        best_profit = max(
            self._safe_float(best_profit_pct, self.state.best_profit_pct),
            0.0,
        )
        if best_profit <= 0:
            return None
        resolved_trail_ratio = self._safe_float(trail_ratio, 0.25)
        lock_ratio = min(
            max(self.protective_stop_profit_lock_ratio, 1.0 - resolved_trail_ratio, 0.0),
            1.0,
        )
        locked_profit = max(
            self.protective_stop_min_profit_pct,
            best_profit * lock_ratio,
        )
        if current_profit_pct is not None:
            lockable_profit = max(self._safe_float(current_profit_pct, 0.0) - 0.001, 0.0)
            if lockable_profit < self.protective_stop_min_profit_pct:
                return None
            locked_profit = min(locked_profit, lockable_profit)
        raw_move_ratio = locked_profit / leverage
        trigger_price = (
            entry_price * (1 - raw_move_ratio)
            if side == "short"
            else entry_price * (1 + raw_move_ratio)
        )
        return {
            "entry_price": entry_price,
            "locked_profit_pct": locked_profit,
            "trigger_price": trigger_price,
            "side": side,
            "hold_side": side,
            "leverage": leverage,
            "trail_ratio": resolved_trail_ratio,
            "lock_ratio": lock_ratio,
        }


class ProtectiveStopBot(patch.GateOrderTradeSafetyMixin, ProtectiveStopBaseBot):
    pass


class GateOrderScopedTradeTests(unittest.TestCase):
    def setUp(self):
        self.recent = [
            {
                "id": f"r{i}",
                "order": f"other{i}",
                "amount": 1,
                "price": 1,
            }
            for i in range(60)
        ]

    def test_full_account_window_zero_fill_canceled_order_is_not_stuck(self):
        order_id = "36028835537893272"
        exchange = FakeExchange({order_id: []}, self.recent)
        bot = Bot(exchange)
        bot.order_status[order_id] = "canceled"

        self.assertEqual(
            bot._query_latest_entry_fill(
                {"side": "long"},
                preferred_order_id=order_id,
            ),
            (True, None),
        )
        self.assertEqual(bot._entry_order_fill_barrier([order_id]), "")
        self.assertTrue(
            any(call[2].get("order") == int(order_id) for call in exchange.calls)
        )

    def test_boundary_fill_still_blocks_zero_fill_rehang(self):
        order_id = "42"
        fill = {
            "id": "f1",
            "order": order_id,
            "amount": 0.1,
            "price": 100.0,
            "timestamp": 1,
        }
        exchange = FakeExchange({order_id: [fill]}, self.recent)
        bot = Bot(exchange)
        bot.order_status[order_id] = "canceled"

        complete, recovered = bot._query_latest_entry_fill(
            {"side": "long"},
            preferred_order_id=order_id,
        )
        self.assertTrue(complete)
        self.assertEqual(recovered["order_id"], order_id)
        self.assertAlmostEqual(recovered["amount"], 0.1)
        self.assertEqual(bot._entry_order_fill_barrier([order_id]), order_id)

    def test_gate_arms_protective_stop_at_minimum_activation_without_extra_wait(self):
        bot = ProtectiveStopBot("Gate.io")
        position = {"side": "long", "entryPrice": 100.0, "leverage": 3.0}

        target = bot._protective_stop_target(
            position,
            current_profit_pct=0.005,
            best_profit_pct=0.005,
            trail_ratio=0.25,
        )

        self.assertIsNotNone(target)
        self.assertAlmostEqual(target["locked_profit_pct"], 0.004)
        current_mark = 100.0 * (1 + 0.005 / 3.0)
        self.assertLess(target["trigger_price"], current_mark)
        self.assertAlmostEqual(target["gate_arm_buffer_pct"], 0.001)

    def test_non_gate_keeps_core_minimum_profit_gate(self):
        bot = ProtectiveStopBot("Bitget")
        position = {"side": "long", "entryPrice": 100.0, "leverage": 3.0}

        target = bot._protective_stop_target(
            position,
            current_profit_pct=0.005,
            best_profit_pct=0.005,
            trail_ratio=0.25,
        )

        self.assertIsNone(target)

    def test_gate_uses_normal_core_target_once_buffered_profit_reaches_floor(self):
        bot = ProtectiveStopBot("Gate.io")
        position = {"side": "long", "entryPrice": 100.0, "leverage": 3.0}

        target = bot._protective_stop_target(
            position,
            current_profit_pct=0.006,
            best_profit_pct=0.006,
            trail_ratio=0.25,
        )

        self.assertIsNotNone(target)
        self.assertAlmostEqual(target["locked_profit_pct"], 0.005)
        self.assertNotIn("gate_arm_buffer_pct", target)


if __name__ == "__main__":
    unittest.main()
