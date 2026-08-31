import importlib.util
import json
import math
import sys
import tempfile
import threading
import types
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent

# The strategy module imports the exchange factory at module load time.  These
# tests exercise pure strategy logic and provide a fake exchange, so avoid
# importing optional live-exchange dependencies such as ccxt.
trading_package = types.ModuleType("trading")
trading_package.__path__ = [str(ROOT / "trading")]
exchange_module = types.ModuleType("trading.exchanges")
exchange_module.create_exchange_adapter = lambda config: None
runtime_config_module = types.ModuleType("trading.runtime_config")
runtime_config_module.load_runtime_config = lambda path, default=None: default or {}
sys.modules.setdefault("trading", trading_package)
sys.modules.setdefault("trading.exchanges", exchange_module)
sys.modules.setdefault("trading.runtime_config", runtime_config_module)

module_path = ROOT / "scripts" / "martin-bot.py"
spec = importlib.util.spec_from_file_location("martin_bot_under_test", module_path)
martin = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = martin
spec.loader.exec_module(martin)


class FakeExchange:
    def __init__(self, trades=None, trade_error=None, price_step=0.1):
        self.trades = list(trades or [])
        self.trade_error = trade_error
        self.trade_calls = 0
        self.price_step = price_step
        self.created_orders = []
        self.open_orders = []
        self.cancel_removes_orders = True
        self.cancel_error = None
        self.cancel_results = None
        self.order_statuses = {}

    def fetch_my_trades(self, symbol, since=None, limit=None, params=None):
        self.trade_calls += 1
        if self.trade_error is not None:
            raise self.trade_error
        return self.trades[-(limit or len(self.trades)):]

    def market(self, symbol):
        return {"precision": {"price": self.price_step}}

    def load_markets(self):
        return {"ETH/USDT:USDT": self.market("ETH/USDT:USDT")}

    def price_to_precision(self, symbol, price):
        units = math.floor((float(price) / self.price_step) + 1e-12)
        return str(units * self.price_step)

    def create_order(self, symbol, order_type, side, amount, price=None, params=None):
        order = {
            "id": f"order-{len(self.created_orders) + 1}",
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "amount": amount,
            "price": price,
            "clientOrderId": (params or {}).get("clientOrderId") or (params or {}).get("clientOid"),
            "params": dict(params or {}),
        }
        self.created_orders.append(order)
        return order

    def create_trigger_order(
        self,
        symbol,
        side,
        amount,
        trigger_price,
        price=None,
        trigger_type=None,
        order_type=None,
        params=None,
    ):
        order = {
            "id": f"trigger-{len(self.created_orders) + 1}",
            "symbol": symbol,
            "type": "trigger",
            "side": side,
            "amount": amount,
            "price": price,
            "triggerPrice": trigger_price,
            "clientOrderId": (params or {}).get("clientOrderId") or (params or {}).get("clientOid"),
            "params": dict(params or {}),
        }
        self.created_orders.append(order)
        return order

    def fetch_open_orders(self, symbol=None, since=None, limit=None, params=None):
        return list(self.open_orders)

    def cancel_orders(self, orders, symbol=None):
        if self.cancel_error is not None:
            raise self.cancel_error
        if self.cancel_removes_orders:
            target_ids = {str(order.get("id") or "") for order in orders}
            self.open_orders = [
                order for order in self.open_orders if str(order.get("id") or "") not in target_ids
            ]
        if self.cancel_results is not None:
            return [dict(row) for row in self.cancel_results]
        return [{"id": order.get("id")} for order in orders]

    def cancel_order(self, order_id, symbol=None, params=None):
        if self.cancel_error is not None:
            raise self.cancel_error
        self.open_orders = [
            order for order in self.open_orders if str(order.get("id") or "") != str(order_id)
        ]
        return {"id": order_id, "status": "canceled"}

    def fetch_order(self, order_id, symbol=None, params=None):
        if order_id not in self.order_statuses:
            raise RuntimeError("order unavailable")
        return dict(self.order_statuses[order_id])


def make_bot(side="long", layer=2, last_fill=0.0, source="", order_id="", trades=None):
    bot = martin.MartinBot.__new__(martin.MartinBot)
    bot.symbol = "ETH/USDT:USDT"
    bot.state = martin.RuntimeState(
        symbol=bot.symbol,
        layer=layer,
        pending_layer=layer,
        position_side=side,
        bot_state="IN_STRATEGY",
        last_fill_price=last_fill,
        last_fill_price_source=source,
        last_fill_order_id=order_id,
        initial_balance=1000.0,
    )
    bot.state_lock = threading.RLock()
    bot.action_lock = threading.RLock()
    bot.runtime_save_lock = threading.Lock()
    bot._exit_in_progress = threading.Event()
    bot._state_sync_required = False
    bot._state_sync_reason = ""
    bot.exchange = FakeExchange(trades=trades)
    bot.markets = None
    bot._save_runtime_state = lambda: None
    bot._write_live_snapshot = lambda **kwargs: None
    return bot


class LastFillRecoveryTests(unittest.TestCase):
    def test_recovers_latest_order_vwap_and_ignores_reduce_only_trade(self):
        trades = [
            {"id": "t1", "order": "first", "timestamp": 1000, "side": "buy", "price": 100, "amount": 1, "info": {}},
            {"id": "t2", "order": "add-2", "timestamp": 3000, "side": "buy", "price": 90, "amount": 1, "info": {}},
            {"id": "t3", "order": "add-2", "timestamp": 3001, "side": "buy", "price": 88, "amount": 3, "info": {}},
            {"id": "t4", "order": "close", "timestamp": 4000, "side": "sell", "price": 95, "amount": 0.5, "info": {"reduceOnly": True}},
        ]
        bot = make_bot(side="long", layer=2, trades=trades)
        position = {"side": "long", "entryPrice": 93.33}

        self.assertTrue(bot._ensure_verified_last_fill_price(position))
        self.assertAlmostEqual(bot.state.last_fill_price, 88.5)
        self.assertEqual(bot.state.last_fill_order_id, "add-2")
        self.assertEqual(bot.state.last_fill_price_source, "trade_history")

    def test_preferred_order_id_prevents_later_manual_trade_from_becoming_anchor(self):
        trades = [
            {"id": "t1", "order": "add-2", "timestamp": 1000, "side": "buy", "price": 90, "amount": 2, "info": {}},
            {"id": "t2", "order": "manual", "timestamp": 2000, "side": "buy", "price": 95, "amount": 1, "info": {}},
        ]
        bot = make_bot(side="long", layer=2, trades=trades)

        self.assertTrue(
            bot._ensure_verified_last_fill_price(
                {"side": "long"},
                force_refresh=True,
                preferred_order_id="add-2",
            )
        )
        self.assertEqual(bot.state.last_fill_price, 90)
        self.assertEqual(bot.state.last_fill_order_id, "add-2")

    def test_missing_preferred_order_never_falls_back_to_another_trade(self):
        trades = [
            {"id": "t1", "order": "manual", "timestamp": 2000, "side": "buy", "price": 95, "amount": 1, "info": {}},
        ]
        bot = make_bot(side="long", layer=2, trades=trades)

        self.assertFalse(
            bot._ensure_verified_last_fill_price(
                {"side": "long"},
                preferred_order_id="expected-add-2",
            )
        )
        self.assertEqual(bot.exchange.trade_calls, 1)
        self.assertEqual(bot.state.last_fill_price, 0.0)
        self.assertEqual(bot.state.unresolved_fill_order_id, "expected-add-2")

    def test_manual_authoritative_order_cannot_become_fill_anchor(self):
        trades = [
            {
                "id": "manual-fill",
                "order": "manual-order",
                "timestamp": 2000,
                "side": "buy",
                "price": 90.0,
                "amount": 1.0,
                "info": {},
            }
        ]
        bot = make_bot("long", layer=1, trades=trades)
        bot.exchange.order_statuses["manual-order"] = {
            "id": "manual-order",
            "clientOrderId": "manual-order-id",
            "status": "closed",
            "side": "buy",
            "filled": 1.0,
            "reduceOnly": False,
        }

        self.assertFalse(
            bot._ensure_verified_last_fill_price(
                {"side": "long"},
                force_refresh=True,
                preferred_order_id="manual-order",
                expected_contract_delta=1.0,
            )
        )
        self.assertEqual(bot.state.last_fill_price, 0.0)
        self.assertEqual(bot.state.unresolved_fill_order_id, "manual-order")

    def test_authoritative_bot_client_id_can_become_fill_anchor(self):
        trades = [
            {
                "id": "bot-fill",
                "order": "bot-order",
                "timestamp": 2000,
                "side": "buy",
                "price": 90.0,
                "amount": 1.0,
                "info": {},
            }
        ]
        bot = make_bot("long", layer=1, trades=trades)
        bot.exchange.order_statuses["bot-order"] = {
            "id": "bot-order",
            "clientOrderId": "t-martin-bot-order",
            "status": "closed",
            "side": "buy",
            "filled": 1.0,
            "reduceOnly": False,
        }

        self.assertTrue(
            bot._ensure_verified_last_fill_price(
                {"side": "long"},
                force_refresh=True,
                preferred_order_id="bot-order",
                expected_contract_delta=1.0,
            )
        )
        self.assertEqual(bot.state.last_fill_order_id, "bot-order")

    def test_unverified_legacy_average_is_rejected_when_trades_are_missing(self):
        bot = make_bot(side="long", layer=2, last_fill=93.33, trades=[])

        self.assertFalse(bot._ensure_verified_last_fill_price({"side": "long", "entryPrice": 93.33}))
        self.assertEqual(bot.state.last_fill_price, 0.0)
        self.assertEqual(bot.state.last_fill_price_source, "")

    def test_add_plan_fails_closed_when_real_fill_cannot_be_recovered(self):
        bot = make_bot(side="long", layer=2, last_fill=93.33, trades=[])

        plan = bot._build_add_order_plan(
            3,
            92,
            position={"side": "long", "entryPrice": 93.33, "contracts": 2},
        )

        self.assertIsNone(plan)
        self.assertEqual(bot.exchange.trade_calls, 0)

    def test_fill_delta_mismatch_rejects_layer_promotion(self):
        trades = [
            {
                "id": "bot-fill",
                "order": "add-2",
                "timestamp": 1000,
                "side": "buy",
                "price": 90.0,
                "amount": 1.15,
                "info": {},
            },
            {
                "id": "manual-fill",
                "order": "manual",
                "timestamp": 1001,
                "side": "buy",
                "price": 91.0,
                "amount": 1.0,
                "info": {},
            },
        ]
        bot = make_bot("long", layer=1, trades=trades)

        self.assertFalse(
            bot._ensure_verified_last_fill_price(
                {"side": "long"},
                force_refresh=True,
                preferred_order_id="add-2",
                expected_contract_delta=2.15,
            )
        )
        self.assertEqual(bot.state.last_fill_price, 0.0)
        self.assertEqual(bot.state.unresolved_fill_order_id, "add-2")

    def test_partial_fill_delta_uses_persisted_accounted_amount(self):
        trades = [
            {
                "id": "part-1",
                "order": "add-2",
                "timestamp": 1000,
                "side": "buy",
                "price": 90.0,
                "amount": 0.4,
                "info": {},
            },
            {
                "id": "part-2",
                "order": "add-2",
                "timestamp": 1001,
                "side": "buy",
                "price": 89.0,
                "amount": 0.2,
                "info": {},
            },
        ]
        bot = make_bot(
            "long",
            layer=2,
            last_fill=90.0,
            source="trade_history",
            order_id="add-2",
            trades=trades,
        )
        bot.state.last_fill_amount = 0.4
        bot._record_unresolved_entry_fill(
            "add-2",
            {
                "order_id": "add-2",
                "price": (90.0 * 0.4 + 89.0 * 0.2) / 0.6,
                "amount": 0.6,
                "timestamp_ms": 1001,
            },
        )

        self.assertAlmostEqual(bot.state.unresolved_fill_accounted_amount, 0.4)
        self.assertTrue(
            bot._ensure_verified_last_fill_price(
                {"side": "long"},
                force_refresh=True,
                preferred_order_id="add-2",
                expected_contract_delta=0.2,
                previously_accounted_fill_amount=(
                    bot.state.unresolved_fill_accounted_amount
                ),
            )
        )
        self.assertAlmostEqual(bot.state.last_fill_amount, 0.6)
        self.assertEqual(bot.state.unresolved_fill_accounted_amount, 0.0)

    def test_add_plan_retries_the_unresolved_order_id(self):
        bot = make_bot(side="long", layer=2, trades=[])
        bot.state.unresolved_fill_order_id = "expected-add-2"
        observed_order_ids = []

        def reject_recovery(
            position,
            *,
            force_refresh=False,
            preferred_order_id=None,
            preferred_client_oid=None,
        ):
            observed_order_ids.append(preferred_order_id)
            return False

        bot._ensure_verified_last_fill_price = reject_recovery

        plan = bot._build_add_order_plan(
            3,
            92,
            position={"side": "long", "entryPrice": 93.33, "contracts": 2},
        )

        self.assertIsNone(plan)
        self.assertEqual(observed_order_ids, ["expected-add-2"])

    def test_trade_api_error_invalidates_old_anchor(self):
        bot = make_bot(
            side="short",
            layer=3,
            last_fill=110,
            source="trade_history",
            order_id="old-order",
        )
        bot.exchange.trade_error = RuntimeError("temporary API failure")

        self.assertFalse(
            bot._ensure_verified_last_fill_price(
                {"side": "short"},
                force_refresh=True,
                preferred_order_id="new-order",
            )
        )
        self.assertEqual(bot.state.last_fill_price, 0.0)
        self.assertEqual(bot.state.last_fill_order_id, "")

    def test_single_layer_recovery_rejects_non_bot_trade(self):
        trades = [
            {
                "id": "manual-fill",
                "order": "manual-order",
                "timestamp": 1000,
                "side": "buy",
                "price": 100.0,
                "amount": 1.0,
                "info": {},
            }
        ]
        bot = make_bot("long", layer=1, trades=trades)
        bot.exchange.order_statuses["manual-order"] = {
            "id": "manual-order",
            "clientOrderId": "manual-trade",
            "status": "closed",
            "side": "buy",
            "filled": 1.0,
            "reduceOnly": False,
        }

        self.assertFalse(
            bot._recover_single_layer_position_fill(
                {"side": "long", "contracts": 1.0, "entryPrice": 100.0}
            )
        )
        self.assertEqual(bot.state.last_fill_price, 0.0)
        self.assertEqual(bot.state.last_fill_order_id, "")

    def test_single_layer_recovery_rejects_fill_from_older_position_cycle(self):
        old_timestamp = 1_700_000_000_000
        trades = [
            {
                "id": "old-fill",
                "order": "old-order",
                "timestamp": old_timestamp,
                "side": "buy",
                "price": 100.0,
                "amount": 1.0,
                "info": {},
            }
        ]
        bot = make_bot("long", layer=1, trades=trades)
        bot.exchange.order_statuses["old-order"] = {
            "id": "old-order",
            "clientOrderId": "t-martin-old",
            "status": "closed",
            "side": "buy",
            "filled": 1.0,
            "reduceOnly": False,
        }

        self.assertFalse(
            bot._recover_single_layer_position_fill(
                {
                    "side": "long",
                    "contracts": 1.0,
                    "entryPrice": 100.0,
                    "timestamp": old_timestamp + 60_000,
                }
            )
        )
        self.assertEqual(bot.state.last_fill_order_id, "")

    def test_exact_old_cycle_anchor_is_rejected_even_when_delta_is_zero(self):
        old_timestamp = 1_700_000_000_000
        trades = [
            {
                "id": "old-fill",
                "order": "old-order",
                "timestamp": old_timestamp,
                "side": "buy",
                "price": 100.0,
                "amount": 1.0,
                "info": {},
            }
        ]
        bot = make_bot(
            "long",
            layer=1,
            last_fill=100.0,
            source="trade_history",
            order_id="old-order",
            trades=trades,
        )
        bot.state.last_fill_amount = 1.0
        bot.state.pending_layer = 2
        bot.state.pending_entry_order_id = "old-order"
        bot.state.last_known_contracts = 1.0
        bot.exchange.order_statuses["old-order"] = {
            "id": "old-order",
            "clientOrderId": "t-martin-old-order",
            "status": "closed",
            "side": "buy",
            "filled": 1.0,
            "reduceOnly": False,
        }

        self.assertFalse(
            bot._ensure_verified_last_fill_price(
                {
                    "side": "long",
                    "contracts": 1.0,
                    "timestamp": old_timestamp + 60_000,
                },
                force_refresh=True,
                preferred_order_id="old-order",
                expected_contract_delta=0.0,
            )
        )
        self.assertEqual(bot.state.layer, 1)
        self.assertEqual(bot.state.last_fill_price, 0.0)
        self.assertEqual(bot.state.unresolved_fill_order_id, "old-order")

    def test_fill_within_position_open_time_tolerance_is_accepted(self):
        position_open = 1_700_000_000_000
        trades = [
            {
                "id": "current-fill",
                "order": "current-order",
                "timestamp": position_open - 4_000,
                "side": "buy",
                "price": 100.0,
                "amount": 1.0,
                "info": {},
            }
        ]
        bot = make_bot("long", layer=1, trades=trades)
        bot.exchange.order_statuses["current-order"] = {
            "id": "current-order",
            "clientOrderId": "t-martin-current-order",
            "status": "closed",
            "side": "buy",
            "filled": 1.0,
            "reduceOnly": False,
        }

        self.assertTrue(
            bot._ensure_verified_last_fill_price(
                {"side": "long", "info": {"open_time": position_open}},
                force_refresh=True,
                preferred_order_id="current-order",
                expected_contract_delta=1.0,
            )
        )
        self.assertEqual(bot.state.last_fill_order_id, "current-order")

    def test_gate_trigger_parent_promotes_exact_child_fill_identity(self):
        trades = [
            {
                "id": "child-fill",
                "order": "child-order",
                "timestamp": 2000,
                "side": "buy",
                "price": 90.0,
                "amount": 0.4,
                "info": {},
            }
        ]
        bot = make_bot("long", layer=1, trades=trades)
        bot.state.pending_layer = 2
        bot.state.pending_entry_order_id = "auto-order"
        bot.exchange.order_statuses["auto-order"] = {
            "id": "auto-order",
            "clientOrderId": "t-martin-auto-order",
            "type": "trigger",
            "status": "closed",
            "triggerExecutionOrderId": "child-order",
            "info": {
                "trade_id": "child-order",
                "me_order_id": "must-not-be-used",
            },
        }

        self.assertTrue(
            bot._ensure_verified_last_fill_price(
                {"side": "long"},
                force_refresh=True,
                preferred_order_id="auto-order",
                expected_contract_delta=0.4,
            )
        )
        self.assertEqual(bot.state.last_fill_order_id, "child-order")
        self.assertEqual(bot.state.pending_entry_order_id, "child-order")
        self.assertEqual(bot.state.last_fill_price, 90.0)

    def test_gate_me_order_id_is_never_used_as_entry_execution_id(self):
        self.assertEqual(
            martin.MartinBot._entry_execution_order_id(
                {"type": "trigger", "info": {"me_order_id": "wrong-order"}}
            ),
            "",
        )

    def test_gate_canceled_trigger_without_child_is_zero_fill_terminal(self):
        bot = make_bot("long", layer=2, trades=[])
        bot.state.pending_layer = 3
        bot.state.pending_entry_order_id = "auto-canceled"
        bot.exchange.order_statuses["auto-canceled"] = {
            "id": "auto-canceled",
            "type": "trigger",
            "status": "closed",
            "info": {
                "status": "finished",
                "finish_as": "cancelled",
                "trade_id": 0,
            },
        }

        self.assertEqual(
            bot._resolve_missing_pending_entry_order({"side": "long"}),
            "canceled_zero_fill",
        )

    def test_gate_finished_trigger_without_child_or_cancel_reason_is_unknown(self):
        bot = make_bot("long", layer=2, trades=[])
        bot.state.pending_layer = 3
        bot.state.pending_entry_order_id = "auto-ambiguous"
        bot.exchange.order_statuses["auto-ambiguous"] = {
            "id": "auto-ambiguous",
            "type": "trigger",
            "status": "closed",
            "info": {
                "status": "finished",
                "finish_as": "succeeded",
                "trade_id": 0,
            },
        }

        self.assertEqual(
            bot._resolve_missing_pending_entry_order({"side": "long"}),
            "unknown",
        )


class LayerSpacingTests(unittest.TestCase):
    def test_long_and_short_spacing_use_only_verified_last_fill(self):
        long_bot = make_bot("long", last_fill=90, source="trade_history", order_id="add-2")
        long_bot._gap_ratio = lambda **kwargs: 0.01
        long_price = long_bot._enforce_layer_spacing(95, "long", 3, 99, 94)
        self.assertAlmostEqual(long_price, 89.1)

        short_bot = make_bot("short", last_fill=110, source="trade_history", order_id="add-2")
        short_bot._gap_ratio = lambda **kwargs: 0.01
        short_price = short_bot._enforce_layer_spacing(105, "short", 3, 101, 106)
        self.assertAlmostEqual(short_price, 111.1)

    def test_short_price_precision_rounds_outward(self):
        bot = make_bot("short", last_fill=100, source="trade_history", order_id="add-2")
        quantized = bot._layer_price_to_safe_precision(100.15, "short", 100, 0.0015)
        self.assertAlmostEqual(quantized, 100.2)

    def test_structure_plan_is_also_forced_through_spacing(self):
        bot = make_bot("long", layer=2, last_fill=100, source="trade_history", order_id="add-2")
        bot.min_balance = 1.0
        bot.leverage = 1.0
        bot.max_layers = 5
        bot._current_phase = lambda position, current_price=None: "PHASE1"
        phase_cfg = {
            "phase": "PHASE1",
            "max_layers": 5,
            "first_order_ratio": 0.01,
            "layer_multipliers": [1, 1, 1, 1, 1],
            "layer_index_offset": 0,
        }
        bot._phase_config = lambda phase=None: phase_cfg
        bot._select_structure_entry_price = lambda *args, **kwargs: (99.8, {"atr": 0.0})
        bot._extract_latest_atr = lambda **kwargs: (0.0, 0.0)
        bot._gap_ratio = lambda **kwargs: 0.01
        bot._normalize_amount = lambda amount: amount
        bot.get_balance_snapshot = lambda: {"equity": 1000.0, "safe_tradable_margin": 1000.0}

        plan = bot._build_add_order_plan(
            3,
            105,
            position={"side": "long", "entryPrice": 103, "contracts": 2},
        )

        self.assertIsNotNone(plan)
        self.assertEqual(plan["source"], "STRUCTURE")
        self.assertLessEqual(plan["execute_price"], 99.0)
        self.assertTrue(bot._is_add_order_plan_spacing_valid(plan))

    def test_refresh_hysteresis_cannot_keep_an_unsafe_existing_order(self):
        bot = make_bot("long", last_fill=90, source="trade_history", order_id="add-2")
        bot.entry_amount_refresh_tolerance = 0.1
        bot.structure_refresh_threshold = 0.008
        bot._normalize_amount = lambda amount: amount
        plan = {
            "order_side": "buy",
            "amount": 1.0,
            "entry_price": 89.1,
            "execute_price": 89.1,
            "trigger_price": 0.0,
            "side": "long",
            "spacing_ratio": 0.01,
        }
        existing = [{"side": "buy", "type": "limit", "price": 89.5, "amount": 1.0}]

        reason = bot._entry_order_refresh_reason(existing, plan)
        self.assertIn("最小层间距", reason)

    def test_submit_rejects_api_error_or_changed_position_before_order_creation(self):
        bot = make_bot("long", last_fill=100, source="trade_history", order_id="add-2")
        bot.max_layers = 5
        plan = {
            "phase": "PHASE1",
            "layer_num": 3,
            "order_side": "buy",
            "amount": 1.0,
            "execute_price": 99.0,
            "trigger_price": 0.0,
            "position": {"side": "long", "contracts": 2.0},
            "side": "long",
            "spacing_ratio": 0.01,
        }

        bot.get_active_position = lambda: bot._POSITION_API_ERROR
        self.assertFalse(bot._submit_add_order_plan(plan))
        self.assertEqual(bot.exchange.created_orders, [])

        bot.get_active_position = lambda: {"side": "long", "contracts": 2.5}
        self.assertFalse(bot._submit_add_order_plan(plan))
        self.assertEqual(bot.exchange.created_orders, [])

        bot.get_active_position = lambda: {"side": "long", "contracts": 2.0}
        bot.exchange.open_orders = [
            {"id": "unexpected", "side": "buy", "reduceOnly": False}
        ]
        self.assertFalse(bot._submit_add_order_plan(plan))
        self.assertEqual(bot.exchange.created_orders, [])

    def test_final_position_check_and_trigger_submit_share_action_lock(self):
        bot = make_bot("long", last_fill=100, source="trade_history", order_id="add-2")
        bot.max_layers = 5
        bot.layer_trigger_type = "mark_price"
        lock_observations = []
        bot.get_active_position = lambda: (
            lock_observations.append(bot.action_lock._is_owned())
            or {"side": "long", "contracts": 2.0}
        )
        bot._save_runtime_state = lambda: lock_observations.append(
            bot.action_lock._is_owned()
        )
        bot._write_live_snapshot = lambda **kwargs: lock_observations.append(
            bot.action_lock._is_owned()
        )
        original_create_trigger = bot.exchange.create_trigger_order

        def create_trigger(*args, **kwargs):
            lock_observations.append(bot.action_lock._is_owned())
            return original_create_trigger(*args, **kwargs)

        bot.exchange.create_trigger_order = create_trigger
        plan = {
            "phase": "PHASE1",
            "layer_num": 3,
            "order_side": "buy",
            "amount": 1.0,
            "execute_price": 99.0,
            "trigger_price": 99.5,
            "position": {"side": "long", "contracts": 2.0},
            "side": "long",
            "spacing_ratio": 0.01,
        }

        self.assertTrue(bot._submit_add_order_plan(plan))
        self.assertEqual(lock_observations, [True, True, True, False])
        self.assertEqual(len(bot.exchange.created_orders), 1)

    def test_cancel_boundary_fill_blocks_replacement_order(self):
        trades = [
            {
                "id": "fill-1",
                "order": "old-add",
                "timestamp": 1000,
                "side": "buy",
                "price": 99.0,
                "amount": 0.25,
                "info": {},
            }
        ]
        bot = make_bot(
            "long",
            last_fill=100,
            source="trade_history",
            order_id="add-2",
            trades=trades,
        )
        bot.max_layers = 5
        bot.get_active_position = lambda: {"side": "long", "contracts": 2.0}
        plan = {
            "phase": "PHASE1",
            "layer_num": 3,
            "order_side": "buy",
            "amount": 1.0,
            "execute_price": 99.0,
            "trigger_price": 0.0,
            "position": {"side": "long", "contracts": 2.0},
            "side": "long",
            "spacing_ratio": 0.01,
            "replaced_entry_order_ids": ["old-add"],
        }

        self.assertFalse(bot._submit_add_order_plan(plan))
        self.assertEqual(bot.exchange.created_orders, [])
        self.assertEqual(bot.state.last_fill_price, 0.0)
        self.assertEqual(bot.state.unresolved_fill_order_id, "old-add")


class FirstOrderConcurrencyTests(unittest.TestCase):
    def test_first_order_submission_and_state_commit_share_action_lock(self):
        bot = make_bot("long", layer=0)
        bot.min_balance = 1.0
        bot.first_order_ratio = 0.1
        bot.leverage = 1.0
        bot.get_balance_snapshot = lambda: {
            "equity": 1000.0,
            "safe_tradable_margin": 1000.0,
        }
        bot._calculate_order_margin = lambda desired, snapshot, label: desired
        bot.get_trend_context = lambda: None
        bot.find_support_resistance = lambda: None
        bot._build_first_entry_plan = lambda *args, **kwargs: {
            "entry_type": "limit",
            "entry_price": 100.0,
            "source": "TEST",
        }
        bot._normalize_amount = lambda amount: amount
        lock_observations = []
        bot.get_active_position = lambda: (
            lock_observations.append(bot.action_lock._is_owned()) or None
        )
        original_create_order = bot.exchange.create_order

        def create_order(*args, **kwargs):
            lock_observations.append(bot.action_lock._is_owned())
            return original_create_order(*args, **kwargs)

        bot.exchange.create_order = create_order
        bot._save_runtime_state = lambda: lock_observations.append(
            bot.action_lock._is_owned()
        )
        bot._write_live_snapshot = lambda **kwargs: lock_observations.append(
            bot.action_lock._is_owned()
        )

        self.assertTrue(bot.place_first_order("long", 100.0))
        self.assertEqual(lock_observations, [True, True, True, False])
        self.assertEqual(bot.state.pending_entry_order_id, "order-1")
        self.assertEqual(bot.state.bot_state, "IN_STRATEGY")


class CancellationSafetyTests(unittest.TestCase):
    def test_cancel_requires_target_to_disappear_before_success(self):
        bot = make_bot("long", last_fill=100, source="trade_history", order_id="add-2")
        old_order = {
            "id": "old-add",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "price": 99.0,
            "reduceOnly": False,
        }
        bot.exchange.open_orders = [old_order]
        bot.exchange.cancel_removes_orders = False

        with mock.patch.object(martin.time, "sleep", return_value=None):
            self.assertFalse(bot._cancel_entry_orders([old_order]))
        self.assertEqual(bot.exchange.open_orders, [old_order])

    def test_zero_fill_cancel_response_is_persisted_when_gate_order_disappears(self):
        bot = make_bot("long", layer=2, last_fill=90, source="trade_history", order_id="add-2")
        old_order = {
            "id": "add-3",
            "clientOrderId": "t-martin-add-3",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "filled": 0.0,
            "remaining": 1.0,
            "price": 80.0,
            "reduceOnly": False,
        }
        bot.state.pending_layer = 3
        bot.state.pending_entry_order_id = "add-3"
        bot.state.pending_entry_client_oid = "t-martin-add-3"
        bot.exchange.open_orders = [old_order]
        bot.exchange.cancel_results = [
            {
                "id": "add-3",
                "clientOrderId": "t-martin-add-3",
                "status": "canceled",
                "amount": 1.0,
                "filled": 0.0,
                "remaining": 1.0,
                "info": {"finish_as": "cancelled"},
            }
        ]

        self.assertTrue(bot._cancel_entry_orders([old_order]))
        self.assertEqual(bot.state.zero_fill_canceled_entry_order_id, "add-3")
        # Gate may now return ORDER_NOT_FOUND for this canceled order.  The
        # persisted cancel response + empty authoritative trade window is enough.
        self.assertEqual(
            bot._resolve_missing_pending_entry_order({"side": "long"}),
            "canceled_zero_fill",
        )
        self.assertEqual(bot._entry_order_fill_barrier(["add-3"]), "")
        bot._clear_pending_entry_tracking(
            reset_pending_layer=True,
            clear_unresolved_order_id="add-3",
        )
        self.assertEqual(bot.state.zero_fill_canceled_entry_order_id, "")

    def test_gate_canceled_trigger_parent_passes_fill_barrier(self):
        bot = make_bot("long", layer=2, trades=[])
        bot.exchange.order_statuses["auto-canceled"] = {
            "id": "auto-canceled",
            "type": "trigger",
            "status": "closed",
            "info": {
                "status": "finished",
                "finish_as": "cancelled",
                "trade_id": 0,
            },
        }

        self.assertEqual(
            bot._entry_order_fill_barrier(["auto-canceled"]),
            "",
        )

    def test_gate_ambiguous_trigger_parent_blocks_fill_barrier(self):
        bot = make_bot("long", layer=2, trades=[])
        bot.exchange.order_statuses["auto-ambiguous"] = {
            "id": "auto-ambiguous",
            "type": "trigger",
            "status": "closed",
            "info": {
                "status": "finished",
                "finish_as": "succeeded",
                "trade_id": 0,
            },
        }

        self.assertIsNone(
            bot._entry_order_fill_barrier(["auto-ambiguous"])
        )

    def test_gate_trigger_child_fill_is_detected_by_parent_barrier(self):
        trades = [
            {
                "id": "child-fill",
                "order": "child-order",
                "timestamp": 2000,
                "side": "buy",
                "price": 90.0,
                "amount": 0.4,
                "info": {},
            }
        ]
        bot = make_bot("long", layer=2, trades=trades)
        bot.exchange.order_statuses["auto-order"] = {
            "id": "auto-order",
            "type": "trigger",
            "status": "closed",
            "triggerExecutionOrderId": "child-order",
            "info": {"trade_id": "child-order"},
        }

        self.assertEqual(
            bot._entry_order_fill_barrier(["auto-order"]),
            "child-order",
        )

    def test_trigger_zero_fill_cancel_requires_explicit_zero_child_id(self):
        bot = make_bot("long", layer=2, trades=[])
        target = {
            "id": "auto-order",
            "type": "trigger",
            "amount": 1.0,
            "filled": 0.0,
        }
        base_response = {
            "id": "auto-order",
            "type": "trigger",
            "status": "canceled",
            "amount": 1.0,
            "filled": 0.0,
            "remaining": 1.0,
        }

        self.assertFalse(
            bot._cancel_response_confirms_zero_fill(target, base_response)
        )
        zero_child = dict(base_response)
        zero_child["info"] = {"trade_id": 0, "status": "canceled"}
        self.assertTrue(
            bot._cancel_response_confirms_zero_fill(target, zero_child)
        )
        has_child = dict(base_response)
        has_child["info"] = {
            "trade_id": "child-order",
            "status": "canceled",
        }
        self.assertFalse(
            bot._cancel_response_confirms_zero_fill(target, has_child)
        )

    def test_residual_order_cancels_any_extra_add_order(self):
        bot = make_bot("long", last_fill=99, source="trade_history", order_id="residual")
        bot._normalize_amount = lambda amount: amount
        residual = {
            "id": "residual",
            "clientOrderId": "t-martin-residual",
            "side": "buy",
            "type": "limit",
            "amount": 0.75,
            "price": 99.0,
            "reduceOnly": False,
        }
        extra = {
            "id": "extra",
            "clientOrderId": "t-martin-extra",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "price": 98.0,
            "reduceOnly": False,
        }
        bot.exchange.open_orders = [residual, extra]

        self.assertTrue(
            bot._retain_residual_fill_order(
                [residual, extra],
                "PHASE1",
                {"side": "long", "contracts": 2.0},
            )
        )
        self.assertEqual([order["id"] for order in bot.exchange.open_orders], ["residual"])
        self.assertEqual(bot.state.pending_entry_order_id, "residual")

    def test_old_cycle_residual_is_canceled_and_never_retained(self):
        bot = make_bot(
            "long",
            layer=2,
            last_fill=90.0,
            source="trade_history",
            order_id="old-order",
        )
        bot.state.last_fill_amount = 1.0
        bot.state.last_fill_time = "2023-11-14T22:13:20"
        residual = {
            "id": "old-order",
            "clientOrderId": "t-martin-old-order",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "price": 89.0,
            "reduceOnly": False,
        }
        bot.exchange.open_orders = [residual]
        position = {
            "side": "long",
            "contracts": 2.0,
            "timestamp": 1_700_000_060_000,
        }

        self.assertFalse(
            bot._retain_residual_fill_order([residual], "PHASE1", position)
        )
        self.assertEqual(bot.exchange.open_orders, [])
        self.assertEqual(bot.exchange.created_orders, [])
        self.assertEqual(bot.state.last_fill_price, 0.0)
        self.assertEqual(bot.state.unresolved_fill_order_id, "old-order")


class StateSyncTests(unittest.TestCase):
    @staticmethod
    def _prepare_full_position_sync(bot, position, open_orders):
        bot.max_layers = 9
        bot._fetch_active_position = lambda suppress_error=False: (True, position)
        bot.fetch_open_orders = lambda: list(open_orders)
        bot._force_sync_position_side = lambda *args, **kwargs: True
        bot._cancel_wrong_direction_entry_orders = lambda *args, **kwargs: True
        bot.get_wallet_balance = lambda: 1000.0
        bot.estimate_current_layer = lambda *args, **kwargs: 1
        bot._current_phase = lambda *args, **kwargs: "PHASE1"
        bot._repair_phase2_start_layer = lambda: False
        bot._normalize_amount = lambda amount: amount

    def test_startup_revalidates_current_layer_fill_not_next_pending_order(self):
        trades = [
            {
                "id": "first-fill",
                "order": "first-order",
                "timestamp": 1000,
                "side": "buy",
                "price": 100.0,
                "amount": 1.0,
                "info": {"text": "t-martin-first"},
            }
        ]
        bot = make_bot(
            "long",
            layer=1,
            last_fill=100.0,
            source="trade_history",
            order_id="first-order",
            trades=trades,
        )
        bot.state.last_fill_amount = 1.0
        bot.state.pending_layer = 2
        bot.state.pending_entry_order_id = "add-2"
        bot.state.pending_entry_client_oid = "t-martin-add-2"
        bot.state.last_known_contracts = 1.0
        position = {"side": "long", "contracts": 1.0, "entryPrice": 100.0}
        open_orders = [
            {
                "id": "add-2",
                "clientOrderId": "t-martin-add-2",
                "side": "buy",
                "type": "limit",
                "price": 99.0,
                "amount": 1.0,
                "reduceOnly": False,
            }
        ]
        self._prepare_full_position_sync(bot, position, open_orders)
        bot._cancel_entry_orders = lambda orders: self.fail(
            "a valid pending add order must not be canceled while revalidating layer 1"
        )

        bot.sync_state_with_exchange()

        self.assertEqual(bot.state.last_fill_order_id, "first-order")
        self.assertEqual(bot.state.last_fill_price, 100.0)
        self.assertEqual(bot.state.pending_entry_order_id, "add-2")
        self.assertEqual(bot.state.pending_layer, 2)
        self.assertEqual(bot.state.unresolved_fill_order_id, "")

    def test_startup_then_build_plan_rejects_verified_anchor_from_old_cycle(self):
        position = {
            "side": "long",
            "contracts": 2.0,
            "entryPrice": 95.0,
            "timestamp": 1_700_000_060_000,
            "markPrice": 95.0,
        }
        bot = make_bot(
            "long",
            layer=2,
            last_fill=90.0,
            source="trade_history",
            order_id="old-add",
        )
        bot.state.last_fill_amount = 1.0
        bot.state.last_fill_time = "2023-11-14T22:13:20"
        bot.state.last_known_contracts = 2.0
        bot.enforce_exchange_position_sync = lambda reason="": (True, position)
        bot.fetch_open_orders = lambda: []

        bot._reconcile_startup_entry_orders()

        self.assertIsNone(
            bot._build_add_order_plan(
                3,
                95.0,
                position=position,
            )
        )
        self.assertEqual(bot.exchange.created_orders, [])
        self.assertEqual(bot.state.last_fill_price, 0.0)
        self.assertEqual(bot.state.unresolved_fill_order_id, "old-add")

    def test_build_plan_rechecks_position_cycle_for_verified_anchor(self):
        bot = make_bot(
            "long",
            layer=2,
            last_fill=90.0,
            source="trade_history",
            order_id="old-add",
        )
        bot.state.last_fill_amount = 1.0
        bot.state.last_fill_time = "2023-11-14T22:13:20"
        position = {
            "side": "long",
            "contracts": 2.0,
            "entryPrice": 95.0,
            "info": {"open_time": 1_700_000_060_000},
        }

        self.assertIsNone(
            bot._build_add_order_plan(
                3,
                95.0,
                position=position,
            )
        )
        self.assertEqual(bot.exchange.created_orders, [])
        self.assertEqual(bot.state.layer, 2)
        self.assertEqual(bot.state.last_fill_price, 0.0)

    def test_foreign_same_side_open_order_is_not_adopted_or_canceled(self):
        bot = make_bot("long", layer=1)
        bot.state.last_known_contracts = 1.0
        manual_order = {
            "id": "manual-open",
            "clientOrderId": "manual-order",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "price": 99.0,
            "reduceOnly": False,
        }
        bot.exchange.open_orders = [manual_order]
        position = {
            "side": "long",
            "contracts": 1.0,
            "entryPrice": 100.0,
        }
        self._prepare_full_position_sync(bot, position, [manual_order])
        bot._cancel_entry_orders = lambda orders: self.fail(
            "foreign order must not be canceled"
        )

        bot.sync_state_with_exchange()

        self.assertEqual(bot.state.pending_entry_order_id, "")
        self.assertEqual(bot.state.pending_entry_client_oid, "")
        self.assertEqual(bot.exchange.open_orders, [manual_order])
        self.assertTrue(bot._state_sync_required)

    def test_startup_foreign_same_side_order_is_not_reconciled(self):
        bot = make_bot("long", layer=1)
        manual_order = {
            "id": "manual-startup",
            "clientOrderId": "manual-startup-order",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "price": 99.0,
            "reduceOnly": False,
        }
        position = {
            "side": "long",
            "contracts": 1.0,
            "entryPrice": 100.0,
            "markPrice": 100.0,
        }
        bot.enforce_exchange_position_sync = lambda reason="": (True, position)
        bot.fetch_open_orders = lambda: [manual_order]
        bot._cancel_entry_orders = lambda orders: self.fail(
            "foreign order must not be canceled during startup"
        )
        bot._reconcile_active_entry_orders = lambda *args, **kwargs: self.fail(
            "foreign order must not be reconciled during startup"
        )

        bot._reconcile_startup_entry_orders()

        self.assertEqual(bot.state.pending_entry_order_id, "")
        self.assertEqual(bot.state.pending_entry_client_oid, "")
        self.assertTrue(bot._state_sync_required)

    def test_startup_reconcile_keeps_safe_existing_next_layer_order(self):
        bot = make_bot(
            "long",
            layer=1,
            last_fill=100.0,
            source="trade_history",
            order_id="first-order",
        )
        bot.max_layers = 9
        position = {
            "side": "long",
            "contracts": 1.0,
            "entryPrice": 100.0,
            "markPrice": 100.0,
        }
        add_order = {
            "id": "add-2",
            "clientOrderId": "t-martin-add-2",
            "side": "buy",
            "type": "limit",
            "price": 99.0,
            "amount": 1.0,
            "reduceOnly": False,
        }
        bot.enforce_exchange_position_sync = lambda reason="": (True, position)
        bot.fetch_open_orders = lambda: [add_order]
        bot._retain_residual_fill_order = lambda *args, **kwargs: False
        bot._live_price_from_ws = lambda *args, **kwargs: 100.0
        bot._current_phase = lambda *args, **kwargs: "PHASE1"
        bot._phase_config = lambda *args, **kwargs: {"max_layers": 3}
        reconciled_layers = []
        bot._reconcile_active_entry_orders = lambda orders, next_layer, *args, **kwargs: (
            reconciled_layers.append(next_layer) or True
        )
        bot.sync_state_with_exchange = lambda: None
        bot._cancel_entry_orders = lambda orders: self.fail(
            "startup reconcile must not cancel a safe existing layer-2 order"
        )

        bot._reconcile_startup_entry_orders()

        self.assertEqual(reconciled_layers, [2])

    def test_startup_repairs_corrupted_layer_one_anchor_from_bot_fill(self):
        trades = [
            {
                "id": "first-fill",
                "order": "first-order",
                "timestamp": 1000,
                "side": "buy",
                "price": 100.0,
                "amount": 1.0,
                "info": {"text": "t-martin-first"},
            }
        ]
        bot = make_bot("long", layer=1, trades=trades)
        bot.state.pending_layer = 1
        bot.state.last_known_contracts = 1.0
        bot.state.unresolved_fill_order_id = "canceled-add-2"
        bot.exchange.order_statuses["first-order"] = {
            "id": "first-order",
            "clientOrderId": "t-martin-first",
            "status": "closed",
            "side": "buy",
            "filled": 1.0,
            "reduceOnly": False,
        }
        # entryPrice is deliberately not identical: it may only validate the trade,
        # while the stored spacing anchor must remain the trade-history VWAP (100.0).
        position = {"side": "long", "contracts": 1.0, "entryPrice": 100.04}
        self._prepare_full_position_sync(bot, position, [])

        bot.sync_state_with_exchange()

        self.assertTrue(bot._has_verified_last_fill_price())
        self.assertEqual(bot.state.last_fill_order_id, "first-order")
        self.assertEqual(bot.state.last_fill_price, 100.0)
        self.assertEqual(bot.state.last_fill_price_source, "trade_history")
        self.assertEqual(bot.state.unresolved_fill_order_id, "")

    def test_position_growth_must_equal_pending_order_fill_before_layer_promotion(self):
        trades = [
            {
                "id": "bot-add",
                "order": "add-2",
                "timestamp": 2000,
                "side": "buy",
                "price": 90.0,
                "amount": 1.15,
                "info": {},
            },
            {
                "id": "manual-add",
                "order": "manual",
                "timestamp": 2001,
                "side": "buy",
                "price": 91.0,
                "amount": 1.0,
                "info": {},
            },
        ]
        bot = make_bot(
            "long",
            layer=1,
            last_fill=100.0,
            source="trade_history",
            order_id="first-order",
            trades=trades,
        )
        bot.state.last_fill_amount = 2.0
        bot.state.pending_layer = 2
        bot.state.pending_entry_order_id = "add-2"
        bot.state.last_known_contracts = 2.0
        position = {"side": "long", "contracts": 4.15, "entryPrice": 95.0}
        self._prepare_full_position_sync(bot, position, [])

        bot.sync_state_with_exchange()

        self.assertEqual(bot.state.layer, 1)
        self.assertEqual(bot.state.last_known_contracts, 2.0)
        self.assertEqual(bot.state.unresolved_fill_order_id, "add-2")
        self.assertFalse(bot._has_verified_last_fill_price())

    def test_partial_fill_with_transient_empty_open_view_keeps_pending_identity(self):
        trades = [
            {
                "id": "partial-fill",
                "order": "add-2",
                "timestamp": 2000,
                "side": "buy",
                "price": 90.0,
                "amount": 0.4,
                "info": {},
            }
        ]
        bot = make_bot(
            "long",
            layer=1,
            last_fill=100.0,
            source="trade_history",
            order_id="first-order",
            trades=trades,
        )
        bot.state.last_fill_amount = 1.0
        bot.state.pending_layer = 2
        bot.state.pending_entry_order_id = "add-2"
        bot.state.pending_entry_client_oid = "t-martin-add-2"
        bot.state.last_known_contracts = 1.0
        bot.exchange.order_statuses["add-2"] = {
            "id": "add-2",
            "status": "partially_filled",
            "filled": 0.4,
            "side": "buy",
            "reduceOnly": False,
        }
        position = {"side": "long", "contracts": 1.4, "entryPrice": 97.0}
        # Reproduce a transient Gate snapshot that omits the still-open residual.
        self._prepare_full_position_sync(bot, position, [])

        bot.sync_state_with_exchange()

        self.assertEqual(bot.state.layer, 2)
        self.assertEqual(bot.state.last_known_contracts, 1.4)
        self.assertEqual(bot.state.last_fill_order_id, "add-2")
        self.assertEqual(bot.state.pending_entry_order_id, "add-2")
        self.assertEqual(bot.state.pending_entry_client_oid, "t-martin-add-2")
        self.assertEqual(
            bot._resolve_missing_pending_entry_order(position),
            "open",
        )

    def test_unresolved_fill_is_not_verified_before_position_delta_arrives(self):
        recovered = {
            "order_id": "add-3",
            "price": 80.0,
            "amount": 0.4,
            "timestamp_ms": 3000,
        }
        trades = [
            {
                "id": "partial-fill",
                "order": "add-3",
                "timestamp": 3000,
                "side": "buy",
                "price": 80.0,
                "amount": 0.4,
                "info": {},
            }
        ]
        bot = make_bot(
            "long",
            layer=2,
            last_fill=90.0,
            source="trade_history",
            order_id="add-2",
            trades=trades,
        )
        bot.state.last_fill_amount = 1.0
        bot.state.pending_layer = 3
        bot.state.pending_entry_order_id = "add-3"
        bot.state.last_known_contracts = 2.0
        bot._record_unresolved_entry_fill("add-3", recovered)
        add_order = {
            "id": "add-3",
            "side": "buy",
            "type": "limit",
            "price": 80.0,
            "amount": 1.0,
            "filled": 0.4,
            "reduceOnly": False,
        }

        lagging_position = {
            "side": "long",
            "contracts": 2.0,
            "entryPrice": 95.0,
        }
        self._prepare_full_position_sync(bot, lagging_position, [add_order])
        bot.sync_state_with_exchange()

        self.assertFalse(bot._has_verified_last_fill_price())
        self.assertEqual(bot.state.unresolved_fill_order_id, "add-3")
        self.assertEqual(bot.state.unresolved_fill_accounted_amount, 0.0)
        self.assertEqual(bot.state.pending_entry_order_id, "add-3")

        caught_up_position = {
            "side": "long",
            "contracts": 2.4,
            "entryPrice": 92.5,
        }
        self._prepare_full_position_sync(bot, caught_up_position, [add_order])
        bot.sync_state_with_exchange()

        self.assertTrue(bot._has_verified_last_fill_price())
        self.assertEqual(bot.state.last_fill_order_id, "add-3")
        self.assertEqual(bot.state.last_fill_amount, 0.4)
        self.assertEqual(bot.state.layer, 3)
        self.assertEqual(bot.state.pending_entry_order_id, "add-3")

    def test_new_residual_fill_waits_for_position_before_advancing_vwap(self):
        trades = [
            {
                "id": "part-1",
                "order": "add-3",
                "timestamp": 3000,
                "side": "buy",
                "price": 80.0,
                "amount": 0.4,
                "info": {},
            },
            {
                "id": "part-2",
                "order": "add-3",
                "timestamp": 3001,
                "side": "buy",
                "price": 79.0,
                "amount": 0.2,
                "info": {},
            },
        ]
        bot = make_bot(
            "long",
            layer=3,
            last_fill=80.0,
            source="trade_history",
            order_id="add-3",
            trades=trades,
        )
        bot.state.last_fill_amount = 0.4
        bot.state.pending_layer = 3
        bot.state.pending_entry_order_id = "add-3"
        bot.state.last_known_contracts = 2.4
        residual_order = {
            "id": "add-3",
            "side": "buy",
            "type": "limit",
            "price": 79.0,
            "amount": 1.0,
            "filled": 0.6,
            "reduceOnly": False,
        }
        lagging_position = {
            "side": "long",
            "contracts": 2.4,
            "entryPrice": 92.0,
        }
        self._prepare_full_position_sync(
            bot,
            lagging_position,
            [residual_order],
        )

        bot.sync_state_with_exchange()

        self.assertFalse(bot._has_verified_last_fill_price())
        self.assertEqual(bot.state.unresolved_fill_order_id, "add-3")
        self.assertEqual(bot.state.unresolved_fill_accounted_amount, 0.4)
        self.assertEqual(bot.state.pending_layer, 3)
        self.assertEqual(bot.state.layer, 3)

        caught_up_position = {
            "side": "long",
            "contracts": 2.6,
            "entryPrice": 91.0,
        }
        self._prepare_full_position_sync(
            bot,
            caught_up_position,
            [residual_order],
        )
        bot.sync_state_with_exchange()

        self.assertTrue(bot._has_verified_last_fill_price())
        self.assertAlmostEqual(bot.state.last_fill_amount, 0.6)
        self.assertAlmostEqual(
            bot.state.last_fill_price,
            (80.0 * 0.4 + 79.0 * 0.2) / 0.6,
        )
        self.assertEqual(bot.state.layer, 3)
        self.assertEqual(bot.state.pending_layer, 3)

    def test_terminal_partial_fill_uses_persisted_accounted_amount(self):
        bot = make_bot("long", layer=3, trades=[])
        bot.state.pending_layer = 3
        bot.state.pending_entry_order_id = "add-3"
        bot.state.unresolved_fill_order_id = "add-3"
        bot.state.unresolved_fill_accounted_amount = 0.4
        bot.exchange.order_statuses["add-3"] = {
            "id": "add-3",
            "status": "canceled",
            "filled": 0.4,
        }

        self.assertEqual(
            bot._resolve_missing_pending_entry_order({"side": "long"}),
            "known_terminal",
        )

    def test_side_change_cancel_failure_preserves_old_order_identity(self):
        bot = make_bot("long", layer=2, trades=[])
        bot.state.pending_layer = 3
        bot.state.pending_entry_order_id = "old-long-add-3"
        bot.state.pending_entry_client_oid = "t-martin-old-add-3"
        bot.state.last_known_contracts = 2.0
        old_order = {
            "id": "old-long-add-3",
            "clientOrderId": "t-martin-old-add-3",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "filled": 0.0,
            "price": 80.0,
            "reduceOnly": False,
        }
        bot.exchange.open_orders = [old_order]
        bot.exchange.cancel_removes_orders = False
        bot.fetch_open_orders = lambda: list(bot.exchange.open_orders)
        bot._fetch_active_position = lambda suppress_error=False: (
            True,
            {"side": "short", "contracts": 1.0, "entryPrice": 110.0},
        )

        with mock.patch.object(martin.time, "sleep", return_value=None):
            ok, _ = bot.enforce_exchange_position_sync(
                reason="test side change",
                sync_contracts=False,
            )

        self.assertFalse(ok)
        self.assertEqual(bot.state.position_side, "long")
        self.assertEqual(bot.state.layer, 2)
        self.assertEqual(bot.state.pending_layer, 3)
        self.assertEqual(bot.state.pending_entry_order_id, "old-long-add-3")
        self.assertEqual(bot.state.pending_entry_client_oid, "t-martin-old-add-3")

    def test_side_change_cannot_reuse_stale_pending_layer_or_manual_trade(self):
        trades = [
            {
                "id": "manual-short",
                "order": "manual-short-order",
                "timestamp": 3000,
                "side": "sell",
                "price": 110.0,
                "amount": 1.0,
                "info": {},
            }
        ]
        bot = make_bot("long", layer=2, trades=trades)
        bot.state.pending_layer = 3
        bot.state.pending_entry_order_id = "stale-long-add-3"
        bot.state.last_known_contracts = 2.0
        bot.exchange.order_statuses["manual-short-order"] = {
            "id": "manual-short-order",
            "clientOrderId": "manual",
            "status": "closed",
            "side": "sell",
            "filled": 1.0,
            "reduceOnly": False,
        }
        bot.exchange.order_statuses["stale-long-add-3"] = {
            "id": "stale-long-add-3",
            "status": "canceled",
            "filled": 0.0,
        }
        position = {"side": "short", "contracts": 1.0, "entryPrice": 110.0}
        self._prepare_full_position_sync(bot, position, [])
        bot._force_sync_position_side = types.MethodType(
            martin.MartinBot._force_sync_position_side,
            bot,
        )

        bot.sync_state_with_exchange()

        self.assertEqual(bot.state.position_side, "short")
        self.assertEqual(bot.state.layer, 1)
        self.assertEqual(bot.state.pending_layer, 1)
        self.assertNotEqual(bot.state.layer, 3)
        self.assertEqual(bot.state.last_fill_order_id, "")

    def test_no_position_sync_uses_non_reduce_entry_order_identity(self):
        bot = make_bot("long", layer=0)
        bot.state.pending_layer = 0
        bot.state.last_known_contracts = 0.0
        bot.state.last_fill_price = 90.0
        bot.state.last_fill_order_id = "old-cycle"
        bot.state.last_fill_price_source = "trade_history"
        bot.state.unresolved_fill_order_id = "stale-order"
        bot._fetch_active_position = lambda suppress_error=False: (True, None)
        bot._normalize_amount = lambda amount: amount
        bot.fetch_open_orders = lambda: [
            {
                "id": "reduce-only",
                "side": "sell",
                "amount": 0.5,
                "price": 110.0,
                "reduceOnly": True,
            },
            {
                "id": "first-entry",
                "clientOrderId": "t-martin-first-client",
                "side": "buy",
                "amount": 1.0,
                "price": 100.0,
                "reduceOnly": False,
            },
        ]

        bot.sync_state_with_exchange()

        self.assertEqual(bot.state.position_side, "long")
        self.assertEqual(bot.state.pending_entry_price, 100.0)
        self.assertEqual(bot.state.pending_entry_amount, 1.0)
        self.assertEqual(bot.state.pending_entry_order_id, "first-entry")
        self.assertEqual(bot.state.pending_entry_client_oid, "t-martin-first-client")
        self.assertEqual(bot.state.last_fill_price, 0.0)
        self.assertEqual(bot.state.last_fill_order_id, "")
        self.assertEqual(bot.state.unresolved_fill_order_id, "")

    def test_no_position_sync_rejects_multiple_entry_orders(self):
        bot = make_bot("long", layer=0)
        bot.state.pending_layer = 0
        bot._fetch_active_position = lambda suppress_error=False: (True, None)
        bot.fetch_open_orders = lambda: [
            {"id": "first-a", "clientOrderId": "t-martin-first-a", "side": "buy", "reduceOnly": False},
            {"id": "first-b", "clientOrderId": "t-martin-first-b", "side": "buy", "reduceOnly": False},
        ]
        cleanup_reasons = []
        bot._finalize_full_exit = lambda reason="": cleanup_reasons.append(reason) or True

        bot.sync_state_with_exchange()

        self.assertEqual(cleanup_reasons, ["无仓异常开仓单清理"])
        self.assertNotEqual(bot.state.pending_entry_order_id, "first-a")

    def test_filled_first_order_is_preserved_while_position_api_lags(self):
        trades = [
            {
                "id": "fill-1",
                "order": "first-entry",
                "timestamp": 1000,
                "side": "buy",
                "price": 100.0,
                "amount": 1.0,
                "info": {},
            }
        ]
        bot = make_bot("long", layer=1, trades=trades)
        bot.state.pending_entry_order_id = "first-entry"
        bot.state.pending_entry_client_oid = "first-client"
        bot.state.last_known_contracts = 0.0
        bot._fetch_active_position = lambda suppress_error=False: (True, None)
        bot.fetch_open_orders = lambda: []

        bot.sync_state_with_exchange()

        self.assertEqual(bot.state.bot_state, "IN_STRATEGY")
        self.assertEqual(bot.state.pending_entry_order_id, "first-entry")
        self.assertEqual(bot.state.last_fill_order_id, "first-entry")
        self.assertEqual(bot.state.last_fill_price, 100.0)

    def test_missing_add_order_never_falls_back_to_previous_layer_fill(self):
        trades = [
            {
                "id": "old-fill",
                "order": "add-2",
                "timestamp": 1000,
                "side": "buy",
                "price": 90.0,
                "amount": 1.0,
                "info": {},
            }
        ]
        bot = make_bot(
            "long",
            layer=2,
            last_fill=90.0,
            source="trade_history",
            order_id="add-2",
            trades=trades,
        )
        bot.state.last_fill_amount = 1.0
        bot.state.pending_layer = 3
        bot.state.pending_entry_order_id = "add-3"
        bot.state.pending_entry_client_oid = "client-3"
        bot.state.last_known_contracts = 2.0
        bot._fetch_active_position = lambda suppress_error=False: (
            True,
            {"side": "long", "contracts": 2.0, "entryPrice": 95.0},
        )
        bot.fetch_open_orders = lambda: []

        bot.sync_state_with_exchange()

        self.assertEqual(bot.state.pending_layer, 3)
        self.assertEqual(bot.state.pending_entry_order_id, "add-3")
        self.assertEqual(bot.state.last_fill_order_id, "add-2")
        self.assertEqual(bot.state.unresolved_fill_order_id, "")

    def test_missing_add_fill_is_held_until_position_catches_up(self):
        trades = [
            {
                "id": "new-fill",
                "order": "add-3",
                "timestamp": 2000,
                "side": "buy",
                "price": 80.0,
                "amount": 1.0,
                "info": {},
            }
        ]
        bot = make_bot(
            "long",
            layer=2,
            last_fill=90.0,
            source="trade_history",
            order_id="add-2",
            trades=trades,
        )
        bot.state.last_fill_amount = 1.0
        bot.state.pending_layer = 3
        bot.state.pending_entry_order_id = "add-3"
        bot.exchange.order_statuses["add-3"] = {
            "id": "add-3",
            "status": "closed",
            "filled": 1.0,
        }

        resolution = bot._resolve_missing_pending_entry_order(
            {"side": "long", "contracts": 2.0}
        )

        self.assertEqual(resolution, "filled_wait_position")
        self.assertEqual(bot.state.pending_layer, 3)
        self.assertEqual(bot.state.pending_entry_order_id, "add-3")
        self.assertEqual(bot.state.last_fill_order_id, "add-3")
        self.assertEqual(bot.state.last_fill_amount, 1.0)
        self.assertEqual(bot.state.unresolved_fill_order_id, "add-3")
        self.assertFalse(bot._has_verified_last_fill_price())

    def test_only_confirmed_zero_fill_cancel_releases_missing_add(self):
        bot = make_bot(
            "long",
            layer=2,
            last_fill=90.0,
            source="trade_history",
            order_id="add-2",
            trades=[],
        )
        bot.state.last_fill_amount = 1.0
        bot.state.pending_layer = 3
        bot.state.pending_entry_order_id = "add-3"
        bot.exchange.order_statuses["add-3"] = {
            "id": "add-3",
            "status": "canceled",
            "filled": 0.0,
        }

        self.assertEqual(
            bot._resolve_missing_pending_entry_order({"side": "long"}),
            "canceled_zero_fill",
        )
        bot._clear_pending_entry_tracking(reset_pending_layer=True)
        self.assertEqual(bot.state.pending_layer, 2)
        self.assertEqual(bot.state.pending_entry_order_id, "")
        self.assertEqual(bot.state.last_fill_order_id, "add-2")

    def test_client_oid_only_pending_identity_is_preserved_and_can_recover_fill(self):
        trades = [
            {
                "id": "fill-3",
                "order": "add-3",
                "timestamp": 3000,
                "side": "buy",
                "price": 80.0,
                "amount": 1.0,
                "info": {},
            }
        ]
        bot = make_bot("long", layer=2, trades=trades)
        bot.state.pending_layer = 3
        bot.state.pending_entry_order_id = ""
        bot.state.pending_entry_client_oid = "client-3"
        bot.exchange.fetch_order_by_client_id_authoritative = lambda client_oid, symbol: {
            "id": "add-3",
            "clientOrderId": client_oid,
            "status": "closed",
        }

        resolution = bot._resolve_missing_pending_entry_order({"side": "long"})

        self.assertEqual(resolution, "filled_wait_position")
        self.assertEqual(bot.state.pending_entry_order_id, "add-3")
        self.assertEqual(bot.state.pending_entry_client_oid, "client-3")
        self.assertEqual(bot.state.unresolved_fill_order_id, "add-3")

    def test_sync_never_clears_client_oid_only_pending_order(self):
        bot = make_bot("long", layer=2, trades=[])
        bot.state.pending_layer = 3
        bot.state.pending_entry_order_id = ""
        bot.state.pending_entry_client_oid = "client-3"
        bot.state.last_known_contracts = 2.0
        bot._fetch_active_position = lambda suppress_error=False: (
            True,
            {"side": "long", "contracts": 2.0, "entryPrice": 95.0},
        )
        bot.fetch_open_orders = lambda: []

        bot.sync_state_with_exchange()

        self.assertEqual(bot.state.pending_layer, 3)
        self.assertEqual(bot.state.pending_entry_order_id, "")
        self.assertEqual(bot.state.pending_entry_client_oid, "client-3")

    def test_transient_empty_sync_uses_confirmed_exit_path(self):
        bot = make_bot("long", layer=2)
        bot.state.last_known_contracts = 2.0
        bot.state.pending_entry_order_id = ""
        bot.state.pending_entry_client_oid = ""
        bot._fetch_active_position = lambda suppress_error=False: (True, None)
        bot.fetch_open_orders = lambda: []
        finalize_calls = []
        bot._finalize_full_exit = lambda reason="": finalize_calls.append(reason) or False

        bot.sync_state_with_exchange()

        self.assertEqual(finalize_calls, ["状态同步确认周期结束"])
        self.assertEqual(bot.state.bot_state, "IN_STRATEGY")
        self.assertEqual(bot.state.layer, 2)


class IdempotencyTests(unittest.TestCase):
    def test_entry_and_trigger_orders_always_carry_client_oid(self):
        bot = make_bot("long")

        normal = bot._submit_entry_order("buy", 1.0, 100.0, "test")
        trigger = bot._create_trigger_order_idempotent(
            "buy",
            1.0,
            99.0,
            price=98.5,
            trigger_type="mark_price",
            order_type="limit",
        )

        normal_oid = normal["clientOrderId"]
        trigger_oid = trigger["clientOrderId"]
        self.assertTrue(normal_oid.startswith("t-martin-"))
        self.assertTrue(trigger_oid.startswith("t-martin-"))
        self.assertLessEqual(len(normal_oid), 28)
        self.assertLessEqual(len(trigger_oid), 28)
        self.assertNotEqual(normal_oid, trigger_oid)
        self.assertEqual(bot.exchange.created_orders[0]["params"]["clientOrderId"], normal_oid)
        self.assertEqual(bot.exchange.created_orders[1]["params"]["clientOrderId"], trigger_oid)


class LimitCloseSafetyTests(unittest.TestCase):
    def test_cancel_boundary_uses_final_filled_amount(self):
        bot = make_bot("long")
        bot._normalize_amount = lambda amount: round(float(amount), 8)
        snapshots = iter(
            [
                {"id": "close-1", "status": "open", "filled": 0.1},
                {"id": "close-1", "status": "closed", "filled": 0.3},
            ]
        )
        bot._wait_for_order_terminal = lambda *args, **kwargs: next(snapshots)
        bot.exchange.cancel_error = RuntimeError("already filled")

        safe, remaining = bot._resolve_limit_close_remainder("close-1", 0.3, 0.01)

        self.assertTrue(safe)
        self.assertEqual(remaining, 0.0)

    def test_unknown_limit_terminal_never_places_market_supplement(self):
        bot = make_bot("long")
        bot._normalize_amount = lambda amount: amount
        bot._wait_for_order_terminal = lambda *args, **kwargs: {
            "id": "close-1",
            "status": "open",
            "filled": 0.1,
        }

        safe, remaining = bot._resolve_limit_close_remainder("close-1", 0.3, 0.01)

        self.assertFalse(safe)
        self.assertEqual(remaining, 0.0)


class TrailingTakeProfitTests(unittest.TestCase):
    ACTIVATION_MULTIPLIERS = [1.0, 0.9, 0.8, 0.7, 0.6, 0.52, 0.44, 0.37, 0.3]
    DRAWDOWN_MULTIPLIERS = [1.0, 0.92, 0.84, 0.76, 0.68, 0.6, 0.52, 0.44, 0.36]

    @classmethod
    def _configured_bot(cls, layer=1):
        bot = make_bot("long", layer=layer)
        bot.leverage = 3.0
        bot.fee_rate = 0.0005
        bot.max_loss_pct = 0.5
        bot.protective_stop_min_profit_pct = 0.005
        bot.trailing_activation_min_pct = 0.005
        bot.trailing_activation_max_pct = 0.01
        bot.trailing_drawdown_min_ratio = 0.10
        bot.trailing_activation_layer_multipliers = list(cls.ACTIVATION_MULTIPLIERS)
        bot.trailing_drawdown_layer_multipliers = list(cls.DRAWDOWN_MULTIPLIERS)
        bot._live_price_from_ws = lambda *args, **kwargs: 100.0
        bot._refresh_realtime_risk_context = lambda *args, **kwargs: None
        bot._arm_protective_stop = lambda *args, **kwargs: False
        return bot

    def test_activation_remains_market_dynamic_and_is_lower_than_old_first_layer(self):
        bot = self._configured_bot()

        quiet_market = bot._dynamic_tp_values(adx=31, volatility_pct=0.005, layer=1)
        volatile_market = bot._dynamic_tp_values(adx=31, volatility_pct=0.035, layer=1)
        weak_market = bot._dynamic_tp_values(adx=18, volatility_pct=0.035, layer=1)

        self.assertAlmostEqual(quiet_market[0], 0.006)
        self.assertAlmostEqual(volatile_market[0], 0.00975)
        self.assertLess(volatile_market[0], 0.012)
        self.assertGreater(volatile_market[0], quiet_market[0])
        self.assertAlmostEqual(weak_market[0], 0.005)
        self.assertAlmostEqual(quiet_market[1], 0.25)
        self.assertAlmostEqual(weak_market[1], 0.35)

    def test_activation_and_drawdown_get_tighter_at_every_layer(self):
        bot = self._configured_bot()

        values = [
            bot._dynamic_tp_values(adx=31, volatility_pct=0.035, layer=layer)
            for layer in range(1, 10)
        ]

        self.assertTrue(all(values[index][0] > values[index + 1][0] for index in range(8)))
        self.assertTrue(all(values[index][1] > values[index + 1][1] for index in range(8)))
        self.assertAlmostEqual(values[0][0], 0.00975)
        self.assertAlmostEqual(values[-1][0], 0.006425)
        self.assertAlmostEqual(values[0][1], 0.25)
        self.assertAlmostEqual(values[-1][1], 0.10)

    def test_partial_take_profit_runtime_and_handlers_are_removed(self):
        state = martin.RuntimeState()

        self.assertFalse(hasattr(state, "partial_tp_1_done"))
        self.assertFalse(hasattr(state, "partial_tp_2_done"))
        self.assertFalse(hasattr(martin.MartinBot, "_partial_close"))
        self.assertFalse(hasattr(martin.MartinBot, "_execute_partial_take_profit"))

    def test_full_layer_uses_lower_dynamic_threshold(self):
        bot = self._configured_bot(layer=9)
        activate_pct, trail_ratio = bot._dynamic_tp_values(
            adx=31,
            volatility_pct=0.035,
            layer=9,
        )
        bot._refresh_realtime_risk_context = lambda *args, **kwargs: {
            "layer": 9,
            "adx": 31.0,
            "volatility_pct": 0.035,
            "activate_pct": activate_pct,
            "trail_ratio": trail_ratio,
            "current_price": 100.0,
            "trail_price": 90.0,
            "trail_desc": "test trail",
        }
        bot.state.best_profit_pct = 0.0065
        bot._position_profit_pct = lambda *args, **kwargs: 0.0065
        position = {"side": "long", "contracts": 1.0, "markPrice": 100.0}

        self.assertFalse(bot.check_trailing_tp(position))
        self.assertTrue(bot.state.activated)
        self.assertAlmostEqual(bot.state.active_trailing_drawdown_ratio, 0.10)

    def test_active_drawdown_ratio_can_only_tighten(self):
        bot = self._configured_bot(layer=1)

        self.assertAlmostEqual(bot._tighten_active_trail_ratio(0.25), 0.25)
        self.assertAlmostEqual(bot._tighten_active_trail_ratio(0.35), 0.25)
        self.assertAlmostEqual(bot._tighten_active_trail_ratio(0.15), 0.15)
        self.assertAlmostEqual(bot.state.active_trailing_drawdown_ratio, 0.15)

    def test_activated_trailing_is_sticky_and_exits_below_old_half_percent_floor(self):
        bot = self._configured_bot(layer=1)
        bot.state.activated = True
        bot.state.active_trailing_drawdown_ratio = 0.25
        bot.state.best_profit_pct = 0.008  # lower than the newly refreshed 0.975% threshold
        bot._refresh_realtime_risk_context = lambda *args, **kwargs: {
            "layer": 1,
            "adx": 31.0,
            "volatility_pct": 0.035,
            "activate_pct": 0.00975,
            "trail_ratio": 0.25,
            "current_price": 100.0,
            "trail_price": 90.0,
            "trail_desc": "test trail",
        }
        bot._position_profit_pct = lambda *args, **kwargs: 0.001  # 0.1%, below the removed 0.5% veto
        position = {"side": "long", "contracts": 1.0, "markPrice": 100.0}

        self.assertTrue(bot.check_trailing_tp(position))
        self.assertTrue(bot.state.activated)

    def test_ws_path_also_exits_after_sticky_low_profit_drawdown(self):
        bot = self._configured_bot(layer=1)
        bot.state.activated = True
        bot.state.active_trailing_drawdown_ratio = 0.25
        bot.state.best_profit_pct = 0.008
        bot.ws_risk_log_step_pct = 0.0025
        bot._live_price_from_ws = lambda *args, **kwargs: 100.0
        bot._realtime_position_snapshot = lambda: {
            "side": "long",
            "contracts": 1.0,
            "entryPrice": 100.0,
            "markPrice": 100.0,
        }
        bot._position_profit_pct = lambda *args, **kwargs: 0.001
        exits = []
        bot._execute_exit_pipeline = lambda reason, position=None: exits.append(reason) or True

        bot._ws_risk_step()

        self.assertEqual(exits, ["WS 回撤保护触发，执行总平仓"])


class ProtectiveStopSafetyTests(unittest.TestCase):
    @staticmethod
    def _configured_bot():
        bot = make_bot("long")
        bot.leverage = 1.0
        bot.protective_stop_enabled = True
        bot.protective_stop_min_profit_pct = 0.01
        bot.protective_stop_profit_lock_ratio = 0.5
        bot.protective_stop_update_step_pct = 0.001
        bot.protective_stop_trigger_type = "mark_price"
        bot.protective_stop_execute_price = 0.0
        bot.state.best_profit_pct = 0.1
        position = {
            "side": "long",
            "contracts": 1.0,
            "entryPrice": 100.0,
            "leverage": 1.0,
            "info": {},
        }
        bot.get_active_position = lambda: position
        return bot, position

    def test_concurrent_arm_creates_only_one_protective_stop(self):
        bot, position = self._configured_bot()
        placements = []
        bot.exchange.place_position_stop_loss = lambda *args, **kwargs: (
            placements.append(kwargs)
            or {"id": "stop-1", "clientOrderId": kwargs.get("client_oid")}
        )
        start = threading.Barrier(3)

        def worker():
            start.wait()
            bot._arm_protective_stop(position, 0.08, "test", best_profit_pct=0.1)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        start.wait()
        for thread in threads:
            thread.join(timeout=2.0)

        self.assertEqual(len(placements), 1)
        self.assertTrue(bot.state.protective_stop_active)
        self.assertEqual(bot.state.protective_stop_order_id, "stop-1")

    def test_server_stop_locks_same_profit_fraction_as_layer_drawdown(self):
        bot, position = self._configured_bot()
        bot.protective_stop_min_profit_pct = 0.005

        target = bot._protective_stop_target(
            position,
            current_profit_pct=0.10,
            best_profit_pct=0.10,
            trail_ratio=0.25,
        )

        self.assertIsNotNone(target)
        self.assertAlmostEqual(target["lock_ratio"], 0.75)
        self.assertAlmostEqual(target["locked_profit_pct"], 0.075)

    def test_full_layer_server_stop_locks_ninety_percent_of_peak(self):
        bot, position = self._configured_bot()
        bot.state.layer = 9
        bot.protective_stop_min_profit_pct = 0.005

        target = bot._protective_stop_target(
            position,
            current_profit_pct=0.10,
            best_profit_pct=0.10,
            trail_ratio=0.10,
        )

        self.assertIsNotNone(target)
        self.assertAlmostEqual(target["lock_ratio"], 0.90)
        self.assertAlmostEqual(target["locked_profit_pct"], 0.09)

    def test_cancel_failure_preserves_protective_stop_identity(self):
        bot, _ = self._configured_bot()
        bot.state.protective_stop_active = True
        bot.state.protective_stop_order_id = "stop-1"
        bot.state.protective_stop_client_oid = "client-1"
        bot.state.protective_stop_price = 105.0

        def cancel_failure(*args, **kwargs):
            raise RuntimeError("remote cancel failed")

        bot.exchange.cancel_position_stop_loss = cancel_failure

        self.assertFalse(bot._clear_protective_stop(remote=True))
        self.assertTrue(bot.state.protective_stop_active)
        self.assertEqual(bot.state.protective_stop_order_id, "stop-1")
        self.assertEqual(bot.state.protective_stop_client_oid, "client-1")
        self.assertEqual(bot.state.protective_stop_price, 105.0)

    def test_confirmed_missing_auto_order_allows_reset_to_idle(self):
        bot, _ = self._configured_bot()
        bot.state.protective_stop_active = True
        bot.state.protective_stop_order_id = "stop-1"
        bot.state.protective_stop_client_oid = "client-1"
        bot.state.protective_stop_price = 105.0
        bot.exchange.cancel_position_stop_loss = mock.Mock(
            return_value={
                "id": "stop-1",
                "status": "canceled",
                "alreadyAbsent": True,
            }
        )

        self.assertTrue(bot._reset_state())
        self.assertEqual(bot.exchange.cancel_position_stop_loss.call_count, 1)
        self.assertEqual(bot.state.bot_state, "IDLE")
        self.assertFalse(bot.state.protective_stop_active)
        self.assertEqual(bot.state.protective_stop_order_id, "")
        self.assertEqual(bot.state.protective_stop_client_oid, "")

    def test_modify_fallback_never_replaces_stop_after_cancel_failure(self):
        bot, position = self._configured_bot()
        bot.state.protective_stop_active = True
        bot.state.protective_stop_order_id = "stop-1"
        bot.state.protective_stop_client_oid = "client-1"
        bot.state.protective_stop_price = 104.0
        placements = []
        bot.exchange.modify_tpsl_order = lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("modify unsupported")
        )
        bot.exchange.cancel_position_stop_loss = lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("cancel failed")
        )
        bot.exchange.place_position_stop_loss = lambda *args, **kwargs: placements.append(kwargs)

        self.assertFalse(
            bot._arm_protective_stop(position, 0.08, "test", best_profit_pct=0.1)
        )
        self.assertEqual(placements, [])
        self.assertTrue(bot.state.protective_stop_active)
        self.assertEqual(bot.state.protective_stop_order_id, "stop-1")

    def test_modify_fallback_replaces_stop_when_gate_confirms_old_order_missing(self):
        bot, position = self._configured_bot()
        bot.state.protective_stop_active = True
        bot.state.protective_stop_order_id = "stop-1"
        bot.state.protective_stop_client_oid = "client-1"
        bot.state.protective_stop_price = 104.0
        bot.exchange.modify_tpsl_order = lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("modify unsupported")
        )
        bot.exchange.cancel_position_stop_loss = lambda *args, **kwargs: {
            "id": "stop-1",
            "status": "canceled",
            "alreadyAbsent": True,
        }
        placements = []
        bot._new_client_order_id = lambda: "new-client"
        bot.exchange.place_position_stop_loss = lambda *args, **kwargs: (
            placements.append(kwargs)
            or {"id": "stop-2", "clientOrderId": kwargs.get("client_oid")}
        )

        self.assertTrue(
            bot._arm_protective_stop(position, 0.08, "test", best_profit_pct=0.1)
        )
        self.assertEqual(len(placements), 1)
        self.assertTrue(bot.state.protective_stop_active)
        self.assertEqual(bot.state.protective_stop_order_id, "stop-2")
        self.assertEqual(bot.state.protective_stop_client_oid, "new-client")


class RuntimePersistenceTests(unittest.TestCase):
    @staticmethod
    def _windows_replace_error(winerror):
        exc = PermissionError(13, "file is temporarily locked")
        exc.winerror = winerror
        return exc

    def test_atomic_runtime_replace_retries_transient_windows_lock(self):
        bot = make_bot("long", layer=1)
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "runtime.json"
            path.write_text('{"value": "old"}', encoding="utf-8")
            real_replace = martin.os.replace
            calls = []

            def flaky_replace(source, target):
                calls.append((source, target))
                if len(calls) <= 2:
                    raise self._windows_replace_error(5)
                return real_replace(source, target)

            with mock.patch.object(martin.os, "replace", side_effect=flaky_replace), mock.patch.object(
                martin.time,
                "sleep",
                return_value=None,
            ) as sleep_mock:
                bot._save_json(path, {"value": "new"})

            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"value": "new"})
            self.assertEqual(len(calls), 3)
            self.assertEqual([call.args[0] for call in sleep_mock.call_args_list], [0.02, 0.05])
            self.assertEqual(list(Path(temp_dir).glob("*.tmp")), [])

    def test_atomic_runtime_replace_exhaustion_keeps_old_file(self):
        bot = make_bot("long", layer=1)
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "runtime.json"
            path.write_text('{"value": "old"}', encoding="utf-8")

            with mock.patch.object(
                martin.os,
                "replace",
                side_effect=lambda *args: (_ for _ in ()).throw(
                    self._windows_replace_error(32)
                ),
            ), mock.patch.object(martin.time, "sleep", return_value=None) as sleep_mock:
                with self.assertRaises(PermissionError):
                    bot._save_json(path, {"value": "new"})

            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"value": "old"})
            self.assertEqual(sleep_mock.call_count, 5)
            self.assertEqual(list(Path(temp_dir).glob("*.tmp")), [])

    def test_atomic_runtime_replace_does_not_retry_real_permission_error(self):
        bot = make_bot("long", layer=1)
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "runtime.json"
            path.write_text('{"value": "old"}', encoding="utf-8")
            error = self._windows_replace_error(13)

            with mock.patch.object(martin.os, "replace", side_effect=error) as replace_mock, mock.patch.object(
                martin.time,
                "sleep",
                return_value=None,
            ) as sleep_mock:
                with self.assertRaises(PermissionError):
                    bot._save_json(path, {"value": "new"})

            self.assertEqual(replace_mock.call_count, 1)
            sleep_mock.assert_not_called()
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"value": "old"})
            self.assertEqual(list(Path(temp_dir).glob("*.tmp")), [])

    def test_runtime_saves_are_serialized_in_state_order(self):
        bot = make_bot("long", layer=1)
        bot._save_runtime_state = types.MethodType(
            martin.MartinBot._save_runtime_state,
            bot,
        )
        bot.runtime_file = Path("unused-runtime.json")
        first_write_started = threading.Event()
        release_first_write = threading.Event()
        writes = []

        def fake_save_json(path, payload):
            if not writes:
                first_write_started.set()
                release_first_write.wait(timeout=2.0)
            writes.append(payload["layer"])

        bot._save_json = fake_save_json
        first = threading.Thread(target=bot._save_runtime_state)
        first.start()
        self.assertTrue(first_write_started.wait(timeout=2.0))
        bot.state.layer = 2
        second = threading.Thread(target=bot._save_runtime_state)
        second.start()
        release_first_write.set()
        first.join(timeout=2.0)
        second.join(timeout=2.0)

        self.assertEqual(writes, [1, 2])


class ExitSafetyTests(unittest.TestCase):
    def test_position_close_wait_rejects_api_errors_and_small_residuals(self):
        bot = make_bot("long")
        bot.get_active_position = lambda: bot._POSITION_API_ERROR
        with mock.patch.object(martin.time, "sleep", return_value=None):
            self.assertFalse(bot._wait_for_position_close(2.0, timeout_sec=0.001))

        bot.get_active_position = lambda: {"side": "long", "contracts": 0.001}
        with mock.patch.object(martin.time, "sleep", return_value=None):
            self.assertFalse(bot._wait_for_position_close(2.0, timeout_sec=0.001))

    def test_exit_pipeline_refetches_position_inside_lock(self):
        bot = make_bot("long")
        bot._clear_protective_stop = lambda **kwargs: True
        bot.fetch_open_orders = lambda: []
        bot.get_active_position = lambda: {"side": "long", "contracts": 3.0}
        closed_contracts = []
        bot.close_position = lambda position: closed_contracts.append(position["contracts"]) or True
        bot._wait_for_position_close = lambda expected, timeout_sec=3.0: False
        bot.sync_state_with_exchange = lambda: None

        self.assertFalse(
            bot._execute_exit_pipeline(
                "test exit",
                {"side": "long", "contracts": 2.0},
            )
        )
        self.assertEqual(closed_contracts, [3.0])

    def test_exit_pipeline_cancels_owned_only_and_quarantines_foreign(self):
        bot = make_bot("long")
        owned = {
            "id": "bot-entry",
            "clientOrderId": "t-martin-entry",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "reduceOnly": False,
        }
        foreign = {
            "id": "manual-entry",
            "clientOrderId": "manual-entry",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "reduceOnly": False,
        }
        bot.exchange.open_orders = [owned, foreign]
        bot._clear_protective_stop = lambda **kwargs: True
        bot.cancel_all_orders = mock.Mock(side_effect=AssertionError("cancel-all forbidden"))
        bot.fetch_open_orders = lambda: list(bot.exchange.open_orders)
        bot.close_position = mock.Mock(side_effect=AssertionError("must quarantine"))

        self.assertFalse(
            bot._execute_exit_pipeline(
                "test exit",
                {"side": "long", "contracts": 1.0},
            )
        )
        bot.cancel_all_orders.assert_not_called()
        bot.close_position.assert_not_called()
        self.assertEqual(bot.exchange.open_orders, [foreign])
        self.assertEqual(bot.state.bot_state, "IN_STRATEGY")

    def test_finalize_cancels_owned_only_and_keeps_foreign_order(self):
        bot = make_bot("long", layer=2)
        owned = {
            "id": "bot-entry",
            "clientOrderId": "t-martin-entry",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "reduceOnly": False,
        }
        foreign = {
            "id": "manual-entry",
            "clientOrderId": "manual-entry",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "reduceOnly": False,
        }
        bot.exchange.open_orders = [owned, foreign]
        bot.cancel_all_orders = mock.Mock(side_effect=AssertionError("cancel-all forbidden"))
        bot.fetch_open_orders = lambda: list(bot.exchange.open_orders)
        bot._reset_state = mock.Mock(side_effect=AssertionError("must not reset"))

        self.assertFalse(bot._finalize_full_exit("test"))
        bot.cancel_all_orders.assert_not_called()
        bot._reset_state.assert_not_called()
        self.assertEqual(bot.exchange.open_orders, [foreign])
        self.assertEqual(bot.state.bot_state, "IN_STRATEGY")

    def test_finalize_owned_only_cleanup_still_resets(self):
        bot = make_bot("long", layer=2)
        owned = {
            "id": "bot-entry",
            "clientOrderId": "t-martin-entry",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "reduceOnly": False,
        }
        bot.exchange.open_orders = [owned]
        bot.fetch_open_orders = lambda: list(bot.exchange.open_orders)
        bot._wait_for_position_close = lambda *args, **kwargs: True
        reset_calls = []
        bot._reset_state = lambda: reset_calls.append(True) or True

        self.assertTrue(bot._finalize_full_exit("test"))
        self.assertEqual(reset_calls, [True])
        self.assertEqual(bot.exchange.open_orders, [])

    def test_finalize_does_not_reset_when_orders_remain(self):
        bot = make_bot("long")
        bot.cancel_all_orders = lambda: False
        bot.fetch_open_orders = lambda: [{"id": "still-open"}]
        reset_calls = []
        bot._reset_state = lambda: reset_calls.append(True)

        with mock.patch.object(martin.time, "sleep", return_value=None):
            self.assertFalse(bot._finalize_full_exit("test"))
        self.assertEqual(reset_calls, [])

    def test_finalize_resets_only_after_zero_orders_and_repeated_zero_position(self):
        bot = make_bot("long")
        bot.cancel_all_orders = lambda: True
        bot.fetch_open_orders = lambda: []
        bot.get_active_position = lambda: None
        reset_calls = []
        bot._reset_state = lambda: reset_calls.append(True)

        with mock.patch.object(martin.time, "sleep", return_value=None):
            self.assertTrue(bot._finalize_full_exit("test"))
        self.assertEqual(reset_calls, [True])


if __name__ == "__main__":
    unittest.main()
