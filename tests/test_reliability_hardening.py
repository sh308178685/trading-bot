import asyncio
import importlib.util
import inspect
import json
import threading
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import dashboard.data_provider as dashboard_provider
from trading.exchanges.bitget import (
    BitgetAPIError,
    BitgetExchangeAdapter,
    BitgetWebSocketClient,
)


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "martin_bot_reliability",
    ROOT / "scripts" / "martin-bot.py",
)
MARTIN_BOT = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MARTIN_BOT)
MartinBot = MARTIN_BOT.MartinBot
RuntimeState = MARTIN_BOT.RuntimeState


class _EmptyWebSocket:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def send(self, _payload):
        return None

    async def close(self):
        return None


class _ConnectionContext:
    def __init__(self, factory):
        self.factory = factory
        self.websocket = _EmptyWebSocket()

    async def __aenter__(self):
        self.factory.enters += 1
        return self.websocket

    async def __aexit__(self, _exc_type, _exc, _tb):
        self.factory.exits += 1
        if self.factory.exits >= 2:
            self.factory.client._stop_event.set()
        return False


class _ConnectionFactory:
    def __init__(self, client):
        self.client = client
        self.enters = 0
        self.exits = 0

    def __call__(self, *_args, **_kwargs):
        return _ConnectionContext(self)


class WebSocketReliabilityTests(unittest.TestCase):
    def test_normal_ping_task_cancellation_reconnects_instead_of_terminating_worker(self):
        client = BitgetWebSocketClient({
            "wsEnabled": True,
            "wsPublicEnabled": True,
            "wsPrivateEnabled": False,
            "symbol": "BTC/USDT:USDT",
            "sandbox": True,
            "wsReconnectDelay": 0,
        })
        factory = _ConnectionFactory(client)

        async def run_worker():
            with patch("trading.exchanges.bitget.websockets.connect", factory):
                await asyncio.wait_for(client._connection_worker("public"), timeout=1)

        asyncio.run(run_worker())

        self.assertEqual(factory.enters, 2)
        self.assertEqual(client.snapshot()["public"]["reconnects"], 2)

    def test_channel_activity_does_not_make_an_old_ticker_fresh(self):
        client = BitgetWebSocketClient({
            "wsEnabled": True,
            "symbol": "BTC/USDT:USDT",
            "sandbox": True,
            "wsFreshSeconds": 15,
        })
        now = time.time()
        with client._lock:
            client._ticker[client.inst_id] = {"instId": client.inst_id, "lastPr": "64987.3"}
            client._status["public"]["last_message_at"] = now
            client._status["public"]["last_ticker_at"] = now - 60

        self.assertTrue(client.is_fresh("public"))
        self.assertFalse(client.is_ticker_fresh(client.inst_id))

        client._consume_ticker([{"instId": client.inst_id, "lastPr": "64658.8"}])
        self.assertTrue(client.is_ticker_fresh(client.inst_id))

    def test_entry_health_gate_blocks_disconnected_or_stale_channels(self):
        bot = MartinBot.__new__(MartinBot)
        bot.ws_entry_health_gate_enabled = True
        bot.ws_entry_max_stale_sec = 45.0
        bot.ws_health_alert_interval_sec = 60.0
        bot._last_ws_health_alert_at = 0.0
        bot._last_ws_health_reason = ""
        ws = SimpleNamespace(enabled=True, public_enabled=True, private_enabled=True)
        status = {
            "public": {"connected": False, "last_message_at": None, "last_pong_at": None},
            "private": {"connected": True, "last_message_at": time.time(), "last_pong_at": None},
        }
        bot.exchange = SimpleNamespace(ws=ws, ws_status=lambda: status)

        healthy, reason = bot._entry_transport_health(log_warning=False)
        self.assertFalse(healthy)
        self.assertIn("public", reason)

        now = time.time()
        status["public"] = {"connected": True, "last_message_at": now, "last_pong_at": None}
        status["private"] = {"connected": True, "last_message_at": now, "last_pong_at": None}
        healthy, reason = bot._entry_transport_health(log_warning=False)
        self.assertTrue(healthy)
        self.assertEqual(reason, "")


class TrailingProfitSafetyTests(unittest.TestCase):
    def _bot(self):
        bot = MartinBot.__new__(MartinBot)
        bot.symbol = "BTC/USDT:USDT"
        bot.state_lock = threading.RLock()
        bot.runtime_persistence_lock = threading.Lock()
        bot.state = RuntimeState(
            symbol="BTC/USDT:USDT",
            bot_state="IN_STRATEGY",
            position_side="long",
            best_profit_pct=0.08,
        )
        bot._save_runtime_state = lambda: None
        return bot

    def test_historical_peak_does_not_activate_when_current_profit_is_below_threshold(self):
        bot = self._bot()
        self.assertFalse(bot._activate_trailing_on_current_cross(-0.015, 0.03, "test"))
        self.assertFalse(bot.state.activated)

        self.assertTrue(bot._activate_trailing_on_current_cross(0.031, 0.03, "test"))
        self.assertTrue(bot.state.activated)

    def test_activated_drawdown_is_not_rejected_after_profit_turns_negative(self):
        bot = self._bot()
        bot.state.activated = True
        self.assertTrue(bot._allow_trailing_close({}, -0.015, "test", 0.08))

    def test_partial_target_uses_current_profit(self):
        self.assertFalse(MartinBot._current_profit_reaches_target(0.02, 0.04))
        self.assertTrue(MartinBot._current_profit_reaches_target(0.04, 0.04))


class Phase1QuickTakeProfitTests(unittest.TestCase):
    def _bot(self):
        bot = MartinBot.__new__(MartinBot)
        bot.symbol = "BTC/USDT:USDT"
        bot.state_lock = threading.RLock()
        bot.runtime_persistence_lock = threading.Lock()
        bot._exit_in_progress = threading.Event()
        bot.state = RuntimeState(
            symbol="BTC/USDT:USDT",
            bot_state="IN_STRATEGY",
            position_side="long",
            layer=1,
            phase="PHASE1",
            last_known_contracts=0.0012,
            entry_price=62_864.0,
        )
        bot._save_runtime_state = lambda: None
        bot.phase_switch_layer = 4
        bot.phase_switch_loss_pct = 0.025
        bot.phase1_max_layers = 3
        bot.phase1_quick_take_profit_enabled = True
        bot.phase1_quick_take_profit_dynamic_enabled = True
        bot.phase1_quick_take_profit_pct = 0.006
        bot.phase1_quick_take_profit_min_net_pct = 0.002
        bot.phase1_quick_take_profit_min_pct = 0.005
        bot.phase1_quick_take_profit_max_pct = 0.0075
        bot.phase1_quick_take_profit_adx_floor = 18.0
        bot.phase1_quick_take_profit_adx_ceiling = 40.0
        bot.phase1_quick_take_profit_volatility_floor_pct = 0.001
        bot.phase1_quick_take_profit_volatility_ceiling_pct = 0.005
        bot.phase1_quick_take_profit_trend_weight = 0.60
        bot.leverage = 3
        bot.fee_rate = 0.0005
        bot.max_loss_pct = 0.5
        bot.ws_risk_log_step_pct = 0.001
        bot.profit_exit_authoritative_confirm_enabled = False
        bot.profit_exit_confirm_cooldown_sec = 2.0
        bot.profit_exit_basis_price_tolerance_pct = 0.00001
        bot.ws_risk_price_max_deviation_pct = 0.0015
        bot.profit_exit_guard_log_interval_sec = 10.0
        bot._profit_exit_confirmation_blocked_until = 0.0
        bot._last_profit_exit_guard_log_at = 0.0
        bot._last_profit_exit_guard_reason = ""
        bot._refresh_realtime_risk_context = lambda *_args, **_kwargs: {
            "adx": 30.0,
            "volatility_pct": 0.001,
        }
        return bot

    @staticmethod
    def _position():
        return {
            "contracts": 0.0012,
            "side": "long",
            "entryPrice": 62_864.0,
            "markPrice": 63_000.0,
        }

    @staticmethod
    def _freeze(bot, position, target=0.006):
        bot.state.phase1_quick_tp_target_pct = target
        bot.state.phase1_quick_tp_basis_contracts = position["contracts"]
        bot.state.phase1_quick_tp_basis_entry_price = position["entryPrice"]
        bot.state.phase1_quick_tp_basis_layer = bot.state.layer

    def test_effective_target_cannot_fall_below_round_trip_fees_plus_buffer(self):
        bot = self._bot()
        bot.phase1_quick_take_profit_pct = 0.004

        self.assertAlmostEqual(bot._phase1_quick_take_profit_target(), 0.005)

    def test_quick_take_profit_is_phase1_only_and_uses_current_profit(self):
        bot = self._bot()
        position = self._position()
        expected_target = bot._calculate_phase1_quick_take_profit_target(
            {"adx": 30.0, "volatility_pct": 0.001}
        )[0]

        self.assertFalse(
            bot._should_phase1_quick_take_profit(position, expected_target - 1e-6)
        )
        self.assertTrue(bot._should_phase1_quick_take_profit(position, expected_target))

        bot.state.phase = "PHASE2"
        self.assertFalse(bot._should_phase1_quick_take_profit(position, 0.02))

    def test_switch_to_phase2_clears_and_disables_frozen_target(self):
        bot = self._bot()
        position = self._position()
        self._freeze(bot, position)

        self.assertFalse(bot._should_phase1_quick_take_profit(position, -0.026))
        self.assertEqual(bot.state.phase, "PHASE2")
        self.assertEqual(bot.state.phase1_quick_tp_target_pct, 0.0)

    def test_dynamic_target_moves_between_cost_safe_bounds(self):
        bot = self._bot()

        calm = bot._calculate_phase1_quick_take_profit_target(
            {"adx": 18.0, "volatility_pct": 0.001}
        )
        strong = bot._calculate_phase1_quick_take_profit_target(
            {"adx": 40.0, "volatility_pct": 0.005}
        )

        self.assertAlmostEqual(calm[0], 0.005)
        self.assertAlmostEqual(strong[0], 0.0075)

    def test_dynamic_target_uses_symbol_relative_atr_percentile_when_ready(self):
        bot = self._bot()
        low_percentile = bot._calculate_phase1_quick_take_profit_target({
            "adx": 18.0,
            "volatility_pct": 0.003,
            "volatility_profile_ready": True,
            "atr_percentile": 0.0,
        })
        high_percentile = bot._calculate_phase1_quick_take_profit_target({
            "adx": 18.0,
            "volatility_pct": 0.003,
            "volatility_profile_ready": True,
            "atr_percentile": 1.0,
        })

        self.assertAlmostEqual(low_percentile[0], 0.005)
        self.assertGreater(high_percentile[0], low_percentile[0])
        self.assertEqual(high_percentile[3], "dynamic_percentile")

    def test_target_is_frozen_until_position_basis_changes(self):
        bot = self._bot()
        position = self._position()
        calm_context = {"adx": 18.0, "volatility_pct": 0.001}
        strong_context = {"adx": 40.0, "volatility_pct": 0.005}

        first = bot._ensure_phase1_quick_take_profit_target(position, context=calm_context)
        still_frozen = bot._ensure_phase1_quick_take_profit_target(position, context=strong_context)
        self.assertAlmostEqual(first, 0.005)
        self.assertAlmostEqual(still_frozen, first)

        bot.state.last_known_contracts = position["contracts"]
        bot.state.pending_layer = 2
        grown_position = dict(position, contracts=0.0016, entryPrice=62_700.0)
        recalculated = bot._ensure_phase1_quick_take_profit_target(
            grown_position,
            context=strong_context,
        )
        self.assertAlmostEqual(recalculated, 0.0075)
        self.assertEqual(bot.state.phase1_quick_tp_basis_layer, 2)

    def test_exposure_reset_clears_frozen_target(self):
        bot = self._bot()
        position = self._position()
        self._freeze(bot, position)
        bot._clear_protective_stop = lambda **_kwargs: None

        bot._reset_trailing_baseline("test")

        self.assertEqual(bot.state.phase1_quick_tp_target_pct, 0.0)
        self.assertEqual(bot.state.phase1_quick_tp_basis_contracts, 0.0)

    def test_polling_path_exits_before_waiting_for_dynamic_targets(self):
        bot = self._bot()
        bot._live_price_from_ws = lambda *_args, **_kwargs: 63_000.0
        bot._position_profit_pct = lambda *_args, **_kwargs: 0.0065
        bot._update_best_profit = lambda *_args, **_kwargs: None
        position = self._position()
        self._freeze(bot, position)
        bot._refresh_realtime_risk_context = lambda *_args, **_kwargs: self.fail(
            "Phase1 quick TP should not wait for dynamic risk context"
        )

        self.assertTrue(bot.check_trailing_tp(position))

    def test_websocket_path_uses_same_quick_whole_position_exit(self):
        bot = self._bot()
        position = self._position()
        self._freeze(bot, position)
        bot._live_price_from_ws = lambda *_args, **_kwargs: 63_000.0
        bot._realtime_position_snapshot = lambda: dict(position)
        bot._position_profit_pct = lambda *_args, **_kwargs: 0.0065
        bot._update_best_profit = lambda *_args, **_kwargs: None
        bot._write_live_snapshot = lambda **_kwargs: None
        bot._refresh_realtime_risk_context = lambda *_args, **_kwargs: self.fail(
            "Phase1 quick TP should not wait for dynamic risk context"
        )
        exits = []
        bot._execute_exit_pipeline = lambda reason, live_position: exits.append(
            (reason, live_position)
        )

        bot._ws_risk_step()

        self.assertEqual(len(exits), 1)
        self.assertIn("第一阶段快速止盈", exits[0][0])

    def test_websocket_quick_profit_is_blocked_when_rest_says_position_is_losing(self):
        bot = self._bot()
        bot.profit_exit_authoritative_confirm_enabled = True
        position = {
            "contracts": 0.0155,
            "side": "long",
            "entryPrice": 64_689.79,
            "markPrice": 64_987.3,
            "_snapshotSource": "ws_position",
        }
        bot.state.last_known_contracts = 0.0155
        bot.state.entry_price = 64_689.79
        bot.state.layer = 2
        self._freeze(bot, position, target=0.0064)
        bot._live_price_from_ws = lambda *_args, **_kwargs: 64_987.3
        bot._realtime_position_snapshot = lambda: dict(position)
        bot._write_live_snapshot = lambda **_kwargs: None
        bot._update_best_profit = lambda *_args, **_kwargs: None
        authoritative = dict(position, markPrice=64_658.8, _snapshotSource="rest_position")
        bot.get_active_position = lambda force_rest=False: dict(authoritative)
        rest_calls = []
        bot.exchange = SimpleNamespace(
            fetch_ticker=lambda _symbol, params=None: (
                rest_calls.append(dict(params or {})) or {"last": 64_658.8}
            ),
        )
        exits = []
        bot._execute_exit_pipeline = lambda reason, live_position: exits.append((reason, live_position))

        bot._ws_risk_step()

        self.assertEqual(exits, [])
        self.assertEqual(rest_calls, [{"_force_rest": True}])

    def test_profit_exit_waits_when_add_fill_has_not_synced_runtime_basis(self):
        bot = self._bot()
        position = {
            "contracts": 0.0155,
            "side": "long",
            "entryPrice": 64_689.79,
            "markPrice": 64_987.3,
            "_snapshotSource": "ws_position",
        }
        bot.state.last_known_contracts = 0.0072
        bot.state.entry_price = 64_898.2
        bot._live_price_from_ws = lambda *_args, **_kwargs: 64_987.3
        bot._realtime_position_snapshot = lambda: dict(position)
        bot._write_live_snapshot = lambda **_kwargs: None
        exits = []
        bot._execute_exit_pipeline = lambda reason, live_position: exits.append((reason, live_position))

        bot._ws_risk_step()

        self.assertEqual(exits, [])
        self.assertEqual(bot.state.best_profit_pct, 0.0)


class FullExitIdempotencyTests(unittest.TestCase):
    def test_unknown_market_close_is_not_submitted_twice(self):
        bot = MartinBot.__new__(MartinBot)
        bot.symbol = "BTC/USDT:USDT"
        bot.state_lock = threading.RLock()
        bot.action_lock = threading.RLock()
        bot.runtime_persistence_lock = threading.Lock()
        bot.state = RuntimeState(
            symbol=bot.symbol,
            bot_state="IN_STRATEGY",
            position_side="long",
            last_known_contracts=0.0012,
        )
        bot._exit_oid_sequence = 0
        bot._normalize_amount = lambda value: value
        bot._amount_step = lambda: 0.0001
        bot._save_runtime_state = lambda: None
        bot._mark_state_sync_required = lambda *_args, **_kwargs: None
        position = {"contracts": 0.0012, "side": "long", "entryPrice": 63_000.0}
        bot.get_active_position = lambda force_rest=False: dict(position)
        calls = []

        def create_order(*args, **kwargs):
            calls.append((args, kwargs))
            raise ConnectionError("response lost")

        bot.exchange = SimpleNamespace(
            create_order=create_order,
            fetch_order_detail=lambda *_args, **_kwargs: None,
            fetch_history_order=lambda *_args, **_kwargs: None,
        )

        first = bot.close_position(position, reason="test")
        second = bot.close_position(position, reason="test")

        self.assertTrue(first["pending"])
        self.assertTrue(second["pending"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(bot.state.exit_state, "ORDER_SUBMITTED")
        self.assertTrue(bot.state.exit_client_oid)


class ProtectiveStopIdempotencyTests(unittest.TestCase):
    def test_unknown_stop_submission_is_persisted_and_not_replayed(self):
        bot = MartinBot.__new__(MartinBot)
        bot.symbol = "BTC/USDT:USDT"
        bot.protective_stop_enabled = True
        bot.protective_stop_trigger_type = "mark_price"
        bot.protective_stop_execute_price = 0.0
        bot.state_lock = threading.RLock()
        bot.action_lock = threading.RLock()
        bot.runtime_persistence_lock = threading.Lock()
        bot._exit_in_progress = threading.Event()
        bot.state = RuntimeState(
            symbol=bot.symbol,
            bot_state="IN_STRATEGY",
            position_side="long",
            best_profit_pct=0.05,
        )
        bot._save_runtime_state = lambda: None
        bot._write_live_snapshot = lambda **_kwargs: None
        bot._mark_state_sync_required = lambda *_args, **_kwargs: None
        bot._should_update_protective_stop = lambda *_args, **_kwargs: True
        bot._protective_stop_target = lambda *_args, **_kwargs: {
            "side": "long",
            "hold_side": "buy",
            "trigger_price": 63_100.0,
            "locked_profit_pct": 0.01,
        }
        calls = []

        def place_stop(*_args, **_kwargs):
            calls.append(1)
            raise ConnectionError("response lost")

        bot.exchange = SimpleNamespace(
            place_position_stop_loss=place_stop,
            fetch_position_stop_loss=lambda *_args, **_kwargs: None,
        )
        position = {"contracts": 0.0012, "side": "long", "entryPrice": 63_000.0}

        self.assertFalse(bot._arm_protective_stop(position, 0.05, "test", best_profit_pct=0.05))
        self.assertFalse(bot._arm_protective_stop(position, 0.05, "test", best_profit_pct=0.05))
        self.assertEqual(len(calls), 1)
        self.assertEqual(bot.state.protective_stop_submission_state, "UNKNOWN")


class RestAndLedgerReliabilityTests(unittest.TestCase):
    def test_position_stop_query_uses_profit_loss_group(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        captured = []

        def fetch_pending(_symbol, plan_type, limit):
            captured.append((plan_type, limit))
            return [{"id": "stop-1", "clientOrderId": "client-1", "info": {}}]

        adapter.fetch_pending_trigger_orders = fetch_pending
        row = adapter.fetch_position_stop_loss(
            "BTC/USDT:USDT",
            order_id="stop-1",
        )

        self.assertEqual(row["id"], "stop-1")
        self.assertEqual(captured, [("profit_loss", 100)])

    def test_429_retry_honors_retry_after(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.retry_attempts = 2
        adapter.retry_delay = 0.1
        adapter.retry_backoff = 2.0
        adapter._rest_jitter = 0.0
        attempts = []

        def request():
            attempts.append(1)
            if len(attempts) == 1:
                raise BitgetAPIError(
                    "429",
                    "Too Many Requests",
                    status_code=429,
                    request_rejected=True,
                    retry_after=2.0,
                )
            return "ok"

        with patch("trading.exchanges.bitget.time.sleep") as sleep:
            self.assertEqual(adapter._retry("test", request), "ok")
        sleep.assert_called_once_with(2.0)

    def test_force_rest_ticker_bypasses_fresh_websocket_cache(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.symbol = "BTC/USDT:USDT"
        adapter.inst_type = "USDT-FUTURES"
        adapter.inst_id = "BTCUSDT"
        adapter.ws = SimpleNamespace(
            is_ticker_fresh=lambda _inst_id: True,
            get_ticker=lambda _inst_id: {"lastPr": "64987.3"},
        )
        adapter._public_request = lambda *_args, **_kwargs: None
        calls = []

        def retry(_label, _fn, path, query):
            calls.append((path, query))
            return [{"lastPr": "64658.8", "bidPr": "64658.7", "askPr": "64658.9"}]

        adapter._retry = retry
        ticker = adapter.fetch_ticker(adapter.symbol, {"_force_rest": True})

        self.assertEqual(ticker["last"], 64_658.8)
        self.assertEqual(calls[0][0], "/api/v2/mix/market/ticker")

    def test_account_bill_dict_payload_is_normalized(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.inst_type = "USDT-FUTURES"
        adapter.config = {}
        adapter._private_get = lambda *_args, **_kwargs: None
        adapter._retry = lambda *_args, **_kwargs: {
            "bills": [{
                "billId": "b1",
                "amount": "1.25",
                "fee": "-0.01",
                "coin": "USDT",
                "balance": "101.25",
                "businessType": "close_long",
                "cTime": "1700000000000",
            }],
            "endId": "b1",
        }

        rows = adapter.fetch_ledger(limit=1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], "b1")
        self.assertAlmostEqual(rows[0]["amount"], 1.25)
        self.assertAlmostEqual(rows[0]["fee"]["cost"], -0.01)

    def test_order_book_depth_is_normalized(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.inst_type = "USDT-FUTURES"
        adapter._retry = lambda *_args, **_kwargs: {
            "bids": [["100.0", "2.5"], ["bad", "1"]],
            "asks": [["100.1", "3.0"]],
            "ts": "1700000000000",
        }

        book = adapter.fetch_order_book("BTC/USDT:USDT", limit=50)

        self.assertEqual(book["bids"], [[100.0, 2.5]])
        self.assertEqual(book["asks"], [[100.1, 3.0]])
        self.assertEqual(book["timestamp"], 1700000000000)


class SymbolAdaptiveVolatilityTests(unittest.TestCase):
    class _Frame:
        def __init__(self, close, atr):
            self._columns = {"close": close, "atr": atr}
            self.empty = not close
            self.columns = self._columns.keys()

        def __getitem__(self, key):
            return SimpleNamespace(tolist=lambda: list(self._columns[key]))

    def _bot(self):
        bot = MartinBot.__new__(MartinBot)
        bot.volatility_percentile_enabled = True
        bot.volatility_percentile_lookback = 100
        bot.volatility_percentile_min_samples = 80
        bot.volatility_regime_low_percentile = 0.25
        bot.volatility_regime_high_percentile = 0.75
        bot.volatility_regime_extreme_percentile = 0.90
        bot.volatility_spacing_multipliers = {
            "LOW": 0.9,
            "NORMAL": 1.0,
            "HIGH": 1.2,
            "EXTREME": 1.5,
        }
        return bot

    def test_percentile_profile_uses_each_symbols_own_history(self):
        bot = self._bot()
        history = [0.10 + index * 0.001 for index in range(100)]
        frame = self._Frame([100.0] * 101, history + [0.30])

        profile = bot._volatility_profile_from_frame(frame)

        self.assertTrue(profile["ready"])
        self.assertAlmostEqual(profile["percentile"], 1.0)
        self.assertEqual(profile["regime"], "EXTREME")
        self.assertAlmostEqual(profile["spacing_multiplier"], 1.5)

    def test_low_relative_volatility_can_tighten_atr_spacing(self):
        bot = self._bot()
        history = [0.10 + index * 0.001 for index in range(100)]
        frame = self._Frame([100.0] * 101, history + [0.05])

        profile = bot._volatility_profile_from_frame(frame)
        normal_gap = bot._gap_ratio(100.0, 1.0, base_pct=0.0, atr_multiplier=1.0)
        adaptive_gap = bot._gap_ratio(
            100.0,
            1.0,
            base_pct=0.0,
            atr_multiplier=1.0,
            volatility_multiplier=profile["spacing_multiplier"],
        )

        self.assertEqual(profile["regime"], "LOW")
        self.assertAlmostEqual(adaptive_gap, normal_gap * 0.9)


class LiquidityEntryGateTests(unittest.TestCase):
    def _bot(self, ticker, book):
        bot = MartinBot.__new__(MartinBot)
        bot.symbol = "BTC/USDT:USDT"
        bot.liquidity_gate_enabled = True
        bot.liquidity_max_spread_pct = 0.002
        bot.liquidity_min_quote_volume_24h = 2_000_000.0
        bot.liquidity_depth_range_pct = 0.005
        bot.liquidity_min_depth_notional = 10_000.0
        bot.liquidity_order_book_limit = 50
        bot.liquidity_gate_cache_sec = 15.0
        bot.liquidity_gate_fail_closed = True
        bot._liquidity_snapshot_cache = {}
        bot._liquidity_snapshot_at = 0.0
        bot.exchange = SimpleNamespace(
            fetch_ticker=lambda *_args, **_kwargs: ticker,
            fetch_order_book=lambda *_args, **_kwargs: book,
        )
        return bot

    def test_liquid_symbol_is_allowed(self):
        bot = self._bot(
            {"bid": 99.95, "ask": 100.05, "quoteVolume": 5_000_000.0},
            {"bids": [[99.95, 200.0]], "asks": [[100.05, 200.0]]},
        )

        snapshot = bot._liquidity_snapshot(force=True)

        self.assertTrue(snapshot["allowed"])
        self.assertEqual(snapshot["reasons"], [])

    def test_wide_spread_blocks_only_new_first_entry_path(self):
        bot = self._bot(
            {"bid": 99.5, "ask": 100.5, "quoteVolume": 5_000_000.0},
            {"bids": [[99.5, 200.0]], "asks": [[100.5, 200.0]]},
        )

        snapshot = bot._liquidity_snapshot(force=True)

        self.assertFalse(snapshot["allowed"])
        self.assertTrue(any("点差" in reason for reason in snapshot["reasons"]))
        self.assertIn("_entry_liquidity_allowed", inspect.getsource(MartinBot.place_first_order))
        self.assertNotIn("_entry_liquidity_allowed", inspect.getsource(MartinBot.place_add_order))


class DashboardAccountingTests(unittest.TestCase):
    def test_opposite_one_way_fill_is_counted_as_exit_without_reduce_only_flag(self):
        service = dashboard_provider.DashboardService.__new__(dashboard_provider.DashboardService)
        rows = [
            {"side": "sell", "amount": 1.0, "price": 100.0, "cost": 100.0, "fee": 0.1, "reduce_only": False, "timestamp": "2026-01-01T00:00:00"},
            {"side": "buy", "amount": 1.0, "price": 90.0, "cost": 90.0, "fee": 0.1, "reduce_only": False, "timestamp": "2026-01-01T01:00:00"},
        ]

        analytics = service._build_trade_analytics(rows)
        self.assertEqual(analytics["entry_fills"], 1)
        self.assertEqual(analytics["exit_fills"], 1)
        self.assertEqual(analytics["closed_cycle_count"], 1)

    def test_roi_is_account_return_not_position_roe(self):
        service = dashboard_provider.DashboardService.__new__(dashboard_provider.DashboardService)
        with tempfile.TemporaryDirectory() as temp_dir:
            history_path = Path(temp_dir) / "history.json"
            tracker_path = Path(temp_dir) / "performance.json"
            history_path.write_text(
                json.dumps([{"timestamp": "2026-01-01T00:00:00", "equity_estimate": 80.0}]),
                encoding="utf-8",
            )
            tracker_path.write_text(
                json.dumps({
                    "baseline_equity": 80.0,
                    "peak_equity": 100.0,
                    "max_drawdown_pct": 10.0,
                    "tracking_started_at": "2026-01-01T00:00:00",
                }),
                encoding="utf-8",
            )
            with patch.object(dashboard_provider, "HISTORY_FILE", history_path), patch.object(
                dashboard_provider,
                "PERFORMANCE_FILE",
                tracker_path,
            ):
                result = service._build_performance(
                    {"total": 100.0},
                    {"unrealized_pnl": 5.0, "percentage": 7.5},
                    [],
                    [],
                    63_000.0,
                )

        self.assertAlmostEqual(result["roi_pct"], 25.0)
        self.assertAlmostEqual(result["position_roe_pct"], 7.5)
        self.assertAlmostEqual(result["equity_estimate"], 100.0)
        self.assertGreaterEqual(result["max_drawdown_pct"], 10.0)


class BacktestParityTests(unittest.TestCase):
    def test_pine_uses_nine_layer_pyramiding_and_live_multipliers(self):
        source = (ROOT / "martin-backtest.pine").read_text(encoding="utf-8")
        self.assertIn("pyramiding=9", source)
        self.assertIn('input.int(9, "最大加仓层数"', source)
        self.assertIn('input.float(9.8, "第9层倍率")', source)
        self.assertIn("if current_p >= activate_pct", source)
        self.assertIn("if current_p >= dynamic_tp1", source)
        self.assertIn("phase1_dynamic_target", source)
        self.assertIn("phase1_frozen_target", source)
        self.assertIn("ta.percentrank(vol_pct, i_vol_percentile_len)", source)
        self.assertIn("atr_mult_add * vol_spacing_mult", source)
        self.assertIn("current_p >= phase1_frozen_target", source)
        self.assertIn('if trade_signal == "LONG"\n            place_first_order("long")', source)
        self.assertIn('else if trade_signal == "SHORT"\n            place_first_order("short")', source)


if __name__ == "__main__":
    unittest.main()
