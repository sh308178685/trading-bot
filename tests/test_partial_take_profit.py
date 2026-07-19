import importlib.util
import threading
import time
import tempfile
import unittest
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from trading.exchanges.bitget import BitgetAPIError, BitgetExchangeAdapter


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("martin_bot", ROOT / "scripts" / "martin-bot.py")
MARTIN_BOT = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MARTIN_BOT)
MartinBot = MARTIN_BOT.MartinBot


def floor_to_step(value: float, step: float) -> float:
    value_decimal = Decimal(str(value))
    step_decimal = Decimal(str(step))
    units = (value_decimal / step_decimal).to_integral_value(rounding=ROUND_DOWN)
    return float(units * step_decimal)


def planner_bot(*, step=0.0001, min_amount=0.0001, min_cost=5.0):
    bot = MartinBot.__new__(MartinBot)
    market = {
        "precision": {"amount": step},
        "limits": {
            "amount": {"min": min_amount},
            "cost": {"min": min_cost},
        },
    }
    bot.partial_tp_max_ratio_overshoot = 0.10
    bot.partial_tp_min_cost_buffer_ratio = 0.01
    bot._market = lambda: market
    bot._amount_to_precision = lambda amount: floor_to_step(amount, step)
    return bot


def empty_runtime_state():
    return SimpleNamespace(
        partial_tp_1_done=False,
        partial_tp_2_done=False,
        partial_tp_pending_flag="",
        partial_tp_pending_order_id="",
        partial_tp_pending_client_oid="",
        partial_tp_pending_before_contracts=0.0,
        partial_tp_pending_amount=0.0,
        partial_tp_pending_started_at=0.0,
    )


def execution_bot():
    bot = planner_bot()
    bot.symbol = "BTC/USDT:USDT"
    bot.action_lock = threading.RLock()
    bot.state_lock = threading.RLock()
    bot._exit_in_progress = threading.Event()
    bot.state = empty_runtime_state()
    bot._partial_tp_skip_keys = {}
    bot._partial_tp_last_authoritative_check = {}
    bot.partial_tp_authoritative_recheck_sec = 5.0
    bot.partial_tp_confirm_timeout = 3.0
    bot.partial_tp_reconcile_interval_sec = 1.0
    bot.partial_tp_rejection_recheck_sec = 30.0
    bot._partial_tp_last_reconcile_at = 0.0
    bot._partial_tp_rejection_cache = {}
    bot._partial_tp_stale_pending_notices = set()
    bot._live_price_from_ws = lambda *_args, **_kwargs: 63_000.0
    bot._save_runtime_state = lambda: None
    bot._write_live_snapshot = lambda **_kwargs: None
    bot._mark_state_sync_required = lambda *_args, **_kwargs: None
    return bot


class PartialClosePlanTests(unittest.TestCase):
    def test_tp1_uses_one_min_lot_for_point_zero_zero_zero_three_btc(self):
        bot = planner_bot()

        plan = bot._build_partial_close_plan(0.0003, 0.30, 63_000.0)

        self.assertTrue(plan["executable"])
        self.assertEqual(plan["status"], "min_lot_adjusted")
        self.assertAlmostEqual(plan["close_amount"], 0.0001)
        self.assertAlmostEqual(plan["remaining_amount"], 0.0002)
        self.assertAlmostEqual(plan["actual_ratio"], 1 / 3)

    def test_tp2_is_skipped_when_min_lot_would_overshoot_too_far(self):
        bot = planner_bot()

        plan = bot._build_partial_close_plan(0.0003, 0.20, 63_000.0)

        self.assertFalse(plan["executable"])
        self.assertEqual(plan["status"], "ratio_overshoot_too_large")

    def test_small_position_is_left_for_whole_position_trailing_exit(self):
        bot = planner_bot()

        for contracts in (0.0001, 0.0002):
            with self.subTest(contracts=contracts):
                plan = bot._build_partial_close_plan(contracts, 0.30, 63_000.0)
                self.assertFalse(plan["executable"])

    def test_tp2_becomes_executable_after_position_grows(self):
        bot = planner_bot()

        plan = bot._build_partial_close_plan(0.0005, 0.20, 63_000.0)

        self.assertTrue(plan["executable"])
        self.assertEqual(plan["status"], "normal")
        self.assertAlmostEqual(plan["close_amount"], 0.0001)
        self.assertAlmostEqual(plan["remaining_amount"], 0.0004)

    def test_min_notional_is_rounded_up_to_amount_step(self):
        bot = planner_bot(step=0.00001, min_amount=0.00001, min_cost=5.0)

        plan = bot._build_partial_close_plan(0.001, 0.20, 20_000.0)

        self.assertTrue(plan["executable"])
        self.assertEqual(plan["status"], "min_lot_adjusted")
        self.assertAlmostEqual(plan["effective_min_amount"], 0.00026)
        self.assertAlmostEqual(plan["close_amount"], 0.00026)

    def test_close_amount_is_reduced_to_avoid_dust_remainder(self):
        bot = planner_bot(step=0.0001, min_amount=0.0002, min_cost=0.0)

        plan = bot._build_partial_close_plan(0.0005, 0.80, 63_000.0)

        self.assertTrue(plan["executable"])
        self.assertEqual(plan["status"], "dust_adjusted")
        self.assertAlmostEqual(plan["close_amount"], 0.0003)
        self.assertAlmostEqual(plan["remaining_amount"], 0.0002)

    def test_min_notional_requires_a_reference_price(self):
        bot = planner_bot()

        plan = bot._build_partial_close_plan(0.001, 0.30, 0.0)

        self.assertFalse(plan["executable"])
        self.assertEqual(plan["status"], "price_required_for_min_cost")

    def test_same_unavailable_position_is_logged_only_once(self):
        bot = planner_bot()
        bot.symbol = "BTC/USDT:USDT"
        bot._partial_tp_skip_keys = {}
        plan = bot._build_partial_close_plan(0.0003, 0.20, 63_000.0)

        with patch("builtins.print") as print_mock:
            bot._log_partial_tp_skip_once("partial_tp_2_done", "分批止盈2", plan)
            bot._log_partial_tp_skip_once("partial_tp_2_done", "分批止盈2", plan)

        print_mock.assert_called_once()


class PartialCloseConcurrencyTests(unittest.TestCase):
    def test_pending_partial_still_maintains_exchange_protective_stop(self):
        bot = execution_bot()
        bot.state.bot_state = "IN_STRATEGY"
        bot.state.position_side = "long"
        bot.state.best_profit_pct = 0.10
        bot.state.activated = True
        bot.state.layer = 3
        bot.state.partial_tp_pending_flag = "partial_tp_1_done"
        bot.max_loss_pct = 0.50
        bot.ws_risk_log_step_pct = 0.0025
        position = {
            "contracts": 0.0003,
            "side": "long",
            "markPrice": 63_000.0,
            "entryPrice": 61_000.0,
            "info": {"posMode": "one_way_mode"},
        }
        bot._realtime_position_snapshot = lambda: position
        bot._position_profit_pct = lambda *_args, **_kwargs: 0.10
        bot._update_best_profit = lambda *_args, **_kwargs: None
        bot._execute_partial_take_profit = lambda *_args, **_kwargs: bot._PARTIAL_PENDING
        bot._refresh_realtime_risk_context = lambda *_args, **_kwargs: {
            "activate_pct": 0.05,
            "trail_ratio": 0.50,
            "current_price": 63_000.0,
        }
        bot._dynamic_partial_tp_targets = lambda **_kwargs: {
            "tp1_threshold": 0.05,
            "tp1_ratio": 0.30,
            "tp2_threshold": 0.08,
            "tp2_ratio": 0.20,
        }
        armed = []
        bot._arm_protective_stop = lambda *_args, **_kwargs: armed.append(1)

        bot._ws_risk_step()

        self.assertEqual(armed, [1])

    def test_state_flag_is_rechecked_inside_action_lock(self):
        bot = execution_bot()
        bot.get_active_position = lambda **_kwargs: {
            "contracts": 0.0003,
            "side": "long",
            "markPrice": 63_000.0,
        }
        calls = []

        def partial_close(*_args, **_kwargs):
            calls.append(1)
            time.sleep(0.05)
            return bot._PARTIAL_COMPLETED

        bot._partial_close = partial_close
        bot._set_runtime_flag = lambda name, value: setattr(bot.state, name, value) or True

        results = []
        errors = []

        def worker():
            try:
                results.append(
                    bot._execute_partial_take_profit(
                        {"contracts": 0.0003, "side": "long", "markPrice": 63_000.0},
                        0.30,
                        "分批止盈1",
                        "partial_tp_1_done",
                    )
                )
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        threads = [
            threading.Thread(target=worker)
            for _ in range(2)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(calls), 1)
        self.assertCountEqual(results, [bot._PARTIAL_COMPLETED, bot._PARTIAL_SKIPPED])
        self.assertTrue(bot.state.partial_tp_1_done)

    def test_pending_order_blocks_a_second_partial_close(self):
        bot = execution_bot()
        bot.state.partial_tp_pending_flag = "partial_tp_2_done"
        bot.state.partial_tp_pending_client_oid = "martin-ptp2-existing"
        bot._reconcile_pending_partial_tp = lambda _reason: bot._PARTIAL_PENDING
        bot.get_active_position = lambda **_kwargs: self.fail("pending path must not submit another order")

        first = bot._execute_partial_take_profit(
            {"contracts": 0.0005, "side": "long", "markPrice": 63_000.0},
            0.20,
            "分批止盈2",
            "partial_tp_2_done",
        )
        second = bot._execute_partial_take_profit(
            {"contracts": 0.0005, "side": "long", "markPrice": 63_000.0},
            0.30,
            "分批止盈1",
            "partial_tp_1_done",
        )

        self.assertEqual(first, bot._PARTIAL_PENDING)
        self.assertEqual(second, bot._PARTIAL_PENDING)


class PartialCloseOrderTests(unittest.TestCase):
    def test_hedge_mode_position_never_submits_reduce_only_partial_close(self):
        bot = execution_bot()

        class FakeExchange:
            def create_order(self, *_args, **_kwargs):
                self.fail("hedge mode must not submit a one-way reduce-only order")

        bot.exchange = FakeExchange()
        bot._mark_state_sync_required = lambda *_args, **_kwargs: None
        position = {
            "contracts": 0.0003,
            "side": "long",
            "markPrice": 63_000.0,
            "info": {"posMode": "hedge_mode"},
        }

        with patch("builtins.print"):
            result = bot._partial_close(
                position,
                0.30,
                "分批止盈1",
                "partial_tp_1_done",
            )

        self.assertEqual(result, bot._PARTIAL_PENDING)
        self.assertEqual(bot.state.partial_tp_pending_flag, "")

    def test_definite_exchange_rejection_clears_pending_and_is_cooled_down(self):
        bot = execution_bot()
        create_calls = []

        class FakeExchange:
            def create_order(self, *args, **kwargs):
                create_calls.append((args, kwargs))
                raise BitgetAPIError("45110", "less than the minimum amount")

        bot.exchange = FakeExchange()
        position = {
            "contracts": 0.0003,
            "side": "long",
            "markPrice": 63_000.0,
        }
        bot.get_active_position = lambda **_kwargs: position

        first = bot._execute_partial_take_profit(
            position,
            0.30,
            "分批止盈1",
            "partial_tp_1_done",
        )
        second = bot._execute_partial_take_profit(
            position,
            0.30,
            "分批止盈1",
            "partial_tp_1_done",
        )

        self.assertEqual(first, bot._PARTIAL_SKIPPED)
        self.assertEqual(second, bot._PARTIAL_SKIPPED)
        self.assertEqual(len(create_calls), 1)
        self.assertEqual(bot.state.partial_tp_pending_flag, "")
        self.assertFalse(bot.state.partial_tp_1_done)

    def test_unknown_post_is_reconciled_by_persisted_client_oid_without_resubmit(self):
        bot = execution_bot()
        create_calls = []
        detail = {"value": None}

        class FakeExchange:
            def create_order(self, *args, **kwargs):
                create_calls.append((args, kwargs))
                raise RuntimeError("connection reset after send")

            def fetch_order_detail(self, _symbol, order_id=None, client_oid=None):
                if detail["value"] is None:
                    raise RuntimeError("temporary network failure")
                return detail["value"]

        bot.exchange = FakeExchange()
        bot.get_active_position = lambda **_kwargs: {
            "contracts": 0.0003,
            "side": "long",
            "markPrice": 63_000.0,
        }

        first = bot._partial_close(
            {"contracts": 0.0003, "side": "long", "markPrice": 63_000.0},
            0.30,
            "分批止盈1",
            "partial_tp_1_done",
        )
        client_oid = bot.state.partial_tp_pending_client_oid
        self.assertEqual(first, bot._PARTIAL_PENDING)
        self.assertTrue(client_oid.startswith("martin-ptp1-"))
        self.assertEqual(len(create_calls), 1)

        detail["value"] = {
            "id": "order-1",
            "clientOrderId": client_oid,
            "status": "filled",
            "filled": 0.0001,
        }
        second = bot._execute_partial_take_profit(
            {"contracts": 0.0003, "side": "long", "markPrice": 63_000.0},
            0.30,
            "分批止盈1",
            "partial_tp_1_done",
        )

        self.assertEqual(second, bot._PARTIAL_COMPLETED)
        self.assertEqual(len(create_calls), 1)
        self.assertTrue(bot.state.partial_tp_1_done)
        self.assertEqual(bot.state.partial_tp_pending_flag, "")

    def test_not_found_after_timeout_keeps_same_pending_client_oid(self):
        bot = execution_bot()
        create_calls = []

        class FakeExchange:
            def create_order(self, *args, **kwargs):
                create_calls.append((args, kwargs))
                raise RuntimeError("connection reset after send")

            def fetch_order_detail(self, _symbol, order_id=None, client_oid=None):
                raise RuntimeError("order not exist")

            def fetch_history_order(self, _symbol, order_id=None, client_oid=None):
                return None

        bot.exchange = FakeExchange()
        unchanged_position = {
            "contracts": 0.0003,
            "side": "long",
            "markPrice": 63_000.0,
        }
        bot.get_active_position = lambda **_kwargs: unchanged_position

        first = bot._partial_close(
            unchanged_position,
            0.30,
            "分批止盈1",
            "partial_tp_1_done",
        )
        client_oid = bot.state.partial_tp_pending_client_oid
        self.assertEqual(first, bot._PARTIAL_PENDING)
        self.assertEqual(len(create_calls), 1)

        bot.state.partial_tp_pending_started_at = time.time() - 30.0
        for _ in range(2):
            bot._partial_tp_last_reconcile_at = 0.0
            result = bot._execute_partial_take_profit(
                unchanged_position,
                0.30,
                "分批止盈1",
                "partial_tp_1_done",
            )
            self.assertEqual(result, bot._PARTIAL_PENDING)
            self.assertEqual(bot.state.partial_tp_pending_client_oid, client_oid)
            self.assertEqual(bot.state.partial_tp_pending_flag, "partial_tp_1_done")

        self.assertEqual(len(create_calls), 1)


class FullCloseConfirmationTests(unittest.TestCase):
    def test_unchanged_small_position_is_not_mistaken_for_closed(self):
        bot = planner_bot()
        force_rest_values = []

        def active_position(force_rest=False):
            force_rest_values.append(force_rest)
            return {"contracts": 0.0012}

        bot.get_active_position = active_position
        self.assertFalse(bot._wait_for_position_close(0.0012, timeout_sec=0.5))
        self.assertTrue(force_rest_values)
        self.assertTrue(all(force_rest_values))

    def test_api_error_is_not_mistaken_for_closed(self):
        bot = planner_bot()
        bot.get_active_position = lambda **_kwargs: bot._POSITION_API_ERROR

        self.assertFalse(bot._wait_for_position_close(0.0012, timeout_sec=0.5))

    def test_two_authoritative_empty_reads_confirm_close(self):
        bot = planner_bot()
        reads = iter([None, None])
        bot.get_active_position = lambda **_kwargs: next(reads, None)

        self.assertTrue(bot._wait_for_position_close(0.0012, timeout_sec=0.5))


class SingleInstanceLockTests(unittest.TestCase):
    def test_second_trading_process_lock_is_rejected_until_release(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / "martin-bot.lock"
            first = MartinBot.__new__(MartinBot)
            first.instance_lock_file = lock_path
            first._instance_lock_handle = None
            first.symbol = "BTC/USDT:USDT"
            first.config = {"sandbox": True}

            second = MartinBot.__new__(MartinBot)
            second.instance_lock_file = lock_path
            second._instance_lock_handle = None
            second.symbol = "BTC/USDT:USDT"
            second.config = {"sandbox": True}

            try:
                self.assertTrue(first._acquire_instance_lock())
                self.assertFalse(second._acquire_instance_lock())
                first._release_instance_lock()
                self.assertTrue(second._acquire_instance_lock())
            finally:
                first._release_instance_lock()
                second._release_instance_lock()


class RuntimeStateRecoveryTests(unittest.TestCase):
    def test_corrupt_existing_runtime_enters_fail_closed_mode(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_file = Path(temp_dir) / "martin-runtime.json"
            runtime_file.write_text("{broken-json", encoding="utf-8")
            bot = MartinBot.__new__(MartinBot)
            bot.runtime_file = runtime_file
            bot.symbol = "BTC/USDT:USDT"
            bot.state = empty_runtime_state()
            bot._runtime_state_load_failed = False
            bot._runtime_state_load_error = ""

            with patch("builtins.print"):
                bot._load_runtime_state()

            self.assertTrue(bot._runtime_state_load_failed)
            self.assertTrue(bot._runtime_state_load_error)


class BitgetAuthoritativeQueryTests(unittest.TestCase):
    def test_position_mode_is_queried_from_single_account_endpoint(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.symbol = "BTC/USDT:USDT"
        adapter.inst_type = "USDT-FUTURES"
        adapter.market = lambda _symbol: {"id": "BTCUSDT", "settleId": "USDT"}
        adapter._private_get = lambda *_args, **_kwargs: None
        captured = {}

        def retry(_label, _fn, path, query):
            captured.update({"path": path, "query": query})
            return {"posMode": "one_way_mode"}

        adapter._retry = retry
        mode = adapter.fetch_position_mode(adapter.symbol)

        self.assertEqual(mode, "one_way_mode")
        self.assertEqual(captured["path"], "/api/v2/mix/account/account")

    def test_place_order_timeout_code_is_ambiguous_but_minimum_code_is_rejected(self):
        class FakeResponse:
            status_code = 200

            def __init__(self, code, message):
                self.code = code
                self.message = message

            def json(self):
                return {"code": self.code, "msg": self.message, "data": None}

            @staticmethod
            def raise_for_status():
                return None

        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.config = {"apiKey": "", "secretKey": "", "passphrase": ""}
        adapter._base_url = "https://api.bitget.test"
        adapter._rest_timeout = 1

        for code, expected_rejected in (("40010", False), ("45110", True)):
            with self.subTest(code=code):
                adapter._session = SimpleNamespace(
                    request=lambda *_args, **_kwargs: FakeResponse(code, "test error")
                )
                with self.assertRaises(BitgetAPIError) as raised:
                    adapter._request(
                        "POST",
                        "/api/v2/mix/order/place-order",
                        payload={"symbol": "BTCUSDT"},
                        private=True,
                    )
                self.assertEqual(raised.exception.request_rejected, expected_rejected)

    def test_order_post_is_not_automatically_retried_after_connection_error(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.symbol = "BTC/USDT:USDT"
        adapter.inst_type = "USDT-FUTURES"
        adapter.margin_mode = "crossed"
        adapter.market = lambda _symbol: {
            "id": "BTCUSDT",
            "settleId": "USDT",
        }
        adapter.amount_to_precision = lambda _symbol, amount: str(amount)
        post_calls = []

        def private_post(path, request):
            post_calls.append((path, request))
            raise ConnectionError("connection reset after send")

        adapter._private_post = private_post
        adapter._retry = lambda *_args, **_kwargs: self.fail("order POST must not use automatic retry")

        with self.assertRaises(ConnectionError):
            adapter.create_order(
                adapter.symbol,
                "market",
                "sell",
                0.0001,
                None,
                {"reduceOnly": True, "clientOid": "martin-ptp1-123"},
            )

        self.assertEqual(len(post_calls), 1)

    def test_trigger_order_post_is_not_automatically_retried_after_connection_error(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.symbol = "BTC/USDT:USDT"
        adapter.inst_type = "USDT-FUTURES"
        adapter.margin_mode = "crossed"
        adapter.market = lambda _symbol: {
            "id": "BTCUSDT",
            "settleId": "USDT",
        }
        adapter.amount_to_precision = lambda _symbol, amount: str(amount)
        adapter.price_to_precision = lambda _symbol, price: str(price)
        post_calls = []

        def private_post(path, request):
            post_calls.append((path, request))
            raise ConnectionError("connection reset after send")

        adapter._private_post = private_post
        adapter._retry = lambda *_args, **_kwargs: self.fail("trigger POST must not use automatic retry")

        with self.assertRaises(ConnectionError):
            adapter.create_trigger_order(
                adapter.symbol,
                "buy",
                0.0001,
                62_000.0,
                price=62_000.0,
                order_type="limit",
            )

        self.assertEqual(len(post_calls), 1)

    def test_force_rest_bypasses_fresh_private_ws_position(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.symbol = "BTC/USDT:USDT"
        adapter.inst_id = "BTCUSDT"
        adapter.inst_type = "USDT-FUTURES"
        adapter.margin_coin = "USDT"

        class FakeWs:
            @staticmethod
            def is_fresh(_channel):
                return True

            @staticmethod
            def get_position(_inst_id):
                return {
                    "total": "0.0003",
                    "holdSide": "long",
                    "openPriceAvg": "63000",
                    "markPrice": "63000",
                    "unrealizedPL": "0",
                    "leverage": "3",
                }

        adapter.ws = FakeWs()
        adapter._private_get = lambda *_args, **_kwargs: None
        adapter._retry = lambda *_args, **_kwargs: [
            {
                "total": "0.0005",
                "holdSide": "long",
                "openPriceAvg": "63000",
                "markPrice": "63000",
                "unrealizedPL": "0",
                "leverage": "3",
            }
        ]

        cached = adapter.fetch_positions([adapter.symbol])
        authoritative = adapter.fetch_positions([adapter.symbol], {"_force_rest": True})

        self.assertAlmostEqual(cached[0]["contracts"], 0.0003)
        self.assertAlmostEqual(authoritative[0]["contracts"], 0.0005)

    def test_order_detail_can_be_queried_by_client_oid(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.symbol = "BTC/USDT:USDT"
        adapter.inst_type = "USDT-FUTURES"
        adapter.market = lambda _symbol: {"id": "BTCUSDT"}
        adapter._private_get = lambda *_args, **_kwargs: None
        captured = {}

        def retry(_label, _fn, path, query):
            captured.update({"path": path, "query": query})
            return {
                "orderId": "order-1",
                "clientOid": "martin-ptp1-123",
                "state": "filled",
                "size": "0.0001",
                "baseVolume": "0.0001",
                "priceAvg": "63000",
            }

        adapter._retry = retry
        detail = adapter.fetch_order_detail(
            adapter.symbol,
            client_oid="martin-ptp1-123",
        )

        self.assertEqual(captured["path"], "/api/v2/mix/order/detail")
        self.assertEqual(captured["query"]["clientOid"], "martin-ptp1-123")
        self.assertEqual(detail["status"], "filled")
        self.assertAlmostEqual(detail["filled"], 0.0001)

    def test_history_order_can_be_queried_by_client_oid(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.symbol = "BTC/USDT:USDT"
        adapter.inst_type = "USDT-FUTURES"
        adapter.market = lambda _symbol: {"id": "BTCUSDT"}
        adapter._private_get = lambda *_args, **_kwargs: None
        captured = {}

        def retry(_label, _fn, path, query):
            captured.update({"path": path, "query": query})
            return {
                "entrustedList": [{
                    "orderId": "order-1",
                    "clientOid": "martin-ptp1-123",
                    "status": "filled",
                    "size": "0.0001",
                    "baseVolume": "0.0001",
                    "priceAvg": "63000",
                }]
            }

        adapter._retry = retry
        detail = adapter.fetch_history_order(
            adapter.symbol,
            client_oid="martin-ptp1-123",
        )

        self.assertEqual(captured["path"], "/api/v2/mix/order/orders-history")
        self.assertEqual(captured["query"]["clientOid"], "martin-ptp1-123")
        self.assertEqual(detail["status"], "filled")
        self.assertAlmostEqual(detail["filled"], 0.0001)


if __name__ == "__main__":
    unittest.main()
