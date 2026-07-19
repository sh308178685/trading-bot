import importlib.util
import json
import threading
import time
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from trading.exchanges.bitget import BitgetAPIError, BitgetExchangeAdapter


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("martin_bot_entry_safety", ROOT / "scripts" / "martin-bot.py")
MARTIN_BOT = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MARTIN_BOT)
MartinBot = MARTIN_BOT.MartinBot
RuntimeState = MARTIN_BOT.RuntimeState


def entry_bot():
    bot = MartinBot.__new__(MartinBot)
    bot.symbol = "BTC/USDT:USDT"
    bot.max_layers = 9
    bot.layer_trigger_type = "mark_price"
    bot.state_lock = threading.RLock()
    bot.action_lock = threading.RLock()
    bot._exit_in_progress = threading.Event()
    bot.state = RuntimeState(
        symbol=bot.symbol,
        layer=3,
        pending_layer=3,
        bot_state="IN_STRATEGY",
        position_side="long",
        last_known_contracts=0.0012,
    )
    bot.entry_submission_confirm_timeout = 0.5
    bot.entry_submission_reconcile_interval_sec = 0.25
    bot._entry_submission_last_reconcile_at = 0.0
    bot._entry_submission_stale_notices = set()
    bot._amount_step = lambda: 0.0001
    bot._write_live_snapshot = lambda **_kwargs: None
    bot._mark_state_sync_required = lambda *_args, **_kwargs: None
    bot._is_insufficient_balance_error = lambda _exc: False
    bot._calculate_retry_amount = lambda *_args, **_kwargs: 0.0
    bot.fetch_open_orders = lambda force_rest=False: []
    bot.get_active_position = lambda force_rest=False: {
        "contracts": 0.0012,
        "side": "long",
        "entryPrice": 63_000.0,
    }
    bot.saved_states = []
    bot._save_runtime_state = lambda: bot.saved_states.append(asdict(bot.state))
    return bot


class EntrySubmissionStateMachineTests(unittest.TestCase):
    def test_intent_is_persisted_before_post_and_transport_error_is_not_retried(self):
        bot = entry_bot()
        calls = []

        def create_order(_symbol, _order_type, _side, _amount, _price, params):
            calls.append(dict(params))
            self.assertTrue(bot.saved_states)
            self.assertEqual(bot.saved_states[-1]["pending_entry_submission_state"], "submitting")
            self.assertEqual(
                bot.saved_states[-1]["pending_entry_client_oid"],
                params["clientOid"],
            )
            raise ConnectionError("response lost after request was sent")

        bot.exchange = SimpleNamespace(create_order=create_order)

        result = bot._submit_entry_order(
            "buy",
            0.0003,
            62_000.0,
            "第4层",
            order_type="limit",
            layer_num=4,
            before_contracts=0.0012,
        )

        self.assertEqual(result, bot._ENTRY_SUBMIT_PENDING)
        self.assertEqual(len(calls), 1)
        self.assertEqual(bot.state.pending_entry_submission_state, "unknown")
        self.assertEqual(bot.state.pending_entry_client_oid, calls[0]["clientOid"])
        self.assertEqual(bot.state.pending_layer, 4)

    def test_explicit_rejection_clears_pending_intent(self):
        bot = entry_bot()

        def create_order(*_args, **_kwargs):
            raise BitgetAPIError("45110", "minimum order amount", request_rejected=True)

        bot.exchange = SimpleNamespace(create_order=create_order)

        result = bot._submit_entry_order(
            "buy",
            0.0003,
            62_000.0,
            "第4层",
            order_type="limit",
            layer_num=4,
            before_contracts=0.0012,
        )

        self.assertEqual(result, bot._ENTRY_SUBMIT_REJECTED)
        self.assertEqual(bot.state.pending_entry_client_oid, "")
        self.assertEqual(bot.state.pending_entry_submission_state, "")
        self.assertEqual(bot.state.pending_layer, 3)
        self.assertEqual(bot.state.pending_entry_amount, 0.0)

    def test_not_found_everywhere_keeps_fail_closed_pending_state(self):
        bot = entry_bot()
        bot.exchange = SimpleNamespace(
            fetch_order_detail=lambda *_args, **_kwargs: None,
            fetch_history_order=lambda *_args, **_kwargs: None,
        )
        client_oid = bot._begin_entry_submission(
            layer_num=4,
            order_side="buy",
            amount=0.0003,
            entry_price=62_000.0,
            order_type="limit",
            before_contracts=0.0012,
            is_trigger=False,
        )
        bot._update_pending_entry_submission(submission_state="unknown")
        bot.state.pending_entry_started_at = time.time() - 5.0

        result = bot._reconcile_pending_entry_submission(
            position={"contracts": 0.0012},
            open_orders=[],
            force=True,
        )

        self.assertEqual(result, bot._ENTRY_SUBMIT_PENDING)
        self.assertEqual(bot.state.pending_entry_client_oid, client_oid)
        self.assertEqual(bot.state.pending_entry_submission_state, "unknown")

    def test_matching_open_order_confirms_without_resubmission(self):
        bot = entry_bot()
        bot.exchange = SimpleNamespace()
        client_oid = bot._begin_entry_submission(
            layer_num=4,
            order_side="buy",
            amount=0.0003,
            entry_price=62_000.0,
            order_type="limit",
            before_contracts=0.0012,
            is_trigger=False,
        )
        bot._update_pending_entry_submission(submission_state="unknown")

        result = bot._reconcile_pending_entry_submission(
            position={"contracts": 0.0012},
            open_orders=[{
                "id": "order-4",
                "clientOrderId": client_oid,
                "reduceOnly": False,
            }],
            force=True,
        )

        self.assertEqual(result, bot._ENTRY_SUBMIT_PENDING)
        self.assertEqual(bot.state.pending_entry_submission_state, "confirmed")
        self.assertEqual(bot.state.pending_entry_order_id, "order-4")

    def test_visible_pending_order_matches_by_client_oid_or_order_id(self):
        bot = entry_bot()
        client_oid = bot._begin_entry_submission(
            layer_num=4,
            order_side="buy",
            amount=0.0003,
            entry_price=62_000.0,
            order_type="limit",
            before_contracts=0.0012,
            is_trigger=False,
        )
        bot._update_pending_entry_submission(
            submission_state="confirmed",
            order_id="order-4",
        )

        by_client = bot._find_visible_pending_entry_order([{
            "id": "another-id",
            "clientOrderId": client_oid,
            "reduceOnly": False,
        }])
        by_order_id = bot._find_visible_pending_entry_order([{
            "id": "order-4",
            "clientOrderId": "",
            "reduceOnly": False,
        }])
        reduce_only = bot._find_visible_pending_entry_order([{
            "id": "order-4",
            "clientOrderId": client_oid,
            "reduceOnly": True,
        }])

        self.assertIsNotNone(by_client)
        self.assertIsNotNone(by_order_id)
        self.assertIsNone(reduce_only)

    def test_visible_entry_replan_waits_for_confirmed_cancel(self):
        bot = entry_bot()
        cancel_calls = []
        bot.exchange = SimpleNamespace(
            cancel_order_by_reference=lambda *_args, **kwargs: cancel_calls.append(kwargs),
        )
        reset_calls = []
        bot._reset_state = lambda: reset_calls.append(1)
        bot._begin_entry_submission(
            layer_num=1,
            order_side="buy",
            amount=0.0073,
            entry_price=62_000.0,
            order_type="limit",
            before_contracts=0.0,
            is_trigger=False,
        )
        bot._update_pending_entry_submission(
            submission_state="confirmed",
            order_id="order-1",
        )

        ready_to_replan = bot._cancel_visible_entry_before_replan()

        self.assertFalse(ready_to_replan)
        self.assertEqual(reset_calls, [])
        self.assertTrue(bot.state.pending_entry_cancel_requested)
        self.assertEqual(cancel_calls[0]["order_id"], "order-1")

    def test_authoritative_position_growth_resolves_unknown_submission(self):
        bot = entry_bot()
        bot.exchange = SimpleNamespace()
        bot._begin_entry_submission(
            layer_num=4,
            order_side="buy",
            amount=0.0003,
            entry_price=62_000.0,
            order_type="limit",
            before_contracts=0.0012,
            is_trigger=False,
        )
        bot._update_pending_entry_submission(submission_state="unknown")

        result = bot._reconcile_pending_entry_submission(
            position={"contracts": 0.0015},
            open_orders=[],
            force=True,
        )

        self.assertEqual(result, bot._ENTRY_SUBMIT_CONFIRMED)
        self.assertEqual(bot.state.pending_entry_client_oid, "")
        self.assertEqual(bot.state.pending_layer, 4)
        self.assertAlmostEqual(bot.state.pending_entry_amount, 0.0003)

    def test_terminal_unfilled_order_allows_replan(self):
        bot = entry_bot()
        bot.exchange = SimpleNamespace(
            fetch_order_detail=lambda *_args, **_kwargs: {
                "id": "order-4",
                "status": "canceled",
                "filled": 0.0,
            },
        )
        bot._begin_entry_submission(
            layer_num=4,
            order_side="buy",
            amount=0.0003,
            entry_price=62_000.0,
            order_type="limit",
            before_contracts=0.0012,
            is_trigger=False,
        )
        bot._update_pending_entry_submission(submission_state="unknown")

        result = bot._reconcile_pending_entry_submission(
            position={"contracts": 0.0012},
            open_orders=[],
            force=True,
        )

        self.assertEqual(result, bot._ENTRY_SUBMIT_REJECTED)
        self.assertEqual(bot.state.pending_entry_client_oid, "")
        self.assertEqual(bot.state.pending_layer, 3)

    def test_pending_submission_survives_runtime_reload(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_file = Path(temp_dir) / "martin-runtime.json"
            runtime_file.write_text(
                json.dumps({
                    "symbol": "BTC/USDT:USDT",
                    "layer": 3,
                    "pending_layer": 4,
                    "bot_state": "IN_STRATEGY",
                    "position_side": "long",
                    "pending_entry_price": 62_000.0,
                    "pending_entry_amount": 0.0003,
                    "pending_entry_client_oid": "mrt-e4-persisted",
                    "pending_entry_order_id": "",
                    "pending_entry_execute_order_id": "",
                    "pending_entry_submission_state": "unknown",
                    "pending_entry_order_type": "limit",
                    "pending_entry_side": "buy",
                    "pending_entry_before_contracts": 0.0012,
                    "pending_entry_started_at": 123.0,
                }),
                encoding="utf-8",
            )
            bot = MartinBot.__new__(MartinBot)
            bot.runtime_file = runtime_file
            bot.symbol = "BTC/USDT:USDT"
            bot.state = RuntimeState(symbol=bot.symbol)
            bot._runtime_state_load_failed = False
            bot._runtime_state_load_error = ""
            bot._repair_phase2_start_layer = lambda: False

            bot._load_runtime_state()

            self.assertEqual(bot.state.pending_entry_client_oid, "mrt-e4-persisted")
            self.assertEqual(bot.state.pending_entry_submission_state, "unknown")
            self.assertEqual(bot.state.pending_layer, 4)
            self.assertAlmostEqual(bot.state.pending_entry_before_contracts, 0.0012)

    def test_full_exit_preserves_unknown_entry_until_cancellation_is_confirmed(self):
        bot = entry_bot()
        bot.exchange = SimpleNamespace(
            cancel_order_by_reference=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("order not exist")
            ),
            fetch_order_detail=lambda *_args, **_kwargs: None,
            fetch_history_order=lambda *_args, **_kwargs: None,
        )
        bot.cancel_all_orders = lambda: None
        bot.fetch_open_orders = lambda force_rest=False: []
        bot.get_active_position = lambda force_rest=False: None
        reset_calls = []
        bot._reset_state = lambda: reset_calls.append(1)
        bot._begin_entry_submission(
            layer_num=4,
            order_side="buy",
            amount=0.0003,
            entry_price=62_000.0,
            order_type="limit",
            before_contracts=0.0012,
            is_trigger=False,
        )
        bot._update_pending_entry_submission(submission_state="unknown")
        bot.state.pending_entry_started_at = time.time() - 5.0

        with patch.object(MARTIN_BOT.time, "sleep", return_value=None):
            cleared = bot._finalize_full_exit(reason="test")

        self.assertFalse(cleared)
        self.assertEqual(reset_calls, [])
        self.assertTrue(bot.state.pending_entry_cancel_requested)
        self.assertEqual(bot.state.pending_entry_submission_state, "unknown")
        self.assertEqual(bot.state.bot_state, "EXITING")

    def test_full_exit_resets_only_after_pending_order_is_confirmed_canceled(self):
        bot = entry_bot()
        bot.exchange = SimpleNamespace(
            cancel_order_by_reference=lambda *_args, **_kwargs: {},
            fetch_order_detail=lambda *_args, **_kwargs: {
                "id": "order-4",
                "status": "canceled",
                "filled": 0.0,
            },
        )
        bot.cancel_all_orders = lambda: None
        bot.fetch_open_orders = lambda force_rest=False: []
        bot.get_active_position = lambda force_rest=False: None
        reset_calls = []
        bot._reset_state = lambda: reset_calls.append(1)
        bot._begin_entry_submission(
            layer_num=4,
            order_side="buy",
            amount=0.0003,
            entry_price=62_000.0,
            order_type="limit",
            before_contracts=0.0012,
            is_trigger=False,
        )
        bot._update_pending_entry_submission(submission_state="unknown")

        with patch.object(MARTIN_BOT.time, "sleep", return_value=None):
            cleared = bot._finalize_full_exit(reason="test")

        self.assertTrue(cleared)
        self.assertEqual(reset_calls, [1])
        self.assertEqual(bot.state.pending_entry_client_oid, "")

    def test_late_fill_after_exit_request_remains_marked_for_immediate_exit(self):
        bot = entry_bot()
        bot.exchange = SimpleNamespace(cancel_order_by_reference=lambda *_args, **_kwargs: {})
        bot._begin_entry_submission(
            layer_num=4,
            order_side="buy",
            amount=0.0003,
            entry_price=62_000.0,
            order_type="limit",
            before_contracts=0.0012,
            is_trigger=False,
        )
        bot._update_pending_entry_submission(submission_state="unknown")
        bot._request_pending_entry_cancel()

        result = bot._reconcile_pending_entry_submission(
            position={"contracts": 0.0015},
            open_orders=[],
            force=True,
        )

        self.assertEqual(result, bot._ENTRY_SUBMIT_PENDING)
        self.assertTrue(bot.state.pending_entry_cancel_requested)
        self.assertEqual(bot.state.pending_entry_submission_state, "filled_exit_required")
        self.assertNotEqual(bot.state.pending_entry_client_oid, "")


class BitgetEntryReconciliationAdapterTests(unittest.TestCase):
    def test_plan_order_timeout_code_is_ambiguous(self):
        class FakeResponse:
            status_code = 200

            @staticmethod
            def json():
                return {"code": "40010", "msg": "request timed out", "data": None}

            @staticmethod
            def raise_for_status():
                return None

        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.config = {"apiKey": "", "secretKey": "", "passphrase": ""}
        adapter._base_url = "https://api.bitget.test"
        adapter._rest_timeout = 1
        adapter._session = SimpleNamespace(request=lambda *_args, **_kwargs: FakeResponse())

        with self.assertRaises(BitgetAPIError) as raised:
            adapter._request(
                "POST",
                "/api/v2/mix/order/place-plan-order",
                payload={"symbol": "BTCUSDT"},
                private=True,
            )

        self.assertFalse(raised.exception.request_rejected)

    def test_trigger_history_can_be_queried_by_client_oid(self):
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
                    "orderId": "plan-4",
                    "executeOrderId": "order-4",
                    "clientOid": "mrt-e4-123",
                    "planStatus": "executed",
                    "size": "0.0003",
                    "baseVolume": "0.0003",
                    "executePrice": "62000",
                }]
            }

        adapter._retry = retry
        detail = adapter.fetch_history_trigger_order(
            adapter.symbol,
            client_oid="mrt-e4-123",
        )

        self.assertEqual(captured["path"], "/api/v2/mix/order/orders-plan-history")
        self.assertEqual(captured["query"]["clientOid"], "mrt-e4-123")
        self.assertEqual(detail["status"], "executed")
        self.assertEqual(detail["executeOrderId"], "order-4")

    def test_force_rest_open_orders_returns_client_oid(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.symbol = "BTC/USDT:USDT"
        adapter.inst_type = "USDT-FUTURES"
        adapter.inst_id = "BTCUSDT"

        class FreshWs:
            @staticmethod
            def is_fresh(_channel):
                return True

            @staticmethod
            def get_orders(_inst_id):
                return [{"orderId": "ws-order", "clientOid": "ws-client", "status": "live"}]

        adapter.ws = FreshWs()
        rest_calls = []

        def retry(_label, _fn, path, query):
            rest_calls.append((path, query))
            return {
                "entrustedList": [{
                    "orderId": "rest-order",
                    "clientOid": "mrt-e4-123",
                    "status": "live",
                    "size": "0.0003",
                    "baseVolume": "0",
                    "price": "62000",
                    "side": "buy",
                    "orderType": "limit",
                }]
            }

        adapter._private_get = lambda *_args, **_kwargs: None
        adapter._retry = retry
        orders = adapter._fetch_standard_open_orders(
            adapter.symbol,
            params={"_force_rest": True},
        )

        self.assertEqual(len(rest_calls), 1)
        self.assertEqual(orders[0]["id"], "rest-order")
        self.assertEqual(orders[0]["clientOrderId"], "mrt-e4-123")

    def test_cancel_normal_order_by_client_oid_uses_single_cancel_endpoint(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.symbol = "BTC/USDT:USDT"
        adapter.inst_type = "USDT-FUTURES"
        adapter.market = lambda _symbol: {"id": "BTCUSDT", "settleId": "USDT"}
        adapter._private_post = lambda *_args, **_kwargs: None
        captured = {}

        def retry(_label, _fn, path, request):
            captured.update({"path": path, "request": request})
            return {"clientOid": "mrt-e4-123"}

        adapter._retry = retry
        adapter.cancel_order_by_reference(
            adapter.symbol,
            client_oid="mrt-e4-123",
        )

        self.assertEqual(captured["path"], "/api/v2/mix/order/cancel-order")
        self.assertEqual(captured["request"]["clientOid"], "mrt-e4-123")

    def test_cancel_trigger_order_by_client_oid_uses_plan_cancel_endpoint(self):
        adapter = BitgetExchangeAdapter.__new__(BitgetExchangeAdapter)
        adapter.symbol = "BTC/USDT:USDT"
        adapter.inst_type = "USDT-FUTURES"
        adapter.market = lambda _symbol: {"id": "BTCUSDT", "settleId": "USDT"}
        adapter._private_post = lambda *_args, **_kwargs: None
        captured = {}

        def retry(_label, _fn, path, request):
            captured.update({"path": path, "request": request})
            return {"successList": [{"clientOid": "mrt-e4-123"}]}

        adapter._retry = retry
        adapter.cancel_trigger_order_by_reference(
            adapter.symbol,
            client_oid="mrt-e4-123",
        )

        self.assertEqual(captured["path"], "/api/v2/mix/order/cancel-plan-order")
        self.assertEqual(
            captured["request"]["orderIdList"][0]["clientOid"],
            "mrt-e4-123",
        )


if __name__ == "__main__":
    unittest.main()
