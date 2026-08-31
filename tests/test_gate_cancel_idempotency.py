import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent
PACKAGE_NAME = "gate_adapter_test_package"
package = types.ModuleType(PACKAGE_NAME)
package.__path__ = [str(ROOT / "trading" / "exchanges")]
sys.modules.setdefault(PACKAGE_NAME, package)

module_path = ROOT / "trading" / "exchanges" / "gate.py"
spec = importlib.util.spec_from_file_location(f"{PACKAGE_NAME}.gate", module_path)
gate = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = gate
spec.loader.exec_module(gate)


class GateProtectiveStopCancellationTests(unittest.TestCase):
    @staticmethod
    def _adapter(cancel_error=None):
        adapter = gate.GateExchangeAdapter.__new__(gate.GateExchangeAdapter)
        adapter.symbol = "ETH/USDT:USDT"
        adapter.settle = "usdt"
        adapter.rest = mock.Mock()
        if cancel_error is not None:
            adapter.rest.cancel_order.side_effect = cancel_error
        else:
            adapter.rest.cancel_order.return_value = {"id": "stop-1", "status": "canceled"}
        return adapter

    @staticmethod
    def _gate_error(code="1034", label="AUTO_ORDER_NOT_FOUND"):
        return gate.ccxt.ExchangeError(
            "gate "
            f'{{"code":"{code}","label":"{label}",'
            '"message":"No order found with the given ID"}'
        )

    def test_exact_1034_is_idempotent_cancel_success(self):
        adapter = self._adapter(self._gate_error())

        result = adapter.cancel_position_stop_loss(
            order_id="stop-1",
            client_oid="client-1",
        )

        self.assertEqual(result["status"], "canceled")
        self.assertTrue(result["alreadyAbsent"])
        self.assertEqual(result["id"], "stop-1")

    def test_similar_label_is_not_accepted(self):
        error = self._gate_error(label="NOT_AUTO_ORDER_NOT_FOUND_X")
        adapter = self._adapter(error)

        with self.assertRaises(gate.ccxt.ExchangeError) as raised:
            adapter.cancel_position_stop_loss(order_id="stop-1")

        self.assertIs(raised.exception, error)

    def test_code_without_exact_label_is_not_accepted(self):
        error = self._gate_error(label="PERMISSION_DENIED")
        adapter = self._adapter(error)

        with self.assertRaises(gate.ccxt.ExchangeError) as raised:
            adapter.cancel_position_stop_loss(order_id="stop-1")

        self.assertIs(raised.exception, error)

    def test_network_error_is_never_treated_as_missing_order(self):
        error = gate.ccxt.NetworkError("temporary network failure")
        adapter = self._adapter(error)

        with self.assertRaises(gate.ccxt.NetworkError) as raised:
            adapter.cancel_position_stop_loss(order_id="stop-1")

        self.assertIs(raised.exception, error)

    def test_client_oid_without_gate_order_id_still_fails(self):
        adapter = self._adapter()

        with self.assertRaisesRegex(RuntimeError, "requires order_id"):
            adapter.cancel_position_stop_loss(client_oid="client-1")

        adapter.rest.cancel_order.assert_not_called()


class GateAuthoritativeOrderLookupTests(unittest.TestCase):
    @staticmethod
    def _adapter():
        adapter = gate.GateExchangeAdapter.__new__(gate.GateExchangeAdapter)
        adapter.symbol = "ETH/USDT:USDT"
        adapter.settle = "usdt"
        adapter.rest = mock.Mock()
        adapter._map_order = lambda row: dict(row)
        return adapter

    @staticmethod
    def _mapped_adapter():
        adapter = gate.GateExchangeAdapter.__new__(gate.GateExchangeAdapter)
        adapter.symbol = "ETH/USDT:USDT"
        adapter.settle = "usdt"
        adapter.margin_mode = "cross"
        adapter.rest = mock.Mock()
        adapter._base_amount = lambda symbol, amount: abs(float(amount))
        adapter._contracts = lambda symbol, amount: float(amount)
        return adapter

    def test_completed_order_falls_back_to_exact_closed_order(self):
        adapter = self._adapter()
        adapter.fetch_order = mock.Mock(
            side_effect=gate.ccxt.OrderNotFound("direct order lookup expired")
        )
        adapter.rest.fetch_closed_orders.return_value = [
            {"id": "other", "status": "closed"},
            {
                "id": "first-order",
                "status": "closed",
                "filled": 1.0,
                "clientOrderId": "t-martin-first",
            },
        ]

        result = adapter.fetch_order_authoritative("first-order")

        self.assertEqual(result["id"], "first-order")
        adapter.rest.fetch_closed_orders.assert_called_once_with(
            "ETH/USDT:USDT",
            None,
            100,
            {"settle": "usdt"},
        )

    def test_missing_exact_closed_order_preserves_order_not_found(self):
        adapter = self._adapter()
        error = gate.ccxt.OrderNotFound("direct order lookup expired")
        adapter.fetch_order = mock.Mock(side_effect=error)
        adapter.rest.fetch_closed_orders.return_value = [{"id": "other", "status": "closed"}]

        with self.assertRaises(gate.ccxt.OrderNotFound) as raised:
            adapter.fetch_order_authoritative("missing")

        self.assertIs(raised.exception, error)

    def test_client_oid_lookup_uses_gate_text_identity(self):
        adapter = self._adapter()
        adapter.fetch_order = mock.Mock(
            return_value={
                "id": "server-order",
                "clientOrderId": "t-martin-client",
                "status": "open",
            }
        )

        result = adapter.fetch_order_by_client_id_authoritative("t-martin-client")

        self.assertEqual(result["id"], "server-order")
        adapter.fetch_order.assert_called_once_with(
            "t-martin-client",
            "ETH/USDT:USDT",
            {"clientOrderId": "t-martin-client"},
        )

    def test_finished_label_falls_back_but_unrelated_exchange_error_does_not(self):
        adapter = self._adapter()
        adapter.fetch_order = mock.Mock(
            side_effect=gate.ccxt.InvalidOrder(
                'gate {"label":"ORDER_FINISHED","message":"finished"}'
            )
        )
        adapter.rest.fetch_closed_orders.return_value = [
            {"id": "finished", "status": "closed"}
        ]

        self.assertEqual(adapter.fetch_order_authoritative("finished")["id"], "finished")

        unrelated = gate.ccxt.ExchangeError(
            'gate {"label":"PERMISSION_DENIED","message":"denied"}'
        )
        adapter.fetch_order.side_effect = unrelated
        adapter.rest.fetch_closed_orders.reset_mock()
        with self.assertRaises(gate.ccxt.ExchangeError) as raised:
            adapter.fetch_order_authoritative("finished")
        self.assertIs(raised.exception, unrelated)
        adapter.rest.fetch_closed_orders.assert_not_called()

    def test_trigger_mapping_restores_initial_identity_and_child_order(self):
        adapter = self._mapped_adapter()
        mapped = adapter._map_order(
            {
                "id": "auto-1",
                "amount": 1.0,
                "filled": 0.0,
                "remaining": 1.0,
                "clientOrderId": None,
                "reduceOnly": None,
                "info": {
                    "id": "auto-1",
                    "trigger": {"price": "90"},
                    "initial": {
                        "text": "t-martin-auto",
                        "is_reduce_only": True,
                    },
                    "trade_id": "child-1",
                    "me_order_id": "unrelated-tpsl-order",
                },
            }
        )

        self.assertEqual(mapped["type"], "trigger")
        self.assertEqual(mapped["clientOrderId"], "t-martin-auto")
        self.assertTrue(mapped["reduceOnly"])
        self.assertEqual(mapped["triggerExecutionOrderId"], "child-1")
        self.assertEqual(mapped["relatedOrderId"], "unrelated-tpsl-order")

    def test_authoritative_lookup_queries_trigger_namespace(self):
        adapter = self._mapped_adapter()

        def fetch_order(order_id, symbol=None, params=None):
            if (params or {}).get("trigger"):
                return adapter._map_order(
                    {
                        "id": order_id,
                        "status": "closed",
                        "info": {
                            "id": order_id,
                            "trigger": {"price": "90"},
                            "trade_id_string": "child-7",
                            "initial": {"text": "t-martin-trigger"},
                        },
                    }
                )
            raise gate.ccxt.OrderNotFound("not a regular order")

        adapter.fetch_order = mock.Mock(side_effect=fetch_order)
        result = adapter.fetch_order_authoritative("auto-7")

        self.assertEqual(result["id"], "auto-7")
        self.assertEqual(result["triggerExecutionOrderId"], "child-7")
        self.assertEqual(
            adapter.fetch_order.call_args_list[-1].args[2],
            {"trigger": True},
        )

    def test_trigger_client_identity_can_be_found_in_initial_text(self):
        adapter = self._mapped_adapter()
        adapter.fetch_order = mock.Mock(
            side_effect=gate.ccxt.OrderNotFound("direct lookup expired")
        )
        adapter.fetch_open_orders = mock.Mock(
            return_value=[
                adapter._map_order(
                    {
                        "id": "auto-8",
                        "info": {
                            "trigger": {"price": "90"},
                            "initial": {"text": "t-martin-auto-8"},
                        },
                    }
                )
            ]
        )

        result = adapter.fetch_order_by_client_id_authoritative(
            "t-martin-auto-8"
        )

        self.assertEqual(result["id"], "auto-8")
        adapter.rest.fetch_closed_orders.assert_not_called()

    def test_trigger_creation_nests_gate_text_under_initial_payload(self):
        adapter = self._mapped_adapter()
        adapter.rest.market.return_value = {"symbol": "ETH/USDT:USDT"}
        adapter.rest.create_order_request.return_value = {
            "settle": "usdt",
            "initial": {
                "contract": "ETH_USDT",
                "size": 1,
                "price": "89.5",
            },
            "trigger": {
                "price_type": 1,
                "price": "90",
                "rule": 1,
            },
            "text": "t-martin-auto-9",
        }
        adapter.rest.privateFuturesPostSettlePriceOrders.return_value = {
            "id": "auto-9"
        }
        adapter.rest.parse_order.return_value = {
            "id": "auto-9",
            "info": {"trigger": {"price": "90"}},
        }

        result = adapter.create_trigger_order(
            "ETH/USDT:USDT",
            "buy",
            1.0,
            90.0,
            price=89.5,
            params={"clientOrderId": "t-martin-auto-9"},
        )

        sent_request = (
            adapter.rest.privateFuturesPostSettlePriceOrders.call_args.args[0]
        )
        self.assertEqual(
            sent_request["initial"]["text"],
            "t-martin-auto-9",
        )
        self.assertNotIn("text", sent_request)
        self.assertNotIn("clientOrderId", sent_request)
        self.assertNotIn("clientOid", sent_request)
        self.assertEqual(result["clientOrderId"], "t-martin-auto-9")
        self.assertEqual(result["type"], "trigger")


if __name__ == "__main__":
    unittest.main()
