import asyncio
import base64
import hashlib
import hmac
import json
import os
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
for dependency_dir in (ROOT / ".deps-local", ROOT / ".deps"):
    if dependency_dir.exists() and str(dependency_dir) not in sys.path:
        sys.path.insert(0, str(dependency_dir))

from trading.exchanges import create_exchange_adapter
from trading.exchanges.weex import (
    WeexAPIError,
    WeexExchangeAdapter,
    WeexServerClock,
    WeexWebSocketClient,
    normalize_client_id,
    symbol_to_weex_id,
)
from trading.runtime_config import apply_env_overrides


BASE_CONFIG = {
    "exchange": "weex",
    "sandbox": False,
    "symbol": "BTC/USDT:USDT",
    "apiKey": "test-key",
    "secretKey": "test-secret",
    "passphrase": "test-passphrase",
    "restMinIntervalSeconds": 0,
    "networkRetryAttempts": 1,
    "wsEnabled": False,
    "weexTimeSyncEnabled": False,
}


def make_adapter(test_case: unittest.TestCase) -> WeexExchangeAdapter:
    # Some test environments preload another ``requests`` namespace before
    # this module can add the repository's isolated dependencies to sys.path.
    # Network behavior is mocked in every adapter test, so provide a disposable
    # session constructor and keep the tests independent of that global state.
    with patch("trading.exchanges.weex.requests.Session", create=True):
        adapter = WeexExchangeAdapter(dict(BASE_CONFIG))
    adapter._market_cache = {
        "BTCUSDT": {
            "id": "BTCUSDT",
            "symbol": "BTC/USDT:USDT",
            "precision": {"amount": 0.000001, "price": 0.1},
            "limits": {"amount": {"min": 0.0001, "max": 10000}},
        }
    }
    test_case.addCleanup(adapter.close)
    return adapter


class WeexAdapterTests(unittest.TestCase):
    def test_factory_selects_weex_and_demo_mode_fails_closed(self):
        with patch("trading.exchanges.weex.requests.Session", create=True):
            adapter = create_exchange_adapter(dict(BASE_CONFIG))
        self.addCleanup(adapter.close)
        self.assertIsInstance(adapter, WeexExchangeAdapter)
        self.assertEqual(adapter.name, "WEEX")
        self.assertEqual(adapter.ws_status()["transport"], "websocket")
        self.assertFalse(adapter.ws_status()["enabled"])

        with self.assertRaisesRegex(ValueError, "demo mode is incomplete"):
            create_exchange_adapter({**BASE_CONFIG, "sandbox": True})

    def test_symbol_client_id_and_signature_follow_weex_v3_rules(self):
        adapter = make_adapter(self)
        self.assertEqual(symbol_to_weex_id("BTC/USDT:USDT"), "BTCUSDT")
        self.assertEqual(normalize_client_id("cycle-1:entry_2"), "cycle-1:entry_2")
        with self.assertRaises(ValueError):
            normalize_client_id("x" * 37)

        timestamp = "1659076670000"
        method = "POST"
        path = "/capi/v3/order"
        query = ""
        body = '{"symbol":"BTCUSDT"}'
        message = f"{timestamp}{method}{path}{body}".encode()
        expected = base64.b64encode(
            hmac.new(b"test-secret", message, hashlib.sha256).digest()
        ).decode()
        self.assertEqual(adapter._signature(timestamp, method, path, query, body), expected)
        headers = adapter._headers(True, timestamp, expected)
        self.assertEqual(headers["ACCESS-KEY"], "test-key")
        self.assertEqual(headers["ACCESS-SIGN"], expected)
        self.assertEqual(headers["ACCESS-PASSPHRASE"], "test-passphrase")
        self.assertEqual(headers["ACCESS-TIMESTAMP"], timestamp)

    def test_load_markets_maps_precision_limits_and_contract_size(self):
        adapter = make_adapter(self)
        adapter._market_cache.clear()
        exchange_info = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "pricePrecision": 1,
                    "quantityPrecision": 6,
                    "contractVal": "0.000001",
                    "minLeverage": 1,
                    "maxLeverage": 408,
                    "minOrderSize": "0.0001",
                    "maxOrderSize": "10000",
                    "makerFeeRate": "0.0002",
                    "takerFeeRate": "0.0006",
                }
            ]
        }
        def public_get(path, _params=None):
            if path.endswith("/exchangeInfo"):
                return exchange_info
            if path.endswith("/apiTradingSymbols"):
                return ["BTCUSDT", "ETHUSDT"]
            raise AssertionError(path)

        with patch.object(adapter, "_public_get", side_effect=public_get):
            markets = adapter.load_markets()

        market = markets["BTC/USDT:USDT"]
        self.assertEqual(market["precision"], {"amount": 0.000001, "price": 0.1})
        self.assertEqual(market["limits"]["amount"]["min"], 0.0001)
        self.assertEqual(market["contractSize"], 0.000001)
        self.assertTrue(market["active"])
        self.assertEqual(adapter.amount_to_precision(adapter.symbol, 0.0012349), "0.001234")
        self.assertEqual(adapter.price_to_precision(adapter.symbol, 69000.19), "69000.1")

    def test_load_markets_rejects_symbol_not_open_for_api_trading(self):
        adapter = make_adapter(self)
        adapter._market_cache.clear()
        exchange_info = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "pricePrecision": 1,
                    "quantityPrecision": 6,
                    "contractVal": "0.000001",
                    "minOrderSize": "0.0001",
                }
            ]
        }

        def public_get(path, _params=None):
            return exchange_info if path.endswith("/exchangeInfo") else ["ETHUSDT"]

        with patch.object(adapter, "_public_get", side_effect=public_get):
            with self.assertRaisesRegex(RuntimeError, "not enabled for API trading"):
                adapter.load_markets()

    def test_normal_orders_map_open_and_reduce_only_position_sides(self):
        adapter = make_adapter(self)
        responses = [
            {"orderId": "open-1", "clientOrderId": "entry-1", "success": True},
            {"orderId": "close-1", "clientOrderId": "exit-1", "success": True},
        ]
        with patch.object(adapter, "_private_post", side_effect=responses) as send:
            adapter.create_order(
                adapter.symbol,
                "limit",
                "buy",
                0.0012349,
                69000.19,
                {"clientOid": "entry-1"},
            )
            adapter.create_order(
                adapter.symbol,
                "market",
                "sell",
                0.001,
                None,
                {"clientOid": "exit-1", "reduceOnly": True},
            )

        open_payload = send.call_args_list[0].args[1]
        close_payload = send.call_args_list[1].args[1]
        self.assertEqual(open_payload["positionSide"], "LONG")
        self.assertEqual(open_payload["quantity"], "0.001234")
        self.assertEqual(open_payload["price"], "69000.1")
        self.assertEqual(open_payload["newClientOrderId"], "entry-1")
        self.assertEqual(close_payload["side"], "SELL")
        self.assertEqual(close_payload["positionSide"], "LONG")
        self.assertEqual(close_payload["type"], "MARKET")
        self.assertNotIn("price", close_payload)

    def test_conditional_orders_select_stop_type_and_trigger_source(self):
        adapter = make_adapter(self)
        response = {"orderId": "algo-1", "clientOrderId": "layer-2", "success": True}
        with (
            patch.object(adapter, "fetch_ticker", return_value={"last": 70000.0}),
            patch.object(adapter, "_private_post", return_value=response) as send,
        ):
            adapter.create_trigger_order(
                adapter.symbol,
                "sell",
                0.001,
                68000,
                order_type="market",
                trigger_type="mark_price",
                params={"clientOid": "layer-2", "reduceOnly": True},
            )

        payload = send.call_args.args[1]
        self.assertEqual(payload["type"], "STOP_MARKET")
        self.assertEqual(payload["positionSide"], "LONG")
        self.assertEqual(payload["SlWorkingType"], "MARK_PRICE")
        self.assertNotIn("TpWorkingType", payload)

    def test_take_profit_conditional_uses_tp_working_type(self):
        adapter = make_adapter(self)
        with (
            patch.object(adapter, "fetch_ticker", return_value={"last": 70000.0}),
            patch.object(
                adapter,
                "_private_post",
                return_value={"orderId": "algo-2", "clientOrderId": "layer-buy"},
            ) as send,
        ):
            adapter.create_trigger_order(
                adapter.symbol,
                "buy",
                0.001,
                69000,
                price=68990,
                trigger_type="mark_price",
                params={"clientOid": "layer-buy"},
            )

        payload = send.call_args.args[1]
        self.assertEqual(payload["type"], "TAKE_PROFIT")
        self.assertEqual(payload["positionSide"], "LONG")
        self.assertEqual(payload["TpWorkingType"], "MARK_PRICE")
        self.assertNotIn("SlWorkingType", payload)

    def test_order_creating_post_is_not_automatically_retried(self):
        adapter = make_adapter(self)
        error = WeexAPIError("network_error", "response lost", request_rejected=False)
        with patch.object(adapter, "_private_post", side_effect=error) as send:
            with self.assertRaises(WeexAPIError):
                adapter.create_order(
                    adapter.symbol,
                    "market",
                    "buy",
                    0.001,
                    params={"clientOid": "one-shot"},
                )
        self.assertEqual(send.call_count, 1)

    def test_position_stop_loss_uses_full_position_and_mark_price(self):
        adapter = make_adapter(self)
        with patch.object(
            adapter,
            "_private_post",
            return_value=[{"success": True, "orderId": "stop-1"}],
        ) as send:
            result = adapter.place_position_stop_loss(
                adapter.symbol,
                "buy",
                65000.09,
                trigger_type="mark_price",
                client_oid="protect-1",
            )

        payload = send.call_args.args[1]
        self.assertEqual(send.call_args.args[0], "/capi/v3/placeTpSlOrder")
        self.assertEqual(payload["planType"], "STOP_LOSS")
        self.assertEqual(payload["positionSide"], "LONG")
        self.assertEqual(payload["quantity"], "0")
        self.assertEqual(payload["executePrice"], "0")
        self.assertEqual(payload["triggerPriceType"], "MARK_PRICE")
        self.assertEqual(result["id"], "stop-1")

    def test_balance_and_position_are_normalized_for_strategy(self):
        adapter = make_adapter(self)
        balance_rows = [
            {
                "asset": "USDT",
                "balance": "1000",
                "availableBalance": "800",
                "frozen": "100",
                "unrealizePnl": "25",
            }
        ]
        position_rows = [
            {
                "symbol": "BTCUSDT",
                "side": "LONG",
                "marginType": "CROSSED",
                "leverage": "10",
                "size": "0.02",
                "openValue": "1801.067",
                "marginSize": "180.1067",
                "unrealizePnl": "10",
                "liquidatePrice": "50000",
            }
        ]

        def private_get(path, _params=None):
            if path.endswith("/balance"):
                return balance_rows
            if path.endswith("/allPosition"):
                return position_rows
            raise AssertionError(path)

        with (
            patch.object(adapter, "_private_get", side_effect=private_get),
            patch.object(adapter, "_mark_price", return_value=91000.0),
        ):
            balance = adapter.fetch_balance()
            positions = adapter.fetch_positions([adapter.symbol])

        self.assertEqual(balance["USDT"], {"free": 800.0, "used": 100.0, "total": 1025.0})
        self.assertEqual(positions[0]["side"], "long")
        self.assertAlmostEqual(positions[0]["entryPrice"], 90053.35)
        self.assertEqual(positions[0]["markPrice"], 91000.0)
        self.assertEqual(positions[0]["contracts"], 0.02)
        self.assertEqual(positions[0]["info"]["posMode"], "hedge_mode")

    def test_position_mode_requires_futures_trading_permission(self):
        adapter = make_adapter(self)
        with patch.object(
            adapter,
            "_private_get",
            return_value={"canTrade": False, "dualSidePosition": False},
        ):
            with self.assertRaisesRegex(RuntimeError, "not permitted to trade futures"):
                adapter.fetch_position_mode()

        with patch.object(
            adapter,
            "_private_get",
            return_value={"canTrade": True, "dualSidePosition": False},
        ):
            self.assertEqual(adapter.fetch_position_mode(), "one_way_mode")

        with patch.object(
            adapter,
            "_private_get",
            return_value={"canTrade": True, "dualSidePosition": True},
        ):
            self.assertEqual(adapter.fetch_position_mode(), "hedge_mode")

    def test_order_normalization_infers_weex_hedge_mode_close_direction(self):
        adapter = make_adapter(self)
        close_long = adapter._normalize_standard_order(
            {
                "orderId": "close-long",
                "side": "SELL",
                "positionSide": "LONG",
                "origQty": "0.01",
                "status": "NEW",
            }
        )
        open_long = adapter._normalize_standard_order(
            {
                "orderId": "open-long",
                "side": "BUY",
                "positionSide": "LONG",
                "origQty": "0.01",
                "status": "NEW",
            }
        )

        self.assertTrue(close_long["reduceOnly"])
        self.assertEqual(close_long["positionSide"], "long")
        self.assertFalse(open_long["reduceOnly"])
        self.assertEqual(open_long["positionSide"], "long")

    def test_current_conditional_orders_separate_layers_and_position_tpsl(self):
        adapter = make_adapter(self)
        rows = [
            {
                "algoId": "layer-1",
                "clientAlgoId": "layer-client",
                "orderType": "TAKE_PROFIT",
                "quantity": "0.01",
                "algoStatus": "UNTRIGGERED",
                "closePosition": False,
            },
            {
                "algoId": "stop-1",
                "clientAlgoId": "stop-client",
                "orderType": "STOP_MARKET",
                "quantity": "0",
                "algoStatus": "UNTRIGGERED",
                "closePosition": True,
            },
            {
                "algoId": "tp-1",
                "clientAlgoId": "tp-client",
                "orderType": "TAKE_PROFIT_MARKET",
                "quantity": "0",
                "algoStatus": "UNTRIGGERED",
                "closePosition": True,
            },
        ]
        with patch.object(adapter, "_private_get", return_value=rows):
            layers = adapter.fetch_pending_trigger_orders(plan_type="normal_plan")
            stops = adapter.fetch_pending_trigger_orders(plan_type="pos_loss")
            take_profits = adapter.fetch_pending_trigger_orders(plan_type="profit_loss")

        self.assertEqual([row["id"] for row in layers], ["layer-1"])
        self.assertEqual([row["id"] for row in stops], ["stop-1"])
        self.assertEqual([row["id"] for row in take_profits], ["tp-1"])

    def test_ticker_uses_book_midpoint_and_rest_status(self):
        adapter = make_adapter(self)

        def public_get(path, _params=None):
            if path.endswith("/bookTicker"):
                return [{"bidPrice": "69999", "askPrice": "70001", "bidQty": "2", "askQty": "3"}]
            if path.endswith("/24hr"):
                return [{"lastPrice": "69995", "highPrice": "71000", "lowPrice": "68000"}]
            raise AssertionError(path)

        with (
            patch.object(adapter, "_public_get", side_effect=public_get),
            patch.object(adapter, "_mark_price", return_value=70002.0),
        ):
            ticker = adapter.fetch_ticker(adapter.symbol)

        self.assertEqual(ticker["last"], 70000.0)
        self.assertEqual(ticker["mark"], 70002.0)
        self.assertEqual(ticker["bidSize"], 2.0)
        self.assertEqual(ticker["high"], 71000.0)

    def test_adapter_prefers_fresh_ws_cache_and_falls_back_safely(self):
        adapter = make_adapter(self)
        adapter.ws.enabled = True
        adapter.ws.public_enabled = True
        adapter.ws._consume_ticker(
            {"s": "BTCUSDT", "E": 1773295738939},
            [{"c": "70000.1", "m": "70001.2", "h": "71000", "l": "68000"}],
        )
        adapter.ws._touch("public")
        adapter.ws._consume_candles(
            [
                {
                    "t": 1773295500000,
                    "i": "5m",
                    "o": "69900",
                    "h": "70100",
                    "l": "69800",
                    "c": "70000",
                    "v": "12.5",
                }
            ]
        )
        adapter.ws._consume_positions(
            [
                {
                    "symbol": "BTCUSDT",
                    "side": "LONG",
                    "size": "0.01",
                    "openValue": "690",
                    "leverage": "10",
                    "marginMode": "CROSSED",
                }
            ]
        )
        adapter.ws._consume_fills(
            [
                {
                    "id": "fill-cache-1",
                    "symbol": "BTCUSDT",
                    "orderId": "order-cache-1",
                    "positionSide": "LONG",
                    "orderSide": "BUY",
                    "fillSize": "0.01",
                    "fillValue": "700",
                    "fillFee": "0.42",
                    "coin": "USDT",
                    "direction": "TAKER",
                    "createdTime": "1773295739200",
                }
            ]
        )

        with patch.object(adapter, "_public_get", side_effect=AssertionError("REST ticker used")):
            ticker = adapter.fetch_ticker(adapter.symbol)
        self.assertEqual(ticker["last"], 70000.1)

        with patch.object(adapter, "_private_get", side_effect=AssertionError("REST position used")):
            positions = adapter.fetch_positions([adapter.symbol])
        self.assertEqual(positions[0]["entryPrice"], 69000.0)

        with patch.object(adapter, "_seed_ohlcv", return_value=[]):
            candles = adapter.fetch_ohlcv(adapter.symbol, "5m", limit=10)
        self.assertEqual(candles[-1][4], 70000.0)

        with patch.object(adapter, "_retry", side_effect=ConnectionError("REST unavailable")):
            trades = adapter.fetch_my_trades(adapter.symbol, limit=10)
        self.assertEqual(trades[-1]["id"], "fill-cache-1")

    def test_adapter_exposes_both_hedge_sides_from_fresh_ws_cache(self):
        adapter = make_adapter(self)
        adapter.ws.enabled = True
        adapter.ws.private_enabled = True
        adapter.ws._consume_positions(
            [
                {
                    "symbol": "BTCUSDT",
                    "side": "LONG",
                    "size": "0.01",
                    "openValue": "690",
                    "leverage": "10",
                    "marginMode": "CROSSED",
                },
                {
                    "symbol": "BTCUSDT",
                    "side": "SHORT",
                    "size": "0.02",
                    "openValue": "1420",
                    "leverage": "10",
                    "marginMode": "CROSSED",
                },
            ]
        )

        with patch.object(adapter, "_private_get", side_effect=AssertionError("REST position used")):
            positions = adapter.fetch_positions([adapter.symbol])

        self.assertEqual(len(positions), 2)
        self.assertEqual({row["side"] for row in positions}, {"long", "short"})
        self.assertEqual({row["info"]["posMode"] for row in positions}, {"hedge_mode"})


class WeexWebSocketTests(unittest.TestCase):
    def make_client(self, **overrides):
        return WeexWebSocketClient(
            {
                **BASE_CONFIG,
                "wsEnabled": True,
                "wsPublicEnabled": True,
                "wsPrivateEnabled": True,
                "timeframe": "5m",
                "sr_timeframe": "1h",
                "wsReconnectDelay": 0,
                **overrides,
            }
        )

    def test_headers_and_subscriptions_follow_weex_v3_protocol(self):
        client = self.make_client()
        with patch("trading.exchanges.weex.time.time", return_value=1_659_076_670.0):
            headers = client._private_headers()
        timestamp = "1659076670000"
        expected = base64.b64encode(
            hmac.new(
                b"test-secret",
                f"{timestamp}/v3/ws/private".encode(),
                hashlib.sha256,
            ).digest()
        ).decode()
        self.assertEqual(headers["ACCESS-TIMESTAMP"], timestamp)
        self.assertEqual(headers["ACCESS-SIGN"], expected)
        self.assertEqual(headers["User-Agent"], "bitget-pro-trader/WEEX-V3")
        self.assertEqual(
            client._build_subscriptions("private"),
            ["account", "positions", "fill", "orders"],
        )
        public = client._build_subscriptions("public")
        self.assertIn("BTCUSDT@ticker", public)
        self.assertIn("BTCUSDT@depth15", public)
        self.assertIn("BTCUSDT@kline_5m_LAST_PRICE", public)
        self.assertIn("BTCUSDT@kline_1h_LAST_PRICE", public)

    def test_server_clock_uses_round_trip_midpoint_offset(self):
        class Response:
            status_code = 200

            @staticmethod
            def json():
                return {"serverTime": 1_001_100}

        class Session:
            @staticmethod
            def request(*_args, **_kwargs):
                return Response()

            @staticmethod
            def close():
                return None

        clock = WeexServerClock(
            {
                **BASE_CONFIG,
                "weexTimeSyncEnabled": True,
                "weexTimeSyncIntervalSeconds": 300,
            }
        )
        with (
            patch("trading.exchanges.weex.requests.Session", return_value=Session(), create=True),
            patch("trading.exchanges.weex.time.time", side_effect=[1000.0, 1000.2]),
        ):
            self.assertTrue(clock.sync(force=True))
        self.assertAlmostEqual(clock.offset_ms, 1000.0, places=6)

    def test_public_and_private_payloads_are_normalized_for_the_bot(self):
        client = self.make_client(wsFreshSeconds=30)

        async def feed():
            await client._handle_raw_message(
                "public",
                json.dumps(
                    {
                        "e": "ticker",
                        "E": 1773295738939,
                        "s": "BTCUSDT",
                        "d": [
                            {
                                "c": "70000.1",
                                "m": "70001.2",
                                "i": "69999.8",
                                "h": "71000",
                                "l": "68000",
                                "P": "1.25",
                                "v": "123",
                                "q": "8610000",
                            }
                        ],
                    }
                ),
            )
            await client._handle_raw_message(
                "public",
                json.dumps(
                    {
                        "e": "depth",
                        "E": 1773295738940,
                        "s": "BTCUSDT",
                        "b": [["70000.0", "2"]],
                        "a": [["70000.2", "3"]],
                    }
                ),
            )
            await client._handle_raw_message(
                "public",
                json.dumps(
                    {
                        "e": "kline",
                        "E": 1773295739000,
                        "s": "BTCUSDT",
                        "d": [
                            {
                                "t": 1773295500000,
                                "i": "5m",
                                "o": "69900",
                                "h": "70100",
                                "l": "69800",
                                "c": "70000",
                                "v": "12.5",
                            }
                        ],
                    }
                ),
            )
            await client._handle_raw_message(
                "private",
                json.dumps(
                    {
                        "e": "positions",
                        "E": 1773295739100,
                        "d": [
                            {
                                "symbol": "BTCUSDT",
                                "side": "LONG",
                                "size": "0.01",
                                "openValue": "690",
                                "leverage": "10",
                                "marginMode": "CROSSED",
                                "isolatedMargin": "0",
                            }
                        ],
                    }
                ),
            )
            await client._handle_raw_message(
                "private",
                json.dumps(
                    {
                        "e": "fill",
                        "E": 1773295739200,
                        "d": [
                            {
                                "id": "fill-1",
                                "symbol": "BTCUSDT",
                                "orderId": "order-1",
                                "positionSide": "LONG",
                                "orderSide": "BUY",
                                "fillSize": "0.01",
                                "fillValue": "700",
                                "fillFee": "0.42",
                                "coin": "USDT",
                                "direction": "TAKER",
                                "createdTime": "1773295739200",
                            }
                        ],
                    }
                ),
            )
            await client._handle_raw_message(
                "private",
                json.dumps(
                    {
                        "e": "orders",
                        "E": 1773295739300,
                        "d": [
                            {
                                "id": "order-2",
                                "symbol": "BTCUSDT",
                                "orderSide": "BUY",
                                "size": "0.02",
                                "cumFillSize": "0",
                                "status": "NEW",
                            }
                        ],
                    }
                ),
            )

        asyncio.run(feed())
        ticker = client.get_ticker("BTCUSDT")
        self.assertEqual(ticker["lastPr"], "70000.1")
        self.assertEqual(ticker["bidPr"], "70000.0")
        self.assertEqual(ticker["askPr"], "70000.2")
        self.assertTrue(client.is_ticker_fresh("BTCUSDT"))
        self.assertEqual(client.get_candles("5m")[0][4], 70000.0)
        position = client.get_position("BTCUSDT")
        self.assertEqual(position["total"], 0.01)
        self.assertEqual(position["holdSide"], "long")
        self.assertEqual(position["openPriceAvg"], 69000.0)
        self.assertEqual(position["markPrice"], 70001.2)
        self.assertTrue(client.is_position_fresh("BTCUSDT"))
        self.assertEqual(client.get_fills("BTCUSDT")[0]["id"], "fill-1")
        self.assertEqual(client.get_orders("BTCUSDT")[0]["id"], "order-2")

    def test_invalid_or_partial_kline_never_overwrites_valid_candle(self):
        client = self.make_client(wsFreshSeconds=30)
        valid = {
            "t": 1773295500000,
            "i": "5m",
            "o": "69900",
            "h": "70100",
            "l": "69800",
            "c": "70000",
            "v": "12.5",
        }
        client._consume_candles([valid])
        expected = client.get_candles("5m")[0]

        client._consume_candles([{**valid, "l": "0", "c": "65000"}])
        client._consume_candles([{key: value for key, value in valid.items() if key != "h"}])

        self.assertEqual(client.get_candles("5m"), [expected])
        self.assertIn("invalid WEEX kline", client.snapshot()["public"]["last_error"])

        client._consume_candles([{**valid, "c": "70010"}])
        self.assertEqual(client.get_candles("5m")[0][4], 70010.0)
        self.assertEqual(client.snapshot()["public"]["last_error"], "")

    def test_private_position_cache_keeps_long_and_short_rows_separate(self):
        client = self.make_client(wsFreshSeconds=30)
        client._consume_positions(
            [
                {
                    "symbol": "BTCUSDT",
                    "side": "LONG",
                    "size": "0.01",
                    "openValue": "690",
                    "leverage": "10",
                    "marginMode": "CROSSED",
                },
                {
                    "symbol": "BTCUSDT",
                    "side": "SHORT",
                    "size": "0.02",
                    "openValue": "1420",
                    "leverage": "10",
                    "marginMode": "CROSSED",
                },
            ]
        )

        positions = client.get_positions("BTCUSDT")

        self.assertEqual(len(positions), 2)
        self.assertEqual({row["holdSide"] for row in positions}, {"long", "short"})
        self.assertEqual(client.get_position("BTCUSDT")["holdSide"], "long")

    def test_server_ping_gets_pong_and_disconnect_reconnects(self):
        client = self.make_client(wsPrivateEnabled=False)

        class Socket:
            def __init__(self):
                self.sent = []

            async def send(self, payload):
                self.sent.append(payload)

            async def recv(self):
                raise ConnectionError("connection dropped")

            async def close(self):
                return None

        ping_socket = Socket()
        client._connections["public"] = ping_socket
        asyncio.run(
            client._handle_raw_message(
                "public",
                '{"event":"ping","time":"1693208170000"}',
            )
        )
        self.assertEqual(ping_socket.sent, ['{"method":"PONG","id":1}'])
        self.assertIsNotNone(client.snapshot()["public"]["last_pong_at"])

        class Context:
            def __init__(self, factory):
                self.factory = factory
                self.socket = Socket()

            async def __aenter__(self):
                self.factory.enters += 1
                return self.socket

            async def __aexit__(self, _exc_type, _exc, _tb):
                if self.factory.enters >= 2:
                    client._stop_event.set()
                return False

        class Factory:
            def __init__(self):
                self.enters = 0
                self.kwargs = []

            def __call__(self, *_args, **kwargs):
                self.kwargs.append(kwargs)
                return Context(self)

        factory = Factory()

        async def run_worker():
            with patch("trading.exchanges.weex.websockets.connect", factory):
                await asyncio.wait_for(client._connection_worker("public"), timeout=1)

        asyncio.run(run_worker())
        self.assertEqual(factory.enters, 2)
        self.assertEqual(client.snapshot()["public"]["reconnects"], 2)
        self.assertEqual(factory.kwargs[0]["user_agent_header"], "bitget-pro-trader/WEEX-V3")


class WeexRuntimeConfigTests(unittest.TestCase):
    def test_weex_credentials_do_not_get_overridden_by_bitget_environment(self):
        env = {
            "BITGET_API_KEY": "bitget-key",
            "BITGET_SECRET_KEY": "bitget-secret",
            "BITGET_PASSPHRASE": "bitget-pass",
            "WEEX_API_KEY": "weex-key",
            "WEEX_SECRET_KEY": "weex-secret",
            "WEEX_PASSPHRASE": "weex-pass",
        }
        with patch.dict(os.environ, env, clear=True):
            resolved = apply_env_overrides({"exchange": "weex"})
        self.assertEqual(resolved["apiKey"], "weex-key")
        self.assertEqual(resolved["secretKey"], "weex-secret")
        self.assertEqual(resolved["passphrase"], "weex-pass")

    def test_bitget_environment_remains_backward_compatible(self):
        env = {
            "BITGET_API_KEY": "bitget-key",
            "BITGET_SECRET_KEY": "bitget-secret",
            "BITGET_PASSPHRASE": "bitget-pass",
            "WEEX_API_KEY": "weex-key",
        }
        with patch.dict(os.environ, env, clear=True):
            resolved = apply_env_overrides({"exchange": "bitget"})
        self.assertEqual(resolved["apiKey"], "bitget-key")
        self.assertEqual(resolved["secretKey"], "bitget-secret")
        self.assertEqual(resolved["passphrase"], "bitget-pass")


if __name__ == "__main__":
    unittest.main()
