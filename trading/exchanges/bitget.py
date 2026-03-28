"""Bitget adapter with REST trading and WebSocket market/account cache."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import hmac
import json
import threading
import time
from collections import deque
from copy import deepcopy
from decimal import Decimal, ROUND_DOWN
from typing import Any
from urllib.parse import urlencode

import ccxt
import requests
import websockets

from .base import ExchangeAdapter


TIMEFRAME_TO_CHANNEL = {
    "1m": "candle1m",
    "5m": "candle5m",
    "15m": "candle15m",
    "30m": "candle30m",
    "1h": "candle1H",
    "4h": "candle4H",
    "6h": "candle6H",
    "12h": "candle12H",
    "1d": "candle1D",
    "1w": "candle1W",
}
TIMEFRAME_TO_REST_GRANULARITY = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "4h": "4H",
    "6h": "6H",
    "12h": "12H",
    "1d": "1D",
    "1w": "1W",
    "1M": "1M",
}
OPEN_ORDER_STATUSES = {"live", "new", "partially_filled"}
CLOSED_ORDER_STATUSES = {"filled", "canceled", "cancelled"}
CLOSE_TRADE_SIDE_TOKENS = (
    "close",
    "reduce_close",
    "burst_close",
    "offset_close",
    "delivery_close",
    "adl_close",
    "dte_sys_adl_close",
)
DEFAULT_TIMEOUT = 10


def normalize_timeframe(timeframe: str | None, default: str = "5m") -> str:
    value = str(timeframe or default).strip()
    if value == "1M":
        return value
    if value.endswith("H"):
        return value[:-1] + "h"
    if value.endswith("D"):
        return value[:-1] + "d"
    if value.endswith("W"):
        return value[:-1] + "w"
    return value.lower()


def timeframe_to_rest_granularity(timeframe: str | None, default: str = "5m") -> str:
    normalized = normalize_timeframe(timeframe, default)
    return TIMEFRAME_TO_REST_GRANULARITY.get(normalized, normalized)


def symbol_to_inst_id(symbol: str) -> str:
    if ":" in symbol:
        pair, settle = symbol.split(":", 1)
        base, quote = pair.split("/", 1)
        return f"{base}{settle or quote}"
    if "/" in symbol:
        return "".join(symbol.split("/"))
    return symbol.replace("-", "").replace("_", "").replace(":", "")


def infer_inst_type(config: dict[str, Any], symbol: str) -> str:
    default_type = str(config.get("defaultType", "swap")).lower()
    if config.get("instType"):
        return str(config["instType"])
    if default_type == "spot" and ":" not in symbol:
        return "SPOT"
    if symbol.endswith(":USDT"):
        return "USDT-FUTURES"
    if symbol.endswith(":USDC"):
        return "USDC-FUTURES"
    return "USDT-FUTURES"


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_margin_mode(value: str | None, default: str = "crossed") -> str:
    mode = str(value or default).strip().lower()
    if mode in {"cross", "crossed"}:
        return "crossed"
    if mode in {"isolated", "fixed"}:
        return "isolated"
    return default


def quantize_to_step(value: float, step: float) -> str:
    step_decimal = Decimal(str(step))
    value_decimal = Decimal(str(value))
    if step_decimal <= 0:
        return format(value_decimal, "f")
    units = (value_decimal / step_decimal).to_integral_value(rounding=ROUND_DOWN)
    quantized = units * step_decimal
    decimals = max(-step_decimal.as_tuple().exponent, 0)
    return f"{quantized:.{decimals}f}"


def position_percentage_from_row(row: dict[str, Any], side: str, entry_price: float, mark_price: float, unrealized_pnl: float) -> float:
    for key in ("unrealizedPLR", "uplRate", "profitRate"):
        raw = row.get(key)
        if raw not in (None, ""):
            explicit_ratio = safe_float(raw, 0.0)
            if explicit_ratio != 0:
                return explicit_ratio * 100

    margin_size = safe_float(row.get("marginSize", row.get("margin", 0)), 0.0)
    if margin_size > 0:
        return (unrealized_pnl / margin_size) * 100

    if entry_price > 0 and mark_price > 0:
        leverage = safe_float(row.get("leverage", 0), 0.0)
        if leverage <= 0:
            leverage = 1.0
        direction = -1.0 if str(side or "").lower() == "short" else 1.0
        return direction * ((mark_price - entry_price) / entry_price) * leverage * 100

    return 0.0


class BitgetWebSocketClient:
    """Background WebSocket client with local caches."""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.enabled = bool(config.get("wsEnabled", True))
        self.public_enabled = bool(config.get("wsPublicEnabled", True))
        self.private_enabled = bool(config.get("wsPrivateEnabled", True))
        self.symbol = config.get("symbol", "ETH/USDT:USDT")
        self.inst_id = symbol_to_inst_id(self.symbol)
        self.inst_type = infer_inst_type(config, self.symbol)
        self.sandbox = bool(config.get("sandbox", True))
        self.timeframes = sorted(
            {
                normalize_timeframe(config.get("timeframe", "5m")),
                normalize_timeframe(config.get("sr_timeframe", "1h")),
            }
        )
        self.fresh_seconds = float(config.get("wsFreshSeconds", 15))
        self.max_fills = max(int(config.get("wsMaxFills", 200)), 20)
        self.max_candles = max(int(config.get("wsMaxCandles", 400)), 120)
        self.reconnect_delay = float(config.get("wsReconnectDelay", 3))
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._started = False
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tasks: list[asyncio.Task[Any]] = []
        self._connections: dict[str, Any] = {}
        self._public_url = "wss://wspap.bitget.com/v2/ws/public" if self.sandbox else "wss://ws.bitget.com/v2/ws/public"
        self._private_url = "wss://wspap.bitget.com/v2/ws/private" if self.sandbox else "wss://ws.bitget.com/v2/ws/private"
        self._ticker: dict[str, dict[str, Any]] = {}
        self._positions: dict[str, dict[str, Any]] = {}
        self._orders: dict[str, dict[str, Any]] = {}
        self._fills: deque[dict[str, Any]] = deque(maxlen=self.max_fills)
        self._accounts: dict[str, dict[str, Any]] = {}
        self._candles: dict[str, dict[int, list[float]]] = {
            TIMEFRAME_TO_CHANNEL[timeframe]: {}
            for timeframe in self.timeframes
            if timeframe in TIMEFRAME_TO_CHANNEL
        }
        self._status = {
            "enabled": self.enabled,
            "transport": "websocket",
            "symbol": self.symbol,
            "inst_id": self.inst_id,
            "inst_type": self.inst_type,
            "started": False,
            "public": self._make_channel_status(),
            "private": self._make_channel_status(),
        }

    def _make_channel_status(self) -> dict[str, Any]:
        return {
            "connected": False,
            "last_message_at": None,
            "last_pong_at": None,
            "last_error": None,
            "subscriptions": [],
            "reconnects": 0,
        }

    def start(self) -> None:
        if not self.enabled or self._started:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._thread_main, name="bitget-ws", daemon=True)
        self._thread.start()
        self._started = True
        with self._lock:
            self._status["started"] = True

    def stop(self) -> None:
        if not self._started:
            return
        self._stop_event.set()
        if self._loop is not None:
            with contextlib.suppress(Exception):
                asyncio.run_coroutine_threadsafe(self._close_connections(), self._loop).result(timeout=5)
            for task in list(self._tasks):
                task.cancel()
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._thread = None
        self._loop = None
        self._tasks = []
        self._started = False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return deepcopy(self._status)

    def is_fresh(self, channel: str) -> bool:
        last_message = self.snapshot().get(channel, {}).get("last_message_at")
        if not last_message:
            return False
        return (time.time() - last_message) <= self.fresh_seconds

    def get_ticker(self, inst_id: str) -> dict[str, Any] | None:
        with self._lock:
            payload = self._ticker.get(inst_id)
            return deepcopy(payload) if payload else None

    def get_position(self, inst_id: str) -> dict[str, Any] | None:
        with self._lock:
            payload = self._positions.get(inst_id)
            return deepcopy(payload) if payload else None

    def get_orders(self, inst_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = [deepcopy(order) for order in self._orders.values() if order.get("instId") == inst_id]
        return rows

    def get_account(self, coin: str) -> dict[str, Any] | None:
        with self._lock:
            payload = self._accounts.get(coin)
            return deepcopy(payload) if payload else None

    def get_fills(self, inst_id: str, limit: int = 60) -> list[dict[str, Any]]:
        with self._lock:
            rows = [deepcopy(fill) for fill in self._fills if fill.get("symbol") == inst_id]
        return rows[-limit:]

    def get_candles(self, timeframe: str) -> list[list[float]]:
        channel = TIMEFRAME_TO_CHANNEL.get(normalize_timeframe(timeframe))
        if channel is None:
            return []
        with self._lock:
            rows = list(self._candles.get(channel, {}).values())
        rows.sort(key=lambda row: row[0])
        return deepcopy(rows)

    def _thread_main(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._tasks = []
        if self.public_enabled:
            self._tasks.append(self._loop.create_task(self._connection_worker("public")))
        if self.private_enabled and self._has_private_credentials():
            self._tasks.append(self._loop.create_task(self._connection_worker("private")))
        if not self._tasks:
            return
        try:
            self._loop.run_forever()
        finally:
            with contextlib.suppress(Exception):
                self._loop.run_until_complete(self._close_connections())
                self._loop.run_until_complete(asyncio.sleep(0.05))
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            with contextlib.suppress(Exception):
                self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                self._loop.run_until_complete(asyncio.sleep(0.05))
            self._loop.close()

    async def _close_connections(self) -> None:
        for websocket in list(self._connections.values()):
            with contextlib.suppress(Exception):
                await websocket.close()

    def _has_private_credentials(self) -> bool:
        return all(
            str(self.config.get(key, "")).strip()
            for key in ("apiKey", "secretKey", "passphrase")
        )

    async def _connection_worker(self, channel: str) -> None:
        url = self._public_url if channel == "public" else self._private_url
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(url, ping_interval=None, close_timeout=5, open_timeout=10) as websocket:
                    self._connections[channel] = websocket
                    self._set_channel_state(channel, connected=True, last_error=None, bump_reconnect=True)
                    if channel == "private":
                        await self._login(websocket)
                    await self._subscribe(websocket, channel)
                    ping_task = asyncio.create_task(self._ping_loop(websocket, channel))
                    try:
                        async for raw_message in websocket:
                            if self._stop_event.is_set():
                                break
                            await self._handle_raw_message(channel, raw_message)
                    finally:
                        ping_task.cancel()
                        with contextlib.suppress(Exception):
                            await ping_task
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._set_channel_state(channel, connected=False, last_error=str(exc))
                if self._stop_event.is_set():
                    break
                await asyncio.sleep(self.reconnect_delay)
            finally:
                self._connections.pop(channel, None)
                self._set_channel_state(channel, connected=False)

    async def _ping_loop(self, websocket: Any, channel: str) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(25)
            await websocket.send("ping")
            self._touch(channel)

    async def _login(self, websocket: Any) -> None:
        timestamp = str(int(time.time() * 1000))
        payload = {
            "op": "login",
            "args": [
                {
                    "apiKey": self.config.get("apiKey", ""),
                    "passphrase": self.config.get("passphrase", ""),
                    "timestamp": timestamp,
                    "sign": self._build_signature(timestamp),
                }
            ],
        }
        await websocket.send(json.dumps(payload))
        deadline = time.time() + 10
        while time.time() < deadline:
            raw_message = await websocket.recv()
            if raw_message == "pong":
                self._set_channel_state("private", last_pong_at=time.time())
                continue
            data = json.loads(raw_message)
            if data.get("event") == "login":
                code = str(data.get("code", "0"))
                if code not in {"0", "", "00000"}:
                    raise RuntimeError(f"Bitget WebSocket login failed: {data.get('msg') or code}")
                self._touch("private")
                return
            await self._handle_payload("private", data)
        raise RuntimeError("Bitget WebSocket login timed out")

    def _build_signature(self, timestamp: str) -> str:
        message = f"{timestamp}GET/user/verify".encode("utf-8")
        secret = str(self.config.get("secretKey", "")).encode("utf-8")
        digest = hmac.new(secret, message, digestmod=hashlib.sha256).digest()
        return base64.b64encode(digest).decode("utf-8")

    async def _subscribe(self, websocket: Any, channel: str) -> None:
        args = self._build_subscriptions(channel)
        if not args:
            return
        await websocket.send(json.dumps({"op": "subscribe", "args": args}))
        self._set_channel_state(channel, subscriptions=args)

    def _build_subscriptions(self, channel: str) -> list[dict[str, Any]]:
        if channel == "public":
            args = [
                {"instType": self.inst_type, "channel": "ticker", "instId": self.inst_id},
            ]
            for timeframe in self.timeframes:
                candle_channel = TIMEFRAME_TO_CHANNEL.get(timeframe)
                if candle_channel:
                    args.append({"instType": self.inst_type, "channel": candle_channel, "instId": self.inst_id})
            return args
        return [
            {"instType": self.inst_type, "channel": "account", "coin": "default"},
            {"instType": self.inst_type, "channel": "positions", "instId": "default"},
            {"instType": self.inst_type, "channel": "orders", "instId": "default"},
            {"instType": self.inst_type, "channel": "fill", "instId": "default"},
        ]

    async def _handle_raw_message(self, channel: str, raw_message: str) -> None:
        self._touch(channel)
        if raw_message == "pong":
            self._set_channel_state(channel, last_pong_at=time.time())
            return
        if raw_message == "ping":
            websocket = self._connections.get(channel)
            if websocket is not None:
                await websocket.send("pong")
            return
        payload = json.loads(raw_message)
        await self._handle_payload(channel, payload)

    async def _handle_payload(self, channel: str, payload: dict[str, Any]) -> None:
        if payload.get("event") == "error":
            self._set_channel_state(channel, last_error=payload.get("msg") or payload.get("code"))
            return

        arg = payload.get("arg") or {}
        topic = arg.get("channel")
        data = payload.get("data") or []
        if topic == "ticker":
            self._consume_ticker(data)
            return
        if topic in self._candles:
            self._consume_candles(topic, data)
            return
        if topic == "positions":
            self._consume_positions(data)
            return
        if topic == "account":
            self._consume_accounts(data)
            return
        if topic == "orders":
            self._consume_orders(data)
            return
        if topic == "fill":
            self._consume_fills(data)

    def _consume_ticker(self, data: list[dict[str, Any]]) -> None:
        with self._lock:
            for row in data:
                inst_id = row.get("instId") or row.get("symbol")
                if inst_id:
                    self._ticker[inst_id] = dict(row)

    def _consume_candles(self, channel: str, data: list[list[str]]) -> None:
        with self._lock:
            store = self._candles.setdefault(channel, {})
            for candle in data:
                if len(candle) < 6:
                    continue
                timestamp = int(float(candle[0]))
                store[timestamp] = [
                    timestamp,
                    safe_float(candle[1]),
                    safe_float(candle[2]),
                    safe_float(candle[3]),
                    safe_float(candle[4]),
                    safe_float(candle[5]),
                ]
            while len(store) > self.max_candles:
                oldest = min(store)
                store.pop(oldest, None)

    def _consume_positions(self, data: list[dict[str, Any]]) -> None:
        with self._lock:
            for row in data:
                inst_id = row.get("instId")
                if not inst_id:
                    continue
                self._positions[inst_id] = dict(row)
            if not data:
                self._positions.pop(self.inst_id, None)

    def _consume_accounts(self, data: list[dict[str, Any]]) -> None:
        with self._lock:
            for row in data:
                margin_coin = row.get("marginCoin")
                if margin_coin:
                    self._accounts[margin_coin] = dict(row)

    def _consume_orders(self, data: list[dict[str, Any]]) -> None:
        with self._lock:
            for row in data:
                order_id = row.get("orderId")
                if not order_id:
                    continue
                status = str(row.get("status", "")).lower()
                if status in CLOSED_ORDER_STATUSES:
                    self._orders.pop(order_id, None)
                    continue
                self._orders[order_id] = dict(row)

    def _consume_fills(self, data: list[dict[str, Any]]) -> None:
        with self._lock:
            for row in data:
                self._fills.append(dict(row))

    def _touch(self, channel: str) -> None:
        self._set_channel_state(channel, last_message_at=time.time())

    def _set_channel_state(self, channel: str, bump_reconnect: bool = False, **updates: Any) -> None:
        with self._lock:
            state = self._status[channel]
            if bump_reconnect:
                state["reconnects"] += 1
            state.update({key: value for key, value in updates.items() if value is not None})


class BitgetExchangeAdapter(ExchangeAdapter):
    """Adapter used by strategy and dashboard code."""

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.symbol = config.get("symbol", "ETH/USDT:USDT")
        self.inst_id = symbol_to_inst_id(self.symbol)
        self.inst_type = infer_inst_type(config, self.symbol)
        self.margin_coin = str(config.get("marginCoin", "USDT"))
        self.margin_mode = normalize_margin_mode(config.get("marginMode"), "crossed")
        self.retry_attempts = max(int(config.get("networkRetryAttempts", 4)), 1)
        self.retry_delay = float(config.get("networkRetryDelay", 2))
        self.retry_backoff = float(config.get("networkRetryBackoff", 1.8))
        self.ws = BitgetWebSocketClient(config)
        self._ohlcv_seed: dict[str, list[list[float]]] = {}
        self._ohlcv_seed_at: dict[str, float] = {}
        self._market_cache: dict[str, dict[str, Any]] = {}
        self._base_url = str(config.get("restBaseUrl", "https://api.bitget.com")).rstrip("/")
        self._rest_timeout = float(config.get("restTimeoutSeconds", DEFAULT_TIMEOUT))
        self._session = requests.Session()
        self.rest = self._create_rest_client()
        self.start()

    @property
    def name(self) -> str:
        return "Bitget"

    def __getattr__(self, name: str) -> Any:
        return getattr(self.rest, name)

    def start(self) -> None:
        self.ws.start()

    def close(self) -> None:
        self.ws.stop()
        with contextlib.suppress(Exception):
            self._session.close()
        with contextlib.suppress(Exception):
            self.rest.close()

    def ws_status(self) -> dict[str, Any]:
        return self.ws.snapshot()

    def _create_rest_client(self):
        client = ccxt.bitget(
            {
                "apiKey": self.config.get("apiKey", ""),
                "secret": self.config.get("secretKey", ""),
                "password": self.config.get("passphrase", ""),
                "enableRateLimit": True,
                "options": {
                    "defaultType": self.config.get("defaultType", "swap"),
                },
            }
        )
        if self.config.get("sandbox", True):
            client.set_sandbox_mode(True)
        return client

    def _headers(self, private: bool, timestamp: str = "", signature: str = "") -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "locale": "zh-CN",
        }
        if private:
            headers.update(
                {
                    "ACCESS-KEY": str(self.config.get("apiKey", "")),
                    "ACCESS-SIGN": signature,
                    "ACCESS-TIMESTAMP": timestamp,
                    "ACCESS-PASSPHRASE": str(self.config.get("passphrase", "")),
                }
            )
            if self.config.get("sandbox", True):
                headers["paptrading"] = "1"
        return headers

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        private: bool = False,
    ) -> Any:
        query = ""
        clean_params: dict[str, Any] = {}
        if params:
            clean_params = {
                key: value
                for key, value in params.items()
                if value is not None
            }
            if clean_params:
                query = "?" + urlencode(clean_params, doseq=True)
        body = ""
        if payload is not None:
            clean_payload = {key: value for key, value in payload.items() if value is not None}
            body = json.dumps(clean_payload, separators=(",", ":"))
        timestamp = str(int(time.time() * 1000))
        signature = ""
        if private:
            prehash = f"{timestamp}{method.upper()}{path}{query}{body}"
            digest = hmac.new(
                str(self.config.get("secretKey", "")).encode("utf-8"),
                prehash.encode("utf-8"),
                digestmod=hashlib.sha256,
            ).digest()
            signature = base64.b64encode(digest).decode("utf-8")
        response = self._session.request(
            method.upper(),
            f"{self._base_url}{path}{query}",
            headers=self._headers(private, timestamp, signature),
            data=body or None,
            timeout=self._rest_timeout,
        )
        try:
            decoded = response.json()
        except ValueError:
            response.raise_for_status()
            raise
        if response.status_code >= 400:
            raise RuntimeError(decoded.get("msg") or f"HTTP {response.status_code}")
        code = str(decoded.get("code", ""))
        if code not in {"0", "00000", ""}:
            raise RuntimeError(decoded.get("msg") or code)
        return decoded.get("data")

    def _public_request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._request("GET", path, params=params, private=False)

    def _private_get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._request("GET", path, params=params, private=True)

    def _private_post(self, path: str, payload: dict[str, Any] | None = None) -> Any:
        return self._request("POST", path, payload=payload, private=True)

    def _ensure_contract_config(self, symbol: str | None = None) -> dict[str, Any]:
        target_symbol = symbol or self.symbol
        cached = self._market_cache.get(target_symbol)
        if cached:
            return cached
        inst_id = symbol_to_inst_id(target_symbol)
        rows = self._retry(
            "加载合约配置",
            self._public_request,
            "/api/v2/mix/market/contracts",
            {"productType": self.inst_type, "symbol": inst_id},
        )
        if not rows:
            raise RuntimeError(f"Bitget contract config not found: {target_symbol}")
        info = rows[0]
        price_step = safe_float(info.get("priceEndStep", 1)) / (10 ** int(info.get("pricePlace", 0)))
        amount_step = safe_float(info.get("sizeMultiplier", 0)) or (1 / (10 ** int(info.get("volumePlace", 0))))
        market = {
            "id": info.get("symbol", inst_id),
            "symbol": target_symbol,
            "base": info.get("baseCoin"),
            "quote": info.get("quoteCoin"),
            "settle": self.margin_coin,
            "settleId": self.margin_coin,
            "type": "swap",
            "swap": True,
            "future": False,
            "spot": False,
            "contract": True,
            "precision": {
                "price": price_step,
                "amount": amount_step,
            },
            "limits": {
                "amount": {"min": safe_float(info.get("minTradeNum", 0))},
                "cost": {"min": safe_float(info.get("minTradeUSDT", 0))},
            },
            "info": info,
        }
        self._market_cache[target_symbol] = market
        return market

    def _retry(self, label: str, fn, *args, **kwargs):
        delay = self.retry_delay
        last_error = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                last_error = exc
                is_retryable = isinstance(exc, getattr(ccxt, "NetworkError", Exception)) or any(
                    token in str(exc).lower()
                    for token in ("timeout", "timed out", "connection", "reset", "temporarily", "aborted")
                )
                if not is_retryable or attempt >= self.retry_attempts:
                    break
                print(f"⚠️ {label} 失败，正在重试 {attempt}/{self.retry_attempts}: {exc}")
                time.sleep(delay)
                delay *= self.retry_backoff
        raise last_error  # type: ignore[misc]

    def load_markets(self, reload: bool = False, params: dict[str, Any] | None = None):
        if reload:
            self._market_cache.pop(self.symbol, None)
        market = self._ensure_contract_config(self.symbol)
        self.rest.markets = {self.symbol: market}
        return self.rest.markets

    def market(self, symbol: str):
        return self._ensure_contract_config(symbol)

    def amount_to_precision(self, symbol: str, amount: float) -> str:
        market = self.market(symbol)
        step = safe_float(market["precision"].get("amount", 0))
        return quantize_to_step(amount, step)

    def price_to_precision(self, symbol: str, price: float) -> str:
        market = self.market(symbol)
        step = safe_float(market["precision"].get("price", 0))
        return quantize_to_step(price, step)

    def set_leverage(self, leverage: int, symbol: str, params: dict[str, Any] | None = None):
        market = self.market(symbol)
        payload = {
            "symbol": market["id"],
            "marginCoin": market["settleId"],
            "leverage": str(leverage),
            "productType": self.inst_type,
        }
        if params:
            payload.update(params)
        return self._retry("设置杠杆", self._private_post, "/api/v2/mix/account/set-leverage", payload)

    def fetch_balance(self, params: dict[str, Any] | None = None):
        if self.ws.is_fresh("private"):
            account = self.ws.get_account("USDT")
            if account:
                available = safe_float(account.get("available", 0))
                frozen = safe_float(account.get("frozen", 0))
                equity = safe_float(account.get("equity", 0))
                return {
                    "USDT": {
                        "free": available,
                        "used": frozen,
                        "total": equity,
                    },
                    "free": {"USDT": available},
                    "used": {"USDT": frozen},
                    "total": {"USDT": equity},
                    "info": [account],
                }
        rows = self._retry(
            "获取余额",
            self._private_get,
            "/api/v2/mix/account/accounts",
            {"productType": self.inst_type},
        )
        selected = None
        for row in rows or []:
            if str(row.get("marginCoin", "")).upper() == self.margin_coin:
                selected = row
                break
        if selected is None and rows:
            selected = rows[0]
        selected = selected or {}
        available = safe_float(selected.get("available", 0))
        used = safe_float(selected.get("locked", 0))
        total = safe_float(selected.get("accountEquity", 0) or selected.get("equity", 0))
        coin = str(selected.get("marginCoin", self.margin_coin)).upper()
        return {
            coin: {
                "free": available,
                "used": used,
                "total": total,
            },
            "free": {coin: available},
            "used": {coin: used},
            "total": {coin: total},
            "info": rows or [],
        }

    def fetch_positions(self, symbols: list[str] | None = None, params: dict[str, Any] | None = None):
        if self.ws.is_fresh("private"):
            position = self.ws.get_position(self.inst_id)
            if position:
                contracts = safe_float(position.get("total", 0))
                if contracts > 0:
                    side = position.get("holdSide")
                    entry_price = safe_float(position.get("openPriceAvg", 0))
                    mark_price = safe_float(position.get("markPrice", 0))
                    unrealized_pnl = safe_float(position.get("unrealizedPL", 0))
                    roi = position_percentage_from_row(position, side, entry_price, mark_price, unrealized_pnl)
                    return [
                        {
                            "symbol": self.symbol,
                            "contracts": contracts,
                            "side": side,
                            "entryPrice": entry_price,
                            "markPrice": mark_price,
                            "unrealizedPnl": unrealized_pnl,
                            "percentage": roi,
                            "liquidationPrice": safe_float(position.get("liquidationPrice", 0)),
                            "marginMode": position.get("marginMode"),
                            "marginSize": safe_float(position.get("marginSize", 0)),
                            "leverage": safe_float(position.get("leverage", 0)),
                            "info": position,
                        }
                    ]
        query = {
            "productType": self.inst_type,
            "marginCoin": self.margin_coin,
        }
        target_symbol = symbols[0] if symbols else self.symbol
        if target_symbol:
            query["symbol"] = symbol_to_inst_id(target_symbol)
        rows = self._retry("获取持仓", self._private_get, "/api/v2/mix/position/all-position", query) or []
        positions = []
        for row in rows:
            contracts = safe_float(row.get("total", 0))
            if contracts <= 0:
                continue
            side = row.get("holdSide")
            entry_price = safe_float(row.get("openPriceAvg", 0))
            mark_price = safe_float(row.get("markPrice", 0))
            unrealized_pnl = safe_float(row.get("unrealizedPL", 0))
            positions.append(
                {
                    "symbol": self.symbol,
                    "contracts": contracts,
                    "side": side,
                    "entryPrice": entry_price,
                    "markPrice": mark_price,
                    "unrealizedPnl": unrealized_pnl,
                    "percentage": position_percentage_from_row(row, side, entry_price, mark_price, unrealized_pnl),
                    "liquidationPrice": safe_float(row.get("liquidationPrice", 0)),
                    "marginMode": row.get("marginMode"),
                    "marginSize": safe_float(row.get("marginSize", 0)),
                    "leverage": safe_float(row.get("leverage", 0)),
                    "info": row,
                }
            )
        return positions

    def _fetch_standard_open_orders(self, symbol: str | None = None, since: int | None = None, limit: int | None = None, params: dict[str, Any] | None = None):
        if self.ws.is_fresh("private"):
            rows = []
            for order in self.ws.get_orders(self.inst_id):
                status = str(order.get("status", "")).lower()
                if status and status not in OPEN_ORDER_STATUSES:
                    continue
                amount = safe_float(order.get("size", 0))
                filled = safe_float(order.get("accBaseVolume", 0))
                rows.append(
                    {
                        "id": order.get("orderId"),
                        "symbol": self.symbol,
                        "type": order.get("orderType"),
                        "side": order.get("side"),
                        "price": safe_float(order.get("price", 0)),
                        "amount": amount,
                        "remaining": max(amount - filled, 0.0),
                        "filled": filled,
                        "status": status,
                        "reduceOnly": str(order.get("reduceOnly", "no")).lower() == "yes",
                        "timestamp": int(safe_float(order.get("cTime", 0))),
                        "info": order,
                    }
                )
            if rows:
                rows.sort(key=lambda item: item.get("price", 0))
                return rows
        query = {
            "productType": self.inst_type,
            "symbol": symbol_to_inst_id(symbol or self.symbol),
        }
        payload = self._retry("获取挂单", self._private_get, "/api/v2/mix/order/orders-pending", query)
        if not payload:
            rows = []
        elif isinstance(payload, dict):
            rows = payload.get("entrustedList") or []
        else:
            rows = payload or []
        orders = []
        for order in rows:
            amount = safe_float(order.get("size", 0))
            filled = safe_float(order.get("baseVolume", 0) or order.get("filledQty", 0))
            orders.append(
                {
                    "id": order.get("orderId"),
                    "symbol": self.symbol,
                    "type": order.get("orderType"),
                    "side": order.get("side"),
                    "price": safe_float(order.get("price", 0)),
                    "amount": amount,
                    "remaining": max(amount - filled, 0.0),
                    "filled": filled,
                    "status": str(order.get("status", "")).lower(),
                    "reduceOnly": str(order.get("reduceOnly", "NO")).lower() == "yes",
                    "timestamp": int(safe_float(order.get("cTime", 0))),
                    "info": order,
                }
            )
        orders.sort(key=lambda item: item.get("price", 0))
        return orders

    def fetch_pending_trigger_orders(
        self,
        symbol: str | None = None,
        plan_type: str = "normal_plan",
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        query = {
            "productType": self.inst_type,
            "planType": plan_type,
            "symbol": symbol_to_inst_id(symbol or self.symbol),
            "limit": str(limit or 100),
        }
        extra = dict(params or {})
        query.update({key: value for key, value in extra.items() if value not in (None, "")})
        payload = self._retry("获取触发委托", self._private_get, "/api/v2/mix/order/orders-plan-pending", query)
        if not payload:
            rows = []
        elif isinstance(payload, dict):
            rows = payload.get("entrustedList") or []
        else:
            rows = payload or []

        orders = []
        for order in rows:
            amount = safe_float(order.get("size", 0))
            execute_price = safe_float(order.get("executePrice", order.get("price", 0)))
            status = str(order.get("planStatus", order.get("status", "live"))).lower()
            orders.append(
                {
                    "id": order.get("orderId"),
                    "clientOrderId": order.get("clientOid"),
                    "symbol": self.symbol,
                    "type": "trigger",
                    "planType": order.get("planType", plan_type),
                    "side": str(order.get("side", "")).lower(),
                    "price": execute_price,
                    "triggerPrice": safe_float(order.get("triggerPrice", 0)),
                    "triggerType": str(order.get("triggerType", "")).lower(),
                    "amount": amount,
                    "remaining": amount,
                    "filled": 0.0,
                    "status": status,
                    "reduceOnly": str(order.get("reduceOnly", "no")).lower() == "yes"
                    or str(order.get("tradeSide", "")).lower() in CLOSE_TRADE_SIDE_TOKENS,
                    "timestamp": int(safe_float(order.get("cTime", 0))),
                    "info": order,
                }
            )
        orders.sort(key=lambda item: item.get("price", 0))
        return orders

    def fetch_open_orders(self, symbol: str | None = None, since: int | None = None, limit: int | None = None, params: dict[str, Any] | None = None):
        standard_orders = self._fetch_standard_open_orders(symbol=symbol, since=since, limit=limit, params=params)
        trigger_orders = self.fetch_pending_trigger_orders(symbol=symbol, plan_type="normal_plan", limit=limit, params=params)
        orders = standard_orders + trigger_orders
        orders.sort(key=lambda item: item.get("price", 0))
        return orders

    def _cancel_all_orders_legacy(self, symbol: str | None = None, params: dict[str, Any] | None = None):
        open_orders = self.fetch_open_orders(symbol or self.symbol)
        if not open_orders:
            return []
        target_symbol = symbol or self.symbol
        payload = {
            "symbol": symbol_to_inst_id(target_symbol),
            "productType": self.inst_type,
            "marginCoin": self.margin_coin,
            "orderIdList": [
                {"orderId": order["id"]}
                for order in open_orders
                if order.get("id")
            ],
        }
        try:
            data = self._retry("取消挂单", self._private_post, "/api/v2/mix/order/batch-cancel-orders", payload) or {}
            success_list = data.get("successList", [])
            failure_list = data.get("failureList", [])
            if failure_list:
                for failed in failure_list:
                    print(f"⚠️ 撤单失败: {failed.get('orderId') or failed.get('clientOid')} - {failed.get('errorMsg')}")
            return success_list
        except Exception:
            # Fallback to single-order cancellation when batch cancel is rejected.
            success_list = []
            for order in open_orders:
                cancel_payload = {
                    "symbol": symbol_to_inst_id(target_symbol),
                    "productType": self.inst_type,
                    "marginCoin": self.margin_coin,
                    "orderId": order.get("id"),
                }
                result = self._retry("取消挂单", self._private_post, "/api/v2/mix/order/cancel-order", cancel_payload) or {}
                success_list.append(result)
            return success_list

    def create_order(
        self,
        symbol: str,
        type: str,
        side: str,
        amount: float,
        price: float | None = None,
        params: dict[str, Any] | None = None,
    ):
        order_params = dict(params or {})
        market = self.market(symbol)
        order_type = str(type).lower()
        request = {
            "symbol": market["id"],
            "productType": self.inst_type,
            "marginCoin": market["settleId"],
            "marginMode": normalize_margin_mode(order_params.pop("marginMode", self.margin_mode)),
            "side": side,
            "orderType": order_type,
            "size": self.amount_to_precision(symbol, amount),
            "force": order_params.pop("force", order_params.pop("timeInForce", "ioc" if order_type == "market" else "gtc")).upper(),
        }
        if price is not None and order_type == "limit":
            request["price"] = self.price_to_precision(symbol, price)
        if order_params.pop("reduceOnly", False):
            request["reduceOnly"] = "YES"
        client_oid = order_params.pop("clientOid", order_params.pop("clientOrderId", None))
        if client_oid:
            request["clientOid"] = client_oid
        request.update(order_params)
        data = self._retry("下单", self._private_post, "/api/v2/mix/order/place-order", request) or {}
        return {
            "id": data.get("orderId"),
            "clientOrderId": data.get("clientOid"),
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "amount": amount,
            "price": price,
            "info": data,
        }

    def cancel_all_orders(self, symbol: str | None = None, params: dict[str, Any] | None = None):
        open_orders = self.fetch_open_orders(symbol or self.symbol)
        if not open_orders:
            return []
        target_symbol = symbol or self.symbol
        normal_orders = [order for order in open_orders if order.get("type") != "trigger"]
        trigger_orders = [order for order in open_orders if order.get("type") == "trigger" or order.get("planType") == "normal_plan"]
        success_list: list[dict[str, Any]] = []

        if normal_orders:
            payload = {
                "symbol": symbol_to_inst_id(target_symbol),
                "productType": self.inst_type,
                "marginCoin": self.margin_coin,
                "orderIdList": [
                    {"orderId": order["id"]}
                    for order in normal_orders
                    if order.get("id")
                ],
            }
            try:
                data = self._retry("取消挂单", self._private_post, "/api/v2/mix/order/batch-cancel-orders", payload) or {}
                success_list.extend(data.get("successList", []))
                failure_list = data.get("failureList", [])
                if failure_list:
                    for failed in failure_list:
                        print(f"⚠️ 撤单失败: {failed.get('orderId') or failed.get('clientOid')} - {failed.get('errorMsg')}")
            except Exception:
                for order in normal_orders:
                    cancel_payload = {
                        "symbol": symbol_to_inst_id(target_symbol),
                        "productType": self.inst_type,
                        "marginCoin": self.margin_coin,
                        "orderId": order.get("id"),
                    }
                    result = self._retry("取消挂单", self._private_post, "/api/v2/mix/order/cancel-order", cancel_payload) or {}
                    success_list.append(result)

        if trigger_orders:
            trigger_payload = {
                "symbol": symbol_to_inst_id(target_symbol),
                "productType": self.inst_type,
                "marginCoin": self.margin_coin,
                "planType": "normal_plan",
                "orderIdList": [
                    {
                        "orderId": order.get("id") or "",
                        "clientOid": order.get("clientOrderId") or "",
                    }
                    for order in trigger_orders
                    if order.get("id") or order.get("clientOrderId")
                ],
            }
            if trigger_payload["orderIdList"]:
                data = self._retry("取消触发单", self._private_post, "/api/v2/mix/order/cancel-plan-order", trigger_payload) or {}
                success_list.extend(data.get("successList", []))
                failure_list = data.get("failureList", [])
                if failure_list:
                    for failed in failure_list:
                        print(f"⚠️ 取消触发单失败: {failed.get('orderId') or failed.get('clientOid')} - {failed.get('errorMsg')}")

        return success_list

    def cancel_orders(self, orders: list[dict[str, Any]], symbol: str | None = None):
        if not orders:
            return []
        target_symbol = symbol or self.symbol
        normal_orders = [order for order in orders if order.get("type") != "trigger"]
        trigger_orders = [order for order in orders if order.get("type") == "trigger" or order.get("planType") == "normal_plan"]
        success_list: list[dict[str, Any]] = []

        if normal_orders:
            payload = {
                "symbol": symbol_to_inst_id(target_symbol),
                "productType": self.inst_type,
                "marginCoin": self.margin_coin,
                "orderIdList": [
                    {"orderId": order["id"]}
                    for order in normal_orders
                    if order.get("id")
                ],
            }
            if payload["orderIdList"]:
                try:
                    data = self._retry("取消挂单", self._private_post, "/api/v2/mix/order/batch-cancel-orders", payload) or {}
                    success_list.extend(data.get("successList", []))
                    failure_list = data.get("failureList", [])
                    if failure_list:
                        for failed in failure_list:
                            print(f"⚠️ 撤单失败: {failed.get('orderId') or failed.get('clientOid')} - {failed.get('errorMsg')}")
                except Exception:
                    for order in normal_orders:
                        cancel_payload = {
                            "symbol": symbol_to_inst_id(target_symbol),
                            "productType": self.inst_type,
                            "marginCoin": self.margin_coin,
                            "orderId": order.get("id"),
                        }
                        result = self._retry("取消挂单", self._private_post, "/api/v2/mix/order/cancel-order", cancel_payload) or {}
                        success_list.append(result)

        if trigger_orders:
            trigger_payload = {
                "symbol": symbol_to_inst_id(target_symbol),
                "productType": self.inst_type,
                "marginCoin": self.margin_coin,
                "planType": "normal_plan",
                "orderIdList": [
                    {
                        "orderId": order.get("id") or "",
                        "clientOid": order.get("clientOrderId") or "",
                    }
                    for order in trigger_orders
                    if order.get("id") or order.get("clientOrderId")
                ],
            }
            if trigger_payload["orderIdList"]:
                data = self._retry("取消触发单", self._private_post, "/api/v2/mix/order/cancel-plan-order", trigger_payload) or {}
                success_list.extend(data.get("successList", []))
                failure_list = data.get("failureList", [])
                if failure_list:
                    for failed in failure_list:
                        print(f"⚠️ 取消触发单失败: {failed.get('orderId') or failed.get('clientOid')} - {failed.get('errorMsg')}")

        return success_list

    def create_trigger_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        trigger_price: float,
        price: float | None = None,
        trigger_type: str = "mark_price",
        order_type: str = "limit",
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        order_params = dict(params or {})
        market = self.market(symbol)
        normalized_trigger_price = safe_float(trigger_price, 0.0)
        if normalized_trigger_price <= 0:
            raise ValueError("trigger_price must be positive")

        normalized_order_type = str(order_type or "limit").lower()
        request = {
            "planType": order_params.pop("planType", "normal_plan"),
            "symbol": market["id"],
            "productType": self.inst_type,
            "marginMode": normalize_margin_mode(order_params.pop("marginMode", self.margin_mode)),
            "marginCoin": market["settleId"],
            "size": self.amount_to_precision(symbol, amount),
            "triggerPrice": self.price_to_precision(symbol, normalized_trigger_price),
            "triggerType": trigger_type,
            "side": side,
            "orderType": normalized_order_type,
            "reduceOnly": "YES" if order_params.pop("reduceOnly", False) else "NO",
        }
        if normalized_order_type == "limit":
            execute_price = safe_float(price, 0.0)
            if execute_price <= 0:
                raise ValueError("price is required for limit trigger orders")
            request["price"] = self.price_to_precision(symbol, execute_price)
        client_oid = order_params.pop("clientOid", order_params.pop("clientOrderId", None))
        if client_oid:
            request["clientOid"] = client_oid
        trade_side = order_params.pop("tradeSide", None)
        if trade_side:
            request["tradeSide"] = trade_side
        request.update(order_params)
        data = self._retry("下触发单", self._private_post, "/api/v2/mix/order/place-plan-order", request) or {}
        return {
            "id": data.get("orderId"),
            "clientOrderId": data.get("clientOid") or client_oid,
            "symbol": symbol,
            "type": "trigger",
            "planType": request["planType"],
            "side": side,
            "price": price,
            "triggerPrice": normalized_trigger_price,
            "triggerType": trigger_type,
            "amount": amount,
            "info": data,
        }

    def place_position_stop_loss(
        self,
        symbol: str,
        hold_side: str,
        trigger_price: float,
        trigger_type: str = "mark_price",
        execute_price: float | None = 0.0,
        client_oid: str | None = None,
    ) -> dict[str, Any]:
        market = self.market(symbol)
        normalized_trigger_price = safe_float(trigger_price, 0)
        execute_value = safe_float(execute_price, 0)
        if execute_price is None or execute_value <= 0:
            execute_value = normalized_trigger_price
        request = {
            "symbol": market["id"],
            "productType": self.inst_type,
            "marginCoin": market["settleId"],
            "holdSide": hold_side,
            "stopLossTriggerPrice": self.price_to_precision(symbol, normalized_trigger_price),
            "stopLossTriggerType": trigger_type,
            "stopLossExecutePrice": self.price_to_precision(symbol, execute_value),
        }
        if client_oid:
            request["stopLossClientOid"] = client_oid

        data = self._retry("下仓位止损单", self._private_post, "/api/v2/mix/order/place-pos-tpsl", request)
        if isinstance(data, list):
            row = data[0] if data else {}
        else:
            row = data or {}
        return {
            "id": row.get("stopLossOrderId") or row.get("orderId"),
            "clientOrderId": row.get("stopLossClientOid") or row.get("clientOid") or client_oid,
            "symbol": symbol,
            "type": "pos_loss",
            "side": hold_side,
            "triggerPrice": trigger_price,
            "executePrice": execute_value,
            "info": row,
        }

    def modify_tpsl_order(
        self,
        symbol: str,
        trigger_price: float,
        trigger_type: str = "mark_price",
        execute_price: float | None = 0.0,
        order_id: str | None = None,
        client_oid: str | None = None,
        size: Any = None,
    ) -> dict[str, Any]:
        if not order_id and not client_oid:
            raise ValueError("order_id or client_oid is required")

        market = self.market(symbol)
        normalized_trigger_price = safe_float(trigger_price, 0)
        execute_value = safe_float(execute_price, 0)
        if execute_price is None or execute_value <= 0:
            execute_value = normalized_trigger_price
        request = {
            "symbol": market["id"],
            "productType": self.inst_type,
            "marginCoin": market["settleId"],
            "triggerPrice": self.price_to_precision(symbol, normalized_trigger_price),
            "triggerType": trigger_type,
            "executePrice": self.price_to_precision(symbol, execute_value),
        }
        if order_id:
            request["orderId"] = order_id
        if client_oid:
            request["clientOid"] = client_oid
        if size is not None:
            if size == "":
                request["size"] = ""
            else:
                request["size"] = self.amount_to_precision(symbol, size)

        data = self._retry("修改止损单", self._private_post, "/api/v2/mix/order/modify-tpsl-order", request)
        if isinstance(data, list):
            row = data[0] if data else {}
        else:
            row = data or {}
        return {
            "id": row.get("orderId") or order_id,
            "clientOrderId": row.get("clientOid") or client_oid,
            "symbol": symbol,
            "type": "tpsl",
            "triggerPrice": trigger_price,
            "executePrice": execute_value,
            "info": row,
        }

    def cancel_position_stop_loss(
        self,
        symbol: str | None = None,
        order_id: str | None = None,
        client_oid: str | None = None,
    ) -> dict[str, Any]:
        target_symbol = symbol or self.symbol
        payload = {
            "symbol": symbol_to_inst_id(target_symbol),
            "productType": self.inst_type,
            "marginCoin": self.margin_coin,
            "planType": "pos_loss",
        }
        if order_id or client_oid:
            payload["orderIdList"] = [{
                "orderId": order_id or "",
                "clientOid": client_oid or "",
            }]
        return self._retry("取消仓位止损单", self._private_post, "/api/v2/mix/order/cancel-plan-order", payload) or {}

    def fetch_ticker(self, symbol: str, params: dict[str, Any] | None = None):
        ticker = self._wait_for_ws_ticker(timeout=3.0)
        if ticker and safe_float(ticker.get("lastPr", 0)) > 0:
            return {
                "symbol": self.symbol,
                "last": safe_float(ticker.get("lastPr", 0)),
                "bid": safe_float(ticker.get("bidPr", 0)),
                "ask": safe_float(ticker.get("askPr", 0)),
                "high": safe_float(ticker.get("high24h", 0)),
                "low": safe_float(ticker.get("low24h", 0)),
                "percentage": safe_float(ticker.get("change24h", 0)) * 100,
                "baseVolume": safe_float(ticker.get("baseVolume", 0)),
                "quoteVolume": safe_float(ticker.get("quoteVolume", 0)),
                "info": ticker,
            }
        rows = self._retry(
            "获取行情",
            self._public_request,
            "/api/v2/mix/market/ticker",
            {"productType": self.inst_type, "symbol": symbol_to_inst_id(symbol)},
        ) or []
        row = rows[0] if rows else {}
        return {
            "symbol": symbol,
            "last": safe_float(row.get("lastPr", 0)),
            "bid": safe_float(row.get("bidPr", 0)),
            "ask": safe_float(row.get("askPr", 0)),
            "high": safe_float(row.get("high24h", 0)),
            "low": safe_float(row.get("low24h", 0)),
            "percentage": safe_float(row.get("change24h", 0)) * 100,
            "baseVolume": safe_float(row.get("baseVolume", 0)),
            "quoteVolume": safe_float(row.get("quoteVolume", 0)),
            "info": row,
        }

    def _wait_for_ws_ticker(self, timeout: float = 3.0) -> dict[str, Any] | None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.ws.is_fresh("public"):
                ticker = self.ws.get_ticker(self.inst_id)
                if ticker:
                    return ticker
            time.sleep(0.2)
        if self.ws.is_fresh("public"):
            ticker = self.ws.get_ticker(self.inst_id)
            if ticker:
                return ticker
        return None

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ):
        normalized = normalize_timeframe(timeframe, "1m")
        desired = limit or 100
        merged = self._merge_ohlcv_with_ws(symbol, normalized, desired, since, params or {})
        if merged:
            return merged[-desired:]
        return self._fetch_ohlcv_rest(symbol, normalized, desired)

    def _merge_ohlcv_with_ws(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        since: int | None,
        params: dict[str, Any],
    ) -> list[list[float]]:
        if not self.ws.is_fresh("public"):
            return []
        ws_rows = self.ws.get_candles(timeframe)
        if not ws_rows:
            return []
        seed = self._seed_ohlcv(symbol, timeframe, max(limit, 200), since, params)
        merged: dict[int, list[float]] = {int(row[0]): list(row[:6]) for row in seed}
        for row in ws_rows:
            merged[int(row[0])] = list(row[:6])
        rows = sorted(merged.values(), key=lambda item: item[0])
        return rows

    def _fetch_ohlcv_rest(self, symbol: str, timeframe: str, limit: int) -> list[list[float]]:
        granularity = timeframe_to_rest_granularity(timeframe)
        rows = self._retry(
            "获取K线",
            self._public_request,
            "/api/v2/mix/market/candles",
            {
                "productType": self.inst_type,
                "symbol": symbol_to_inst_id(symbol),
                "granularity": granularity,
                "limit": limit,
            },
        ) or []
        candles = []
        for row in rows:
            if len(row) < 6:
                continue
            candles.append(
                [
                    int(float(row[0])),
                    safe_float(row[1]),
                    safe_float(row[2]),
                    safe_float(row[3]),
                    safe_float(row[4]),
                    safe_float(row[5]),
                ]
            )
        candles.sort(key=lambda item: item[0])
        return candles

    def _seed_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        since: int | None,
        params: dict[str, Any],
    ) -> list[list[float]]:
        cache_key = normalize_timeframe(timeframe)
        ttl = max(30.0, min(self._timeframe_seconds(cache_key), 300.0))
        now = time.time()
        cached = self._ohlcv_seed.get(cache_key)
        cached_at = self._ohlcv_seed_at.get(cache_key, 0.0)
        if cached and (now - cached_at) < ttl and len(cached) >= limit:
            return cached
        rows = self._fetch_ohlcv_rest(symbol, cache_key, limit)
        self._ohlcv_seed[cache_key] = rows
        self._ohlcv_seed_at[cache_key] = now
        return rows

    def _timeframe_seconds(self, timeframe: str) -> float:
        value = normalize_timeframe(timeframe)
        if value.endswith("m"):
            return float(value[:-1]) * 60
        if value.endswith("h"):
            return float(value[:-1]) * 3600
        if value.endswith("d"):
            return float(value[:-1]) * 86400
        if value.endswith("w"):
            return float(value[:-1]) * 604800
        return 60.0

    def fetch_my_trades(self, symbol: str | None = None, since: int | None = None, limit: int | None = None, params: dict[str, Any] | None = None):
        limit = limit or 60
        ws_rows = self._map_ws_fills(limit)
        try:
            query = {
                "productType": self.inst_type,
                "symbol": symbol_to_inst_id(symbol or self.symbol),
                "limit": limit,
            }
            if since is not None:
                query["startTime"] = since
            rest_payload = self._retry("获取成交", self._private_get, "/api/v2/mix/order/fills", query) or {}
            if isinstance(rest_payload, dict):
                rest_data = rest_payload.get("fillList", [])
            else:
                rest_data = rest_payload or []
            rest_rows = []
            for trade in rest_data:
                fee_details = trade.get("feeDetail") or {}
                if isinstance(fee_details, list):
                    fee_details = fee_details[0] if fee_details else {}
                elif not isinstance(fee_details, dict):
                    fee_details = {}
                price = safe_float(trade.get("priceAvg", 0) or trade.get("price", 0))
                amount = safe_float(trade.get("size", 0) or trade.get("baseVolume", 0))
                cost = safe_float(trade.get("amount", 0)) or amount * price
                trade_side = str(trade.get("tradeSide", "")).lower()
                rest_rows.append(
                    {
                        "id": trade.get("tradeId"),
                        "order": trade.get("orderId"),
                        "timestamp": int(safe_float(trade.get("cTime", 0))),
                        "datetime": None,
                        "symbol": symbol or self.symbol,
                        "side": trade.get("side"),
                        "takerOrMaker": trade.get("tradeScope"),
                        "price": price,
                        "amount": amount,
                        "cost": cost,
                        "fee": {
                            "cost": abs(safe_float(fee_details.get("totalFee", 0))),
                            "currency": fee_details.get("feeCoin"),
                        },
                        "info": {
                            **trade,
                            "reduceOnly": any(token in trade_side for token in CLOSE_TRADE_SIDE_TOKENS),
                        },
                    }
                )
        except Exception:
            if ws_rows:
                return ws_rows[-limit:]
            raise
        return self._merge_trades(rest_rows, ws_rows, limit)

    def _map_ws_fills(self, limit: int) -> list[dict[str, Any]]:
        rows = []
        for fill in self.ws.get_fills(self.inst_id, limit=limit * 2):
            fee_details = fill.get("feeDetail") or []
            fee_cost = 0.0
            fee_coin = None
            if fee_details:
                first_fee = fee_details[0]
                fee_cost = abs(safe_float(first_fee.get("totalFee", 0)))
                fee_coin = first_fee.get("feeCoin")
            trade_side = str(fill.get("tradeSide", "")).lower()
            reduce_only = any(token in trade_side for token in CLOSE_TRADE_SIDE_TOKENS)
            timestamp = int(safe_float(fill.get("uTime") or fill.get("cTime"), 0))
            price = safe_float(fill.get("price", 0))
            amount = safe_float(fill.get("baseVolume", 0))
            cost = safe_float(fill.get("quoteVolume", 0)) or amount * price
            rows.append(
                {
                    "id": fill.get("tradeId"),
                    "order": fill.get("orderId"),
                    "timestamp": timestamp,
                    "datetime": None,
                    "symbol": self.symbol,
                    "side": fill.get("side"),
                    "takerOrMaker": fill.get("tradeScope"),
                    "price": price,
                    "amount": amount,
                    "cost": cost,
                    "fee": {
                        "cost": fee_cost,
                        "currency": fee_coin,
                    },
                    "info": {
                        **fill,
                        "reduceOnly": reduce_only,
                    },
                }
            )
        rows.sort(key=lambda item: item.get("timestamp", 0))
        return rows[-limit:]

    def _merge_trades(self, rest_rows: list[dict[str, Any]], ws_rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for trade in rest_rows:
            trade_id = str(trade.get("id") or trade.get("order") or trade.get("timestamp"))
            merged[trade_id] = trade
        for trade in ws_rows:
            trade_id = str(trade.get("id") or trade.get("order") or trade.get("timestamp"))
            merged[trade_id] = trade
        rows = list(merged.values())
        rows.sort(key=lambda item: safe_float(item.get("timestamp", 0)))
        return rows[-limit:]

    def fetch_ledger(self, code: str | None = None, since: int | None = None, limit: int | None = None, params: dict[str, Any] | None = None):
        query = {
            "productType": self.inst_type,
            "limit": limit or 80,
        }
        if since is not None:
            query["startTime"] = since
        try:
            rows = self._retry("获取资金流水", self._private_get, "/api/v2/mix/account/bill", query) or []
        except Exception as exc:
            if "url" in str(exc).lower() and "not found" in str(exc).lower():
                return []
            if "请求的url不存在" in str(exc).lower():
                return []
            raise
        ledger = []
        for row in rows:
            ledger.append(
                {
                    "id": row.get("billId"),
                    "timestamp": int(safe_float(row.get("cTime", 0))),
                    "datetime": None,
                    "currency": row.get("coin"),
                    "amount": safe_float(row.get("size", 0)),
                    "before": None,
                    "after": safe_float(row.get("balance", 0)),
                    "type": row.get("businessType") or row.get("groupType"),
                    "fee": {
                        "cost": safe_float(row.get("fees", 0)),
                        "currency": row.get("coin"),
                    },
                    "info": row,
                }
            )
        return ledger
