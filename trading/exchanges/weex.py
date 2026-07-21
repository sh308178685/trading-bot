"""WEEX V3 USDT-margined futures adapter.

The strategy uses a compact CCXT-like shape, while all authenticated trading
requests below target WEEX's official V3 contract API.  Order-creating POSTs
are deliberately never retried after an ambiguous transport failure; callers
reconcile them by the persisted client order identifier.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import hmac
import json
import math
import random
import re
import threading
import time
from collections import deque
from copy import deepcopy
from decimal import Decimal, ROUND_DOWN
from typing import Any
from urllib.parse import urlencode

import requests
import websockets

from .base import ExchangeAdapter


WEEX_DEFAULT_REST_URL = "https://api-contract.weex.com"
WEEX_DEFAULT_WS_PUBLIC_URL = "wss://ws-contract.weex.com/v3/ws/public"
WEEX_DEFAULT_WS_PRIVATE_URL = "wss://ws-contract.weex.com/v3/ws/private"
WEEX_CLIENT_ID_PATTERN = re.compile(r"^[.A-Z:/a-z0-9_-]{1,36}$")
WEEX_SUCCESS_CODES = {"0", "00000", "200", "SUCCESS"}
OPEN_ORDER_STATUSES = {"new", "pending", "partially_filled", "untriggered", "canceling"}
WS_CLOSED_ORDER_STATUSES = {"filled", "canceled", "cancelled", "rejected", "expired"}
TERMINAL_TRIGGER_FILLED = {"filled", "triggered", "executed"}
WEEX_WS_TIMEFRAMES = {
    "1m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "1w",
    "1M",
}


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_ohlcv_row(
    timestamp: Any,
    open_price: Any,
    high_price: Any,
    low_price: Any,
    close_price: Any,
    volume: Any,
) -> list[float] | None:
    """Return a validated OHLCV row or reject it without synthesizing zero prices."""
    parsed_timestamp = int(safe_float(timestamp, 0.0))
    parsed_open = safe_float(open_price, 0.0)
    parsed_high = safe_float(high_price, 0.0)
    parsed_low = safe_float(low_price, 0.0)
    parsed_close = safe_float(close_price, 0.0)
    parsed_volume = safe_float(volume, -1.0)
    prices = (parsed_open, parsed_high, parsed_low, parsed_close)
    if (
        parsed_timestamp <= 0
        or not all(math.isfinite(value) and value > 0 for value in prices)
        or not math.isfinite(parsed_volume)
        or parsed_volume < 0
        or parsed_high < max(parsed_open, parsed_low, parsed_close)
        or parsed_low > min(parsed_open, parsed_high, parsed_close)
    ):
        return None
    return [
        parsed_timestamp,
        parsed_open,
        parsed_high,
        parsed_low,
        parsed_close,
        parsed_volume,
    ]


def symbol_to_weex_id(symbol: str) -> str:
    """Convert ``BTC/USDT:USDT`` (or a similar unified symbol) to BTCUSDT."""
    value = str(symbol or "").strip()
    pair = value.split(":", 1)[0]
    return pair.replace("/", "").replace("-", "").replace("_", "").upper()


def precision_step(decimals: Any) -> float:
    try:
        places = max(int(decimals), 0)
    except (TypeError, ValueError):
        places = 0
    return float(Decimal(1).scaleb(-places))


def quantize_to_step(value: float, step: float) -> str:
    step_decimal = Decimal(str(step))
    value_decimal = Decimal(str(value))
    if step_decimal <= 0:
        return format(value_decimal, "f")
    units = (value_decimal / step_decimal).to_integral_value(rounding=ROUND_DOWN)
    quantized = units * step_decimal
    decimals = max(-step_decimal.as_tuple().exponent, 0)
    return f"{quantized:.{decimals}f}"


def normalize_margin_mode(value: Any) -> str:
    mode = str(value or "crossed").strip().lower()
    return "ISOLATED" if mode in {"isolated", "fixed"} else "CROSSED"


def normalize_trigger_type(value: Any) -> str:
    token = str(value or "contract_price").strip().upper()
    return "MARK_PRICE" if token in {"MARK", "MARK_PRICE", "MARKPRICE"} else "CONTRACT_PRICE"


def normalize_position_side(value: Any) -> str:
    token = str(value or "").strip().upper()
    if token in {"BUY", "LONG"}:
        return "LONG"
    if token in {"SELL", "SHORT"}:
        return "SHORT"
    raise ValueError(f"Unsupported WEEX position side: {value}")


def normalize_client_id(value: Any, field: str = "client order id") -> str:
    client_id = str(value or "").strip()
    if not WEEX_CLIENT_ID_PATTERN.fullmatch(client_id):
        raise ValueError(f"WEEX {field} must match {WEEX_CLIENT_ID_PATTERN.pattern}")
    return client_id


class WeexAPIError(RuntimeError):
    """Structured WEEX error distinguishing a rejection from an unknown POST."""

    def __init__(
        self,
        code: Any,
        message: str,
        *,
        status_code: int | None = None,
        payload: Any = None,
        request_rejected: bool = False,
        retry_after: float | None = None,
    ):
        self.code = str(code or "")
        self.status_code = status_code
        self.payload = payload
        self.request_rejected = bool(request_rejected)
        self.retry_after = retry_after
        suffix = f" (code={self.code})" if self.code else ""
        super().__init__(f"{message or 'WEEX API error'}{suffix}")


class WeexServerClock:
    """Maintain a WEEX server-time offset for signed REST and WS requests."""

    def __init__(self, config: dict[str, Any]):
        self.enabled = bool(config.get("weexTimeSyncEnabled", True))
        self.base_url = str(config.get("weexRestBaseUrl", WEEX_DEFAULT_REST_URL)).rstrip("/")
        self.timeout = max(float(config.get("restTimeoutSeconds", 10.0)), 1.0)
        self.sync_interval = max(float(config.get("weexTimeSyncIntervalSeconds", 300.0)), 30.0)
        self.user_agent = str(config.get("weexUserAgent", "martin-bot-weex/1.0"))
        self._offset_ms = safe_float(config.get("weexTimeOffsetMs"), 0.0)
        self._last_sync_at = 0.0
        self._last_error = ""
        self._lock = threading.RLock()

    @property
    def offset_ms(self) -> float:
        with self._lock:
            return self._offset_ms

    @property
    def last_error(self) -> str:
        with self._lock:
            return self._last_error

    def timestamp_ms(self) -> int:
        self.sync()
        with self._lock:
            offset = self._offset_ms
        return int(time.time() * 1000 + offset)

    def sync(self, force: bool = False) -> bool:
        if not self.enabled:
            return False
        with self._lock:
            now = time.monotonic()
            if not force and self._last_sync_at and now - self._last_sync_at < self.sync_interval:
                return True
            try:
                started_ms = time.time() * 1000
                session = requests.Session()
                try:
                    response = session.request(
                        "GET",
                        f"{self.base_url}/capi/v3/market/time",
                        headers={"Accept": "application/json", "User-Agent": self.user_agent},
                        timeout=self.timeout,
                    )
                finally:
                    with contextlib.suppress(Exception):
                        session.close()
                ended_ms = time.time() * 1000
                if response.status_code >= 400:
                    raise RuntimeError(f"HTTP {response.status_code}")
                data = response.json()
                if isinstance(data, dict) and isinstance(data.get("data"), dict):
                    data = data["data"]
                server_time = safe_float((data or {}).get("serverTime"), 0.0) if isinstance(data, dict) else 0.0
                if server_time <= 0:
                    raise RuntimeError("WEEX server time response is missing serverTime")
                midpoint_ms = (started_ms + ended_ms) / 2.0
                self._offset_ms = server_time - midpoint_ms
                self._last_sync_at = now
                self._last_error = ""
                return True
            except Exception as exc:
                self._last_error = str(exc)
                return False


class WeexWebSocketClient:
    """WEEX V3 WebSocket transport normalized to the strategy cache contract."""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.enabled = bool(config.get("wsEnabled", True))
        self.public_enabled = bool(config.get("wsPublicEnabled", True))
        self.private_enabled = bool(config.get("wsPrivateEnabled", True))
        self.symbol = str(config.get("symbol", "BTC/USDT:USDT"))
        self.inst_id = symbol_to_weex_id(self.symbol)
        self.inst_type = "USDT-FUTURES"
        self.sandbox = bool(config.get("sandbox", False))
        self.timeframes = sorted(
            {
                str(config.get("timeframe", "5m")),
                str(config.get("sr_timeframe", "1h")),
            }
            & WEEX_WS_TIMEFRAMES
        )
        self.fresh_seconds = max(float(config.get("wsFreshSeconds", 15)), 1.0)
        self.max_fills = max(int(config.get("wsMaxFills", 200)), 20)
        self.max_candles = max(int(config.get("wsMaxCandles", 400)), 120)
        self.reconnect_delay = max(float(config.get("wsReconnectDelay", 3)), 0.0)
        self.heartbeat_timeout = max(float(config.get("wsHeartbeatTimeoutSeconds", 45)), 10.0)
        self.user_agent = str(config.get("weexWsUserAgent", "bitget-pro-trader/WEEX-V3")).strip()
        self._clock = config.get("_weex_clock") or WeexServerClock(config)
        self._public_url = str(config.get("weexWsPublicUrl", WEEX_DEFAULT_WS_PUBLIC_URL)).strip()
        self._private_url = str(config.get("weexWsPrivateUrl", WEEX_DEFAULT_WS_PRIVATE_URL)).strip()

        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._started = False
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tasks: list[asyncio.Task[Any]] = []
        self._connections: dict[str, Any] = {}
        self._ticker: dict[str, dict[str, Any]] = {}
        # WEEX accounts normally use hedge mode, so a symbol may have one
        # LONG row and one SHORT row at the same time.  Keep both rows visible
        # to the strategy instead of letting the latest update overwrite the
        # other side.
        self._positions: dict[str, dict[str, Any]] = {}
        self._orders: dict[str, dict[str, Any]] = {}
        self._fills: deque[dict[str, Any]] = deque(maxlen=self.max_fills)
        self._accounts: dict[str, dict[str, Any]] = {}
        self._candles: dict[str, dict[int, list[float]]] = {
            timeframe: {} for timeframe in self.timeframes
        }
        self._status = {
            "enabled": self.enabled,
            "transport": "websocket",
            "exchange": "WEEX",
            "symbol": self.symbol,
            "inst_id": self.inst_id,
            "inst_type": self.inst_type,
            "started": False,
            "started_at": None,
            "server_time_offset_ms": 0.0,
            "server_time_sync_error": "",
            "public": self._make_channel_status(),
            "private": self._make_channel_status(),
        }

    @staticmethod
    def _make_channel_status() -> dict[str, Any]:
        return {
            "connected": False,
            "last_message_at": None,
            "last_pong_at": None,
            "last_ticker_at": None,
            "last_position_at": None,
            "last_error": None,
            "subscriptions": [],
            "reconnects": 0,
        }

    def start(self) -> None:
        if not self.enabled or self._started:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._thread_main, name="weex-ws", daemon=True)
        self._started = True
        with self._lock:
            self._status["started"] = True
            self._status["started_at"] = time.time()
        self._thread.start()

    def stop(self) -> None:
        if not self._started:
            return
        self._stop_event.set()
        if self._loop is not None and not self._loop.is_closed():
            with contextlib.suppress(Exception):
                asyncio.run_coroutine_threadsafe(self._close_connections(), self._loop).result(timeout=5)
            for task in list(self._tasks):
                with contextlib.suppress(Exception):
                    self._loop.call_soon_threadsafe(task.cancel)
            with contextlib.suppress(Exception):
                self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._thread = None
        self._loop = None
        self._tasks = []
        self._started = False
        with self._lock:
            self._status["started"] = False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            status = deepcopy(self._status)
        status["server_time_offset_ms"] = self._clock.offset_ms
        status["server_time_sync_error"] = self._clock.last_error
        return status

    def is_fresh(self, channel: str) -> bool:
        last_message = self.snapshot().get(channel, {}).get("last_message_at")
        return bool(last_message and (time.time() - float(last_message)) <= self.fresh_seconds)

    def is_ticker_fresh(self, inst_id: str | None = None) -> bool:
        target = symbol_to_weex_id(inst_id or self.inst_id)
        with self._lock:
            updated_at = self._status["public"].get("last_ticker_at")
            has_payload = bool(self._ticker.get(target))
        return bool(
            has_payload
            and updated_at
            and (time.time() - float(updated_at)) <= self.fresh_seconds
        )

    def is_position_fresh(self, inst_id: str | None = None) -> bool:
        target = symbol_to_weex_id(inst_id or self.inst_id)
        with self._lock:
            updated_at = self._status["private"].get("last_position_at")
            has_payload = any(
                str(position.get("instId") or "").upper() == target
                and safe_float(position.get("total"), 0.0) > 0
                for position in self._positions.values()
            )
        return bool(
            has_payload
            and updated_at
            and (time.time() - float(updated_at)) <= self.fresh_seconds
        )

    def get_ticker(self, inst_id: str) -> dict[str, Any] | None:
        with self._lock:
            payload = self._ticker.get(symbol_to_weex_id(inst_id))
            return deepcopy(payload) if payload else None

    def get_position(self, inst_id: str) -> dict[str, Any] | None:
        rows = self.get_positions(inst_id)
        return rows[0] if rows else None

    def get_positions(self, inst_id: str) -> list[dict[str, Any]]:
        target = symbol_to_weex_id(inst_id)
        with self._lock:
            rows = [
                deepcopy(position)
                for position in self._positions.values()
                if str(position.get("instId") or "").upper() == target
                and safe_float(position.get("total"), 0.0) > 0
            ]
        rows.sort(key=lambda row: str(row.get("holdSide") or ""))
        return rows

    def get_orders(self, inst_id: str) -> list[dict[str, Any]]:
        target = symbol_to_weex_id(inst_id)
        with self._lock:
            rows = [
                deepcopy(order)
                for order in self._orders.values()
                if str(order.get("symbol") or order.get("instId") or "").upper() == target
            ]
        return rows

    def get_account(self, coin: str) -> dict[str, Any] | None:
        with self._lock:
            payload = self._accounts.get(str(coin or "").upper())
            return deepcopy(payload) if payload else None

    def get_fills(self, inst_id: str, limit: int = 60) -> list[dict[str, Any]]:
        target = symbol_to_weex_id(inst_id)
        with self._lock:
            rows = [
                deepcopy(fill)
                for fill in self._fills
                if str(fill.get("symbol") or "").upper() == target
            ]
        return rows[-max(int(limit), 1) :]

    def get_candles(self, timeframe: str) -> list[list[float]]:
        with self._lock:
            rows = list(self._candles.get(str(timeframe), {}).values())
        rows.sort(key=lambda row: row[0])
        return deepcopy(rows)

    def _thread_main(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._tasks = []
        if self.public_enabled:
            self._tasks.append(self._loop.create_task(self._connection_worker("public")))
        if self.private_enabled:
            if self._has_private_credentials():
                self._tasks.append(self._loop.create_task(self._connection_worker("private")))
            else:
                self._set_channel_state(
                    "private",
                    last_error="WEEX private WebSocket credentials are missing",
                )
        if not self._tasks:
            with self._lock:
                self._status["started"] = False
            self._loop.close()
            return
        try:
            self._loop.run_forever()
        finally:
            with contextlib.suppress(Exception):
                self._loop.run_until_complete(self._close_connections())
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            with contextlib.suppress(Exception):
                self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
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

    def _private_headers(self) -> dict[str, str]:
        timestamp = str(self._clock.timestamp_ms())
        message = f"{timestamp}/v3/ws/private".encode("utf-8")
        secret = str(self.config.get("secretKey", "")).encode("utf-8")
        signature = base64.b64encode(
            hmac.new(secret, message, digestmod=hashlib.sha256).digest()
        ).decode("utf-8")
        return {
            "User-Agent": self.user_agent,
            "ACCESS-KEY": str(self.config.get("apiKey", "")),
            "ACCESS-PASSPHRASE": str(self.config.get("passphrase", "")),
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-SIGN": signature,
        }

    def _connection_headers(self, channel: str) -> dict[str, str]:
        if channel == "private":
            return self._private_headers()
        return {"User-Agent": self.user_agent}

    async def _connection_worker(self, channel: str) -> None:
        url = self._public_url if channel == "public" else self._private_url
        while not self._stop_event.is_set():
            try:
                headers = self._connection_headers(channel)
                user_agent = headers.pop("User-Agent", self.user_agent)
                async with websockets.connect(
                    url,
                    additional_headers=headers or None,
                    user_agent_header=user_agent,
                    ping_interval=None,
                    close_timeout=5,
                    open_timeout=10,
                ) as websocket:
                    self._connections[channel] = websocket
                    self._set_channel_state(
                        channel,
                        connected=True,
                        last_error="",
                        bump_reconnect=True,
                    )
                    await self._subscribe(websocket, channel)
                    while not self._stop_event.is_set():
                        try:
                            raw_message = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=self.heartbeat_timeout,
                            )
                        except asyncio.TimeoutError as exc:
                            raise RuntimeError(
                                f"WEEX {channel} WebSocket heartbeat timed out"
                            ) from exc
                        await self._handle_raw_message(channel, raw_message)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._set_channel_state(channel, connected=False, last_error=str(exc))
                if self._stop_event.is_set():
                    break
                delay = self.reconnect_delay
                if delay > 0:
                    delay += random.uniform(0, min(delay * 0.2, 1.0))
                    await asyncio.sleep(delay)
            finally:
                self._connections.pop(channel, None)
                self._set_channel_state(channel, connected=False)

    async def _subscribe(self, websocket: Any, channel: str) -> None:
        subscriptions = self._build_subscriptions(channel)
        for request_id, topic in enumerate(subscriptions, start=1):
            await websocket.send(
                json.dumps(
                    {"method": "SUBSCRIBE", "params": [topic], "id": request_id},
                    separators=(",", ":"),
                )
            )
        self._set_channel_state(channel, subscriptions=subscriptions)

    def _build_subscriptions(self, channel: str) -> list[str]:
        if channel == "public":
            topics = [f"{self.inst_id}@ticker", f"{self.inst_id}@depth15"]
            topics.extend(
                f"{self.inst_id}@kline_{timeframe}_LAST_PRICE"
                for timeframe in self.timeframes
            )
            return topics
        return ["account", "positions", "fill", "orders"]

    async def _handle_raw_message(self, channel: str, raw_message: Any) -> None:
        self._touch(channel)
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8")
        payload = json.loads(raw_message)
        if not isinstance(payload, dict):
            return
        if payload.get("event") == "ping" or payload.get("type") == "ping":
            websocket = self._connections.get(channel)
            if websocket is not None:
                await websocket.send('{"method":"PONG","id":1}')
            self._set_channel_state(channel, last_pong_at=time.time())
            return
        await self._handle_payload(channel, payload)

    async def _handle_payload(self, channel: str, payload: dict[str, Any]) -> None:
        if "result" in payload:
            if payload.get("result") is False:
                message = payload.get("msg") or f"subscription {payload.get('id')} rejected"
                self._set_channel_state(channel, last_error=str(message))
                raise RuntimeError(f"WEEX WebSocket subscription failed: {message}")
            return
        event = str(payload.get("e") or "").lower()
        rows = payload.get("d") or []
        if not isinstance(rows, list):
            rows = []
        if event == "ticker":
            self._consume_ticker(payload, rows)
        elif event == "depth":
            self._consume_depth(payload)
        elif event == "kline":
            self._consume_candles(rows)
        elif event == "positions":
            self._consume_positions(rows)
        elif event == "account":
            self._consume_accounts(rows)
        elif event == "orders":
            self._consume_orders(rows)
        elif event == "fill":
            self._consume_fills(rows)

    def _consume_ticker(self, payload: dict[str, Any], rows: list[dict[str, Any]]) -> None:
        inst_id = str(payload.get("s") or self.inst_id).upper()
        received_at = time.time()
        with self._lock:
            for source in rows:
                if not isinstance(source, dict):
                    continue
                existing = self._ticker.get(inst_id, {})
                row = {
                    **existing,
                    **source,
                    "instId": inst_id,
                    "lastPr": source.get("c"),
                    "markPrice": source.get("m"),
                    "indexPrice": source.get("i"),
                    "high24h": source.get("h"),
                    "low24h": source.get("l"),
                    "change24h": source.get("P"),
                    "baseVolume": source.get("v"),
                    "quoteVolume": source.get("q"),
                    "ts": payload.get("E"),
                }
                self._ticker[inst_id] = row
                position_keys = [
                    key
                    for key, position in self._positions.items()
                    if str(position.get("instId") or "").upper() == inst_id
                ]
                for position_key in position_keys:
                    position = self._positions.get(position_key)
                    if position is None:
                        continue
                    position = dict(position)
                    size = safe_float(position.get("total"), 0.0)
                    entry = safe_float(position.get("openPriceAvg"), 0.0)
                    mark = safe_float(row.get("markPrice") or row.get("lastPr"), 0.0)
                    side = str(position.get("holdSide") or "").lower()
                    direction = -1.0 if side == "short" else 1.0
                    position["markPrice"] = mark
                    position["unrealizedPL"] = direction * size * (mark - entry)
                    self._positions[position_key] = position
                if inst_id == self.inst_id and safe_float(row.get("lastPr"), 0.0) > 0:
                    self._status["public"]["last_ticker_at"] = received_at

    def _consume_depth(self, payload: dict[str, Any]) -> None:
        inst_id = str(payload.get("s") or self.inst_id).upper()
        bids = payload.get("b") or []
        asks = payload.get("a") or []
        with self._lock:
            ticker = dict(self._ticker.get(inst_id, {"instId": inst_id}))
            if bids and isinstance(bids[0], (list, tuple)) and len(bids[0]) >= 2:
                ticker["bidPr"] = bids[0][0]
                ticker["bidSz"] = bids[0][1]
            if asks and isinstance(asks[0], (list, tuple)) and len(asks[0]) >= 2:
                ticker["askPr"] = asks[0][0]
                ticker["askSz"] = asks[0][1]
            self._ticker[inst_id] = ticker

    def _consume_candles(self, rows: list[dict[str, Any]]) -> None:
        accepted = 0
        rejected = 0
        with self._lock:
            for source in rows:
                if not isinstance(source, dict):
                    rejected += 1
                    continue
                timeframe = str(source.get("i") or "")
                if timeframe not in self._candles:
                    continue
                if not all(key in source for key in ("t", "o", "h", "l", "c", "v")):
                    rejected += 1
                    continue
                candle = normalize_ohlcv_row(
                    source.get("t"),
                    source.get("o"),
                    source.get("h"),
                    source.get("l"),
                    source.get("c"),
                    source.get("v"),
                )
                if candle is None:
                    rejected += 1
                    continue
                timestamp = int(candle[0])
                store = self._candles[timeframe]
                store[timestamp] = candle
                accepted += 1
                while len(store) > self.max_candles:
                    store.pop(min(store), None)
        if rejected:
            self._set_channel_state(
                "public",
                last_error=f"ignored {rejected} invalid WEEX kline row(s)",
            )
        elif accepted:
            self._set_channel_state("public", last_error="")

    def _normalize_position(self, source: dict[str, Any]) -> dict[str, Any]:
        row = dict(source)
        inst_id = str(row.get("symbol") or self.inst_id).upper()
        size = safe_float(row.get("size"), 0.0)
        open_value = safe_float(row.get("openValue"), 0.0)
        entry_price = open_value / size if size > 0 else 0.0
        ticker = self._ticker.get(inst_id, {})
        mark_price = safe_float(ticker.get("markPrice") or ticker.get("lastPr"), 0.0)
        side = str(row.get("side") or "").lower()
        direction = -1.0 if side == "short" else 1.0
        unrealized = direction * size * (mark_price - entry_price) if mark_price > 0 else 0.0
        leverage = safe_float(row.get("leverage"), 0.0)
        margin_size = safe_float(row.get("isolatedMargin"), 0.0)
        if margin_size <= 0 and leverage > 0:
            margin_size = open_value / leverage
        row.update(
            {
                "instId": inst_id,
                "total": size,
                "holdSide": side,
                "openPriceAvg": entry_price,
                "markPrice": mark_price,
                "unrealizedPL": unrealized,
                "liquidationPrice": safe_float(row.get("liquidatePrice"), 0.0),
                "marginMode": str(row.get("marginMode") or "").lower(),
                "marginSize": margin_size,
            }
        )
        return row

    def _consume_positions(self, rows: list[dict[str, Any]]) -> None:
        received_at = time.time()
        with self._lock:
            touched_target = False
            for source in rows:
                if not isinstance(source, dict):
                    continue
                row = self._normalize_position(source)
                inst_id = row["instId"]
                side = str(row.get("holdSide") or "unknown").lower()
                cache_key = f"{inst_id}:{side}"
                if safe_float(row.get("total"), 0.0) > 0:
                    self._positions[cache_key] = row
                else:
                    self._positions.pop(cache_key, None)
                if inst_id == self.inst_id:
                    touched_target = True
            if not rows:
                self._positions = {
                    key: position
                    for key, position in self._positions.items()
                    if str(position.get("instId") or "").upper() != self.inst_id
                }
                touched_target = True
            if touched_target:
                self._status["private"]["last_position_at"] = received_at

    def _consume_accounts(self, rows: list[dict[str, Any]]) -> None:
        with self._lock:
            for source in rows:
                if not isinstance(source, dict):
                    continue
                row = dict(source)
                coin = str(row.get("coin") or "").upper()
                if not coin:
                    continue
                amount = safe_float(row.get("amount"), 0.0)
                row.update(
                    {
                        "marginCoin": coin,
                        "available": amount,
                        "equity": amount,
                    }
                )
                self._accounts[coin] = row

    def _consume_orders(self, rows: list[dict[str, Any]]) -> None:
        with self._lock:
            for source in rows:
                if not isinstance(source, dict):
                    continue
                row = dict(source)
                order_id = str(row.get("id") or row.get("orderId") or "")
                if not order_id:
                    continue
                status = str(row.get("status") or "").lower()
                if status in WS_CLOSED_ORDER_STATUSES:
                    self._orders.pop(order_id, None)
                else:
                    self._orders[order_id] = row

    def _consume_fills(self, rows: list[dict[str, Any]]) -> None:
        with self._lock:
            existing_ids = {str(row.get("id") or "") for row in self._fills}
            for source in rows:
                if not isinstance(source, dict):
                    continue
                fill_id = str(source.get("id") or "")
                if fill_id and fill_id in existing_ids:
                    continue
                self._fills.append(dict(source))
                if fill_id:
                    existing_ids.add(fill_id)

    def _touch(self, channel: str) -> None:
        self._set_channel_state(channel, last_message_at=time.time())

    def _set_channel_state(
        self,
        channel: str,
        bump_reconnect: bool = False,
        **updates: Any,
    ) -> None:
        with self._lock:
            state = self._status[channel]
            if bump_reconnect:
                state["reconnects"] += 1
            state.update(updates)


class WeexExchangeAdapter(ExchangeAdapter):
    """WEEX V3 adapter with WebSocket caches and authoritative REST fallback."""

    # WEEX V3 requires every order to carry LONG/SHORT explicitly.  The
    # strategy can therefore operate its single managed direction while the
    # account remains in WEEX's normal hedge mode.
    supports_single_direction_hedge_mode = True

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.symbol = str(config.get("symbol", "BTC/USDT:USDT"))
        self.inst_id = symbol_to_weex_id(self.symbol)
        self.margin_coin = str(config.get("marginCoin", "USDT")).upper()
        self.margin_mode = normalize_margin_mode(config.get("marginMode"))
        self.sandbox = bool(config.get("sandbox", False))
        if self.sandbox:
            raise ValueError(
                "WEEX V3 demo mode is incomplete: the official demo API has no current-order, "
                "cancel-order, conditional-order, or TPSL endpoints. Set sandbox=false only after "
                "configuring a dedicated WEEX API key and validating the account manually."
            )

        self._clock = WeexServerClock(config)
        ws_config = dict(config)
        ws_config["_weex_clock"] = self._clock
        self.ws = WeexWebSocketClient(ws_config)
        self._base_url = str(config.get("weexRestBaseUrl", WEEX_DEFAULT_REST_URL)).rstrip("/")
        self._timeout = max(float(config.get("restTimeoutSeconds", 10.0)), 1.0)
        self._min_interval = max(float(config.get("restMinIntervalSeconds", 0.08)), 0.0)
        self._retry_attempts = max(int(config.get("networkRetryAttempts", 4)), 1)
        self._retry_delay = max(float(config.get("networkRetryDelay", 2.0)), 0.0)
        self._retry_backoff = max(float(config.get("networkRetryBackoff", 1.8)), 1.0)
        self._retry_jitter = max(float(config.get("restRetryJitterSeconds", 0.25)), 0.0)
        self._rate_lock = threading.Lock()
        self._next_request_at = 0.0
        self._session = requests.Session()
        self._market_cache: dict[str, dict[str, Any]] = {}
        self._position_mode: str | None = None
        self._ticker_stats_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._ticker_stats_ttl = max(float(config.get("weexTickerStatsCacheSeconds", 15.0)), 1.0)
        self._ohlcv_seed: dict[str, list[list[float]]] = {}
        self._ohlcv_seed_at: dict[str, float] = {}
        self.start()

    @property
    def name(self) -> str:
        return "WEEX"

    def start(self) -> None:
        self.ws.start()

    def close(self) -> None:
        self.ws.stop()
        with contextlib.suppress(Exception):
            self._session.close()

    def ws_status(self) -> dict[str, Any]:
        return self.ws.snapshot()

    def _wait_for_request_slot(self) -> None:
        if self._min_interval <= 0:
            return
        with self._rate_lock:
            now = time.monotonic()
            wait_for = self._next_request_at - now
            if wait_for > 0:
                time.sleep(wait_for)
                now = time.monotonic()
            self._next_request_at = now + self._min_interval

    def _signature(self, timestamp: str, method: str, path: str, query: str, body: str) -> str:
        query_part = f"?{query}" if query else ""
        message = f"{timestamp}{method.upper()}{path}{query_part}{body}"
        digest = hmac.new(
            str(self.config.get("secretKey", "")).encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("ascii")

    @staticmethod
    def _parse_retry_after(value: Any) -> float | None:
        try:
            parsed = float(value)
            return max(parsed, 0.0)
        except (TypeError, ValueError):
            return None

    def _headers(self, private: bool, timestamp: str = "", signature: str = "") -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": str(self.config.get("weexUserAgent", "martin-bot-weex/1.0")),
        }
        if private:
            headers.update(
                {
                    "ACCESS-KEY": str(self.config.get("apiKey", "")),
                    "ACCESS-SIGN": signature,
                    "ACCESS-PASSPHRASE": str(self.config.get("passphrase", "")),
                    "ACCESS-TIMESTAMP": timestamp,
                }
            )
        return headers

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        private: bool = False,
        _time_retry: bool = True,
    ) -> Any:
        method = method.upper()
        clean_params = {key: value for key, value in (params or {}).items() if value not in (None, "")}
        query = urlencode(clean_params, doseq=True)
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) if payload is not None else ""
        if private:
            missing = [
                key
                for key in ("apiKey", "secretKey", "passphrase")
                if not str(self.config.get(key, "")).strip()
            ]
            if missing:
                raise ValueError(f"WEEX private request missing credentials: {', '.join(missing)}")
        timestamp = str(self._clock.timestamp_ms() if private else int(time.time() * 1000))
        signature = self._signature(timestamp, method, path, query, body) if private else ""
        url = f"{self._base_url}{path}"
        if query:
            url = f"{url}?{query}"
        self._wait_for_request_slot()
        try:
            response = self._session.request(
                method,
                url,
                headers=self._headers(private, timestamp, signature),
                data=body or None,
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise WeexAPIError(
                "network_error",
                str(exc),
                request_rejected=False,
            ) from exc

        try:
            data = response.json() if response.content else None
        except ValueError:
            data = response.text

        retry_after = self._parse_retry_after(response.headers.get("Retry-After"))
        if response.status_code >= 400:
            error_row = data if isinstance(data, dict) else {}
            code = error_row.get("errorCode") or error_row.get("code") or response.status_code
            message = error_row.get("errorMessage") or error_row.get("msg") or error_row.get("message") or response.text
            if private and str(code) == "-1046" and _time_retry:
                self._clock.sync(force=True)
                return self._request(
                    method,
                    path,
                    params=params,
                    payload=payload,
                    private=private,
                    _time_retry=False,
                )
            ambiguous = response.status_code == 408 or response.status_code >= 500
            raise WeexAPIError(
                code,
                str(message or response.reason),
                status_code=response.status_code,
                payload=data,
                request_rejected=not ambiguous,
                retry_after=retry_after,
            )

        if isinstance(data, dict):
            if data.get("success") is False:
                code = data.get("errorCode") or data.get("code")
                if private and str(code) == "-1046" and _time_retry:
                    self._clock.sync(force=True)
                    return self._request(
                        method,
                        path,
                        params=params,
                        payload=payload,
                        private=private,
                        _time_retry=False,
                    )
                raise WeexAPIError(
                    code,
                    str(data.get("errorMessage") or data.get("msg") or "WEEX request rejected"),
                    status_code=response.status_code,
                    payload=data,
                    request_rejected=True,
                )
            if "code" in data:
                code = str(data.get("code") or "").upper()
                if code and code not in WEEX_SUCCESS_CODES:
                    if private and code == "-1046" and _time_retry:
                        self._clock.sync(force=True)
                        return self._request(
                            method,
                            path,
                            params=params,
                            payload=payload,
                            private=private,
                            _time_retry=False,
                        )
                    raise WeexAPIError(
                        code,
                        str(data.get("msg") or data.get("message") or "WEEX request rejected"),
                        status_code=response.status_code,
                        payload=data,
                        request_rejected=True,
                    )
                if "data" in data:
                    return data.get("data")
        return data

    def _retry(self, label: str, fn, *args, **kwargs):
        delay = self._retry_delay
        last_error: Exception | None = None
        for attempt in range(1, self._retry_attempts + 1):
            try:
                return fn(*args, **kwargs)
            except WeexAPIError as exc:
                last_error = exc
                if exc.request_rejected and exc.status_code != 429:
                    raise
                if attempt >= self._retry_attempts:
                    raise
                sleep_for = exc.retry_after if exc.retry_after is not None else delay
            except Exception as exc:
                last_error = exc
                if attempt >= self._retry_attempts:
                    raise
                sleep_for = delay
            sleep_for = max(float(sleep_for), 0.0) + random.uniform(0, self._retry_jitter)
            if sleep_for > 0:
                time.sleep(sleep_for)
            delay = max(delay * self._retry_backoff, self._retry_delay)
        if last_error:
            raise last_error
        raise RuntimeError(f"{label} failed")

    def _public_get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._request("GET", path, params=params, private=False)

    def _private_get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._request("GET", path, params=params, private=True)

    def _private_post(self, path: str, payload: dict[str, Any] | None = None) -> Any:
        return self._request("POST", path, payload=payload or {}, private=True)

    def _private_delete(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._request("DELETE", path, params=params, private=True)

    def load_markets(self, reload: bool = False, params: dict[str, Any] | None = None):
        if reload:
            self._market_cache.clear()
        query = dict(params or {})
        query.setdefault("symbol", self.inst_id)
        payload = self._retry("load WEEX markets", self._public_get, "/capi/v3/market/exchangeInfo", query) or {}
        tradable_payload = self._retry(
            "load WEEX API trading symbols",
            self._public_get,
            "/capi/v3/market/apiTradingSymbols",
            None,
        ) or []
        api_trading_symbols = {
            str(item).upper()
            for item in tradable_payload
            if isinstance(item, (str, int)) and str(item).strip()
        }
        rows = payload.get("symbols") if isinstance(payload, dict) else []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            market_id = str(row.get("symbol") or "").upper()
            base = str(row.get("baseAsset") or "")
            quote = str(row.get("quoteAsset") or "")
            settle = str(row.get("marginAsset") or quote)
            unified = self.symbol if market_id == self.inst_id else f"{base}/{quote}:{settle}"
            amount_step = precision_step(row.get("quantityPrecision"))
            price_step = precision_step(row.get("pricePrecision"))
            market = {
                "id": market_id,
                "symbol": unified,
                "base": base,
                "quote": quote,
                "settle": settle,
                "baseId": base,
                "quoteId": quote,
                "settleId": settle,
                "contract": True,
                "swap": True,
                "linear": True,
                "active": market_id in api_trading_symbols,
                "contractSize": safe_float(row.get("contractVal"), 1.0),
                "precision": {"amount": amount_step, "price": price_step},
                "limits": {
                    "amount": {
                        "min": safe_float(row.get("minOrderSize"), amount_step),
                        "max": safe_float(row.get("maxOrderSize"), 0.0) or None,
                    },
                    "cost": {"min": None, "max": None},
                    "leverage": {
                        "min": safe_float(row.get("minLeverage"), 1.0),
                        "max": safe_float(row.get("maxLeverage"), 0.0) or None,
                    },
                },
                "maker": safe_float(row.get("makerFeeRate"), 0.0),
                "taker": safe_float(row.get("takerFeeRate"), 0.0),
                "info": row,
            }
            self._market_cache[unified] = market
            self._market_cache[market_id] = market
        if self.inst_id not in self._market_cache:
            raise RuntimeError(f"WEEX contract is unavailable through API trading: {self.inst_id}")
        selected_market = self._market_cache[self.inst_id]
        if not selected_market.get("active"):
            raise RuntimeError(f"WEEX contract is not enabled for API trading: {self.inst_id}")
        if str(selected_market.get("settle") or "").upper() != self.margin_coin:
            raise RuntimeError(
                f"WEEX adapter supports {self.margin_coin}-margined contracts only: {self.inst_id}"
            )
        return {self.symbol: self._market_cache[self.inst_id]}

    def market(self, symbol: str):
        market_id = symbol_to_weex_id(symbol)
        if market_id not in self._market_cache:
            self.load_markets(params={"symbol": market_id})
        return self._market_cache[market_id]

    def amount_to_precision(self, symbol: str, amount: float) -> str:
        return quantize_to_step(amount, safe_float(self.market(symbol)["precision"].get("amount"), 0.0))

    def price_to_precision(self, symbol: str, price: float) -> str:
        return quantize_to_step(price, safe_float(self.market(symbol)["precision"].get("price"), 0.0))

    def set_leverage(self, leverage: int, symbol: str, params: dict[str, Any] | None = None):
        payload: dict[str, Any] = {
            "symbol": symbol_to_weex_id(symbol),
            "marginType": self.margin_mode,
        }
        if self.margin_mode == "ISOLATED":
            payload["isolatedLongLeverage"] = str(leverage)
            payload["isolatedShortLeverage"] = str(leverage)
        else:
            payload["crossLeverage"] = str(leverage)
        payload.update({key: value for key, value in (params or {}).items() if value is not None})
        return self._retry("set WEEX leverage", self._private_post, "/capi/v3/account/leverage", payload)

    def fetch_position_mode(self, symbol: str | None = None) -> str:
        data = self._retry(
            "fetch WEEX position mode",
            self._private_get,
            "/capi/v3/account/accountConfig",
            None,
        ) or {}
        if data.get("canTrade") is False:
            raise RuntimeError("WEEX API key/account is not permitted to trade futures")
        if "dualSidePosition" not in data:
            raise RuntimeError("WEEX account configuration is missing dualSidePosition")
        self._position_mode = "hedge_mode" if bool(data.get("dualSidePosition")) else "one_way_mode"
        return self._position_mode

    def fetch_balance(self, params: dict[str, Any] | None = None):
        rows = self._retry(
            "fetch WEEX balance",
            self._private_get,
            "/capi/v3/account/balance",
            None,
        ) or []
        selected = next(
            (row for row in rows if str((row or {}).get("asset", "")).upper() == self.margin_coin),
            rows[0] if rows else {},
        )
        selected = dict(selected or {})
        coin = str(selected.get("asset") or self.margin_coin).upper()
        available = safe_float(selected.get("availableBalance"), 0.0)
        frozen = safe_float(selected.get("frozen"), 0.0)
        wallet_balance = safe_float(selected.get("balance"), 0.0)
        unrealized = safe_float(selected.get("unrealizePnl"), 0.0)
        equity = wallet_balance + unrealized
        selected.update(
            {
                "marginCoin": coin,
                "available": available,
                "locked": frozen,
                "accountEquity": equity,
                "crossedMaxAvailable": available,
            }
        )
        return {
            coin: {"free": available, "used": frozen, "total": equity},
            "free": {coin: available},
            "used": {coin: frozen},
            "total": {coin: equity},
            "info": [selected],
        }

    def _mark_price(self, symbol: str) -> float:
        row = self._retry(
            "fetch WEEX mark price",
            self._public_get,
            "/capi/v3/market/symbolPrice",
            {"symbol": symbol_to_weex_id(symbol), "priceType": "MARK"},
        ) or {}
        return safe_float(row.get("price"), 0.0) if isinstance(row, dict) else 0.0

    def fetch_positions(self, symbols: list[str] | None = None, params: dict[str, Any] | None = None):
        force_rest = bool((params or {}).get("_force_rest", False))
        if not force_rest and self.ws.is_position_fresh(self.inst_id):
            cached_rows = self.ws.get_positions(self.inst_id)
            if cached_rows:
                result = []
                for cached in cached_rows:
                    cached = dict(cached)
                    cached["posMode"] = self._position_mode or "hedge_mode"
                    contracts = safe_float(cached.get("total"), 0.0)
                    if contracts <= 0:
                        continue
                    entry_price = safe_float(cached.get("openPriceAvg"), 0.0)
                    mark_price = safe_float(cached.get("markPrice"), 0.0)
                    unrealized = safe_float(cached.get("unrealizedPL"), 0.0)
                    margin_size = safe_float(cached.get("marginSize"), 0.0)
                    percentage = (unrealized / margin_size * 100.0) if margin_size > 0 else 0.0
                    result.append(
                        {
                            "symbol": self.symbol,
                            "contracts": contracts,
                            "side": str(cached.get("holdSide") or "").lower(),
                            "entryPrice": entry_price,
                            "markPrice": mark_price,
                            "unrealizedPnl": unrealized,
                            "percentage": percentage,
                            "liquidationPrice": safe_float(cached.get("liquidationPrice"), 0.0),
                            "marginMode": str(cached.get("marginMode") or "").lower(),
                            "marginSize": margin_size,
                            "leverage": safe_float(cached.get("leverage"), 0.0),
                            "info": cached,
                        }
                    )
                return result
        rows = self._retry(
            "fetch WEEX positions",
            self._private_get,
            "/capi/v3/account/position/allPosition",
            None,
        ) or []
        wanted = {symbol_to_weex_id(symbol) for symbol in (symbols or [self.symbol])}
        mark_prices: dict[str, float] = {}
        result = []
        for source in rows:
            if not isinstance(source, dict):
                continue
            market_id = str(source.get("symbol") or "").upper()
            contracts = safe_float(source.get("size"), 0.0)
            if market_id not in wanted or contracts <= 0:
                continue
            row = dict(source)
            open_value = safe_float(row.get("openValue"), 0.0)
            entry_price = open_value / contracts if contracts > 0 else 0.0
            if entry_price <= 0:
                entry_price = safe_float(row.get("openPrice") or row.get("entryPrice"), 0.0)
            if market_id not in mark_prices:
                mark_prices[market_id] = self._mark_price(self.symbol)
            mark_price = mark_prices[market_id]
            unrealized = safe_float(row.get("unrealizePnl"), 0.0)
            margin_size = safe_float(row.get("marginSize") or row.get("isolatedMargin"), 0.0)
            percentage = (unrealized / margin_size * 100.0) if margin_size > 0 else 0.0
            side = str(row.get("side") or "").lower()
            row.update(
                {
                    "posMode": self._position_mode or "hedge_mode",
                    "holdSide": side,
                    "openPriceAvg": entry_price,
                    "markPrice": mark_price,
                    "unrealizedPL": unrealized,
                }
            )
            result.append(
                {
                    "symbol": self.symbol,
                    "contracts": contracts,
                    "side": side,
                    "entryPrice": entry_price,
                    "markPrice": mark_price,
                    "unrealizedPnl": unrealized,
                    "percentage": percentage,
                    "liquidationPrice": safe_float(row.get("liquidatePrice"), 0.0),
                    "marginMode": str(row.get("marginType") or "").lower(),
                    "marginSize": margin_size,
                    "leverage": safe_float(row.get("leverage"), 0.0),
                    "info": row,
                }
            )
        return result

    @staticmethod
    def _standard_status(value: Any) -> str:
        status = str(value or "").lower()
        if status == "filled":
            return "closed"
        if status in {"canceled", "cancelled"}:
            return "canceled"
        if status in OPEN_ORDER_STATUSES:
            return "open"
        return status

    @staticmethod
    def _trigger_status(value: Any) -> str:
        status = str(value or "").lower()
        if status in TERMINAL_TRIGGER_FILLED:
            return "executed"
        if status in {"canceled", "cancelled"}:
            return "canceled"
        if status in OPEN_ORDER_STATUSES:
            return "live"
        return status

    def _normalize_standard_order(self, row: dict[str, Any], symbol: str | None = None) -> dict[str, Any]:
        amount = safe_float(row.get("origQty") or row.get("quantity") or row.get("size"), 0.0)
        filled = safe_float(row.get("executedQty") or row.get("cumFillSize"), 0.0)
        order_side = str(row.get("side") or row.get("orderSide") or "").upper()
        position_side = str(row.get("positionSide") or "").upper()
        inferred_reduce_only = (position_side == "LONG" and order_side == "SELL") or (
            position_side == "SHORT" and order_side == "BUY"
        )
        return {
            "id": row.get("orderId") or row.get("id"),
            "clientOrderId": row.get("clientOrderId"),
            "symbol": symbol or self.symbol,
            "type": str(row.get("type") or "").lower(),
            "side": order_side.lower(),
            "positionSide": position_side.lower(),
            "price": safe_float(row.get("price"), 0.0),
            "average": safe_float(row.get("avgPrice") or row.get("latestFillPrice"), 0.0),
            "amount": amount,
            "remaining": max(amount - filled, 0.0),
            "filled": filled,
            "status": self._standard_status(row.get("status")),
            "reduceOnly": bool(row.get("reduceOnly", False) or inferred_reduce_only),
            "timestamp": int(safe_float(row.get("time") or row.get("createdTime"), 0.0)),
            "info": row,
        }

    @staticmethod
    def _is_position_tpsl(row: dict[str, Any]) -> bool:
        plan_type = str(row.get("planType") or "").upper()
        return bool(row.get("closePosition") or row.get("positionTpsl")) or plan_type in {
            "STOP_LOSS",
            "TAKE_PROFIT",
        }

    def _normalize_trigger_order(self, row: dict[str, Any], symbol: str | None = None) -> dict[str, Any]:
        amount = safe_float(row.get("quantity"), 0.0)
        is_tpsl = self._is_position_tpsl(row)
        raw_type = str(row.get("orderType") or row.get("type") or "").upper()
        order_side = str(row.get("side") or row.get("orderSide") or "").upper()
        position_side = str(row.get("positionSide") or "").upper()
        inferred_reduce_only = (position_side == "LONG" and order_side == "SELL") or (
            position_side == "SHORT" and order_side == "BUY"
        )
        derived_plan_type = "pos_loss" if is_tpsl and "STOP" in raw_type else "profit_loss" if is_tpsl else "normal_plan"
        return {
            "id": row.get("algoId") or row.get("orderId"),
            "clientOrderId": row.get("clientAlgoId") or row.get("clientOrderId"),
            "executeOrderId": row.get("actualOrderId"),
            "symbol": symbol or self.symbol,
            "type": "trigger",
            "planType": derived_plan_type,
            "side": order_side.lower(),
            "positionSide": position_side.lower(),
            "price": safe_float(row.get("price") or row.get("actualPrice"), 0.0),
            "average": safe_float(row.get("actualPrice"), 0.0),
            "triggerPrice": safe_float(row.get("triggerPrice"), 0.0),
            "triggerType": str(row.get("workingType") or row.get("triggerPriceType") or "").lower(),
            "amount": amount,
            "remaining": amount,
            "filled": amount if self._trigger_status(row.get("algoStatus")) == "executed" else 0.0,
            "status": self._trigger_status(row.get("algoStatus")),
            "reduceOnly": bool(row.get("reduceOnly", False) or is_tpsl or inferred_reduce_only),
            "timestamp": int(safe_float(row.get("createTime"), 0.0)),
            "info": row,
        }

    def _fetch_standard_open_orders(
        self,
        symbol: str | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        target = symbol or self.symbol
        force_rest = bool((params or {}).get("_force_rest", False))
        if not force_rest and self.ws.is_fresh("private"):
            cached = [
                self._normalize_standard_order(row, target)
                for row in self.ws.get_orders(symbol_to_weex_id(target))
            ]
            if cached:
                cached.sort(key=lambda item: (item.get("price", 0.0), str(item.get("id") or "")))
                return cached[: max(int(limit or len(cached)), 1)]
        rows = self._retry(
            "fetch WEEX open orders",
            self._private_get,
            "/capi/v3/openOrders",
            {"symbol": symbol_to_weex_id(target), "limit": min(max(int(limit or 100), 1), 100), "page": 0},
        ) or []
        return [self._normalize_standard_order(row, target) for row in rows if isinstance(row, dict)]

    def fetch_pending_trigger_orders(
        self,
        symbol: str | None = None,
        plan_type: str = "normal_plan",
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        target = symbol or self.symbol
        rows = self._retry(
            "fetch WEEX conditional orders",
            self._private_get,
            "/capi/v3/openAlgoOrders",
            {"symbol": symbol_to_weex_id(target), "limit": min(max(int(limit or 100), 1), 100), "page": 1},
        ) or []
        want_tpsl = str(plan_type).lower() in {"pos_loss", "profit_loss", "tpsl"}
        normalized = []
        for row in rows:
            if not isinstance(row, dict) or self._is_position_tpsl(row) != want_tpsl:
                continue
            order_type = str(row.get("orderType") or row.get("type") or "").upper()
            requested_plan = str(plan_type).lower()
            if want_tpsl and requested_plan == "pos_loss" and "STOP" not in order_type:
                continue
            if want_tpsl and requested_plan == "profit_loss" and "TAKE_PROFIT" not in order_type:
                continue
            normalized.append(self._normalize_trigger_order(row, target))
        return normalized

    def fetch_open_orders(
        self,
        symbol: str | None = None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        target = symbol or self.symbol
        orders = self._fetch_standard_open_orders(target, limit, params)
        orders.extend(self.fetch_pending_trigger_orders(target, "normal_plan", limit, params))
        orders.sort(key=lambda item: (item.get("price", 0.0), str(item.get("id") or "")))
        return orders

    def _history_rows(self, symbol: str, limit: int = 1000, max_pages: int = 5) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for page in range(max_pages):
            batch = self._retry(
                "fetch WEEX order history",
                self._private_get,
                "/capi/v3/order/history",
                {"symbol": symbol_to_weex_id(symbol), "limit": min(max(limit, 1), 1000), "page": page},
            ) or []
            valid = [row for row in batch if isinstance(row, dict)]
            rows.extend(valid)
            if len(valid) < min(max(limit, 1), 1000):
                break
        return rows

    def fetch_order_detail(
        self,
        symbol: str,
        order_id: str | None = None,
        client_oid: str | None = None,
    ) -> dict[str, Any] | None:
        if not order_id and not client_oid:
            raise ValueError("order_id or client_oid is required")
        if order_id:
            try:
                row = self._retry(
                    "fetch WEEX order detail",
                    self._private_get,
                    "/capi/v3/order",
                    {"orderId": str(order_id)},
                )
                if isinstance(row, dict):
                    return self._normalize_standard_order(row, symbol)
            except WeexAPIError as exc:
                if str(exc.code) not in {"-1054", "404"}:
                    raise
        for row in self._fetch_standard_open_orders(symbol, 100):
            if client_oid and str(row.get("clientOrderId") or "") == str(client_oid):
                return row
        return self.fetch_history_order(symbol, order_id=order_id, client_oid=client_oid)

    def fetch_history_order(
        self,
        symbol: str,
        order_id: str | None = None,
        client_oid: str | None = None,
    ) -> dict[str, Any] | None:
        if not order_id and not client_oid:
            raise ValueError("order_id or client_oid is required")
        for row in self._history_rows(symbol):
            if (order_id and str(row.get("orderId") or "") == str(order_id)) or (
                client_oid and str(row.get("clientOrderId") or "") == str(client_oid)
            ):
                return self._normalize_standard_order(row, symbol)
        return None

    def fetch_order(self, order_id: str, symbol: str | None = None, params: dict[str, Any] | None = None):
        detail = self.fetch_order_detail(symbol or self.symbol, order_id=order_id)
        if detail is None:
            raise WeexAPIError("-1054", "WEEX order does not exist", request_rejected=True)
        return detail

    def fetch_history_trigger_order(
        self,
        symbol: str,
        order_id: str | None = None,
        client_oid: str | None = None,
        plan_type: str = "normal_plan",
    ) -> dict[str, Any] | None:
        if not order_id and not client_oid:
            raise ValueError("order_id or client_oid is required")
        payload = self._retry(
            "fetch WEEX conditional history",
            self._private_get,
            "/capi/v3/allAlgoOrders",
            {"symbol": symbol_to_weex_id(symbol), "limit": 1000},
        ) or {}
        rows = payload.get("orders") if isinstance(payload, dict) else payload
        want_tpsl = str(plan_type).lower() in {"pos_loss", "profit_loss", "tpsl"}
        for row in rows or []:
            if not isinstance(row, dict) or self._is_position_tpsl(row) != want_tpsl:
                continue
            if (order_id and str(row.get("algoId") or row.get("orderId") or "") == str(order_id)) or (
                client_oid and str(row.get("clientAlgoId") or row.get("clientOrderId") or "") == str(client_oid)
            ):
                return self._normalize_trigger_order(row, symbol)
        return None

    def create_order(
        self,
        symbol: str,
        type: str,
        side: str,
        amount: float,
        price: float | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        order_params = dict(params or {})
        reduce_only = bool(order_params.pop("reduceOnly", False))
        client_oid = normalize_client_id(
            order_params.pop("clientOid", order_params.pop("clientOrderId", ""))
        )
        side_token = str(side).upper()
        if side_token not in {"BUY", "SELL"}:
            raise ValueError(f"Unsupported WEEX side: {side}")
        order_type = str(type or "limit").upper()
        if order_type not in {"LIMIT", "MARKET"}:
            raise ValueError(f"Unsupported WEEX order type: {type}")
        position_side = "LONG" if ((side_token == "BUY") != reduce_only) else "SHORT"
        request: dict[str, Any] = {
            "symbol": symbol_to_weex_id(symbol),
            "side": side_token,
            "positionSide": position_side,
            "type": order_type,
            "quantity": self.amount_to_precision(symbol, amount),
            "newClientOrderId": client_oid,
        }
        if order_type == "LIMIT":
            if safe_float(price, 0.0) <= 0:
                raise ValueError("price is required for WEEX limit orders")
            request["timeInForce"] = str(order_params.pop("timeInForce", order_params.pop("force", "GTC"))).upper()
            request["price"] = self.price_to_precision(symbol, safe_float(price))
        data = self._private_post("/capi/v3/order", request) or {}
        return {
            "id": data.get("orderId"),
            "clientOrderId": data.get("clientOrderId") or client_oid,
            "symbol": symbol,
            "type": order_type.lower(),
            "side": side_token.lower(),
            "amount": amount,
            "price": price,
            "reduceOnly": reduce_only,
            "info": {**request, **data},
        }

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
        reduce_only = bool(order_params.pop("reduceOnly", False))
        client_oid = normalize_client_id(
            order_params.pop("clientOid", order_params.pop("clientOrderId", "")),
            "conditional client id",
        )
        normalized_trigger = safe_float(trigger_price, 0.0)
        if normalized_trigger <= 0:
            raise ValueError("trigger_price must be positive")
        side_token = str(side).upper()
        if side_token not in {"BUY", "SELL"}:
            raise ValueError(f"Unsupported WEEX side: {side}")
        position_side = "LONG" if ((side_token == "BUY") != reduce_only) else "SHORT"
        current = self.fetch_ticker(symbol).get("last") or 0.0
        stop_direction = normalized_trigger >= safe_float(current) if side_token == "BUY" else normalized_trigger <= safe_float(current)
        base_type = "STOP" if stop_direction else "TAKE_PROFIT"
        normalized_order_type = str(order_type or "limit").lower()
        algo_type = f"{base_type}_MARKET" if normalized_order_type == "market" else base_type
        request: dict[str, Any] = {
            "symbol": symbol_to_weex_id(symbol),
            "side": side_token,
            "positionSide": position_side,
            "type": algo_type,
            "quantity": self.amount_to_precision(symbol, amount),
            "triggerPrice": self.price_to_precision(symbol, normalized_trigger),
            "clientAlgoId": client_oid,
        }
        request["SlWorkingType" if base_type == "STOP" else "TpWorkingType"] = normalize_trigger_type(
            trigger_type
        )
        if normalized_order_type == "limit":
            if safe_float(price, 0.0) <= 0:
                raise ValueError("price is required for WEEX limit conditional orders")
            request["price"] = self.price_to_precision(symbol, safe_float(price))
        data = self._private_post("/capi/v3/algoOrder", request) or {}
        return {
            "id": data.get("orderId"),
            "clientOrderId": data.get("clientOrderId") or client_oid,
            "symbol": symbol,
            "type": "trigger",
            "planType": "normal_plan",
            "side": side_token.lower(),
            "price": price,
            "triggerPrice": normalized_trigger,
            "triggerType": trigger_type,
            "amount": amount,
            "info": {**request, **data},
        }

    @staticmethod
    def _assert_batch_success(data: Any, operation: str) -> dict[str, Any]:
        row = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else {}
        if not row or row.get("success") is False:
            raise WeexAPIError(
                row.get("errorCode") if isinstance(row, dict) else "",
                str((row or {}).get("errorMessage") or f"WEEX {operation} failed"),
                payload=data,
                request_rejected=True,
            )
        return row

    def place_position_stop_loss(
        self,
        symbol: str,
        hold_side: str,
        trigger_price: float,
        trigger_type: str = "mark_price",
        execute_price: float | None = 0.0,
        client_oid: str | None = None,
    ) -> dict[str, Any]:
        client_id = normalize_client_id(client_oid, "TPSL client id")
        request: dict[str, Any] = {
            "symbol": symbol_to_weex_id(symbol),
            "clientAlgoId": client_id,
            "planType": "STOP_LOSS",
            "triggerPrice": self.price_to_precision(symbol, trigger_price),
            "executePrice": self.price_to_precision(symbol, execute_price) if safe_float(execute_price) > 0 else "0",
            "quantity": "0",
            # The strategy's one-way abstraction calls a long position "buy"
            # and a short position "sell"; WEEX requires LONG/SHORT here.
            "positionSide": normalize_position_side(hold_side),
            "triggerPriceType": normalize_trigger_type(trigger_type),
        }
        row = self._assert_batch_success(
            self._private_post("/capi/v3/placeTpSlOrder", request),
            "place stop loss",
        )
        return {
            "id": row.get("orderId"),
            "clientOrderId": client_id,
            "symbol": symbol,
            "type": "pos_loss",
            "side": str(hold_side).lower(),
            "triggerPrice": trigger_price,
            "executePrice": execute_price,
            "info": row,
        }

    def fetch_position_stop_loss(
        self,
        symbol: str,
        order_id: str | None = None,
        client_oid: str | None = None,
    ) -> dict[str, Any] | None:
        if not order_id and not client_oid:
            raise ValueError("order_id or client_oid is required")
        for row in self.fetch_pending_trigger_orders(symbol, plan_type="pos_loss", limit=100):
            if (order_id and str(row.get("id") or "") == str(order_id)) or (
                client_oid and str(row.get("clientOrderId") or "") == str(client_oid)
            ):
                return row
        return self.fetch_history_trigger_order(
            symbol,
            order_id=order_id,
            client_oid=client_oid,
            plan_type="pos_loss",
        )

    def modify_tpsl_order(
        self,
        symbol: str,
        trigger_price: float,
        trigger_type: str = "mark_price",
        execute_price: float | None = 0.0,
        order_id: str | None = None,
        client_oid: str | None = None,
        size: Any = None,
        delegate_type: str = "2",
    ) -> dict[str, Any]:
        resolved_id = str(order_id or "")
        if not resolved_id and client_oid:
            existing = self.fetch_position_stop_loss(symbol, client_oid=client_oid)
            resolved_id = str((existing or {}).get("id") or "")
        if not resolved_id:
            raise ValueError("WEEX TPSL modification requires a resolvable order id")
        request = {
            "orderId": resolved_id,
            "triggerPrice": self.price_to_precision(symbol, trigger_price),
            "executePrice": self.price_to_precision(symbol, execute_price) if safe_float(execute_price) > 0 else "0",
            "triggerPriceType": normalize_trigger_type(trigger_type),
        }
        data = self._private_post("/capi/v3/modifyTpSlOrder", request) or {}
        if isinstance(data, dict) and data.get("success") is False:
            raise WeexAPIError("", "WEEX TPSL modification rejected", payload=data, request_rejected=True)
        return {
            "id": resolved_id,
            "clientOrderId": client_oid,
            "symbol": symbol,
            "type": "tpsl",
            "triggerPrice": trigger_price,
            "executePrice": execute_price,
            "info": data,
        }

    def cancel_order_by_reference(
        self,
        symbol: str,
        order_id: str | None = None,
        client_oid: str | None = None,
    ) -> dict[str, Any]:
        if not order_id and not client_oid:
            raise ValueError("order_id or client_oid is required")
        query = {"orderId": str(order_id)} if order_id else {"origClientOrderId": str(client_oid)}
        return self._private_delete("/capi/v3/order", query) or {}

    def cancel_trigger_order_by_reference(
        self,
        symbol: str,
        order_id: str | None = None,
        client_oid: str | None = None,
        plan_type: str = "normal_plan",
    ) -> dict[str, Any]:
        resolved_id = str(order_id or "")
        if not resolved_id and client_oid:
            pending = self.fetch_pending_trigger_orders(symbol, plan_type=plan_type, limit=100)
            match = next((row for row in pending if str(row.get("clientOrderId") or "") == str(client_oid)), None)
            resolved_id = str((match or {}).get("id") or "")
            if not resolved_id:
                history = self.fetch_history_trigger_order(symbol, client_oid=client_oid, plan_type=plan_type)
                if history and str(history.get("status") or "") in {"executed", "canceled"}:
                    return {"orderId": history.get("id"), "success": True, "terminal": True}
        if not resolved_id:
            raise WeexAPIError("-1054", "WEEX conditional order does not exist", request_rejected=True)
        return self._private_delete("/capi/v3/algoOrder", {"orderId": resolved_id}) or {}

    def cancel_position_stop_loss(
        self,
        symbol: str | None = None,
        order_id: str | None = None,
        client_oid: str | None = None,
    ) -> dict[str, Any]:
        target = symbol or self.symbol
        if order_id or client_oid:
            return self.cancel_trigger_order_by_reference(
                target,
                order_id=order_id,
                client_oid=client_oid,
                plan_type="pos_loss",
            )
        results = []
        for row in self.fetch_pending_trigger_orders(target, plan_type="pos_loss", limit=100):
            if row.get("id"):
                results.append(self._private_delete("/capi/v3/algoOrder", {"orderId": str(row["id"])}))
        return {"success": True, "results": results}

    def cancel_orders(self, orders: list[dict[str, Any]], symbol: str | None = None):
        target = symbol or self.symbol
        results = []
        for order in orders:
            if str(order.get("type") or "").lower() == "trigger":
                results.append(
                    self.cancel_trigger_order_by_reference(
                        target,
                        order_id=str(order.get("id") or "") or None,
                        client_oid=str(order.get("clientOrderId") or "") or None,
                    )
                )
            else:
                results.append(
                    self.cancel_order_by_reference(
                        target,
                        order_id=str(order.get("id") or "") or None,
                        client_oid=str(order.get("clientOrderId") or "") or None,
                    )
                )
        return results

    def cancel_all_orders(self, symbol: str | None = None, params: dict[str, Any] | None = None):
        target = symbol or self.symbol
        standard = self._private_delete(
            "/capi/v3/allOpenOrders",
            {"symbol": symbol_to_weex_id(target)},
        ) or []
        triggers = self.fetch_pending_trigger_orders(target, plan_type="normal_plan", limit=100)
        trigger_results = []
        for order in triggers:
            if order.get("id"):
                trigger_results.append(
                    self._private_delete("/capi/v3/algoOrder", {"orderId": str(order["id"])})
                )
        return {"standard": standard, "conditional": trigger_results}

    def _ticker_stats(self, symbol: str, force: bool = False) -> dict[str, Any]:
        market_id = symbol_to_weex_id(symbol)
        now = time.monotonic()
        cached = self._ticker_stats_cache.get(market_id)
        if cached and not force and now - cached[0] < self._ticker_stats_ttl:
            return dict(cached[1])
        rows = self._retry(
            "fetch WEEX 24h ticker",
            self._public_get,
            "/capi/v3/market/ticker/24hr",
            {"symbol": market_id},
        ) or []
        row = rows[0] if isinstance(rows, list) and rows else rows if isinstance(rows, dict) else {}
        self._ticker_stats_cache[market_id] = (now, dict(row or {}))
        return dict(row or {})

    def _wait_for_ws_ticker(self, timeout: float = 3.0) -> dict[str, Any] | None:
        if not self.ws.enabled or not self.ws.public_enabled:
            return None
        deadline = time.monotonic() + max(timeout, 0.0)
        while time.monotonic() < deadline:
            if self.ws.is_ticker_fresh(self.inst_id):
                ticker = self.ws.get_ticker(self.inst_id)
                if ticker:
                    return ticker
            time.sleep(0.2)
        if self.ws.is_ticker_fresh(self.inst_id):
            return self.ws.get_ticker(self.inst_id)
        return None

    def fetch_ticker(self, symbol: str, params: dict[str, Any] | None = None):
        force = bool((params or {}).get("_force_rest", False))
        market_id = symbol_to_weex_id(symbol)
        ticker = None if force else self._wait_for_ws_ticker(timeout=3.0)
        if ticker and safe_float(ticker.get("lastPr"), 0.0) > 0:
            return {
                "symbol": symbol,
                "last": safe_float(ticker.get("lastPr"), 0.0),
                "bid": safe_float(ticker.get("bidPr"), 0.0),
                "ask": safe_float(ticker.get("askPr"), 0.0),
                "high": safe_float(ticker.get("high24h"), 0.0),
                "low": safe_float(ticker.get("low24h"), 0.0),
                "percentage": safe_float(ticker.get("change24h"), 0.0),
                "baseVolume": safe_float(ticker.get("baseVolume"), 0.0),
                "quoteVolume": safe_float(ticker.get("quoteVolume"), 0.0),
                "bidSize": safe_float(ticker.get("bidSz"), 0.0),
                "askSize": safe_float(ticker.get("askSz"), 0.0),
                "mark": safe_float(ticker.get("markPrice"), 0.0),
                "timestamp": int(safe_float(ticker.get("ts"), 0.0)),
                "info": ticker,
            }
        book_rows = self._retry(
            "fetch WEEX best bid/ask",
            self._public_get,
            "/capi/v3/market/ticker/bookTicker",
            {"symbol": market_id},
        ) or []
        book = book_rows[0] if isinstance(book_rows, list) and book_rows else book_rows if isinstance(book_rows, dict) else {}
        mark = self._mark_price(symbol)
        stats = self._ticker_stats(symbol, force=force)
        bid = safe_float(book.get("bidPrice"), 0.0)
        ask = safe_float(book.get("askPrice"), 0.0)
        midpoint = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
        last = midpoint or safe_float(stats.get("lastPrice"), 0.0) or mark
        return {
            "symbol": symbol,
            "last": last,
            "bid": bid,
            "ask": ask,
            "high": safe_float(stats.get("highPrice"), 0.0),
            "low": safe_float(stats.get("lowPrice"), 0.0),
            "percentage": safe_float(stats.get("priceChangePercent"), 0.0),
            "baseVolume": safe_float(stats.get("volume"), 0.0),
            "quoteVolume": safe_float(stats.get("quoteVolume"), 0.0),
            "bidSize": safe_float(book.get("bidQty"), 0.0),
            "askSize": safe_float(book.get("askQty"), 0.0),
            "mark": mark,
            "timestamp": int(safe_float(book.get("time") or stats.get("closeTime"), 0.0)),
            "info": {"book": book, "stats": stats, "markPrice": mark},
        }

    def fetch_order_book(
        self,
        symbol: str,
        limit: int = 50,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        requested = max(int(limit or 50), 1)
        api_limit = 15 if requested <= 15 else 200
        row = self._retry(
            "fetch WEEX order book",
            self._public_get,
            "/capi/v3/market/depth",
            {"symbol": symbol_to_weex_id(symbol), "limit": api_limit},
        ) or {}

        def levels(values: Any) -> list[list[float]]:
            result = []
            for value in values or []:
                if isinstance(value, (list, tuple)) and len(value) >= 2:
                    price = safe_float(value[0], 0.0)
                    amount = safe_float(value[1], 0.0)
                    if price > 0 and amount > 0:
                        result.append([price, amount])
            return result[:requested]

        return {
            "symbol": symbol,
            "bids": levels(row.get("bids")),
            "asks": levels(row.get("asks")),
            "timestamp": None,
            "datetime": None,
            "nonce": row.get("lastUpdateId"),
            "info": row,
        }

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[list[float]]:
        interval = str(timeframe or "1m")
        if interval not in WEEX_WS_TIMEFRAMES:
            raise ValueError(f"Unsupported WEEX kline interval: {interval}")
        desired = min(max(int(limit or 100), 1), 1000)
        force_rest = bool((params or {}).get("_force_rest", False))
        if not force_rest and self.ws.is_fresh("public"):
            ws_rows = self.ws.get_candles(interval)
            if ws_rows:
                seed = self._seed_ohlcv(symbol, interval, max(desired, 200))
                merged = {int(row[0]): list(row[:6]) for row in seed}
                for row in ws_rows:
                    merged[int(row[0])] = list(row[:6])
                candles = sorted(merged.values(), key=lambda item: item[0])
                if since is not None:
                    candles = [row for row in candles if int(row[0]) >= int(since)]
                return candles[-desired:]
        return self._fetch_ohlcv_rest(symbol, interval, desired, since)

    def _fetch_ohlcv_rest(
        self,
        symbol: str,
        interval: str,
        limit: int,
        since: int | None = None,
    ) -> list[list[float]]:
        rows = self._retry(
            "fetch WEEX OHLCV",
            self._public_get,
            "/capi/v3/market/klines",
            {
                "symbol": symbol_to_weex_id(symbol),
                "interval": interval,
                "limit": min(max(int(limit), 1), 1000),
            },
        ) or []
        candles = []
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) < 6:
                continue
            candle = normalize_ohlcv_row(*row[:6])
            if candle is None:
                continue
            timestamp = int(candle[0])
            if since is not None and timestamp < since:
                continue
            candles.append(candle)
        candles.sort(key=lambda item: item[0])
        return candles

    def _seed_ohlcv(self, symbol: str, interval: str, limit: int) -> list[list[float]]:
        now = time.monotonic()
        cached = self._ohlcv_seed.get(interval)
        cached_at = self._ohlcv_seed_at.get(interval, 0.0)
        ttl = max(30.0, min(self._timeframe_seconds(interval), 300.0))
        if cached and len(cached) >= limit and now - cached_at < ttl:
            return cached
        rows = self._fetch_ohlcv_rest(symbol, interval, limit)
        self._ohlcv_seed[interval] = rows
        self._ohlcv_seed_at[interval] = now
        return rows

    @staticmethod
    def _timeframe_seconds(timeframe: str) -> float:
        value = str(timeframe)
        if value.endswith("m"):
            return safe_float(value[:-1], 1.0) * 60.0
        if value.endswith("h"):
            return safe_float(value[:-1], 1.0) * 3600.0
        if value.endswith("d"):
            return safe_float(value[:-1], 1.0) * 86400.0
        if value.endswith("w"):
            return safe_float(value[:-1], 1.0) * 604800.0
        if value.endswith("M"):
            return safe_float(value[:-1], 1.0) * 2592000.0
        return 60.0

    def fetch_my_trades(
        self,
        symbol: str | None = None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        target = symbol or self.symbol
        requested_limit = min(max(int(limit or 100), 1), 100)
        ws_rows = self._map_ws_fills(target, requested_limit)
        query: dict[str, Any] = {
            "symbol": symbol_to_weex_id(target),
            "limit": requested_limit,
        }
        if since is not None:
            query["startTime"] = int(since)
            query["endTime"] = min(int(time.time() * 1000), int(since) + 7 * 24 * 60 * 60 * 1000)
        try:
            rows = self._retry(
                "fetch WEEX trades",
                self._private_get,
                "/capi/v3/userTrades",
                query,
            ) or []
        except Exception:
            if ws_rows:
                return ws_rows[-requested_limit:]
            raise
        result: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            price = safe_float(row.get("price"), 0.0)
            amount = safe_float(row.get("qty"), 0.0)
            realized = safe_float(row.get("realizedPnl"), 0.0)
            position_side = str(row.get("positionSide") or "").upper()
            order_side = str(row.get("side") or "").upper()
            reduce_only = (position_side == "LONG" and order_side == "SELL") or (
                position_side == "SHORT" and order_side == "BUY"
            )
            result.append(
                {
                    "id": row.get("id"),
                    "order": row.get("orderId"),
                    "timestamp": int(safe_float(row.get("time"), 0.0)),
                    "datetime": None,
                    "symbol": target,
                    "side": order_side.lower(),
                    "takerOrMaker": "maker" if bool(row.get("maker")) else "taker",
                    "price": price,
                    "amount": amount,
                    "cost": safe_float(row.get("quoteQty"), amount * price),
                    "fee": {
                        "cost": abs(safe_float(row.get("commission"), 0.0)),
                        "currency": row.get("commissionAsset"),
                    },
                    "info": {**row, "realizedPnl": realized, "reduceOnly": reduce_only},
                }
            )
        result.sort(key=lambda item: item.get("timestamp", 0))
        return self._merge_trades(result, ws_rows, requested_limit)

    def _map_ws_fills(self, symbol: str, limit: int) -> list[dict[str, Any]]:
        result = []
        for row in self.ws.get_fills(symbol_to_weex_id(symbol), limit=limit * 2):
            amount = safe_float(row.get("fillSize"), 0.0)
            cost = safe_float(row.get("fillValue"), 0.0)
            price = cost / amount if amount > 0 else 0.0
            position_side = str(row.get("positionSide") or "").upper()
            order_side = str(row.get("orderSide") or "").upper()
            reduce_only = (position_side == "LONG" and order_side == "SELL") or (
                position_side == "SHORT" and order_side == "BUY"
            )
            result.append(
                {
                    "id": row.get("id"),
                    "order": row.get("orderId"),
                    "timestamp": int(safe_float(row.get("createdTime") or row.get("updatedTime"), 0.0)),
                    "datetime": None,
                    "symbol": symbol,
                    "side": order_side.lower(),
                    "takerOrMaker": str(row.get("direction") or "").lower(),
                    "price": price,
                    "amount": amount,
                    "cost": cost or amount * price,
                    "fee": {
                        "cost": abs(safe_float(row.get("fillFee"), 0.0)),
                        "currency": row.get("coin"),
                    },
                    "info": {
                        **row,
                        "realizedPnl": safe_float(row.get("realizePnl"), 0.0),
                        "reduceOnly": reduce_only,
                    },
                }
            )
        result.sort(key=lambda item: item.get("timestamp", 0))
        return result[-limit:]

    @staticmethod
    def _merge_trades(
        rest_rows: list[dict[str, Any]],
        ws_rows: list[dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for trade in [*rest_rows, *ws_rows]:
            key = str(trade.get("id") or trade.get("order") or trade.get("timestamp"))
            merged[key] = trade
        rows = list(merged.values())
        rows.sort(key=lambda item: safe_float(item.get("timestamp"), 0.0))
        return rows[-limit:]

    def fetch_ledger(
        self,
        code: str | None = None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        requested = max(int(limit or 100), 1)
        max_pages = max(int((params or {}).get("maxPages") or self.config.get("ledgerHistoryMaxPages", 100)), 1)
        cursor_id = None
        cursor_time = None
        raw_rows: list[dict[str, Any]] = []
        for _ in range(max_pages):
            payload: dict[str, Any] = {"asset": code or self.margin_coin, "limit": min(requested - len(raw_rows), 100)}
            if since is not None:
                payload["startTime"] = int(since)
            if cursor_id is not None:
                payload["nextKeyId"] = cursor_id
                payload["nextKeyTime"] = cursor_time
            page = self._retry(
                "fetch WEEX account income",
                self._private_post,
                "/capi/v3/account/income",
                payload,
            ) or {}
            items = page.get("items") if isinstance(page, dict) else []
            valid = [row for row in items or [] if isinstance(row, dict)]
            raw_rows.extend(valid)
            if len(raw_rows) >= requested or not bool((page or {}).get("hasNextPage")) or not valid:
                break
            next_key = page.get("nextKey") or {}
            cursor_id = next_key.get("nextKeyId")
            cursor_time = next_key.get("nextKeyTime")
            if cursor_id is None:
                break
        result = []
        for row in raw_rows[:requested]:
            result.append(
                {
                    "id": row.get("billId"),
                    "timestamp": int(safe_float(row.get("time"), 0.0)),
                    "datetime": None,
                    "currency": row.get("asset"),
                    "amount": safe_float(row.get("income"), 0.0),
                    "before": None,
                    "after": safe_float(row.get("balance"), 0.0),
                    "type": row.get("incomeType"),
                    "fee": {
                        "cost": abs(safe_float(row.get("fillFee"), 0.0)),
                        "currency": row.get("asset"),
                    },
                    "info": row,
                }
            )
        return result
