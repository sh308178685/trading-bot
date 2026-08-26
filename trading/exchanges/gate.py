"""Gate.io USDT perpetual-futures adapter.

The strategy uses base-asset quantities while Gate's futures API uses contract
counts.  This adapter keeps that exchange-specific detail out of the strategy.
"""

from __future__ import annotations

import contextlib
from copy import deepcopy
from decimal import Decimal, ROUND_DOWN
from typing import Any

import ccxt

from .base import ExchangeAdapter


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


class GateExchangeAdapter(ExchangeAdapter):
    """REST adapter for Gate API v4 USDT perpetual futures."""

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.symbol = str(config.get("symbol", "ETH/USDT:USDT"))
        # Gate v4 path parameters are lowercase (``/futures/usdt/...``).
        self.settle = str(config.get("settleCoin", "USDT")).lower()
        self.margin_mode = str(config.get("marginMode", "cross")).lower()
        if self.margin_mode == "crossed":
            self.margin_mode = "cross"
        self.rest = self._create_rest_client()
        self._markets: dict[str, dict[str, Any]] = {}

    @property
    def name(self) -> str:
        return "Gate.io"

    def __getattr__(self, name: str) -> Any:
        return getattr(self.rest, name)

    def _create_rest_client(self) -> ccxt.gate:
        client = ccxt.gate({
            "apiKey": self.config.get("apiKey", ""),
            "secret": self.config.get("secretKey", self.config.get("secret", "")),
            "enableRateLimit": True,
            "options": {"defaultType": self.config.get("defaultType", "swap"), "defaultMarginMode": self.margin_mode},
        })
        if self.config.get("sandbox", True):
            client.set_sandbox_mode(True)
            # Gate's documented testnet REST host.  CCXT 4.4.92 still points
            # futures requests at the retired fx-api-testnet.gateio.ws host.
            testnet_url = "https://api-testnet.gateapi.io/api/v4"
            client.urls["api"]["public"]["futures"] = testnet_url
            client.urls["api"]["private"]["futures"] = testnet_url
        # Required by current Gate futures API versions for consistent size parsing.
        client.headers["X-Gate-Size-Decimal"] = "1"
        return client

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.rest.close()

    def ws_status(self) -> dict[str, Any]:
        return {"enabled": False, "transport": "rest", "exchange": self.name}

    def load_markets(self, reload: bool = False, params: dict[str, Any] | None = None):
        if reload or not self.rest.markets:
            rows = self.rest.publicFuturesGetSettleContracts({"settle": self.settle, **(params or {})})
            self.rest.set_markets([self.rest.parse_contract_market(row, self.settle) for row in rows])
        if reload:
            self._markets.clear()
        return {symbol: self.market(symbol) for symbol in self.rest.markets}

    def market(self, symbol: str) -> dict[str, Any]:
        if symbol not in self._markets:
            raw = deepcopy(self.rest.market(symbol))
            contract_size = _number(raw.get("contractSize"), 1.0)
            # Strategy quantities are base units, not Gate contract counts.
            raw["precision"]["amount"] = _number(raw["precision"].get("amount"), 1.0) * contract_size
            raw["limits"]["amount"] = {
                key: (_number(value) * contract_size if value is not None else None)
                for key, value in raw["limits"].get("amount", {}).items()
            }
            raw["baseAmountContractSize"] = contract_size
            self._markets[symbol] = raw
        return self._markets[symbol]

    def _contract_size(self, symbol: str) -> float:
        return _number(self.market(symbol).get("baseAmountContractSize"), 1.0)

    def _contracts(self, symbol: str, amount: float) -> float:
        size = self._contract_size(symbol)
        if size <= 0:
            raise ValueError(f"Invalid Gate contract size for {symbol}")
        return float((Decimal(str(amount)) / Decimal(str(size))).to_integral_value(rounding=ROUND_DOWN))

    def _base_amount(self, symbol: str, contracts: Any) -> float:
        return abs(_number(contracts)) * self._contract_size(symbol)

    def amount_to_precision(self, symbol: str, amount: float) -> str:
        contracts = self._contracts(symbol, amount)
        return format(Decimal(str(contracts)) * Decimal(str(self._contract_size(symbol))), "f")

    def price_to_precision(self, symbol: str, price: float) -> str:
        return self.rest.price_to_precision(symbol, price)

    def set_leverage(self, leverage: int, symbol: str, params: dict[str, Any] | None = None):
        """Set leverage with Gate API v4's explicit cross-margin fields."""
        self.load_markets()
        market = self.rest.market(symbol)
        request: dict[str, Any] = {
            "settle": market["settleId"],
            "contract": market["id"],
        }
        if self.margin_mode == "cross":
            # Gate requires leverage=0 together with cross_leverage_limit.
            request.update({"leverage": "0", "cross_leverage_limit": str(leverage)})
        else:
            request["leverage"] = str(leverage)
        # ``marginMode`` is not an API parameter and makes Gate testnet return
        # a misleading INTERNAL error when it is passed through CCXT.
        request.update({key: value for key, value in (params or {}).items() if key == "pid"})
        return self.rest.privateFuturesPostSettlePositionsContractLeverage(request)

    def fetch_balance(self, params: dict[str, Any] | None = None):
        return self.rest.fetch_balance({"type": "swap", "settle": self.settle, **(params or {})})

    def fetch_positions(self, symbols: list[str] | None = None, params: dict[str, Any] | None = None):
        rows = self.rest.fetch_positions(symbols or [self.symbol], {"settle": self.settle, **(params or {})})
        result = []
        for row in rows:
            symbol = row.get("symbol") or self.symbol
            if symbol not in self.rest.markets:
                continue
            item = dict(row)
            item["contracts"] = self._base_amount(symbol, row.get("contracts"))
            entry, mark, pnl = _number(row.get("entryPrice")), _number(row.get("markPrice")), _number(row.get("unrealizedPnl"))
            margin = _number(row.get("collateral"))
            if margin > 0:
                item["percentage"] = pnl / margin * 100
            elif entry > 0 and mark > 0:
                direction = -1 if str(row.get("side", "")).lower() == "short" else 1
                item["percentage"] = direction * (mark - entry) / entry * _number(row.get("leverage"), 1) * 100
            result.append(item)
        return result

    def _map_order(self, row: dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        symbol = item.get("symbol") or self.symbol
        for field in ("amount", "filled", "remaining"):
            if item.get(field) is not None:
                item[field] = self._base_amount(symbol, item[field])
        if item.get("triggerPrice") is not None or (item.get("info") or {}).get("trigger"):
            item["type"] = "trigger"
        return item

    def fetch_open_orders(self, symbol: str | None = None, since: int | None = None, limit: int | None = None, params: dict[str, Any] | None = None):
        options = {"settle": self.settle, **(params or {})}
        regular = self.rest.fetch_open_orders(symbol or self.symbol, since, limit, options)
        trigger = self.rest.fetch_open_orders(symbol or self.symbol, since, limit, {**options, "trigger": True})
        return [self._map_order(row) for row in regular + trigger]

    def create_order(self, symbol: str, type: str, side: str, amount: float, price: float | None = None, params: dict[str, Any] | None = None):
        options = {"settle": self.settle, "marginMode": self.margin_mode}
        options.update(params or {})
        return self._map_order(self.rest.create_order(symbol, type, side, self._contracts(symbol, amount), price, options))

    def cancel_all_orders(self, symbol: str | None = None, params: dict[str, Any] | None = None):
        options = {"settle": self.settle, **(params or {})}
        normal = self.rest.cancel_all_orders(symbol or self.symbol, options)
        trigger = self.rest.cancel_all_orders(symbol or self.symbol, {**options, "trigger": True})
        return normal + trigger

    def cancel_orders(self, orders: list[dict[str, Any]], symbol: str | None = None):
        return [
            self.rest.cancel_order(str(order["id"]), symbol or self.symbol, {"settle": self.settle, "trigger": order.get("type") == "trigger"})
            for order in orders if order.get("id")
        ]

    def fetch_order(self, id: str, symbol: str | None = None, params: dict[str, Any] | None = None):
        return self._map_order(self.rest.fetch_order(id, symbol or self.symbol, {"settle": self.settle, **(params or {})}))

    def create_trigger_order(self, symbol: str, side: str, amount: float, trigger_price: float, price: float | None = None, trigger_type: str = "mark_price", order_type: str = "limit", params: dict[str, Any] | None = None) -> dict[str, Any]:
        options = {"settle": self.settle, "price_type": {"last_price": 0, "mark_price": 1, "index_price": 2}.get(trigger_type, 1)}
        options.update(params or {})
        # CCXT maps stopLossPrice to Gate's /price_orders endpoint and derives the correct rule from side.
        response = self.rest.create_order(symbol, order_type, side, self._contracts(symbol, amount), price, {**options, "stopLossPrice": trigger_price})
        return self._map_order(response)

    def place_position_stop_loss(self, symbol: str, hold_side: str, trigger_price: float, trigger_type: str = "mark_price", execute_price: float | None = 0.0, client_oid: str | None = None) -> dict[str, Any]:
        positions = self.fetch_positions([symbol])
        position = next((row for row in positions if _number(row.get("contracts")) > 0 and row.get("side") == hold_side), None)
        if not position:
            raise RuntimeError("Cannot place Gate stop loss without an active position")
        side = "sell" if hold_side == "long" else "buy"
        options: dict[str, Any] = {"reduceOnly": True}
        if client_oid:
            options["clientOrderId"] = client_oid
        return self.create_trigger_order(symbol, side, _number(position["contracts"]), trigger_price, execute_price or None, trigger_type, "market" if not execute_price else "limit", options)

    def modify_tpsl_order(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        # The strategy already cancels and recreates on modification failure. Gate's amend endpoint has a
        # different payload shape, so fail explicitly to take that safe, tested path.
        raise NotImplementedError("Gate conditional orders are replaced by cancel-and-recreate")

    def cancel_position_stop_loss(self, symbol: str | None = None, order_id: str | None = None, client_oid: str | None = None) -> dict[str, Any]:
        if not order_id:
            return {}
        return self.rest.cancel_order(order_id, symbol or self.symbol, {"settle": self.settle, "trigger": True})

    def fetch_ticker(self, symbol: str, params: dict[str, Any] | None = None):
        return self.rest.fetch_ticker(symbol, {"settle": self.settle, **(params or {})})

    def fetch_ohlcv(self, symbol: str, timeframe: str = "1m", since: int | None = None, limit: int | None = None, params: dict[str, Any] | None = None):
        return self.rest.fetch_ohlcv(symbol, timeframe, since, limit, {"settle": self.settle, **(params or {})})

    def fetch_my_trades(self, symbol: str | None = None, since: int | None = None, limit: int | None = None, params: dict[str, Any] | None = None):
        rows = self.rest.fetch_my_trades(symbol or self.symbol, since, limit, {"settle": self.settle, **(params or {})})
        return [self._map_order(row) for row in rows]

    def fetch_ledger(self, code: str | None = None, since: int | None = None, limit: int | None = None, params: dict[str, Any] | None = None):
        return self.rest.fetch_ledger(code, since, limit, {"settle": self.settle, **(params or {})})
