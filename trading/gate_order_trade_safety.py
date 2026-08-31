"""Gate-specific order-scoped fill verification for the martingale bot.

The core strategy intentionally fails closed when its generic recent-trade window
is full. Gate supports filtering personal futures trades by exact order ID, so
we can replace that ambiguous account-wide window with a complete order-scoped
window whenever the bot already knows the server order ID.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


class GateOrderTradeSafetyMixin:
    """Use Gate's ``my_trades?order=...`` before falling back to broad history."""

    _gate_trade_window_override: Optional[List[Dict[str, Any]]] = None

    def _is_gate_order_trade_query_available(self) -> bool:
        exchange_name = str(getattr(self.exchange, "name", "") or "").strip().lower()
        return exchange_name in {"gate.io", "gateio", "gate"} and callable(
            getattr(self.exchange, "fetch_my_trades", None)
        )

    @staticmethod
    def _trade_order_id(trade: Dict[str, Any]) -> str:
        info = trade.get("info") or {}
        return str(
            trade.get("order")
            or trade.get("orderId")
            or info.get("order_id")
            or info.get("orderId")
            or ""
        ).strip()

    def _fetch_gate_order_trades_complete(
        self,
        order_id: str,
        *,
        page_size: int = 100,
        max_pages: int = 100,
    ) -> List[Dict[str, Any]]:
        """Fetch every visible fill for one Gate futures order.

        Gate's order filter removes the ambiguity that caused the old 60-trade
        account-wide window to deadlock. Pagination remains fail-closed: if the
        configured page cap is exhausted, raise instead of claiming completeness.
        """
        expected_id = str(order_id or "").strip()
        if not expected_id:
            raise ValueError("order_id is required")
        if not self._is_gate_order_trade_query_available():
            raise RuntimeError("Gate order-scoped trade query is unavailable")

        order_param: Any = int(expected_id) if expected_id.isdigit() else expected_id
        collected: List[Dict[str, Any]] = []
        seen_trade_ids = set()
        offset = 0

        for _ in range(max_pages):
            page = self.exchange.fetch_my_trades(
                self.symbol,
                limit=page_size,
                params={"order": order_param, "offset": offset},
            ) or []
            for trade in page:
                trade_order_id = self._trade_order_id(trade)
                if trade_order_id and trade_order_id != expected_id:
                    raise RuntimeError(
                        "Gate order-scoped trade query returned a different order: "
                        f"expected={expected_id}, got={trade_order_id}"
                    )
                info = trade.get("info") or {}
                trade_id = str(
                    trade.get("id")
                    or info.get("trade_id")
                    or info.get("tradeId")
                    or info.get("id")
                    or ""
                ).strip()
                if trade_id and trade_id in seen_trade_ids:
                    continue
                if trade_id:
                    seen_trade_ids.add(trade_id)
                collected.append(trade)

            if len(page) < page_size:
                return collected
            offset += len(page)

        raise RuntimeError(
            f"Gate order {expected_id} fill history exceeded safe pagination cap"
        )

    def _fetch_authoritative_my_trades(self, limit: int) -> List[Dict[str, Any]]:
        override = getattr(self, "_gate_trade_window_override", None)
        if override is not None:
            return list(override)
        return super()._fetch_authoritative_my_trades(limit)

    def _query_latest_entry_fill(
        self,
        position: Optional[Dict[str, Any]],
        preferred_order_id: Optional[str] = None,
        preferred_client_oid: Optional[str] = None,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        preferred_id = str(preferred_order_id or "").strip()
        if preferred_id and self._is_gate_order_trade_query_available():
            try:
                trades = self._fetch_gate_order_trades_complete(preferred_id)
            except Exception as exc:
                print(
                    f"⚠️ Gate 按订单查询成交失败(orderId={preferred_id})，"
                    f"回退原安全校验: {exc}"
                )
            else:
                # The core logic treats a full 60-row account-wide window as
                # incomplete. This window is exact-order and fully paginated, so
                # an empty result is authoritative proof of zero fills. For a
                # heavily fragmented order (>=60 fills), retain fail-closed
                # behavior rather than weakening the core partial-fill checks.
                if len(trades) >= 60:
                    print(
                        f"⚠️ Gate 订单 {preferred_id} 存在 {len(trades)} 条及以上成交，"
                        "保持严格模式并回退原成交聚合校验"
                    )
                else:
                    self._gate_trade_window_override = list(trades)
                    try:
                        return super()._query_latest_entry_fill(
                            position,
                            preferred_order_id=preferred_id,
                            preferred_client_oid=preferred_client_oid,
                        )
                    finally:
                        self._gate_trade_window_override = None

        return super()._query_latest_entry_fill(
            position,
            preferred_order_id=preferred_order_id,
            preferred_client_oid=preferred_client_oid,
        )

    def _entry_order_fill_barrier(self, order_ids: List[str]) -> Optional[str]:
        expected_ids = [
            str(order_id).strip() for order_id in order_ids if str(order_id).strip()
        ]
        if not expected_ids or not self._is_gate_order_trade_query_available():
            return super()._entry_order_fill_barrier(order_ids)

        try:
            for order_id in expected_ids:
                trades = self._fetch_gate_order_trades_complete(order_id)
                for trade in trades:
                    info = trade.get("info") or {}
                    amount = self._safe_float(
                        trade.get(
                            "amount",
                            info.get("size", info.get("baseVolume", 0.0)),
                        ),
                        0.0,
                    )
                    if amount > 0:
                        # Exact order-scoped history proves a boundary fill.
                        return order_id
        except Exception as exc:
            print(
                "⚠️ Gate 按订单核对撤单边界成交失败，"
                f"保持 fail-closed 并回退原安全校验: {exc}"
            )
            return super()._entry_order_fill_barrier(order_ids)

        # Every target order was queried directly and returned zero fills. Feed an
        # authoritative empty trade window into the existing terminal-order checks,
        # which still require canceled/expired/rejected state before allowing rehang.
        self._gate_trade_window_override = []
        try:
            return super()._entry_order_fill_barrier(order_ids)
        finally:
            self._gate_trade_window_override = None
