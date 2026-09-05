"""Gate-specific order-scoped fill and protective-stop safety extensions.

The core strategy intentionally fails closed when its generic recent-trade window
is full. Gate supports filtering personal futures trades by exact order ID, so
we can replace that ambiguous account-wide window with a complete order-scoped
window whenever the bot already knows the server order ID.

Gate also requires a newly-created downside/upside trigger to sit on the safe
side of the current reference price.  The core keeps a 0.1 percentage-point ROI
buffer for that reason, but historically treated the configured minimum locked
profit as an additional arming gate.  Around the minimum activation threshold
that left an activated strategy without a server-side protective order.  This
mixin keeps the Gate buffer while allowing the first protective order to lock
whatever positive profit is currently safe to place; later updates remain
monotonic and can tighten the stop normally.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


class GateOrderTradeSafetyMixin:
    """Add Gate-specific fill verification and protective-stop arming safety."""

    _gate_trade_window_override: Optional[List[Dict[str, Any]]] = None

    def _is_gate_exchange(self) -> bool:
        exchange_name = str(getattr(self.exchange, "name", "") or "").strip().lower()
        return exchange_name in {"gate.io", "gateio", "gate"}

    def _is_gate_order_trade_query_available(self) -> bool:
        return self._is_gate_exchange() and callable(
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

    def _gate_protective_stop_arm_buffer_pct(self) -> float:
        config = getattr(self, "config", {}) or {}
        return max(
            self._safe_float(
                config.get("gate_protective_stop_arm_buffer_pct", 0.001),
                0.001,
            ),
            0.0,
        )

    def _protective_stop_target(
        self,
        position: Dict[str, Any],
        current_profit_pct: Optional[float] = None,
        best_profit_pct: Optional[float] = None,
        trail_ratio: Optional[float] = None,
    ) -> Optional[Dict[str, float]]:
        """Arm Gate protection immediately once trailing TP is activated.

        The core intentionally subtracts 0.1 percentage points from current ROI
        so a Gate trigger is not submitted at/through the current mark price.  Its
        old minimum-profit check could then return ``None`` at the exact activation
        threshold (for example 0.50% current ROI -> 0.40% safely lockable ROI while
        the configured minimum is 0.50%).  For Gate only, retain the placement
        buffer but permit that first stop to lock the smaller positive amount.

        Existing stops are never loosened by this method: the core
        ``_should_update_protective_stop`` gate still accepts only tighter prices.
        """
        target = super()._protective_stop_target(
            position,
            current_profit_pct=current_profit_pct,
            best_profit_pct=best_profit_pct,
            trail_ratio=trail_ratio,
        )
        if target is not None or not self._is_gate_exchange() or current_profit_pct is None:
            return target

        current_profit = self._safe_float(current_profit_pct, 0.0)
        arm_buffer = self._gate_protective_stop_arm_buffer_pct()
        lockable_profit = max(current_profit - arm_buffer, 0.0)
        if lockable_profit <= 0:
            return None

        # Ask the core for the otherwise-desired target without the live-price cap.
        # If that also fails, the missing entry/best-profit prerequisites are real
        # and must remain fail-closed.
        desired = super()._protective_stop_target(
            position,
            current_profit_pct=None,
            best_profit_pct=best_profit_pct,
            trail_ratio=trail_ratio,
        )
        if not desired:
            return None

        locked_profit = min(
            self._safe_float(desired.get("locked_profit_pct"), 0.0),
            lockable_profit,
        )
        entry_price = self._safe_float(desired.get("entry_price"), 0.0)
        leverage = max(self._safe_float(desired.get("leverage"), 1.0), 1.0)
        side = str(desired.get("side") or "").lower()
        if locked_profit <= 0 or entry_price <= 0 or side not in {"long", "short"}:
            return None

        raw_move_ratio = locked_profit / leverage
        trigger_price = (
            entry_price * (1 - raw_move_ratio)
            if side == "short"
            else entry_price * (1 + raw_move_ratio)
        )
        relaxed = dict(desired)
        relaxed["locked_profit_pct"] = locked_profit
        relaxed["trigger_price"] = trigger_price
        relaxed["gate_arm_buffer_pct"] = arm_buffer
        return relaxed

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
