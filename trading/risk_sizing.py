"""Dynamic first-order sizing for the Gate risk overlay.

The core GateRiskMixin owns stop placement, exits and persistent safety state.
This mixin only replaces the fixed first-order percentage with fixed-equity-risk
sizing: quantity is derived from the ATR stop distance, with a hard margin cap.
"""
from __future__ import annotations

import time

from trading.risk_guard import number


POSITION_DEFAULTS = {
    'risk_position_loss_pct': 0.01,
    'risk_max_margin_ratio': 0.20,
    'risk_position_max_notional_ratio': 0.60,
}


class GateDynamicSizingMixin:
    """Size Gate first orders from stop risk instead of a fixed margin percent."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self._risk_on():
            return

        values = {}
        for key, default in POSITION_DEFAULTS.items():
            value = number(self.config.get(key, default), None)
            if value is None or value <= 0:
                raise ValueError(f'{key} must be finite and positive')
            values[key] = value
        if values['risk_position_loss_pct'] >= 1:
            raise ValueError('risk_position_loss_pct must be below 1')
        if values['risk_max_margin_ratio'] >= 1:
            raise ValueError('risk_max_margin_ratio must be below 1')
        if values['risk_position_max_notional_ratio'] > 1:
            raise ValueError('risk_position_max_notional_ratio must be at most 1')

        self.risk_position_loss_pct = values['risk_position_loss_pct']
        self.risk_max_margin_ratio = values['risk_max_margin_ratio']
        self.risk_position_max_notional_ratio = values['risk_position_max_notional_ratio']

        self.risk['risk_trade_loss_pct'] = self.risk_position_loss_pct
        self.risk['risk_max_notional_ratio'] = min(
            self.risk_position_max_notional_ratio,
            self.risk_max_margin_ratio * max(number(getattr(self, 'leverage', self.config.get('leverage', 3)), 1.0), 1.0),
        )
        self._risk_last_sizing = None

    def _position_risk_loss_pct(self):
        return number(
            getattr(self, 'risk_position_loss_pct', self.risk.get('risk_trade_loss_pct', 0.01)),
            0.01,
        )

    def _position_max_margin_ratio(self):
        return number(
            getattr(self, 'risk_max_margin_ratio', self.risk.get('risk_first_margin_ratio', 0.025)),
            0.025,
        )

    def _position_max_notional_ratio(self):
        return number(
            getattr(self, 'risk_position_max_notional_ratio', self.risk.get('risk_max_notional_ratio', 0.50)),
            0.50,
        )

    def _risk_stop_distance_for_sizing(self, entry_price, trend=None):
        price = number(entry_price, None)
        if price is None or price <= 0:
            return None
        trend = self._risk_trend() if trend is None else trend
        atr = number((trend or {}).get('atr'), None)
        if atr is None or atr <= 0:
            return None
        return min(
            self.risk['risk_stop_max_pct'],
            max(
                self.risk['risk_stop_min_pct'],
                atr / price * self.risk['risk_stop_atr_multiplier'],
            ),
        )

    def _risk_entry_size(self, entry_price, equity=None, trend=None):
        price = number(entry_price, None)
        equity = number(self._risk_equity() if equity is None else equity, None)
        leverage = max(number(self.leverage, 1.0), 1.0)
        if price is None or price <= 0 or equity is None or equity <= 0:
            return None

        trend = self._risk_trend() if trend is None else trend
        distance = self._risk_stop_distance_for_sizing(price, trend)
        if distance is None:
            return None

        reserve = self.risk['risk_fee_slippage_reserve_pct']
        risk_per_notional = distance + reserve
        if risk_per_notional <= 0:
            return None

        risk_budget = equity * self._position_risk_loss_pct()
        by_loss_budget = risk_budget / risk_per_notional
        by_margin_cap = equity * self._position_max_margin_ratio() * leverage
        by_notional_cap = equity * self._position_max_notional_ratio()
        notional = min(by_loss_budget, by_margin_cap, by_notional_cap)
        if notional <= 0:
            return None

        margin = notional / leverage
        return {
            'equity': equity,
            'price': price,
            'stop_distance': distance,
            'notional': notional,
            'margin': margin,
            'margin_ratio': margin / equity,
            'risk_budget': risk_budget,
            'estimated_loss_with_reserve': notional * risk_per_notional,
        }

    def _risk_entry_allowed(self, side, amount=0.0, price=0.0):
        if self.book['halted'] or self.book['exit_reason'] or self.book['cycle'] or time.time() < self.book['paused_until']:
            return False
        equity = self._risk_equity()
        if not self._risk_observe_equity(equity) or self.book['halted'] or time.time() < self.book['paused_until']:
            return False
        trend = self._risk_trend()
        if trend is None or (side and trend['direction'] and trend['direction'] != side):
            return False
        if amount:
            if number(price) <= 0 or number(amount) <= 0:
                return False
            leverage = max(number(self.leverage, 1.0), 1.0)
            distance = self._risk_stop_distance_for_sizing(price, trend)
            if distance is None:
                return False
            notional = number(amount) * number(price)
            cap = min(
                self._position_max_notional_ratio(),
                self._position_max_margin_ratio() * leverage,
            ) * equity
            if notional > cap * (1 + 1e-9):
                return False
            expected_loss = notional * (
                distance + self.risk['risk_fee_slippage_reserve_pct']
            )
            if expected_loss > equity * self._position_risk_loss_pct() * (1 + 1e-9):
                return False
        return True

    def place_first_order(self, trade_side, current_price):
        if not self._risk_on():
            return super().place_first_order(trade_side, current_price)

        sizing = self._risk_entry_size(current_price)
        if sizing is None:
            print('RISK: cannot size first order from current equity/ATR; entry blocked')
            return False

        previous_ratio = self.first_order_ratio
        self.first_order_ratio = min(
            sizing['margin_ratio'], self._position_max_margin_ratio()
        )
        self._risk_last_sizing = dict(sizing)
        print(
            'RISK SIZE: '
            f"risk={self._position_risk_loss_pct() * 100:.2f}% equity, "
            f"stop={sizing['stop_distance'] * 100:.2f}%, "
            f"margin={sizing['margin_ratio'] * 100:.2f}% equity, "
            f"notional={sizing['notional']:.2f} USDT"
        )
        try:
            return super().place_first_order(trade_side, current_price)
        finally:
            self.first_order_ratio = previous_ratio

    def _snapshot_strategy(self, stream=None):
        result = super()._snapshot_strategy(stream)
        if self._risk_on():
            result.update(
                risk_sizing_mode='fixed_equity_risk',
                risk_position_loss_pct=self._position_risk_loss_pct(),
                risk_max_margin_ratio=self._position_max_margin_ratio(),
                risk_position_max_notional_ratio=self._position_max_notional_ratio(),
                risk_last_sizing=dict(self._risk_last_sizing or {}),
            )
        return result
