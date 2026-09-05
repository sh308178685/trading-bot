"""Conservative Gate risk overlay. No exchange calls occur at import time.

All quantities here are BASE units: GateExchangeAdapter already converts lots.
Risk percentages are trigger budgets, not guaranteed fill prices/loss ceilings.
Use a dedicated futures account. Transfers distort equity-based circuit breakers.
"""
from __future__ import annotations

import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path


def number(value, default=0.0):
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError, OverflowError):
        return default


def gate_equity(balance):
    """Gate futures total is wallet balance; add unrealised_pnl exactly once."""
    info = (balance or {}).get('info')
    rows = info if isinstance(info, list) else [info]
    for row in rows:
        if not isinstance(row, dict):
            continue
        if 'total' in row and 'unrealised_pnl' in row:
            a, b = number(row['total'], None), number(row['unrealised_pnl'], None)
            if a is not None and b is not None:
                return number(a + b, None)
    return None  # Never silently treat a wallet balance as marked-to-market equity.


DEFAULTS = {
    'risk_trade_loss_pct': 0.02,
    'risk_daily_loss_pct': 0.04,
    'risk_max_drawdown_pct': 0.08,
    'risk_max_notional_ratio': 0.50,
    'risk_first_margin_ratio': 0.025,
    'risk_stop_min_pct': 0.006,
    'risk_stop_max_pct': 0.02,
    'risk_stop_atr_multiplier': 3.0,
    'risk_trend_adx': 25.0,
    'risk_fee_slippage_reserve_pct': 0.002,
    'risk_loss_cooldown_seconds': 14400.0,
    'risk_profit_cooldown_seconds': 900.0,
    'risk_max_holding_seconds': 86400.0,
}


class GateRiskMixin:
    """Independent risk layer above the existing execution/state machine."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        is_gate = str(self.config.get('exchange', '')).lower() in {'gate', 'gateio', 'gate.io'}
        enabled = self.config.get('risk_enabled', is_gate)
        if not isinstance(enabled, bool):
            raise ValueError('risk_enabled must be a JSON boolean')
        if enabled and not is_gate:
            raise ValueError('This risk overlay supports Gate only')
        self.risk_enabled = enabled
        if not enabled:
            return
        self.risk = {}
        for key, default in DEFAULTS.items():
            value = number(self.config.get(key, default), None)
            if value is None or value <= 0:
                raise ValueError(f'{key} must be finite and positive')
            self.risk[key] = value
        for key in ('risk_trade_loss_pct', 'risk_daily_loss_pct', 'risk_max_drawdown_pct',
                    'risk_stop_min_pct', 'risk_stop_max_pct', 'risk_first_margin_ratio'):
            if self.risk[key] >= 1:
                raise ValueError(f'{key} must be below 1')
        if self.risk['risk_stop_min_pct'] > self.risk['risk_stop_max_pct']:
            raise ValueError('risk_stop_min_pct exceeds risk_stop_max_pct')
        mode = 'demo' if self.config.get('sandbox', True) else 'live'
        symbol = ''.join(c if c.isalnum() else '_' for c in self.symbol)
        self.risk_file = self.runtime_file.with_name(f'martin-risk-gate-{mode}-{symbol}.json')
        self.book = {'version': 1, 'cycle': {}, 'stops': [], 'close_intent': {},
                     'exit_reason': '', 'halted': '', 'paused_until': 0.0,
                     'loss_streak': 0, 'day': '', 'day_equity': 0.0, 'high_water': 0.0}
        if self.risk_file.exists():
            try:
                raw = json.loads(self.risk_file.read_text(encoding='utf-8'), parse_constant=lambda value: (_ for _ in ()).throw(ValueError('non-finite state')))
                if not isinstance(raw, dict) or raw.get('version') != 1:
                    raise ValueError('unsupported risk state')
                for key in self.book:
                    if key not in raw or type(raw[key]) is not type(self.book[key]):
                        # JSON can encode a float as an integer.
                        if key not in raw or not isinstance(self.book[key], float) or not isinstance(raw[key], (float, int)):
                            raise ValueError(f'invalid risk state field: {key}')
                if raw['cycle']:
                    c = raw['cycle']
                    if c.get('side') not in {'long', 'short'} or any(number(c.get(k), None) is None for k in ('entry', 'stop', 'opened', 'equity')) or min(c['entry'], c['stop'], c['opened']) <= 0 or not isinstance(c.get('losing'), bool):
                        raise ValueError('invalid persisted cycle')
                for item in raw['stops'] + ([raw['close_intent']] if raw['close_intent'] else []):
                    if not isinstance(item, dict) or not isinstance(item.get('id'), str) or not isinstance(item.get('client'), str) or not item['client']:
                        raise ValueError('invalid persisted order identity')
                self.book.update(raw)
            except (OSError, ValueError, TypeError) as exc:
                raise RuntimeError('Risk state is unreadable; refusing to reset safety history') from exc
        self._risk_trend_cache = None
        self._risk_trend_at = 0.0
        # Preserve existing tighter native protection when upgrading.
        if self.state.protective_stop_order_id and not self.book['stops']:
            self.book['stops'].append({'id': self.state.protective_stop_order_id,
                                      'client': self.state.protective_stop_client_oid})
        self.first_order_ratio = min(self.first_order_ratio, self.risk['risk_first_margin_ratio'])
        self.loop_interval = min(self.loop_interval, 5)
        # Read-only status/dashboard construction must not overwrite risk history.

    def _risk_on(self):
        return bool(getattr(self, 'risk_enabled', False))

    def _risk_save(self):
        self._save_json(self.risk_file, self.book)

    def _risk_equity(self):
        try:
            return gate_equity(self.exchange.fetch_balance({'type': 'swap'}))
        except Exception:
            return None

    def _risk_observe_equity(self, equity):
        if equity is None or not math.isfinite(equity):
            return False
        day = datetime.fromtimestamp(time.time(), timezone.utc).date().isoformat()
        # A circuit breaker remains latched across midnight and restarts.
        if day != self.book['day']:
            self.book['day'], self.book['day_equity'] = day, max(equity, 0.0)
        self.book['high_water'] = max(number(self.book['high_water']), equity)
        if equity <= 0 or (self.book['high_water'] > 0 and equity <= self.book['high_water'] * (1 - self.risk['risk_max_drawdown_pct'])):
            self.book['halted'] = 'equity_drawdown'
        if self.book['day_equity'] > 0 and equity <= self.book['day_equity'] * (1 - self.risk['risk_daily_loss_pct']):
            self.book['paused_until'] = max(self.book['paused_until'], time.time() + 86400)
            if self.book['cycle']:
                self.book['exit_reason'] = self.book['exit_reason'] or 'daily_loss'
        self._risk_save()
        return equity > 0

    def fetch_ohlcv_df(self, timeframe=None, limit=100):
        frame = super().fetch_ohlcv_df(timeframe, limit)
        if not self._risk_on() or frame is None:
            return frame
        seconds = self._timeframe_seconds(timeframe or self.timeframe)
        if seconds <= 0:
            return None
        frame = frame.drop_duplicates('timestamp').sort_values('timestamp')
        frame = frame[frame['timestamp'] + seconds * 1000 <= time.time() * 1000].copy()
        if len(frame) < 20 or time.time() * 1000 - (number(frame.iloc[-1]['timestamp']) + seconds * 1000) > seconds * 1000:
            return None
        return frame

    def _risk_trend(self):
        if time.time() - getattr(self, '_risk_trend_at', 0.0) < 20:
            return self._risk_trend_cache
        self._risk_trend_at = time.time()
        self._risk_trend_cache = None
        try:
            frame = self.fetch_ohlcv_df('1h', max(120, self.slow_ema_period * 3))
            if frame is None or len(frame) < self.slow_ema_period + self.adx_period * 2:
                return None
            frame = self.add_indicators(frame)
            rows = frame.iloc[-2:]
            fields = ('close', 'ema_fast', 'ema_slow', 'adx', 'atr')
            if any(number(row[key], None) is None for _, row in rows.iterrows() for key in fields):
                return None
            direction = ''
            if (rows['adx'] >= self.risk['risk_trend_adx']).all():
                if (rows['ema_fast'] > rows['ema_slow']).all() and (rows['close'] > rows['ema_slow']).all():
                    direction = 'long'
                elif (rows['ema_fast'] < rows['ema_slow']).all() and (rows['close'] < rows['ema_slow']).all():
                    direction = 'short'
            self._risk_trend_cache = {'direction': direction, 'atr': number(rows.iloc[-1]['atr']),
                                      'price': number(rows.iloc[-1]['close'])}
        except Exception:
            pass
        return self._risk_trend_cache

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
            notional = amount * price
            cap = min(self.risk['risk_max_notional_ratio'], self.risk['risk_first_margin_ratio'] * max(number(self.leverage), 1)) * equity
            if notional > cap * (1 + 1e-9):
                return False
            if notional * (self.risk['risk_stop_max_pct'] + self.risk['risk_fee_slippage_reserve_pct']) > equity * self.risk['risk_trade_loss_pct']:
                return False
        return True

    def _submit_entry_order(self, order_side, amount, entry_price, label, order_type='limit'):
        if self._risk_on():
            with self.action_lock:
                live = self.get_active_position()
                if live is self._POSITION_API_ERROR or live or not self._risk_entry_allowed('long' if order_side == 'buy' else 'short', amount, entry_price):
                    print('RISK: entry blocked (trend, exposure, cooldown, position, or unavailable data)')
                    return None
                self.book['pending_equity'] = self._risk_equity() or 0.0
                self._risk_save()
                return super()._submit_entry_order(order_side, amount, entry_price, label, order_type)
        return super()._submit_entry_order(order_side, amount, entry_price, label, order_type)

    def place_first_order(self, *args, **kwargs):
        result = super().place_first_order(*args, **kwargs)
        if result and self._risk_on():
            with self.action_lock:
                ok, position = self._fetch_active_position()
                if ok and position:
                    self._risk_tick(position)
        return result

    def _build_add_order_plan(self, *args, **kwargs):
        if self._risk_on():
            return None  # Deliberately remove martingale, including phase-2 rescue.
        return super()._build_add_order_plan(*args, **kwargs)

    def _submit_add_order_plan(self, plan):
        return False if self._risk_on() else super()._submit_add_order_plan(plan)

    def place_add_order(self, *args, **kwargs):
        return False if self._risk_on() else super().place_add_order(*args, **kwargs)

    def _risk_adopt(self, position, equity):
        side, entry = position['side'], number(position.get('entryPrice'))
        if side not in {'long', 'short'} or entry <= 0:
            raise ValueError('Invalid authoritative position')
        if self.book['cycle']:
            if self.book['cycle']['side'] != side:
                self.book['halted'] = 'position_side_changed'
                self._risk_save()
                raise RuntimeError('Position side changed without a confirmed flat state')
            return
        trend = self._risk_trend()
        atr = number((trend or {}).get('atr'))
        distance = min(self.risk['risk_stop_max_pct'], max(self.risk['risk_stop_min_pct'], atr / entry * self.risk['risk_stop_atr_multiplier'])) if atr > 0 else self.risk['risk_stop_max_pct']
        reference = number(self.book.pop('pending_equity', 0.0)) or number(self.state.initial_balance) or number(equity)
        if equity is not None and equity > 0:
            reference = min(reference, equity) if reference > 0 else equity
        opened = self._position_open_timestamp_ms(position) / 1000
        self.book['cycle'] = {'side': side, 'entry': entry,
                              'stop': entry * (1 - distance if side == 'long' else 1 + distance),
                              'opened': min(opened, time.time()) if opened > 0 else time.time(),
                              'equity': reference, 'losing': False}
        self._risk_save()

    def _protective_stop_target(self, position, current_profit_pct=None, best_profit_pct=None, trail_ratio=None):
        if not self._risk_on():
            return super()._protective_stop_target(position, current_profit_pct, best_profit_pct, trail_ratio)
        cycle = self.book['cycle']
        if not cycle:
            return None
        side, entry = position['side'], number(position.get('entryPrice'))
        qty = number(position.get('contracts'))
        if entry <= 0 or qty <= 0 or side != cycle['side']:
            return None
        candidates = [cycle['stop']]
        if cycle['equity'] > 0:
            budget = max(0.0, cycle['equity'] * self.risk['risk_trade_loss_pct'] - qty * entry * self.risk['risk_fee_slippage_reserve_pct'])
            candidates.append(entry - budget / qty if side == 'long' else entry + budget / qty)
        leverage = max(number(position.get('leverage'), number(self.leverage)), 1.0)
        locked = 0.0
        if self.state.activated:
            best = max(number(best_profit_pct, self.state.best_profit_pct), 0.0)
            ratio = number(trail_ratio) or number(self.state.active_trailing_drawdown_ratio) or 0.25
            locked = best * min(1.0, max(1 - ratio, self.protective_stop_profit_lock_ratio))
            candidates.append(entry * (1 + locked / leverage if side == 'long' else 1 - locked / leverage))
        previous = number(self.state.protective_stop_price)
        if previous > 0:
            candidates.append(previous)
        target = max(candidates) if side == 'long' else min(candidates)
        tick = number(self._market().get('precision', {}).get('price'))
        if tick > 0:
            target = (math.ceil(target / tick - 1e-10) if side == 'long' else math.floor(target / tick + 1e-10)) * tick
        target = self._price_to_precision(target)
        return {'side': side, 'hold_side': side, 'entry_price': entry, 'trigger_price': target,
                'locked_profit_pct': locked, 'leverage': leverage}

    def _is_owned_exit_order(self, order):
        if self._risk_on():
            oid, client = self._entry_order_identity(order)
            identities = self.book['stops'] + [self.book['close_intent']]
            if any((oid and oid == item.get('id')) or (client and client == item.get('client')) for item in identities):
                return True
        return super()._is_owned_exit_order(order)

    def _arm_protective_stop(self, position, current_profit_pct, reason, best_profit_pct=None, trail_ratio=None, force=False):
        if not self._risk_on():
            return super()._arm_protective_stop(position, current_profit_pct, reason, best_profit_pct, trail_ratio, force)
        with self.action_lock:
            live = self.get_active_position()
            if live is self._POSITION_API_ERROR or not live:
                return False
            target = self._protective_stop_target(live, current_profit_pct, best_profit_pct, trail_ratio)
            orders = self.fetch_open_orders()
            if target is None or orders is None:
                return False
            mark, trigger = number(live.get('markPrice')), target['trigger_price']
            if mark <= 0:
                return False
            if (live['side'] == 'long' and mark <= trigger) or (live['side'] == 'short' and mark >= trigger):
                self.book['exit_reason'] = self.book['exit_reason'] or 'protective_level_crossed'
                self._risk_save()
                return False
            # Recover a timed-out create by its persisted client identity. Absence
            # alone does not prove rejection; never blindly submit a duplicate.
            for item in self.book['stops']:
                if not item.get('id'):
                    match = next((o for o in orders if self._entry_order_identity(o)[1] == item['client']), None)
                    if match is None:
                        self.book['exit_reason'] = self.book['exit_reason'] or 'unconfirmed_native_stop'
                        self._risk_save()
                        return False
                    item['id'] = str(match['id'])
            active = next((o for o in orders if str(o.get('id')) == self.state.protective_stop_order_id), None)
            if self.state.protective_stop_order_id and active is None:
                self.book['exit_reason'] = self.book['exit_reason'] or 'native_stop_missing'
                self._risk_save()
                return False
            if active and abs(self.state.protective_stop_price - trigger) < 1e-10 and number(active.get('amount')) >= number(live['contracts']) * (1 - 1e-9):
                return True
            new = {'id': '', 'client': self._new_client_order_id()}
            self.book['stops'].append(new)
            self._risk_save()  # Write-ahead identity before an exchange mutation.
            try:
                response = self.exchange.place_position_stop_loss(
                    self.symbol, hold_side=live['side'], trigger_price=trigger,
                    trigger_type='mark_price', execute_price=0.0, client_oid=new['client'])
                if not isinstance(response, dict) or not response.get('id'):
                    raise RuntimeError('Native stop has no acknowledged order ID')
                new['id'] = str(response['id'])
                self.state.protective_stop_active = True
                self.state.protective_stop_order_id = new['id']
                self.state.protective_stop_client_oid = new['client']
                self.state.protective_stop_price = trigger
                self._save_runtime_state()
                self._risk_save()
                # Place-before-cancel: never deliberately remove the only stop.
                for old in list(self.book['stops']):
                    if old is new:
                        continue
                    try:
                        self.exchange.cancel_position_stop_loss(self.symbol, order_id=old['id'], client_oid=old['client'])
                        self.book['stops'].remove(old)
                    except Exception:
                        pass  # Both are reduce-only; retain ownership for cleanup.
                self._risk_save()
                return True
            except Exception as exc:
                print(f'RISK: native protection unconfirmed: {exc}')
                self.book['exit_reason'] = self.book['exit_reason'] or 'native_stop_failed'
                self._risk_save()
                return False

    def _risk_tick(self, position):
        equity = self._risk_equity()
        self._risk_observe_equity(equity)
        if not position:
            if self.book['cycle']:
                self._finalize_full_exit('risk: confirmed-flat reconciliation')
                return False
            orders = self.fetch_open_orders()
            side = self.state.position_side
            allowed = self._risk_entry_allowed(side)
            if orders is not None and not allowed:
                entries = [o for o in orders if not o.get('reduceOnly') and self._is_owned_entry_order(o)]
                if entries:
                    self._cancel_entry_orders(entries)
            return allowed
        self._risk_adopt(position, equity)
        cycle = self.book['cycle']
        mark, entry, qty = number(position.get('markPrice')), number(position.get('entryPrice')), number(position.get('contracts'))
        if mark <= 0 or entry <= 0 or qty <= 0:
            return False
        cycle['losing'] = (mark < entry if position['side'] == 'long' else mark > entry)
        if equity is not None and cycle['equity'] > 0 and equity <= cycle['equity'] * (1 - self.risk['risk_trade_loss_pct']):
            self.book['exit_reason'] = self.book['exit_reason'] or 'trade_equity_budget'
        if self.book['halted']:
            self.book['exit_reason'] = self.book['exit_reason'] or self.book['halted']
        if cycle['equity'] > 0 and qty * mark > cycle['equity'] * self.risk['risk_max_notional_ratio']:
            self.book['exit_reason'] = self.book['exit_reason'] or 'exposure_limit'
        if time.time() - cycle['opened'] >= self.risk['risk_max_holding_seconds']:
            self.book['exit_reason'] = self.book['exit_reason'] or 'maximum_holding_time'
        trend = self._risk_trend()
        if trend and trend['direction'] and trend['direction'] != position['side'] and cycle['losing'] and abs(mark - entry) >= max(trend['atr'], entry * self.risk['risk_stop_min_pct']):
            self.book['exit_reason'] = self.book['exit_reason'] or 'confirmed_opposite_trend'
        self._risk_save()
        if not self.book['exit_reason']:
            if not self._arm_protective_stop(position, self._position_profit_pct(position), 'initial/continuous risk protection'):
                self.book['exit_reason'] = self.book['exit_reason'] or 'protection_unavailable'
                self._risk_save()
        if self.book['exit_reason']:
            self._execute_exit_pipeline(self.book['exit_reason'], position)
            return False
        # Cancel inherited martingale orders, including a residual partial fill.
        # Keep identity tracking intact until the legacy reconciler confirms it.
        orders = self.fetch_open_orders()
        if orders is None:
            return False
        entries = [o for o in orders if not o.get('reduceOnly') and self._is_owned_entry_order(o)]
        if entries and not self._cancel_entry_orders(entries):
            return False
        return True

    def enforce_exchange_position_sync(self, *args, **kwargs):
        ok, position = super().enforce_exchange_position_sync(*args, **kwargs)
        if self._risk_on() and (ok or position):
            with self.action_lock:
                if not self._risk_tick(position):
                    return False, position
        return ok, position

    def check_trailing_tp(self, position):
        result = super().check_trailing_tp(position)
        return result or (self._risk_on() and bool(self.book['exit_reason']))

    def close_position(self, position):
        if not self._risk_on():
            return super().close_position(position)
        # Immediate reduce-only market exit. Reconcile an uncertain/partial IOC
        # before retrying; use a fresh authoritative remaining position each time.
        intent = self.book['close_intent']
        if intent:
            try:
                oid = intent.get('id') or self._resolve_order_id_from_client_oid(intent['client'])
                if not oid:
                    return None
                order = self._fetch_authoritative_order(oid)
                if str(order.get('status', '')).lower() not in {'closed', 'filled', 'canceled', 'cancelled', 'expired', 'rejected'}:
                    return None
                filled = number(order.get('filled'), None)
                if filled is None or number(position['contracts']) > intent['before'] - filled + max(intent['before'] * 1e-8, 1e-9):
                    return None
            except Exception:
                return None
        intent = {'id': '', 'client': self._new_client_order_id(), 'before': number(position['contracts'])}
        self.book['close_intent'] = intent
        self._risk_save()
        try:
            result = self._create_order_idempotent('market', 'sell' if position['side'] == 'long' else 'buy',
                                                  number(position['contracts']), None,
                                                  {'reduceOnly': True, 'clientOrderId': intent['client']})
            intent['id'] = str((result or {}).get('id') or '')
            self._risk_save()
            return result
        except Exception as exc:
            print(f'RISK: exit response uncertain; retain identity, protection and exit latch: {exc}')
            return None

    def _execute_exit_pipeline(self, reason, position=None):
        if not self._risk_on():
            return super()._execute_exit_pipeline(reason, position)
        with self.action_lock:
            if self._exit_in_progress.is_set():
                return False
            self.book['exit_reason'] = self.book['exit_reason'] or reason
            self._risk_save()
            self._exit_in_progress.set()
            try:
                orders = self.fetch_open_orders()
                if orders is None:
                    return False  # In particular, DO NOT cancel native protection.
                entries = [o for o in orders if not o.get('reduceOnly')]
                if any(not self._is_owned_entry_order(o) for o in entries):
                    self.book['halted'] = 'foreign_entry_orders_require_review'
                    self._risk_save()
                    return False
                if entries and not self._cancel_entry_orders(entries):
                    return False
                live = self.get_active_position()
                if live is self._POSITION_API_ERROR:
                    return False
                if not live:
                    return self._finalize_full_exit(reason)
                if live.get('side') != self.book['cycle'].get('side'):
                    return False
                self.close_position(live)
                if self._wait_for_position_close(number(live['contracts'])):
                    return self._finalize_full_exit(reason)
                return False  # Latch survives partial fills, failures and restarts.
            finally:
                self._exit_in_progress.clear()

    def _finalize_full_exit(self, reason=''):
        if self._risk_on() and self.book['cycle']:
            if not self._wait_for_position_close(0.0, timeout_sec=1.0):
                return False
        return super()._finalize_full_exit(reason)

    def _reset_state(self):
        cycle = dict(self.book['cycle']) if self._risk_on() else {}
        if self._risk_on() and cycle:
            # Prevent a stale zero snapshot from clearing protection/history.
            live = self.get_active_position()
            if live is self._POSITION_API_ERROR or live:
                return False
        result = super()._reset_state()
        if result and self._risk_on():
            if cycle:
                equity = self._risk_equity()
                losing = equity < cycle['equity'] if equity is not None and cycle['equity'] > 0 else cycle['losing']
                self.book['loss_streak'] = self.book['loss_streak'] + 1 if losing else 0
                seconds = self.risk['risk_loss_cooldown_seconds'] if losing else self.risk['risk_profit_cooldown_seconds']
                if self.book['loss_streak'] >= 3:
                    seconds = max(seconds, 86400)
                self.book['paused_until'] = max(self.book['paused_until'], time.time() + seconds)
            self.book.update(cycle={}, stops=[], close_intent={}, exit_reason='')
            self.book.pop('pending_equity', None)
            self._risk_save()
        return result

    def _snapshot_strategy(self, stream=None):
        result = super()._snapshot_strategy(stream)
        if self._risk_on():
            result.update(risk_enabled=True, risk_adds_disabled=True, risk_halted=self.book['halted'],
                          risk_paused_until=self.book['paused_until'], risk_exit_reason=self.book['exit_reason'],
                          risk_cycle=dict(self.book['cycle']))
        return result
