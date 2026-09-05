"""Offline risk scenarios; all exchange interactions are fakes."""
import copy
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
from test_last_fill_safety import make_bot, martin
from trading.risk_guard import DEFAULTS, GateRiskMixin, gate_equity, number
from trading.martin_core import MartinBot as Core


def risk_bot(side='long', price=100.0, qty=1.0):
    bot = make_bot(side=side, layer=1)
    bot.risk_enabled = True
    bot.risk = dict(DEFAULTS)
    bot.leverage = 3
    bot.protective_stop_profit_lock_ratio = .75
    bot.protective_stop_min_profit_pct = .005
    bot.book = {'version': 1, 'cycle': {}, 'stops': [], 'close_intent': {},
                'exit_reason': '', 'halted': '', 'paused_until': 0.0,
                'loss_streak': 0, 'day': '', 'day_equity': 0.0, 'high_water': 0.0}
    bot._risk_save = Mock()
    bot._risk_equity = Mock(return_value=1000.0)
    bot._risk_trend = Mock(return_value={'direction': '', 'atr': 0.5, 'price': 100})
    bot._market = Mock(return_value={'precision': {'price': .01}})
    bot._price_to_precision = lambda x: round(x, 2)
    pos = {'side': side, 'entryPrice': 100.0, 'markPrice': price, 'contracts': qty, 'leverage': 3}
    bot.get_active_position = Mock(return_value=pos)
    bot.fetch_open_orders = Mock(return_value=[])
    bot._risk_adopt(pos, 1000.0)
    return bot, pos


class PureRiskTests(unittest.TestCase):
    def test_wallet_plus_unrealized_exactly_once(self):
        self.assertEqual(gate_equity({'USDT': {'total': 9999}, 'info': {'total': '1000', 'unrealised_pnl': '-50'}}), 950)

    def test_unknown_equity_does_not_use_wallet_fallback(self):
        self.assertIsNone(gate_equity({'USDT': {'total': 1000}}))

    def test_nonfinite_data_rejected(self):
        self.assertIsNone(number('NaN', None))
        self.assertIsNone(number('Infinity', None))
        self.assertIsNone(gate_equity({'info': {'total': 'inf', 'unrealised_pnl': '0'}}))

    def test_long_stop_before_any_profit(self):
        bot, pos = risk_bot()
        self.assertEqual(bot._protective_stop_target(pos)['trigger_price'], 98.5)

    def test_short_stop_before_any_profit(self):
        bot, pos = risk_bot('short')
        self.assertEqual(bot._protective_stop_target(pos)['trigger_price'], 101.5)

    def test_stop_cannot_widen_after_average_entry_changes(self):
        bot, pos = risk_bot()
        original = bot._protective_stop_target(pos)['trigger_price']
        pos['entryPrice'] = 95
        bot._risk_trend.return_value['atr'] = 99
        bot._risk_adopt(pos, 1000)
        self.assertGreaterEqual(bot._protective_stop_target(pos)['trigger_price'], original)

    def test_stop_respects_equity_budget_and_base_units(self):
        bot, pos = risk_bot(qty=100)
        # $20 budget minus $20 reserve leaves no additional price loss.
        self.assertEqual(bot._protective_stop_target(pos)['trigger_price'], 100)

    def test_tighter_existing_stop_is_preserved(self):
        for side, old in [('long', 99.1), ('short', 100.9)]:
            with self.subTest(side=side):
                bot, pos = risk_bot(side)
                bot.state.protective_stop_price = old
                self.assertEqual(bot._protective_stop_target(pos)['trigger_price'], old)

    def test_activation_does_not_wait_for_001_buffer(self):
        bot, pos = risk_bot(price=100.2)
        bot.state.activated = True
        bot.state.best_profit_pct = .005
        target = bot._protective_stop_target(pos, current_profit_pct=.005, trail_ratio=.25)
        self.assertGreater(target['trigger_price'], 100)
        self.assertLess(target['trigger_price'], pos['markPrice'])

    def test_all_add_paths_blocked(self):
        bot, pos = risk_bot()
        self.assertIsNone(bot._build_add_order_plan(2, 99, position=pos))
        self.assertFalse(bot._submit_add_order_plan({'layer_num': 2}))
        self.assertFalse(bot.place_add_order(2, 99))
        self.assertIsNone(bot._submit_entry_order('buy', .1, 99, 'add'))
        self.assertEqual(bot.exchange.created_orders, [])

    def test_opposite_trend_blocks_first_entry(self):
        bot, _ = risk_bot()
        bot.book['cycle'] = {}
        bot._risk_trend.return_value['direction'] = 'short'
        self.assertFalse(bot._risk_entry_allowed('long', .1, 100))
        self.assertTrue(bot._risk_entry_allowed('short', .1, 100))
        self.assertTrue(bot._risk_entry_allowed(None))  # IDLE must not bias long.

    def test_missing_indicators_block_entry(self):
        bot, _ = risk_bot()
        bot.book['cycle'] = {}
        bot._risk_trend.return_value = None
        self.assertFalse(bot._risk_entry_allowed('long'))

    def test_missing_equity_blocks_entry(self):
        bot, _ = risk_bot()
        bot.book['cycle'] = {}
        bot._risk_equity.return_value = None
        self.assertFalse(bot._risk_entry_allowed('long'))

    def test_exposure_cap_includes_leverage(self):
        bot, _ = risk_bot()
        bot.book['cycle'] = {}
        self.assertTrue(bot._risk_entry_allowed('long', .75, 100))
        self.assertFalse(bot._risk_entry_allowed('long', .76, 100))

    def test_cooldown_blocks_reentry(self):
        bot, _ = risk_bot()
        bot.book.update(cycle={}, paused_until=time.time() + 60)
        self.assertFalse(bot._risk_entry_allowed('long'))

    def test_daily_loss_latches_for_24h(self):
        bot, _ = risk_bot()
        bot._risk_observe_equity(1000)
        bot._risk_observe_equity(960)
        self.assertGreater(bot.book['paused_until'], time.time() + 86000)
        self.assertEqual(bot.book['exit_reason'], 'daily_loss')

    def test_high_water_drawdown_remains_halted_after_recovery(self):
        bot, _ = risk_bot()
        bot._risk_observe_equity(1000)
        bot._risk_observe_equity(919)
        bot._risk_observe_equity(1050)
        self.assertEqual(bot.book['halted'], 'equity_drawdown')

    def test_clock_does_not_release_daily_pause_at_midnight(self):
        bot, _ = risk_bot()
        bot._risk_observe_equity(1000)
        bot._risk_observe_equity(950)
        pause = bot.book['paused_until']
        with patch('trading.risk_guard.time.time', return_value=time.time() + 3600):
            bot.book['day'] = 'yesterday'
            bot._risk_observe_equity(950)
            self.assertEqual(bot.book['paused_until'], pause)

    def test_closed_candle_filter(self):
        bot, _ = risk_bot()
        bot.timeframe = '5m'
        now = 1800000000.0
        timestamps = [(now - i * 300) * 1000 for i in range(25, -1, -1)]
        frame = pd.DataFrame({'timestamp': timestamps, 'close': 100})
        with patch.object(Core, 'fetch_ohlcv_df', return_value=frame), patch('trading.risk_guard.time.time', return_value=now):
            result = bot.fetch_ohlcv_df('5m')
        self.assertEqual(len(result), 25)
        self.assertEqual(result.iloc[-1]['timestamp'], (now - 300) * 1000)

    def test_stale_candles_rejected(self):
        bot, _ = risk_bot()
        bot.timeframe = '5m'
        frame = pd.DataFrame({'timestamp': [i * 300000 for i in range(50)], 'close': 100})
        with patch.object(Core, 'fetch_ohlcv_df', return_value=frame):
            self.assertIsNone(bot.fetch_ohlcv_df('5m'))

    def test_loss_streak_and_cooldown_survive_strategy_reset(self):
        bot, _ = risk_bot()
        bot.get_active_position.return_value = None
        bot.book['loss_streak'] = 2
        bot._risk_equity.return_value = 999
        with patch.object(Core, '_reset_state', return_value=True):
            self.assertTrue(bot._reset_state())
        self.assertEqual(bot.book['loss_streak'], 3)
        self.assertGreater(bot.book['paused_until'], time.time() + 86000)
        self.assertEqual(bot.book['cycle'], {})

    def test_reset_refuses_unknown_position(self):
        bot, _ = risk_bot()
        bot.get_active_position.return_value = bot._POSITION_API_ERROR
        self.assertFalse(bot._reset_state())
        self.assertTrue(bot.book['cycle'])


class ExchangeSafetyTests(unittest.TestCase):
    def test_replacement_created_before_old_stop_canceled(self):
        bot, pos = risk_bot()
        bot.state.protective_stop_order_id = 'old'
        bot.state.protective_stop_price = 98
        bot.book['stops'] = [{'id': 'old', 'client': 't-martin-old'}]
        bot.fetch_open_orders.return_value = [{'id': 'old', 'amount': 1, 'reduceOnly': True}]
        events = []
        bot.exchange.place_position_stop_loss = lambda *a, **k: events.append('create') or {'id': 'new'}
        bot.exchange.cancel_position_stop_loss = lambda *a, **k: events.append('cancel') or {}
        self.assertTrue(bot._arm_protective_stop(pos, 0, 'test'))
        self.assertEqual(events, ['create', 'cancel'])

    def test_failed_stop_create_keeps_old_stop_and_latches_exit(self):
        bot, pos = risk_bot()
        bot.book['stops'] = [{'id': 'old', 'client': 't-martin-old'}]
        bot.exchange.place_position_stop_loss = Mock(side_effect=TimeoutError())
        bot.exchange.cancel_position_stop_loss = Mock()
        self.assertFalse(bot._arm_protective_stop(pos, 0, 'test'))
        bot.exchange.cancel_position_stop_loss.assert_not_called()
        self.assertTrue(bot.book['exit_reason'])
        self.assertEqual(len(bot.book['stops']), 2)
        bot._arm_protective_stop(pos, 0, 'retry')
        self.assertEqual(bot.exchange.place_position_stop_loss.call_count, 1)

    def test_stop_create_identity_persisted_before_request(self):
        bot, pos = risk_bot()
        def create(*args, **kwargs):
            self.assertEqual(bot.book['stops'][-1]['client'], kwargs['client_oid'])
            self.assertGreater(bot._risk_save.call_count, 0)
            return {'id': 'new'}
        bot.exchange.place_position_stop_loss = create
        self.assertTrue(bot._arm_protective_stop(pos, 0, 'test'))

    def test_crossed_stop_exits_instead_of_placing_invalid_trigger(self):
        bot, pos = risk_bot(price=97)
        bot.exchange.place_position_stop_loss = Mock()
        self.assertFalse(bot._arm_protective_stop(pos, -.09, 'test'))
        bot.exchange.place_position_stop_loss.assert_not_called()
        self.assertEqual(bot.book['exit_reason'], 'protective_level_crossed')

    def test_native_quantity_replaced_after_partial_fill_growth(self):
        bot, pos = risk_bot(qty=2)
        bot.state.protective_stop_order_id = 'old'
        bot.state.protective_stop_price = 98.5
        bot.book['stops'] = [{'id': 'old', 'client': 't-martin-old'}]
        bot.fetch_open_orders.return_value = [{'id': 'old', 'amount': 1, 'reduceOnly': True}]
        bot.exchange.place_position_stop_loss = Mock(return_value={'id': 'new'})
        bot.exchange.cancel_position_stop_loss = Mock()
        self.assertTrue(bot._arm_protective_stop(pos, 0, 'test'))
        bot.exchange.place_position_stop_loss.assert_called_once()

    def test_exit_keeps_protection_when_order_api_fails(self):
        bot, pos = risk_bot()
        bot.fetch_open_orders.return_value = None
        bot._clear_protective_stop = Mock()
        bot.close_position = Mock()
        self.assertFalse(bot._execute_exit_pipeline('stop', pos))
        bot._clear_protective_stop.assert_not_called()
        bot.close_position.assert_not_called()
        self.assertEqual(bot.book['exit_reason'], 'stop')

    def test_exit_is_reduce_only_market(self):
        bot, pos = risk_bot()
        result = bot.close_position(pos)
        self.assertEqual(result['type'], 'market')
        self.assertTrue(result['params']['reduceOnly'])
        self.assertEqual(result['side'], 'sell')
        self.assertEqual(bot.book['close_intent']['id'], result['id'])

    def test_uncertain_market_exit_is_not_blindly_retried(self):
        bot, pos = risk_bot()
        bot._create_order_idempotent = Mock(side_effect=TimeoutError())
        bot._resolve_order_id_from_client_oid = Mock(return_value=None)
        bot.close_position(pos)
        bot.close_position(pos)
        self.assertEqual(bot._create_order_idempotent.call_count, 1)
        self.assertTrue(bot.book['close_intent']['client'])

    def test_partial_exit_waits_for_position_consistency(self):
        bot, pos = risk_bot(qty=2)
        bot.book['close_intent'] = {'id': 'first', 'client': 't-martin-first', 'before': 2}
        bot._fetch_authoritative_order = Mock(return_value={'status': 'closed', 'filled': 1})
        self.assertIsNone(bot.close_position(pos))
        self.assertEqual(bot.exchange.created_orders, [])
        pos['contracts'] = 1
        self.assertEqual(bot.close_position(pos)['amount'], 1)

    def test_finalize_does_not_cancel_stop_on_transient_zero(self):
        bot, _ = risk_bot()
        bot._wait_for_position_close = Mock(return_value=False)
        with patch.object(Core, '_finalize_full_exit') as legacy:
            self.assertFalse(bot._finalize_full_exit('zero?'))
            legacy.assert_not_called()

    def test_sustained_long_and_short_adverse_paths_trigger_exit(self):
        for side, prices in [('long', [100, 99.8, 99.5, 98.4, 80]), ('short', [100, 100.2, 100.5, 101.6, 120])]:
            bot, pos = risk_bot(side)
            def native(*args, **kwargs):
                bot.fetch_open_orders.return_value = [{'id': 'native', 'amount': pos['contracts'], 'reduceOnly': True}]
                return {'id': 'native'}
            bot.exchange.place_position_stop_loss = Mock(side_effect=native)
            for index, price in enumerate(prices):
                pos['markPrice'] = price
                bot._arm_protective_stop(pos, 0, 'path')
                if bot.book['exit_reason']:
                    break
            self.assertEqual(index, 3)
            self.assertEqual(bot.book['exit_reason'], 'protective_level_crossed')
            bot.exchange.place_position_stop_loss.assert_called_once()
            self.assertEqual(bot.exchange.created_orders, [])

    def test_missing_known_stop_latches_exit_instead_of_waiting_for_recovery(self):
        bot, pos = risk_bot()
        bot.state.protective_stop_order_id = 'gone'
        bot.state.protective_stop_price = 98.5
        bot.exchange.place_position_stop_loss = Mock()
        self.assertFalse(bot._arm_protective_stop(pos, 0, 'missing'))
        self.assertEqual(bot.book['exit_reason'], 'native_stop_missing')
        bot.exchange.place_position_stop_loss.assert_not_called()

    def test_exposure_or_time_limit_latches_exit(self):
        for cause in ('exposure', 'time'):
            bot, pos = risk_bot()
            if cause == 'exposure':
                pos['contracts'] = 6
            else:
                bot.book['cycle']['opened'] = time.time() - 90000
            bot._execute_exit_pipeline = Mock(return_value=False)
            self.assertFalse(bot._risk_tick(pos))
            self.assertIn(bot.book['exit_reason'], {'exposure_limit', 'maximum_holding_time'})
            bot._execute_exit_pipeline.assert_called_once()

    def test_owned_foreign_orders_are_not_canceled(self):
        bot, pos = risk_bot()
        bot.fetch_open_orders.return_value = [{'id': 'manual', 'side': 'buy', 'reduceOnly': False}]
        bot._cancel_entry_orders = Mock()
        self.assertFalse(bot._execute_exit_pipeline('stop', pos))
        bot._cancel_entry_orders.assert_not_called()
        self.assertEqual(bot.book['halted'], 'foreign_entry_orders_require_review')




class RiskPersistenceTests(unittest.TestCase):
    def construct(self, root, config=None):
        def fake_init(bot):
            bot.config = config or {'exchange': 'gate', 'sandbox': True}
            bot.symbol = 'ETH/USDT:USDT'
            bot.runtime_file = Path(root) / 'martin-runtime.json'
            bot.state = martin.RuntimeState(symbol=bot.symbol)
            bot.first_order_ratio = .10
            bot.loop_interval = 20
        with patch.object(Core, '__init__', fake_init):
            return martin.MartinBot()

    def test_defaults_and_restart_preserve_exit_cooldown_and_stop_anchor(self):
        with tempfile.TemporaryDirectory() as root:
            bot = self.construct(root)
            self.assertTrue(bot.risk_enabled)
            self.assertEqual(bot.first_order_ratio, .025)
            self.assertEqual(bot.loop_interval, 5)
            bot.book.update(paused_until=time.time()+14400, loss_streak=2, exit_reason='stop')
            bot.book['cycle'] = {'side':'long', 'entry':100., 'stop':98., 'opened':time.time(), 'equity':1000., 'losing':True}
            bot._risk_save()
            restored = self.construct(root)
            self.assertEqual(restored.book, bot.book)

    def test_corrupt_risk_file_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            bot = self.construct(root)
            bot.risk_file.write_text('{bad')
            with self.assertRaises(RuntimeError):
                self.construct(root)

    def test_nonfinite_risk_file_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            bot = self.construct(root)
            bot.book['paused_until'] = float('nan')
            bot._risk_save()
            with self.assertRaises(RuntimeError):
                self.construct(root)

    def test_invalid_config_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            for config in ({'exchange':'gate','risk_enabled':'false'},
                           {'exchange':'gate','risk_trade_loss_pct':0},
                           {'exchange':'gate','risk_stop_min_pct':.03,'risk_stop_max_pct':.02}):
                with self.subTest(config=config), self.assertRaises(ValueError):
                    self.construct(root, config)


if __name__ == '__main__':
    unittest.main()
