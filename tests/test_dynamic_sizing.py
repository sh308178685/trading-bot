"""Offline tests for Gate fixed-equity-risk position sizing."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from test_last_fill_safety import make_bot, martin
from trading.martin_core import MartinBot as Core
from trading.risk_guard import DEFAULTS


def sized_bot(atr=.5):
    bot = make_bot(side='long', layer=1)
    bot.risk_enabled = True
    bot.risk = dict(DEFAULTS)
    bot.risk['risk_trade_loss_pct'] = .01
    bot.risk['risk_max_notional_ratio'] = .60
    bot.risk_position_loss_pct = .01
    bot.risk_max_margin_ratio = .20
    bot.risk_position_max_notional_ratio = .60
    bot.leverage = 3
    bot.first_order_ratio = .025
    bot.book = {'version': 1, 'cycle': {}, 'stops': [], 'close_intent': {},
                'exit_reason': '', 'halted': '', 'paused_until': 0.0,
                'loss_streak': 0, 'day': '', 'day_equity': 0.0, 'high_water': 0.0}
    bot._risk_save = Mock()
    bot._risk_equity = Mock(return_value=1000.0)
    bot._risk_trend = Mock(return_value={'direction': '', 'atr': atr, 'price': 100})
    bot.get_active_position = Mock(return_value=None)
    return bot


class DynamicSizingTests(unittest.TestCase):
    def test_one_percent_budget_sizes_from_actual_atr_stop(self):
        bot = sized_bot(.5)
        sizing = bot._risk_entry_size(100, equity=1000, trend=bot._risk_trend.return_value)
        self.assertAlmostEqual(sizing['stop_distance'], .015)
        self.assertAlmostEqual(sizing['notional'], 1000 * .01 / (.015 + .002))
        self.assertAlmostEqual(sizing['margin_ratio'], sizing['notional'] / 3 / 1000)
        self.assertAlmostEqual(sizing['estimated_loss_with_reserve'], 10.0)
        self.assertLessEqual(sizing['margin_ratio'], .20)

    def test_narrow_stop_is_capped_at_twenty_percent_margin(self):
        bot = sized_bot(.1)
        sizing = bot._risk_entry_size(100, equity=1000, trend=bot._risk_trend.return_value)
        self.assertAlmostEqual(sizing['stop_distance'], .006)
        self.assertAlmostEqual(sizing['notional'], 600.0)
        self.assertAlmostEqual(sizing['margin'], 200.0)
        self.assertAlmostEqual(sizing['margin_ratio'], .20)
        self.assertLess(sizing['estimated_loss_with_reserve'], 10.0)

    def test_wide_stop_reduces_margin(self):
        bot = sized_bot(1.0)
        sizing = bot._risk_entry_size(100, equity=1000, trend=bot._risk_trend.return_value)
        self.assertAlmostEqual(sizing['stop_distance'], .02)
        self.assertAlmostEqual(sizing['notional'], 1000 * .01 / .022)
        self.assertAlmostEqual(sizing['margin_ratio'], (1000 * .01 / .022) / 3 / 1000)
        self.assertLess(sizing['margin_ratio'], .20)

    def test_final_entry_validation_uses_actual_stop_distance(self):
        bot = sized_bot(.5)
        self.assertTrue(bot._risk_entry_allowed('long', 5.88, 100))
        self.assertFalse(bot._risk_entry_allowed('long', 5.89, 100))

    def test_missing_atr_blocks_sizing(self):
        bot = sized_bot(0)
        self.assertIsNone(bot._risk_entry_size(100, equity=1000, trend=bot._risk_trend.return_value))

    def test_place_first_order_injects_dynamic_ratio_then_restores_legacy_field(self):
        bot = sized_bot(.5)
        observed = []

        def fake_core(core_bot, side, price):
            observed.append(core_bot.first_order_ratio)
            return False

        with patch.object(Core, 'place_first_order', new=fake_core):
            self.assertFalse(bot.place_first_order('long', 100))
        self.assertEqual(len(observed), 1)
        self.assertAlmostEqual(observed[0], (1000 * .01 / .017) / 3 / 1000)
        self.assertAlmostEqual(bot.first_order_ratio, .025)

    def test_new_defaults_override_old_two_percent_and_half_notional_config(self):
        def fake_init(bot):
            bot.config = {
                'exchange': 'gate', 'sandbox': True,
                'risk_trade_loss_pct': .02,
                'risk_max_notional_ratio': .50,
            }
            bot.symbol = 'ETH/USDT:USDT'
            bot.runtime_file = Path(root) / 'martin-runtime.json'
            bot.state = martin.RuntimeState(symbol=bot.symbol)
            bot.first_order_ratio = .10
            bot.loop_interval = 20
            bot.leverage = 3

        with tempfile.TemporaryDirectory() as root, patch.object(Core, '__init__', fake_init):
            bot = martin.MartinBot()
            self.assertAlmostEqual(bot.risk_position_loss_pct, .01)
            self.assertAlmostEqual(bot.risk_max_margin_ratio, .20)
            self.assertAlmostEqual(bot.risk_position_max_notional_ratio, .60)
            self.assertAlmostEqual(bot.risk['risk_trade_loss_pct'], .01)
            self.assertAlmostEqual(bot.risk['risk_max_notional_ratio'], .60)
            self.assertEqual(bot.loop_interval, 5)

    def test_invalid_dynamic_sizing_config_fails_closed(self):
        def fake_init(bot):
            bot.config = {'exchange': 'gate', 'sandbox': True, 'risk_max_margin_ratio': 1.0}
            bot.symbol = 'ETH/USDT:USDT'
            bot.runtime_file = Path(root) / 'martin-runtime.json'
            bot.state = martin.RuntimeState(symbol=bot.symbol)
            bot.first_order_ratio = .10
            bot.loop_interval = 20
            bot.leverage = 3

        with tempfile.TemporaryDirectory() as root, patch.object(Core, '__init__', fake_init):
            with self.assertRaises(ValueError):
                martin.MartinBot()


if __name__ == '__main__':
    unittest.main()
