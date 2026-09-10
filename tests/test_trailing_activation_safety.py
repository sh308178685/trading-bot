"""Regression tests for the 2026-09-10 stale-peak activation incident.

All exchange operations are fakes. These tests never create a live client.
"""

import math
import json
import tempfile
import threading
import unittest
from dataclasses import asdict
from pathlib import Path
from unittest import mock

import test_last_fill_safety as fixtures


class TrailingActivationSafetyTests(unittest.TestCase):
    def make_case(self, profit=-0.0112, threshold=0.005, history=0.0055):
        bot = fixtures.TrailingTakeProfitTests._configured_bot(layer=3)
        bot.state.position_side = "short"
        bot.state.best_profit_pct = history
        bot._last_risk_log_profit_pct = history
        bot.ws_risk_log_step_pct = 0.0025
        bot.test_profit = profit
        bot._position_profit_pct = lambda *a, **kw: bot.test_profit
        position = {"side": "short", "contracts": 0.0015, "markPrice": 100.0}
        bot._realtime_position_snapshot = lambda: position
        bot._refresh_realtime_risk_context = lambda *a, **kw: {
            "layer": 3, "adx": 26.9, "volatility_pct": 0.0014,
            "activate_pct": threshold, "trail_ratio": 0.24,
            "current_price": 100.0, "trail_price": 200.0,
            "trail_desc": "non-crossed test ATR trail",
        }
        bot._arm_protective_stop = mock.Mock(return_value=False)
        bot._execute_exit_pipeline = mock.Mock(return_value=True)
        return bot, position

    @staticmethod
    def step(bot, position, path):
        if path == "poll":
            return bot.check_trailing_tp(position)
        before = bot._execute_exit_pipeline.call_count
        bot._ws_risk_step()
        return bot._execute_exit_pipeline.call_count > before

    def test_incident_does_not_activate_or_exit_on_historical_peak(self):
        for path in ("poll", "ws"):
            with self.subTest(path=path):
                bot, position = self.make_case()
                self.assertFalse(self.step(bot, position, path))
                self.assertFalse(bot.state.activated)
                bot._arm_protective_stop.assert_not_called()

    def test_current_positive_but_below_threshold_is_not_enough(self):
        for path in ("poll", "ws"):
            with self.subTest(path=path):
                bot, position = self.make_case(profit=0.004)
                self.assertFalse(self.step(bot, position, path))
                self.assertFalse(bot.state.activated)

    def test_activation_at_threshold_starts_a_new_tracking_peak(self):
        for path in ("poll", "ws"):
            with self.subTest(path=path):
                bot, position = self.make_case(profit=0.005, history=0.02)
                self.assertFalse(self.step(bot, position, path))
                self.assertTrue(bot.state.activated)
                self.assertAlmostEqual(bot.state.trailing_peak_profit_pct, 0.005)
                self.assertAlmostEqual(bot.state.best_profit_pct, 0.02)
                self.assertAlmostEqual(
                    bot._arm_protective_stop.call_args.kwargs["best_profit_pct"], 0.005
                )
                bot.test_profit = 0.006
                self.assertFalse(self.step(bot, position, path))
                self.assertAlmostEqual(bot.state.trailing_peak_profit_pct, 0.006)
                bot.test_profit = 0.0045
                self.assertTrue(self.step(bot, position, path))

    def test_activated_protection_still_exits_after_profit_turns_negative(self):
        for path in ("poll", "ws"):
            with self.subTest(path=path):
                bot, position = self.make_case(profit=0.006)
                self.assertFalse(self.step(bot, position, path))
                bot.test_profit = -0.0112
                bot._refresh_realtime_risk_context = lambda *a, **kw: None
                self.assertTrue(self.step(bot, position, path))
                self.assertTrue(bot.state.activated)

    def test_legacy_active_state_preserves_existing_protection(self):
        for path in ("poll", "ws"):
            with self.subTest(path=path):
                bot, position = self.make_case(threshold=0.03)
                bot.state.activated = True
                bot.state.active_trailing_drawdown_ratio = 0.24
                self.assertTrue(self.step(bot, position, path))
                self.assertTrue(bot.state.activated)

    def test_hard_stop_is_independent_of_activation(self):
        for path in ("poll", "ws"):
            with self.subTest(path=path):
                bot, position = self.make_case()
                bot.max_loss_pct = 0.005
                self.assertTrue(self.step(bot, position, path))
                self.assertFalse(bot.state.activated)

    def test_half_is_fifty_percent_not_half_percent(self):
        bot, position = self.make_case(profit=-0.0112, history=0.0)
        bot.max_loss_pct = 0.5
        self.assertFalse(bot.check_trailing_tp(position))
        bot.max_loss_pct = 0.005
        self.assertTrue(bot.check_trailing_tp(position))

    def test_nonfinite_profit_cannot_activate_or_poison_peak(self):
        for path in ("poll", "ws"):
            for value in (float("nan"), float("inf")):
                with self.subTest(path=path, value=value):
                    bot, position = self.make_case(profit=value)
                    self.assertFalse(self.step(bot, position, path))
                    self.assertFalse(bot.state.activated)
                    self.assertTrue(math.isfinite(bot.state.best_profit_pct))

    def test_concurrent_activation_does_not_reinitialize_active_peak(self):
        bot, _ = self.make_case(profit=0.005, history=0.02)
        gate = threading.Barrier(2)
        errors = []

        def activate(value):
            try:
                gate.wait(timeout=3)
                bot._advance_trailing_peak(value, 0.005, source="test")
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=activate, args=(value,))
                   for value in (0.005, 0.006)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(bot.state.activated)
        self.assertAlmostEqual(bot.state.trailing_peak_profit_pct, 0.006)


class TrailingStatePersistenceTests(unittest.TestCase):
    def test_new_peak_survives_restart_without_inheriting_history(self):
        bot = fixtures.TrailingTakeProfitTests._configured_bot()
        bot._advance_trailing_peak(0.005, 0.005)
        bot.state.best_profit_pct = 0.02
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.json"
            path.write_text(json.dumps(asdict(bot.state)), encoding="utf-8")
            other = fixtures.TrailingTakeProfitTests._configured_bot()
            other.runtime_file = path
            other._repair_phase2_start_layer = lambda: False
            other._load_runtime_state()
            self.assertTrue(other.state.activated)
            self.assertAlmostEqual(other.state.trailing_peak_profit_pct, 0.005)
            self.assertAlmostEqual(other._advance_trailing_peak(0.004, 0.03), 0.005)

    def test_legacy_inactive_restart_does_not_activate_on_old_peak(self):
        bot = fixtures.TrailingTakeProfitTests._configured_bot()
        raw = asdict(bot.state)
        raw.pop("trailing_peak_profit_pct")
        raw["best_profit_pct"] = 0.0055
        with tempfile.TemporaryDirectory() as directory:
            bot.runtime_file = Path(directory) / "runtime.json"
            bot.runtime_file.write_text(json.dumps(raw), encoding="utf-8")
            bot._repair_phase2_start_layer = lambda: False
            bot._load_runtime_state()
            self.assertIsNone(bot._advance_trailing_peak(-0.0112, 0.005))
            self.assertFalse(bot.state.activated)

    def test_direction_change_clears_active_peak(self):
        bot = fixtures.TrailingTakeProfitTests._configured_bot()
        bot._advance_trailing_peak(0.005, 0.005)
        bot._force_sync_position_side({"side": "short", "contracts": 1, "entryPrice": 100})
        self.assertFalse(bot.state.activated)
        self.assertIsNone(bot.state.trailing_peak_profit_pct)

    def test_confirmed_cycle_reset_clears_active_peak(self):
        bot = fixtures.TrailingTakeProfitTests._configured_bot()
        bot._advance_trailing_peak(0.005, 0.005)
        bot._clear_protective_stop = lambda **kwargs: True
        self.assertTrue(bot._reset_state())
        self.assertFalse(bot.state.activated)
        self.assertIsNone(bot.state.trailing_peak_profit_pct)


class ProtectiveStopActivationBoundaryTests(unittest.TestCase):
    def test_stop_is_submitted_at_half_percent_activation(self):
        bot, position = fixtures.ProtectiveStopSafetyTests._configured_bot()
        bot.protective_stop_min_profit_pct = 0.005
        bot.exchange.place_position_stop_loss = mock.Mock(return_value={"id": "stop-boundary"})
        self.assertTrue(bot._arm_protective_stop(position, 0.005, "test", best_profit_pct=0.005, trail_ratio=0.24))
        bot.exchange.place_position_stop_loss.assert_called_once()
        self.assertTrue(bot.state.protective_stop_active)

    def test_small_positive_profit_is_not_blocked_by_fixed_cushion(self):
        bot, position = fixtures.ProtectiveStopSafetyTests._configured_bot()
        target = bot._protective_stop_target(position, 0.0005, 0.005, 0.24)
        self.assertIsNotNone(target)
        self.assertGreater(target["locked_profit_pct"], 0)
        self.assertLess(target["locked_profit_pct"], 0.0005)

    def test_nonpositive_or_invalid_profit_never_creates_profit_stop(self):
        bot, position = fixtures.ProtectiveStopSafetyTests._configured_bot()
        for profit in (0, -0.0112, float("nan"), float("inf")):
            with self.subTest(profit=profit):
                self.assertIsNone(bot._protective_stop_target(position, profit, 0.0055, 0.24))


if __name__ == "__main__":
    unittest.main()
