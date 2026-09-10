"""No-network regression coverage of signed Gate ledger accounting."""

from datetime import datetime, timedelta
import unittest

from trading.ledger import ledger_category, normalize_ledger_entry
from dashboard.data_provider import DashboardService, DashboardBotProfile


class LedgerAccountingTests(unittest.TestCase):
    def test_ccxt_outflow_magnitude_is_negative(self):
        result = normalize_ledger_entry({"amount": 0.42873, "direction": "out", "type": "trade"})
        self.assertEqual(result["signed_amount"], -0.42873)
        self.assertEqual(result["amount"], 0.42873)
        self.assertTrue(result["sign_resolved"])

    def test_raw_gate_change_and_type_survive(self):
        row = {"id": "test", "amount": 0.42873, "type": "trade",
               "info": {"change": "-0.42873", "type": "pnl", "text": "BTC_USDT:test"}}
        result = normalize_ledger_entry(row)
        self.assertEqual(result["signed_amount"], -0.42873)
        self.assertEqual(result["raw_type"], "pnl")
        self.assertEqual(result["info"], row["info"])
        self.assertEqual(ledger_category(result), "pnl")
        self.assertNotIn("signed_amount", row)

    def test_inflow_and_negative_legacy_are_not_sign_flipped(self):
        self.assertEqual(normalize_ledger_entry({"amount": 2, "direction": "in"})["signed_amount"], 2)
        self.assertEqual(normalize_ledger_entry({"amount": -2})["signed_amount"], -2)

    def test_legacy_balance_delta_recovers_sign_only_when_it_matches(self):
        row = {"amount": 2, "before": 100, "after": 98, "type": "pnl"}
        self.assertEqual(normalize_ledger_entry(row)["signed_amount"], -2)
        for row in ({"amount": 2, "before": 0, "after": 0},
                    {"amount": 2, "before": 100, "after": 95},
                    {"amount": 2}, {"amount": float("nan")}, {"amount": float("inf")}):
            with self.subTest(row=row):
                self.assertIsNone(normalize_ledger_entry(row)["signed_amount"])

    def test_normalization_is_idempotent(self):
        row = normalize_ledger_entry({"amount": 2, "before": 100, "after": 98})
        self.assertEqual(normalize_ledger_entry(row), row)

    @staticmethod
    def performance(rows):
        service = DashboardService.__new__(DashboardService)
        service.bot = DashboardBotProfile({"settleCoin": "USDT"})
        return service._build_performance({"total": 100}, None, [], rows, 78000)

    @staticmethod
    def row(change, raw_type, **kwargs):
        return {"timestamp": datetime.now().isoformat(), "currency": "USDT",
                "amount": abs(change), "direction": "in" if change >= 0 else "out",
                "type": "trade" if raw_type == "pnl" else "fee",
                "info": {"change": str(change), "type": raw_type}, **kwargs}

    def test_incident_net_is_minus_half_a_dollar_not_a_win(self):
        result = self.performance([self.row(-0.42873, "pnl"), self.row(-0.0915, "fee"), self.row(0.0029, "fund")])
        self.assertAlmostEqual(result["realized_pnl_24h"], -0.42873)
        self.assertAlmostEqual(result["trading_fees_24h"], -0.0915)
        self.assertAlmostEqual(result["funding_pnl_24h"], 0.0029)
        self.assertAlmostEqual(result["net_pnl_24h"], -0.51733)
        self.assertEqual(result["ledger_unresolved_24h"], 0)
        self.assertFalse(result["ledger_window_covers_24h"])

    def test_ambiguous_legacy_record_is_not_counted_as_profit(self):
        now = datetime.now().isoformat()
        result = self.performance([{"timestamp": now, "type": "pnl", "amount": 0.42873},
                                   {"timestamp": now, "type": "trade", "amount": 0.42873, "direction": "out"}])
        self.assertEqual(result["realized_pnl_24h"], 0)
        self.assertEqual(result["ledger_unresolved_24h"], 2)

    def test_window_currency_and_transfers_are_not_mixed_with_profit(self):
        old = (datetime.now() - timedelta(hours=25)).isoformat()
        result = self.performance([self.row(-2, "pnl", timestamp=old),
                                   self.row(10, "pnl", currency="BTC"),
                                   self.row(100, "dnw"), self.row(1, "pnl")])
        self.assertEqual(result["net_pnl_24h"], 1)
        self.assertTrue(result["ledger_window_covers_24h"])

    def test_missing_time_cannot_be_reported_as_24h_profit(self):
        result = self.performance([self.row(10, "pnl", timestamp=None)])
        self.assertEqual(result["net_pnl_24h"], 0)
        self.assertEqual(result["ledger_unresolved_24h"], 1)

    def test_real_ccxt_gate_parser_preserves_pnl_through_normalization(self):
        import ccxt
        exchange = ccxt.gate()  # Construct parser only; no keys or network calls.
        parsed = exchange.parse_ledger_entry(
            {"id": "fake-pnl", "time": "1788993300", "change": "-0.42873",
             "balance": "100", "type": "pnl", "currency": "USDT"}
        )
        self.assertEqual(parsed["direction"], "out")
        normalized = normalize_ledger_entry(parsed)
        self.assertEqual(normalized["signed_amount"], -0.42873)
        self.assertEqual(ledger_category(normalized), "pnl")

    def test_bot_snapshot_preserves_accounting_evidence(self):
        import test_last_fill_safety as fixtures
        from unittest import mock
        bot = fixtures.make_bot()
        row = {"timestamp": 1788993300000, "amount": 0.42873, "direction": "out",
               "type": "trade", "id": "fake-loss", "before": None, "after": None,
               "info": {"change": "-0.42873", "type": "pnl"}}
        bot.exchange.fetch_ledger = mock.Mock(return_value=[row])
        result = bot._snapshot_ledger()[0]
        self.assertEqual(result["signed_amount"], -0.42873)
        self.assertEqual(result["direction"], "out")
        self.assertEqual(result["raw_type"], "pnl")
        self.assertIsNone(result["before"])
        self.assertEqual(result["id"], "fake-loss")


if __name__ == "__main__":
    unittest.main()
