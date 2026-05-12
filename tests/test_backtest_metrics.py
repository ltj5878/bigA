from __future__ import annotations

import inspect
import unittest

import pandas as pd

from strategy_layer.backtest import _calc_metrics, run_backtest


class BacktestMetricsTests(unittest.TestCase):
    def test_monthly_metrics_include_best_worst_win_rate_and_ten_percent_count(self):
        dates = pd.to_datetime([
            "2026-01-01",
            "2026-02-01",
            "2026-03-01",
            "2026-04-01",
        ])
        nav = pd.Series([1.0, 1.12, 1.064, 1.19168], index=dates)
        bench = pd.Series([1.0, 1.01, 1.00, 1.02], index=dates)

        metrics = _calc_metrics(nav, bench, dates[0], dates[-1])

        self.assertEqual(metrics["best_month"], 12.0)
        self.assertEqual(metrics["worst_month"], -5.0)
        self.assertEqual(metrics["months_ge_10pct"], 2)
        self.assertAlmostEqual(metrics["avg_month_return"], 6.33, places=2)
        self.assertAlmostEqual(metrics["win_rate"], 66.67, places=2)

    def test_run_backtest_keeps_three_factor_as_default_strategy(self):
        signature = inspect.signature(run_backtest)

        self.assertIn("strategy", signature.parameters)
        self.assertEqual(signature.parameters["strategy"].default, "three_factor")


if __name__ == "__main__":
    unittest.main()
