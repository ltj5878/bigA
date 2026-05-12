from __future__ import annotations

import unittest
from unittest.mock import patch

from strategy_layer.strategy_search import PRESETS, run_aggressive_alpha_search


class StrategySearchTests(unittest.TestCase):
    def test_runs_fixed_presets_and_sorts_best_first(self):
        calls = []

        def fake_backtest(**kwargs):
            calls.append(kwargs)
            annual_by_preset = {
                "balanced_10": 11.0,
                "concentrated_5": 8.0,
                "momentum_10": 18.0,
                "low_vol_10": 10.0,
                "broad_15": 12.0,
            }
            annual = annual_by_preset[kwargs["preset_name"]]
            return {
                "start": kwargs["start"],
                "end": kwargs["end"],
                "metrics": {
                    "annual_return": annual,
                    "max_drawdown": -20.0 + annual / 10,
                    "months_ge_10pct": 1 if annual >= 12.0 else 0,
                    "sharpe": annual / 10,
                },
            }

        with patch("strategy_layer.strategy_search.run_backtest", side_effect=fake_backtest):
            result = run_aggressive_alpha_search(
                start="2022-01-01",
                end="2026-05-12",
                universe_scope="hs300",
                use_cache=False,
            )

        self.assertEqual(result["strategy"], "aggressive_alpha")
        self.assertEqual(result["best_preset"], "momentum_10")
        self.assertEqual([r["preset"] for r in result["results"]][0], "momentum_10")
        self.assertEqual(len(result["results"]), len(PRESETS))
        self.assertTrue(all(c["strategy"] == "aggressive_alpha" for c in calls))
        self.assertTrue(all(c["use_timing"] is False for c in calls))


if __name__ == "__main__":
    unittest.main()
