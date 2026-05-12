from __future__ import annotations

import unittest

import pandas as pd

from strategy_layer.aggressive_alpha import _score_candidates, _zscore


def _row(symbol: str, name: str, industry: str, **overrides):
    base = {
        "symbol": symbol,
        "name": name,
        "close": 10.0,
        "industry": industry,
        "pe_ttm": 20.0,
        "pb": 2.0,
        "ps": 3.0,
        "mcap": 80e8,
        "roe": 8.0,
        "sentiment": 0.0,
        "f_score": 6,
        "momentum20": 5.0,
        "momentum60": 15.0,
        "momentum120": 30.0,
        "momentum12_1": 40.0,
        "ma120_bias": 8.0,
        "volatility60": 20.0,
        "asof": "2026-05-12",
    }
    base.update(overrides)
    return base


class AggressiveAlphaTests(unittest.TestCase):
    def test_zscore_preserves_order_and_centers_values(self):
        scored = _zscore(pd.Series([1.0, 2.0, 3.0]))

        self.assertAlmostEqual(float(scored.mean()), 0.0, places=7)
        self.assertLess(scored.iloc[0], scored.iloc[1])
        self.assertLess(scored.iloc[1], scored.iloc[2])

    def test_low_volatility_momentum_beats_high_volatility_chase(self):
        frame = pd.DataFrame([
            _row(
                "000001", "低波动趋势", "通信",
                momentum20=12.0, momentum60=35.0, momentum120=55.0,
                momentum12_1=80.0, ma120_bias=15.0, volatility60=14.0,
            ),
            _row(
                "000002", "高波动追涨", "通信",
                momentum20=14.0, momentum60=39.0, momentum120=62.0,
                momentum12_1=90.0, ma120_bias=18.0, volatility60=95.0,
            ),
        ])

        selected = _score_candidates(
            frame, top_n=2, max_per_industry=0, volatility_penalty=1.0
        )

        self.assertEqual(selected.iloc[0]["symbol"], "000001")
        self.assertGreater(
            selected.iloc[0]["aggressive_score"],
            selected.iloc[1]["aggressive_score"],
        )

    def test_filters_st_low_fscore_and_applies_industry_cap(self):
        frame = pd.DataFrame([
            _row("000001", "行业冠军", "传媒", momentum60=40.0),
            _row("000002", "同业第二", "传媒", momentum60=35.0),
            _row("000003", "跨行业入选", "通信", momentum60=25.0),
            _row("000004", "ST问题股", "医药生物", momentum60=80.0),
            _row("000005", "低F分", "电子", momentum60=75.0, f_score=3),
        ])

        selected = _score_candidates(
            frame,
            top_n=3,
            max_per_industry=1,
            min_f_score=5,
            exclude_st=True,
            volatility_penalty=0.2,
        )

        self.assertEqual(selected["symbol"].tolist(), ["000001", "000003"])
        self.assertNotIn("000004", selected["symbol"].tolist())
        self.assertNotIn("000005", selected["symbol"].tolist())


if __name__ == "__main__":
    unittest.main()
