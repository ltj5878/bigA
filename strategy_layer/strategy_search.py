"""激进 Alpha 参数预设搜索。

固定少量预设，避免一次性网格搜索过慢。排序优先看年化收益，
再看回撤、单月 10%+ 次数和夏普。
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from .backtest import run_backtest


PRESETS: list[dict[str, Any]] = [
    {
        "name": "balanced_10",
        "label": "均衡激进 Top10",
        "top_n": 10,
        "max_per_industry": 2,
        "volatility_penalty": 0.2,
        "aggressive_weights": None,
    },
    {
        "name": "concentrated_5",
        "label": "集中冲击 Top5",
        "top_n": 5,
        "max_per_industry": 1,
        "volatility_penalty": 0.1,
        "aggressive_weights": {
            "momentum20": 0.25,
            "momentum60": 0.30,
            "momentum120": 0.20,
            "momentum12_1": 0.20,
            "ma120_bias": 0.15,
            "roe": 0.03,
            "sentiment": 0.12,
        },
    },
    {
        "name": "momentum_10",
        "label": "强动量 Top10",
        "top_n": 10,
        "max_per_industry": 2,
        "volatility_penalty": 0.1,
        "aggressive_weights": {
            "momentum20": 0.25,
            "momentum60": 0.35,
            "momentum120": 0.25,
            "momentum12_1": 0.25,
            "ma120_bias": 0.15,
            "roe": 0.02,
            "sentiment": 0.10,
        },
    },
    {
        "name": "low_vol_10",
        "label": "低波动 Top10",
        "top_n": 10,
        "max_per_industry": 2,
        "volatility_penalty": 0.35,
        "aggressive_weights": None,
    },
    {
        "name": "broad_15",
        "label": "分散 Top15",
        "top_n": 15,
        "max_per_industry": 3,
        "volatility_penalty": 0.2,
        "aggressive_weights": None,
    },
]


def run_aggressive_alpha_search(
    start: str = "2022-01-01",
    end: str | None = None,
    universe_scope: str = "hs300",
    min_mcap_yi: float = 30.0,
    min_f_score: int = 5,
    use_cache: bool = True,
) -> dict:
    """运行固定预设搜索并返回按表现排序的结果。"""
    t0 = time.time()
    end = end or datetime.now().strftime("%Y-%m-%d")
    results = []
    for preset in PRESETS:
        payload = run_backtest(
            start=start,
            end=end,
            strategy="aggressive_alpha",
            top_n=preset["top_n"],
            universe_scope=universe_scope,
            use_timing=False,
            max_per_industry=preset["max_per_industry"],
            min_mcap_yi=min_mcap_yi,
            min_f_score=min_f_score,
            volatility_penalty=preset["volatility_penalty"],
            aggressive_weights=preset["aggressive_weights"],
            preset_name=preset["name"],
            use_cache=use_cache,
        )
        metrics = payload.get("metrics", {})
        results.append({
            "preset": preset["name"],
            "label": preset["label"],
            "params": {
                "top_n": preset["top_n"],
                "max_per_industry": preset["max_per_industry"],
                "volatility_penalty": preset["volatility_penalty"],
            },
            "metrics": metrics,
            "start": payload.get("start", start),
            "end": payload.get("end", end),
        })

    def sort_key(row: dict) -> tuple:
        m = row.get("metrics", {})
        return (
            _num(m.get("annual_return")),
            _num(m.get("max_drawdown")),
            _num(m.get("months_ge_10pct")),
            _num(m.get("sharpe")),
        )

    results = sorted(results, key=sort_key, reverse=True)
    return {
        "strategy": "aggressive_alpha",
        "start": start,
        "end": end,
        "best_preset": results[0]["preset"] if results else None,
        "elapsed_sec": round(time.time() - t0, 1),
        "results": results,
    }


def _num(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("-inf")
