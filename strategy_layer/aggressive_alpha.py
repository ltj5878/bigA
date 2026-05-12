"""激进 Alpha 策略：趋势/动量主导，叠加质量过滤和波动惩罚。

该策略用于研究高弹性组合，不承诺收益。默认更集中、更偏趋势，
所以必须和回撤、月度亏损、胜率一起看。
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from data_layer import cache
from .f_score import f_score
from .factors import fetch_factors, industry_map, sentiment_map
from .magic_formula import get_universe
from .market_timing import hs300_ma200_position

_MODULE = "strategy_aggressive_alpha"

DEFAULT_WEIGHTS = {
    "momentum20": 0.20,
    "momentum60": 0.25,
    "momentum120": 0.20,
    "momentum12_1": 0.20,
    "ma120_bias": 0.10,
    "roe": 0.05,
    "sentiment": 0.10,
}


def _zscore(s: pd.Series) -> pd.Series:
    """横截面去极值 Z-Score。缺失/无波动时返回 0。"""
    s = pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan)
    if s.notna().sum() == 0:
        return pd.Series([0.0] * len(s), index=s.index)
    s = s.fillna(s.median())
    if len(s) >= 4:
        lo, hi = s.quantile(0.01), s.quantile(0.99)
        s = s.clip(lower=lo, upper=hi)
    sd = s.std()
    if sd == 0 or pd.isna(sd):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - s.mean()) / sd


def price_metrics_from_close(close: pd.Series) -> dict[str, float | None]:
    """从收盘价序列计算趋势和波动指标，单位均为百分比。"""
    if close is None or len(close) == 0:
        return _empty_price_metrics()
    close = pd.to_numeric(close, errors="coerce").dropna().astype(float)
    if close.empty:
        return _empty_price_metrics()

    def momentum(days: int) -> float | None:
        if len(close) < days + 1:
            return None
        base = float(close.iloc[-days - 1])
        if base <= 0:
            return None
        return (float(close.iloc[-1]) / base - 1) * 100

    ma120_bias = None
    if len(close) >= 120:
        ma120 = float(close.tail(120).mean())
        if ma120 > 0:
            ma120_bias = (float(close.iloc[-1]) / ma120 - 1) * 100

    volatility60 = None
    if len(close) >= 61:
        rets = close.pct_change().dropna().tail(60)
        if not rets.empty:
            volatility60 = float(rets.std() * np.sqrt(252) * 100)

    return {
        "momentum20": momentum(20),
        "momentum60": momentum(60),
        "momentum120": momentum(120),
        "momentum12_1": _momentum_12_1(close),
        "ma120_bias": ma120_bias,
        "volatility60": volatility60,
    }


def _empty_price_metrics() -> dict[str, None]:
    return {
        "momentum20": None,
        "momentum60": None,
        "momentum120": None,
        "momentum12_1": None,
        "ma120_bias": None,
        "volatility60": None,
    }


def _momentum_12_1(close: pd.Series) -> float | None:
    if len(close) < 252:
        return None
    base = float(close.iloc[-252])
    if base <= 0:
        return None
    return (float(close.iloc[-21]) / base - 1) * 100


def _score_candidates(
    frame: pd.DataFrame,
    top_n: int = 10,
    min_mcap_yi: float = 30.0,
    min_f_score: int = 5,
    exclude_st: bool = True,
    max_per_industry: int = 2,
    volatility_penalty: float = 0.2,
    weights: dict[str, float] | None = None,
) -> pd.DataFrame:
    """过滤、打分并按行业上限选出激进 Alpha 候选股。"""
    if frame is None or frame.empty:
        return pd.DataFrame()
    weights = {**DEFAULT_WEIGHTS, **(weights or {})}
    df = frame.copy()

    if exclude_st:
        df = df[~df["name"].fillna("").astype(str).str.upper().str.contains("ST")]

    required = ["pe_ttm", "pb", "ps", "roe", "f_score"]
    if "mcap" in df.columns and min_mcap_yi:
        required.append("mcap")
    df = df.dropna(subset=[c for c in required if c in df.columns])
    df = df[(df["pe_ttm"] > 0) & (df["pb"] > 0) & (df["ps"] > 0) & (df["roe"] > 0)]
    df = df[df["f_score"] >= min_f_score]
    if "mcap" in df.columns and min_mcap_yi:
        df = df[df["mcap"] >= min_mcap_yi * 1e8]
    if df.empty:
        return df

    for col in [
        "momentum20", "momentum60", "momentum120",
        "momentum12_1", "ma120_bias", "sentiment", "volatility60",
    ]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["aggressive_score"] = 0.0
    for col, weight in weights.items():
        if col not in df.columns:
            df[col] = 0.0
        df["aggressive_score"] += weight * _zscore(df[col])

    df["volatility_penalty_score"] = volatility_penalty * _zscore(df["volatility60"])
    df["aggressive_score"] -= df["volatility_penalty_score"]
    df = df.sort_values("aggressive_score", ascending=False).reset_index(drop=True)

    if max_per_industry and max_per_industry > 0:
        from collections import Counter
        counts: Counter = Counter()
        keep_idx = []
        for i, row in df.iterrows():
            industry = row.get("industry", "其他")
            if counts[industry] < max_per_industry:
                keep_idx.append(i)
                counts[industry] += 1
            if len(keep_idx) >= top_n:
                break
        df = df.loc[keep_idx].reset_index(drop=True)
    else:
        df = df.head(top_n).reset_index(drop=True)

    return df


def _daily_close(symbol: str) -> pd.Series | None:
    try:
        from data_layer.quotes.mootdx_provider import MootdxQuotes
        df = MootdxQuotes().daily(symbol)
        if df is None or df.empty or "close" not in df.columns:
            return None
        return pd.to_numeric(df["close"], errors="coerce").dropna()
    except Exception as e:
        logger.debug(f"aggressive daily close {symbol} 失败: {e}")
        return None


def _enrich_one(symbol: str, name: str, use_cache: bool) -> dict[str, Any] | None:
    try:
        factors = fetch_factors(symbol, name, use_cache=use_cache)
        score, items = f_score(factors)
        factors["f_score"] = score
        factors["f_score_items"] = items
        factors.update(price_metrics_from_close(_daily_close(symbol)))
        return factors
    except Exception as e:
        logger.debug(f"aggressive enrich {symbol} 失败: {e}")
        return None


def run_strategy(
    scope: str = "hs300_zz500",
    top_n: int = 10,
    min_mcap_yi: float = 30.0,
    min_f_score: int = 5,
    exclude_st: bool = True,
    use_cache: bool = True,
    max_workers: int = 16,
    max_per_industry: int = 2,
    volatility_penalty: float = 0.2,
    weights: dict[str, float] | None = None,
) -> dict:
    """运行激进 Alpha 当前选股。"""
    t0 = time.time()
    weight_key = "_".join(f"{k}{v}" for k, v in sorted((weights or {}).items()))
    cache_key = (
        f"picks_{scope}_top{top_n}_mcap{min_mcap_yi}_f{min_f_score}"
        f"_st{int(exclude_st)}_ind{max_per_industry}_vol{volatility_penalty}_{weight_key}"
    )
    if use_cache:
        cached = cache.read(_MODULE, cache_key, max_age_sec=60 * 60 * 6)
        if cached is not None and not cached.empty:
            return {
                "as_of": str(datetime.now()),
                "from_cache": True,
                "picks": cached.to_dict(orient="records"),
                "timing": hs300_ma200_position(),
                "stats": {"cached": True, "elapsed_sec": round(time.time() - t0, 2)},
            }

    universe = get_universe(scope)
    if exclude_st:
        universe = [u for u in universe if "ST" not in (u.get("name") or "").upper()]
    if not universe:
        return {"as_of": str(datetime.now()), "picks": [], "stats": {"err": "股池为空"}}

    ind_map = industry_map(use_cache=use_cache)
    sent_map = sentiment_map(use_cache=use_cache)
    logger.info(f"激进 Alpha：股池 {len(universe)} 只，开始并发拉取")

    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = [pool.submit(_enrich_one, u["symbol"], u.get("name", ""), use_cache) for u in universe]
        for fut in as_completed(futs):
            row = fut.result()
            if row:
                rows.append(row)

    df = pd.DataFrame(rows)
    raw_count = len(df)
    if df.empty:
        return {"as_of": str(datetime.now()), "picks": [], "stats": {"universe": len(universe), "raw": 0}}

    df["industry"] = df["symbol"].map(ind_map).fillna("其他")
    df["sentiment"] = df["symbol"].map(sent_map).fillna(df.get("sentiment", 0.0)).fillna(0.0)
    selected = _score_candidates(
        df,
        top_n=top_n,
        min_mcap_yi=min_mcap_yi,
        min_f_score=min_f_score,
        exclude_st=exclude_st,
        max_per_industry=max_per_industry,
        volatility_penalty=volatility_penalty,
        weights=weights,
    )

    if selected.empty:
        return {
            "as_of": str(datetime.now()),
            "picks": [],
            "timing": hs300_ma200_position(),
            "stats": {"universe": len(universe), "raw": raw_count, "cleaned": 0},
        }

    out = _format_output(selected)
    cache.write(_MODULE, cache_key, out)
    return {
        "as_of": str(datetime.now()),
        "from_cache": False,
        "picks": out.to_dict(orient="records"),
        "timing": hs300_ma200_position(),
        "stats": {
            "universe": len(universe),
            "raw": raw_count,
            "cleaned": len(selected),
            "elapsed_sec": round(time.time() - t0, 1),
            "profile": "aggressive_alpha",
        },
    }


def _format_output(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().reset_index(drop=True)
    out["排名"] = out.index + 1
    out["总市值(亿)"] = (out.get("mcap", 0) / 1e8).round(1)
    rename = {
        "symbol": "代码",
        "name": "名称",
        "close": "股价",
        "industry": "行业",
        "pe_ttm": "市盈率",
        "pb": "市净率",
        "ps": "市销率",
        "roe": "ROE%",
        "f_score": "F-Score",
        "momentum20": "20日动量%",
        "momentum60": "60日动量%",
        "momentum120": "120日动量%",
        "momentum12_1": "12-1动量%",
        "ma120_bias": "120日均线偏离%",
        "volatility_penalty_score": "波动惩罚",
        "aggressive_score": "激进分",
        "asof": "数据日期",
    }
    out = out.rename(columns=rename)
    columns = [
        "排名", "代码", "名称", "股价", "行业", "市盈率", "市净率", "市销率",
        "ROE%", "F-Score", "总市值(亿)", "20日动量%", "60日动量%",
        "120日动量%", "12-1动量%", "120日均线偏离%", "波动惩罚", "激进分", "数据日期",
    ]
    for col in columns:
        if col not in out.columns:
            out[col] = None
    for col in [
        "股价", "市盈率", "市净率", "市销率", "ROE%", "20日动量%", "60日动量%",
        "120日动量%", "12-1动量%", "120日均线偏离%", "波动惩罚", "激进分",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce").round(2)
    return out[columns]
