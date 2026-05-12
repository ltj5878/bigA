"""三因子策略主入口：Quality + Value + Momentum + F-Score + 大盘择时 + 月调仓对比。

打分公式：
    Q = z(ROE) + z(毛利率) + z(经营现金流/营收)
    V = z(1/PE) + z(1/PB) + z(1/PS)
    M = z(12-1 动量)
    总分 = 0.4·Q + 0.4·V + 0.2·M   （越大越好）

过滤：
    PE > 0 / ROE > 0 / 总市值 ≥ 30 亿 / 非 ST / F-Score ≥ 5

月调仓对比：
    把当前 picks 与"上月相同参数 picks"对比，
    NEW = 本月新加 / DROPPED = 本月剔除
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

from data_layer import cache
from data_layer.config import config
from .factors import fetch_factors, industry_map, sentiment_map
from .f_score import f_score
from .magic_formula import get_universe  # 复用股池逻辑
from .market_timing import hs300_ma200_position

_MODULE = "strategy_three_factor"
_HISTORY_DIR = Path.home() / ".big_a" / "cache" / "strategy_history"
_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _zscore(s: pd.Series) -> pd.Series:
    """横截面 Z-Score：去极值（1%/99% 分位裁剪）后标准化。"""
    s = s.astype(float)
    lo, hi = s.quantile(0.01), s.quantile(0.99)
    s = s.clip(lower=lo, upper=hi)
    mu, sd = s.mean(), s.std()
    if sd == 0 or pd.isna(sd):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - mu) / sd


def _enrich_one(symbol: str, name: str, use_cache: bool) -> dict | None:
    """单股：拉因子 + 算 F-Score，返回扁平字典，失败返 None。"""
    try:
        f = fetch_factors(symbol, name, use_cache=use_cache)
        score, items = f_score(f)
        f["f_score"] = score
        f["f_score_items"] = items
        return f
    except Exception as e:
        logger.debug(f"_enrich_one {symbol} 失败: {e}")
        return None


def _load_previous(cache_key: str) -> pd.DataFrame | None:
    """加载上一次跑同样参数的 picks（如果存在）。"""
    cur = _HISTORY_DIR / f"{cache_key}.parquet"
    prev = _HISTORY_DIR / f"{cache_key}.prev.parquet"
    if prev.exists():
        try: return pd.read_parquet(prev)
        except Exception: return None
    if cur.exists():
        try: return pd.read_parquet(cur)
        except Exception: return None
    return None


def _save_current(cache_key: str, df: pd.DataFrame):
    """保存当前 picks 为 history（先把旧的存为 .prev）。"""
    cur = _HISTORY_DIR / f"{cache_key}.parquet"
    prev = _HISTORY_DIR / f"{cache_key}.prev.parquet"
    if cur.exists():
        cur.replace(prev)
    df.to_parquet(cur, index=False)


def run_strategy(
    scope: str = "hs300_zz500",
    top_n: int = 25,
    min_mcap_yi: float = 30.0,
    min_f_score: int = 5,
    exclude_st: bool = True,
    use_cache: bool = True,
    max_workers: int = 16,
    weight_q: float = 0.35,
    weight_v: float = 0.35,
    weight_m: float = 0.15,
    weight_s: float = 0.15,         # 研报情绪权重
    max_per_industry: int = 3,      # 单一申万一级行业最大入选数；0 表示不限制
) -> dict:
    """跑四因子选股 + F-Score 排雷 + 行业中性化 + 大盘择时 + 月调仓对比。

    四因子：质量 Q · 价值 V · 动量 M · 研报情绪 S（机构覆盖+评级变动加权）。
    """
    t0 = time.time()
    cache_key = (
        f"picks_{scope}_top{top_n}_mcap{min_mcap_yi}_f{min_f_score}_st{int(exclude_st)}"
        f"_q{weight_q}_v{weight_v}_m{weight_m}_s{weight_s}_ind{max_per_industry}"
    )

    # ---- 结果级缓存（6 小时）----
    if use_cache:
        cached = cache.read(_MODULE, cache_key, max_age_sec=60 * 60 * 6)
        if cached is not None and not cached.empty:
            return {
                "as_of":     str(datetime.now()),
                "from_cache": True,
                "picks":     cached.to_dict(orient="records"),
                "timing":    hs300_ma200_position(),
                "changes":   _changes_payload(cache_key, cached),
                "stats":     {"cached": True, "elapsed_sec": round(time.time() - t0, 2)},
            }

    # ---- 拉股池 ----
    universe = get_universe(scope)
    if not universe:
        return {"as_of": str(datetime.now()), "picks": [], "timing": hs300_ma200_position(),
                "stats": {"err": "股池为空"}}

    if exclude_st:
        universe = [u for u in universe if "ST" not in (u.get("name") or "").upper()]

    # ---- 预加载：行业 + 全市场情绪 ----
    ind_map = industry_map(use_cache=use_cache)
    sent_map = sentiment_map(use_cache=use_cache)

    logger.info(f"四因子策略：股池 {len(universe)} 只，行业 {len(ind_map)} 只，情绪覆盖 {len(sent_map)} 只")

    # ---- 并发拉单股因子 ----
    enriched: list[dict] = []
    total = len(universe); done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(_enrich_one, u["symbol"], u.get("name", ""), use_cache): u
                for u in universe}
        for fut in as_completed(futs):
            done += 1
            try:
                f = fut.result()
            except Exception:
                f = None
            if f: enriched.append(f)

    df = pd.DataFrame(enriched)
    raw_count = len(df)
    if df.empty:
        return {"as_of": str(datetime.now()), "picks": [], "timing": hs300_ma200_position(),
                "stats": {"universe": total, "raw": 0}}

    # ---- 数据完整性过滤 ----
    needed = ["pe_ttm", "pb", "ps", "mcap", "roe", "gross_margin", "ocf_revenue"]
    df = df.dropna(subset=needed)
    df = df[df["pe_ttm"] > 0]
    df = df[df["pb"] > 0]
    df = df[df["ps"] > 0]
    df = df[df["roe"] > 0]
    if min_mcap_yi:
        df = df[df["mcap"] >= min_mcap_yi * 1e8]
    df = df[df["f_score"] >= min_f_score]
    cleaned_count = len(df)
    if df.empty:
        return {"as_of": str(datetime.now()), "picks": [], "timing": hs300_ma200_position(),
                "stats": {"universe": total, "raw": raw_count, "cleaned": 0,
                          "elapsed_sec": round(time.time() - t0, 1)}}

    df = df.reset_index(drop=True)
    # 注入行业 + 情绪（factors 里也有但用预加载映射更稳）
    df["industry"]  = df["symbol"].map(ind_map).fillna("其他")
    df["sentiment"] = df["symbol"].map(sent_map).fillna(0.0)

    # ---- Z-Score 因子 ----
    df["z_roe"]  = _zscore(df["roe"])
    df["z_gm"]   = _zscore(df["gross_margin"])
    df["z_ocf"]  = _zscore(df["ocf_revenue"])
    df["Q"] = df["z_roe"] + df["z_gm"] + df["z_ocf"]

    df["z_ey"]   = _zscore(1 / df["pe_ttm"])
    df["z_bp"]   = _zscore(1 / df["pb"])
    df["z_sp"]   = _zscore(1 / df["ps"])
    df["V"] = df["z_ey"] + df["z_bp"] + df["z_sp"]

    df["momentum12_1"] = df["momentum12_1"].fillna(0)
    df["z_mom"] = _zscore(df["momentum12_1"])
    df["M"] = df["z_mom"]

    df["z_sent"] = _zscore(df["sentiment"])
    df["S"] = df["z_sent"]

    # ---- 总分 ----
    df["total_score"] = (weight_q * df["Q"] + weight_v * df["V"] +
                         weight_m * df["M"] + weight_s * df["S"])

    df = df.sort_values("total_score", ascending=False).reset_index(drop=True)

    # ---- 行业中性化：按总分降序遍历，单行业不超过 max_per_industry ----
    if max_per_industry and max_per_industry > 0:
        from collections import Counter
        counts: Counter = Counter()
        keep_idx = []
        for i, row in df.iterrows():
            ind = row["industry"]
            if counts[ind] < max_per_industry:
                keep_idx.append(i)
                counts[ind] += 1
            if len(keep_idx) >= top_n:
                break
        top = df.loc[keep_idx].reset_index(drop=True).copy()
    else:
        top = df.head(top_n).copy()

    top["pick_rank"] = top.index + 1

    # ---- 美化输出 ----
    top["pe_ttm"]       = top["pe_ttm"].round(2)
    top["pb"]           = top["pb"].round(2)
    top["ps"]           = top["ps"].round(2)
    top["roe"]          = top["roe"].round(2)
    top["gross_margin"] = top["gross_margin"].round(2)
    top["ocf_revenue"]  = top["ocf_revenue"].round(2)
    top["mcap_yi"]      = (top["mcap"] / 1e8).round(1)
    top["momentum12_1"] = top["momentum12_1"].round(2)
    top["sentiment"]    = top["sentiment"].round(1)
    top["Q"] = top["Q"].round(2)
    top["V"] = top["V"].round(2)
    top["M"] = top["M"].round(2)
    top["S"] = top["S"].round(2)
    top["total_score"] = top["total_score"].round(3)

    out = top[[
        "pick_rank", "symbol", "name", "close", "industry",
        "pe_ttm", "pb", "ps",
        "roe", "gross_margin", "ocf_revenue",
        "momentum12_1", "sentiment", "f_score",
        "mcap_yi", "Q", "V", "M", "S", "total_score", "asof",
    ]].rename(columns={
        "pick_rank":    "排名",
        "symbol":       "代码",
        "name":         "名称",
        "close":        "股价",
        "industry":     "行业",
        "pe_ttm":       "市盈率",
        "pb":           "市净率",
        "ps":           "市销率",
        "roe":          "ROE%",
        "gross_margin": "毛利率%",
        "ocf_revenue":  "现金流/营收%",
        "momentum12_1": "12-1动量%",
        "sentiment":    "研报情绪",
        "f_score":      "F-Score",
        "mcap_yi":      "总市值(亿)",
        "Q":            "质量分",
        "V":            "价值分",
        "M":            "动量分",
        "S":            "情绪分",
        "total_score":  "综合分",
        "asof":         "数据日期",
    })

    # 结果缓存 + 历史快照
    cache.write(_MODULE, cache_key, out)
    _save_current(cache_key, out)

    return {
        "as_of":      str(datetime.now()),
        "from_cache": False,
        "picks":      out.to_dict(orient="records"),
        "timing":     hs300_ma200_position(),
        "changes":    _changes_payload(cache_key, out),
        "stats": {
            "universe":    total,
            "raw":         raw_count,
            "cleaned":     cleaned_count,
            "top_n":       top_n,
            "scope":       scope,
            "elapsed_sec": round(time.time() - t0, 1),
        },
    }


def _changes_payload(cache_key: str, current: pd.DataFrame) -> dict:
    """对比上一次同参数 picks，给出 NEW / DROPPED 列表。"""
    prev = _load_previous(cache_key)
    if prev is None or "代码" not in prev.columns or "代码" not in current.columns:
        return {"new": [], "dropped": [], "kept": []}
    cur_codes  = set(current["代码"].tolist())
    prev_codes = set(prev["代码"].tolist())
    new     = sorted(cur_codes - prev_codes)
    dropped = sorted(prev_codes - cur_codes)
    kept    = sorted(cur_codes & prev_codes)
    return {"new": new, "dropped": dropped, "kept": kept}
