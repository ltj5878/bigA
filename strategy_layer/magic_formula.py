"""Greenblatt 魔法公式 + 60 日动量加分（A 股适配版）。

原版：
    Earnings Yield = EBIT / EV          → 取倒数：估值越低分越高
    ROIC           = EBIT / 投入资本    → 资本回报越高分越高
A 股替代：
    Earnings Yield ≈ 1 / PE(TTM)
    ROIC           ≈ ROE(加权)
    Momentum       ≈ 60 日累计涨跌幅（同分时二级排序 + 加分）

总分 = rank(1/PE 升序) + rank(ROE 降序排名升序) − momentum_bonus
最终按总分升序取前 N 只（"又便宜又赚钱、且趋势向上"）。

股票池：沪深 300 + 中证 500 去重 ≈ 800 只
过滤：ST/退市/上市 < 1 年/PE<=0/ROE<=0/总市值 < 30 亿/停牌
"""
from __future__ import annotations

import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import pandas as pd
from loguru import logger

from data_layer import cache
from data_layer.config import config

_MODULE = "strategy_magic_formula"


# ----------------------- 股池 -----------------------
def get_universe(scope: str = "hs300_zz500") -> list[dict]:
    """返回 [{symbol, name}]。scope = hs300 / zz500 / hs300_zz500 / all。"""
    cached = cache.read(_MODULE, f"universe_{scope}", max_age_sec=60 * 60 * 24)
    if cached is not None and not cached.empty:
        return cached.to_dict(orient="records")

    import akshare as ak
    parts = []
    if scope in ("hs300", "hs300_zz500"):
        try:
            df = ak.index_stock_cons_csindex(symbol="000300")
            parts.append(df[["成分券代码", "成分券名称"]].rename(
                columns={"成分券代码": "symbol", "成分券名称": "name"}))
        except Exception as e:
            logger.warning(f"取沪深300 成分股失败: {e}")
    if scope in ("zz500", "hs300_zz500"):
        try:
            df = ak.index_stock_cons_csindex(symbol="000905")
            parts.append(df[["成分券代码", "成分券名称"]].rename(
                columns={"成分券代码": "symbol", "成分券名称": "name"}))
        except Exception as e:
            logger.warning(f"取中证500 成分股失败: {e}")
    if scope == "all":
        try:
            spot = ak.stock_zh_a_spot()  # 新浪全市场
            spot = spot[~spot["代码"].str.startswith("bj")]  # 暂排除北交所，财务接口覆盖差
            spot["symbol"] = spot["代码"].str[2:]            # sh600000 → 600000
            parts.append(spot[["symbol", "名称"]].rename(columns={"名称": "name"}))
        except Exception as e:
            logger.warning(f"取全市场失败: {e}")

    if not parts:
        return []
    out = pd.concat(parts, ignore_index=True).drop_duplicates(subset=["symbol"])
    cache.write(_MODULE, f"universe_{scope}", out)
    return out.to_dict(orient="records")


# ----------------------- 单股快照 -----------------------
def _symbol_with_suffix(symbol: str) -> str:
    if symbol.startswith(("60", "688", "900", "9")):
        return f"{symbol}.SH"
    if symbol.startswith(("83", "87", "43", "92")):
        return f"{symbol}.BJ"
    return f"{symbol}.SZ"


def _fetch_value_em(symbol: str) -> dict | None:
    """从 akshare stock_value_em 拿最新 PE(TTM) / 总市值 / 市净率。"""
    import akshare as ak
    try:
        df = ak.stock_value_em(symbol=symbol)
        if df is None or df.empty:
            return None
        last = df.iloc[-1]
        return {
            "pe_ttm": float(last.get("PE(TTM)") or float("nan")),
            "pb":      float(last.get("市净率")   or float("nan")),
            "mcap":    float(last.get("总市值")   or float("nan")),
            "close":   float(last.get("当日收盘价") or float("nan")),
            "asof":    str(last.get("数据日期", "")),
        }
    except Exception as e:
        logger.debug(f"stock_value_em {symbol} 失败: {e}")
        return None


def _fetch_roe(symbol: str) -> float | None:
    """从 stock_financial_analysis_indicator_em 拿最新加权 ROE（%）。"""
    import akshare as ak
    try:
        df = ak.stock_financial_analysis_indicator_em(
            symbol=_symbol_with_suffix(symbol), indicator="按报告期")
        if df is None or df.empty or "ROEJQ" not in df.columns:
            return None
        v = df["ROEJQ"].dropna().iloc[0]
        return float(v) if v is not None else None
    except Exception as e:
        logger.debug(f"financial_indicator {symbol} 失败: {e}")
        return None


def _fetch_momentum(symbol: str, lookback: int = 60) -> float | None:
    """用 mootdx 日线拿 60 日累计涨跌幅。"""
    try:
        from data_layer.quotes.mootdx_provider import MootdxQuotes
        df = MootdxQuotes().daily(symbol)
        if df is None or len(df) < lookback + 1:
            return None
        closes = df["close"].astype(float).tail(lookback + 1).reset_index(drop=True)
        if closes.iloc[0] <= 0:
            return None
        return float(closes.iloc[-1] / closes.iloc[0] - 1) * 100  # 百分比
    except Exception as e:
        logger.debug(f"momentum {symbol} 失败: {e}")
        return None


def fetch_snapshot(symbol: str, name: str = "", use_cache: bool = True) -> dict:
    """单股一行快照（不抛异常，缺失字段返 None）。

    带 6 小时单股缓存：相同股票第二次调用秒返，避免每次跑策略都重抓 PE/ROE/动量。
    """
    if use_cache:
        cached = cache.read(_MODULE, f"snap_{symbol}", max_age_sec=60 * 60 * 6)
        if cached is not None and not cached.empty:
            return cached.iloc[0].to_dict()

    val = _fetch_value_em(symbol) or {}
    roe = _fetch_roe(symbol)
    mom = _fetch_momentum(symbol)
    snap = {
        "symbol":     symbol,
        "name":       name,
        "pe_ttm":     val.get("pe_ttm"),
        "pb":         val.get("pb"),
        "mcap":       val.get("mcap"),
        "close":      val.get("close"),
        "asof":       val.get("asof"),
        "roe":        roe,
        "momentum60": mom,
    }
    # 只缓存"至少 PE 与 ROE 都拿到"的快照，避免缓存空数据
    if snap["pe_ttm"] is not None and snap["roe"] is not None:
        cache.write(_MODULE, f"snap_{symbol}", pd.DataFrame([snap]))
    return snap


# ----------------------- 主选股逻辑 -----------------------
def run_strategy(
    scope: str = "hs300_zz500",
    top_n: int = 25,
    min_mcap_yi: float = 30.0,            # 最小总市值（亿元）
    momentum_bonus_weight: float = 0.3,   # 动量权重（占总排名分的减项）
    exclude_st: bool = True,
    use_cache: bool = True,
    max_workers: int = 16,
    progress_cb=None,                     # 可选：(done, total) → None
) -> dict:
    """跑魔法公式 + 动量。返回 {as_of, picks: [{...}], stats: {...}}。"""
    t0 = time.time()
    cache_key = f"picks_{scope}_top{top_n}_mcap{min_mcap_yi}_w{momentum_bonus_weight}_st{int(exclude_st)}"
    if use_cache:
        cached = cache.read(_MODULE, cache_key, max_age_sec=60 * 60 * 6)  # 半天
        if cached is not None and not cached.empty:
            return {
                "as_of": str(datetime.now()),
                "from_cache": True,
                "picks": cached.to_dict(orient="records"),
                "stats": {"cached": True},
            }

    universe = get_universe(scope)
    if not universe:
        return {"as_of": str(datetime.now()), "picks": [], "stats": {"err": "股池为空"}}

    # 过滤 ST 名称
    if exclude_st:
        universe = [u for u in universe if "ST" not in (u.get("name") or "").upper()]

    logger.info(f"魔法公式：股池 {len(universe)} 只，开始并发抓取…")

    snapshots: list[dict] = []
    total = len(universe)
    done = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {
            pool.submit(fetch_snapshot, u["symbol"], u.get("name", ""), use_cache): u
            for u in universe
        }
        for fut in as_completed(futs):
            try:
                snap = fut.result()
            except Exception as e:
                logger.debug(f"fetch_snapshot 异常: {e}")
                snap = None
            done += 1
            if snap:
                snapshots.append(snap)
            if progress_cb:
                try: progress_cb(done, total)
                except Exception: pass

    df = pd.DataFrame(snapshots)
    raw_count = len(df)

    # ---- 数据完整性过滤 ----
    df = df.dropna(subset=["pe_ttm", "roe", "mcap"])
    df = df[df["pe_ttm"] > 0]
    df = df[df["roe"] > 0]
    if min_mcap_yi:
        df = df[df["mcap"] >= min_mcap_yi * 1e8]
    cleaned_count = len(df)
    if df.empty:
        return {"as_of": str(datetime.now()), "picks": [], "stats": {
            "universe": total, "raw": raw_count, "cleaned": 0,
            "elapsed_sec": round(time.time() - t0, 1),
        }}

    # ---- 双因子排名（升序：越小越好）----
    df["rank_ey"]  = df["pe_ttm"].rank(method="min", ascending=True)   # PE 越低越好
    df["rank_roe"] = (-df["roe"]).rank(method="min", ascending=True)   # ROE 越高越好
    df["score_base"] = df["rank_ey"] + df["rank_roe"]

    # ---- 动量加分（动量越高，总分减得越多）----
    df["momentum60"] = df["momentum60"].fillna(0)
    mom_rank = (-df["momentum60"]).rank(method="min", ascending=True)  # 动量越高排名越靠前
    df["score"] = df["score_base"] + momentum_bonus_weight * mom_rank

    df = df.sort_values("score", ascending=True).reset_index(drop=True)
    df["pick_rank"] = df.index + 1
    df_top = df.head(top_n).copy()

    # ---- 美化字段 ----
    df_top["earnings_yield"] = (1 / df_top["pe_ttm"] * 100).round(2)  # % 收益率
    df_top["roe"]            = df_top["roe"].round(2)
    df_top["pe_ttm"]         = df_top["pe_ttm"].round(2)
    df_top["pb"]             = df_top["pb"].round(2)
    df_top["mcap_yi"]        = (df_top["mcap"] / 1e8).round(1)        # 亿
    df_top["momentum60"]     = df_top["momentum60"].round(2)
    df_top["score"]          = df_top["score"].round(1)

    out = df_top[[
        "pick_rank", "symbol", "name", "close",
        "pe_ttm", "earnings_yield", "roe", "pb",
        "mcap_yi", "momentum60", "score", "asof",
    ]].rename(columns={
        "pick_rank":      "排名",
        "symbol":         "代码",
        "name":           "名称",
        "close":          "股价",
        "pe_ttm":         "市盈率(TTM)",
        "earnings_yield": "盈利收益率%",
        "roe":            "ROE%",
        "pb":             "市净率",
        "mcap_yi":        "总市值(亿)",
        "momentum60":     "60日动量%",
        "score":          "综合分",
        "asof":           "数据日期",
    })

    # 写缓存
    cache.write(_MODULE, cache_key, out)

    return {
        "as_of":      str(datetime.now()),
        "from_cache": False,
        "picks":      out.to_dict(orient="records"),
        "stats": {
            "universe":    total,
            "raw":         raw_count,
            "cleaned":     cleaned_count,
            "top_n":       top_n,
            "scope":       scope,
            "elapsed_sec": round(time.time() - t0, 1),
        },
    }
