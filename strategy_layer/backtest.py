"""通用回测引擎：as-of 选股 + 月调仓 + 大盘择时 + 净值/指标。

设计：
- 输入：策略名 (three_factor / magic_formula) + 起止时间 + 调仓频率 + 单股最大占比
- 输出：净值序列（每个交易日）、月度调仓明细、关键指标
- 数据：
  - 历史财报：复用 factors.fetch_financial_history (按 REPORT_DATE 截断到调仓日)
  - 历史价格：复用 MootdxQuotes().daily(symbol)
  - 历史指数：用 stock_zh_index_daily(sh000300) 做基准

简化假设：
- 月初等权重买入 25 只（如择时 0.3 仓位，则 25 只各 0.3/25 = 1.2%，剩余 70% 现金）
- 不考虑滑点、佣金、停牌
- 月末按收盘价计算组合净值
- "Survivorship bias"：股池用当前 HS300+ZZ500，不复原历史成分（接受偏差，结果偏好看）
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

import numpy as np
import pandas as pd
from loguru import logger

from data_layer import cache
from .factors import fetch_financial_history, _to_float, industry_map
from .f_score import f_score
from .magic_formula import get_universe
from .market_timing import _hs300_daily
from .aggressive_alpha import _score_candidates as _score_aggressive_candidates
from .aggressive_alpha import price_metrics_from_close

_MODULE = "strategy_backtest"
_REPORT_LAG_DAYS = 45  # 财报披露延迟


# ----------------------- 历史价格批量 -----------------------
def _daily_close(symbol: str) -> pd.Series | None:
    """返回某股 date → close 的 Series（按日期升序）。

    回测专用：通过 akshare stock_zh_a_daily（新浪源）拿 4+ 年完整历史（前复权）。
    新浪 API 比东财 push2his 稳定。单独 strategy_backtest 缓存。
    """
    cached = cache.read(_MODULE, f"dailyclose_{symbol}", max_age_sec=60 * 60 * 24 * 7)
    if cached is not None and not cached.empty:
        cached["date"] = pd.to_datetime(cached["date"])
        return cached.set_index("date")["close"].astype(float).sort_index()

    # 新浪日线代码格式：sh600519 / sz000001 / bj870166
    if symbol.startswith(("60", "688", "900", "9")):
        sina_sym = f"sh{symbol}"
    elif symbol.startswith(("83", "87", "43", "92")):
        sina_sym = f"bj{symbol}"
    else:
        sina_sym = f"sz{symbol}"

    try:
        import akshare as ak
        df = ak.stock_zh_a_daily(symbol=sina_sym, start_date="20180101",
                                 end_date="20991231", adjust="qfq")
        if df is None or df.empty:
            return None
        out = df[["date", "close"]].copy()
        cache.write(_MODULE, f"dailyclose_{symbol}", out)
        out["date"] = pd.to_datetime(out["date"])
        return out.set_index("date")["close"].astype(float).sort_index()
    except Exception as e:
        logger.debug(f"_daily_close {symbol} 失败: {e}")
        return None


# ----------------------- as-of 因子快照 -----------------------
def _factors_asof(symbol: str, asof_date: pd.Timestamp,
                  close_series: pd.Series | None) -> dict | None:
    """按 asof_date 取财报和价格，模拟当时的因子值。

    财报：取 REPORT_DATE <= asof_date - 45d 的最新一期
    价格：取 close_series[asof_date] 最近一日
    动量 12-1：close[asof - 21d] / close[asof - 252d] - 1
    """
    fh = fetch_financial_history(symbol, use_cache=True)
    if fh is None or fh.empty:
        return None
    if close_series is None:
        return None

    # 1) 财报快照
    cutoff = asof_date - pd.Timedelta(days=_REPORT_LAG_DAYS)
    if "REPORT_DATE" not in fh.columns:
        return None
    fh = fh.copy()
    fh["REPORT_DATE"] = pd.to_datetime(fh["REPORT_DATE"])
    fh_asof = fh[fh["REPORT_DATE"] <= cutoff]
    if fh_asof.empty:
        return None
    latest = fh_asof.iloc[0]  # 因为 fetch_financial_history 已按降序排好

    # 2) 价格快照
    cs = close_series[close_series.index <= asof_date]
    if len(cs) < 252:
        return None
    close = float(cs.iloc[-1])
    c_t_minus_21  = float(cs.iloc[-21])
    c_t_minus_252 = float(cs.iloc[-252])

    momentum = (c_t_minus_21 / c_t_minus_252 - 1) * 100 if c_t_minus_252 > 0 else 0.0

    return {
        "symbol":       symbol,
        "close":        close,
        "roe":          _to_float(latest.get("ROEJQ")),
        "roa":          _to_float(latest.get("ZZCJLL")),
        "gross_margin": _to_float(latest.get("XSMLL")),
        "gross_margin_yoy": _to_float(latest.get("XSMLL_TB")),
        "ocf_revenue":  _to_float(latest.get("JYXJLYYSR")),
        "net_profit":   _to_float(latest.get("KCFJCXSYJLR")),
        "revenue":      _to_float(latest.get("TOTALOPERATEREVE")),
        "asset_turn":   _to_float(latest.get("TOAZZL")),
        "current_ratio": _to_float(latest.get("LD")),
        "debt_ratio":   _to_float(latest.get("ZCFZL")),
        "debt_ratio_yoy": _to_float(latest.get("ZCFZLTZ")),
        "net_profit_yoy": _to_float(latest.get("KCFJCXSYJLRTZ")),
        "roa_prev": None,
        "asset_turn_prev": None,
        "current_ratio_prev": None,
        "momentum":     momentum,
        # 估值 PE/PB/PS 历史每日没法直接拿（stock_value_em 是当日表，不带历史按日切片函数）
        # 用最新一期市盈率代替——回测会有少量偏差但量级可接受
        "pe_ttm":       None,   # 见下文 _estimate_pe
        "pb":           None,
        "ps":           None,
    } | _previous_financial_fields(fh_asof)


def _previous_financial_fields(fh_asof: pd.DataFrame) -> dict:
    if len(fh_asof) < 2:
        return {}
    prev = fh_asof.iloc[1]
    return {
        "roa_prev": _to_float(prev.get("ZZCJLL")),
        "asset_turn_prev": _to_float(prev.get("TOAZZL")),
        "current_ratio_prev": _to_float(prev.get("LD")),
    }


# 估值字段历史回填：用 stock_value_em(symbol) 表本身就是历史每日 PE/PB/PS！
# 但每只股拉一次很慢。这里加缓存：单股完整历史估值表
def _value_history(symbol: str) -> pd.DataFrame | None:
    """完整历史每日 PE_TTM / PB / PS 表，按 date 升序。"""
    cached = cache.read(_MODULE, f"valuehist_{symbol}", max_age_sec=60 * 60 * 24)
    if cached is not None and not cached.empty:
        return cached

    import akshare as ak
    try:
        df = ak.stock_value_em(symbol=symbol)
        if df is None or df.empty:
            return None
        df = df.copy()
        df["date"] = pd.to_datetime(df["数据日期"])
        df = df.sort_values("date").reset_index(drop=True)
        cols = ["date", "PE(TTM)", "市净率", "市销率"]
        rename = {"PE(TTM)": "pe_ttm", "市净率": "pb", "市销率": "ps"}
        if "总市值" in df.columns:
            cols.append("总市值")
            rename["总市值"] = "mcap"
        df = df[cols].rename(columns=rename)
        cache.write(_MODULE, f"valuehist_{symbol}", df)
        return df
    except Exception as e:
        logger.debug(f"value_history {symbol} 失败: {e}")
        return None


# ----------------------- 月初日期序列 -----------------------
def month_starts(start: str, end: str) -> list[pd.Timestamp]:
    return list(pd.date_range(start=start, end=end, freq="MS"))


# ----------------------- z-score（cross-sectional）-----------------------
def _zscore(s: pd.Series) -> pd.Series:
    s = s.astype(float)
    lo, hi = s.quantile(0.01), s.quantile(0.99)
    s = s.clip(lower=lo, upper=hi)
    mu, sd = s.mean(), s.std()
    if sd == 0 or pd.isna(sd):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - mu) / sd


# ----------------------- 每月调仓核心 -----------------------
def _pick_at(asof: pd.Timestamp, universe: list[dict],
             min_mcap_yi: float = 30.0, top_n: int = 25,
             max_workers: int = 16,
             max_per_industry: int = 3,
             ind_map: dict | None = None,
             strategy: str = "three_factor",
             min_f_score: int = 5,
             volatility_penalty: float = 0.2,
             aggressive_weights: dict | None = None) -> pd.DataFrame:
    """在指定日期回看选 Top N（带行业中性化）。"""

    if ind_map is None:
        ind_map = industry_map(use_cache=True)

    rows = []

    def _one(u):
        sym = u["symbol"]
        cs = _daily_close(sym)
        snap = _factors_asof(sym, asof, cs)
        if not snap: return None
        # 估值：从 value_history 取截止 asof 的最近一行
        vh = _value_history(sym)
        if vh is None or vh.empty: return None
        vh_at = vh[vh["date"] <= asof]
        if vh_at.empty: return None
        v = vh_at.iloc[-1]
        snap["pe_ttm"] = _to_float(v["pe_ttm"])
        snap["pb"]     = _to_float(v["pb"])
        snap["ps"]     = _to_float(v["ps"])
        snap["mcap"]   = _to_float(v.get("mcap")) if hasattr(v, "get") else None
        snap["name"]   = u.get("name", "")
        snap["industry"] = ind_map.get(sym, "其他")
        snap["sentiment"] = 0.0
        if strategy == "aggressive_alpha":
            snap["momentum12_1"] = snap.pop("momentum", None)
            snap.update(price_metrics_from_close(cs[cs.index <= asof]))
            score, _items = f_score(snap)
            snap["f_score"] = score
        return snap

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for r in pool.map(_one, universe):
            if r: rows.append(r)

    df = pd.DataFrame(rows)
    if df.empty: return df

    if strategy == "aggressive_alpha":
        selected = _score_aggressive_candidates(
            df,
            top_n=top_n,
            min_mcap_yi=min_mcap_yi if df["mcap"].notna().any() else 0.0,
            min_f_score=min_f_score,
            exclude_st=True,
            max_per_industry=max_per_industry,
            volatility_penalty=volatility_penalty,
            weights=aggressive_weights,
        )
        if selected.empty:
            return selected
        return selected[["symbol", "name", "close", "aggressive_score"]].rename(
            columns={"aggressive_score": "total_score"}
        )

    # 过滤
    df = df.dropna(subset=["pe_ttm", "pb", "ps", "roe", "gross_margin", "ocf_revenue"])
    df = df[(df["pe_ttm"] > 0) & (df["pb"] > 0) & (df["ps"] > 0) & (df["roe"] > 0)]
    if df.empty: return df

    # z-score 三因子
    df = df.reset_index(drop=True)
    df["Q"] = _zscore(df["roe"]) + _zscore(df["gross_margin"]) + _zscore(df["ocf_revenue"])
    df["V"] = _zscore(1 / df["pe_ttm"]) + _zscore(1 / df["pb"]) + _zscore(1 / df["ps"])
    df["M"] = _zscore(df["momentum"])
    df["total_score"] = 0.4 * df["Q"] + 0.4 * df["V"] + 0.2 * df["M"]
    df = df.sort_values("total_score", ascending=False).reset_index(drop=True)

    # 行业中性化（与实盘策略保持一致）
    df["industry"] = df["symbol"].map(ind_map).fillna("其他")
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
        df = df.loc[keep_idx].reset_index(drop=True)
    else:
        df = df.head(top_n).reset_index(drop=True)

    return df[["symbol", "name", "close", "total_score"]]


# ----------------------- 主回测 -----------------------
@dataclass
class BacktestResult:
    nav: pd.Series           # 策略净值（按交易日）
    hs300_nav: pd.Series     # 基准净值
    monthly_picks: list      # 每月 picks
    metrics: dict


def run_backtest(
    start: str = "2022-01-01",
    end: str | None = None,
    strategy: str = "three_factor",
    top_n: int = 25,
    universe_scope: str = "hs300_zz500",
    use_timing: bool = True,
    defensive_position: float = 0.3,
    max_per_industry: int = 3,
    min_mcap_yi: float = 30.0,
    min_f_score: int = 5,
    volatility_penalty: float = 0.2,
    aggressive_weights: dict | None = None,
    preset_name: str | None = None,
    max_workers: int = 16,
    progress_cb: Callable[[int, int], None] | None = None,
    use_cache: bool = True,
) -> dict:
    """执行回测。注意：首次跑 ~800 只 × 每月调仓 × 4 年 ≈ 几十次单股因子调用；
    由于 valuehist / finhist 都强缓存，预期总耗时 5-15 分钟（取决于网络）。
    """
    t0 = time.time()
    end_date = pd.to_datetime(end) if end else pd.Timestamp.today().normalize()
    start_date = pd.to_datetime(start)

    # 结果级缓存（6 小时）
    weight_key = "_".join(f"{k}{v}" for k, v in sorted((aggressive_weights or {}).items()))
    cache_key = (f"result_{strategy}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
                 f"_top{top_n}_{universe_scope}_t{int(use_timing)}_ind{max_per_industry}"
                 f"_f{min_f_score}_vol{volatility_penalty}_{weight_key}")
    if use_cache:
        cached = cache.read(_MODULE, cache_key, max_age_sec=60 * 60 * 6)
        if cached is not None and not cached.empty:
            import json as _json
            try:
                payload = _json.loads(cached.iloc[0]["payload"])
                payload.setdefault("metrics", {})["elapsed_sec"] = round(time.time() - t0, 2)
                payload["metrics"]["from_cache"] = True
                logger.info(f"回测结果命中缓存：{cache_key}")
                return payload
            except Exception:
                pass

    universe = get_universe(universe_scope)
    universe = [u for u in universe if "ST" not in (u.get("name") or "").upper()]
    if not universe:
        return {"error": "股池为空"}

    months = month_starts(start, end_date.strftime("%Y-%m-%d"))
    ind_map = industry_map(use_cache=True)
    logger.info(f"回测: {len(months)} 个调仓月，股池 {len(universe)} 只，行业映射 {len(ind_map)} 只")

    # 1) 预加载基准
    hs300 = _hs300_daily()
    if hs300 is None or hs300.empty:
        return {"error": "沪深 300 数据不可用"}
    hs300 = hs300.copy()
    hs300["date"] = pd.to_datetime(hs300["date"])
    hs300 = hs300.set_index("date")["close"].astype(float).sort_index()

    # 2) 月度调仓
    monthly_picks: list = []
    portfolio_returns: list = []
    nav_dates: list = [start_date]
    nav_values: list = [1.0]

    for i, m_start in enumerate(months):
        # 选股
        picks = _pick_at(
            m_start, universe, top_n=top_n,
            min_mcap_yi=min_mcap_yi,
            max_workers=max_workers,
            max_per_industry=max_per_industry,
            ind_map=ind_map,
            strategy=strategy,
            min_f_score=min_f_score,
            volatility_penalty=volatility_penalty,
            aggressive_weights=aggressive_weights,
        )
        if picks.empty:
            monthly_picks.append({"date": str(m_start.date()), "picks": []})
            if progress_cb: progress_cb(i + 1, len(months))
            continue

        # 择时仓位
        position = 1.0
        if use_timing:
            hs300_asof = hs300[hs300.index <= m_start]
            if len(hs300_asof) >= 200:
                ma200 = hs300_asof.rolling(200).mean().iloc[-1]
                position = 1.0 if hs300_asof.iloc[-1] > ma200 else defensive_position

        # 计算这个月的回报（从 m_start 到下个月初 或 end_date）
        m_end = months[i + 1] if i + 1 < len(months) else end_date

        symbols = picks["symbol"].tolist()
        rets = []
        for s in symbols:
            cs = _daily_close(s)
            if cs is None: continue
            s_in_window = cs[(cs.index >= m_start) & (cs.index <= m_end)]
            if len(s_in_window) < 2: continue
            ret = float(s_in_window.iloc[-1] / s_in_window.iloc[0] - 1)
            rets.append(ret)
        if not rets:
            monthly_picks.append({"date": str(m_start.date()), "picks": symbols,
                                  "position": position, "month_ret": 0.0})
            if progress_cb: progress_cb(i + 1, len(months))
            continue

        # 等权组合 + 择时仓位
        equity_ret = float(np.mean(rets))
        port_ret   = equity_ret * position  # 剩余 1-position 现金，回报 0

        monthly_picks.append({
            "date":        str(m_start.date()),
            "picks":       symbols,
            "position":    position,
            "month_ret":   round(equity_ret * 100, 2),  # 选股组合（满仓口径）
            "port_ret":    round(port_ret * 100, 2),    # 策略组合（含择时）
        })

        # 累加净值
        new_nav = nav_values[-1] * (1 + port_ret)
        nav_dates.append(m_end)
        nav_values.append(new_nav)

        if progress_cb: progress_cb(i + 1, len(months))

    nav = pd.Series(nav_values, index=pd.DatetimeIndex(nav_dates))

    # 3) 基准净值（同周期内沪深 300）
    h_in = hs300[(hs300.index >= start_date) & (hs300.index <= end_date)]
    if not h_in.empty:
        h_norm = h_in / h_in.iloc[0]
        # 对齐到调仓日：先 ffill（往后填）再 bfill（补齐第一个点的 NaN）
        h_aligned = h_norm.reindex(nav.index, method="ffill").bfill().fillna(1.0)
    else:
        h_aligned = pd.Series([1.0] * len(nav), index=nav.index)

    # 4) 指标
    metrics = _calc_metrics(nav, h_aligned, start_date, end_date)
    metrics["elapsed_sec"] = round(time.time() - t0, 1)

    # 序列化兜底：把所有 NaN/Inf 转 None，JSON 才能序列化
    def _safe_float(v):
        try:
            f = float(v)
            return None if np.isnan(f) or np.isinf(f) else round(f, 4)
        except (TypeError, ValueError):
            return None

    payload = {
        "start":  start_date.strftime("%Y-%m-%d"),
        "end":    end_date.strftime("%Y-%m-%d"),
        "strategy": strategy,
        "preset_name": preset_name,
        "nav":    [{"date": d.strftime("%Y-%m-%d"), "value": _safe_float(v)}
                   for d, v in nav.items()],
        "hs300_nav": [{"date": d.strftime("%Y-%m-%d"), "value": _safe_float(v)}
                      for d, v in h_aligned.items()],
        "monthly_picks": monthly_picks,
        "metrics": metrics,
    }

    # 写结果级缓存
    try:
        import json as _json
        cache.write(_MODULE, cache_key,
                    pd.DataFrame([{"payload": _json.dumps(payload, ensure_ascii=False)}]))
    except Exception as e:
        logger.warning(f"回测结果缓存写入失败: {e}")

    return payload


def _calc_metrics(nav: pd.Series, bench: pd.Series,
                  start: pd.Timestamp, end: pd.Timestamp) -> dict:
    if len(nav) < 2: return {}
    years = (end - start).days / 365.25 or 1
    final = float(nav.iloc[-1])
    bench_final = float(bench.iloc[-1])
    if not np.isfinite(bench_final) or bench_final <= 0:
        bench_final = 1.0

    annual = (final ** (1 / years) - 1) * 100 if final > 0 else 0.0
    bench_annual = (bench_final ** (1 / years) - 1) * 100

    rets = nav.pct_change().dropna()
    sharpe = float(rets.mean() / rets.std() * np.sqrt(12)) if rets.std() > 0 else 0.0
    if not np.isfinite(sharpe): sharpe = 0.0

    running_max = nav.cummax()
    drawdown = (nav / running_max - 1) * 100
    max_dd = float(drawdown.min())
    if not np.isfinite(max_dd): max_dd = 0.0

    def _r(v):
        return round(v, 2) if np.isfinite(v) else None

    return {
        "total_return":   _r((final - 1) * 100),
        "annual_return":  _r(annual),
        "bench_total":    _r((bench_final - 1) * 100),
        "bench_annual":   _r(bench_annual),
        "excess_annual":  _r(annual - bench_annual),
        "max_drawdown":   _r(max_dd),
        "sharpe":         _r(sharpe),
        "n_months":       len(nav) - 1,
        "best_month":     _r(float((rets * 100).max())) if len(rets) else 0.0,
        "worst_month":    _r(float((rets * 100).min())) if len(rets) else 0.0,
        "win_rate":       _r(float((rets > 0).mean() * 100)) if len(rets) else 0.0,
        "months_ge_10pct": int((rets >= 0.10).sum()) if len(rets) else 0,
        "avg_month_return": _r(float((rets * 100).mean())) if len(rets) else 0.0,
    }
