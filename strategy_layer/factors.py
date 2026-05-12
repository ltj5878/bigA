"""单股原始因子拉取（质量 + 价值 + 动量 + 行业 + 研报情绪 + F-Score 所需历史财报）。

接口：
- `fetch_factors(symbol, name="")` 同步返回完整字典；带 6 小时缓存
- `fetch_financial_history(symbol)` 拿历史财报表（缓存 12 小时，回测复用）
- `industry_map()` 全市场申万一级行业映射（缓存 7 天）
- `sentiment_score(symbol)` 近 30 天研报评级情绪净分（缓存 12 小时）

字段定位（akshare stock_financial_analysis_indicator_em 列名）：
- ROE 加权          → ROEJQ              （%）
- 毛利率            → XSMLL              （%）
- 毛利率同比变动     → XSMLL_TB           （%, 同比百分点变化）
- 经营现金流/营收    → JYXJLYYSR          （%）
- 净利润扣非        → KCFJCXSYJLR        （元）
- 营业总收入        → TOTALOPERATEREVE   （元）
- 总资产净利率(ROA) → ZZCJLL             （%）
- 总资产周转        → TOAZZL             （次）
- 流动比率          → LD                 （倍）
- 资产负债率        → ZCFZL              （%）
- 资产负债率同比     → ZCFZLTZ            （%）
- 净利润同比        → KCFJCXSYJLRTZ      （%）

stock_value_em：当日 PE_TTM / PB / 总市值 / 收盘价 / 数据日期
mootdx daily：12 个月收盘价 → 12-1 动量
"""
from __future__ import annotations

import math
from typing import Any

import pandas as pd
from loguru import logger

from data_layer import cache

_MODULE = "strategy_factors"


# ----------------------- 工具 -----------------------
def _to_float(v: Any) -> float | None:
    if v is None: return None
    try:
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return None


def _symbol_with_suffix(symbol: str) -> str:
    if symbol.startswith(("60", "688", "900", "9")):
        return f"{symbol}.SH"
    if symbol.startswith(("83", "87", "43", "92")):
        return f"{symbol}.BJ"
    return f"{symbol}.SZ"


# ----------------------- 历史财报（核心数据源）-----------------------
def fetch_financial_history(symbol: str, use_cache: bool = True) -> pd.DataFrame | None:
    """返回历史报告期表（行=报告期，列=指标）。

    回测共用此表，避免重复请求。
    """
    if use_cache:
        cached = cache.read(_MODULE, f"finhist_{symbol}", max_age_sec=60 * 60 * 12)
        if cached is not None and not cached.empty:
            return cached

    import akshare as ak
    try:
        df = ak.stock_financial_analysis_indicator_em(
            symbol=_symbol_with_suffix(symbol), indicator="按报告期")
    except Exception as e:
        logger.debug(f"fetch_financial_history {symbol} 失败: {e}")
        return None
    if df is None or df.empty:
        return None
    # 按报告期降序，最新在前；落盘
    if "REPORT_DATE" in df.columns:
        df = df.sort_values("REPORT_DATE", ascending=False).reset_index(drop=True)
    cache.write(_MODULE, f"finhist_{symbol}", df)
    return df


# ----------------------- 估值快照 -----------------------
def fetch_value_snapshot(symbol: str, use_cache: bool = True) -> dict | None:
    """返回 {pe_ttm, pb, ps, mcap, close, asof}。"""
    if use_cache:
        cached = cache.read(_MODULE, f"value_{symbol}", max_age_sec=60 * 60 * 6)
        if cached is not None and not cached.empty:
            return cached.iloc[0].to_dict()

    import akshare as ak
    try:
        df = ak.stock_value_em(symbol=symbol)
        if df is None or df.empty:
            return None
        last = df.iloc[-1]
    except Exception as e:
        logger.debug(f"stock_value_em {symbol} 失败: {e}")
        return None

    snap = {
        "pe_ttm": _to_float(last.get("PE(TTM)")),
        "pb":     _to_float(last.get("市净率")),
        "ps":     _to_float(last.get("市销率")),
        "mcap":   _to_float(last.get("总市值")),
        "close":  _to_float(last.get("当日收盘价")),
        "asof":   str(last.get("数据日期", "")),
    }
    if snap["pe_ttm"] is not None:
        cache.write(_MODULE, f"value_{symbol}", pd.DataFrame([snap]))
    return snap


# ----------------------- 12-1 动量 -----------------------
def fetch_momentum_12_1(symbol: str) -> float | None:
    """12-1 动量：12 个月前到 1 个月前的累计涨跌幅（剔除最近 1 月反转效应）。

    单位：百分比。约 252 个交易日 - 21 个交易日。
    """
    try:
        from data_layer.quotes.mootdx_provider import MootdxQuotes
        df = MootdxQuotes().daily(symbol)
        if df is None or len(df) < 252:
            return None
        closes = df["close"].astype(float).reset_index(drop=True)
        c_t_minus_21  = closes.iloc[-21]
        c_t_minus_252 = closes.iloc[-252]
        if c_t_minus_252 <= 0:
            return None
        return (c_t_minus_21 / c_t_minus_252 - 1) * 100
    except Exception as e:
        logger.debug(f"momentum_12_1 {symbol} 失败: {e}")
        return None


# ----------------------- 主入口：单股完整因子 -----------------------
def fetch_factors(symbol: str, name: str = "", use_cache: bool = True) -> dict:
    """返回单股全部原始因子（含 F-Score 需要的最近 2 期对比字段）。

    所有缺失字段返 None，永不抛异常。
    """
    value  = fetch_value_snapshot(symbol, use_cache=use_cache) or {}
    finhist = fetch_financial_history(symbol, use_cache=use_cache)

    out: dict = {
        "symbol": symbol,
        "name":   name,
        # 估值
        "pe_ttm": value.get("pe_ttm"),
        "pb":     value.get("pb"),
        "ps":     value.get("ps"),
        "mcap":   value.get("mcap"),
        "close":  value.get("close"),
        "asof":   value.get("asof"),
        # 财务（最新一期）
        "roe":          None,
        "roa":          None,
        "gross_margin": None,
        "gross_margin_yoy": None,  # 同比变动百分点
        "ocf_revenue":  None,      # 经营现金流/营收
        "net_profit":   None,
        "revenue":      None,
        "asset_turn":   None,
        "current_ratio": None,
        "debt_ratio":    None,
        "debt_ratio_yoy": None,    # 同比变动（负代表下降）
        "net_profit_yoy": None,
        # 上一期（用于 F-Score 同比判断）
        "roa_prev":         None,
        "asset_turn_prev":  None,
        "current_ratio_prev": None,
        # 动量
        "momentum12_1": fetch_momentum_12_1(symbol),
    }

    if finhist is not None and not finhist.empty:
        latest = finhist.iloc[0]
        out["roe"]              = _to_float(latest.get("ROEJQ"))
        out["roa"]              = _to_float(latest.get("ZZCJLL"))
        out["gross_margin"]     = _to_float(latest.get("XSMLL"))
        out["gross_margin_yoy"] = _to_float(latest.get("XSMLL_TB"))
        out["ocf_revenue"]      = _to_float(latest.get("JYXJLYYSR"))
        out["net_profit"]       = _to_float(latest.get("KCFJCXSYJLR"))
        out["revenue"]          = _to_float(latest.get("TOTALOPERATEREVE"))
        out["asset_turn"]       = _to_float(latest.get("TOAZZL"))
        out["current_ratio"]    = _to_float(latest.get("LD"))
        out["debt_ratio"]       = _to_float(latest.get("ZCFZL"))
        out["debt_ratio_yoy"]   = _to_float(latest.get("ZCFZLTZ"))
        out["net_profit_yoy"]   = _to_float(latest.get("KCFJCXSYJLRTZ"))

        # 上一期（用于趋势对比）
        if len(finhist) >= 2:
            prev = finhist.iloc[1]
            out["roa_prev"]           = _to_float(prev.get("ZZCJLL"))
            out["asset_turn_prev"]    = _to_float(prev.get("TOAZZL"))
            out["current_ratio_prev"] = _to_float(prev.get("LD"))

    # ---- 行业（申万一级） ----
    out["industry_l1"] = industry_l1_of(symbol)

    # ---- 研报情绪（近 30 天）----
    out["sentiment"] = sentiment_score(symbol)

    return out


# ----------------------- 申万一级行业 -----------------------
# 申万一级行业代码（前 2 位）→ 中文名映射
_SW_L1 = {
    "11": "农林牧渔", "21": "采掘", "22": "化工", "23": "钢铁", "24": "有色金属",
    "27": "电子", "28": "汽车", "33": "家用电器", "34": "食品饮料", "35": "纺织服饰",
    "36": "轻工制造", "37": "医药生物", "41": "公用事业", "42": "交通运输", "43": "房地产",
    "45": "商贸零售", "46": "社会服务", "48": "银行", "49": "非银金融", "51": "综合",
    "61": "建筑材料", "62": "建筑装饰", "63": "电力设备", "64": "机械设备", "65": "国防军工",
    "71": "计算机", "72": "传媒", "73": "通信", "74": "煤炭", "75": "石油石化",
    "76": "环保", "77": "美容护理",
}


def industry_map(use_cache: bool = True) -> dict:
    """返回 {symbol: 申万一级行业中文名}，全市场一次性获取。"""
    if use_cache:
        cached = cache.read(_MODULE, "industry_l1_map", max_age_sec=60 * 60 * 24 * 7)
        if cached is not None and not cached.empty:
            return dict(zip(cached["symbol"], cached["industry"]))

    import akshare as ak
    try:
        df = ak.stock_industry_clf_hist_sw()
        if df is None or df.empty:
            return {}
    except Exception as e:
        logger.warning(f"申万行业接口失败: {e}")
        return {}
    # 一只股票可能换过行业 → 取 start_date 最新那条
    df = df.sort_values(["symbol", "start_date"]).groupby("symbol", as_index=False).tail(1)
    df["l1_code"] = df["industry_code"].astype(str).str[:2]
    df["industry"] = df["l1_code"].map(_SW_L1).fillna("其他")
    df = df[["symbol", "industry"]]
    cache.write(_MODULE, "industry_l1_map", df)
    return dict(zip(df["symbol"], df["industry"]))


def industry_l1_of(symbol: str) -> str:
    """单股一级行业（中文）。带全市场映射缓存。"""
    m = industry_map(use_cache=True)
    return m.get(symbol, "其他")


# ----------------------- 研报情绪（近 30 天评级净分）-----------------------
# 评级分映射：上调/买入计正分，下调/减持计负分
_RATING_SCORE = {
    "买入":  2, "强烈买入": 2, "推荐": 2, "强推": 2, "强烈推荐": 2,
    "增持":  1, "审慎增持": 1, "谨慎增持": 1, "看好": 1, "优于大市": 1, "跑赢行业": 1, "跑赢大市": 1,
    "中性":  0, "持有":  0, "观望":  0, "维持评级": 0, "不评级": 0,
    "减持": -1, "谨慎减持": -1, "回避": -1, "落后大市": -1, "弱于大市": -1,
    "卖出": -2, "强烈卖出": -2,
}
# 评级变化字段加分：调高记 +2，维持 0，调低 -2（变化比绝对水平更有 alpha）
_RATING_CHANGE_SCORE = {
    "调高": 2, "上调": 2, "首次": 1, "首次评级": 1,
    "维持": 0, "持平": 0,
    "下调": -2, "调低": -2,
}


def sentiment_map(lookback_days: int = 30, use_cache: bool = True) -> dict:
    """批量拉全市场近 N 天评级，返回 {symbol: 情绪净分}。

    全市场单次扫描 ~ 45 秒，比逐股调用快两个数量级。带 12h 缓存。
    """
    key = f"sentiment_map_{lookback_days}d"
    if use_cache:
        cached = cache.read(_MODULE, key, max_age_sec=60 * 60 * 12)
        if cached is not None and not cached.empty:
            return dict(zip(cached["symbol"], cached["score"]))

    import akshare as ak
    from datetime import datetime, timedelta

    rows = []
    end = datetime.now()
    for i in range(lookback_days):
        d = (end - timedelta(days=i)).strftime("%Y%m%d")
        try:
            df = ak.stock_rank_forecast_cninfo(date=d)
            if df is not None and not df.empty:
                rows.append(df)
        except Exception:
            continue
    if not rows:
        logger.warning("评级批量拉取无数据")
        return {}

    big = pd.concat(rows, ignore_index=True)
    big["rating_s"] = big["投资评级"].map(_RATING_SCORE).fillna(0)
    big["change_s"] = big["评级变化"].map(_RATING_CHANGE_SCORE).fillna(0)
    big["score"] = big["rating_s"] + big["change_s"]

    agg = big.groupby("证券代码", as_index=False)["score"].sum()
    agg = agg.rename(columns={"证券代码": "symbol"})
    cache.write(_MODULE, key, agg)
    return dict(zip(agg["symbol"], agg["score"]))


def sentiment_score(symbol: str, lookback_days: int = 30,
                    use_cache: bool = True) -> float | None:
    """单股情绪分，复用 sentiment_map 缓存。无评级返 0。"""
    m = sentiment_map(lookback_days=lookback_days, use_cache=use_cache)
    if not m:
        return None
    return float(m.get(symbol, 0.0))
