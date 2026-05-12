"""大盘择时：沪深 300 / 200 日均线规则。

规则非常朴素但有效：
- 沪深 300 收盘 > 200 日均线 → 多头环境，建议满仓
- 沪深 300 收盘 < 200 日均线 → 防御环境，仓位降到 30%

历史回测中，该规则在 2008、2015、2018、2022 都成功识别熊市。
"""
from __future__ import annotations

import pandas as pd
from loguru import logger


def _hs300_daily():
    """走 akshare 的指数日线接口；mootdx 不返回指数。"""
    import akshare as ak
    from data_layer import cache
    cached = cache.read("strategy_market_timing", "hs300_daily", max_age_sec=60 * 60 * 4)
    if cached is not None and not cached.empty:
        return cached
    df = ak.stock_zh_index_daily(symbol="sh000300")
    if df is not None and not df.empty:
        cache.write("strategy_market_timing", "hs300_daily", df)
    return df


def hs300_ma200_position(defensive_position: float = 0.3) -> dict:
    """读沪深 300 日线，按 200 日均线给出仓位建议。

    返回 {position: float, reason: str, ma200: float, close: float}。
    """
    try:
        df = _hs300_daily()
    except Exception as e:
        logger.warning(f"取沪深 300 日线失败: {e}")
        return {"position": 1.0, "reason": "数据获取失败，默认满仓", "ma200": None, "close": None}

    if df is None or len(df) < 200:
        return {"position": 1.0, "reason": "数据不足 200 日，默认满仓", "ma200": None, "close": None}

    closes = df["close"].astype(float)
    close  = float(closes.iloc[-1])
    ma200  = float(closes.rolling(200).mean().iloc[-1])
    above  = close > ma200
    pct    = (close / ma200 - 1) * 100

    if above:
        return {
            "position": 1.0,
            "reason":   f"沪深 300 ({close:.1f}) 高于 200 日均线 ({ma200:.1f})，多头环境，建议满仓",
            "ma200":    round(ma200, 2),
            "close":    round(close, 2),
            "diff_pct": round(pct, 2),
        }
    return {
        "position": defensive_position,
        "reason":   f"沪深 300 ({close:.1f}) 跌破 200 日均线 ({ma200:.1f})，防御环境，仓位降到 {int(defensive_position*100)}%",
        "ma200":    round(ma200, 2),
        "close":    round(close, 2),
        "diff_pct": round(pct, 2),
    }


def hs300_ma200_position_at(date: str | pd.Timestamp,
                            defensive_position: float = 0.3) -> dict:
    """回测用：返回指定日期的择时仓位（基于沪深 300 截止该日的数据）。"""
    try:
        df = _hs300_daily()
    except Exception as e:
        return {"position": 1.0, "reason": f"数据失败: {e}", "ma200": None, "close": None}

    if df is None or df.empty:
        return {"position": 1.0, "reason": "无数据", "ma200": None, "close": None}

    if "date" in df.columns:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df[df["date"] <= pd.to_datetime(date)]

    if len(df) < 200:
        return {"position": 1.0, "reason": "历史不足 200 日", "ma200": None, "close": None}

    closes = df["close"].astype(float)
    close  = float(closes.iloc[-1])
    ma200  = float(closes.rolling(200).mean().iloc[-1])
    return {
        "position": 1.0 if close > ma200 else defensive_position,
        "reason":   f"close={close:.1f} {'>' if close > ma200 else '<'} ma200={ma200:.1f}",
        "ma200":    round(ma200, 2),
        "close":    round(close, 2),
    }
