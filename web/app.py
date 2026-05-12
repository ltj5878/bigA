"""Big A 数据接入层的纯 API 服务（前后端分离）。

启动：uvicorn web.app:app --host 127.0.0.1 --port 8006
所有数据接口走 /api/*，返回统一 {ok, data, error} 结构。
前端由独立的 Vue3 项目（frontend/，5176 端口）通过 CORS 调用。
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger

from data_layer.announcements.cninfo_provider import CninfoAnnouncements
from data_layer.fundamentals.akshare_fundamentals import AkshareFundamentals
from data_layer.news.cls_telegraph import CLSTelegraph
from data_layer.news.global_info_em import GlobalInfoEM
from data_layer.news.stock_news_em import StockNewsEM
from data_layer.quotes.mootdx_provider import MootdxQuotes
from data_layer.quotes.tencent_provider import TencentQuotes
from data_layer.quotes.ths_hotspot_provider import THSHotspot
from data_layer.reports.akshare_ths_provider import AkshareResearchReport
from data_layer.reports.eastmoney_provider import EastmoneyReports
from data_layer.reports.iwencai_provider import IwencaiReports

app = FastAPI(title="Big A · 数据接入层", version="0.2.0")

# 允许 Vue 前端跨域访问。从环境变量读，允许多个 origin（逗号分隔）。
_default_origins = "http://localhost:5176,http://127.0.0.1:5176"
_allow_origins = [o.strip() for o in
                  os.environ.get("BIG_A_CORS_ORIGINS", _default_origins).split(",")
                  if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


def _ok(data: Any) -> dict:
    return {"ok": True, "data": data}


def _df_to_records(df, limit: int | None = None) -> list[dict]:
    import math
    if df is None or len(df) == 0:
        return []
    if limit:
        df = df.head(limit)
    df = df.copy()
    # datetime / object 列统一转字符串
    for col in df.columns:
        if str(df[col].dtype).startswith(("datetime", "object")):
            df[col] = df[col].astype(str)
    # NaN / Inf → None，否则 FastAPI 默认 JSON 编码会失败
    def clean(v):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v
    return [{k: clean(v) for k, v in row.items()} for row in df.to_dict(orient="records")]


def _safe(fn, *args, **kwargs):
    """统一异常包装。"""
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        logger.exception(f"API 调用失败 {fn.__name__}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------- 根路由：API 元信息 ----------
@app.get("/", include_in_schema=False)
def index():
    return {
        "service": "Big A 数据接入层 API",
        "version": app.version,
        "frontend": "请访问 Vue 前端：http://localhost:5176",
        "docs": "/docs",
    }


# ---------- 模块一：行情层 ----------
@app.get("/api/quotes/mootdx")
def api_quote_mootdx(symbol: str = Query(..., min_length=4)):
    df = _safe(MootdxQuotes().daily, symbol)
    return _ok(_df_to_records(df, limit=300))


@app.get("/api/quotes/tencent")
def api_quote_tencent(symbol: str):
    df = _safe(TencentQuotes().daily, symbol)
    return _ok(_df_to_records(df, limit=300))


@app.get("/api/quotes/ths_hotspot")
def api_ths_hotspot(query: str = "今日热点板块涨幅排行"):
    df = _safe(THSHotspot().custom, query)
    return _ok(_df_to_records(df, limit=100))


# ---------- 模块二：研报层 ----------
@app.get("/api/reports/eastmoney")
def api_reports_em(page_no: int = 1, page_size: int = 30, q_type: int = 0):
    df = _safe(EastmoneyReports().list_recent, q_type=q_type, page_no=page_no, page_size=page_size)
    return _ok(_df_to_records(df))


@app.get("/api/reports/akshare")
def api_reports_ak(symbol: str):
    df = _safe(AkshareResearchReport().by_stock, symbol)
    return _ok(_df_to_records(df, limit=200))


@app.get("/api/reports/iwencai")
def api_reports_iwencai(query: str):
    df = _safe(IwencaiReports().fetch, query=query)
    return _ok(_df_to_records(df, limit=100))


# ---------- 模块三：新闻层 ----------
@app.get("/api/news/stock")
def api_news_stock(symbol: str):
    df = _safe(StockNewsEM().by_stock, symbol)
    return _ok(_df_to_records(df, limit=100))


@app.get("/api/news/cls")
def api_news_cls(n: int = 50):
    df = _safe(CLSTelegraph().latest, n)
    return _ok(_df_to_records(df))


@app.get("/api/news/global")
def api_news_global(n: int = 50):
    df = _safe(GlobalInfoEM().latest, n)
    return _ok(_df_to_records(df))


# ---------- 模块四：基础数据层 ----------
@app.get("/api/fundamentals/info")
def api_fund_info(symbol: str):
    df = _safe(AkshareFundamentals().individual_info, symbol)
    return _ok(_df_to_records(df))


@app.get("/api/fundamentals/indicator")
def api_fund_indicator(symbol: str):
    df = _safe(AkshareFundamentals().financial_indicator, symbol)
    return _ok(_df_to_records(df, limit=50))


@app.get("/api/fundamentals/abstract")
def api_fund_abstract(symbol: str):
    df = _safe(AkshareFundamentals().financial_abstract, symbol)
    return _ok(_df_to_records(df, limit=50))


# ---------- 模块五：公告层 ----------
@app.get("/api/announcements/cninfo")
def api_announcements(symbol: str, days: int = 30):
    today = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    df = _safe(CninfoAnnouncements().query, symbol, start, today)
    return _ok(_df_to_records(df, limit=200))


# ---------- 模块六：策略层 ----------
from strategy_layer.aggressive_alpha import run_strategy as _run_aggressive_alpha
from strategy_layer.magic_formula import run_strategy as _run_magic_formula
from strategy_layer.strategy_search import run_aggressive_alpha_search as _run_aggressive_search
from strategy_layer.three_factor import run_strategy as _run_three_factor


@app.get("/api/strategy/magic_formula")
def api_strategy_magic_formula(
    scope: str = Query("hs300_zz500", description="hs300 / zz500 / hs300_zz500 / all"),
    top_n: int = Query(25, ge=5, le=100),
    min_mcap_yi: float = Query(30.0, ge=0, description="最小总市值（亿元）"),
    momentum_weight: float = Query(0.3, ge=0, le=2.0, description="动量加分权重"),
    exclude_st: bool = Query(True),
    use_cache: bool = Query(True),
):
    """魔法公式选股（旧版）：1/PE + ROE 双因子排名 + 60 日动量加分。"""
    result = _safe(
        _run_magic_formula,
        scope=scope,
        top_n=top_n,
        min_mcap_yi=min_mcap_yi,
        momentum_bonus_weight=momentum_weight,
        exclude_st=exclude_st,
        use_cache=use_cache,
    )
    return _ok(result)


from strategy_layer.backtest import run_backtest as _run_backtest


@app.get("/api/strategy/backtest")
def api_strategy_backtest(
    start: str = Query("2022-01-01"),
    end: str | None = Query(None),
    strategy: str = Query("three_factor", description="three_factor / aggressive_alpha"),
    top_n: int = Query(25, ge=5, le=100),
    universe_scope: str = Query("hs300_zz500"),
    use_timing: bool = Query(True),
    max_per_industry: int = Query(3, ge=0, le=25,
                                  description="单一申万一级行业最大入选数；0=不限制"),
    min_mcap_yi: float = Query(30.0, ge=0),
    min_f_score: int = Query(5, ge=0, le=9),
    volatility_penalty: float = Query(0.2, ge=0, le=2.0),
    use_cache: bool = Query(True, description="结果级缓存（6h）"),
):
    """三因子策略回测：月度调仓 + 行业中性化 + 大盘择时，输出净值曲线和指标。"""
    result = _safe(
        _run_backtest,
        start=start, end=end, strategy=strategy, top_n=top_n,
        universe_scope=universe_scope, use_timing=use_timing,
        max_per_industry=max_per_industry, min_mcap_yi=min_mcap_yi,
        min_f_score=min_f_score, volatility_penalty=volatility_penalty,
        use_cache=use_cache,
    )
    return _ok(result)


@app.get("/api/strategy/three_factor")
def api_strategy_three_factor(
    scope: str = Query("hs300_zz500"),
    top_n: int = Query(25, ge=5, le=100),
    min_mcap_yi: float = Query(30.0, ge=0),
    min_f_score: int = Query(5, ge=0, le=9, description="F-Score 最小阈值"),
    exclude_st: bool = Query(True),
    use_cache: bool = Query(True),
    weight_q: float = Query(0.35, ge=0, le=1, description="质量权重"),
    weight_v: float = Query(0.35, ge=0, le=1, description="价值权重"),
    weight_m: float = Query(0.15, ge=0, le=1, description="动量权重"),
    weight_s: float = Query(0.15, ge=0, le=1, description="研报情绪权重"),
    max_per_industry: int = Query(3, ge=0, le=25,
                                  description="单一申万一级行业最大入选数；0=不限制"),
):
    """四因子选股：Quality + Value + Momentum + Sentiment + F-Score + 行业中性化 + 大盘择时。"""
    result = _safe(
        _run_three_factor,
        scope=scope, top_n=top_n,
        min_mcap_yi=min_mcap_yi, min_f_score=min_f_score,
        exclude_st=exclude_st, use_cache=use_cache,
        weight_q=weight_q, weight_v=weight_v,
        weight_m=weight_m, weight_s=weight_s,
        max_per_industry=max_per_industry,
    )
    return _ok(result)


@app.get("/api/strategy/aggressive_alpha")
def api_strategy_aggressive_alpha(
    scope: str = Query("hs300_zz500"),
    top_n: int = Query(10, ge=5, le=100),
    min_mcap_yi: float = Query(30.0, ge=0),
    min_f_score: int = Query(5, ge=0, le=9),
    exclude_st: bool = Query(True),
    use_cache: bool = Query(True),
    volatility_penalty: float = Query(0.2, ge=0, le=2.0),
    max_per_industry: int = Query(2, ge=0, le=25),
):
    """激进 Alpha 选股：趋势/动量主导 + 波动惩罚 + F-Score 过滤。"""
    result = _safe(
        _run_aggressive_alpha,
        scope=scope,
        top_n=top_n,
        min_mcap_yi=min_mcap_yi,
        min_f_score=min_f_score,
        exclude_st=exclude_st,
        use_cache=use_cache,
        volatility_penalty=volatility_penalty,
        max_per_industry=max_per_industry,
    )
    return _ok(result)


@app.get("/api/strategy/search/aggressive_alpha")
def api_strategy_search_aggressive_alpha(
    start: str = Query("2022-01-01"),
    end: str | None = Query(None),
    universe_scope: str = Query("hs300"),
    min_mcap_yi: float = Query(30.0, ge=0),
    min_f_score: int = Query(5, ge=0, le=9),
    use_cache: bool = Query(True),
):
    """固定预设搜索：比较激进 Alpha 的收益、回撤、月胜率和单月 10%+ 次数。"""
    result = _safe(
        _run_aggressive_search,
        start=start,
        end=end,
        universe_scope=universe_scope,
        min_mcap_yi=min_mcap_yi,
        min_f_score=min_f_score,
        use_cache=use_cache,
    )
    return _ok(result)


# ---------- 健康检查 ----------
@app.get("/api/health")
def health():
    return _ok({"status": "ok", "ts": datetime.now().isoformat()})


@app.exception_handler(HTTPException)
async def http_exc_handler(_request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"ok": False, "error": exc.detail},
    )
