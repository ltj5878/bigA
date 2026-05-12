"""个股新闻（数据源：东财）。

akshare 1.18 的 stock_news_em 在 PyArrow 后端环境下用了 \\u 正则字面量，
被 re2 拒绝。这里改成直连东财搜索接口，绕开该 bug，字段保持一致。
"""
from __future__ import annotations

import json
import re
import time

import pandas as pd
import requests
from loguru import logger

from .. import cache
from ..base import BaseProvider
from ..config import config


_EM_NEWS_URL = "https://search-api-web.eastmoney.com/search/jsonp"


def _strip_html(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = re.sub(r"\(<em>|</em>\)|<em>|</em>", "", s)
    s = s.replace("　", "").replace("\r\n", " ")
    return s


class StockNewsEM(BaseProvider):
    module = "news_stock_em"
    cache_ttl = 60 * 5  # 5 分钟

    def fetch_raw(self, symbol: str, **_) -> pd.DataFrame:
        params = {
            "cb": "jQuery",
            "param": json.dumps({
                "uid": "",
                "keyword": symbol,
                "type": ["cmsArticleWebOld"],
                "client": "web",
                "clientType": "web",
                "clientVersion": "curr",
                "param": {"cmsArticleWebOld": {
                    "searchScope": "default",
                    "sort": "default",
                    "pageIndex": 1,
                    "pageSize": 100,
                    "preTag": "<em>",
                    "postTag": "</em>",
                }},
            }, ensure_ascii=False),
            "_": int(time.time() * 1000),
        }
        headers = {"User-Agent": config.ua, "Referer": "https://so.eastmoney.com/"}
        r = requests.get(_EM_NEWS_URL, params=params, headers=headers, timeout=15)
        r.raise_for_status()
        # 去掉 jsonp 包装
        m = re.search(r"\((\{.*\})\)\s*;?\s*$", r.text, re.DOTALL)
        if not m:
            return pd.DataFrame()
        data = json.loads(m.group(1))
        items = (data.get("result") or {}).get("cmsArticleWebOld") or []
        rows = []
        for it in items:
            rows.append({
                "关键词":   symbol,
                "新闻标题": _strip_html(it.get("title", "")),
                "新闻内容": _strip_html(it.get("content", "")),
                "发布时间": it.get("date", ""),
                "文章来源": it.get("mediaName", ""),
                "新闻链接": it.get("url", ""),
            })
        return pd.DataFrame(rows)

    def normalize(self, raw: pd.DataFrame, symbol: str = "", **_) -> pd.DataFrame:
        if raw is None or len(raw) == 0:
            return pd.DataFrame()
        df = raw.copy()
        df["symbol"] = symbol
        return df.reset_index(drop=True)

    def cache_key(self, **kwargs) -> str:
        return kwargs.get("symbol", "")

    def by_stock(self, symbol: str) -> pd.DataFrame:
        return self.fetch(symbol=symbol)

    def poll_watchlist(self):
        """调度器入口：增量轮询自选股新闻并去重入库。"""
        for sym in config.watchlist:
            try:
                df = self.fetch_raw(symbol=sym)
                df = self.normalize(df, symbol=sym)
                if df.empty:
                    continue
                # 用 (symbol, 发布时间, 标题) 三元组去重
                dedup_cols = [c for c in ["symbol", "发布时间", "新闻标题"] if c in df.columns]
                cache.append_dedup(self.module, sym, df, dedup_cols or ["symbol"])
            except Exception as e:
                logger.exception(f"stock_news_em 轮询 {sym} 失败: {e}")
