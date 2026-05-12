"""iwencai 问财研报查询（pywencai）。

支持自然语言问句，如 "平安银行的研报"、"银行行业2026年目标价"。
⚠️ 需要 IWENCAI_COOKIE。
"""
from __future__ import annotations

import pandas as pd
from loguru import logger

from ..base import BaseProvider
from ..config import config


class IwencaiReports(BaseProvider):
    module = "reports_iwencai"
    cache_ttl = 60 * 60

    def fetch_raw(self, query: str, loop: int = 1, **_):
        if not config.iwencai_cookie:
            logger.warning("IWENCAI_COOKIE 未配置，问财查询可能失败。")
        import pywencai
        try:
            return pywencai.get(query=query, loop=loop, cookie=config.iwencai_cookie or None)
        except TypeError:
            return pywencai.get(query=query, loop=loop)

    def normalize(self, raw, **_) -> pd.DataFrame:
        if raw is None:
            return pd.DataFrame()
        if isinstance(raw, pd.DataFrame):
            return raw.reset_index(drop=True)
        if isinstance(raw, list):
            return pd.DataFrame(raw)
        if isinstance(raw, dict):
            # pywencai 偶尔会直接返回单条 dict，包成单行 DataFrame
            return pd.DataFrame([raw])
        return pd.DataFrame()

    def cache_key(self, **kwargs) -> str:
        return kwargs.get("query", "")[:60]

    def by_stock(self, stock_name_or_code: str) -> pd.DataFrame:
        return self.fetch(query=f"{stock_name_or_code}的研报")

    def by_industry(self, industry: str) -> pd.DataFrame:
        return self.fetch(query=f"{industry}行业最近的研报和目标价")
