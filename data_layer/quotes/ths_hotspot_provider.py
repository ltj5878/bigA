"""同花顺热点（通过 pywencai / 问财问句查询）。

⚠️ 必须配置 IWENCAI_COOKIE 才能使用。cookie 有效期约 7 天。
"""
from __future__ import annotations

import pandas as pd
from loguru import logger

from ..base import BaseProvider
from ..config import config


class THSHotspot(BaseProvider):
    module = "quotes_ths_hotspot"
    cache_ttl = 60 * 10  # 10 分钟

    def fetch_raw(self, query: str, loop: int = 1, **_):
        if not config.iwencai_cookie:
            logger.warning("IWENCAI_COOKIE 未配置，pywencai 可能返回空。"
                           "请打开 http://www.iwencai.com/ 登录后从 F12 复制 cookie 中的 v 字段。")
        import pywencai
        # pywencai 0.12+ 支持 cookie 参数；老版本忽略
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
            return pd.DataFrame([raw])
        return pd.DataFrame()

    def cache_key(self, **kwargs) -> str:
        q = kwargs.get("query", "")
        return q[:60]

    # 常用问句封装
    def hot_sectors(self) -> pd.DataFrame:
        return self.fetch(query="今日热点板块涨幅排行")

    def dragon_tiger(self, date: str | None = None) -> pd.DataFrame:
        q = f"{date}龙虎榜" if date else "今日龙虎榜"
        return self.fetch(query=q)

    def hot_stocks(self) -> pd.DataFrame:
        return self.fetch(query="今日热门股票排行")

    def custom(self, query: str) -> pd.DataFrame:
        return self.fetch(query=query)
