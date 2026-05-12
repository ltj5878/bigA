"""mootdx F10 公告兜底源。

mootdx 对 F10 公告类的封装较弱，仅作为巨潮失败时的应急回退。
"""
from __future__ import annotations

import pandas as pd
from loguru import logger

from ..base import BaseProvider


class MootdxF10Announce(BaseProvider):
    module = "announcements_mootdx_f10"
    cache_ttl = 60 * 60 * 6

    def __init__(self):
        from mootdx.quotes import Quotes
        self._q = Quotes.factory(market="std")

    def fetch_raw(self, symbol: str, **_):
        try:
            # 不同版本 API 名差异较大，做兼容尝试
            for method in ("notice", "f10_notice", "company_news"):
                fn = getattr(self._q, method, None)
                if fn:
                    return fn(symbol=symbol)
        except Exception as e:
            logger.warning(f"mootdx F10 公告获取失败: {e}")
        return None

    def normalize(self, raw, symbol: str = "", **_) -> pd.DataFrame:
        if raw is None:
            return pd.DataFrame()
        df = raw if isinstance(raw, pd.DataFrame) else pd.DataFrame(raw)
        df["symbol"] = symbol
        return df.reset_index(drop=True)

    def cache_key(self, **kwargs) -> str:
        return kwargs.get("symbol", "")

    def by_stock(self, symbol: str) -> pd.DataFrame:
        return self.fetch(symbol=symbol)
