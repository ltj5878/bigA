"""mootdx F10 公司信息（在线接口）。

注意：mootdx 对 F10 的封装较弱，部分券商版本字段不全。建议主要走 akshare，本类作为兜底/对照。
"""
from __future__ import annotations

import pandas as pd
from loguru import logger

from ..base import BaseProvider


class MootdxF10(BaseProvider):
    module = "fundamentals_mootdx_f10"
    cache_ttl = 60 * 60 * 24  # 一天

    def __init__(self):
        from mootdx.quotes import Quotes
        self._q = Quotes.factory(market="std")

    def fetch_raw(self, symbol: str, **_):
        # get_company_info_category 返回 F10 分类目录
        try:
            return self._q.f10_cate(symbol=symbol)
        except AttributeError:
            try:
                return self._q.f10(symbol=symbol)
            except Exception as e:
                logger.warning(f"mootdx F10 暂不支持或失败: {e}")
                return None

    def normalize(self, raw, symbol: str = "", **_) -> pd.DataFrame:
        if raw is None:
            return pd.DataFrame()
        if isinstance(raw, pd.DataFrame):
            df = raw.copy()
        else:
            try:
                df = pd.DataFrame(raw)
            except Exception:
                return pd.DataFrame()
        df["symbol"] = symbol
        return df.reset_index(drop=True)

    def cache_key(self, **kwargs) -> str:
        return kwargs.get("symbol", "")

    def by_stock(self, symbol: str) -> pd.DataFrame:
        return self.fetch(symbol=symbol)
