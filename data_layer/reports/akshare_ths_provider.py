"""akshare 研报（个股研报，数据底层是东财/THS）。"""
from __future__ import annotations

import pandas as pd

from ..base import BaseProvider


class AkshareResearchReport(BaseProvider):
    module = "reports_akshare"
    cache_ttl = 60 * 60

    def fetch_raw(self, symbol: str, **_) -> pd.DataFrame:
        import akshare as ak
        return ak.stock_research_report_em(symbol=symbol)

    def normalize(self, raw: pd.DataFrame, symbol: str = "", **_) -> pd.DataFrame:
        if raw is None or len(raw) == 0:
            return pd.DataFrame()
        df = raw.copy()
        df["symbol"] = symbol
        # akshare 已返回中文列名，原样保留
        return df.reset_index(drop=True)

    def cache_key(self, **kwargs) -> str:
        return kwargs.get("symbol", "")

    def by_stock(self, symbol: str) -> pd.DataFrame:
        return self.fetch(symbol=symbol)
