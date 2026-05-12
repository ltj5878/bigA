"""akshare 东财全球财经资讯。"""
from __future__ import annotations

import pandas as pd
from loguru import logger

from .. import cache
from ..base import BaseProvider


class GlobalInfoEM(BaseProvider):
    module = "news_global_em"
    cache_ttl = 60 * 2  # 2 分钟

    def fetch_raw(self, **_) -> pd.DataFrame:
        import akshare as ak
        return ak.stock_info_global_em()

    def normalize(self, raw: pd.DataFrame, **_) -> pd.DataFrame:
        if raw is None or len(raw) == 0:
            return pd.DataFrame()
        return raw.reset_index(drop=True)

    def cache_key(self, **_) -> str:
        return "latest"

    def latest(self, n: int = 50) -> pd.DataFrame:
        df = self.fetch()
        return df.head(n) if not df.empty else df

    def poll_incremental(self):
        try:
            df = self.normalize(self.fetch_raw())
            if df.empty:
                return
            dedup_cols = [c for c in ["发布时间", "标题"] if c in df.columns] or list(df.columns[:2])
            cache.append_dedup(self.module, "history", df, dedup_cols)
        except Exception as e:
            logger.exception(f"东财全球资讯轮询失败: {e}")
