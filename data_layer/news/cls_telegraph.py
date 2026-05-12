"""akshare 财联社快讯（秒级实时）。"""
from __future__ import annotations

import pandas as pd
from loguru import logger

from .. import cache
from ..base import BaseProvider


class CLSTelegraph(BaseProvider):
    module = "news_cls"
    cache_ttl = 30  # 30 秒，避免短时间重复请求

    def fetch_raw(self, **_) -> pd.DataFrame:
        import akshare as ak
        # 优先用快讯接口，fallback 到 global_cls
        try:
            return ak.stock_telegraph_cls()
        except AttributeError:
            return ak.stock_info_global_cls(symbol="全部")

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
        """调度器入口：每 60s 拉一次并去重入库（长期累积）。"""
        try:
            df = self.fetch_raw()
            df = self.normalize(df)
            if df.empty:
                return
            # 财联社快讯返回列名通常为 "发布日期"+"发布时间"+"标题"+"内容"
            dedup_cols = [c for c in ["发布时间", "标题"] if c in df.columns] or list(df.columns[:2])
            cache.append_dedup(self.module, "history", df, dedup_cols)
        except Exception as e:
            logger.exception(f"财联社快讯轮询失败: {e}")
