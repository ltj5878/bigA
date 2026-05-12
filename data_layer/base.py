"""数据源抽象基类。

子类只需实现 fetch_raw() 与 normalize()，由 fetch() 串联 缓存→抓取→标准化→落盘。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd
from loguru import logger

from . import cache


class BaseProvider(ABC):
    # 子类必须覆盖：用于 cache 目录分区
    module: str = "unknown"
    # 缓存有效期（秒），None 表示永久；新闻类设短，行情类设长
    cache_ttl: float | None = None

    @abstractmethod
    def fetch_raw(self, **kwargs) -> Any:
        """从远端拿原始数据（dict / list / DataFrame 均可）。"""

    @abstractmethod
    def normalize(self, raw: Any, **kwargs) -> pd.DataFrame:
        """把原始数据转为统一 schema 的 DataFrame。"""

    def cache_key(self, **kwargs) -> str:
        # 默认用所有 kwargs 拼一个 key，子类可重写
        return "_".join(f"{k}={v}" for k, v in sorted(kwargs.items()))

    def fetch(self, use_cache: bool = True, **kwargs) -> pd.DataFrame:
        key = self.cache_key(**kwargs)
        if use_cache:
            cached = cache.read(self.module, key, self.cache_ttl)
            if cached is not None:
                logger.debug(f"[{self.module}] cache hit: {key}")
                return cached
        logger.info(f"[{self.module}] fetch from remote: {key}")
        raw = self.fetch_raw(**kwargs)
        df = self.normalize(raw, **kwargs)
        cache.write(self.module, key, df)
        return df
