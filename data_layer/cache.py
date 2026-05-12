"""Parquet 落盘缓存：按模块/标的分文件。"""
from __future__ import annotations

import hashlib
import time
from pathlib import Path

import pandas as pd
from loguru import logger

from .config import config


def _safe_key(key: str) -> str:
    # 文件名安全化：超长或含特殊字符就用 md5
    if len(key) > 80 or any(c in key for c in '/\\:*?"<>|'):
        return hashlib.md5(key.encode()).hexdigest()
    return key


def cache_path(module: str, key: str) -> Path:
    d = config.cache_dir / module
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{_safe_key(key)}.parquet"


def write(module: str, key: str, df: pd.DataFrame) -> Path:
    if df is None or df.empty:
        logger.warning(f"cache.write 跳过空 DataFrame: {module}/{key}")
        return cache_path(module, key)
    path = cache_path(module, key)
    # akshare 经常返回混合类型 object 列（如同时包含数字与字符串），pyarrow
    # 会推断成 double 然后转换失败。强制把 object 列转字符串保证可写。
    safe = df.copy()
    for col in safe.columns:
        if safe[col].dtype == "object":
            safe[col] = safe[col].astype(str)
    try:
        safe.to_parquet(path, index=False)
    except Exception as e:
        logger.warning(f"Parquet 落盘失败 {module}/{key}: {e}；回退为不缓存。")
    return path


def read(module: str, key: str, max_age_sec: float | None = None) -> pd.DataFrame | None:
    path = cache_path(module, key)
    if not path.exists():
        return None
    if max_age_sec is not None and (time.time() - path.stat().st_mtime) > max_age_sec:
        return None
    return pd.read_parquet(path)


def append_dedup(module: str, key: str, df_new: pd.DataFrame, dedup_cols: list[str]) -> pd.DataFrame:
    """增量追加并按指定列去重，常用于新闻/快讯。"""
    old = read(module, key)
    merged = pd.concat([old, df_new], ignore_index=True) if old is not None else df_new
    merged = merged.drop_duplicates(subset=dedup_cols, keep="last").reset_index(drop=True)
    write(module, key, merged)
    return merged
