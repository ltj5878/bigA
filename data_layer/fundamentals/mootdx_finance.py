"""mootdx 财务数据：本地解析通达信 gpcw*.zip 财务库。

包含 EPS、ROE、净利率、营收、净利等周期性披露字段。
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from loguru import logger

from ..base import BaseProvider
from ..config import config


class MootdxFinance(BaseProvider):
    module = "fundamentals_mootdx_finance"
    cache_ttl = None  # 财务数据按报告期变化，不主动失效

    def __init__(self, downdir: str | Path | None = None):
        self.downdir = Path(downdir or (config.cache_dir / "mootdx_finance_raw"))
        self.downdir.mkdir(parents=True, exist_ok=True)

    def fetch_raw(self, filename: str = "", **_):
        """下载并解析单个 gpcw 财务包。"""
        from mootdx.affair import Affair
        if not filename:
            # 拉最新一份
            files = Affair.files()
            if not files:
                return pd.DataFrame()
            filename = files[0]["filename"] if isinstance(files[0], dict) else files[0]
        Affair.fetch(downdir=str(self.downdir), filename=filename)
        # parse() 默认解析 downdir 下所有 zip
        return Affair.parse(downdir=str(self.downdir))

    def normalize(self, raw, **_) -> pd.DataFrame:
        if raw is None:
            return pd.DataFrame()
        if isinstance(raw, pd.DataFrame):
            return raw.reset_index(drop=True)
        try:
            return pd.DataFrame(raw)
        except Exception as e:
            logger.warning(f"mootdx finance 数据格式异常: {e}")
            return pd.DataFrame()

    def cache_key(self, **kwargs) -> str:
        return kwargs.get("filename", "latest")

    def latest(self) -> pd.DataFrame:
        return self.fetch()
