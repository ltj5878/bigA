"""行情备份源：腾讯财经（通过 efinance 封装）。

用于与 mootdx 做交叉校验。
"""
from __future__ import annotations

import pandas as pd
from loguru import logger

from ..base import BaseProvider


class TencentQuotes(BaseProvider):
    module = "quotes_tencent"
    cache_ttl = 60 * 60 * 4

    def fetch_raw(self, symbol: str, klt: int = 101, **_) -> pd.DataFrame:
        # efinance 用东财接口，但同样能拿到日线，作为腾讯/通达信的对账源
        # klt: 101=日线, 102=周线, 103=月线, 1=1分钟, 5=5分钟
        import efinance as ef
        return ef.stock.get_quote_history(symbol, klt=klt)

    def normalize(self, raw: pd.DataFrame, symbol: str = "", **_) -> pd.DataFrame:
        if raw is None or len(raw) == 0:
            return pd.DataFrame()
        df = raw.copy()
        rename = {
            "股票代码": "symbol", "股票名称": "name", "日期": "date",
            "开盘": "open", "收盘": "close", "最高": "high", "最低": "low",
            "成交量": "vol", "成交额": "amount",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        keep = [c for c in ["symbol", "name", "date", "open", "high", "low",
                            "close", "vol", "amount"] if c in df.columns]
        return df[keep].reset_index(drop=True)

    def cache_key(self, **kwargs) -> str:
        return f"{kwargs.get('symbol', '')}_{kwargs.get('klt', 101)}"

    def daily(self, symbol: str) -> pd.DataFrame:
        return self.fetch(symbol=symbol, klt=101)

    def realtime(self, symbols: list[str]) -> pd.DataFrame:
        """实时报价（不走缓存）。"""
        import efinance as ef
        df = ef.stock.get_realtime_quotes(stock_codes=symbols)
        return df
