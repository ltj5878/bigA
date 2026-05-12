"""行情主源：mootdx（基于 pytdx 的通达信本地行情）。

默认拉日线。首次使用前请执行 `mootdx bestip -v` 选最快服务器。
"""
from __future__ import annotations

import pandas as pd
from loguru import logger

from ..base import BaseProvider


class MootdxQuotes(BaseProvider):
    module = "quotes_mootdx"
    cache_ttl = 60 * 60 * 4  # 4 小时

    def __init__(self):
        import json
        from pathlib import Path

        from mootdx.quotes import Quotes

        # mootdx 0.11.7 在 BESTIP.HQ 为空字符串时会解包失败；又因为父类
        # BaseQuotes.__init__ 会先调 config.setup() 从磁盘重读配置，所以必须
        # 直接把 BESTIP.HQ 修补到 config.json 文件里，再启动 Quotes。
        cfg_path = Path.home() / ".mootdx" / "config.json"
        if cfg_path.exists():
            try:
                cfg = json.loads(cfg_path.read_text())
                bestip = (cfg.get("BESTIP") or {}).get("HQ")
                if not bestip or not isinstance(bestip, list) or len(bestip) < 2:
                    hq = (cfg.get("SERVER") or {}).get("HQ") or []
                    if hq:
                        first = hq[0]  # ["深圳双线主站1", "110.41.147.114", 7709]
                        cfg.setdefault("BESTIP", {})["HQ"] = [first[-2], first[-1]]
                        cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2))
                        logger.info(f"已修补 mootdx BESTIP.HQ = {cfg['BESTIP']['HQ']}")
            except Exception as e:
                logger.warning(f"修补 mootdx 配置失败: {e}")

        self._q = Quotes.factory(market="std")

    def fetch_raw(self, symbol: str, frequency: int = 9, offset: int = 800, **_) -> pd.DataFrame:
        # frequency=9 表示日线；9 的解释见 mootdx 文档
        return self._q.bars(symbol=symbol, frequency=frequency, offset=offset)

    def normalize(self, raw: pd.DataFrame, symbol: str = "", **_) -> pd.DataFrame:
        if raw is None or len(raw) == 0:
            return pd.DataFrame()
        df = raw.copy()
        # mootdx 返回的列名因版本不同，统一处理
        if "datetime" in df.columns:
            df["date"] = pd.to_datetime(df["datetime"]).dt.date
        df["symbol"] = symbol
        keep = [c for c in ["symbol", "date", "open", "high", "low", "close",
                            "vol", "amount"] if c in df.columns]
        return df[keep].reset_index(drop=True)

    def cache_key(self, **kwargs) -> str:
        return f"{kwargs.get('symbol', '')}_{kwargs.get('frequency', 9)}"

    def daily(self, symbol: str, offset: int = 800) -> pd.DataFrame:
        return self.fetch(symbol=symbol, frequency=9, offset=offset)
