"""akshare 公司基本面（主用）。

覆盖：个股概况、财务摘要、财务指标、ST 列表、股东户数、业绩快报。
"""
from __future__ import annotations

import pandas as pd
import requests
from loguru import logger

from ..base import BaseProvider
from ..config import config as _cfg


# akshare 部分接口不带 UA，会被东财拒掉。这里给 requests 全局 Session 注入默认 UA。
# 只在 import 时执行一次，影响范围限于 akshare 用 requests.get 的调用。
_orig_get = requests.get
def _patched_get(url, **kw):
    headers = kw.pop("headers", {}) or {}
    headers.setdefault("User-Agent", _cfg.ua)
    return _orig_get(url, headers=headers, **kw)
requests.get = _patched_get  # noqa: monkey-patch


class AkshareFundamentals(BaseProvider):
    module = "fundamentals_akshare"
    cache_ttl = 60 * 60 * 12  # 12 小时

    # ---------- 抽象方法用作通用入口；各方法走独立 cache_key ----------
    def fetch_raw(self, fn: str, **kwargs):
        import akshare as ak
        func = getattr(ak, fn, None)
        if func is None:
            logger.warning(f"akshare 无函数 {fn}")
            return None
        try:
            return func(**kwargs)
        except Exception as e:
            logger.warning(f"akshare.{fn}({kwargs}) 调用失败: {e}")
            return None

    def normalize(self, raw, **_) -> pd.DataFrame:
        if raw is None:
            return pd.DataFrame()
        if isinstance(raw, pd.DataFrame):
            return raw.reset_index(drop=True)
        try:
            return pd.DataFrame(raw)
        except Exception:
            return pd.DataFrame()

    def cache_key(self, **kwargs) -> str:
        fn = kwargs.get("fn", "")
        rest = {k: v for k, v in kwargs.items() if k != "fn"}
        suffix = "_".join(f"{k}={v}" for k, v in sorted(rest.items()))
        return f"{fn}__{suffix}" if suffix else fn

    # ---------- 便捷封装 ----------
    def individual_info(self, symbol: str) -> pd.DataFrame:
        df = self.fetch(fn="stock_individual_info_em", symbol=symbol)
        if df is not None and not df.empty:
            return df
        # akshare 接口偶尔 502/JSON 损坏 → 自己直连东财 push2
        fb = self._individual_info_fallback(symbol)
        if fb is not None and not fb.empty:
            return fb
        # 全部失败：返回一行说明，避免前端看到空白
        return pd.DataFrame([{
            "item": "提示",
            "value": f"东财个股概况接口暂不可用（{symbol}）。已尝试 akshare 与 push2 直连，均失败；通常是东财 push2 网关 502。建议改用'财务指标'/'财务摘要'查看。",
        }])

    def _individual_info_fallback(self, symbol: str) -> pd.DataFrame:
        market_code = 1 if symbol.startswith(("6", "5", "9")) else 0
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "fltt": "2", "invt": "2",
            "fields": "f57,f58,f43,f60,f116,f117,f127,f128,f173",
            "secid": f"{market_code}.{symbol}",
        }
        try:
            r = requests.get(url, params=params, timeout=10,
                             headers={"User-Agent": _cfg.ua, "Referer": "https://quote.eastmoney.com/"})
            j = r.json()
            d = j.get("data") or {}
            mapping = {
                "f57": "股票代码", "f58": "股票简称", "f43": "最新", "f60": "昨收",
                "f116": "总市值", "f117": "流通市值",
                "f127": "行业", "f128": "板块",
                "f173": "上市时间",
            }
            rows = [{"item": v, "value": d.get(k)} for k, v in mapping.items() if k in d]
            return pd.DataFrame(rows)
        except Exception as e:
            logger.warning(f"individual_info 自实现回退失败 {symbol}: {e}")
            return pd.DataFrame()

    def financial_abstract(self, symbol: str) -> pd.DataFrame:
        return self.fetch(fn="stock_financial_abstract", symbol=symbol)

    @staticmethod
    def _with_suffix(symbol: str) -> str:
        """akshare 部分函数要求 600000.SH / 000001.SZ 格式。"""
        if "." in symbol:
            return symbol
        if symbol.startswith(("60", "688", "900", "9")):
            return f"{symbol}.SH"
        if symbol.startswith(("83", "87", "43", "92")):
            return f"{symbol}.BJ"
        return f"{symbol}.SZ"

    def financial_indicator(self, symbol: str) -> pd.DataFrame:
        return self.fetch(fn="stock_financial_analysis_indicator_em",
                          symbol=self._with_suffix(symbol),
                          indicator="按报告期")

    def st_list(self) -> pd.DataFrame:
        return self.fetch(fn="stock_zh_a_st_em")

    def shareholders_count(self, symbol: str) -> pd.DataFrame:
        return self.fetch(fn="stock_zh_a_gdhs_detail_em", symbol=symbol)

    def performance_express(self, date: str) -> pd.DataFrame:
        """业绩快报。date 格式 YYYYMMDD（季末）。"""
        return self.fetch(fn="stock_yjbb_em", date=date)
