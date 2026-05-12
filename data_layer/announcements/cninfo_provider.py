"""巨潮 cninfo 公告查询。

接口：POST http://www.cninfo.com.cn/new/hisAnnouncement/query
PDF 链接拼接：http://static.cninfo.com.cn/ + adjunctUrl
反爬严格：必须带 UA + Referer，限速 1 req/s。
"""
from __future__ import annotations

import time

import pandas as pd
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from ..base import BaseProvider
from ..config import config


CNINFO_QUERY_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_STATIC_BASE = "http://static.cninfo.com.cn/"


def _plate_and_orgid(symbol: str) -> tuple[str, str]:
    """根据股票代码判断交易所与组合 ID。"""
    s = symbol.lstrip()
    if s.startswith(("60", "688", "900")):
        return "sse", f"gssh0{s}"
    if s.startswith(("000", "001", "002", "003", "300", "301", "200")):
        return "szse", f"gssz0{s}"
    if s.startswith(("83", "87", "43", "92")):
        return "bj", f"9900{s}"
    return "szse", f"gssz0{s}"


class CninfoAnnouncements(BaseProvider):
    module = "announcements_cninfo"
    cache_ttl = 60 * 60 * 2  # 2 小时

    _last_call_ts: float = 0.0

    def _throttle(self):
        interval = 1.0 / config.cninfo_qps
        gap = time.time() - self._last_call_ts
        if gap < interval:
            time.sleep(interval - gap)
        type(self)._last_call_ts = time.time()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def fetch_raw(self, symbol: str, start_date: str, end_date: str,
                  page_num: int = 1, page_size: int = 30,
                  category: str = "", **_):
        plate, orgid = _plate_and_orgid(symbol)
        data = {
            "pageNum": page_num,
            "pageSize": page_size,
            "tabName": "fulltext",
            "column": plate,
            "stock": f"{symbol},{orgid}",
            "plate": "sh" if plate == "sse" else ("sz" if plate == "szse" else "bj"),
            "category": category,
            "seDate": f"{start_date}~{end_date}",
        }
        headers = {
            "User-Agent": config.ua,
            "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch",
        }
        self._throttle()
        resp = requests.post(CNINFO_QUERY_URL, data=data, headers=headers, timeout=20)
        resp.raise_for_status()
        return resp.json()

    def normalize(self, raw: dict, symbol: str = "", **_) -> pd.DataFrame:
        if not raw or "announcements" not in raw or raw["announcements"] is None:
            return pd.DataFrame()
        rows = raw["announcements"]
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        keep_map = {
            "secCode": "symbol",
            "secName": "name",
            "announcementTitle": "title",
            "announcementTime": "publish_ts",
            "announcementType": "ann_type",
            "adjunctUrl": "adjunct_url",
        }
        df = df.rename(columns={k: v for k, v in keep_map.items() if k in df.columns})
        df["pdf_url"] = df.get("adjunct_url", "").apply(
            lambda u: (CNINFO_STATIC_BASE + u) if u else ""
        )
        if "publish_ts" in df.columns:
            # publish_ts 为毫秒时间戳
            df["publish_date"] = pd.to_datetime(df["publish_ts"], unit="ms").dt.date
        cols = [c for c in ["symbol", "name", "title", "publish_date",
                            "ann_type", "pdf_url"] if c in df.columns]
        return df[cols].reset_index(drop=True)

    def cache_key(self, **kwargs) -> str:
        return (f"{kwargs.get('symbol','')}_{kwargs.get('start_date','')}"
                f"_{kwargs.get('end_date','')}_{kwargs.get('category','')}"
                f"_p{kwargs.get('page_num',1)}")

    def query(self, symbol: str, start_date: str, end_date: str,
              category: str = "", page_num: int = 1) -> pd.DataFrame:
        return self.fetch(symbol=symbol, start_date=start_date, end_date=end_date,
                          category=category, page_num=page_num)

    def to_pdf_items(self, df: pd.DataFrame) -> list[dict]:
        if df is None or df.empty or "pdf_url" not in df.columns:
            return []
        out = []
        for _, r in df.iterrows():
            if not r.get("pdf_url"):
                continue
            out.append({
                "symbol": str(r.get("symbol", "unknown")),
                "date": str(r.get("publish_date", "")).replace("-", ""),
                "title": str(r.get("title", "")),
                "url": r["pdf_url"],
            })
        return out
