"""东财研报中心 reportapi 直连。

接口：https://reportapi.eastmoney.com/report/list
返回字段（节选）：title、orgSName（机构）、emRatingName（评级）、
indvAimPriceL（目标价）、infoCode（用于拼 PDF URL）、publishDate
PDF URL 模板：https://pdf.dfcfw.com/pdf/H3_{infoCode}_1.pdf
"""
from __future__ import annotations

import time

import pandas as pd
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from ..base import BaseProvider
from ..config import config


REPORT_LIST_URL = "https://reportapi.eastmoney.com/report/list"


class EastmoneyReports(BaseProvider):
    module = "reports_eastmoney"
    cache_ttl = 60 * 60  # 1 小时

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
    def fetch_raw(self, q_type: int = 0, page_no: int = 1, page_size: int = 50,
                  industry_code: str = "*", begin_time: str = "", end_time: str = "", **_):
        params = {
            "industryCode": industry_code,
            "pageSize": page_size,
            "pageNo": page_no,
            "qType": q_type,  # 0=全部, 1=行业, 2=个股, 3=策略
            "fields": "",
            "beginTime": begin_time,
            "endTime": end_time,
            "_": int(time.time() * 1000),
        }
        headers = {"User-Agent": config.ua, "Referer": "https://data.eastmoney.com/"}
        resp = requests.get(REPORT_LIST_URL, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def normalize(self, raw: dict, **_) -> pd.DataFrame:
        if not raw or "data" not in raw:
            return pd.DataFrame()
        df = pd.DataFrame(raw["data"])
        if df.empty:
            return df
        keep_map = {
            "title": "title",
            "stockCode": "symbol",
            "stockName": "name",
            "orgSName": "org",
            "emRatingName": "rating",
            "indvAimPriceL": "target_price_low",
            "indvAimPriceT": "target_price_high",
            "publishDate": "publish_date",
            "infoCode": "info_code",
            "industryName": "industry",
        }
        df = df.rename(columns={k: v for k, v in keep_map.items() if k in df.columns})
        cols = [v for v in keep_map.values() if v in df.columns]
        df = df[cols].copy()
        if "info_code" in df.columns:
            df["pdf_url"] = df["info_code"].apply(
                lambda c: f"https://pdf.dfcfw.com/pdf/H3_{c}_1.pdf" if c else ""
            )
        return df.reset_index(drop=True)

    def cache_key(self, **kwargs) -> str:
        return (f"q{kwargs.get('q_type', 0)}_p{kwargs.get('page_no', 1)}"
                f"_{kwargs.get('begin_time', '')}_{kwargs.get('industry_code', '*')}")

    def list_recent(self, q_type: int = 0, page_no: int = 1, page_size: int = 50) -> pd.DataFrame:
        return self.fetch(q_type=q_type, page_no=page_no, page_size=page_size)

    def to_pdf_items(self, df: pd.DataFrame) -> list[dict]:
        """转成 pdf_downloader 用的格式。"""
        if df is None or df.empty or "pdf_url" not in df.columns:
            return []
        out = []
        for _, r in df.iterrows():
            if not r.get("pdf_url"):
                continue
            out.append({
                "symbol": str(r.get("symbol", "unknown")),
                "date": str(r.get("publish_date", ""))[:10].replace("-", ""),
                "title": str(r.get("title", "")),
                "url": r["pdf_url"],
            })
        return out
