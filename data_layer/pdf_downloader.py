"""异步 PDF 下载器：限速 + 重试 + 去重索引。"""
from __future__ import annotations

import asyncio
import hashlib
import re
from pathlib import Path

import aiohttp
import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from . import cache
from .config import config


_INDEX_KEY = "index"  # pdf 索引存 cache/pdfs/index.parquet


def _safe_filename(s: str) -> str:
    s = re.sub(r"[\\/:*?\"<>|\s]+", "_", s).strip("_")
    return s[:80] or "untitled"


def target_path(kind: str, symbol: str, date: str, title: str) -> Path:
    """kind: 'reports' | 'announcements'"""
    folder = config.pdf_dir / kind / symbol
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{date}_{_safe_filename(title)}.pdf"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
async def _download_one(session: aiohttp.ClientSession, url: str, dest: Path, sem: asyncio.Semaphore):
    async with sem:
        async with session.get(url, headers={"User-Agent": config.ua}, timeout=60) as resp:
            resp.raise_for_status()
            dest.write_bytes(await resp.read())
            logger.info(f"PDF saved: {dest.name}")


async def download_many(items: list[dict], kind: str, concurrency: int = 4):
    """items: [{symbol, date, title, url}]"""
    sem = asyncio.Semaphore(concurrency)
    index = cache.read("pdfs", _INDEX_KEY)
    known_urls = set(index["url"].tolist()) if index is not None else set()

    new_rows = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for it in items:
            if it["url"] in known_urls:
                continue
            dest = target_path(kind, it["symbol"], it["date"], it["title"])
            tasks.append(_download_one(session, it["url"], dest, sem))
            new_rows.append({
                "url": it["url"],
                "md5": hashlib.md5(it["url"].encode()).hexdigest(),
                "kind": kind,
                "symbol": it["symbol"],
                "date": it["date"],
                "title": it["title"],
                "path": str(dest),
            })
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    if new_rows:
        cache.append_dedup("pdfs", _INDEX_KEY, pd.DataFrame(new_rows), dedup_cols=["url"])


def download_sync(items: list[dict], kind: str, concurrency: int = 4):
    """同步入口。"""
    if not items:
        return
    asyncio.run(download_many(items, kind, concurrency))
