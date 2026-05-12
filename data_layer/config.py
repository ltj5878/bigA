"""统一配置：缓存路径、cookie、UA、限频参数。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


HOME_DIR = Path(os.path.expanduser("~/.big_a"))
CACHE_DIR = HOME_DIR / "cache"
PDF_DIR = HOME_DIR / "pdfs"
JOBS_DB = HOME_DIR / "jobs.sqlite"

for _p in (CACHE_DIR, PDF_DIR, PDF_DIR / "reports", PDF_DIR / "announcements"):
    _p.mkdir(parents=True, exist_ok=True)


DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


@dataclass
class Config:
    cache_dir: Path = CACHE_DIR
    pdf_dir: Path = PDF_DIR
    jobs_db: Path = JOBS_DB
    ua: str = DEFAULT_UA

    # pywencai / 问财 cookie，从浏览器登录 iwencai.com 后 F12 复制
    iwencai_cookie: str = field(default_factory=lambda: os.environ.get("IWENCAI_COOKIE", ""))

    # 限速：每秒最大请求数
    cninfo_qps: float = 1.0
    eastmoney_qps: float = 4.0

    # APScheduler 时区
    timezone: str = "Asia/Shanghai"

    # 自选股清单（轮询新闻、研报、公告时用）
    watchlist: tuple[str, ...] = ("000001", "600000", "300750", "601318")


config = Config()
