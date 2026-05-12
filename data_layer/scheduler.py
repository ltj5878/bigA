"""APScheduler 定时任务编排。

注册任务：
- 财联社快讯       每 60 秒
- 东财全球资讯     每 2 分钟
- 个股新闻         每 5 分钟（轮询自选股）
- 同花顺热点       每 10 分钟
- 收盘后全量同步   每天 17:00（基本面 + 公告 + PDF 下载）
"""
from __future__ import annotations

from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger

from .announcements.cninfo_provider import CninfoAnnouncements
from .config import config
from .fundamentals.akshare_fundamentals import AkshareFundamentals
from .news.cls_telegraph import CLSTelegraph
from .news.global_info_em import GlobalInfoEM
from .news.stock_news_em import StockNewsEM
from .pdf_downloader import download_sync
from .quotes.ths_hotspot_provider import THSHotspot
from .reports.eastmoney_provider import EastmoneyReports


def daily_full_sync():
    """收盘后跑一遍：研报、公告、基本面（含 PDF 下载）。"""
    logger.info("daily_full_sync 开始")
    today = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    em = EastmoneyReports()
    cninfo = CninfoAnnouncements()
    fund = AkshareFundamentals()

    # 1) 最新研报 + PDF
    try:
        df = em.list_recent(q_type=0, page_size=50)
        download_sync(em.to_pdf_items(df), kind="reports", concurrency=4)
    except Exception as e:
        logger.exception(f"研报同步失败: {e}")

    # 2) 自选股近 30 天公告 + PDF
    for sym in config.watchlist:
        try:
            df = cninfo.query(sym, start_date=start, end_date=today)
            download_sync(cninfo.to_pdf_items(df), kind="announcements", concurrency=2)
        except Exception as e:
            logger.exception(f"公告同步失败 {sym}: {e}")

    # 3) 基本面快照
    for sym in config.watchlist:
        try:
            fund.individual_info(sym)
            fund.financial_indicator(sym)
        except Exception as e:
            logger.exception(f"基本面同步失败 {sym}: {e}")

    logger.info("daily_full_sync 结束")


def _cls_job():
    CLSTelegraph().poll_incremental()


def _global_job():
    GlobalInfoEM().poll_incremental()


def _stock_news_job():
    StockNewsEM().poll_watchlist()


def _hotspot_job():
    try:
        THSHotspot().hot_sectors()
    except Exception as e:
        logger.warning(f"热点刷新失败: {e}")


def build_scheduler() -> BackgroundScheduler:
    # 用默认内存 jobstore，避免 pickle 限制；调度器重启会重新注册任务，没问题。
    sched = BackgroundScheduler(timezone=config.timezone)
    sched.add_job(_cls_job,         "interval", seconds=60, id="cls_poll",         replace_existing=True)
    sched.add_job(_global_job,      "interval", minutes=2,  id="global_em_poll",   replace_existing=True)
    sched.add_job(_stock_news_job,  "interval", minutes=5,  id="stock_news_poll",  replace_existing=True)
    sched.add_job(_hotspot_job,     "interval", minutes=10, id="hotspot_refresh",  replace_existing=True)
    sched.add_job(daily_full_sync,  "cron", hour=17, minute=0, id="daily_full_sync", replace_existing=True)
    return sched


def run_forever():
    """前台阻塞跑调度器。"""
    sched = build_scheduler()
    sched.start()
    logger.info("调度器已启动；Ctrl+C 退出")
    try:
        import time
        while True:
            time.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()
        logger.info("调度器已停止")


if __name__ == "__main__":
    run_forever()
