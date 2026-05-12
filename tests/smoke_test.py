"""5 个模块的冒烟测试：依次拉一条数据，验证接口通畅。

用法：python -m tests.smoke_test
失败的源会被打印异常并继续，便于一次性看到所有问题。
"""
from __future__ import annotations

import sys
import traceback
from datetime import datetime, timedelta


def _run(name: str, fn):
    print(f"\n=== {name} ===")
    try:
        out = fn()
        if hasattr(out, "head"):
            print(out.head(3))
            print(f"  shape: {out.shape}")
        else:
            print(out)
    except Exception as e:
        print(f"  ❌ 失败: {type(e).__name__}: {e}")
        traceback.print_exc(limit=2)


def main():
    sym = "000001"

    # 模块一：行情层
    from data_layer.quotes.mootdx_provider import MootdxQuotes
    from data_layer.quotes.tencent_provider import TencentQuotes
    from data_layer.quotes.ths_hotspot_provider import THSHotspot
    _run("行情·mootdx 日线", lambda: MootdxQuotes().daily(sym))
    _run("行情·腾讯(efinance)日线", lambda: TencentQuotes().daily(sym))
    _run("行情·同花顺热点(需cookie)", lambda: THSHotspot().hot_sectors())

    # 模块二：研报层
    from data_layer.reports.akshare_ths_provider import AkshareResearchReport
    from data_layer.reports.eastmoney_provider import EastmoneyReports
    from data_layer.reports.iwencai_provider import IwencaiReports
    _run("研报·东财 reportapi", lambda: EastmoneyReports().list_recent(page_size=5))
    _run("研报·akshare 个股研报", lambda: AkshareResearchReport().by_stock(sym))
    _run("研报·iwencai 问财(需cookie)", lambda: IwencaiReports().by_stock(sym))

    # 模块三：新闻层
    from data_layer.news.cls_telegraph import CLSTelegraph
    from data_layer.news.global_info_em import GlobalInfoEM
    from data_layer.news.stock_news_em import StockNewsEM
    _run("新闻·个股新闻 EM", lambda: StockNewsEM().by_stock(sym))
    _run("新闻·财联社快讯", lambda: CLSTelegraph().latest(5))
    _run("新闻·东财全球资讯", lambda: GlobalInfoEM().latest(5))

    # 模块四：基础数据层
    from data_layer.fundamentals.akshare_fundamentals import AkshareFundamentals
    from data_layer.fundamentals.mootdx_f10 import MootdxF10
    from data_layer.fundamentals.mootdx_finance import MootdxFinance
    _run("基础·akshare 个股概况", lambda: AkshareFundamentals().individual_info(sym))
    _run("基础·akshare 财务指标", lambda: AkshareFundamentals().financial_indicator(sym))
    _run("基础·mootdx F10", lambda: MootdxF10().by_stock(sym))
    # mootdx finance 会下载几百 MB 财务库，冒烟阶段跳过
    # _run("基础·mootdx 财务库", lambda: MootdxFinance().latest())

    # 模块五：公告层
    from data_layer.announcements.cninfo_provider import CninfoAnnouncements
    from data_layer.announcements.mootdx_f10_announce import MootdxF10Announce
    today = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    _run("公告·巨潮 cninfo", lambda: CninfoAnnouncements().query(sym, start, today))
    _run("公告·mootdx F10 兜底", lambda: MootdxF10Announce().by_stock(sym))

    print("\n冒烟测试完成。")


if __name__ == "__main__":
    sys.exit(main() or 0)
