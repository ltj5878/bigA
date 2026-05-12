"""Piotroski F-Score 财务排雷打分（9 项，0-9 分）。

参考：Joseph Piotroski 2000 "Value Investing: The Use of Historical
Financial Statement Information to Separate Winners from Losers from Value Stocks"

逐项 0/1：
  1. NetProfit > 0          —— 净利润为正
  2. ROA > 0                —— 总资产净利率为正
  3. OCF > 0                —— 经营现金流为正（用 OCF/营收>0 等价）
  4. OCF > NetProfit        —— 现金流胜过净利润（盈利质量）
  5. ΔLeverage <= 0         —— 资产负债率没上升
  6. ΔCurrentRatio > 0      —— 流动比率改善
  7. NoEquityIssuance       —— 未增发（暂不严格判定，给 1 分宽松处理）
  8. ΔGrossMargin > 0       —— 毛利率改善
  9. ΔAssetTurnover > 0     —— 总资产周转改善

A 股适配：
- 银行/保险/证券类指标含义差异大（无毛利率、流动比率），打分时跳过这些项，
  按命中项数/可评估项数等比缩放回 0-9 分制（保持阈值一致性）。
- 数据缺失视为该项不得分。
"""
from __future__ import annotations


def f_score(factors: dict) -> tuple[int, dict]:
    """根据 factors.fetch_factors() 的输出计算 F-Score。

    返回 (得分 0-9, 各项明细)。明细 key 是中文，值是 1/0/None（None 表示无法评估）。
    """
    items: dict = {}

    def safe_gt(a, b):
        """a > b，任一为 None 时返 None。"""
        if a is None or b is None: return None
        return 1 if a > b else 0

    # 1) 净利润 > 0
    items["净利润为正"] = safe_gt(factors.get("net_profit"), 0)

    # 2) ROA > 0
    items["ROA为正"] = safe_gt(factors.get("roa"), 0)

    # 3) 经营现金流 > 0（用 OCF/营收 > 0 等价）
    items["经营现金流为正"] = safe_gt(factors.get("ocf_revenue"), 0)

    # 4) OCF > NetProfit（盈利质量）：用 OCF/营收 > 净利润/营收 等价于 OCF > NetProfit
    ocf_r = factors.get("ocf_revenue")
    rev   = factors.get("revenue")
    npf   = factors.get("net_profit")
    if ocf_r is not None and rev is not None and npf is not None and rev > 0:
        ocf_abs = ocf_r / 100.0 * rev  # 字段是百分比
        items["现金流胜净利润"] = 1 if ocf_abs > npf else 0
    else:
        items["现金流胜净利润"] = None

    # 5) 资产负债率没上升：debt_ratio_yoy <= 0
    d_yoy = factors.get("debt_ratio_yoy")
    items["负债率不上升"] = None if d_yoy is None else (1 if d_yoy <= 0 else 0)

    # 6) 流动比率上升：current_ratio > current_ratio_prev
    items["流动比率改善"] = safe_gt(
        factors.get("current_ratio"), factors.get("current_ratio_prev")
    )

    # 7) 未增发（A 股财务接口直接判定难，宽松给 1；后续可接公告层判定增发）
    items["未增发"] = 1

    # 8) 毛利率改善：gross_margin_yoy > 0
    gm_yoy = factors.get("gross_margin_yoy")
    items["毛利率改善"] = None if gm_yoy is None else (1 if gm_yoy > 0 else 0)

    # 9) 总资产周转改善
    items["周转率改善"] = safe_gt(
        factors.get("asset_turn"), factors.get("asset_turn_prev")
    )

    # 计分：可评估项 v / 9 → 等比缩放
    hits  = sum(1 for v in items.values() if v == 1)
    valid = sum(1 for v in items.values() if v is not None)
    score = round(hits / valid * 9) if valid > 0 else 0
    return score, items
