#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AI 分析原子能力。
"""

import hashlib
from typing import Any, Dict, List

import streamlit as st

from infra.llm_client import call_kimi_print
from infra.prompt_templates import get_jinja_env


def format_series_for_ai(
    series: List[Dict], asset_name: str, max_items: int = 60, display_count: int = 10
) -> str:
    """格式化资产数据序列为 AI 分析文本。"""
    if not series:
        return f"{asset_name}: 数据暂不可用"

    recent_series = series[-max_items:] if len(series) > max_items else series
    lines = []
    for item in recent_series:
        date_str = item.get("date", "")
        value = item.get("value")
        if value is not None:
            lines.append(f"  {date_str}: {value}")

    values = [item.get("value") for item in recent_series if item.get("value") is not None]
    stats = ""
    if values:
        latest = values[-1]
        earliest = values[0]
        change_pct = ((latest - earliest) / earliest * 100) if earliest != 0 else 0
        stats = (
            f"\n  [统计: 最新={latest:.4f}, 期初={earliest:.4f}, 变化={change_pct:+.2f}%]"
        )

    display_lines = lines[-display_count:] if len(lines) > display_count else lines
    return (
        f"{asset_name} ({len(recent_series)}条数据):\n"
        + "\n".join(display_lines)
        + stats
    )


def format_market_summary_for_ai(market_data: Dict[str, Any]) -> str:
    """格式化市场概况数据为 AI 输入文本。"""
    lines = []
    for key in ["上涨", "下跌", "涨停", "跌停", "成交额", "活跃度"]:
        if key in market_data:
            lines.append(f"{key}: {market_data[key]}")

    if "indices" in market_data:
        for idx_name, idx_data in market_data["indices"].items():
            if isinstance(idx_data, dict) and "close" in idx_data:
                lines.append(f"{idx_name}: {idx_data['close']}")

    return "\n".join(lines) if lines else "市场数据暂不可用"


@st.cache_data(ttl="1h", show_spinner=False)
def cached_ai_analysis(
    prompt_hash: str, prompt: str, cache_key: str = "", timeout: int = 120
) -> str:
    """带缓存的 AI 调用。"""
    return call_kimi_print(prompt, cache_key=cache_key, timeout=timeout)


def run_ai_analysis(
    prompt: str, cache_key: str = "", use_cache: bool = True, timeout: int = 120
) -> str:
    """执行 AI 分析。"""
    if use_cache:
        prompt_hash = hashlib.md5(prompt.encode()).hexdigest()
        return cached_ai_analysis(prompt_hash, prompt, cache_key, timeout)
    return call_kimi_print(prompt, cache_key=cache_key, timeout=timeout)


def build_macro_prompt(
    usdcny_series: List[Dict],
    btc_series: List[Dict],
    xau_series: List[Dict],
    wti_series: List[Dict],
    us10y_series: List[Dict],
) -> str:
    """构建外围资产宏观分析 Prompt。"""
    env = get_jinja_env()
    template = env.get_template("external_assets.md")
    return template.render(
        usdcny_data=format_series_for_ai(usdcny_series, "人民币汇率 (USD/CNY)"),
        btc_data=format_series_for_ai(btc_series, "比特币 (BTC/USD)"),
        xau_data=format_series_for_ai(xau_series, "XAU金价 (USD/oz)"),
        wti_data=format_series_for_ai(wti_series, "WTI油价 (USD/bbl)"),
        us10y_data=format_series_for_ai(us10y_series, "美国10Y国债收益率 (%)"),
    )


def build_market_overview_prompt(
    market_data: Dict[str, Any], date_str: str = ""
) -> str:
    """构建市场全貌分析 Prompt。"""
    env = get_jinja_env()
    template = env.get_template("market_overview.md")
    market_summary = format_market_summary_for_ai(market_data)
    date_info = f" ({date_str})" if date_str else ""
    return template.render(market_summary=market_summary, date_info=date_info)


def build_index_analysis_prompt(
    sh_index_data: Dict[str, Any],
    cyb_index_data: Dict[str, Any],
    kcb_index_data: Dict[str, Any],
) -> str:
    """构建指数技术分析 Prompt。"""
    env = get_jinja_env()
    template = env.get_template("index_technical.md")
    return template.render(
        sh_index_data=sh_index_data,
        cyb_index_data=cyb_index_data,
        kcb_index_data=kcb_index_data,
    )


def format_stock_list_for_classification(stock_list: list) -> str:
    """格式化股票列表为分类分析输入。"""
    if not stock_list:
        return "无股票数据"

    lines = []
    for stock in stock_list:
        code = stock.get("code", "")
        name = stock.get("name", "")
        pct = stock.get("pct_chg", 0)
        mv = stock.get("total_mv", 0) or stock.get("total_mv_yi", 0)
        info_parts = [f"{name}({code})"]
        if pct:
            info_parts.append(f"涨幅:{pct:.1f}%")
        if mv:
            info_parts.append(f"市值:{mv:.0f}亿")
        lines.append(" - " + " ".join(info_parts))
    return "\n".join(lines)


def build_stock_classification_prompt(stock_list: list, group_name: str) -> str:
    """构建股票分类分析 Prompt。"""
    env = get_jinja_env()
    template = env.get_template("stock_classification.md")
    return template.render(
        group_name=group_name,
        stock_list=format_stock_list_for_classification(stock_list),
    )
