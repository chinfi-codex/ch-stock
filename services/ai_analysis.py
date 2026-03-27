#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AI 分析服务层
包含业务流程编排，协调各个原子能力完成复杂业务场景
"""

from typing import List, Dict, Optional
import streamlit as st

# 从原子能力层导入
from tools.ai_analysis import (
    build_macro_prompt,
    build_market_overview_prompt,
    build_index_analysis_prompt,
    build_stock_classification_prompt,
    run_ai_analysis,
    display_ai_analysis,
)


def analyze_external_assets(
    usdcny_series: List[Dict],
    btc_series: List[Dict],
    xau_series: List[Dict],
    wti_series: List[Dict],
    us10y_series: List[Dict],
    show_ui: bool = True,
) -> Optional[str]:
    """外围资产 AI 分析（业务流程）

    协调数据准备、Prompt构建、AI调用、结果展示等原子能力，
    完成外围资产宏观分析的完整业务流程。

    Args:
        usdcny_series: 人民币汇率序列
        btc_series: 比特币序列
        xau_series: 黄金序列
        wti_series: WTI原油序列
        us10y_series: 美债10Y收益率序列
        show_ui: 是否在 Streamlit 中展示结果

    Returns:
        AI 分析结果文本
    """
    # 检查数据是否可用
    has_data = any([usdcny_series, btc_series, xau_series, wti_series, us10y_series])

    if not has_data:
        if show_ui:
            st.warning("暂无外围资产数据，无法进行分析")
        return None

    # 构建 prompt（原子能力）
    prompt = build_macro_prompt(
        usdcny_series, btc_series, xau_series, wti_series, us10y_series
    )

    # 执行业务流程
    if show_ui:
        return display_ai_analysis(
            title="🤖 AI 宏观市场分析",
            prompt=prompt,
            cache_key="external_macro_analysis",
            expanded=True,
            spinner_text="🤖 AI 正在分析全球市场联动...",
            show_title=False,
        )
    else:
        return run_ai_analysis(prompt, cache_key="external_macro_analysis")


def analyze_market_overview(
    market_data: Dict[str, any], date_str: str = "", show_ui: bool = True
) -> Optional[str]:
    """市场全貌 AI 分析（业务流程）

    Args:
        market_data: 市场数据字典
        date_str: 日期字符串
        show_ui: 是否在 Streamlit 中展示结果

    Returns:
        AI 分析结果文本
    """
    prompt = build_market_overview_prompt(market_data, date_str)

    if show_ui:
        return display_ai_analysis(
            title="🤖 AI 市场全貌分析",
            prompt=prompt,
            cache_key=f"market_overview_{date_str}",
            expanded=True,
            spinner_text="🤖 AI 正在分析市场情绪...",
            show_title=False,
        )
    else:
        return run_ai_analysis(prompt, cache_key=f"market_overview_{date_str}")


def analyze_index_technical(
    sh_index_data: Dict[str, any],
    cyb_index_data: Dict[str, any],
    kcb_index_data: Dict[str, any],
    show_ui: bool = True,
) -> Optional[str]:
    """大盘指数技术分析（业务流程）- 三大指数合并分析

    Args:
        sh_index_data: 上证指数数据
        cyb_index_data: 创业板指数数据
        kcb_index_data: 科创板指数数据
        show_ui: 是否在 Streamlit 中展示结果

    Returns:
        AI 分析结果文本
    """
    prompt = build_index_analysis_prompt(sh_index_data, cyb_index_data, kcb_index_data)

    title = "🤖 AI 三大指数技术分析"
    cache_key = "index_tech_combined"

    if show_ui:
        return display_ai_analysis(
            title=title,
            prompt=prompt,
            cache_key=cache_key,
            expanded=True,
            spinner_text="🤖 AI 正在分析三大指数...",
            show_title=False,
            timeout=180,
        )
    else:
        return run_ai_analysis(prompt, cache_key=cache_key)


def analyze_stock_classification(
    stock_list: list,
    group_name: str,
    show_ui: bool = True,
) -> Optional[str]:
    """股票列表AI分类分析（业务流程）

    按行业和概念对股票列表进行智能分类

    Args:
        stock_list: 股票列表
        group_name: 分组名称
        show_ui: 是否在 Streamlit 中展示结果

    Returns:
        AI 分类分析结果文本
    """
    if not stock_list:
        if show_ui:
            st.info("暂无股票数据，无法进行分类分析")
        return None

    prompt = build_stock_classification_prompt(stock_list, group_name)

    cache_key = f"stock_classification_{group_name}_{hash(str(stock_list)) % 10000}"

    if show_ui:
        return display_ai_analysis(
            title=f"🤖 AI 股票分类 - {group_name}",
            prompt=prompt,
            cache_key=cache_key,
            expanded=True,
            spinner_text=f"🤖 AI 正在分析 {group_name} 的股票分类...",
            show_title=False,
            timeout=120,
        )
    else:
        return run_ai_analysis(prompt, cache_key=cache_key)
