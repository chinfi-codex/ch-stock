#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AI 分析业务流程编排。
"""

from typing import Dict, List, Optional

import streamlit as st

from tools.ai_analysis import (
    build_index_analysis_prompt,
    build_macro_prompt,
    build_market_overview_prompt,
    build_stock_classification_prompt,
    run_ai_analysis,
)


def display_ai_analysis(
    title: str = "AI 智能分析",
    ai_result: Optional[str] = None,
    prompt: Optional[str] = None,
    cache_key: str = "",
    expanded: bool = True,
    spinner_text: str = "AI 正在分析...",
    error_message: str = "AI 分析暂时不可用",
    help_text: str = "",
    show_title: bool = True,
    timeout: int = 120,
) -> Optional[str]:
    """统一展示 AI 分析结果。"""
    with st.container():
        if show_title:
            st.markdown(f"#### {title}")

        result = ai_result
        if result is None and prompt is not None:
            with st.spinner(spinner_text):
                try:
                    result = run_ai_analysis(
                        prompt, cache_key=cache_key, timeout=timeout
                    )
                except Exception as e:
                    st.error(f"{error_message}: {str(e)}")
                    st.info("请稍后刷新重试，或检查 API 配置")
                    return None

        if result:
            st.markdown(
                """
                <style>
                .ai-output-container {
                    background-color: #f8f9fa;
                    padding: 15px;
                    border-radius: 8px;
                    border-left: 4px solid #4c6ef5;
                    margin: 10px 0;
                }
                </style>
                """,
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<div class="ai-output-container">{result}</div>',
                unsafe_allow_html=True,
            )
            if help_text:
                st.caption(help_text)

        return result


def analyze_external_assets(
    usdcny_series: List[Dict],
    btc_series: List[Dict],
    xau_series: List[Dict],
    wti_series: List[Dict],
    us10y_series: List[Dict],
    show_ui: bool = True,
) -> Optional[str]:
    """外围资产 AI 分析流程。"""
    has_data = any([usdcny_series, btc_series, xau_series, wti_series, us10y_series])
    if not has_data:
        if show_ui:
            st.warning("暂无外围资产数据，无法进行分析")
        return None

    prompt = build_macro_prompt(
        usdcny_series, btc_series, xau_series, wti_series, us10y_series
    )
    if show_ui:
        return display_ai_analysis(
            title="AI 宏观市场分析",
            prompt=prompt,
            cache_key="external_macro_analysis",
            expanded=True,
            spinner_text="AI 正在分析全球市场联动...",
            show_title=False,
        )
    return run_ai_analysis(prompt, cache_key="external_macro_analysis")


def analyze_market_overview(
    market_data: Dict[str, any], date_str: str = "", show_ui: bool = True
) -> Optional[str]:
    """市场全貌 AI 分析流程。"""
    prompt = build_market_overview_prompt(market_data, date_str)
    if show_ui:
        return display_ai_analysis(
            title="AI 市场全貌分析",
            prompt=prompt,
            cache_key=f"market_overview_{date_str}",
            expanded=True,
            spinner_text="AI 正在分析市场情绪...",
            show_title=False,
        )
    return run_ai_analysis(prompt, cache_key=f"market_overview_{date_str}")


def analyze_index_technical(
    sh_index_data: Dict[str, any],
    cyb_index_data: Dict[str, any],
    kcb_index_data: Dict[str, any],
    show_ui: bool = True,
) -> Optional[str]:
    """三大指数技术分析流程。"""
    prompt = build_index_analysis_prompt(sh_index_data, cyb_index_data, kcb_index_data)
    title = "AI 三大指数技术分析"
    cache_key = "index_tech_combined"

    if show_ui:
        return display_ai_analysis(
            title=title,
            prompt=prompt,
            cache_key=cache_key,
            expanded=True,
            spinner_text="AI 正在分析三大指数...",
            show_title=False,
            timeout=180,
        )
    return run_ai_analysis(prompt, cache_key=cache_key)


def analyze_stock_classification(
    stock_list: list,
    group_name: str,
    show_ui: bool = True,
) -> Optional[str]:
    """股票分类 AI 分析流程。"""
    if not stock_list:
        if show_ui:
            st.info("暂无股票数据，无法进行分类分析")
        return None

    prompt = build_stock_classification_prompt(stock_list, group_name)
    cache_key = f"stock_classification_{group_name}_{hash(str(stock_list)) % 10000}"

    if show_ui:
        return display_ai_analysis(
            title=f"AI 股票分类 - {group_name}",
            prompt=prompt,
            cache_key=cache_key,
            expanded=True,
            spinner_text=f"AI 正在分析 {group_name} 的股票分类...",
            show_title=False,
            timeout=120,
        )
    return run_ai_analysis(prompt, cache_key=cache_key)
