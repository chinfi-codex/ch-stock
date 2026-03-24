#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AI 宏观分析工具模块
提供统一的 AI 分析调用接口，支持外围资产、大盘分析等多种场景
"""

import hashlib
import os
from pathlib import Path

import streamlit as st
from jinja2 import Environment, FileSystemLoader, TemplateNotFound
from typing import List, Dict, Any, Optional, Callable
from tools.ai_utils import call_kimi_print


# Initialize Jinja2 environment for prompt templates
PROMPTS_DIR = Path(__file__).parent.parent / "prompts"
_jinja_env = None


class EscapedMarkdownLoader(FileSystemLoader):
    """自定义加载器，读取模板后移除 Markdown 转义字符"""

    def get_source(self, environment, template):
        source, filename, uptodate = super().get_source(environment, template)
        # 移除常见的 Markdown 转义字符
        source = source.replace("\\-", "-")
        source = source.replace("\\*", "*")
        source = source.replace("\\_", "_")
        source = source.replace("\\#", "#")
        source = source.replace("\\>", ">")
        source = source.replace("\\<", "<")
        source = source.replace("\\`", "`")
        source = source.replace("\\[", "[")
        source = source.replace("\\]", "]")
        source = source.replace("\\(", "(")
        source = source.replace("\\)", ")")
        return source, filename, uptodate


def get_jinja_env():
    """Get or create Jinja2 environment for prompt templates"""
    global _jinja_env
    if _jinja_env is None:
        if PROMPTS_DIR.exists():
            _jinja_env = Environment(
                loader=EscapedMarkdownLoader(str(PROMPTS_DIR)),
                trim_blocks=True,
                lstrip_blocks=True,
            )
        else:
            raise FileNotFoundError(f"Prompts directory not found: {PROMPTS_DIR}")
    return _jinja_env


def load_prompt_template(template_name: str) -> str:
    """Load prompt template from prompts directory

    Args:
        template_name: Template filename (e.g., 'external_assets.md')

    Returns:
        str: Template content

    Raises:
        FileNotFoundError: If template file doesn't exist
        TemplateNotFound: If template cannot be loaded
    """
    env = get_jinja_env()
    template = env.get_template(template_name)
    return template.render()


def format_series_for_ai(
    series: List[Dict], asset_name: str, max_items: int = 60, display_count: int = 10
) -> str:
    """格式化资产数据序列为AI分析的文本格式

    Args:
        series: 数据序列列表，每项包含 date 和 value
        asset_name: 资产名称
        max_items: 最多保留的记录数
        display_count: 在prompt中展示的数据条数

    Returns:
        格式化的字符串，用于 prompt 注入
    """
    if not series or len(series) == 0:
        return f"{asset_name}: 数据暂不可用"

    # 取最近的数据
    recent_series = series[-max_items:] if len(series) > max_items else series

    # 格式化数据点
    lines = []
    for item in recent_series:
        date_str = item.get("date", "")
        value = item.get("value")
        if value is not None:
            lines.append(f"  {date_str}: {value}")

    # 计算统计数据
    values = [
        item.get("value") for item in recent_series if item.get("value") is not None
    ]
    if values:
        latest = values[-1]
        earliest = values[0]
        change_pct = ((latest - earliest) / earliest * 100) if earliest != 0 else 0
        stats = f"\n  [统计: 最新={latest:.4f}, 期初={earliest:.4f}, 变化={change_pct:+.2f}%]"
    else:
        stats = ""

    # 只显示最后 display_count 条避免过长
    display_lines = lines[-display_count:] if len(lines) > display_count else lines
    return (
        f"{asset_name} ({len(recent_series)}条数据):\n"
        + "\n".join(display_lines)
        + stats
    )


def format_market_summary_for_ai(market_data: Dict[str, Any]) -> str:
    """格式化市场概况数据为AI分析的文本格式

    Args:
        market_data: 市场数据字典，包含涨跌家数、涨停跌停、成交额等

    Returns:
        格式化的字符串
    """
    lines = []

    # 基础指标
    if "上涨" in market_data:
        lines.append(f"上涨家数: {market_data['上涨']}")
    if "下跌" in market_data:
        lines.append(f"下跌家数: {market_data['下跌']}")
    if "涨停" in market_data:
        lines.append(f"涨停家数: {market_data['涨停']}")
    if "跌停" in market_data:
        lines.append(f"跌停家数: {market_data['跌停']}")
    if "成交额" in market_data:
        lines.append(f"成交额: {market_data['成交额']}")
    if "活跃度" in market_data:
        lines.append(f"市场活跃度: {market_data['活跃度']}")

    # 指数数据
    if "indices" in market_data:
        for idx_name, idx_data in market_data["indices"].items():
            if isinstance(idx_data, dict) and "close" in idx_data:
                lines.append(f"{idx_name}: {idx_data['close']}")

    return "\n".join(lines) if lines else "市场数据暂不可用"


@st.cache_data(ttl="1h", show_spinner=False)
def cached_ai_analysis(
    prompt_hash: str, prompt: str, cache_key: str = "", timeout: int = 120
) -> str:
    """带缓存的 AI 分析调用

    Args:
        prompt_hash: 提示词的hash值，用于缓存键
        prompt: 完整的提示词内容
        cache_key: 额外的缓存标识
        timeout: 超时时间（秒），默认120秒

    Returns:
        AI 分析结果文本
    """
    return call_kimi_print(prompt, cache_key=cache_key, timeout=timeout)


def run_ai_analysis(
    prompt: str, cache_key: str = "", use_cache: bool = True, timeout: int = 120
) -> str:
    """执行 AI 分析

    Args:
        prompt: 分析提示词
        cache_key: 缓存标识
        use_cache: 是否使用缓存
        timeout: 超时时间（秒），默认120秒

    Returns:
        AI 分析结果
    """
    if use_cache:
        prompt_hash = hashlib.md5(prompt.encode()).hexdigest()
        return cached_ai_analysis(prompt_hash, prompt, cache_key, timeout)
    else:
        return call_kimi_print(prompt, cache_key=cache_key, timeout=timeout)


def display_ai_analysis(
    title: str = "🤖 AI 智能分析",
    ai_result: Optional[str] = None,
    prompt: Optional[str] = None,
    cache_key: str = "",
    expanded: bool = True,
    spinner_text: str = "🤖 AI 正在分析...",
    error_message: str = "AI 分析暂时不可用",
    help_text: str = "",
    show_title: bool = True,
    timeout: int = 120,
) -> Optional[str]:
    """统一的 AI 分析展示组件

    此组件整合了：加载状态 -> AI调用 -> 结果展示 -> 错误处理 的完整流程

    Args:
        title: 模块标题
        ai_result: 已有的 AI 分析结果（如果提供则跳过调用）
        prompt: AI 分析提示词（如果未提供 ai_result）
        cache_key: 缓存键
        expanded: 是否默认展开分析结果
        spinner_text: 加载时显示的文本
        error_message: 错误时显示的消息
        help_text: 底部提示文本
        show_title: 是否显示标题（默认 True）
        timeout: 超时时间（秒），默认120秒

    Returns:
        AI 分析结果文本，如果出错则返回 None
    """
    with st.container():
        if show_title:
            st.markdown(f"#### {title}")

        result = ai_result

        # 如果没有提供结果，则调用 AI
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

        # 展示结果
        if result:
            with st.container():
                # 添加浅色底色容器样式
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


def build_macro_prompt(
    usdcny_series: List[Dict],
    btc_series: List[Dict],
    xau_series: List[Dict],
    wti_series: List[Dict],
    us10y_series: List[Dict],
) -> str:
    """构建外围资产宏观分析 Prompt

    Args:
        usdcny_series: 人民币汇率序列
        btc_series: 比特币序列
        xau_series: 黄金序列
        wti_series: WTI原油序列
        us10y_series: 美债10Y收益率序列

    Returns:
        完整的 prompt 字符串

    Raises:
        FileNotFoundError: 如果模板文件不存在
    """
    # 格式化各类资产数据
    usdcny_data = format_series_for_ai(usdcny_series, "人民币汇率 (USD/CNY)")
    btc_data = format_series_for_ai(btc_series, "比特币 (BTC/USD)")
    xau_data = format_series_for_ai(xau_series, "XAU金价 (USD/oz)")
    wti_data = format_series_for_ai(wti_series, "WTI油价 (USD/bbl)")
    us10y_data = format_series_for_ai(us10y_series, "美国10Y国债收益率 (%)")

    # 从模板文件加载并渲染
    env = get_jinja_env()
    template = env.get_template("external_assets.md")
    return template.render(
        usdcny_data=usdcny_data,
        btc_data=btc_data,
        xau_data=xau_data,
        wti_data=wti_data,
        us10y_data=us10y_data,
    )


def build_market_overview_prompt(
    market_data: Dict[str, Any], date_str: str = ""
) -> str:
    """构建市场全貌分析 Prompt

    Args:
        market_data: 市场数据字典
        date_str: 日期字符串

    Returns:
        完整的 prompt 字符串

    Raises:
        FileNotFoundError: 如果模板文件不存在
    """
    market_summary = format_market_summary_for_ai(market_data)
    date_info = f" ({date_str})" if date_str else ""

    # 从模板文件加载并渲染
    env = get_jinja_env()
    template = env.get_template("market_overview.md")
    return template.render(market_summary=market_summary, date_info=date_info)


def build_index_analysis_prompt(
    sh_index_data: Dict[str, Any],
    cyb_index_data: Dict[str, Any],
    kcb_index_data: Dict[str, Any],
) -> str:
    """构建大盘指数分析 Prompt

    Args:
        sh_index_data: 上证指数数据，包含K线、技术指标等
        cyb_index_data: 创业板指数数据，包含K线、技术指标等
        kcb_index_data: 科创板指数数据，包含K线、技术指标等

    Returns:
        完整的 prompt 字符串

    Raises:
        FileNotFoundError: 如果模板文件不存在
    """
    # 从模板文件加载并渲染
    env = get_jinja_env()
    template = env.get_template("index_technical.md")
    return template.render(
        sh_index_data=sh_index_data,
        cyb_index_data=cyb_index_data,
        kcb_index_data=kcb_index_data,
    )


# 便捷的预设分析函数


def analyze_external_assets(
    usdcny_series: List[Dict],
    btc_series: List[Dict],
    xau_series: List[Dict],
    wti_series: List[Dict],
    us10y_series: List[Dict],
    show_ui: bool = True,
) -> Optional[str]:
    """外围资产 AI 分析（便捷函数）

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

    # 构建 prompt
    prompt = build_macro_prompt(
        usdcny_series, btc_series, xau_series, wti_series, us10y_series
    )

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
    market_data: Dict[str, Any], date_str: str = "", show_ui: bool = True
) -> Optional[str]:
    """市场全貌 AI 分析（便捷函数）

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
    sh_index_data: Dict[str, Any],
    cyb_index_data: Dict[str, Any],
    kcb_index_data: Dict[str, Any],
    show_ui: bool = True,
) -> Optional[str]:
    """大盘指数技术分析（便捷函数）- 三大指数合并分析

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
