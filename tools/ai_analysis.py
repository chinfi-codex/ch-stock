#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""AI 分析原子能力。"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List

import streamlit as st

from infra.llm_client import call_kimi_print
from infra.prompt_templates import get_jinja_env


def format_series_for_ai(
    series: List[Dict[str, Any]],
    asset_name: str,
    max_items: int = 60,
    display_count: int = 10,
) -> str:
    """格式化资产时间序列，供 AI 分析使用。"""
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
    return f"{asset_name} ({len(recent_series)}条数据):\n" + "\n".join(display_lines) + stats


def format_market_summary_for_ai(market_data: Dict[str, Any]) -> str:
    """格式化市场概况数据，供 AI 分析使用。"""
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
    prompt_hash: str,
    prompt: str,
    cache_key: str = "",
    timeout: int = 120,
) -> str:
    """带缓存的 AI 调用。"""
    return call_kimi_print(prompt, cache_key=cache_key, timeout=timeout)


def run_ai_analysis(
    prompt: str,
    cache_key: str = "",
    use_cache: bool = True,
    timeout: int = 120,
) -> str:
    """执行单次 AI 分析。"""
    if use_cache:
        prompt_hash = hashlib.md5(prompt.encode("utf-8")).hexdigest()
        return cached_ai_analysis(prompt_hash, prompt, cache_key, timeout)
    return call_kimi_print(prompt, cache_key=cache_key, timeout=timeout)


def build_macro_prompt(
    usdcny_series: List[Dict[str, Any]],
    btc_series: List[Dict[str, Any]],
    xau_series: List[Dict[str, Any]],
    wti_series: List[Dict[str, Any]],
    us10y_series: List[Dict[str, Any]],
) -> str:
    """构建外围资产分析 Prompt。"""
    env = get_jinja_env()
    template = env.get_template("external_assets.md")
    return template.render(
        usdcny_data=format_series_for_ai(usdcny_series, "人民币汇率 (USD/CNY)"),
        btc_data=format_series_for_ai(btc_series, "比特币 (BTC/USD)"),
        xau_data=format_series_for_ai(xau_series, "XAU 金价 (USD/oz)"),
        wti_data=format_series_for_ai(wti_series, "WTI 油价 (USD/bbl)"),
        us10y_data=format_series_for_ai(us10y_series, "美国 10Y 国债收益率 (%)"),
    )


def build_market_overview_prompt(market_data: Dict[str, Any], date_str: str = "") -> str:
    """构建市场总览分析 Prompt。"""
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


def format_stock_list_for_classification(stock_list: List[Dict[str, Any]]) -> str:
    """格式化股票列表，供分类分析 Prompt 使用。"""
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


def build_stock_classification_prompt(stock_list: List[Dict[str, Any]], group_name: str) -> str:
    """构建股票分类分析 Prompt。"""
    env = get_jinja_env()
    template = env.get_template("stock_classification.md")
    return template.render(
        group_name=group_name,
        stock_list=format_stock_list_for_classification(stock_list),
    )


def _format_evidence_for_ai(
    evidence_items: List[Dict[str, Any]],
    max_items: int = 18,
    summary_limit: int = 300,
) -> str:
    if not evidence_items:
        return "无可用证据"

    lines = []
    for item in evidence_items[:max_items]:
        source_label = item.get("source_label") or item.get("source") or "未知来源"
        date_str = item.get("date") or "时间未知"
        title = item.get("title") or "无标题"
        evidence_level = item.get("evidence_level") or "unknown"
        summary = " ".join(str(item.get("summary") or "").split()).strip()
        tags = ",".join(item.get("tags") or [])
        lines.append(
            "\n".join(
                [
                    f"- 来源: {source_label}",
                    f"  时间: {date_str}",
                    f"  强弱: {evidence_level}",
                    f"  标题: {title}",
                    f"  摘要: {summary[:summary_limit] if summary else '无摘要'}",
                    f"  标签: {tags or '无'}",
                ]
            )
        )
    return "\n\n".join(lines)


def build_evidence_brief_prompt(
    source_label: str,
    title: str,
    content: str,
    max_chars: int = 300,
) -> str:
    """构建单篇强证据压缩摘要 Prompt。"""
    env = get_jinja_env()
    template = env.from_string(
        """你是一名 A 股信息整理助手。

请基于给定正文，为单篇材料输出一段不超过 {{ max_chars }} 个中文字符的压缩摘要。

规则：
1. 只能根据原文内容总结，不能补充外部信息。
2. 保留与股价催化、经营变化、业绩、订单、政策、调研观点相关的信息。
3. 如果正文与上涨归因关系弱，也要如实说明“相关性较弱”。
4. 不输出项目符号，不输出标题，不输出投资建议。

来源：{{ source_label }}
标题：{{ title }}

正文：
{{ content }}
"""
    )
    return template.render(
        source_label=source_label,
        title=title,
        content=content,
        max_chars=max_chars,
    )


def build_stock_rise_attribution_prompt(
    stock_identity: Dict[str, Any],
    evidence_items: List[Dict[str, Any]],
    window_description: str = "",
) -> str:
    """构建上涨归因分析 Prompt。"""
    env = get_jinja_env()
    template = env.get_template("stock_rise_attribution.md")
    return template.render(
        stock_name=stock_identity.get("name", ""),
        stock_code=stock_identity.get("code", ""),
        window_description=window_description,
        evidence_text=_format_evidence_for_ai(evidence_items),
    )
