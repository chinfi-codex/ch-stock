#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""上涨归因验证页。"""

from __future__ import annotations

import streamlit as st

from services.stock_rise_attribution_service import (
    DEFAULT_SELECTED_SOURCES,
    SOURCE_CNINFO,
    SOURCE_LABELS,
    SOURCE_P5W,
    SOURCE_REPORT_EARNINGS,
    SOURCE_RESEARCH,
    SOURCE_SEARCH,
    SOURCE_ZSXQ,
    get_stock_rise_attribution,
    search_stock_candidates,
)


st.set_page_config(page_title="上涨归因", layout="wide")
st.title("上涨归因验证页")


def _render_ai_summary(ai_summary: dict) -> None:
    st.subheader("上涨归因分析")
    status = ai_summary.get("status")
    content = ai_summary.get("content") or ""
    if status == "failed":
        st.warning(content or "上涨归因分析暂不可用")
        return
    if status == "empty":
        st.info(content or "暂无可分析内容")
        return
    st.markdown(content)


def _render_evidence_items(evidence_items: list[dict]) -> None:
    with st.expander("获取到的所有信息列表", expanded=False):
        if not evidence_items:
            st.info("暂无获取到的信息。")
            return

        for item in evidence_items:
            source_label = item.get("source_label") or item.get("source") or "未知来源"
            event_date = item.get("date") or "时间未知"
            title = item.get("title") or "无标题"
            summary = (item.get("summary") or "").strip()
            url = (item.get("url") or "").strip()

            st.markdown(f"- `{source_label}` | `{event_date}` | {title}")
            if summary:
                st.caption(summary)
            if url:
                st.markdown(f"[查看原链接]({url})")


default_sources = set(DEFAULT_SELECTED_SOURCES)
query = st.text_input("股票代码或名称", placeholder="例如：000001 或 平安银行")

selected_sources = []
source_cols = st.columns(6)
with source_cols[0]:
    if st.checkbox(SOURCE_LABELS[SOURCE_CNINFO], value=SOURCE_CNINFO in default_sources):
        selected_sources.append(SOURCE_CNINFO)
with source_cols[1]:
    if st.checkbox(
        SOURCE_LABELS[SOURCE_RESEARCH],
        value=SOURCE_RESEARCH in default_sources,
    ):
        selected_sources.append(SOURCE_RESEARCH)
with source_cols[2]:
    if st.checkbox(SOURCE_LABELS[SOURCE_P5W], value=SOURCE_P5W in default_sources):
        selected_sources.append(SOURCE_P5W)
with source_cols[3]:
    if st.checkbox(SOURCE_LABELS[SOURCE_SEARCH], value=SOURCE_SEARCH in default_sources):
        selected_sources.append(SOURCE_SEARCH)
with source_cols[4]:
    if st.checkbox(SOURCE_LABELS[SOURCE_ZSXQ], value=SOURCE_ZSXQ in default_sources):
        selected_sources.append(SOURCE_ZSXQ)
with source_cols[5]:
    if st.checkbox(
        SOURCE_LABELS[SOURCE_REPORT_EARNINGS],
        value=SOURCE_REPORT_EARNINGS in default_sources,
    ):
        selected_sources.append(SOURCE_REPORT_EARNINGS)

candidates = search_stock_candidates(query, limit=10) if query.strip() else []
selected_stock = None
if query.strip():
    if not candidates:
        st.warning("未找到匹配股票")
    else:
        labels = [f"{item['code']} - {item['name']}" for item in candidates]
        selected_label = st.selectbox("选择股票", labels, index=0)
        selected_item = candidates[labels.index(selected_label)]
        selected_stock = {
            "code": selected_item["code"],
            "name": selected_item["name"],
            "org_id": selected_item.get("orgId", ""),
        }

analyze_disabled = not selected_stock or not selected_sources
if analyze_disabled and query.strip() and not selected_sources:
    st.info("请至少勾选一个数据来源。")

if st.button("开始分析", type="primary", disabled=analyze_disabled):
    with st.spinner("正在汇总多源证据并生成上涨归因分析..."):
        st.session_state["stock_rise_attribution_result"] = get_stock_rise_attribution(
            stock_identity=selected_stock,
            selected_sources=selected_sources,
        )

result = st.session_state.get("stock_rise_attribution_result")
if result:
    identity = result["stock_identity"]
    st.markdown(f"## {identity['name']}（{identity['code']}）")
    _render_ai_summary(result["ai_summary"])
    _render_evidence_items(result.get("evidence_items") or [])
