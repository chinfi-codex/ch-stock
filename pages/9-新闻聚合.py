#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
新闻聚合：财联社电报 + Alpha Vantage
"""

from datetime import datetime, timedelta
import os

import pandas as pd
import requests
import streamlit as st

from tools.crawlers import cls_telegraphs

ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"
DEFAULT_ALPHA_KEY = "X1V6MXF4TJYPBNLL"
ALPHA_TOPICS = ["technology", "economy_fiscal", "economy_monetary", "economy_macro"]


@st.cache_data(ttl=300, show_spinner=False)
def load_cls_news():
    return cls_telegraphs()


@st.cache_data(ttl=900, show_spinner=False)
def load_alpha_news(topic, api_key):
    params = {
        "function": "NEWS_SENTIMENT",
        "topics": topic,
        "apikey": api_key,
    }
    resp = requests.get(ALPHA_VANTAGE_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _get_alpha_key():
    return st.secrets.get("alphavantage_api_key") or os.environ.get("ALPHAVANTAGE_API_KEY") or DEFAULT_ALPHA_KEY


def _build_cls_datetime(df):
    if "发布日期" in df.columns and "发布时间" in df.columns:
        dt = pd.to_datetime(df["发布日期"].astype(str) + " " + df["发布时间"].astype(str), errors="coerce")
        df = df.copy()
        df["发布时间完整"] = dt
    return df


def _parse_alpha_time(value):
    if not value:
        return pd.NaT
    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M%S.%f"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return pd.to_datetime(value, errors="coerce")


def _shorten(text, limit):
    text = str(text or "").strip()
    if limit <= 0 or len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _normalize_cls(df):
    if df is None or df.empty:
        return pd.DataFrame()
    df = _build_cls_datetime(df)
    dt_col = "发布时间完整" if "发布时间完整" in df.columns else None
    dt_values = df[dt_col] if dt_col else pd.to_datetime(df["发布日期"], errors="coerce")
    view = pd.DataFrame(
        {
            "title": df["标题"].astype(str),
            "summary": df["内容"].astype(str),
            "published_at": dt_values,
            "source": "财联社",
            "topic": "cls",
            "tags": df.get("标签", ""),
            "url": "",
            "publisher": "财联社",
            "level": df.get("等级", ""),
            "sentiment": "",
        }
    )
    view["published_date"] = pd.to_datetime(view["published_at"], errors="coerce").dt.date
    return view


def _normalize_alpha_feed(feed, topic):
    rows = []
    for item in feed or []:
        dt_value = _parse_alpha_time(item.get("time_published"))
        topics = item.get("topics") or []
        topic_names = [t.get("topic") for t in topics if t.get("topic")]
        rows.append(
            {
                "title": item.get("title", ""),
                "summary": item.get("summary", ""),
                "published_at": dt_value,
                "source": "AlphaVantage",
                "topic": topic,
                "tags": ", ".join(topic_names),
                "url": item.get("url", ""),
                "publisher": item.get("source", ""),
                "level": "",
                "sentiment": item.get("overall_sentiment_label", ""),
            }
        )
    if not rows:
        return pd.DataFrame()
    view = pd.DataFrame(rows)
    view["published_date"] = pd.to_datetime(view["published_at"], errors="coerce").dt.date
    return view


def _filter_news(df, keyword, sources, date_scope, limit_rows):
    view = df.copy()

    if sources:
        view = view[view["source"].isin(sources)]

    if keyword:
        mask = view["title"].astype(str).str.contains(keyword, case=False, na=False) | view["summary"].astype(str).str.contains(
            keyword, case=False, na=False
        )
        view = view[mask]

    if date_scope != "全部":
        today = datetime.now().date()
        if date_scope == "今天":
            view = view[view["published_date"] == today]
        elif date_scope == "近3天":
            view = view[view["published_date"] >= today - timedelta(days=2)]
        elif date_scope == "近7天":
            view = view[view["published_date"] >= today - timedelta(days=6)]
        elif date_scope == "近30天":
            view = view[view["published_date"] >= today - timedelta(days=29)]

    view = view.sort_values("published_at", ascending=False, na_position="last")

    if limit_rows:
        view = view.head(limit_rows)

    return view


def main():
    st.set_page_config(page_title="新闻聚合", layout="wide")
    st.title("新闻聚合")
    st.caption("信源：财联社电报 + Alpha Vantage（technology / economy_*）")

    with st.sidebar:
        st.subheader("筛选条件")
        refresh = st.button("刷新数据")
        date_scope = st.radio("时间范围", ["全部", "今天", "近3天", "近7天", "近30天"], index=1)
        max_rows = st.slider("显示条数", min_value=20, max_value=400, value=120, step=20)
        keyword = st.text_input("关键词（标题/摘要）", value="")
        summary_len = st.slider("摘要长度", min_value=60, max_value=300, value=160, step=20)
        view_mode = st.radio("展示方式", ["卡片", "表格"], index=0)

        st.subheader("信源")
        include_cls = st.checkbox("财联社", value=True)
        include_alpha = st.checkbox("Alpha Vantage", value=True)
        selected_topics = []
        if include_alpha:
            selected_topics = st.multiselect(
                "AlphaVantage Topics",
                options=ALPHA_TOPICS,
                default=ALPHA_TOPICS,
            )

    if refresh:
        load_cls_news.clear()
        load_alpha_news.clear()

    data_frames = []
    errors = []

    if include_cls:
        with st.spinner("拉取财联社电报..."):
            try:
                cls_df = load_cls_news()
            except Exception as exc:
                cls_df = None
                errors.append(f"财联社电报获取失败: {exc}")
        data_frames.append(_normalize_cls(cls_df))

    if include_alpha and selected_topics:
        api_key = _get_alpha_key()
        for topic in selected_topics:
            with st.spinner(f"拉取 Alpha Vantage: {topic}"):
                try:
                    payload = load_alpha_news(topic, api_key)
                except Exception as exc:
                    payload = {}
                    errors.append(f"Alpha Vantage {topic} 获取失败: {exc}")
            if isinstance(payload, dict):
                info_msg = payload.get("Error Message") or payload.get("Information") or payload.get("Note")
                if info_msg:
                    errors.append(f"Alpha Vantage {topic} 提示: {info_msg}")
                feed = payload.get("feed", [])
                data_frames.append(_normalize_alpha_feed(feed, topic))

    if errors:
        for msg in errors:
            st.warning(msg)

    if not data_frames:
        st.info("当前没有可用的数据源")
        return

    news_df = pd.concat([df for df in data_frames if df is not None and not df.empty], ignore_index=True)
    if news_df.empty:
        st.info("暂无可展示的新闻")
        return

    sources = []
    if include_cls:
        sources.append("财联社")
    if include_alpha:
        sources.append("AlphaVantage")

    view = _filter_news(news_df, keyword, sources, date_scope, max_rows)
    if view.empty:
        st.info("筛选条件下没有匹配的新闻")
        return

    view = view.copy()
    view["summary_short"] = view["summary"].apply(lambda x: _shorten(x, summary_len))

    st.markdown(f"共展示 {len(view)} 条新闻")

    if view_mode == "表格":
        table = view[
            [
                "published_at",
                "title",
                "summary_short",
                "source",
                "topic",
                "publisher",
                "sentiment",
                "url",
            ]
        ].copy()
        table["published_at"] = pd.to_datetime(table["published_at"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        st.dataframe(table, use_container_width=True, hide_index=True)
        return

    for _, row in view.iterrows():
        title = str(row.get("title", "")).strip()
        summary = str(row.get("summary_short", "")).strip()
        source = str(row.get("source", "")).strip()
        topic = str(row.get("topic", "")).strip()
        publisher = str(row.get("publisher", "")).strip()
        sentiment = str(row.get("sentiment", "")).strip()
        url = str(row.get("url", "")).strip()

        dt_value = row.get("published_at")
        dt_text = ""
        if pd.notna(dt_value):
            dt_text = pd.to_datetime(dt_value, errors="coerce").strftime("%Y-%m-%d %H:%M:%S")

        meta_parts = [p for p in [dt_text, source, topic, publisher, sentiment] if p]
        meta = " | ".join(meta_parts)

        with st.container():
            header_left, header_right = st.columns([3, 1])
            if url:
                header_left.markdown(f"**[{title}]({url})**")
            else:
                header_left.markdown(f"**{title}**")
            header_right.caption(meta)
            if summary:
                st.write(summary)
            st.divider()


if __name__ == "__main__":
    main()
