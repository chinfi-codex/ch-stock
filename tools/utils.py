#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票业务工具函数。
"""

import logging
import re

import pandas as pd
import requests
import streamlit as st
from fake_useragent import UserAgent


logger = logging.getLogger(__name__)


def filter_st_bj_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """过滤 ST 和北交所股票。"""
    if df is None or df.empty:
        return df
    if {"code", "name"}.issubset(df.columns):
        view = df.copy()
        code_str = view["code"].astype(str).str.lower().str.strip()
        is_bj = code_str.str.startswith("bj") | code_str.str.startswith(("4", "8"))
        is_st = view["name"].astype(str).str.upper().str.contains("ST", na=False)
        return view[~is_bj & ~is_st]
    return df


@st.cache_data(ttl="15day")
def get_stock_list():
    """获取股票列表。"""
    url = "http://www.cninfo.com.cn/new/data/szse_stock.json"
    resp = requests.get(url).json()["stockList"]
    return pd.DataFrame(resp)


def get_xueqiu_stock_topics(stock_code, cookie, page_id=3):
    """获取雪球股票话题。"""
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
        "Connection": "keep-alive",
        "Cookie": cookie,
        "Host": "xueqiu.com",
        "Referer": "https://xueqiu.com/S/SH688372",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "elastic-apm-traceparent": "00-db43983b4c5505f4d4a674fd89b785f0-574b28f413051bda-00",
        "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    stock_code = ("SH" if stock_code.startswith("6") else "SZ") + stock_code
    topic_texts = []
    for i in range(1, page_id):
        url = (
            "https://xueqiu.com/query/v1/symbol/search/status.json?"
            f"count=10&comment=0&symbol={stock_code}&hl=0&source=all&sort=time&page={i}&q=&type=82"
        )
        try:
            resp = requests.get(url=url, headers=headers, timeout=8)
            resp.raise_for_status()
            topics = resp.json().get("list", [])
        except Exception:
            break
        if not topics:
            break
        for topic in topics:
            if "text" in topic:
                txt = topic.get("text")
                txt = re.sub(r"<img.*?>", "", txt, flags=re.DOTALL)
                txt = re.sub(r"<a.*?>.*?</a>", "", txt, flags=re.DOTALL)
                topic_texts.append(txt)
    return topic_texts


def weibo_comments(wid):
    """获取微博评论。"""
    url = f"https://weibo.com/ajax/statuses/show?id={wid}"
    header = {"user-agent": UserAgent().random}
    res = requests.get(url=url, headers=header)
    json_data = res.json()
    post_id = json_data["id"]
    user_id = json_data["user"]["idstr"]

    comments = []
    max_id = ""
    while max_id != 0:
        pl_url = (
            "https://weibo.com/ajax/statuses/buildComments?"
            f"is_reload=1&id={post_id}&is_show_bulletin=2&is_mix=0&max_id={max_id}&count=10&uid={user_id}"
        )
        resp = requests.get(url=pl_url, headers=header)
        json_data = resp.json()
        max_id = json_data["max_id"]
        for li in json_data["data"]:
            comments.append(li["text_raw"])
    return comments
