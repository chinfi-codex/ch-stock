"""
工具函数模块
包含股票业务相关的工具函数和辅助方法
"""

import re
import logging
from typing import Any, Optional, Union

import pandas as pd
import streamlit as st
import requests
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)


# =============================================================================
# 股票业务数据处理工具函数
# =============================================================================


def latest_metric_from_df(
    df: pd.DataFrame, value_col: str, date_col: str = "date"
) -> Optional[dict]:
    """从 DataFrame 中获取最新和前一行的指标值

    Args:
        df: DataFrame
        value_col: 数值列名
        date_col: 日期列名，默认为 "date"

    Returns:
        dict: 包含 date, value, prev_value 的字典，如果数据为空则返回 None
    """
    if df is None or df.empty or value_col not in df.columns:
        return None
    view = df.copy()
    if date_col in view.columns:
        view[date_col] = pd.to_datetime(view[date_col], errors="coerce")
    view[value_col] = pd.to_numeric(view[value_col], errors="coerce")
    view = view.dropna(subset=[value_col])
    if date_col in view.columns:
        view = view.dropna(subset=[date_col]).sort_values(date_col, ascending=False)
    if view.empty:
        return None
    latest = view.iloc[0]
    prev_value = None
    if len(view) > 1:
        prev_value = view.iloc[1][value_col]
    return {
        "date": latest[date_col] if date_col in view.columns else None,
        "value": float(latest[value_col]),
        "prev_value": float(prev_value)
        if prev_value is not None and not pd.isna(prev_value)
        else None,
    }


def calc_pct_change(current: float, previous: float) -> Optional[float]:
    """计算百分比变化

    Args:
        current: 当前值
        previous: 前一值

    Returns:
        float: 百分比变化，如果无法计算则返回 None
    """
    if current is None or previous is None or previous == 0:
        return None
    return (current / previous - 1) * 100


def series_from_df(df: pd.DataFrame, value_col: str, days: int) -> list:
    """从 DataFrame 提取时间序列数据

    Args:
        df: DataFrame
        value_col: 数值列名
        days: 返回的天数

    Returns:
        list: 包含 date 和 value 的字典列表
    """
    if df is None or df.empty or value_col not in df.columns:
        return []
    view = df.copy()
    if "date" in view.columns:
        view["date"] = pd.to_datetime(view["date"], errors="coerce")
    view[value_col] = pd.to_numeric(view[value_col], errors="coerce")
    view = view.dropna(subset=[value_col])
    if "date" in view.columns:
        view = view.dropna(subset=["date"]).sort_values("date")
    if view.empty:
        return []
    view = view.tail(int(days))
    if "date" in view.columns:
        view["date"] = view["date"].dt.strftime("%Y-%m-%d")
    view = view[["date", value_col]].rename(columns={value_col: "value"})
    return view.to_dict(orient="records")


def filter_st_bj_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """过滤掉 ST 和北交所股票

    Args:
        df: 包含 code 和 name 列的 DataFrame

    Returns:
        pd.DataFrame: 过滤后的 DataFrame
    """
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
    """获取股票列表"""
    url = "http://www.cninfo.com.cn/new/data/szse_stock.json"
    resp = requests.get(url).json()["stockList"]
    df = pd.DataFrame(resp)
    return df


def get_xueqiu_stock_topics(stock_code, cookie, page_id=3):
    """获取雪球股票话题"""
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
        "Connection": "keep-alive",
        "Cookie": cookie,  # 仍为变量
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

    if stock_code.startswith("6"):
        stock_code = "SH" + stock_code
    else:
        stock_code = "SZ" + stock_code

    topic_texts = []
    for i in range(1, page_id):
        url = f"https://xueqiu.com/query/v1/symbol/search/status.json?count=10&comment=0&symbol={stock_code}&hl=0&source=all&sort=time&page={i}&q=&type=82"
        try:
            resp = requests.get(url=url, headers=headers, timeout=8)
            resp.raise_for_status()
            data = resp.json()
            topics = data.get("list", [])
        except Exception as e:
            # 可根据需要打印或记录异常
            break
        if not topics:
            break
        for topic in topics:
            if "text" in topic:
                txt = topic.get("text")
                # 去除<img ...>标签
                txt = re.sub(r"<img.*?>", "", txt, flags=re.DOTALL)
                # 去除<a ...>...</a>标签及其内容
                txt = re.sub(r"<a.*?>.*?</a>", "", txt, flags=re.DOTALL)
                topic_texts.append(txt)
    return topic_texts


def weibo_comments(wid):
    """获取微博评论"""
    url = f"https://weibo.com/ajax/statuses/show?id={wid}"
    header = {"user-agent": UserAgent().random}
    res = requests.get(url=url, headers=header)
    json_data = res.json()
    id = json_data["id"]
    user_id = json_data["user"]["idstr"]

    # 获取评论
    comments = []
    max_id = ""
    while max_id != 0:
        pl_url = f"https://weibo.com/ajax/statuses/buildComments?is_reload=1&id={id}&is_show_bulletin=2&is_mix=0&max_id={max_id}&count=10&uid={user_id}"
        resp = requests.get(url=pl_url, headers=header)
        json_data = resp.json()
        max_id = json_data["max_id"]
        lis = json_data["data"]
        for li in lis:
            text_raw = li["text_raw"]
            comments.append(text_raw)
    return comments
