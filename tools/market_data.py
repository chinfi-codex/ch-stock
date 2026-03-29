#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
市场数据原子能力。
"""

import pandas as pd
import streamlit as st
import akshare as ak
import tushare as ts

from infra.config import get_tushare_token


@st.cache_data(ttl="1h")
def get_financing_net_buy_series(days: int = 60) -> pd.DataFrame:
    """按日期汇总近 N 个交易日融资净买入。"""
    token = get_tushare_token()
    if not token:
        return pd.DataFrame()
    pro = ts.pro_api(token)
    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=days * 2)
    try:
        df = pro.margin(
            start_date=start.strftime("%Y%m%d"), end_date=end.strftime("%Y%m%d")
        )
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty or "trade_date" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    for col in ["rzmre", "rzche"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if {"rzmre", "rzche"}.issubset(df.columns):
        grouped = df.groupby("trade_date")[["rzmre", "rzche"]].sum()
        grouped["net_buy"] = grouped["rzmre"] - grouped["rzche"]
    elif "rzmre" in df.columns:
        grouped = df.groupby("trade_date")[["rzmre"]].sum()
        grouped["net_buy"] = grouped["rzmre"]
    else:
        return pd.DataFrame()

    grouped = grouped.reset_index().dropna(subset=["trade_date"]).sort_values("trade_date")
    grouped = grouped.tail(days).rename(
        columns={"trade_date": "date", "net_buy": "融资净买入"}
    )
    return grouped[["date", "融资净买入"]]


@st.cache_data(ttl="12h")
def get_gem_pe_series(days: int = 500) -> pd.DataFrame:
    """获取创业板市盈率序列。"""
    token = get_tushare_token()
    if not token:
        return pd.DataFrame()
    pro = ts.pro_api(token)
    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=days * 4)
    try:
        df = pro.daily_info(
            ts_code="SZ_GEM",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty or "trade_date" not in df.columns or "pe" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["pe"] = pd.to_numeric(df["pe"], errors="coerce")
    df = df.dropna(subset=["trade_date", "pe"]).sort_values("trade_date").tail(days)
    return df.rename(columns={"trade_date": "date", "pe": "市盈率"})[["date", "市盈率"]]


@st.cache_data(ttl="1d")
def get_dfcf_concept_boards():
    """获取东方财富概念板块列表。"""
    return ak.stock_board_concept_name_em()


@st.cache_data(ttl="0.5d")
def get_concept_board_index(concept_name, count=181):
    """获取概念板块指数。"""
    df = ak.stock_board_concept_hist_em(symbol=concept_name)
    if len(df) > count:
        df = df.tail(count)
    else:
        df = df.tail(len(df))
    df.columns = [
        "date",
        "open",
        "close",
        "high",
        "low",
        "rate_pct",
        "rate",
        "volume_",
        "volume",
        "wide",
        "change",
    ]
    df = df[["date", "open", "high", "low", "close", "volume"]]
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    return df


@st.cache_data(ttl="1h")
def get_market_daily_stats(days: int = 30) -> pd.DataFrame:
    """获取市场每日统计数据。"""
    token = get_tushare_token()
    if not token:
        return pd.DataFrame()
    pro = ts.pro_api(token)
    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=days * 2)
    try:
        df = pro.daily_info(
            start_date=start.strftime("%Y%m%d"), end_date=end.strftime("%Y%m%d")
        )
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty or "trade_date" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df.dropna(subset=["trade_date"])

    result = pd.DataFrame()
    result["日期"] = df["trade_date"]
    if "total_mv" in df.columns:
        result["成交额"] = pd.to_numeric(df["total_mv"], errors="coerce")
    elif "turnover" in df.columns:
        result["成交额"] = pd.to_numeric(df["turnover"], errors="coerce")
    else:
        result["成交额"] = None

    result["上涨"] = pd.to_numeric(df["up_num"], errors="coerce") if "up_num" in df.columns else None
    result["下跌"] = pd.to_numeric(df["down_num"], errors="coerce") if "down_num" in df.columns else None
    result["涨停"] = None
    result["跌停"] = None

    try:
        limit_df = pro.limit_list(
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            limit_type="U",
        )
        if limit_df is not None and not limit_df.empty:
            limit_df["trade_date"] = pd.to_datetime(limit_df["trade_date"], errors="coerce")
            zt_counts = limit_df.groupby("trade_date").size().reset_index(name="涨停")
            result = result.merge(zt_counts, left_on="日期", right_on="trade_date", how="left")
            result = result.drop(columns=["trade_date"], errors="ignore")
    except Exception:
        pass

    try:
        limit_df = pro.limit_list(
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            limit_type="D",
        )
        if limit_df is not None and not limit_df.empty:
            limit_df["trade_date"] = pd.to_datetime(limit_df["trade_date"], errors="coerce")
            dt_counts = limit_df.groupby("trade_date").size().reset_index(name="跌停")
            result = result.merge(dt_counts, left_on="日期", right_on="trade_date", how="left")
            result = result.drop(columns=["trade_date"], errors="ignore")
    except Exception:
        pass

    result["活跃度"] = None
    mask = result["上涨"].notna() & result["下跌"].notna()
    if mask.any():
        total = result.loc[mask, "上涨"] + result.loc[mask, "下跌"]
        result.loc[mask, "活跃度"] = (result.loc[mask, "上涨"] / total * 100).round(2)
    return result.sort_values("日期").tail(days).reset_index(drop=True)


@st.cache_data(ttl="1h")
def get_market_amount_series(days: int = 30) -> pd.DataFrame:
    """获取市场成交额序列。"""
    token = get_tushare_token()
    if not token:
        return pd.DataFrame()
    pro = ts.pro_api(token)
    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=days * 2)
    try:
        df = pro.daily(
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            fields="trade_date,amount",
        )
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty or "trade_date" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["trade_date", "amount"])
    daily_amount = df.groupby("trade_date")["amount"].sum().reset_index()
    daily_amount.columns = ["日期", "成交额"]
    return daily_amount.sort_values("日期").tail(days).reset_index(drop=True)
