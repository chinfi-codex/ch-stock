#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
市场总览业务服务。
"""

import os

import akshare as ak
import pandas as pd
import streamlit as st
import tushare as ts

from infra.config import get_tushare_token
from infra.market_history_repository import load_market_history, upsert_market_history_row


def _fetch_index_kline(symbol: str, ts_code: str) -> pd.DataFrame:
    token = get_tushare_token()
    if token:
        try:
            pro = ts.pro_api(token)
            end = pd.Timestamp.now().strftime("%Y%m%d")
            start = (pd.Timestamp.now() - pd.Timedelta(days=200)).strftime("%Y%m%d")
            df = pro.index_daily(ts_code=ts_code, start_date=start, end_date=end)
            if df is not None and not df.empty:
                df = df.rename(columns={"trade_date": "date", "vol": "volume"})
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df = df.dropna(subset=["date"]).sort_values("date").tail(100)
                df.set_index("date", inplace=True)
                return df
        except Exception:
            pass

    try:
        df = ak.stock_zh_index_daily(symbol=symbol).tail(100)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    return df


@st.cache_data(ttl="1h")
def get_market_data():
    """获取市场总览数据。"""
    sh_df = _fetch_index_kline("sh000001", "000001.SH")
    cyb_df = _fetch_index_kline("sz399006", "399006.SZ")
    kcb_df = _fetch_index_kline("sh000688", "000688.SH")

    market_data = ak.stock_market_activity_legu()
    if market_data is None or market_data.empty:
        return sh_df, cyb_df, kcb_df, pd.DataFrame()

    if "统计日期" in market_data["item"].values:
        stat_date = market_data.loc[
            market_data["item"] == "统计日期", "value"
        ].values[0]
        try:
            stat_date = pd.to_datetime(stat_date).strftime("%Y/%m/%d")
        except Exception:
            stat_date = pd.Timestamp.now().strftime("%Y/%m/%d")
    else:
        stat_date = pd.Timestamp.now().strftime("%Y/%m/%d")

    row = {"日期": stat_date}
    for idx in range(0, min(11, len(market_data))):
        item = str(market_data.iloc[idx]["item"])
        value = market_data.iloc[idx]["value"]
        row[item] = value

    total_amount = 0.0
    up_count = None
    down_count = None
    token = get_tushare_token()
    if token:
        try:
            pro = ts.pro_api(token)
            trade_date = pd.to_datetime(stat_date).strftime("%Y%m%d")
            daily = pro.daily(
                trade_date=trade_date, fields="ts_code,trade_date,amount,pct_chg"
            )
            if daily is not None and not daily.empty:
                if "amount" in daily.columns:
                    total_amount = pd.to_numeric(daily["amount"], errors="coerce").sum()
                if "pct_chg" in daily.columns:
                    daily["pct_chg"] = pd.to_numeric(
                        daily["pct_chg"], errors="coerce"
                    )
                    up_count = int((daily["pct_chg"] > 0).sum())
                    down_count = int((daily["pct_chg"] < 0).sum())
        except Exception:
            total_amount = 0.0

    row["成交额"] = total_amount
    if (
        "上涨" not in row
        or pd.isna(row.get("上涨"))
        or str(row.get("上涨")).strip() == ""
    ) and up_count is not None:
        row["上涨"] = up_count
    if (
        "下跌" not in row
        or pd.isna(row.get("下跌"))
        or str(row.get("下跌")).strip() == ""
    ) and down_count is not None:
        row["下跌"] = down_count

    columns = ["日期"] + [str(market_data.iloc[i]["item"]) for i in range(0, min(11, len(market_data)))]
    if "成交额" not in columns:
        columns.append("成交额")
    if "上涨" not in columns:
        columns.append("上涨")
    if "下跌" not in columns:
        columns.append("下跌")
    upsert_market_history_row(row, columns)

    return sh_df, cyb_df, kcb_df, market_data


@st.cache_data(ttl="30m")
def get_market_history(days: int = 30) -> pd.DataFrame:
    """获取市场历史数据。"""
    return load_market_history(days)
