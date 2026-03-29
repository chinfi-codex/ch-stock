#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票池业务服务。
"""

from datetime import date, datetime

import pandas as pd
import streamlit as st
import tushare as ts

from infra.config import get_tushare_token
from infra.data_utils import to_number
from services.daily_basic_service import get_daily_basic_smart


@st.cache_data(ttl="1d")
def get_all_stocks(base_date=None):
    """获取指定交易日的股票列表。"""
    if base_date is None:
        base_date = datetime.now().date()
    if isinstance(base_date, datetime):
        base_date = base_date.date()
    elif isinstance(base_date, str):
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                base_date = datetime.strptime(base_date, fmt).date()
                break
            except ValueError:
                continue

    if not isinstance(base_date, date):
        return pd.DataFrame()
    trade_date = base_date.strftime("%Y%m%d")
    token = get_tushare_token()
    if not token or not trade_date:
        return pd.DataFrame()

    pro = ts.pro_api(token)
    daily_basic = get_daily_basic_smart(trade_date=trade_date, use_cache=True)
    daily = pro.daily(trade_date=trade_date, fields="ts_code,trade_date,pct_chg,amount")
    if daily_basic is None or daily_basic.empty or daily is None or daily.empty:
        return pd.DataFrame()

    merged = daily_basic.merge(daily, on=["ts_code", "trade_date"], how="left")
    stock_basic = pro.stock_basic(list_status="L", fields="ts_code,name")
    if stock_basic is not None and not stock_basic.empty:
        merged = merged.merge(stock_basic, on="ts_code", how="left")
    merged["code"] = merged["ts_code"].str.split(".").str[0]
    merged = merged.rename(
        columns={"pct_chg": "pct", "amount": "amount", "total_mv": "mkt_cap"}
    )
    merged["pct"] = to_number(merged["pct"])
    merged["amount"] = to_number(merged["amount"])
    merged["mkt_cap"] = to_number(merged["mkt_cap"])
    merged["amount"] = merged["amount"] / 100000
    merged["mkt_cap"] = merged["mkt_cap"] / 10000
    merged["name"] = merged.get("name", "").fillna("")
    merged = merged.dropna(subset=["code", "pct", "amount", "mkt_cap"])
    return merged[["code", "name", "pct", "amount", "mkt_cap"]]
