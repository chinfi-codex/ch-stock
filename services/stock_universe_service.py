#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票池业务服务。
"""

import logging
from datetime import date, datetime

import pandas as pd
import streamlit as st
import tushare as ts

from infra.config import get_tushare_token
from infra.data_utils import to_number
from services.daily_basic_service import get_daily_basic_smart


logger = logging.getLogger(__name__)


def _normalize_base_date(base_date) -> date | None:
    if base_date is None:
        return datetime.now().date()
    if isinstance(base_date, datetime):
        return base_date.date()
    if isinstance(base_date, str):
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(base_date, fmt).date()
            except ValueError:
                continue
        return None
    if isinstance(base_date, date):
        return base_date
    return None


def _resolve_trade_date(pro, base_date: date) -> str:
    trade_date = base_date.strftime("%Y%m%d")
    month_start = base_date.replace(day=1).strftime("%Y%m%d")

    try:
        trade_cal = pro.trade_cal(
            exchange="SSE",
            start_date=month_start,
            end_date=trade_date,
            fields="cal_date,is_open",
        )
    except Exception as exc:
        logger.warning("获取交易日历失败，直接使用输入日期 %s: %s", trade_date, exc)
        return trade_date

    if trade_cal is None or trade_cal.empty:
        return trade_date

    open_days = trade_cal.loc[trade_cal["is_open"] == 1, "cal_date"].tolist()
    if not open_days:
        return trade_date

    return str(open_days[-1])


@st.cache_data(ttl="1d")
def get_all_stocks(base_date=None):
    """获取指定日期对应最近交易日的股票列表。"""
    normalized_date = _normalize_base_date(base_date)
    if normalized_date is None:
        return pd.DataFrame()

    token = get_tushare_token()
    if not token:
        return pd.DataFrame()

    try:
        pro = ts.pro_api(token)
        trade_date = _resolve_trade_date(pro, normalized_date)
    except Exception as exc:
        logger.error("初始化 Tushare 客户端失败: %s", exc)
        return pd.DataFrame()

    daily_basic = get_daily_basic_smart(trade_date=trade_date, use_cache=True)
    try:
        daily = pro.daily(
            trade_date=trade_date, fields="ts_code,trade_date,pct_chg,amount"
        )
    except Exception as exc:
        logger.error("获取日线行情失败: trade_date=%s, error=%s", trade_date, exc)
        return pd.DataFrame()

    if daily_basic is None or daily_basic.empty or daily is None or daily.empty:
        return pd.DataFrame()

    merged = daily_basic.merge(daily, on=["ts_code", "trade_date"], how="left")

    try:
        stock_basic = pro.stock_basic(list_status="L", fields="ts_code,name")
    except Exception as exc:
        logger.warning("获取股票基础信息失败: %s", exc)
        stock_basic = pd.DataFrame()

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
