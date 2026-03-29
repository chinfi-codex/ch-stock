#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
daily_basic 业务服务。
"""

import logging
from typing import List, Optional

import pandas as pd
import tushare as ts

from infra.config import get_tushare_token
from infra.daily_basic_repository import query_daily_basic, save_daily_basic_sync
from infra.database import get_db_connection


logger = logging.getLogger(__name__)


def get_daily_basic_smart(
    trade_date: str, fields: Optional[List[str]] = None, use_cache: bool = True
) -> pd.DataFrame:
    """优先本地查询，缺失时自动回源 Tushare。"""
    query_fields = list(fields) if fields else None

    if use_cache:
        local_data = query_daily_basic(trade_date=trade_date, fields=query_fields)
        if not local_data.empty:
            return local_data

    token = get_tushare_token()
    if not token:
        logger.warning("未找到 Tushare Token")
        return pd.DataFrame()

    try:
        pro = ts.pro_api(token)
        df = pro.daily_basic(trade_date=trade_date)
        if df is None or df.empty:
            return pd.DataFrame()

        save_daily_basic_sync(df)

        if query_fields:
            required_fields = ["ts_code", "trade_date"]
            final_fields = list(dict.fromkeys(query_fields + required_fields))
            return df[final_fields]
        return df
    except Exception as e:
        logger.error(f"从 Tushare 获取 daily_basic 失败: {trade_date}, error: {e}")
        return pd.DataFrame()


def get_missing_dates(start_date: str, end_date: str) -> List[str]:
    """获取缺失的交易日。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT trade_date FROM stock_daily_basic WHERE trade_date BETWEEN ? AND ?",
                (start_date, end_date),
            )
            existing_dates = {row[0] for row in cursor.fetchall()}

        token = get_tushare_token()
        if not token:
            return []

        pro = ts.pro_api(token)
        trade_cal = pro.trade_cal(
            exchange="SSE", start_date=start_date, end_date=end_date
        )
        if trade_cal is None or trade_cal.empty:
            return []

        all_dates = trade_cal[trade_cal["is_open"] == 1]["cal_date"].tolist()
        return [d for d in all_dates if d not in existing_dates]
    except Exception as e:
        logger.error(f"获取缺失日期失败: {e}")
        return []
