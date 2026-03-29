#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
daily_basic 同步服务。
"""

import logging
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, Optional

import streamlit as st
import tushare as ts

from infra.config import get_tushare_token
from infra.daily_basic_repository import get_last_sync_date, save_daily_basic_sync
from infra.database import get_db_connection, init_database
from services.daily_basic_service import get_missing_dates


logger = logging.getLogger(__name__)


def sync_historical_data(
    start_date: str, end_date: Optional[str] = None, show_progress: bool = False
) -> Dict[str, any]:
    """同步历史 daily_basic 数据。"""
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")

    token = get_tushare_token()
    if not token:
        raise ValueError("未找到 Tushare Token，请先配置")

    init_database()
    pro = ts.pro_api(token)
    missing_dates = get_missing_dates(start_date, end_date)
    if not missing_dates:
        return {
            "status": "complete",
            "total": 0,
            "success": 0,
            "failed": 0,
            "failed_dates": [],
            "message": "历史数据已完整",
        }

    success_count = 0
    failed_count = 0
    failed_dates = []

    if show_progress:
        progress_bar = st.progress(0)
        status_text = st.empty()

    for idx, trade_date in enumerate(missing_dates):
        try:
            if show_progress:
                status_text.text(f"正在同步: {trade_date} ({idx + 1}/{len(missing_dates)})")
            df = pro.daily_basic(trade_date=trade_date)
            if df is not None and not df.empty:
                save_daily_basic_sync(df)
                success_count += 1
            else:
                failed_count += 1
                failed_dates.append(trade_date)
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"同步失败: {trade_date}, error: {e}")
            failed_count += 1
            failed_dates.append(trade_date)

        if show_progress:
            progress_bar.progress((idx + 1) / len(missing_dates))

    return {
        "status": "completed" if failed_count == 0 else "partial_failed",
        "total": len(missing_dates),
        "success": success_count,
        "failed": failed_count,
        "failed_dates": failed_dates,
        "message": f"完成: 成功{success_count}, 失败{failed_count}",
    }


def sync_single_date(trade_date: str) -> bool:
    """同步单个交易日数据。"""
    try:
        token = get_tushare_token()
        if not token:
            return False
        init_database()
        pro = ts.pro_api(token)
        df = pro.daily_basic(trade_date=trade_date)
        if df is None or df.empty:
            return False
        save_daily_basic_sync(df)
        return True
    except Exception as e:
        logger.error(f"同步失败: {trade_date}, error: {e}")
        return False


def sync_recent_days(days: int = 7, show_progress: bool = False) -> Dict[str, any]:
    """同步最近 N 天数据。"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days * 2)
    return sync_historical_data(
        start_date.strftime("%Y%m%d"),
        end_date.strftime("%Y%m%d"),
        show_progress=show_progress,
    )


def get_sync_status() -> Dict[str, any]:
    """获取同步状态。"""
    try:
        last_date = get_last_sync_date()
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM stock_daily_basic")
            total_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM stock_daily_basic")
            stock_count = cursor.fetchone()[0]
            cursor.execute(
                "SELECT trade_date FROM stock_daily_basic ORDER BY trade_date DESC LIMIT 1"
            )
            row = cursor.fetchone()
        latest_date = row[0] if row else None
        return {
            "last_sync_date": last_date,
            "latest_date": latest_date,
            "total_records": total_count,
            "unique_stocks": stock_count,
            "database_exists": True,
            "message": f"共 {total_count} 条记录，{stock_count} 只股票",
        }
    except Exception as e:
        logger.error(f"获取同步状态失败: {e}")
        return {"database_exists": False, "message": f"获取状态失败: {e}"}


def daily_sync_job():
    result = sync_recent_days(days=7, show_progress=True)
    if result["failed"] > 0:
        logger.warning(f"同步部分失败: {result['failed_dates']}")
    else:
        logger.info("同步完成")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python services/daily_basic_sync.py [command]")
        sys.exit(1)

    command = sys.argv[1]
    if command == "sync_all":
        start_date = sys.argv[2] if len(sys.argv) > 2 else "20240101"
        end_date = sys.argv[3] if len(sys.argv) > 3 else None
        print(sync_historical_data(start_date, end_date, show_progress=True))
    elif command == "sync_recent":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
        print(sync_recent_days(days, show_progress=True))
    elif command == "sync_single":
        print(sync_single_date(sys.argv[2]))
    elif command == "status":
        print(get_sync_status())
