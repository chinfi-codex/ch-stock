#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
daily_basic 数据同步服务

职责：
- 历史数据补录
- 每日增量同步
- 同步状态管理

设计原则：
- 增量同步：避免重复下载
- 错误容错：单日失败不影响整体
- 进度反馈：实时显示同步进度
"""

import os
import logging
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import pandas as pd
import tushare as ts
import streamlit as st

from infra.database import get_db_connection, execute_sql, init_database
from infra.config import get_tushare_token
from tools.daily_basic_storage import save_daily_basic_sync, get_missing_dates, get_last_sync_date

logger = logging.getLogger(__name__)


def sync_historical_data(
    start_date: str,
    end_date: Optional[str] = None,
    show_progress: bool = False
) -> Dict[str, any]:
    """
    同步历史数据
    
    Args:
        start_date: 开始日期（YYYYMMDD）
        end_date: 结束日期（YYYYMMDD），None表示今天
        show_progress: 是否显示进度条
    
    Returns:
        Dict: 同步结果统计
    """
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    
    token = get_tushare_token()
    if not token:
        raise ValueError("未找到Tushare Token，请配置环境变量或secrets")
    
    init_database()
    
    pro = ts.pro_api(token)
    
    missing_dates = get_missing_dates(start_date, end_date)
    
    if not missing_dates:
        logger.info(f"历史数据已完整: {start_date} ~ {end_date}")
        return {
            "status": "complete",
            "total": 0,
            "success": 0,
            "failed": 0,
            "message": "历史数据已完整"
        }
    
    total_dates = len(missing_dates)
    logger.info(f"开始补录历史数据: {total_dates} 个交易日 ({start_date} ~ {end_date})")
    
    success_count = 0
    failed_count = 0
    failed_dates = []
    
    if show_progress:
        progress_bar = st.progress(0)
        status_text = st.empty()
    
    for idx, trade_date in enumerate(missing_dates):
        try:
            if show_progress:
                status_text.text(f"正在同步: {trade_date} ({idx+1}/{total_dates})")
            
            df = pro.daily_basic(trade_date=trade_date)
            
            if df is not None and not df.empty:
                save_daily_basic_sync(df)
                success_count += 1
                logger.debug(f"成功: {trade_date}, {len(df)} 条")
            else:
                failed_count += 1
                failed_dates.append(trade_date)
                logger.warning(f"无数据: {trade_date}")
            
            time.sleep(0.5)
            
        except Exception as e:
            failed_count += 1
            failed_dates.append(trade_date)
            logger.error(f"同步失败: {trade_date}, error: {e}")
        
        if show_progress:
            progress = (idx + 1) / total_dates
            progress_bar.progress(progress)
    
    result = {
        "status": "completed" if failed_count == 0 else "partial_failed",
        "total": total_dates,
        "success": success_count,
        "failed": failed_count,
        "failed_dates": failed_dates,
        "message": f"完成: 成功{success_count}, 失败{failed_count}"
    }
    
    logger.info(f"历史数据补录完成: {result['message']}")
    
    return result


def sync_single_date(trade_date: str) -> bool:
    """
    同步单个交易日的数据
    
    Args:
        trade_date: 交易日期（YYYYMMDD）
    
    Returns:
        bool: 是否成功
    """
    try:
        token = get_tushare_token()
        if not token:
            logger.error("未找到Tushare Token")
            return False
        
        init_database()
        
        pro = ts.pro_api(token)
        
        df = pro.daily_basic(trade_date=trade_date)
        
        if df is None or df.empty:
            logger.warning(f"无数据: {trade_date}")
            return False
        
        save_daily_basic_sync(df)
        logger.info(f"同步成功: {trade_date}, {len(df)} 条")
        return True
    
    except Exception as e:
        logger.error(f"同步失败: {trade_date}, error: {e}")
        return False


def sync_recent_days(days: int = 7, show_progress: bool = False) -> Dict[str, any]:
    """
    同步最近N天的数据
    
    Args:
        days: 天数
        show_progress: 是否显示进度
    
    Returns:
        Dict: 同步结果
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days * 2)
    
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    
    return sync_historical_data(start_str, end_str, show_progress)


def get_sync_status() -> Dict[str, any]:
    """
    获取同步状态
    
    Returns:
        Dict: 同步状态信息
    """
    try:
        last_date = get_last_sync_date()
        
        cursor = execute_sql("SELECT COUNT(*) FROM stock_daily_basic")
        total_count = cursor.fetchone()[0]
        
        cursor = execute_sql("SELECT COUNT(DISTINCT ts_code) FROM stock_daily_basic")
        stock_count = cursor.fetchone()[0]
        
        cursor = execute_sql("SELECT trade_date FROM stock_daily_basic ORDER BY trade_date DESC LIMIT 1")
        row = cursor.fetchone()
        latest_date = row[0] if row else None
        
        return {
            "last_sync_date": last_date,
            "latest_date": latest_date,
            "total_records": total_count,
            "unique_stocks": stock_count,
            "database_exists": True,
            "message": f"共{total_count}条记录，{stock_count}只股票"
        }
    
    except Exception as e:
        logger.error(f"获取同步状态失败: {e}")
        return {
            "database_exists": False,
            "message": f"获取状态失败: {e}"
        }


def daily_sync_job():
    show_progress = True
    result = sync_recent_days(days=7, show_progress=show_progress)
    
    if result["failed_count"] > 0:
        logger.warning(f"同步部分失败: {result['failed_dates']}")
    else:
        logger.info("同步完成")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("用法: python services/daily_basic_sync.py [command]")
        print("命令:")
        print("  sync_all [start_date] [end_date]  - 补录历史数据")
        print("  sync_recent [days]              - 同步最近N天")
        print("  sync_single [date]              - 同步单日")
        print("  status                          - 查看同步状态")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "sync_all":
        start_date = sys.argv[2] if len(sys.argv) > 2 else "20240101"
        end_date = sys.argv[3] if len(sys.argv) > 3 else None
        result = sync_historical_data(start_date, end_date, show_progress=True)
        print(f"结果: {result}")
    
    elif command == "sync_recent":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
        result = sync_recent_days(days, show_progress=True)
        print(f"结果: {result}")
    
    elif command == "sync_single":
        date_str = sys.argv[2]
        success = sync_single_date(date_str)
        print(f"结果: {'成功' if success else '失败'}")
    
    elif command == "status":
        status = get_sync_status()
        for k, v in status.items():
            print(f"{k}: {v}")
    
    else:
        print(f"未知命令: {command}")
