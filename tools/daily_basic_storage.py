#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
daily_basic 数据存储模块

职责：
- daily_basic 数据的CRUD操作
- 优先本地查询，缺失时自动调用API
- 异步保存机制（后台线程，不阻塞业务）

设计原则：
- 透明替换现有调用，保持API兼容
- 智能降级：本地失败时自动回退到API
- 异步写入：业务不等待，后台处理
"""

import os
import logging
import threading
import queue
import time
from typing import Optional, List, Tuple
import pandas as pd
import tushare as ts

from infra.database import get_db_connection, execute_sql, execute_many_sql, get_db_path
from infra.config import get_tushare_token

logger = logging.getLogger(__name__)


_save_queue: queue.Queue = queue.Queue(maxsize=1000)
_save_worker: Optional[threading.Thread] = None
_worker_running = False


def _background_save_worker():
    """后台保存线程工作函数"""
    global _worker_running
    _worker_running = True
    
    logger.info("后台保存线程已启动")
    
    while _worker_running:
        try:
            task = _save_queue.get(timeout=1)
            
            if task is None:
                break
            
            operation, data = task
            
            if operation == "save_df":
                try:
                    df = data
                    if not df.empty:
                        save_daily_basic_sync(df)
                except Exception as e:
                    logger.error(f"后台保存失败: {e}")
            
            elif operation == "save_list":
                try:
                    records = data
                    if records:
                        save_daily_basic_many(records)
                except Exception as e:
                    logger.error(f"后台批量保存失败: {e}")
            
            _save_queue.task_done()
            
        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"后台保存线程异常: {e}")
            time.sleep(1)
    
    logger.info("后台保存线程已停止")


def _ensure_worker_running():
    """确保后台保存线程已启动"""
    global _save_worker, _worker_running
    
    if _save_worker is None or not _save_worker.is_alive():
        _save_worker = threading.Thread(
            target=_background_save_worker,
            name="DailyBasicSaveWorker",
            daemon=True
        )
        _save_worker.start()
        logger.info("启动后台保存线程")


def shutdown_worker():
    """关闭后台保存线程"""
    global _worker_running
    
    _worker_running = False
    _save_queue.put(None)
    
    if _save_worker and _save_worker.is_alive():
        _save_worker.join(timeout=5)
    
    logger.info("后台保存线程已关闭")


def save_daily_basic_sync(df: pd.DataFrame) -> int:
    """
    同步保存daily_basic数据到数据库
    
    Args:
        df: 包含daily_basic字段的DataFrame
    
    Returns:
        int: 保存的记录数
    """
    if df is None or df.empty:
        return 0
    
    required_columns = ["ts_code", "trade_date"]
    missing_cols = [col for col in required_columns if col not in df.columns]
    
    if missing_cols:
        logger.error(f"缺少必要字段: {missing_cols}")
        raise ValueError(f"DataFrame缺少必要字段: {missing_cols}")
    
    try:
        records = []
        
        for _, row in df.iterrows():
            record = {
                "ts_code": str(row.get("ts_code", "")),
                "trade_date": str(row.get("trade_date", "")),
                "close": float(row.get("close", 0)) if pd.notna(row.get("close")) else None,
                "turnover_rate": float(row) if pd.notna(row.get("turnover_rate")) else None,
                "turnover_rate_f": float(row) if pd.notna(row.get("turnover_rate_f")) else None,
                "volume_ratio": float(row) if pd.notna(row.get("volume_ratio")) else None,
                "pe": float(row) if pd.notna(row.get("pe")) else None,
                "pe_ttm": float(row) if pd.notna(row.get("pe_ttm")) else None,
                "ps": float(row) if pd.notna(row.get("ps")) else None,
                "ps_ttm": float(row) if pd.notna(row.get("ps_ttm")) else None,
                "pb": float(row) if pd.notna(row.get("pb")) else None,
                "pb_ttm": float(row) if pd.notna(row.get("pb_ttm")) else None,
                "dv_ratio": float(row) if pd.notna(row.get("dv_ratio")) else None,
                "dv_ttm": float(row) if pd.notna(row.get("dv_ttm")) else None,
                "total_share": float(row) if pd.notna(row.get("total_share")) else None,
                "float_share": float(row) if pd.notna(row.get("float_share")) else None,
                "total_mv": float(row) if pd.notna(row.get("total_mv")) else None,
                "circ_mv": float(row) if pd.notna(row.get("circ_mv")) else None,
            }
            records.append(tuple(record.values()))
        
        if not records:
            return 0
        
        columns = list(records[0].keys())
        placeholders = ",".join(["?"] * len(columns))
        sql = f"""
            INSERT OR REPLACE INTO stock_daily_basic ({",".join(columns)})
            VALUES ({placeholders})
        """
        
        execute_many_sql(sql, records)
        
        logger.info(f"保存daily_basic数据: {len(records)} 条")
        return len(records)
    
    except Exception as e:
        logger.error(f"保存daily_basic数据失败: {e}")
        raise


def save_daily_basic_many(records: List[dict]) -> int:
    """
    批量保存记录（优化性能）
    
    Args:
        records: 记录字典列表
    
    Returns:
        int: 保存的记录数
    """
    if not records:
        return 0
    
    try:
        row_records = []
        columns = [
            "ts_code", "trade_date", "close", "turnover_rate", 
            "turnover_rate_f", "volume_ratio", "pe", "pe_ttm",
            "ps", "ps_ttm", "pb", "pb_ttm", "dv_ratio", "dv_ttm",
            "total_share", "float_share", "total_mv", "circ_mv"
        ]
        
        for record in records:
            row = tuple(record.get(col) for col in columns)
            row_records.append(row)
        
        placeholders = ",".join(["?"] * len(columns))
        sql = f"""
            INSERT OR REPLACE INTO stock_daily_basic ({",".join(columns)})
            VALUES ({placeholders})
        """
        
        execute_many_sql(sql, row_records)
        
        logger.info(f"批量保存daily_basic数据: {len(records)} 条")
        return len(records)
    
    except Exception as e:
        logger.error(f"批量保存失败: {e}")
        raise


def save_daily_basic_async(df: pd.DataFrame):
    """
    异步保存daily_basic数据（不阻塞业务）
    
    Args:
        df: 要保存的DataFrame
    """
    if df is None or df.empty:
        return
    
    _ensure_worker_running()
    
    try:
        _save_queue.put(("save_df", df.copy()), timeout=5)
        logger.debug(f"异步保存任务已加入队列: {len(df)} 条")
    except queue.Full:
        logger.warning("保存队列已满，丢弃任务")


def query_daily_basic(
    trade_date: Optional[str] = None,
    ts_code: Optional[str] = None,
    fields: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    查询daily_basic数据
    
    Args:
        trade_date: 交易日期（YYYYMMDD）
        ts_code: 股票代码
        fields: 返回字段列表，None返回全部字段
    
    Returns:
        pd.DataFrame: 查询结果
    """
    try:
        where_clauses = []
        params = []
        
        if trade_date:
            where_clauses.append("trade_date = ?")
            params.append(trade_date)
        
        if ts_code:
            where_clauses.append("ts_code = ?")
            params.append(ts_code)
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        if fields:
            fields_sql = ",".join(fields)
        else:
            fields_sql = "*"
        
        sql = f"SELECT {fields_sql} FROM stock_daily_basic WHERE {where_sql}"
        
        cursor = execute_sql(sql, tuple(params) if params else None)
        rows = cursor.fetchall()
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame([dict(row) for row in rows])
        
        return df
    
    except Exception as e:
        logger.error(f"查询daily_basic数据失败: {e}")
        return pd.DataFrame()


def get_daily_basic_smart(
    trade_date: str,
    fields: Optional[List[str]] = None,
    use_cache: bool = True
) -> pd.DataFrame:
    """
    智能获取daily_basic数据（优先本地，缺失自动获取）
    
    Args:
        trade_date: 交易日期
        fields: 返回字段列表
        use_cache: 是否优先使用本地缓存
    
    Returns:
        pd.DataFrame: 数据结果
    """
    if use_cache:
        local_data = query_daily_basic(trade_date=trade_date, fields=fields)
        
        if not local_data.empty:
            logger.debug(f"从本地库获取数据: {trade_date}, {len(local_data)} 条")
            return local_data
    
    logger.info(f"本地库无数据，从Tushare API获取: {trade_date}")
    
    try:
        token = get_tushare_token()
        if not token:
            logger.warning("未找到Tushare Token")
            return pd.DataFrame()
        
        pro = ts.pro_api(token)
        
        df = pro.daily_basic(trade_date=trade_date)
        
        if df is None or df.empty:
            logger.warning(f"Tushare API返回空数据: {trade_date}")
            return pd.DataFrame()
        
        if fields and "ts_code" not in fields:
            fields.append("ts_code")
        if fields and "trade_date" not in fields:
            fields.append("trade_date")
        
        result_df = df[fields] if fields else df
        
        save_daily_basic_async(result_df)
        
        return result_df
    
    except Exception as e:
        logger.error(f"从Tushare获取数据失败: {trade_date}, error: {e}")
        return pd.DataFrame()


def get_missing_dates(start_date: str, end_date: str) -> List[str]:
    """
    获取缺失的交易日期
    
    Args:
        start_date: 开始日期（YYYYMMDD）
        end_date: 结束日期（YYYYMMDD）
    
    Returns:
        List[str]: 缺失的日期列表
    """
    try:
        cursor = execute_sql(
            "SELECT DISTINCT trade_date FROM stock_daily_basic WHERE trade_date BETWEEN ? AND ?",
            (start_date, end_date)
        )
        existing_dates = {row[0] for row in cursor.fetchall()}
        
        token = get_tushare_token()
        if not token:
            return []
        
        pro = ts.pro_api(token)
        trade_cal = pro.trade_cal(
            exchange="SSE",
            start_date=start_date,
            end_date=end_date
        )
        
        if trade_cal is None or trade_cal.empty:
            return []
        
        all_dates = trade_cal[trade_cal["is_open"] == 1]["cal_date"].tolist()
        
        missing_dates = [d for d in all_dates if d not in existing_dates]
        
        logger.info(f"缺失日期: {len(missing_dates)}/{len(all_dates)}")
        return missing_dates
    
    except Exception as e:
        logger.error(f"获取缺失日期失败: {e}")
        return []


def check_data_existence(trade_date: str, ts_code: Optional[str] = None) -> bool:
    """
    检查数据是否存在
    
    Args:
        trade_date: 交易日期
        ts_code: 股票代码（可选）
    
    Returns:
        bool: 数据是否存在
    """
    try:
        if ts_code:
            cursor = execute_sql(
                "SELECT COUNT(*) FROM stock_daily_basic WHERE trade_date = ? AND ts_code = ?",
                (trade_date, ts_code)
            )
        else:
            cursor = execute_sql(
                "SELECT COUNT(*) FROM stock_daily_basic WHERE trade_date = ?",
                (trade_date,)
            )
        
        count = cursor.fetchone()[0]
        return count > 0
    
    except Exception as e:
        logger.error(f"检查数据存在失败: {e}")
        return False


def get_last_sync_date() -> Optional[str]:
    """
    获取最后同步的日期
    
    Returns:
        Optional[str]: 最后同步日期
    """
    try:
        cursor = execute_sql(
            "SELECT MAX(trade_date) FROM stock_daily_basic"
        )
        row = cursor.fetchone()
        return row[0] if row and row[0] else None
    
    except Exception as e:
        logger.error(f"获取最后同步日期失败: {e}")
        return None


def get_database_path() -> str:
    """获取数据库文件路径"""
    return str(get_db_path())
