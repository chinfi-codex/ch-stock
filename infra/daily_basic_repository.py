#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
daily_basic 数据仓储。
"""

import logging
import queue
import threading
import time
from typing import List, Optional

import pandas as pd

from infra.database import execute_many_sql, get_db_connection, get_db_path


logger = logging.getLogger(__name__)

ALLOWED_COLUMNS = {
    "ts_code",
    "trade_date",
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "ps",
    "ps_ttm",
    "pb",
    "pb_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "total_mv",
    "circ_mv",
}


def _safe_float(val):
    if pd.isna(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


_save_queue: queue.Queue = queue.Queue(maxsize=1000)
_save_worker: Optional[threading.Thread] = None
_worker_running = False
_worker_lock = threading.Lock()


def _background_save_worker():
    global _worker_running
    _worker_running = True
    while _worker_running:
        try:
            task = _save_queue.get(timeout=1)
            if task is None:
                break
            operation, data = task
            if operation == "save_df" and data is not None and not data.empty:
                save_daily_basic_sync(data)
            elif operation == "save_list" and data:
                save_daily_basic_many(data)
            _save_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"后台保存线程异常: {e}")
            time.sleep(1)


def _ensure_worker_running():
    global _save_worker
    with _worker_lock:
        if _save_worker is None or not _save_worker.is_alive():
            _save_worker = threading.Thread(
                target=_background_save_worker,
                name="DailyBasicSaveWorker",
                daemon=True,
            )
            _save_worker.start()


def shutdown_worker():
    global _worker_running
    _worker_running = False
    _save_queue.put(None)
    if _save_worker and _save_worker.is_alive():
        _save_worker.join(timeout=5)


def save_daily_basic_sync(df: pd.DataFrame) -> int:
    """同步保存 daily_basic。"""
    if df is None or df.empty:
        return 0

    required_columns = ["ts_code", "trade_date"]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"DataFrame 缺少必要字段: {missing_cols}")

    available_columns = [col for col in df.columns if col in ALLOWED_COLUMNS]
    if not {"ts_code", "trade_date"}.issubset(set(available_columns)):
        raise ValueError("DataFrame 必须包含 ts_code 和 trade_date 字段")

    records = []
    for _, row in df.iterrows():
        record_values = []
        for col in available_columns:
            if col in ("ts_code", "trade_date"):
                record_values.append(str(row.get(col, "")))
            else:
                record_values.append(_safe_float(row.get(col)))
        records.append(tuple(record_values))

    if not records:
        return 0

    placeholders = ",".join(["?"] * len(available_columns))
    sql = f"""
        INSERT OR REPLACE INTO stock_daily_basic ({",".join(available_columns)})
        VALUES ({placeholders})
    """
    execute_many_sql(sql, records)
    return len(records)


def save_daily_basic_many(records: List[dict]) -> int:
    """批量保存 daily_basic。"""
    if not records:
        return 0

    columns = [
        "ts_code",
        "trade_date",
        "close",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "ps",
        "ps_ttm",
        "pb",
        "pb_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "total_mv",
        "circ_mv",
    ]
    row_records = [tuple(record.get(col) for col in columns) for record in records]
    placeholders = ",".join(["?"] * len(columns))
    sql = f"""
        INSERT OR REPLACE INTO stock_daily_basic ({",".join(columns)})
        VALUES ({placeholders})
    """
    execute_many_sql(sql, row_records)
    return len(records)


def save_daily_basic_async(df: pd.DataFrame):
    """异步保存 daily_basic。"""
    if df is None or df.empty:
        return
    _ensure_worker_running()
    try:
        _save_queue.put(("save_df", df.copy()), timeout=5)
    except queue.Full:
        logger.warning("保存队列已满，丢弃任务")


def query_daily_basic(
    trade_date: Optional[str] = None,
    ts_code: Optional[str] = None,
    fields: Optional[List[str]] = None,
) -> pd.DataFrame:
    """查询 daily_basic。"""
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
        fields_sql = ",".join(fields) if fields else "*"
        sql = f"SELECT {fields_sql} FROM stock_daily_basic WHERE {where_sql}"

        with get_db_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, tuple(params))
            else:
                cursor.execute(sql)
            rows = cursor.fetchall()

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(row) for row in rows])
    except Exception as e:
        logger.error(f"查询 daily_basic 失败: {e}")
        return pd.DataFrame()


def check_data_existence(trade_date: str, ts_code: Optional[str] = None) -> bool:
    """检查数据是否存在。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            if ts_code:
                cursor.execute(
                    "SELECT COUNT(*) FROM stock_daily_basic WHERE trade_date = ? AND ts_code = ?",
                    (trade_date, ts_code),
                )
            else:
                cursor.execute(
                    "SELECT COUNT(*) FROM stock_daily_basic WHERE trade_date = ?",
                    (trade_date,),
                )
            count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        logger.error(f"检查数据存在失败: {e}")
        return False


def get_last_sync_date() -> Optional[str]:
    """获取最后同步日期。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(trade_date) FROM stock_daily_basic")
            row = cursor.fetchone()
        return row[0] if row and row[0] else None
    except Exception as e:
        logger.error(f"获取最后同步日期失败: {e}")
        return None


def get_database_path() -> str:
    """获取数据库路径。"""
    return str(get_db_path())
