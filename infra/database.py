#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据库连接和基础操作模块

职责：
- 提供统一的数据库连接管理
- 数据库表结构初始化
- 基础的CRUD操作封装

设计原则：
- 使用SQLite作为默认数据库（轻量、零配置）
- 提供上下文管理器自动关闭连接
- 支持批量操作优化性能
"""

import os
import sqlite3
import logging
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)


DB_DIR = Path(__file__).parent.parent / "datas"
DB_DIR.mkdir(exist_ok=True)
DB_PATH = DB_DIR / "stock_daily_basic.db"


def get_db_path() -> Path:
    """获取数据库文件路径"""
    return DB_PATH


@contextmanager
def get_db_connection():
    """
    获取数据库连接的上下文管理器
    
    Usage:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        yield conn
        conn.commit()
    except Exception as e:
        logger.error(f"数据库操作失败: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def init_database():
    """
    初始化数据库表结构
    
    创建的表：
    - stock_daily_basic: 每日基本面数据主表
    - data_sync_log: 数据同步日志表
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_daily_basic (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    
                    -- 价格与市值
                    close REAL,
                    turnover_rate REAL,
                    turnover_rate_f REAL,
                    volume_ratio REAL,
                    
                    -- 市盈率
                    pe REAL,
                    pe_ttm REAL,
                    
                    -- 市销率
                    ps REAL,
                    ps_ttm REAL,
                    
                    -- 市净率
                    pb REAL,
                    pb_ttm REAL,
                    
                    -- 股息率
                    dv_ratio REAL,
                    dv_ttm REAL,
                    
                    -- 股本
                    total_share REAL,
                    float_share REAL,
                    
                    -- 市值（万元）
                    total_mv REAL,
                    circ_mv REAL,
                    
                    -- 时间戳
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    UNIQUE(ts_code, trade_date)
                )
            """)

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_date ON stock_daily_basic(trade_date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ts_code ON stock_daily_basic(ts_code)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ts_code_date ON stock_daily_basic(ts_code, trade_date)")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_sync_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sync_date TEXT NOT NULL,
                    sync_type TEXT NOT NULL,
                    record_count INTEGER,
                    status TEXT,
                    message TEXT,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_sync_date ON data_sync_log(sync_date)")

        logger.info("数据库初始化成功")
        return True

    except Exception as e:
        logger.error(f"数据库初始化失败: {e}")
        raise


def check_table_exists(table_name: str) -> bool:
    """
    检查表是否存在
    
    Args:
        table_name: 表名
    
    Returns:
        bool: 表是否存在
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"检查表存在失败: {e}")
        return False


def get_table_info(table_name: str) -> List[Dict[str, Any]]:
    """
    获取表的字段信息
    
    Args:
        table_name: 表名
    
    Returns:
        List[Dict]: 字段信息列表
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            return [dict(col) for col in columns]
    except Exception as e:
        logger.error(f"获取表信息失败: {e}")
        return []


def execute_sql(sql: str, params: Optional[tuple] = None) -> sqlite3.Cursor:
    """
    执行SQL语句（通用方法）
    
    Args:
        sql: SQL语句
        params: 参数元组
    
    Returns:
        sqlite3.Cursor: 游标对象
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor


def execute_many_sql(sql: str, params_list: List[tuple]) -> int:
    """
    批量执行SQL语句
    
    Args:
        sql: SQL语句
        params_list: 参数列表
    
    Returns:
        int: 影响的行数
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany(sql, params_list)
        return cursor.rowcount


def get_db_stats() -> Dict[str, Any]:
    """
    获取数据库统计信息
    
    Returns:
        Dict: 统计信息
    """
    try:
        stats = {}
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            if check_table_exists("stock_daily_basic"):
                cursor.execute("SELECT COUNT(*) FROM stock_daily_basic")
                stats["daily_basic_count"] = cursor.fetchone()[0]
                
                cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM stock_daily_basic")
                row = cursor.fetchone()
                stats["date_range"] = f"{row[0]} ~ {row[1]}" if row[0] else "无数据"
            
            if check_table_exists("data_sync_log"):
                cursor.execute("SELECT COUNT(*) FROM data_sync_log")
                stats["sync_log_count"] = cursor.fetchone()[0]
            
            stats["db_size_mb"] = DB_PATH.stat().st_size / (1024 * 1024) if DB_PATH.exists() else 0
        
        return stats
    
    except Exception as e:
        logger.error(f"获取数据库统计信息失败: {e}")
        return {}
