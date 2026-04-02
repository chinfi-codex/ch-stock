#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
通用 MySQL 客户端与基础写入工具。
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Iterable, Iterator

from infra.config import (
    get_mysql_database,
    get_mysql_host,
    get_mysql_password,
    get_mysql_port,
    get_mysql_user,
)


def _import_pymysql():
    import pymysql

    return pymysql


def validate_mysql_config() -> None:
    """校验 MySQL 连接配置。"""
    missing = []
    if not get_mysql_database():
        missing.append("MYSQL_DATABASE")
    if not get_mysql_user():
        missing.append("MYSQL_USER")
    if not get_mysql_password():
        missing.append("MYSQL_PASSWORD")

    if missing:
        raise ValueError(f"MySQL 配置缺失: {', '.join(missing)}")


@contextmanager
def get_mysql_connection():
    """获取 MySQL 连接。"""
    validate_mysql_config()
    pymysql = _import_pymysql()

    connection = pymysql.connect(
        host=get_mysql_host(),
        port=get_mysql_port(),
        user=get_mysql_user(),
        password=get_mysql_password(),
        database=get_mysql_database(),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def chunk_records(records: list[dict[str, Any]], chunk_size: int) -> Iterator[list[dict[str, Any]]]:
    """按块切分记录。"""
    for idx in range(0, len(records), chunk_size):
        yield records[idx : idx + chunk_size]


def normalize_mysql_json_value(value: Any) -> Any:
    """将 numpy/pandas/datetime 等值转换为 JSON 可序列化的基础类型。"""
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    try:
        if value != value:
            return None
    except Exception:
        pass
    return value


def executemany_upsert(
    *,
    table_name: str,
    columns: list[str],
    update_columns: Iterable[str],
    records: list[dict[str, Any]],
    chunk_size: int = 500,
) -> int:
    """批量 UPSERT 通用实现。"""
    if not records:
        return 0

    placeholders = ", ".join(["%s"] * len(columns))
    update_sql = ", ".join([f"{column}=VALUES({column})" for column in update_columns])
    sql = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_sql}
    """

    with get_mysql_connection() as conn:
        with conn.cursor() as cursor:
            for chunk in chunk_records(records, chunk_size):
                values = [tuple(record.get(column) for column in columns) for record in chunk]
                cursor.executemany(sql, values)

    return len(records)


def init_etl_sync_run_log_table() -> None:
    """初始化通用 ETL 日志表。"""
    with get_mysql_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS etl_sync_run_log (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    job_name VARCHAR(64) NOT NULL,
                    mode VARCHAR(32) NOT NULL,
                    target_table VARCHAR(64) NOT NULL,
                    trade_date DATE NULL,
                    source_range VARCHAR(128) NULL,
                    row_count INT NOT NULL DEFAULT 0,
                    status VARCHAR(32) NOT NULL,
                    error_message TEXT NULL,
                    started_at DATETIME NULL,
                    completed_at DATETIME NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    KEY idx_etl_sync_run_log_trade_date (trade_date),
                    KEY idx_etl_sync_run_log_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )


def record_sync_run(
    *,
    job_name: str,
    mode: str,
    target_table: str,
    status: str,
    row_count: int = 0,
    trade_date: str | None = None,
    source_range: str | None = None,
    error_message: str | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> None:
    """写入 ETL 运行日志。"""
    with get_mysql_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO etl_sync_run_log (
                    job_name,
                    mode,
                    target_table,
                    trade_date,
                    source_range,
                    row_count,
                    status,
                    error_message,
                    started_at,
                    completed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    job_name,
                    mode,
                    target_table,
                    trade_date,
                    source_range,
                    row_count,
                    status,
                    error_message,
                    started_at,
                    completed_at,
                ),
            )
