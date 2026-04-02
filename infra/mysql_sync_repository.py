#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MySQL 同步仓储。
"""

from __future__ import annotations

import json
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


MARKET_SNAPSHOT_COLUMNS = [
    "trade_date",
    "up_count",
    "limit_up_count",
    "real_limit_up_count",
    "st_limit_up_count",
    "down_count",
    "limit_down_count",
    "real_limit_down_count",
    "st_limit_down_count",
    "flat_count",
    "suspended_count",
    "activity_rate",
    "turnover_amount",
    "financing_net_buy",
    "source_row_json",
]

DAILY_BASIC_COLUMNS = [
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


def _import_pymysql():
    import pymysql

    return pymysql


def _validate_mysql_config() -> None:
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
    _validate_mysql_config()
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


def init_mysql_tables() -> None:
    """初始化 MySQL 表结构。"""
    with get_mysql_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS market_daily_snapshot (
                    trade_date DATE NOT NULL PRIMARY KEY,
                    up_count INT NULL,
                    limit_up_count INT NULL,
                    real_limit_up_count INT NULL,
                    st_limit_up_count INT NULL,
                    down_count INT NULL,
                    limit_down_count INT NULL,
                    real_limit_down_count INT NULL,
                    st_limit_down_count INT NULL,
                    flat_count INT NULL,
                    suspended_count INT NULL,
                    activity_rate DECIMAL(10, 4) NULL,
                    turnover_amount DECIMAL(20, 4) NULL,
                    financing_net_buy DECIMAL(20, 4) NULL,
                    source_row_json LONGTEXT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_daily_basic (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(16) NOT NULL,
                    trade_date DATE NOT NULL,
                    close DECIMAL(20, 4) NULL,
                    turnover_rate DECIMAL(20, 6) NULL,
                    turnover_rate_f DECIMAL(20, 6) NULL,
                    volume_ratio DECIMAL(20, 6) NULL,
                    pe DECIMAL(20, 6) NULL,
                    pe_ttm DECIMAL(20, 6) NULL,
                    ps DECIMAL(20, 6) NULL,
                    ps_ttm DECIMAL(20, 6) NULL,
                    pb DECIMAL(20, 6) NULL,
                    pb_ttm DECIMAL(20, 6) NULL,
                    dv_ratio DECIMAL(20, 6) NULL,
                    dv_ttm DECIMAL(20, 6) NULL,
                    total_share DECIMAL(24, 6) NULL,
                    float_share DECIMAL(24, 6) NULL,
                    total_mv DECIMAL(24, 6) NULL,
                    circ_mv DECIMAL(24, 6) NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_stock_daily_basic_trade_date_ts_code (trade_date, ts_code),
                    KEY idx_stock_daily_basic_ts_code (ts_code),
                    KEY idx_stock_daily_basic_trade_date (trade_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
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


def _chunk_records(records: list[dict[str, Any]], chunk_size: int) -> Iterator[list[dict[str, Any]]]:
    for idx in range(0, len(records), chunk_size):
        yield records[idx : idx + chunk_size]


def _normalize_json_value(value: Any) -> Any:
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


def _executemany_upsert(
    table_name: str,
    columns: list[str],
    update_columns: Iterable[str],
    records: list[dict[str, Any]],
    chunk_size: int = 500,
) -> int:
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
            for chunk in _chunk_records(records, chunk_size):
                values = [tuple(record.get(column) for column in columns) for record in chunk]
                cursor.executemany(sql, values)

    return len(records)


def upsert_market_daily_snapshots(
    records: list[dict[str, Any]],
    chunk_size: int = 200,
) -> int:
    """批量 UPSERT 市场快照。"""
    serialized_records = []
    for record in records:
        serialized = dict(record)
        if serialized.get("source_row_json") is not None and not isinstance(
            serialized["source_row_json"], str
        ):
            serialized["source_row_json"] = json.dumps(
                {
                    key: _normalize_json_value(value)
                    for key, value in serialized["source_row_json"].items()
                },
                ensure_ascii=False,
                default=_normalize_json_value,
            )
        serialized_records.append(serialized)

    return _executemany_upsert(
        table_name="market_daily_snapshot",
        columns=MARKET_SNAPSHOT_COLUMNS,
        update_columns=[column for column in MARKET_SNAPSHOT_COLUMNS if column != "trade_date"],
        records=serialized_records,
        chunk_size=chunk_size,
    )


def upsert_stock_daily_basic_records(
    records: list[dict[str, Any]],
    chunk_size: int = 500,
) -> int:
    """批量 UPSERT daily_basic。"""
    return _executemany_upsert(
        table_name="stock_daily_basic",
        columns=DAILY_BASIC_COLUMNS,
        update_columns=[
            column for column in DAILY_BASIC_COLUMNS if column not in {"ts_code", "trade_date"}
        ],
        records=records,
        chunk_size=chunk_size,
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
    """写入同步日志。"""
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
