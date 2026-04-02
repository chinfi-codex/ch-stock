#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
电报数据 MySQL 仓储。
"""

from __future__ import annotations

import json
from typing import Any

from infra.mysql_client import (
    executemany_upsert,
    get_mysql_connection,
    init_etl_sync_run_log_table,
    normalize_mysql_json_value,
)


TELEGRAPH_COLUMNS = [
    "source",
    "source_item_id",
    "title",
    "content",
    "level",
    "importance",
    "published_at",
    "tags_json",
    "channels_json",
    "source_link",
    "raw_json",
    "dedupe_key",
]
DEFAULT_MAX_TELEGRAPH_ROWS = 100000


def init_mysql_tables() -> None:
    """初始化 telegraphs 表结构。"""
    with get_mysql_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS telegraphs (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    source VARCHAR(16) NOT NULL,
                    source_item_id VARCHAR(64) NULL,
                    title TEXT NULL,
                    content LONGTEXT NULL,
                    level VARCHAR(32) NULL,
                    importance TINYINT NULL,
                    published_at DATETIME NOT NULL,
                    tags_json JSON NULL,
                    channels_json JSON NULL,
                    source_link TEXT NULL,
                    raw_json LONGTEXT NOT NULL,
                    dedupe_key CHAR(64) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_telegraphs_dedupe_key (dedupe_key),
                    KEY idx_telegraphs_source_published_at (source, published_at),
                    KEY idx_telegraphs_published_at (published_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )

    init_etl_sync_run_log_table()


def _serialize_json_field(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(
        value,
        ensure_ascii=False,
        default=normalize_mysql_json_value,
    )


def upsert_telegraph_records(
    records: list[dict[str, Any]],
    chunk_size: int = 200,
) -> int:
    """批量 UPSERT 电报记录。"""
    serialized_records = []
    for record in records:
        serialized = dict(record)
        serialized["tags_json"] = _serialize_json_field(serialized.get("tags_json"))
        serialized["channels_json"] = _serialize_json_field(serialized.get("channels_json"))
        serialized["raw_json"] = _serialize_json_field(serialized.get("raw_json")) or "{}"
        serialized_records.append(serialized)

    return executemany_upsert(
        table_name="telegraphs",
        columns=TELEGRAPH_COLUMNS,
        update_columns=[column for column in TELEGRAPH_COLUMNS if column != "dedupe_key"],
        records=serialized_records,
        chunk_size=chunk_size,
    )


def trim_telegraph_rows(max_rows: int = DEFAULT_MAX_TELEGRAPH_ROWS) -> int:
    """删除超出上限的旧电报，只保留最新 max_rows 条。"""
    if max_rows <= 0:
        raise ValueError("max_rows 必须大于 0")

    with get_mysql_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS total_count FROM telegraphs")
            result = cursor.fetchone() or {}
            total_count = int(result.get("total_count", 0))
            overflow = total_count - max_rows
            if overflow <= 0:
                return 0

            cursor.execute(
                """
                DELETE FROM telegraphs
                WHERE id IN (
                    SELECT id FROM (
                        SELECT id
                        FROM telegraphs
                        ORDER BY published_at ASC, id ASC
                        LIMIT %s
                    ) AS stale_rows
                )
                """,
                (overflow,),
            )
            return int(cursor.rowcount)
