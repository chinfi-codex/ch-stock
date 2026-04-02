#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
电报同步业务服务。
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from infra import mysql_client, mysql_telegraph_repository
from tools.crawlers import fetch_cls_telegraph_records, fetch_jin10_flash_records


logger = logging.getLogger(__name__)

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
TELEGRAPH_SYNC_JOB_NAME = "scheduled_telegraph_sync"
TELEGRAPH_TARGET = "telegraphs"
SUPPORTED_SOURCES = {"all", "cls", "jin10"}
MAX_TELEGRAPH_ROWS = 100000


def _now_shanghai() -> datetime:
    return datetime.now(SHANGHAI_TZ)


def _normalize_published_at(value: object) -> datetime:
    if value is None:
        raise ValueError("发布时间不能为空")

    if isinstance(value, (int, float)):
        parsed = pd.to_datetime(value, unit="s", utc=True, errors="coerce")
    else:
        parsed = pd.to_datetime(value, utc=True, errors="coerce")

    if pd.isna(parsed):
        raise ValueError(f"无法解析发布时间: {value}")

    return parsed.tz_convert(SHANGHAI_TZ).to_pydatetime()


def _is_within_hours(published_at: datetime, hours: int) -> bool:
    if hours <= 0:
        return True
    cutoff = _now_shanghai() - timedelta(hours=hours)
    return published_at >= cutoff


def _format_published_at_for_mysql(value: datetime) -> str:
    return value.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _ensure_list(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    return []


def _build_dedupe_key(*parts: object) -> str:
    raw = "||".join("" if part is None else str(part).strip() for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _normalize_cls_records(records: list[dict[str, object]], hours: int) -> list[dict[str, object]]:
    normalized_records: list[dict[str, object]] = []
    for record in records:
        published_at = _normalize_published_at(record.get("ctime"))
        if not _is_within_hours(published_at, hours):
            continue

        tags = [
            subject.get("subject_name", "")
            for subject in _ensure_list(record.get("subjects"))
            if isinstance(subject, dict) and subject.get("subject_name")
        ]
        title = record.get("title")
        content = record.get("content")
        dedupe_key = _build_dedupe_key(
            "cls",
            _format_published_at_for_mysql(published_at),
            title,
            content,
        )
        normalized_records.append(
            {
                "source": "cls",
                "source_item_id": record.get("id"),
                "title": title,
                "content": content,
                "level": None if record.get("level") is None else str(record.get("level")),
                "importance": None,
                "published_at": _format_published_at_for_mysql(published_at),
                "tags_json": tags,
                "channels_json": [],
                "source_link": "https://www.cls.cn/telegraph",
                "raw_json": record,
                "dedupe_key": dedupe_key,
            }
        )

    return normalized_records


def _normalize_jin10_records(records: list[dict[str, object]], hours: int) -> list[dict[str, object]]:
    normalized_records: list[dict[str, object]] = []
    for record in records:
        published_at = _normalize_published_at(record.get("time"))
        if not _is_within_hours(published_at, hours):
            continue

        source_item_id = record.get("id")
        title = record.get("title")
        content = record.get("content")
        dedupe_key = _build_dedupe_key(
            "jin10",
            source_item_id or _format_published_at_for_mysql(published_at),
            None if source_item_id else title,
            None if source_item_id else content,
        )
        importance = record.get("important")
        try:
            importance = None if importance is None else int(importance)
        except (TypeError, ValueError):
            importance = None

        normalized_records.append(
            {
                "source": "jin10",
                "source_item_id": None if source_item_id is None else str(source_item_id),
                "title": title,
                "content": content,
                "level": None if record.get("type") is None else str(record.get("type")),
                "importance": importance,
                "published_at": _format_published_at_for_mysql(published_at),
                "tags_json": _ensure_list(record.get("tags")),
                "channels_json": _ensure_list(record.get("channel")),
                "source_link": record.get("source_link"),
                "raw_json": record.get("raw") or record,
                "dedupe_key": dedupe_key,
            }
        )

    return normalized_records


def _log_sync_result(
    *,
    source: str,
    status: str,
    row_count: int,
    started_at: datetime,
    hours: int,
    rn: int,
    timeout: int,
    error_message: str | None = None,
) -> None:
    mysql_client.record_sync_run(
        job_name=TELEGRAPH_SYNC_JOB_NAME,
        mode="scheduled",
        target_table=TELEGRAPH_TARGET,
        trade_date=_now_shanghai().strftime("%Y-%m-%d"),
        source_range=json.dumps(
            {
                "source": source,
                "hours": hours,
                "rn": rn,
                "timeout": timeout,
            },
            ensure_ascii=False,
        ),
        row_count=row_count,
        status=status,
        error_message=error_message,
        started_at=started_at.replace(tzinfo=None),
        completed_at=_now_shanghai().replace(tzinfo=None),
    )


def run_scheduled_telegraph_sync(
    source: str = "all",
    hours: int = 6,
    rn: int = 2000,
    timeout: int = 20,
) -> dict[str, object]:
    """抓取并同步 CLS/Jin10 电报到 MySQL。"""
    if source not in SUPPORTED_SOURCES:
        raise ValueError(f"不支持的 source: {source}")

    mysql_telegraph_repository.init_mysql_tables()
    target_sources = ["cls", "jin10"] if source == "all" else [source]
    results: dict[str, object] = {
        "mode": "scheduled",
        "status": "success",
        "source": source,
        "results": {},
    }

    for current_source in target_sources:
        started_at = _now_shanghai()
        try:
            if current_source == "cls":
                raw_records = fetch_cls_telegraph_records(rn=rn, timeout=timeout)
                normalized_records = _normalize_cls_records(raw_records, hours)
            else:
                raw_records = fetch_jin10_flash_records(timeout=timeout)
                normalized_records = _normalize_jin10_records(raw_records, hours)

            row_count = mysql_telegraph_repository.upsert_telegraph_records(normalized_records)
            trimmed_count = mysql_telegraph_repository.trim_telegraph_rows(MAX_TELEGRAPH_ROWS)
            _log_sync_result(
                source=current_source,
                status="success",
                row_count=row_count,
                started_at=started_at,
                hours=hours,
                rn=rn,
                timeout=timeout,
            )
            results["results"][current_source] = {
                "status": "success",
                "row_count": row_count,
                "trimmed_count": trimmed_count,
            }
        except Exception as exc:
            logger.exception("电报同步失败: source=%s", current_source)
            results["status"] = "partial_failed"
            _log_sync_result(
                source=current_source,
                status="failed",
                row_count=0,
                started_at=started_at,
                hours=hours,
                rn=rn,
                timeout=timeout,
                error_message=str(exc),
            )
            results["results"][current_source] = {
                "status": "failed",
                "error": str(exc),
            }

    return results
