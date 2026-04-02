#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from datetime import datetime

import pandas as pd

from services import telegraph_sync_service


def _to_utc_seconds(local_time: str) -> int:
    return int(
        pd.Timestamp(local_time, tz="Asia/Shanghai").tz_convert("UTC").timestamp()
    )


def test_normalize_jin10_records_same_item_has_same_dedupe_key(monkeypatch):
    fixed_now = datetime(2026, 4, 2, 12, 0, 0, tzinfo=telegraph_sync_service.SHANGHAI_TZ)
    monkeypatch.setattr(telegraph_sync_service, "_now_shanghai", lambda: fixed_now)

    source_item = {
        "id": 1001,
        "time": "2026-04-02T01:30:00.000Z",
        "type": "flash",
        "important": 3,
        "channel": ["market"],
        "tags": ["美股"],
        "title": "美股异动",
        "content": "三大指数盘中分化。",
        "source_link": "https://example.com/jin10/1001",
        "raw": {"id": 1001},
    }

    normalized = telegraph_sync_service._normalize_jin10_records(
        [source_item, dict(source_item)],
        hours=6,
    )

    assert len(normalized) == 2
    assert normalized[0]["dedupe_key"] == normalized[1]["dedupe_key"]


def test_normalize_cls_records_same_payload_has_same_dedupe_key(monkeypatch):
    fixed_now = datetime(2026, 4, 2, 12, 0, 0, tzinfo=telegraph_sync_service.SHANGHAI_TZ)
    monkeypatch.setattr(telegraph_sync_service, "_now_shanghai", lambda: fixed_now)

    source_item = {
        "title": "盘中异动",
        "content": "板块快速拉升。",
        "level": 1,
        "subjects": [{"subject_name": "机器人"}],
        "ctime": _to_utc_seconds("2026-04-02 10:00:00"),
    }

    normalized = telegraph_sync_service._normalize_cls_records(
        [source_item, dict(source_item)],
        hours=6,
    )

    assert len(normalized) == 2
    assert normalized[0]["dedupe_key"] == normalized[1]["dedupe_key"]


def test_run_scheduled_telegraph_sync_filters_hours_and_limits_source(monkeypatch):
    fixed_now = datetime(2026, 4, 2, 12, 0, 0, tzinfo=telegraph_sync_service.SHANGHAI_TZ)
    monkeypatch.setattr(telegraph_sync_service, "_now_shanghai", lambda: fixed_now)
    monkeypatch.setattr(
        telegraph_sync_service.mysql_telegraph_repository,
        "init_mysql_tables",
        lambda: None,
    )

    captured_upserts = []
    captured_logs = []

    monkeypatch.setattr(
        telegraph_sync_service.mysql_telegraph_repository,
        "upsert_telegraph_records",
        lambda records: captured_upserts.append(records) or len(records),
    )
    monkeypatch.setattr(
        telegraph_sync_service.mysql_telegraph_repository,
        "trim_telegraph_rows",
        lambda max_rows: 0,
    )
    monkeypatch.setattr(
        telegraph_sync_service.mysql_client,
        "record_sync_run",
        lambda **kwargs: captured_logs.append(kwargs),
    )
    monkeypatch.setattr(
        telegraph_sync_service,
        "fetch_cls_telegraph_records",
        lambda rn, timeout: [
            {
                "title": "近 6 小时内",
                "content": "保留",
                "level": 1,
                "subjects": [{"subject_name": "算力"}],
                "ctime": _to_utc_seconds("2026-04-02 09:30:00"),
            },
            {
                "title": "超过 6 小时",
                "content": "过滤",
                "level": 1,
                "subjects": [{"subject_name": "地产"}],
                "ctime": _to_utc_seconds("2026-04-02 03:30:00"),
            },
        ],
    )
    monkeypatch.setattr(
        telegraph_sync_service,
        "fetch_jin10_flash_records",
        lambda timeout: (_ for _ in ()).throw(AssertionError("jin10 should not run")),
    )

    result = telegraph_sync_service.run_scheduled_telegraph_sync(
        source="cls",
        hours=6,
        rn=2000,
        timeout=20,
    )

    assert result["status"] == "success"
    assert set(result["results"].keys()) == {"cls"}
    assert len(captured_upserts) == 1
    assert len(captured_upserts[0]) == 1
    assert captured_upserts[0][0]["title"] == "近 6 小时内"
    assert captured_logs[0]["status"] == "success"


def test_run_scheduled_telegraph_sync_all_sources_calls_both(monkeypatch):
    fixed_now = datetime(2026, 4, 2, 12, 0, 0, tzinfo=telegraph_sync_service.SHANGHAI_TZ)
    monkeypatch.setattr(telegraph_sync_service, "_now_shanghai", lambda: fixed_now)
    monkeypatch.setattr(
        telegraph_sync_service.mysql_telegraph_repository,
        "init_mysql_tables",
        lambda: None,
    )
    monkeypatch.setattr(
        telegraph_sync_service.mysql_client,
        "record_sync_run",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        telegraph_sync_service.mysql_telegraph_repository,
        "trim_telegraph_rows",
        lambda max_rows: 0,
    )

    calls = {"cls": 0, "jin10": 0}

    monkeypatch.setattr(
        telegraph_sync_service,
        "fetch_cls_telegraph_records",
        lambda rn, timeout: calls.__setitem__("cls", calls["cls"] + 1) or [],
    )
    monkeypatch.setattr(
        telegraph_sync_service,
        "fetch_jin10_flash_records",
        lambda timeout: calls.__setitem__("jin10", calls["jin10"] + 1) or [],
    )
    monkeypatch.setattr(
        telegraph_sync_service.mysql_telegraph_repository,
        "upsert_telegraph_records",
        lambda records: len(records),
    )

    result = telegraph_sync_service.run_scheduled_telegraph_sync(source="all")

    assert result["status"] == "success"
    assert calls == {"cls": 1, "jin10": 1}


def test_run_scheduled_telegraph_sync_trims_to_max_rows(monkeypatch):
    fixed_now = datetime(2026, 4, 2, 12, 0, 0, tzinfo=telegraph_sync_service.SHANGHAI_TZ)
    monkeypatch.setattr(telegraph_sync_service, "_now_shanghai", lambda: fixed_now)
    monkeypatch.setattr(
        telegraph_sync_service.mysql_telegraph_repository,
        "init_mysql_tables",
        lambda: None,
    )
    monkeypatch.setattr(
        telegraph_sync_service,
        "fetch_cls_telegraph_records",
        lambda rn, timeout: [],
    )
    monkeypatch.setattr(
        telegraph_sync_service.mysql_telegraph_repository,
        "upsert_telegraph_records",
        lambda records: 0,
    )
    monkeypatch.setattr(
        telegraph_sync_service.mysql_client,
        "record_sync_run",
        lambda **kwargs: None,
    )

    captured_max_rows = []
    monkeypatch.setattr(
        telegraph_sync_service.mysql_telegraph_repository,
        "trim_telegraph_rows",
        lambda max_rows: captured_max_rows.append(max_rows) or 12,
    )

    result = telegraph_sync_service.run_scheduled_telegraph_sync(source="cls")

    assert captured_max_rows == [telegraph_sync_service.MAX_TELEGRAPH_ROWS]
    assert result["results"]["cls"]["trimmed_count"] == 12
