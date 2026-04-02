#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from contextlib import contextmanager

from infra import mysql_telegraph_repository


def test_upsert_telegraph_records_serializes_json_fields(monkeypatch):
    captured = {}

    def fake_executemany_upsert(**kwargs):
        captured.update(kwargs)
        return len(kwargs["records"])

    monkeypatch.setattr(
        mysql_telegraph_repository,
        "executemany_upsert",
        fake_executemany_upsert,
    )

    saved = mysql_telegraph_repository.upsert_telegraph_records(
        [
            {
                "source": "cls",
                "source_item_id": None,
                "title": "标题",
                "content": "内容",
                "level": "1",
                "importance": None,
                "published_at": "2026-04-02 10:00:00",
                "tags_json": ["AI", "算力"],
                "channels_json": ["market"],
                "source_link": "https://example.com/cls",
                "raw_json": {"title": "标题", "ctime": 123},
                "dedupe_key": "abc",
            }
        ]
    )

    assert saved == 1
    assert captured["table_name"] == "telegraphs"
    assert captured["records"][0]["tags_json"] == '["AI", "算力"]'
    assert captured["records"][0]["channels_json"] == '["market"]'
    assert '"ctime": 123' in captured["records"][0]["raw_json"]


def test_init_mysql_tables_creates_telegraphs_table_and_log_table(monkeypatch):
    executed_sql = []
    log_init_called = []

    class _FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql):
            executed_sql.append(sql)

    class _FakeConnection:
        def cursor(self):
            return _FakeCursor()

    @contextmanager
    def fake_get_mysql_connection():
        yield _FakeConnection()

    monkeypatch.setattr(
        mysql_telegraph_repository,
        "get_mysql_connection",
        fake_get_mysql_connection,
    )
    monkeypatch.setattr(
        mysql_telegraph_repository,
        "init_etl_sync_run_log_table",
        lambda: log_init_called.append(True),
    )

    mysql_telegraph_repository.init_mysql_tables()

    assert any("CREATE TABLE IF NOT EXISTS telegraphs" in sql for sql in executed_sql)
    assert log_init_called == [True]


def test_trim_telegraph_rows_deletes_overflow(monkeypatch):
    operations = []

    class _FakeCursor:
        def __init__(self):
            self.rowcount = 3
            self._fetchone_result = {"total_count": 100003}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            operations.append((sql, params))

        def fetchone(self):
            return self._fetchone_result

    class _FakeConnection:
        def cursor(self):
            return _FakeCursor()

    @contextmanager
    def fake_get_mysql_connection():
        yield _FakeConnection()

    monkeypatch.setattr(
        mysql_telegraph_repository,
        "get_mysql_connection",
        fake_get_mysql_connection,
    )

    deleted_count = mysql_telegraph_repository.trim_telegraph_rows(100000)

    assert deleted_count == 3
    assert "SELECT COUNT(*) AS total_count FROM telegraphs" in operations[0][0]
    assert operations[1][1] == (3,)
