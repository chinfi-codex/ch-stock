#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import datetime as dt

from tools import crawlers


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


class _FakeJin10Session:
    def get(self, _url, headers=None, timeout=None):
        assert headers is not None
        assert timeout == 12
        return _FakeResponse(
            {
                "data": [
                    {
                        "id": 123,
                        "time": "2026-04-02T01:30:00.000Z",
                        "type": "flash",
                        "important": 2,
                        "channel": ["market", "macro"],
                        "tags": ["美元", "黄金"],
                        "remark": ["test"],
                        "extras": {"lang": "zh"},
                        "data": {
                            "title": "美元指数拉升",
                            "content": "盘中快速走高。",
                            "source": "Jin10",
                            "source_link": "https://example.com/flash/123",
                            "pic": "https://example.com/pic.png",
                        },
                    }
                ]
            }
        )


def test_cls_telegraphs_formats_publish_time_and_tags(monkeypatch):
    monkeypatch.setattr(
        crawlers,
        "fetch_cls_telegraph_records",
        lambda **kwargs: [
            {
                "title": "财联社电报",
                "content": "内容",
                "level": 1,
                "subjects": [{"subject_name": "新能源"}, {"subject_name": "AI"}],
                "ctime": 1710000000,
            }
        ],
    )

    df = crawlers.cls_telegraphs()

    assert list(df.columns) == ["标题", "内容", "等级", "标签", "发布时间", "发布日期"]
    assert df.iloc[0]["标签"] == "新能源,AI"
    assert isinstance(df.iloc[0]["发布时间"], dt.time)


def test_fetch_jin10_flash_records_extracts_expected_fields(monkeypatch):
    monkeypatch.setattr(crawlers, "get_jin10_cookie", lambda: "")

    records = crawlers.fetch_jin10_flash_records(timeout=12, session=_FakeJin10Session())

    assert len(records) == 1
    assert records[0]["id"] == 123
    assert records[0]["title"] == "美元指数拉升"
    assert records[0]["content"] == "盘中快速走高。"
    assert records[0]["source_link"] == "https://example.com/flash/123"
    assert records[0]["tags"] == ["美元", "黄金"]
    assert records[0]["channel"] == ["market", "macro"]
