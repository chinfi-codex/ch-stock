#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""crawlers 中 cninfo 公告抓取测试。"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from tools.crawlers import cninfo_announcement_spider


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


def _build_announcement(
    title: str,
    *,
    timestamp_ms: int = 1711900800000,
    code: str = "300001",
    name: str = "特锐德",
    adjunct_url: str = "finalpage/2026-03-31/test.PDF",
    category: str = "",
):
    return {
        "announcementTime": timestamp_ms,
        "secName": name,
        "secCode": code,
        "announcementTitle": title,
        "adjunctUrl": adjunct_url,
        "category": category,
    }


class TestCninfoAnnouncementSpider(unittest.TestCase):
    """验证 cninfo_announcement_spider 的规则应用行为。"""

    @patch("tools.crawlers.requests.post")
    def test_fulltext_filters_excluded_by_default(self, mock_post) -> None:
        mock_post.return_value = _FakeResponse(
            {
                "announcements": [
                    _build_announcement("关于股票交易异常波动问询函的回复公告"),
                    _build_announcement("2025年年度报告"),
                    _build_announcement("关于召开2025年第一次临时股东大会的通知"),
                    _build_announcement("关于签署战略合作协议的公告"),
                ]
            }
        )

        df = cninfo_announcement_spider(pageNum=1, tabType="fulltext")

        self.assertIsNotNone(df)
        self.assertEqual(len(df), 1)
        self.assertEqual(
            df.iloc[0]["rule_id"],
            "cninfo.fulltext.major_cooperation_project.v1",
        )
        self.assertEqual(df.iloc[0]["announcementTime"], "2024-04-01")
        self.assertEqual(
            df.iloc[0]["adjunctUrl"],
            "http://static.cninfo.com.cn/finalpage/2026-03-31/test.PDF",
        )

    @patch("tools.crawlers.requests.post")
    def test_fulltext_filters_periodic_report_title_by_default(self, mock_post) -> None:
        mock_post.return_value = _FakeResponse(
            {"announcements": [_build_announcement("2025年年度报告摘要")]}
        )

        df = cninfo_announcement_spider(pageNum=1, tabType="fulltext")

        self.assertIsNone(df)

    @patch("tools.crawlers.requests.post")
    def test_fulltext_include_excluded_keeps_filtered_records(self, mock_post) -> None:
        mock_post.return_value = _FakeResponse(
            {"announcements": [_build_announcement("关于股票交易异常波动问询函的回复公告")]}
        )

        df = cninfo_announcement_spider(
            pageNum=1,
            tabType="fulltext",
            include_excluded=True,
        )

        self.assertIsNotNone(df)
        self.assertEqual(len(df), 1)
        self.assertTrue(df.iloc[0]["excluded"])
        self.assertEqual(
            df.iloc[0]["rule_id"],
            "cninfo.fulltext.excluded.abnormal_volatility_reply.v1",
        )

    @patch("tools.crawlers.requests.post")
    def test_relation_adds_rule_fields_without_filtering(self, mock_post) -> None:
        mock_post.return_value = _FakeResponse(
            {
                "announcements": [
                    _build_announcement("机构调研纪要一"),
                    _build_announcement("机构调研纪要二"),
                ]
            }
        )

        df = cninfo_announcement_spider(pageNum=1, tabType="relation")

        self.assertIsNotNone(df)
        self.assertEqual(len(df), 2)
        self.assertIn("rule_id", df.columns)
        self.assertEqual(df.iloc[0]["rule_id"], "cninfo.relation.unclassified.v1")
        self.assertFalse(df.iloc[0]["excluded"])

    @patch("tools.crawlers.requests.post")
    def test_use_rules_false_returns_original_shape(self, mock_post) -> None:
        mock_post.return_value = _FakeResponse(
            {"announcements": [_build_announcement("关于股票交易异常波动问询函的回复公告")]}
        )

        df = cninfo_announcement_spider(pageNum=1, tabType="fulltext", use_rules=False)

        self.assertIsNotNone(df)
        self.assertEqual(len(df), 1)
        self.assertEqual(
            list(df.columns),
            [
                "announcementTime",
                "secName",
                "secCode",
                "announcementTitle",
                "adjunctUrl",
                "category",
            ],
        )

    @patch("tools.crawlers.requests.post")
    def test_normalized_records_keep_category_field(self, mock_post) -> None:
        mock_post.return_value = _FakeResponse(
            {
                "announcements": [
                    _build_announcement(
                        "2025年年度报告",
                        category="category_ndbg_szsh",
                    )
                ]
            }
        )

        df = cninfo_announcement_spider(pageNum=1, tabType="fulltext", use_rules=False)

        self.assertIsNotNone(df)
        self.assertEqual(df.iloc[0]["category"], "category_ndbg_szsh")

    @patch("tools.crawlers.requests.post")
    def test_empty_announcements_returns_none(self, mock_post) -> None:
        mock_post.return_value = _FakeResponse({"announcements": []})

        df = cninfo_announcement_spider(pageNum=1, tabType="fulltext")

        self.assertIsNone(df)

    @patch("tools.crawlers.requests.post", side_effect=RuntimeError("boom"))
    def test_request_exception_returns_none(self, _mock_post) -> None:
        df = cninfo_announcement_spider(pageNum=1, tabType="fulltext")
        self.assertIsNone(df)


if __name__ == "__main__":
    unittest.main()
