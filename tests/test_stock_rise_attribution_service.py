#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""stock_rise_attribution_service 单元测试。"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from services.stock_rise_attribution_service import (
    CNINFO_WINDOW_DAYS,
    DEFAULT_SELECTED_SOURCES,
    DEFAULT_WINDOW_DAYS,
    RESEARCH_WINDOW_DAYS,
    SOURCE_CNINFO,
    SOURCE_LABELS,
    SOURCE_P5W,
    SOURCE_REPORT_EARNINGS,
    SOURCE_RESEARCH,
    SOURCE_SEARCH,
    SOURCE_ZSXQ,
    get_stock_rise_attribution,
)


class TestStockRiseAttributionService(unittest.TestCase):
    @staticmethod
    def _window_dates_side_effect(days: int) -> list[str]:
        mapping = {
            CNINFO_WINDOW_DAYS: ["2026-03-30"],
            RESEARCH_WINDOW_DAYS: ["2026-02-01"],
            DEFAULT_WINDOW_DAYS: ["2026-03-31"],
        }
        return mapping[days]

    def test_default_sources_include_report_earnings(self) -> None:
        self.assertIn(SOURCE_REPORT_EARNINGS, DEFAULT_SELECTED_SOURCES)

    @patch("services.stock_rise_attribution_service._build_ai_summary")
    @patch("services.stock_rise_attribution_service.search_web_content")
    @patch("services.stock_rise_attribution_service._date_range_strings")
    def test_search_source_unconfigured_does_not_break_flow(
        self,
        mock_window_dates,
        mock_search_web_content,
        mock_ai_summary,
    ) -> None:
        mock_window_dates.side_effect = self._window_dates_side_effect
        mock_search_web_content.return_value = {
            "source": SOURCE_SEARCH,
            "query": "平安银行 上涨原因溯源，根据一周内高置信度证据/新闻，直接给出结论，100字以内。",
            "items": [],
            "count": 0,
            "content": "",
            "error": "kimi-cli not found in PATH",
            "status": "unconfigured",
        }
        mock_ai_summary.return_value = {
            "status": "empty",
            "content": "暂无可分析内容",
            "error": "",
        }

        result = get_stock_rise_attribution(
            stock_identity={"code": "000001", "name": "平安银行", "org_id": "g1"},
            selected_sources=[SOURCE_SEARCH],
        )

        search_status = next(
            item for item in result["source_statuses"] if item["source"] == SOURCE_SEARCH
        )
        self.assertEqual(search_status["status"], "unconfigured")
        self.assertEqual(result["evidence_items"], [])
        search_debug = next(
            item for item in result["source_debugs"] if item["source"] == SOURCE_SEARCH
        )
        self.assertEqual(search_debug["window_dates"], ["2026-03-31"])
        self.assertEqual(search_debug["steps"][0]["stage"], "run_query")
        self.assertEqual(
            mock_search_web_content.call_args.args[0],
            "平安银行 上涨原因溯源，根据一周内高置信度证据/新闻，直接给出结论，100字以内。",
        )

    @patch("services.stock_rise_attribution_service._build_ai_summary")
    @patch("services.stock_rise_attribution_service.search_web_content")
    @patch("services.stock_rise_attribution_service._date_range_strings")
    def test_search_source_wraps_raw_content_into_single_evidence(
        self,
        mock_window_dates,
        mock_search_web_content,
        mock_ai_summary,
    ) -> None:
        mock_window_dates.side_effect = self._window_dates_side_effect
        mock_search_web_content.return_value = {
            "source": SOURCE_SEARCH,
            "query": "平安银行 上涨原因溯源，根据一周内高置信度证据/新闻，直接给出结论，100字以内。",
            "items": [],
            "count": 0,
            "content": "近一周上涨主要受业绩改善预期和市场资金关注提升催化。",
            "error": "",
            "status": "available",
        }
        mock_ai_summary.return_value = {
            "status": "available",
            "content": "分析结果",
            "error": "",
        }

        result = get_stock_rise_attribution(
            stock_identity={"code": "000001", "name": "平安银行", "org_id": "g1"},
            selected_sources=[SOURCE_SEARCH],
        )

        self.assertEqual(len(result["evidence_items"]), 1)
        self.assertEqual(
            result["evidence_items"][0]["title"],
            "平安银行 上涨原因溯源",
        )
        self.assertEqual(
            result["evidence_items"][0]["summary"],
            "近一周上涨主要受业绩改善预期和市场资金关注提升催化。",
        )
        self.assertIn("时间未知", result["evidence_items"][0]["tags"])
        self.assertEqual(result["evidence_items"][0]["date"], "")
        self.assertEqual(
            result["evidence_items"][0]["raw"]["query"],
            "平安银行 上涨原因溯源，根据一周内高置信度证据/新闻，直接给出结论，100字以内。",
        )
        search_debug = next(
            item for item in result["source_debugs"] if item["source"] == SOURCE_SEARCH
        )
        self.assertEqual(search_debug["steps"][-1]["stage"], "build_search_evidence")

    @patch("services.stock_rise_attribution_service._build_ai_summary")
    @patch("services.stock_rise_attribution_service._build_strong_evidence_summary")
    @patch("services.stock_rise_attribution_service._fetch_cninfo_records")
    @patch("services.stock_rise_attribution_service._date_range_strings")
    def test_cninfo_filters_earnings_categories(
        self,
        mock_window_dates,
        mock_fetch_records,
        mock_build_summary,
        mock_ai_summary,
    ) -> None:
        mock_window_dates.side_effect = self._window_dates_side_effect
        mock_fetch_records.return_value = [
            {
                "announcementTitle": "2025年年度报告",
                "announcementTime": "2026-03-30",
                "adjunctUrl": "https://example.com/annual.pdf",
                "category": "category_ndbg_szsh",
            },
            {
                "announcementTitle": "关于签署重大合作协议的公告",
                "announcementTime": "2026-03-30",
                "adjunctUrl": "https://example.com/coop.pdf",
                "category": "category_rcjy_szsh",
            },
        ]
        mock_build_summary.return_value = ("合作项目推进", ["原文摘要"])
        mock_ai_summary.return_value = {"status": "empty", "content": "暂无可分析内容", "error": ""}

        result = get_stock_rise_attribution(
            stock_identity={"code": "000001", "name": "平安银行", "org_id": "g1"},
            selected_sources=[SOURCE_CNINFO],
        )

        mock_fetch_records.assert_called_once()
        self.assertEqual(
            mock_fetch_records.call_args.kwargs["window_dates"],
            ["2026-03-30"],
        )
        self.assertEqual(len(result["evidence_items"]), 1)
        self.assertEqual(result["evidence_items"][0]["title"], "关于签署重大合作协议的公告")
        cninfo_status = next(
            item for item in result["source_statuses"] if item["source"] == SOURCE_CNINFO
        )
        self.assertEqual(cninfo_status["status"], "available")
        self.assertEqual(cninfo_status["count"], 1)
        cninfo_debug = next(
            item for item in result["source_debugs"] if item["source"] == SOURCE_CNINFO
        )
        self.assertEqual(cninfo_debug["steps"][1]["stage"], "fetch_records")
        self.assertEqual(
            cninfo_debug["steps"][2]["data"]["filtered_count"],
            1,
        )

    @patch("services.stock_rise_attribution_service._build_ai_summary")
    @patch("services.stock_rise_attribution_service._build_strong_evidence_summary")
    @patch("services.stock_rise_attribution_service._fetch_cninfo_records")
    @patch("services.stock_rise_attribution_service._date_range_strings")
    def test_research_uses_90_day_window(
        self,
        mock_window_dates,
        mock_fetch_records,
        mock_build_summary,
        mock_ai_summary,
    ) -> None:
        mock_window_dates.side_effect = self._window_dates_side_effect
        mock_fetch_records.return_value = [
            {
                "announcementTitle": "机构调研纪要",
                "announcementTime": "2026-02-01",
                "adjunctUrl": "https://example.com/research.pdf",
                "category": "",
            }
        ]
        mock_build_summary.return_value = ("调研纪要摘要", ["原文摘要"])
        mock_ai_summary.return_value = {"status": "empty", "content": "暂无可分析内容", "error": ""}

        result = get_stock_rise_attribution(
            stock_identity={"code": "000001", "name": "平安银行", "org_id": "g1"},
            selected_sources=[SOURCE_RESEARCH],
        )

        mock_fetch_records.assert_called_once()
        self.assertEqual(
            mock_fetch_records.call_args.kwargs["window_dates"],
            ["2026-02-01"],
        )
        self.assertEqual(len(result["evidence_items"]), 1)
        self.assertEqual(result["evidence_items"][0]["title"], "机构调研纪要")
        research_debug = next(
            item for item in result["source_debugs"] if item["source"] == SOURCE_RESEARCH
        )
        self.assertEqual(research_debug["window_dates"], ["2026-02-01"])

    @patch("services.stock_rise_attribution_service._build_ai_summary")
    @patch("services.stock_rise_attribution_service.get_annual_report_parser_result")
    @patch("services.stock_rise_attribution_service._date_range_strings")
    def test_report_earnings_uses_windowed_report_data(
        self,
        mock_window_dates,
        mock_report_result,
        mock_ai_summary,
    ) -> None:
        mock_window_dates.side_effect = self._window_dates_side_effect
        mock_report_result.return_value = {
            "overall_status": "success",
            "errors": [],
            "reports": [
                {
                    "report_title": "2025年年度报告",
                    "report_type": "annual",
                    "report_period": "20251231",
                    "announcement_date": "2026-03-31",
                    "pdf_url": "https://example.com/report.pdf",
                    "report_status": {
                        "financial_status": "financial_loaded",
                    },
                    "financial_changes": [
                        {"metric_name": "营业收入", "change_rate": 20.0, "status": "ok"},
                        {"metric_name": "归母净利润", "change_rate": 50.0, "status": "ok"},
                    ],
                    "management_sections": [
                        {"section_title": "管理层讨论与分析", "full_text_cleaned": "全文"}
                    ],
                }
            ],
        }
        mock_ai_summary.return_value = {"status": "empty", "content": "暂无可分析内容", "error": ""}

        result = get_stock_rise_attribution(
            stock_identity={"code": "000001", "name": "平安银行", "org_id": "g1"},
            selected_sources=[SOURCE_REPORT_EARNINGS],
        )

        self.assertEqual(len(result["evidence_items"]), 1)
        item = result["evidence_items"][0]
        self.assertEqual(item["source"], SOURCE_REPORT_EARNINGS)
        self.assertEqual(item["source_label"], SOURCE_LABELS[SOURCE_REPORT_EARNINGS])
        self.assertIn("营业收入+20.0%", item["summary"])
        self.assertNotIn("管理层讨论与分析", item["summary"])
        self.assertIn("financial_changes", item["raw"])
        self.assertEqual(result["source_windows"][SOURCE_REPORT_EARNINGS], [])
        report_debug = next(
            item
            for item in result["source_debugs"]
            if item["source"] == SOURCE_REPORT_EARNINGS
        )
        self.assertEqual(report_debug["steps"][1]["stage"], "select_latest_report")

    @patch("services.stock_rise_attribution_service._build_ai_summary")
    @patch("services.stock_rise_attribution_service.get_annual_report_parser_result")
    @patch("services.stock_rise_attribution_service._date_range_strings")
    def test_report_earnings_ignores_date_window_and_uses_latest_report(
        self,
        mock_window_dates,
        mock_report_result,
        mock_ai_summary,
    ) -> None:
        mock_window_dates.side_effect = self._window_dates_side_effect
        mock_report_result.return_value = {
            "overall_status": "success",
            "errors": [],
            "reports": [
                {
                    "report_title": "2025年半年度报告",
                    "report_type": "half_year",
                    "report_period": "20250630",
                    "announcement_date": "2025-08-20",
                    "pdf_url": "https://example.com/report.pdf",
                    "report_status": {
                        "financial_status": "financial_loaded",
                    },
                    "financial_changes": [
                        {"metric_name": "营业收入", "change_rate": 12.0, "status": "ok"},
                    ],
                    "management_sections": [],
                }
            ],
        }
        mock_ai_summary.return_value = {"status": "empty", "content": "暂无可分析内容", "error": ""}

        result = get_stock_rise_attribution(
            stock_identity={"code": "000001", "name": "平安银行", "org_id": "g1"},
            selected_sources=[SOURCE_REPORT_EARNINGS],
        )

        self.assertEqual(len(result["evidence_items"]), 1)
        self.assertEqual(result["evidence_items"][0]["title"], "2025年半年度报告")
        report_status = next(
            item
            for item in result["source_statuses"]
            if item["source"] == SOURCE_REPORT_EARNINGS
        )
        self.assertEqual(report_status["status"], "report_earnings_loaded")
        self.assertEqual(report_status["count"], 1)
        self.assertEqual(result["source_windows"][SOURCE_REPORT_EARNINGS], [])

    @patch("services.stock_rise_attribution_service._build_ai_summary")
    @patch("services.stock_rise_attribution_service.fetch_topics_by_date")
    @patch("services.stock_rise_attribution_service._date_range_strings")
    def test_zsxq_debug_records_match_counts(
        self,
        mock_window_dates,
        mock_fetch_topics_by_date,
        mock_ai_summary,
    ) -> None:
        mock_window_dates.side_effect = self._window_dates_side_effect
        mock_fetch_topics_by_date.return_value = {
            "items": [
                {
                    "title": "【平安银行】观点更新",
                    "content": "平安银行 000001 基本面跟踪",
                    "created_at": "2026-03-31 09:00",
                    "url": "https://example.com/topic/1",
                },
                {
                    "title": "其他公司",
                    "content": "无关内容",
                    "created_at": "2026-03-31 10:00",
                    "url": "https://example.com/topic/2",
                },
            ],
            "error": "",
        }
        mock_ai_summary.return_value = {"status": "empty", "content": "暂无可分析内容", "error": ""}

        with patch("services.stock_rise_attribution_service.get_zsxq_cookie", return_value="cookie"):
            with patch(
                "services.stock_rise_attribution_service.get_zsxq_group_ids",
                return_value="group1",
            ):
                result = get_stock_rise_attribution(
                    stock_identity={"code": "000001", "name": "平安银行", "org_id": "g1"},
                    selected_sources=[SOURCE_ZSXQ],
                )

        self.assertEqual(len(result["evidence_items"]), 1)
        zsxq_debug = next(
            item for item in result["source_debugs"] if item["source"] == SOURCE_ZSXQ
        )
        self.assertEqual(zsxq_debug["steps"][0]["stage"], "fetch_topics_by_date")
        self.assertEqual(zsxq_debug["steps"][0]["data"]["raw_count"], 2)
        self.assertEqual(zsxq_debug["steps"][0]["data"]["matched_count"], 1)

    @patch("services.stock_rise_attribution_service._build_ai_summary")
    @patch("services.stock_rise_attribution_service._date_range_strings")
    def test_not_selected_sources_include_debug_info(
        self,
        mock_window_dates,
        mock_ai_summary,
    ) -> None:
        mock_window_dates.side_effect = self._window_dates_side_effect
        mock_ai_summary.return_value = {"status": "empty", "content": "暂无可分析内容", "error": ""}

        result = get_stock_rise_attribution(
            stock_identity={"code": "000001", "name": "平安银行", "org_id": "g1"},
            selected_sources=[SOURCE_P5W],
        )

        cninfo_debug = next(
            item for item in result["source_debugs"] if item["source"] == SOURCE_CNINFO
        )
        self.assertEqual(cninfo_debug["steps"][0]["stage"], "selection")
        self.assertFalse(cninfo_debug["steps"][0]["data"]["selected"])


if __name__ == "__main__":
    unittest.main()
