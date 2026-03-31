#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""annual_report_parser 单元测试。"""

from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from services.annual_report_service import get_annual_report_parser_result
from tools.annual_report_parser import (
    REPORT_TYPE_ANNUAL,
    calculate_financial_changes,
    extract_management_sections_from_text,
    select_target_reports,
)


class TestAnnualReportParser(unittest.TestCase):
    def test_select_target_reports_prefers_formal_reports(self) -> None:
        candidates = [
            {
                "report_title": "2025年年度报告摘要",
                "report_type": "annual",
                "report_period": "20251231",
                "announcement_date": "2026-03-21",
                "pdf_url": "summary.pdf",
                "priority": 90,
            },
            {
                "report_title": "2025年年度报告",
                "report_type": "annual",
                "report_period": "20251231",
                "announcement_date": "2026-03-21",
                "pdf_url": "full.pdf",
                "priority": 0,
            },
            {
                "report_title": "2025年半年度报告",
                "report_type": "half_year",
                "report_period": "20250630",
                "announcement_date": "2025-08-23",
                "pdf_url": "half.pdf",
                "priority": 0,
            },
        ]

        selected = select_target_reports(candidates, report_limit=2)

        self.assertEqual(len(selected), 2)
        self.assertEqual(selected[0]["report_title"], "2025年年度报告")
        self.assertEqual(selected[1]["report_title"], "2025年半年度报告")

    def test_calculate_financial_changes_uses_structured_data(self) -> None:
        frames = {
            "income": pd.DataFrame(
                [
                    {
                        "end_date": "20251231",
                        "total_revenue": 120.0,
                        "revenue": 120.0,
                        "n_income_attr_p": 20.0,
                    },
                    {
                        "end_date": "20241231",
                        "total_revenue": 100.0,
                        "revenue": 100.0,
                        "n_income_attr_p": 10.0,
                    },
                ]
            ),
            "fina_indicator": pd.DataFrame(
                [
                    {
                        "end_date": "20251231",
                        "profit_dedt": 18.0,
                        "grossprofit_margin": 25.0,
                        "netprofit_margin": 16.0,
                        "roe_dt": 12.0,
                        "debt_to_assets": 55.0,
                    },
                    {
                        "end_date": "20241231",
                        "profit_dedt": 9.0,
                        "grossprofit_margin": 20.0,
                        "netprofit_margin": 10.0,
                        "roe_dt": 8.0,
                        "debt_to_assets": 50.0,
                    },
                ]
            ),
            "cashflow": pd.DataFrame(
                [
                    {"end_date": "20251231", "n_cashflow_act": 30.0},
                    {"end_date": "20241231", "n_cashflow_act": 12.0},
                ]
            ),
            "balancesheet": pd.DataFrame(
                [
                    {"end_date": "20251231", "total_assets": 200.0, "total_liab": 110.0},
                    {"end_date": "20241231", "total_assets": 180.0, "total_liab": 90.0},
                ]
            ),
        }

        result = calculate_financial_changes("20251231", REPORT_TYPE_ANNUAL, frames)

        self.assertEqual(result["status"], "financial_loaded")
        revenue_metric = next(
            item for item in result["items"] if item["metric_name"] == "营业收入"
        )
        self.assertEqual(revenue_metric["current_value"], 120.0)
        self.assertEqual(revenue_metric["previous_value"], 100.0)
        self.assertEqual(revenue_metric["change_value"], 20.0)

        debt_metric = next(
            item for item in result["items"] if item["metric_name"] == "资产负债率"
        )
        self.assertEqual(debt_metric["current_value"], 55.0)
        self.assertEqual(debt_metric["previous_value"], 50.0)

    def test_extract_management_sections_returns_full_text(self) -> None:
        full_text = """
第一章 公司概况
这里是公司概况。

第二章 管理层讨论与分析
这是管理层讨论与分析第一段。
这是管理层讨论与分析第二段。

2.1 公司业务概要
这是公司业务概要第一段。
这是公司业务概要第二段。

第三章 重要事项
这里是重要事项。
"""

        result = extract_management_sections_from_text(full_text)

        self.assertEqual(result["status"], "sections_extracted")
        self.assertEqual(len(result["sections"]), 2)
        self.assertIn("这是管理层讨论与分析第一段。", result["sections"][0]["full_text_cleaned"])
        self.assertIn("这是公司业务概要第二段。", result["sections"][1]["full_text_cleaned"])
        self.assertEqual(result["sections"][0]["boundary_status"], "boundary_stable")

    @patch("services.annual_report_service.parse_management_sections_from_pdf")
    @patch("services.annual_report_service.get_financial_changes")
    @patch("services.annual_report_service.locate_periodic_reports")
    @patch("services.annual_report_service.get_stock_identity")
    def test_service_builds_stable_payload(
        self,
        mock_stock_identity,
        mock_locate_reports,
        mock_financial_changes,
        mock_parse_pdf,
    ) -> None:
        mock_stock_identity.return_value = {
            "ts_code": "000001.SZ",
            "code": "000001",
            "name": "平安银行",
        }
        mock_locate_reports.return_value = {
            "status": "report_located",
            "reports": [
                {
                    "report_title": "2025年年度报告",
                    "report_type": "annual",
                    "report_period": "20251231",
                    "announcement_date": "2026-03-21",
                    "pdf_url": "https://example.com/report.pdf",
                    "report_source": "cninfo",
                }
            ],
            "source": "cninfo",
            "fallback_used": True,
            "errors": ["tushare_anns_d:no_permission"],
        }
        mock_financial_changes.return_value = {
            "status": "financial_partially_loaded",
            "items": [{"metric_name": "营业收入", "status": "ok"}],
            "errors": [],
        }
        mock_parse_pdf.return_value = {
            "text_status": "text_extracted",
            "sections_status": "sections_extracted",
            "management_sections": [{"section_title": "管理层讨论与分析"}],
            "errors": [],
        }

        result = get_annual_report_parser_result("000001", report_limit=1)

        self.assertEqual(result["stock"]["ts_code"], "000001.SZ")
        self.assertEqual(result["overall_status"], "partial_success")
        self.assertTrue(result["fallback_used"])
        self.assertEqual(result["reports"][0]["report_status"]["sections_status"], "sections_extracted")
        self.assertIn("tushare_anns_d:no_permission", result["errors"])


if __name__ == "__main__":
    unittest.main()
