#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""年报解析服务编排。"""

from __future__ import annotations

from typing import Any

from infra.data_utils import convert_to_ts_code
from tools.annual_report_parser import (
    get_financial_changes,
    get_stock_identity,
    locate_periodic_reports,
    parse_management_sections_from_pdf,
)


def _overall_status_from_reports(reports: list[dict[str, Any]], locate_status: str) -> str:
    if locate_status != "report_located":
        return locate_status

    if not reports:
        return "report_not_found"

    successful_reports = 0
    partial_reports = 0
    for report in reports:
        financial_status = report["report_status"].get("financial_status", "")
        sections_status = report["report_status"].get("sections_status", "")

        if financial_status == "financial_loaded" and sections_status == "sections_extracted":
            successful_reports += 1
        elif financial_status != "financial_failed" or sections_status != "sections_not_found":
            partial_reports += 1

    if successful_reports == len(reports):
        return "success"
    if successful_reports > 0 or partial_reports > 0:
        return "partial_success"
    return "failed"


def get_annual_report_parser_result(
    stock_input: str,
    report_limit: int = 1,
) -> dict[str, Any]:
    """获取单只股票的年报/半年报解析结果。"""
    ts_code = convert_to_ts_code(stock_input)
    stock_identity = get_stock_identity(ts_code)
    locate_result = locate_periodic_reports(ts_code, report_limit=report_limit)

    reports: list[dict[str, Any]] = []
    errors = list(locate_result.get("errors", []))

    for report in locate_result.get("reports", []):
        financial_result = get_financial_changes(
            ts_code=ts_code,
            report_period=report["report_period"],
            report_type=report["report_type"],
        )
        section_result = parse_management_sections_from_pdf(report.get("pdf_url", ""))

        errors.extend(financial_result.get("errors", []))
        errors.extend(section_result.get("errors", []))

        reports.append(
            {
                "report_title": report["report_title"],
                "report_type": report["report_type"],
                "report_period": report["report_period"],
                "announcement_date": report.get("announcement_date", ""),
                "pdf_url": report.get("pdf_url", ""),
                "report_source": report.get("report_source", locate_result.get("source", "")),
                "report_status": {
                    "locate_status": locate_result.get("status", ""),
                    "financial_status": financial_result.get("status", ""),
                    "text_status": section_result.get("text_status", ""),
                    "sections_status": section_result.get("sections_status", ""),
                },
                "financial_changes": financial_result.get("items", []),
                "management_sections": section_result.get("management_sections", []),
            }
        )

    deduped_errors = [error for error in dict.fromkeys([err for err in errors if err])]
    overall_status = _overall_status_from_reports(reports, locate_result.get("status", ""))

    return {
        "stock": stock_identity,
        "reports": reports,
        "overall_status": overall_status,
        "errors": deduped_errors,
        "report_source": locate_result.get("source", ""),
        "fallback_used": bool(locate_result.get("fallback_used")),
    }
