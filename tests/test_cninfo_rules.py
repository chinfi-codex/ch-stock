#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""cninfo_rules 单元测试。"""

from __future__ import annotations

import unittest

from tools.crawlers import apply_cninfo_rules, classify_cninfo_fulltext


class TestCninfoRules(unittest.TestCase):
    """验证巨潮公告标题规则。"""

    def test_inquiry_reply_rule(self) -> None:
        result = classify_cninfo_fulltext("关于深圳证券交易所问询函的回复公告")
        self.assertEqual(result["rule_id"], "cninfo.fulltext.inquiry_reply.v1")
        self.assertFalse(result["excluded"])

    def test_abnormal_volatility_reply_is_excluded(self) -> None:
        result = classify_cninfo_fulltext("关于股票交易异常波动问询函的回复公告")
        self.assertEqual(
            result["rule_id"],
            "cninfo.fulltext.excluded.abnormal_volatility_reply.v1",
        )
        self.assertTrue(result["excluded"])

    def test_regulatory_rule(self) -> None:
        result = classify_cninfo_fulltext("关于收到监管函的公告")
        self.assertEqual(result["rule_id"], "cninfo.fulltext.regulatory_letter.v1")
        self.assertEqual(result["subcategory"], "监管函")

    def test_increase_and_decrease_rules(self) -> None:
        increase_result = classify_cninfo_fulltext("关于控股股东增持计划的公告")
        decrease_result = classify_cninfo_fulltext("关于股东减持股份的公告")
        self.assertEqual(increase_result["rule_id"], "cninfo.fulltext.increase_hold.v1")
        self.assertEqual(decrease_result["rule_id"], "cninfo.fulltext.decrease_hold.v1")

    def test_decrease_progress_is_excluded(self) -> None:
        result = classify_cninfo_fulltext("关于股东减持计划时间过半的进展公告")
        self.assertEqual(
            result["rule_id"],
            "cninfo.fulltext.excluded.decrease_progress.v1",
        )
        self.assertTrue(result["excluded"])

    def test_major_cooperation_rule(self) -> None:
        result = classify_cninfo_fulltext("关于签署战略合作协议的公告")
        self.assertEqual(
            result["rule_id"],
            "cninfo.fulltext.major_cooperation_project.v1",
        )

    def test_periodic_report_is_excluded(self) -> None:
        result = classify_cninfo_fulltext("2025年年度报告")
        self.assertEqual(
            result["rule_id"],
            "cninfo.fulltext.excluded.periodic_report_disclosure.v1",
        )
        self.assertTrue(result["excluded"])

    def test_periodic_report_summary_is_excluded(self) -> None:
        result = classify_cninfo_fulltext("2025年年度报告摘要")
        self.assertEqual(
            result["rule_id"],
            "cninfo.fulltext.excluded.periodic_report_disclosure.v1",
        )
        self.assertTrue(result["excluded"])

    def test_earnings_forecast_is_excluded(self) -> None:
        result = classify_cninfo_fulltext("2026年第一季度业绩预告")
        self.assertEqual(
            result["rule_id"],
            "cninfo.fulltext.excluded.earnings_forecast_disclosure.v1",
        )
        self.assertTrue(result["excluded"])

    def test_quick_report_rule(self) -> None:
        result = classify_cninfo_fulltext("2025年度业绩快报")
        self.assertEqual(result["rule_id"], "cninfo.fulltext.quick_report.v1")

    def test_other_rule(self) -> None:
        result = classify_cninfo_fulltext("关于召开2025年第一次临时股东大会的通知")
        self.assertEqual(result["rule_id"], "cninfo.fulltext.excluded.other.v1")
        self.assertTrue(result["excluded"])

    def test_apply_cninfo_rules_filters_excluded_by_default(self) -> None:
        announcements = [
            {"announcementTitle": "关于股票交易异常波动问询函的回复公告"},
            {"announcementTitle": "关于召开2025年第一次临时股东大会的通知"},
            {"announcementTitle": "关于签署战略合作协议的公告"},
        ]

        result = apply_cninfo_rules("fulltext", announcements)

        self.assertEqual(len(result), 1)
        self.assertEqual(
            result[0]["rule_id"],
            "cninfo.fulltext.major_cooperation_project.v1",
        )


if __name__ == "__main__":
    unittest.main()
