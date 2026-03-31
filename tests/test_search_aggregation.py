#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""search_aggregation 单元测试。"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from tools.search_aggregation import search_web_content


class TestSearchAggregation(unittest.TestCase):
    @patch("tools.search_aggregation.run_kimi_cli")
    def test_search_returns_raw_content(self, mock_run_kimi_cli) -> None:
        mock_run_kimi_cli.return_value = {
            "success": True,
            "content": "近一周催化主要来自新车型发布与资金关注提升。",
            "error_code": "",
            "error_message": "",
            "user_message": "",
        }

        result = search_web_content("平安银行 上涨原因溯源，根据一周内高置信度证据/新闻，直接给出结论，100字以内。")

        self.assertEqual(result["status"], "available")
        self.assertEqual(result["count"], 0)
        self.assertEqual(result["items"], [])
        self.assertEqual(result["content"], "近一周催化主要来自新车型发布与资金关注提升。")

    @patch("tools.search_aggregation.run_kimi_cli")
    def test_search_empty_when_no_results(self, mock_run_kimi_cli) -> None:
        mock_run_kimi_cli.return_value = {
            "success": True,
            "content": "NO_RESULTS",
            "error_code": "",
            "error_message": "",
            "user_message": "",
        }

        result = search_web_content("平安银行 上涨原因溯源，根据一周内高置信度证据/新闻，直接给出结论，100字以内。")

        self.assertEqual(result["status"], "empty")
        self.assertEqual(result["content"], "")

    @patch("tools.search_aggregation.run_kimi_cli")
    def test_search_unconfigured_when_kimi_missing(self, mock_run_kimi_cli) -> None:
        mock_run_kimi_cli.return_value = {
            "success": False,
            "content": "",
            "error_code": "kimi_not_installed",
            "error_message": "kimi-cli not found in PATH",
            "user_message": "AI分析暂时不可用(kimi-cli 未安装)",
        }

        result = search_web_content("平安银行 上涨原因溯源，根据一周内高置信度证据/新闻，直接给出结论，100字以内。")

        self.assertEqual(result["status"], "unconfigured")
        self.assertIn("kimi-cli", result["error"])

    @patch("tools.search_aggregation.run_kimi_cli")
    def test_search_failed_when_cli_times_out(self, mock_run_kimi_cli) -> None:
        mock_run_kimi_cli.return_value = {
            "success": False,
            "content": "",
            "error_code": "kimi_timeout",
            "error_message": "request timed out",
            "user_message": "",
        }

        result = search_web_content("平安银行 上涨原因溯源，根据一周内高置信度证据/新闻，直接给出结论，100字以内。")

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error"], "request timed out")


if __name__ == "__main__":
    unittest.main()
