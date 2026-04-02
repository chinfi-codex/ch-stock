#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MySQL 同步命令行入口。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.mysql_sync_service import run_bootstrap_sync, run_scheduled_sync


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="同步市场数据与 daily_basic 到 MySQL")
    parser.add_argument(
        "--mode",
        choices=["bootstrap", "scheduled"],
        required=True,
        help="同步模式",
    )
    parser.add_argument(
        "--only",
        choices=["all", "market", "daily_basic"],
        default="all",
        help="同步目标",
    )
    parser.add_argument(
        "--trade-date",
        dest="trade_date",
        default=None,
        help="指定交易日，支持 YYYYMMDD / YYYY-MM-DD",
    )
    parser.add_argument(
        "--lookback-days",
        dest="lookback_days",
        type=int,
        default=3,
        help="daily_basic 回补的最近交易日数量",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.mode == "bootstrap":
        result = run_bootstrap_sync(only=args.only)
    else:
        result = run_scheduled_sync(
            only=args.only,
            trade_date=args.trade_date,
            lookback_days=args.lookback_days,
        )

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("status") != "partial_failed" else 1


if __name__ == "__main__":
    sys.exit(main())
