#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
电报同步命令行入口。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="同步 CLS / Jin10 电报到 MySQL")
    parser.add_argument(
        "--source",
        choices=["all", "cls", "jin10"],
        default="all",
        help="同步来源",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=6,
        help="抓取最近 N 小时的数据，0 表示不过滤",
    )
    parser.add_argument(
        "--rn",
        type=int,
        default=2000,
        help="CLS 接口的 rn 参数",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="请求超时秒数",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    from services.telegraph_sync_service import run_scheduled_telegraph_sync

    result = run_scheduled_telegraph_sync(
        source=args.source,
        hours=args.hours,
        rn=args.rn,
        timeout=args.timeout,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("status") != "partial_failed" else 1


if __name__ == "__main__":
    sys.exit(main())
