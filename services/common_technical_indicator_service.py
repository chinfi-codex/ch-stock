#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
常用技术指标标准输出服务
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from tools.kline_data import get_ak_price_df
from tools.technical_indicators import (
    FULL_GROUP_FIELDS,
    REQUIRED_COLUMNS,
    SUMMARY_FIELDS,
    build_indicator_full_payload,
    build_indicator_summary_payload,
    calculate_common_indicators,
)


MIN_FETCH_COUNT = 400
CORE_HISTORY_WINDOW = 250


def _required_fields_for_view(view: str) -> list[str]:
    if view == "full":
        fields = []
        for group_fields in FULL_GROUP_FIELDS.values():
            fields.extend(group_fields)
        return fields
    if view == "summary":
        return list(SUMMARY_FIELDS)
    raise ValueError(f"unsupported view: {view}")


def _build_unavailable_payload(
    code: str, view: str, missing_fields: Iterable[str], trade_date: str | None = None
) -> dict:
    return {
        "code": code,
        "trade_date": trade_date,
        "view": view,
        "status": "unavailable",
        "groups": {} if view == "summary" else {group: {} for group in FULL_GROUP_FIELDS},
        "missing_fields": list(missing_fields),
    }


def _count_missing(values: list[object]) -> int:
    return sum(value is None for value in values)


def _required_group_missing(full_groups: dict) -> bool:
    required_groups = [
        "trend",
        "momentum",
        "oscillation",
        "volatility",
        "trend_strength",
        "state",
    ]
    for group in required_groups:
        group_values = list((full_groups.get(group) or {}).values())
        if not group_values or all(value is None for value in group_values):
            return True
    return False


def _required_group_has_large_gap(full_groups: dict) -> bool:
    required_groups = [
        "trend",
        "momentum",
        "oscillation",
        "volatility",
        "trend_strength",
        "state",
    ]
    for group in required_groups:
        group_values = list((full_groups.get(group) or {}).values())
        if not group_values:
            return True
        missing_ratio = _count_missing(group_values) / len(group_values)
        if missing_ratio > 0.2:
            return True
    return False


def _determine_status(view: str, groups: dict, missing_fields: list[str]) -> str:
    total_fields = len(_required_fields_for_view(view))
    if total_fields == 0:
        return "unavailable"

    if view == "summary":
        if not groups or len(groups) != total_fields:
            return "unavailable"
        missing_count = _count_missing(list(groups.values()))
        if missing_count == total_fields:
            return "unavailable"
        return "available" if missing_count / total_fields <= 0.2 else "partial"

    if not groups or _required_group_missing(groups):
        all_values = []
        for group_values in groups.values():
            all_values.extend(group_values.values())
        if not all_values or all(value is None for value in all_values):
            return "unavailable"
        return "partial"

    if _required_group_has_large_gap(groups):
        return "partial"

    missing_count = len(missing_fields)
    return "available" if missing_count / total_fields <= 0.2 else "partial"


def get_common_indicators(
    code: str, view: str = "full", end_date: str | None = None, count: int | None = None
) -> dict:
    fetch_count = max(count or MIN_FETCH_COUNT, MIN_FETCH_COUNT)

    try:
        price_df = get_ak_price_df(code, end_date=end_date, count=fetch_count)
    except Exception as exc:
        return _build_unavailable_payload(
            code=code,
            view=view,
            missing_fields=[str(exc)],
        )

    if price_df is None or price_df.empty:
        return _build_unavailable_payload(code=code, view=view, missing_fields=["price_df"])

    missing_columns = [col for col in REQUIRED_COLUMNS if col not in price_df.columns]
    if missing_columns:
        return _build_unavailable_payload(code=code, view=view, missing_fields=missing_columns)

    try:
        indicator_df = calculate_common_indicators(price_df)
    except Exception as exc:
        return _build_unavailable_payload(
            code=code,
            view=view,
            missing_fields=[str(exc)],
        )

    if indicator_df.empty:
        return _build_unavailable_payload(code=code, view=view, missing_fields=["indicator_df"])

    latest = indicator_df.iloc[-1]
    trade_date = indicator_df.index[-1].strftime("%Y-%m-%d")

    if view == "full":
        groups = build_indicator_full_payload(latest)
        missing_fields = [
            field
            for fields in FULL_GROUP_FIELDS.values()
            for field in fields
            if groups.get(next(group for group, group_fields in FULL_GROUP_FIELDS.items() if field in group_fields), {}).get(field) is None
        ]
    elif view == "summary":
        groups = build_indicator_summary_payload(latest)
        missing_fields = [field for field, value in groups.items() if value is None]
    else:
        raise ValueError(f"unsupported view: {view}")

    status = _determine_status(view=view, groups=groups, missing_fields=missing_fields)
    if status == "unavailable":
        return _build_unavailable_payload(
            code=code,
            view=view,
            missing_fields=missing_fields or ["core_fields_unavailable"],
            trade_date=trade_date,
        )

    if len(price_df) < CORE_HISTORY_WINDOW and status == "available":
        status = "partial"

    return {
        "code": code,
        "trade_date": trade_date,
        "view": view,
        "status": status,
        "groups": groups,
        "missing_fields": missing_fields,
    }


def get_common_indicators_full(
    code: str, end_date: str | None = None, count: int | None = None
) -> dict:
    return get_common_indicators(code=code, view="full", end_date=end_date, count=count)


def get_common_indicators_summary(
    code: str, end_date: str | None = None, count: int | None = None
) -> dict:
    return get_common_indicators(code=code, view="summary", end_date=end_date, count=count)
