#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
关注列表服务。
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import streamlit as st


WATCHLIST_FILE = Path("datas/watchlist.json")
WATCHLIST_SESSION_KEY = "watchlist_data"
UNKNOWN_SOURCE_GROUP = "未知来源"


def _normalize_source_group(source_group: str | None) -> str:
    value = str(source_group or "").strip()
    return value or UNKNOWN_SOURCE_GROUP


def _normalize_watch_item(item: dict[str, Any]) -> dict[str, Any]:
    code = str(item.get("code", "")).strip()
    name = str(item.get("name", "")).strip()
    add_time = str(item.get("add_time", "")).strip()
    source_group = _normalize_source_group(item.get("source_group"))

    source_groups = item.get("source_groups")
    if isinstance(source_groups, list):
        normalized_groups: list[str] = []
        for group in source_groups:
            normalized_group = _normalize_source_group(group)
            if normalized_group not in normalized_groups:
                normalized_groups.append(normalized_group)
    else:
        normalized_groups = []

    if source_group not in normalized_groups:
        normalized_groups.append(source_group)

    normalized_item = {
        "code": code,
        "name": name,
        "add_time": add_time,
        "source_group": source_group,
        "source_groups": normalized_groups,
    }
    return normalized_item


def _normalize_watchlist_data(watchlist_data: dict[str, Any] | None) -> dict[str, Any]:
    items = []
    raw_items = (watchlist_data or {}).get("watchlist", [])
    if not isinstance(raw_items, list):
        raw_items = []

    for item in raw_items:
        if not isinstance(item, dict):
            continue
        normalized_item = _normalize_watch_item(item)
        if normalized_item["code"]:
            items.append(normalized_item)

    return {"watchlist": items}


def _read_watchlist_from_file() -> dict[str, Any]:
    if not WATCHLIST_FILE.exists():
        return {"watchlist": []}

    try:
        with WATCHLIST_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {"watchlist": []}

    return _normalize_watchlist_data(data)


def _write_watchlist_to_file(watchlist_data: dict[str, Any]) -> None:
    WATCHLIST_FILE.parent.mkdir(parents=True, exist_ok=True)
    with WATCHLIST_FILE.open("w", encoding="utf-8") as f:
        json.dump(_normalize_watchlist_data(watchlist_data), f, ensure_ascii=False, indent=2)


def init_watchlist_state() -> None:
    """初始化关注列表到 session_state。"""
    if WATCHLIST_SESSION_KEY not in st.session_state:
        st.session_state[WATCHLIST_SESSION_KEY] = _read_watchlist_from_file()


def get_watchlist() -> dict[str, Any]:
    """获取当前关注列表。"""
    init_watchlist_state()
    return _normalize_watchlist_data(st.session_state.get(WATCHLIST_SESSION_KEY))


def save_watchlist(watchlist_data: dict[str, Any]) -> bool:
    """保存关注列表到文件和 session_state。"""
    normalized_data = _normalize_watchlist_data(watchlist_data)
    try:
        _write_watchlist_to_file(normalized_data)
    except Exception as exc:
        st.error(f"保存关注列表失败: {exc}")
        return False

    st.session_state[WATCHLIST_SESSION_KEY] = normalized_data
    return True


def is_watched(code: str, watchlist_data: dict[str, Any] | None = None) -> bool:
    """判断股票是否已关注。"""
    target_code = str(code).strip()
    if not target_code:
        return False

    current_data = _normalize_watchlist_data(watchlist_data) if watchlist_data else get_watchlist()
    watched_codes = {
        str(item.get("code", "")).strip()
        for item in current_data.get("watchlist", [])
        if isinstance(item, dict)
    }
    return target_code in watched_codes


def add_stock_to_watchlist(
    code: str,
    name: str,
    source_group: str,
) -> tuple[bool, str]:
    """添加股票到关注列表，重复关注时补充来源分组。"""
    current_data = get_watchlist()
    normalized_source_group = _normalize_source_group(source_group)
    now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    target_code = str(code).strip()
    target_name = str(name).strip()

    if not target_code:
        return False, "股票代码为空"

    items = current_data.get("watchlist", [])
    for item in items:
        if str(item.get("code", "")).strip() != target_code:
            continue

        normalized_item = _normalize_watch_item(item)
        if target_name and not normalized_item["name"]:
            normalized_item["name"] = target_name

        source_groups = normalized_item.get("source_groups", [])
        if normalized_source_group not in source_groups:
            source_groups.append(normalized_source_group)
            normalized_item["source_groups"] = source_groups
            normalized_item["source_group"] = normalized_source_group
            item.update(normalized_item)
            if save_watchlist(current_data):
                return True, "已补充来源分组"
            return False, "保存失败"

        item.update(normalized_item)
        return False, "已关注"

    items.append(
        {
            "code": target_code,
            "name": target_name,
            "add_time": now_str,
            "source_group": normalized_source_group,
            "source_groups": [normalized_source_group],
        }
    )
    current_data["watchlist"] = items

    if save_watchlist(current_data):
        return True, "关注成功"
    return False, "保存失败"


def remove_stock_from_watchlist(code: str) -> tuple[bool, str]:
    """从关注列表移除股票。"""
    current_data = get_watchlist()
    items = current_data.get("watchlist", [])
    target_code = str(code).strip()

    if not items:
        return False, "列表为空"

    stock_name = ""
    updated_items = []
    for item in items:
        item_code = str(item.get("code", "")).strip()
        if item_code == target_code:
            stock_name = str(item.get("name", "")).strip()
            continue
        updated_items.append(_normalize_watch_item(item))

    if len(updated_items) == len(items):
        return False, "未找到该股票"

    current_data["watchlist"] = updated_items
    if save_watchlist(current_data):
        return True, f"已移除 {stock_name}"
    return False, "保存失败"
