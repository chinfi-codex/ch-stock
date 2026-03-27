"""
数据存储模块
包含数据存储、文件处理等基础设施
"""

import json
import os
from datetime import date, datetime

import pandas as pd

REVIEW_DIR = os.path.join("datas", "reviews")


def _json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass
    return str(obj)


def _normalize_date_str(value):
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def _resolve_review_dir(review_dir=None):
    return os.fspath(review_dir) if review_dir else REVIEW_DIR


def ensure_review_dir(review_dir=None):
    target_dir = _resolve_review_dir(review_dir)
    os.makedirs(target_dir, exist_ok=True)
    return target_dir


def _save_review_file(date_str, payload, review_dir=None):
    target_dir = ensure_review_dir(review_dir)
    file_path = os.path.join(target_dir, f"{date_str}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=_json_default)
    return file_path


def save_review_data(date, data, review_dir=None):
    date_str = _normalize_date_str(date)
    if not date_str:
        raise ValueError(f"Invalid review date: {date}")

    payload = dict(data or {})
    payload["saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload["date"] = date_str

    return _save_review_file(date_str, payload, review_dir=review_dir)


def clean_filename(filename: str) -> str:
    """
    清理文件名，移除非法字符

    Args:
        filename (str): 原始文件名

    Returns:
        str: 清理后的文件名
    """
    # 移除或替换非法字符
    illegal_chars = '<>:"/\\|?*'
    for char in illegal_chars:
        filename = filename.replace(char, "_")

    # 限制文件名长度
    if len(filename) > 100:
        filename = filename[:100]

    return filename
