#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
from datetime import date, datetime


def _json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass
    return str(obj)


REVIEW_DIR = os.path.join("datas", "reviews")


def _resolve_review_dir(review_dir=None):
    if review_dir:
        return os.fspath(review_dir)
    return REVIEW_DIR


def ensure_review_dir(review_dir=None):
    target_dir = _resolve_review_dir(review_dir)
    if not os.path.exists(target_dir):
        os.makedirs(target_dir, exist_ok=True)
    return target_dir


def save_review_data(date, data, review_dir=None):
    target_dir = ensure_review_dir(review_dir)

    if isinstance(date, str):
        date_str = date
    else:
        date_str = date.strftime("%Y-%m-%d")

    file_path = os.path.join(target_dir, f"{date_str}.json")
    payload = dict(data or {})
    payload["saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload["date"] = date_str

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=_json_default)

    return file_path


def load_review_data(date, review_dir=None):
    if isinstance(date, str):
        date_str = date
    else:
        date_str = date.strftime("%Y-%m-%d")

    target_dir = _resolve_review_dir(review_dir)
    file_path = os.path.join(target_dir, f"{date_str}.json")
    if not os.path.exists(file_path):
        return None

    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def list_review_dates(review_dir=None):
    target_dir = ensure_review_dir(review_dir)
    files = [f for f in os.listdir(target_dir) if f.endswith(".json")]
    dates = [f.replace(".json", "") for f in files]
    dates.sort(reverse=True)
    return dates


def df_to_dict(df):
    if df is None or df.empty:
        return None
    return df.to_dict(orient="records")


def prepare_review_data(market_data, top_stocks_stats, top_range_data, longhu_data):
    return {
        "market_overview": market_data,
        "top_100_turnover": top_stocks_stats,
        "top_100_range": top_range_data,
        "longhu": longhu_data,
    }
