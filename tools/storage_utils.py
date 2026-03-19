#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
from datetime import date, datetime

import pandas as pd

REVIEW_DIR = os.path.join("datas", "reviews")
MARKET_CSV_FALLBACK = os.path.join("datas", "market_data.csv")


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


def _to_float(value):
    if value is None:
        return None
    s = str(value).replace("%", "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return None


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


def _load_review_file(date_str, review_dir=None):
    target_dir = _resolve_review_dir(review_dir)
    file_path = os.path.join(target_dir, f"{date_str}.json")
    if not os.path.exists(file_path):
        return None
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_review_data(date, data, review_dir=None):
    date_str = _normalize_date_str(date)
    if not date_str:
        raise ValueError(f"Invalid review date: {date}")

    payload = dict(data or {})
    payload["saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload["date"] = date_str

    return _save_review_file(date_str, payload, review_dir=review_dir)


def load_review_data(date, review_dir=None):
    date_str = _normalize_date_str(date)
    if not date_str:
        return None

    return _load_review_file(date_str, review_dir=review_dir)


def list_review_dates(review_dir=None):
    target_dir = ensure_review_dir(review_dir)
    files = [f for f in os.listdir(target_dir) if f.endswith(".json")]
    dates = [f.replace(".json", "") for f in files]
    dates.sort(reverse=True)
    return dates


def upsert_market_history(stat_date, row_payload: dict):
    date_str = _normalize_date_str(stat_date)
    if not date_str:
        raise ValueError(f"Invalid market stat date: {stat_date}")

    d = row_payload or {}
    df = pd.DataFrame([d])
    os.makedirs(os.path.dirname(MARKET_CSV_FALLBACK), exist_ok=True)
    if os.path.exists(MARKET_CSV_FALLBACK):
        old = pd.read_csv(MARKET_CSV_FALLBACK)
        old = old.loc[old.get("日期", "") != d.get("日期")]
        df = pd.concat([old, df], ignore_index=True)
    df.to_csv(MARKET_CSV_FALLBACK, index=False)


def load_market_history_df(limit: int = 300):
    if os.path.exists(MARKET_CSV_FALLBACK):
        df = pd.read_csv(MARKET_CSV_FALLBACK)
        if "日期" in df.columns:
            df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
            df = df.dropna(subset=["日期"]).sort_values("日期")
        return df.tail(limit)
    return pd.DataFrame()


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
