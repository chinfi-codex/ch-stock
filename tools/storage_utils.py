#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
from datetime import date, datetime

import pandas as pd
from sqlalchemy import create_engine, text

REVIEW_DIR = os.path.join("datas", "reviews")
MARKET_CSV_FALLBACK = os.path.join("datas", "market_data.csv")
_ENGINE = None


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


def _get_mysql_url():
    mysql_url = os.environ.get("MYSQL_URL") or os.environ.get("mysql_url")
    if mysql_url:
        return mysql_url
    try:
        import streamlit as st
        mysql_url = st.secrets.get("mysql_url")
        if mysql_url:
            return mysql_url
    except Exception:
        pass
    return None


def _get_engine():
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    mysql_url = _get_mysql_url()
    if not mysql_url:
        raise RuntimeError("Missing MySQL config: set MYSQL_URL env or streamlit secrets mysql_url")
    _ENGINE = create_engine(mysql_url, pool_pre_ping=True)
    return _ENGINE


def _safe_add_column(conn, table, column_def):
    try:
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column_def}"))
    except Exception:
        pass


def _ensure_tables():
    engine = _get_engine()
    ddl_review = """
    CREATE TABLE IF NOT EXISTS review_daily (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        trade_date DATE NOT NULL,
        payload_json LONGTEXT NOT NULL,
        saved_at DATETIME NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_trade_date (trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    # 新版：结构化字段 + 原始 JSON
    ddl_market = """
    CREATE TABLE IF NOT EXISTS market_history (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        stat_date DATE NOT NULL,
        up_count INT NULL,
        down_count INT NULL,
        limit_up INT NULL,
        limit_down INT NULL,
        activity DECIMAL(10,4) NULL,
        turnover DECIMAL(20,4) NULL,
        raw_payload_json LONGTEXT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_stat_date (stat_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    with engine.begin() as conn:
        conn.execute(text(ddl_review))
        conn.execute(text(ddl_market))

        # 兼容旧表: payload_json -> raw_payload_json
        _safe_add_column(conn, "market_history", "up_count INT NULL")
        _safe_add_column(conn, "market_history", "down_count INT NULL")
        _safe_add_column(conn, "market_history", "limit_up INT NULL")
        _safe_add_column(conn, "market_history", "limit_down INT NULL")
        _safe_add_column(conn, "market_history", "activity DECIMAL(10,4) NULL")
        _safe_add_column(conn, "market_history", "turnover DECIMAL(20,4) NULL")
        _safe_add_column(conn, "market_history", "raw_payload_json LONGTEXT NULL")

        # 如果旧字段 payload_json 存在，迁移到 raw_payload_json（保留旧列不删除）
        try:
            conn.execute(text("""
                UPDATE market_history
                SET raw_payload_json = payload_json
                WHERE raw_payload_json IS NULL AND payload_json IS NOT NULL
            """))
        except Exception:
            pass

        # 回填结构化列（仅空值回填）
        rows = conn.execute(text("SELECT id, raw_payload_json FROM market_history WHERE raw_payload_json IS NOT NULL")).fetchall()
        for rid, raw in rows:
            try:
                d = json.loads(raw)
            except Exception:
                continue
            up_count = _to_float(d.get("上涨"))
            down_count = _to_float(d.get("下跌"))
            limit_up = _to_float(d.get("涨停"))
            limit_down = _to_float(d.get("跌停"))
            activity = _to_float(d.get("活跃度"))
            turnover = _to_float(d.get("成交额"))
            conn.execute(text("""
                UPDATE market_history
                SET up_count = COALESCE(up_count, :up_count),
                    down_count = COALESCE(down_count, :down_count),
                    limit_up = COALESCE(limit_up, :limit_up),
                    limit_down = COALESCE(limit_down, :limit_down),
                    activity = COALESCE(activity, :activity),
                    turnover = COALESCE(turnover, :turnover)
                WHERE id = :id
            """), {
                "id": rid,
                "up_count": int(up_count) if up_count is not None else None,
                "down_count": int(down_count) if down_count is not None else None,
                "limit_up": int(limit_up) if limit_up is not None else None,
                "limit_down": int(limit_down) if limit_down is not None else None,
                "activity": activity,
                "turnover": turnover,
            })


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

    try:
        _ensure_tables()
        sql = """
        INSERT INTO review_daily (trade_date, payload_json, saved_at)
        VALUES (:trade_date, :payload_json, :saved_at)
        ON DUPLICATE KEY UPDATE
          payload_json = VALUES(payload_json),
          saved_at = VALUES(saved_at);
        """
        with _get_engine().begin() as conn:
            conn.execute(text(sql), {
                "trade_date": date_str,
                "payload_json": json.dumps(payload, ensure_ascii=False, default=_json_default),
                "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
        return f"mysql://review_daily/{date_str}"
    except Exception:
        return _save_review_file(date_str, payload, review_dir=review_dir)


def load_review_data(date, review_dir=None):
    date_str = _normalize_date_str(date)
    if not date_str:
        return None

    try:
        _ensure_tables()
        with _get_engine().begin() as conn:
            row = conn.execute(text("SELECT payload_json FROM review_daily WHERE trade_date=:d LIMIT 1"), {"d": date_str}).fetchone()
        if row and row[0]:
            return json.loads(row[0])
    except Exception:
        pass

    return _load_review_file(date_str, review_dir=review_dir)


def list_review_dates(review_dir=None):
    try:
        _ensure_tables()
        with _get_engine().begin() as conn:
            rows = conn.execute(text("SELECT trade_date FROM review_daily ORDER BY trade_date DESC")).fetchall()
        if rows:
            return [r[0].strftime("%Y-%m-%d") for r in rows]
    except Exception:
        pass

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
    up_count = _to_float(d.get("上涨"))
    down_count = _to_float(d.get("下跌"))
    limit_up = _to_float(d.get("涨停"))
    limit_down = _to_float(d.get("跌停"))
    activity = _to_float(d.get("活跃度"))
    turnover = _to_float(d.get("成交额"))

    try:
        _ensure_tables()
        sql = """
        INSERT INTO market_history
        (stat_date, up_count, down_count, limit_up, limit_down, activity, turnover, raw_payload_json)
        VALUES
        (:stat_date, :up_count, :down_count, :limit_up, :limit_down, :activity, :turnover, :raw_payload_json)
        ON DUPLICATE KEY UPDATE
          up_count = VALUES(up_count),
          down_count = VALUES(down_count),
          limit_up = VALUES(limit_up),
          limit_down = VALUES(limit_down),
          activity = VALUES(activity),
          turnover = VALUES(turnover),
          raw_payload_json = VALUES(raw_payload_json);
        """
        with _get_engine().begin() as conn:
            conn.execute(text(sql), {
                "stat_date": date_str,
                "up_count": int(up_count) if up_count is not None else None,
                "down_count": int(down_count) if down_count is not None else None,
                "limit_up": int(limit_up) if limit_up is not None else None,
                "limit_down": int(limit_down) if limit_down is not None else None,
                "activity": activity,
                "turnover": turnover,
                "raw_payload_json": json.dumps(d, ensure_ascii=False, default=_json_default),
            })
        return
    except Exception:
        df = pd.DataFrame([d])
        os.makedirs(os.path.dirname(MARKET_CSV_FALLBACK), exist_ok=True)
        if os.path.exists(MARKET_CSV_FALLBACK):
            old = pd.read_csv(MARKET_CSV_FALLBACK)
            old = old.loc[old.get("日期", "") != d.get("日期")]
            df = pd.concat([old, df], ignore_index=True)
        df.to_csv(MARKET_CSV_FALLBACK, index=False)


def load_market_history_df(limit: int = 300):
    try:
        _ensure_tables()
        sql = """
        SELECT stat_date, up_count, down_count, limit_up, limit_down, activity, turnover
        FROM market_history
        ORDER BY stat_date ASC
        LIMIT :limit_n
        """
        with _get_engine().begin() as conn:
            rows = conn.execute(text(sql), {"limit_n": int(limit)}).fetchall()

        if rows:
            records = []
            for stat_date, up_count, down_count, limit_up, limit_down, activity, turnover in rows:
                records.append({
                    "日期": pd.to_datetime(stat_date),
                    "上涨": up_count,
                    "下跌": down_count,
                    "涨停": limit_up,
                    "跌停": limit_down,
                    "活跃度": activity,
                    "成交额": turnover,
                })
            return pd.DataFrame(records).sort_values("日期")
    except Exception:
        pass

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
