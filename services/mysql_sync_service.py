#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MySQL 同步业务服务。
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import akshare as ak
import pandas as pd
import tushare as ts

from infra import database, market_history_repository, mysql_sync_repository
from infra.config import get_tushare_token


logger = logging.getLogger(__name__)

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
MARKET_SYNC_JOB_NAME = "scheduled_mysql_sync"
BOOTSTRAP_SYNC_JOB_NAME = "bootstrap_mysql_sync"
MARKET_TARGET = "market_daily_snapshot"
DAILY_BASIC_TARGET = "stock_daily_basic"
DAILY_BASIC_SOURCE_COLUMNS = [
    "ts_code",
    "trade_date",
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "ps",
    "ps_ttm",
    "pb",
    "pb_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "total_mv",
    "circ_mv",
]
MARKET_FIELD_MAP = {
    "上涨": "up_count",
    "涨停": "limit_up_count",
    "真实涨停": "real_limit_up_count",
    "st st*涨停": "st_limit_up_count",
    "下跌": "down_count",
    "跌停": "limit_down_count",
    "真实跌停": "real_limit_down_count",
    "st st*跌停": "st_limit_down_count",
    "平盘": "flat_count",
    "停牌": "suspended_count",
    "活跃度": "activity_rate",
    "成交额": "turnover_amount",
    "融资净买入": "financing_net_buy",
}
MARKET_INT_FIELDS = {
    "up_count",
    "limit_up_count",
    "real_limit_up_count",
    "st_limit_up_count",
    "down_count",
    "limit_down_count",
    "real_limit_down_count",
    "st_limit_down_count",
    "flat_count",
    "suspended_count",
}


def _get_trade_api():
    token = get_tushare_token()
    if not token:
        raise ValueError("未找到 Tushare Token")
    return ts.pro_api(token)


def _now_shanghai() -> datetime:
    return datetime.now(SHANGHAI_TZ)


def _normalize_trade_date(trade_date: str | datetime | pd.Timestamp) -> str:
    parsed = pd.to_datetime(trade_date, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"无法解析交易日: {trade_date}")
    return parsed.strftime("%Y%m%d")


def _normalize_trade_date_for_mysql(trade_date: str | datetime | pd.Timestamp) -> str:
    parsed = pd.to_datetime(trade_date, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"无法解析交易日: {trade_date}")
    return parsed.strftime("%Y-%m-%d")


def _to_optional_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None
    if text.endswith("%"):
        text = text[:-1]
    text = text.replace(",", "")

    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _to_optional_int(value: object) -> int | None:
    numeric = _to_optional_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _normalize_market_row(source_row: dict[str, object]) -> dict[str, object]:
    normalized = {
        "trade_date": _normalize_trade_date_for_mysql(source_row["日期"]),
        "source_row_json": source_row,
    }

    for source_name, target_name in MARKET_FIELD_MAP.items():
        raw_value = source_row.get(source_name)
        if target_name in MARKET_INT_FIELDS:
            normalized[target_name] = _to_optional_int(raw_value)
        else:
            normalized[target_name] = _to_optional_float(raw_value)

    return normalized


def _fetch_market_snapshot_from_tushare(
    trade_date: str,
) -> tuple[float | None, int | None, int | None]:
    pro = _get_trade_api()
    daily = pro.daily(
        trade_date=trade_date,
        fields="ts_code,trade_date,amount,pct_chg",
    )
    if daily is None or daily.empty:
        return None, None, None

    total_amount = None
    if "amount" in daily.columns:
        amount_series = pd.to_numeric(daily["amount"], errors="coerce")
        amount_sum = amount_series.sum(min_count=1)
        if pd.notna(amount_sum) and amount_sum > 0:
            total_amount = float(amount_sum)

    up_count = None
    down_count = None
    if "pct_chg" in daily.columns:
        pct_series = pd.to_numeric(daily["pct_chg"], errors="coerce")
        up_count = int((pct_series > 0).sum())
        down_count = int((pct_series < 0).sum())

    return total_amount, up_count, down_count


def _build_live_market_snapshot_row(expected_trade_date: str | None = None) -> dict[str, object]:
    market_data = ak.stock_market_activity_legu()
    if market_data is None or market_data.empty:
        raise ValueError("AKShare 未返回市场总览数据")

    row: dict[str, object] = {}
    stat_date_raw = None
    for _, item in market_data.iterrows():
        key = str(item.get("item", "")).strip()
        value = item.get("value")
        if not key:
            continue
        if key == "统计日期":
            stat_date_raw = value
            continue
        row[key] = value

    stat_date = _normalize_trade_date(stat_date_raw or _now_shanghai())
    if expected_trade_date and stat_date != _normalize_trade_date(expected_trade_date):
        raise ValueError(
            f"实时市场数据交易日不匹配: expected={_normalize_trade_date(expected_trade_date)}, actual={stat_date}"
        )

    total_amount, up_count, down_count = _fetch_market_snapshot_from_tushare(stat_date)
    row["日期"] = _normalize_trade_date_for_mysql(stat_date)
    if total_amount is not None:
        row["成交额"] = total_amount
    row.setdefault("上涨", up_count)
    row.setdefault("下跌", down_count)

    return row


def _load_market_history_dataframe() -> pd.DataFrame:
    csv_path = Path(market_history_repository.get_market_history_csv_path())
    if not csv_path.exists():
        return pd.DataFrame()
    return pd.read_csv(csv_path)


def _find_market_row_in_csv(trade_date: str) -> dict[str, object] | None:
    df = _load_market_history_dataframe()
    if df.empty:
        return None

    normalized_trade_date = _normalize_trade_date_for_mysql(trade_date)
    if "日期" not in df.columns:
        df.columns = ["日期"] + list(df.columns[1:])

    date_series = pd.to_datetime(df["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
    matched = df[date_series == normalized_trade_date]
    if matched.empty:
        return None

    row = matched.iloc[0].to_dict()
    row["日期"] = normalized_trade_date
    return row


def _normalize_daily_basic_record(source: dict[str, object]) -> dict[str, object]:
    normalized = {
        "ts_code": str(source.get("ts_code", "")).strip(),
        "trade_date": _normalize_trade_date_for_mysql(source["trade_date"]),
    }

    for column in DAILY_BASIC_SOURCE_COLUMNS[2:]:
        normalized[column] = _to_optional_float(source.get(column))

    return normalized


def _normalize_daily_basic_dataframe(df: pd.DataFrame) -> list[dict[str, object]]:
    if df is None or df.empty:
        return []

    records = []
    for _, row in df.iterrows():
        record = _normalize_daily_basic_record(row.to_dict())
        if record["ts_code"]:
            records.append(record)
    return records


def _get_recent_trade_dates(trade_date: str, lookback_days: int) -> list[str]:
    normalized_trade_date = _normalize_trade_date(trade_date)
    end = pd.to_datetime(normalized_trade_date)
    start = end - timedelta(days=max(lookback_days * 4, 10))
    pro = _get_trade_api()
    trade_cal = pro.trade_cal(
        exchange="SSE",
        start_date=start.strftime("%Y%m%d"),
        end_date=end.strftime("%Y%m%d"),
        fields="cal_date,is_open",
    )
    if trade_cal is None or trade_cal.empty:
        return []

    open_days = (
        trade_cal[trade_cal["is_open"] == 1]["cal_date"]
        .astype(str)
        .sort_values()
        .tolist()
    )
    return open_days[-max(1, lookback_days) :]


def is_trade_day(trade_date: str) -> bool:
    """判断指定日期是否为交易日。"""
    normalized_trade_date = _normalize_trade_date(trade_date)
    pro = _get_trade_api()
    trade_cal = pro.trade_cal(
        exchange="SSE",
        start_date=normalized_trade_date,
        end_date=normalized_trade_date,
        fields="cal_date,is_open",
    )
    if trade_cal is None or trade_cal.empty:
        return False
    return bool(int(trade_cal.iloc[-1]["is_open"]) == 1)


def bootstrap_market_history_from_csv() -> int:
    """从 CSV 全量导入市场数据。"""
    df = _load_market_history_dataframe()
    if df.empty:
        return 0

    records = []
    for _, row in df.iterrows():
        record = row.to_dict()
        record["日期"] = _normalize_trade_date_for_mysql(record["日期"])
        records.append(_normalize_market_row(record))

    return mysql_sync_repository.upsert_market_daily_snapshots(records)


def bootstrap_daily_basic_from_sqlite(chunk_size: int = 2000) -> int:
    """从本地 SQLite 全量导入 daily_basic。"""
    sqlite_path = database.get_db_path()
    if not sqlite_path.exists():
        return 0

    total = 0
    with sqlite3.connect(sqlite_path) as conn:
        query = f"""
            SELECT {", ".join(DAILY_BASIC_SOURCE_COLUMNS)}
            FROM stock_daily_basic
            ORDER BY trade_date, ts_code
        """
        for chunk in pd.read_sql_query(query, conn, chunksize=chunk_size):
            records = _normalize_daily_basic_dataframe(chunk)
            if not records:
                continue
            total += mysql_sync_repository.upsert_stock_daily_basic_records(records)

    return total


def sync_market_snapshot_incremental(trade_date: str | None = None) -> int:
    """同步单日市场快照。"""
    normalized_trade_date = _normalize_trade_date(trade_date or _now_shanghai())
    today_trade_date = _now_shanghai().strftime("%Y%m%d")

    if normalized_trade_date == today_trade_date:
        raw_row = _build_live_market_snapshot_row(normalized_trade_date)
    else:
        raw_row = _find_market_row_in_csv(normalized_trade_date)
        if raw_row is None:
            raise ValueError(f"CSV 中未找到市场快照: {normalized_trade_date}")

    return mysql_sync_repository.upsert_market_daily_snapshots(
        [_normalize_market_row(raw_row)]
    )


def sync_daily_basic_recent(trade_date: str | None = None, lookback_days: int = 3) -> int:
    """同步最近 N 个交易日的 daily_basic。"""
    normalized_trade_date = _normalize_trade_date(trade_date or _now_shanghai())
    trade_dates = _get_recent_trade_dates(normalized_trade_date, lookback_days)
    if not trade_dates:
        return 0

    pro = _get_trade_api()
    records: list[dict[str, object]] = []
    for item in trade_dates:
        df = pro.daily_basic(trade_date=item)
        records.extend(_normalize_daily_basic_dataframe(df))

    if not records:
        return 0
    return mysql_sync_repository.upsert_stock_daily_basic_records(records)


def _log_sync_result(
    *,
    job_name: str,
    mode: str,
    target_table: str,
    trade_date: str,
    status: str,
    row_count: int,
    started_at: datetime,
    error_message: str | None = None,
    source_range: str | None = None,
) -> None:
    mysql_sync_repository.record_sync_run(
        job_name=job_name,
        mode=mode,
        target_table=target_table,
        trade_date=_normalize_trade_date_for_mysql(trade_date),
        source_range=source_range,
        row_count=row_count,
        status=status,
        error_message=error_message,
        started_at=started_at.replace(tzinfo=None),
        completed_at=_now_shanghai().replace(tzinfo=None),
    )


def run_bootstrap_sync(only: str = "all") -> dict[str, object]:
    """执行全量初始化导入。"""
    mysql_sync_repository.init_mysql_tables()
    results: dict[str, object] = {"mode": "bootstrap", "results": {}}

    if only in {"all", "market"}:
        started_at = _now_shanghai()
        try:
            row_count = bootstrap_market_history_from_csv()
            _log_sync_result(
                job_name=BOOTSTRAP_SYNC_JOB_NAME,
                mode="bootstrap",
                target_table=MARKET_TARGET,
                trade_date=_now_shanghai().strftime("%Y%m%d"),
                source_range="csv:market_data.csv",
                status="success",
                row_count=row_count,
                started_at=started_at,
            )
            results["results"][MARKET_TARGET] = {"status": "success", "row_count": row_count}
        except Exception as exc:
            _log_sync_result(
                job_name=BOOTSTRAP_SYNC_JOB_NAME,
                mode="bootstrap",
                target_table=MARKET_TARGET,
                trade_date=_now_shanghai().strftime("%Y%m%d"),
                source_range="csv:market_data.csv",
                status="failed",
                row_count=0,
                started_at=started_at,
                error_message=str(exc),
            )
            raise

    if only in {"all", "daily_basic"}:
        started_at = _now_shanghai()
        try:
            row_count = bootstrap_daily_basic_from_sqlite()
            _log_sync_result(
                job_name=BOOTSTRAP_SYNC_JOB_NAME,
                mode="bootstrap",
                target_table=DAILY_BASIC_TARGET,
                trade_date=_now_shanghai().strftime("%Y%m%d"),
                source_range=f"sqlite:{database.get_db_path().name}",
                status="success",
                row_count=row_count,
                started_at=started_at,
            )
            results["results"][DAILY_BASIC_TARGET] = {"status": "success", "row_count": row_count}
        except Exception as exc:
            _log_sync_result(
                job_name=BOOTSTRAP_SYNC_JOB_NAME,
                mode="bootstrap",
                target_table=DAILY_BASIC_TARGET,
                trade_date=_now_shanghai().strftime("%Y%m%d"),
                source_range=f"sqlite:{database.get_db_path().name}",
                status="failed",
                row_count=0,
                started_at=started_at,
                error_message=str(exc),
            )
            raise

    return results


def run_scheduled_sync(
    *,
    only: str = "all",
    trade_date: str | None = None,
    lookback_days: int = 3,
) -> dict[str, object]:
    """执行定时同步。"""
    normalized_trade_date = _normalize_trade_date(trade_date or _now_shanghai())
    mysql_sync_repository.init_mysql_tables()

    target_tables = []
    if only in {"all", "market"}:
        target_tables.append(MARKET_TARGET)
    if only in {"all", "daily_basic"}:
        target_tables.append(DAILY_BASIC_TARGET)

    if not is_trade_day(normalized_trade_date):
        for target_table in target_tables:
            _log_sync_result(
                job_name=MARKET_SYNC_JOB_NAME,
                mode="scheduled",
                target_table=target_table,
                trade_date=normalized_trade_date,
                source_range=normalized_trade_date,
                status="skipped",
                row_count=0,
                started_at=_now_shanghai(),
                error_message="非交易日，跳过同步",
            )
        return {
            "mode": "scheduled",
            "trade_date": normalized_trade_date,
            "status": "skipped",
            "results": {},
        }

    results: dict[str, object] = {
        "mode": "scheduled",
        "trade_date": normalized_trade_date,
        "status": "success",
        "results": {},
    }

    if only in {"all", "market"}:
        started_at = _now_shanghai()
        try:
            row_count = sync_market_snapshot_incremental(normalized_trade_date)
            _log_sync_result(
                job_name=MARKET_SYNC_JOB_NAME,
                mode="scheduled",
                target_table=MARKET_TARGET,
                trade_date=normalized_trade_date,
                source_range=normalized_trade_date,
                status="success",
                row_count=row_count,
                started_at=started_at,
            )
            results["results"][MARKET_TARGET] = {"status": "success", "row_count": row_count}
        except Exception as exc:
            results["status"] = "partial_failed"
            _log_sync_result(
                job_name=MARKET_SYNC_JOB_NAME,
                mode="scheduled",
                target_table=MARKET_TARGET,
                trade_date=normalized_trade_date,
                source_range=normalized_trade_date,
                status="failed",
                row_count=0,
                started_at=started_at,
                error_message=str(exc),
            )
            results["results"][MARKET_TARGET] = {"status": "failed", "error": str(exc)}

    if only in {"all", "daily_basic"}:
        started_at = _now_shanghai()
        source_range = json.dumps(
            {
                "trade_date": normalized_trade_date,
                "lookback_days": lookback_days,
            },
            ensure_ascii=False,
        )
        try:
            row_count = sync_daily_basic_recent(
                trade_date=normalized_trade_date,
                lookback_days=lookback_days,
            )
            _log_sync_result(
                job_name=MARKET_SYNC_JOB_NAME,
                mode="scheduled",
                target_table=DAILY_BASIC_TARGET,
                trade_date=normalized_trade_date,
                source_range=source_range,
                status="success",
                row_count=row_count,
                started_at=started_at,
            )
            results["results"][DAILY_BASIC_TARGET] = {"status": "success", "row_count": row_count}
        except Exception as exc:
            results["status"] = "partial_failed"
            _log_sync_result(
                job_name=MARKET_SYNC_JOB_NAME,
                mode="scheduled",
                target_table=DAILY_BASIC_TARGET,
                trade_date=normalized_trade_date,
                source_range=source_range,
                status="failed",
                row_count=0,
                started_at=started_at,
                error_message=str(exc),
            )
            results["results"][DAILY_BASIC_TARGET] = {"status": "failed", "error": str(exc)}

    return results
