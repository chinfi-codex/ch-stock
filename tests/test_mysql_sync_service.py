#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

import pandas as pd

from infra import database
from services import mysql_sync_service


class _FakeTradeApi:
    def __init__(self, trade_cal_df=None, daily_basic_map=None, daily_df=None):
        self.trade_cal_df = trade_cal_df if trade_cal_df is not None else pd.DataFrame()
        self.daily_basic_map = daily_basic_map or {}
        self.daily_df = daily_df if daily_df is not None else pd.DataFrame()

    def trade_cal(self, **kwargs):
        return self.trade_cal_df.copy()

    def daily_basic(self, trade_date):
        if trade_date in self.daily_basic_map:
            return self.daily_basic_map[trade_date].copy()
        return self.daily_df.copy()

    def daily(self, **kwargs):
        return pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "trade_date": kwargs["trade_date"], "amount": 100.0, "pct_chg": 1.2},
                {"ts_code": "000002.SZ", "trade_date": kwargs["trade_date"], "amount": 200.0, "pct_chg": -0.5},
            ]
        )


def test_is_trade_day_uses_tushare_trade_cal(monkeypatch):
    fake_api = _FakeTradeApi(
        trade_cal_df=pd.DataFrame([{"cal_date": "20260402", "is_open": 1}])
    )
    monkeypatch.setattr(mysql_sync_service, "_get_trade_api", lambda: fake_api)

    assert mysql_sync_service.is_trade_day("2026-04-02") is True


def test_run_scheduled_sync_skips_non_trade_day(monkeypatch):
    captured_logs = []

    monkeypatch.setattr(
        mysql_sync_service.mysql_sync_repository,
        "init_mysql_tables",
        lambda: None,
    )
    monkeypatch.setattr(mysql_sync_service, "is_trade_day", lambda trade_date: False)
    monkeypatch.setattr(
        mysql_sync_service.mysql_sync_repository,
        "record_sync_run",
        lambda **kwargs: captured_logs.append(kwargs),
    )

    result = mysql_sync_service.run_scheduled_sync(
        only="all",
        trade_date="20260405",
        lookback_days=3,
    )

    assert result["status"] == "skipped"
    assert [item["target_table"] for item in captured_logs] == [
        mysql_sync_service.MARKET_TARGET,
        mysql_sync_service.DAILY_BASIC_TARGET,
    ]
    assert all(item["status"] == "skipped" for item in captured_logs)


def test_bootstrap_market_history_from_csv_normalizes_fields(monkeypatch, tmp_path: Path):
    csv_path = tmp_path / "market_data.csv"
    pd.DataFrame(
        [
            {
                "日期": "2026/04/02",
                "上涨": "894",
                "涨停": "31",
                "真实涨停": "29",
                "st st*涨停": "4",
                "下跌": "4237",
                "跌停": "20",
                "真实跌停": "13",
                "st st*跌停": "12",
                "平盘": "51",
                "停牌": "12",
                "活跃度": "17.21%",
                "成交额": "1857789952.382",
                "融资净买入": "12345.6",
            }
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.setattr(
        mysql_sync_service.market_history_repository,
        "get_market_history_csv_path",
        lambda: str(csv_path),
    )

    captured = {}

    def fake_upsert(records):
        captured["records"] = records
        return len(records)

    monkeypatch.setattr(
        mysql_sync_service.mysql_sync_repository,
        "upsert_market_daily_snapshots",
        fake_upsert,
    )

    saved_count = mysql_sync_service.bootstrap_market_history_from_csv()

    assert saved_count == 1
    assert captured["records"][0]["trade_date"] == "2026-04-02"
    assert captured["records"][0]["activity_rate"] == 17.21
    assert captured["records"][0]["turnover_amount"] == 1857789952.382
    assert captured["records"][0]["financing_net_buy"] == 12345.6


def test_bootstrap_daily_basic_from_sqlite_normalizes_trade_date(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "stock_daily_basic.db"
    monkeypatch.setattr(database, "DB_PATH", db_path)
    database.init_database()

    with database.get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO stock_daily_basic (
                ts_code,
                trade_date,
                close,
                turnover_rate,
                total_mv
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("000001.SZ", "20260402", 12.34, 1.23, 456789.0),
        )

    captured = {}

    def fake_upsert(records):
        captured["records"] = records
        return len(records)

    monkeypatch.setattr(
        mysql_sync_service.mysql_sync_repository,
        "upsert_stock_daily_basic_records",
        fake_upsert,
    )

    saved_count = mysql_sync_service.bootstrap_daily_basic_from_sqlite(chunk_size=1)

    assert saved_count == 1
    assert captured["records"][0]["trade_date"] == "2026-04-02"
    assert captured["records"][0]["ts_code"] == "000001.SZ"
    assert captured["records"][0]["close"] == 12.34


def test_sync_daily_basic_recent_uses_recent_trade_dates(monkeypatch):
    fake_api = _FakeTradeApi(
        trade_cal_df=pd.DataFrame(
            [
                {"cal_date": "20260401", "is_open": 1},
                {"cal_date": "20260402", "is_open": 1},
            ]
        ),
        daily_basic_map={
            "20260401": pd.DataFrame(
                [{"ts_code": "000001.SZ", "trade_date": "20260401", "close": 10.0}]
            ),
            "20260402": pd.DataFrame(
                [{"ts_code": "000002.SZ", "trade_date": "20260402", "close": 20.0}]
            ),
        },
    )
    monkeypatch.setattr(mysql_sync_service, "_get_trade_api", lambda: fake_api)

    captured = {}

    def fake_upsert(records):
        captured["records"] = records
        return len(records)

    monkeypatch.setattr(
        mysql_sync_service.mysql_sync_repository,
        "upsert_stock_daily_basic_records",
        fake_upsert,
    )

    saved_count = mysql_sync_service.sync_daily_basic_recent(
        trade_date="20260402",
        lookback_days=2,
    )

    assert saved_count == 2
    assert [item["trade_date"] for item in captured["records"]] == [
        "2026-04-01",
        "2026-04-02",
    ]
