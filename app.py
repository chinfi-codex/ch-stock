#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import streamlit as st
import akshare as ak
import datetime
import numpy as np
import plotly.graph_objects as go
from tools import (
    plotK,
    get_market_data,
    get_all_stocks,
    get_financing_net_buy_series,
    get_gem_pe_series,
)
from tools.financial_data import EconomicIndicators
from tools.storage_utils import save_review_data, load_review_data
from data_sources import (
    _normalize_top_stocks_df,
    _df_to_records,
    _records_to_df,
    _build_pct_distribution,
    get_benchmark_kline,
)



def _section_title(title):
    st.markdown(
        f"<div style='font-size:26px;font-weight:700;margin:8px 0 8px 0;'>{title}</div>",
        unsafe_allow_html=True,
    )


def _latest_metric_from_df(df, value_col, date_col="date"):
    if df is None or df.empty or value_col not in df.columns:
        return None
    view = df.copy()
    if date_col in view.columns:
        view[date_col] = pd.to_datetime(view[date_col], errors="coerce")
    view[value_col] = pd.to_numeric(view[value_col], errors="coerce")
    view = view.dropna(subset=[value_col])
    if date_col in view.columns:
        view = view.dropna(subset=[date_col]).sort_values(date_col, ascending=False)
    if view.empty:
        return None
    latest = view.iloc[0]
    prev_value = None
    if len(view) > 1:
        prev_value = view.iloc[1][value_col]
    return {
        "date": latest[date_col] if date_col in view.columns else None,
        "value": float(latest[value_col]),
        "prev_value": float(prev_value) if prev_value is not None and not pd.isna(prev_value) else None,
    }


def _calc_pct_change(current, previous):
    if current is None or previous is None or previous == 0:
        return None
    return (current / previous - 1) * 100


def _series_from_df(df, value_col, days):
    if df is None or df.empty or value_col not in df.columns:
        return []
    view = df.copy()
    if "date" in view.columns:
        view["date"] = pd.to_datetime(view["date"], errors="coerce")
    view[value_col] = pd.to_numeric(view[value_col], errors="coerce")
    view = view.dropna(subset=[value_col])
    if "date" in view.columns:
        view = view.dropna(subset=["date"]).sort_values("date")
    if view.empty:
        return []
    view = view.tail(int(days))
    if "date" in view.columns:
        view["date"] = view["date"].dt.strftime("%Y-%m-%d")
    view = view[["date", value_col]].rename(columns={value_col: "value"})
    return view.to_dict(orient="records")


def build_external_section(days=120):
    btc_metric = None
    us10y_metric = None
    xau_metric = None

    btc_series = []
    us10y_series = []
    xau_series = []

    fetch_len = max(int(days * 2), 60)

    try:
        btc_df = EconomicIndicators.get_crypto_daily(symbol="BTC", market="USD", curDate=fetch_len)
        btc_metric = _latest_metric_from_df(btc_df, "close")
        btc_series = _series_from_df(btc_df, "close", days)
    except Exception:
        btc_metric = None

    try:
        us10y_df = EconomicIndicators.get_treasury_yield(maturity="10year", interval="daily", curDate=fetch_len)
        us10y_metric = _latest_metric_from_df(us10y_df, "value")
        us10y_series = _series_from_df(us10y_df, "value", days)
    except Exception:
        us10y_metric = None

    try:
        xau_df = EconomicIndicators.get_gold_silver_history(symbol="XAU", interval="daily", curDate=fetch_len)
        xau_metric = _latest_metric_from_df(xau_df, "value")
        xau_series = _series_from_df(xau_df, "value", days)
    except Exception:
        xau_metric = None

    def _attach_change(metric, change_kind, series):
        if not metric:
            return None
        current = metric.get("value")
        prev = metric.get("prev_value")
        change = None
        if change_kind == "pct":
            change = _calc_pct_change(current, prev)
        elif change_kind == "bp":
            if prev is not None:
                change = (current - prev) * 100
        return {
            "date": metric.get("date"),
            "value": current,
            "prev_value": prev,
            "change": change,
            "series": series,
        }

    return {
        "btc": _attach_change(btc_metric, "pct", btc_series),
        "us10y": _attach_change(us10y_metric, "bp", us10y_series),
        "xau": _attach_change(xau_metric, "pct", xau_series),
    }


def _to_iso_date(value):
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def _query_mysql(sql, params=None):
    try:
        from database.db_manager import get_db
    except Exception:
        return []
    try:
        with get_db() as db:
            return db.query(sql, params or ())
    except Exception:
        return []


def _query_one_mysql(sql, params=None):
    rows = _query_mysql(sql, params)
    if not rows:
        return None
    return rows[0]


def _load_external_asset_series_from_mysql(asset_code, select_date, days=120):
    target_date = _to_iso_date(select_date)
    if not target_date:
        return pd.DataFrame()
    safe_limit = max(int(days) * 3, 90)
    rows = _query_mysql(
        f"""
        SELECT trade_date AS date, close_price AS value
        FROM external_asset_daily
        WHERE asset_code = %s AND trade_date <= %s
        ORDER BY trade_date DESC
        LIMIT {safe_limit}
        """,
        (asset_code, target_date),
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"]).sort_values("date")
    return df


def _build_external_metric_from_df(df, change_kind="pct", days=120):
    metric = _latest_metric_from_df(df, "value")
    series = _series_from_df(df, "value", days)
    if not metric:
        return None
    current = metric.get("value")
    prev = metric.get("prev_value")
    if change_kind == "bp":
        change = (current - prev) * 100 if prev is not None else None
    else:
        change = _calc_pct_change(current, prev)
    return {
        "date": metric.get("date"),
        "value": current,
        "prev_value": prev,
        "change": change,
        "series": series,
    }


def build_external_section_from_mysql(select_date, days=120):
    btc_df = _load_external_asset_series_from_mysql("BTCUSD", select_date, days=days)
    xau_df = _load_external_asset_series_from_mysql("XAUUSD", select_date, days=days)
    us10y_df = _load_external_asset_series_from_mysql("US10Y", select_date, days=days)
    return {
        "btc": _build_external_metric_from_df(btc_df, "pct", days=days),
        "us10y": _build_external_metric_from_df(us10y_df, "bp", days=days),
        "xau": _build_external_metric_from_df(xau_df, "pct", days=days),
    }


def _load_realtime_indices(select_date):
    end_date = _to_iso_date(select_date) or datetime.datetime.now().strftime("%Y-%m-%d")
    start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=420)).strftime("%Y-%m-%d")
    sh_df = get_benchmark_kline(start_date=start_date, end_date=end_date, symbol="sh000001")
    cyb_df = get_benchmark_kline(start_date=start_date, end_date=end_date, symbol="sz399006")
    kcb_df = get_benchmark_kline(start_date=start_date, end_date=end_date, symbol="sh000688")
    return {
        "sh_df": _df_to_records(sh_df),
        "cyb_df": _df_to_records(cyb_df),
        "kcb_df": _df_to_records(kcb_df),
    }


def _load_range_distribution_from_mysql(select_date):
    target_date = _to_iso_date(select_date)
    if not target_date:
        return []
    date_row = _query_one_mysql(
        """
        SELECT MAX(trade_date) AS trade_date
        FROM stock_daily_basic
        WHERE trade_date <= %s
        """,
        (target_date,),
    )
    if not date_row or not date_row.get("trade_date"):
        return []
    stat_date = pd.to_datetime(date_row.get("trade_date"), errors="coerce")
    if pd.isna(stat_date):
        return []
    rows = _query_mysql(
        """
        SELECT sdb.pct_change AS pct
        FROM stock_daily_basic sdb
        LEFT JOIN stock_master sm ON sdb.ts_code = sm.ts_code
        WHERE sdb.trade_date = %s
          AND COALESCE(sm.is_st, 0) = 0
          AND COALESCE(sm.is_delist, 0) = 0
          AND COALESCE(sm.is_bse, 0) = 0
          AND (sm.symbol IS NULL OR (sm.symbol NOT LIKE '8%%' AND sm.symbol NOT LIKE '4%%'))
        """,
        (stat_date.strftime("%Y-%m-%d"),),
    )
    if not rows:
        return []
    df = pd.DataFrame(rows)
    if df.empty:
        return []
    df["pct"] = pd.to_numeric(df["pct"], errors="coerce")
    df = df.dropna(subset=["pct"])
    if df.empty:
        return []
    return _build_pct_distribution(df)


def _load_market_overview_from_mysql(select_date):
    target_date = _to_iso_date(select_date)
    if not target_date:
        return {
            "上涨": None,
            "下跌": None,
            "涨停": None,
            "跌停": None,
            "活跃度": None,
            "range_distribution": [],
        }
    row = _query_one_mysql(
        """
        SELECT trade_date, up_count, down_count, zt_count, dt_count, activity_index
        FROM market_activity_daily
        WHERE trade_date <= %s
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        (target_date,),
    )
    if not row:
        return {
            "上涨": None,
            "下跌": None,
            "涨停": None,
            "跌停": None,
            "活跃度": None,
            "range_distribution": _load_range_distribution_from_mysql(select_date),
        }
    return {
        "上涨": row.get("up_count"),
        "下跌": row.get("down_count"),
        "涨停": row.get("zt_count"),
        "跌停": row.get("dt_count"),
        "活跃度": row.get("activity_index"),
        "range_distribution": _load_range_distribution_from_mysql(select_date),
    }


def _resolve_group_trade_date(select_date, group_type):
    target_date = _to_iso_date(select_date)
    if not target_date:
        return None
    row = _query_one_mysql(
        """
        SELECT MAX(trade_date) AS trade_date
        FROM stock_group_member
        WHERE trade_date <= %s AND group_type = %s
        """,
        (target_date, group_type),
    )
    if not row:
        return None
    trade_date = pd.to_datetime(row.get("trade_date"), errors="coerce")
    if pd.isna(trade_date):
        return None
    return trade_date.strftime("%Y-%m-%d")


def _load_group_records_from_mysql(select_date, group_type):
    trade_date = _resolve_group_trade_date(select_date, group_type)
    if not trade_date:
        return []
    rows = _query_mysql(
        """
        SELECT rank_no, ts_code, symbol, name, pct_change, amount, total_mv
        FROM stock_group_member
        WHERE trade_date = %s AND group_type = %s
        ORDER BY rank_no ASC
        """,
        (trade_date, group_type),
    )
    if not rows:
        return []
    records = []
    for row in rows:
        code = str(row.get("symbol") or "").strip()
        if not code:
            ts_code = str(row.get("ts_code") or "")
            code = ts_code.split(".")[0] if ts_code else ""

        amount = pd.to_numeric(row.get("amount"), errors="coerce")
        total_mv = pd.to_numeric(row.get("total_mv"), errors="coerce")
        pct = pd.to_numeric(row.get("pct_change"), errors="coerce")

        records.append(
            {
                "code": code,
                "name": str(row.get("name") or ""),
                "pct": None if pd.isna(pct) else float(pct),
                # stock_group_member: amount(千元), total_mv(万元)
                "amount": None if pd.isna(amount) else float(amount) * 1000,
                "mkt_cap": None if pd.isna(total_mv) else float(total_mv) * 10000,
            }
        )
    return records


def _build_top100_range_from_gainers(gainers_records):
    if not gainers_records:
        return {"sh_stocks": [], "cyb_kcb_stocks": []}
    df = pd.DataFrame(gainers_records)
    if df.empty or not {"code", "name", "pct", "amount"}.issubset(df.columns):
        return {"sh_stocks": [], "cyb_kcb_stocks": []}
    df["pct"] = pd.to_numeric(df["pct"], errors="coerce")
    df = df.dropna(subset=["pct"]).sort_values("pct", ascending=False).head(100)
    code6 = df["code"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("")
    sh_df = df[
        (code6.str.startswith("6") | code6.str.startswith("0"))
        & ~code6.str.startswith("688")
    ]
    cyb_kcb_df = df[code6.str.startswith("3") | code6.str.startswith("688")]
    return {
        "sh_stocks": sh_df[["name", "code", "pct", "amount"]].to_dict(orient="records"),
        "cyb_kcb_stocks": cyb_kcb_df[["name", "code", "pct", "amount"]].to_dict(orient="records"),
    }


def build_top100_section_from_mysql(select_date):
    turnover_records = _load_group_records_from_mysql(select_date, "top_100_turnover")
    gainers_records = _load_group_records_from_mysql(select_date, "top_100_gainers")
    losers_records = _load_group_records_from_mysql(select_date, "top_100_losers")
    top_100_range = _build_top100_range_from_gainers(gainers_records)
    return {
        "top_100_turnover": turnover_records,
        "top_100_range": top_100_range,
        "top_100_gainers": gainers_records[:100],
        "top_100_losers": losers_records[:100],
    }


def _load_financing_series_from_mysql(select_date, limit=60):
    target_date = _to_iso_date(select_date)
    if not target_date:
        return []
    safe_limit = max(1, int(limit))
    rows = _query_mysql(
        f"""
        SELECT trade_date AS date, rz_net_buy AS value
        FROM margin_trade_daily
        WHERE trade_date <= %s
        ORDER BY trade_date DESC
        LIMIT {safe_limit}
        """,
        (target_date,),
    )
    if not rows:
        return []
    df = pd.DataFrame(rows)
    if df.empty:
        return []
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"]).sort_values("date")
    if df.empty:
        return []
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df.rename(columns={"value": "融资净买入"})[["date", "融资净买入"]].to_dict(orient="records")


def _load_gem_pe_series_from_mysql(select_date, limit=500):
    target_date = _to_iso_date(select_date)
    if not target_date:
        return []
    safe_limit = max(1, int(limit))
    rows = _query_mysql(
        f"""
        SELECT trade_date AS date, pe_value AS value
        FROM gem_pe_daily
        WHERE trade_date <= %s
        ORDER BY trade_date DESC
        LIMIT {safe_limit}
        """,
        (target_date,),
    )
    if not rows:
        return []
    df = pd.DataFrame(rows)
    if df.empty:
        return []
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"]).sort_values("date")
    if df.empty:
        return []
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df.rename(columns={"value": "市盈率"})[["date", "市盈率"]].to_dict(orient="records")



def _find_market_value_by_keywords(market_data, keywords, default=None):
    if market_data is None or market_data.empty:
        return default
    if not {"item", "value"}.issubset(set(market_data.columns)):
        return default
    for _, row in market_data.iterrows():
        item = str(row.get("item", ""))
        if any(k in item for k in keywords):
            return row.get("value")
    return default


def _get_market_snapshot_from_mysql(target_date=None):
    try:
        from database.db_manager import get_db
    except Exception:
        return None

    try:
        with get_db() as db:
            if target_date is not None:
                dt = pd.to_datetime(target_date, errors="coerce")
                if pd.isna(dt):
                    return None
                return db.query_one(
                    """
                    SELECT trade_date, up_count, down_count, zt_count, dt_count, activity_index
                    FROM market_activity_daily
                    WHERE trade_date = %s
                    LIMIT 1
                    """,
                    (dt.strftime("%Y-%m-%d"),),
                )
            return db.query_one(
                """
                SELECT trade_date, up_count, down_count, zt_count, dt_count, activity_index
                FROM market_activity_daily
                ORDER BY trade_date DESC
                LIMIT 1
                """
            )
    except Exception:
        return None


def _load_market_history_from_mysql(limit=30):
    try:
        from database.db_manager import get_db
    except Exception:
        return pd.DataFrame()

    try:
        safe_limit = max(1, int(limit))
        with get_db() as db:
            rows = db.query(
                f"""
                SELECT
                    trade_date AS 日期,
                    up_count AS 上涨,
                    down_count AS 下跌,
                    zt_count AS 涨停,
                    dt_count AS 跌停,
                    activity_index AS 活跃度,
                    total_amount AS 成交额
                FROM market_activity_daily
                ORDER BY trade_date DESC
                LIMIT {safe_limit}
                """
            )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
        df = df.dropna(subset=["日期"]).sort_values("日期")
        return df
    except Exception:
        return pd.DataFrame()


def build_market_section(select_date, all_stocks_df=None):
    sh_df, cyb_df, kcb_df, market_data = get_market_data()
    mysql_snapshot = _get_market_snapshot_from_mysql(select_date)
    if mysql_snapshot:
        up_stocks = mysql_snapshot.get("up_count")
        down_stocks = mysql_snapshot.get("down_count")
        limit_up = mysql_snapshot.get("zt_count")
        limit_down = mysql_snapshot.get("dt_count")
        activity = mysql_snapshot.get("activity_index")
    else:
        up_stocks = _find_market_value_by_keywords(market_data, ["上涨"])
        down_stocks = _find_market_value_by_keywords(market_data, ["下跌"])
        limit_up = _find_market_value_by_keywords(market_data, ["涨停"])
        limit_down = _find_market_value_by_keywords(market_data, ["跌停"])
        activity = _find_market_value_by_keywords(market_data, ["活跃", "情绪"])
    if isinstance(activity, str) and '%' in str(activity):
        activity = str(activity).replace('%', '')

    if all_stocks_df is None:
        try:
            all_stocks_df = get_all_stocks(select_date)
        except Exception:
            all_stocks_df = None

    market_overview = {
        "上涨": up_stocks,
        "下跌": down_stocks,
        "涨停": limit_up,
        "跌停": limit_down,
        "活跃度": activity,
        "range_distribution": _build_pct_distribution(all_stocks_df),
    }
    indices = {
        'sh_df': _df_to_records(sh_df),
        'cyb_df': _df_to_records(cyb_df),
        'kcb_df': _df_to_records(kcb_df)
    }
    return {'indices': indices, 'market_overview': market_overview}, all_stocks_df


def build_top100_section(select_date, all_stocks_df=None):
    source_df = all_stocks_df if all_stocks_df is not None else get_all_stocks(select_date)
    today_top_stocks = _normalize_top_stocks_df(source_df)

    if not today_top_stocks.empty:
        cols = list(today_top_stocks.columns)
        if len(cols) >= 4:
            rename_pos = {cols[0]: 'name', cols[1]: 'code', cols[2]: 'pct', cols[3]: 'amount'}
            today_top_stocks = today_top_stocks.rename(columns=rename_pos)

    turnover_records = []
    if not today_top_stocks.empty and {'pct', 'amount', 'name', 'code'}.issubset(today_top_stocks.columns):
        top_100_by_turnover = today_top_stocks.sort_values('amount', ascending=False).head(100)
        top_100_by_turnover['pct'] = pd.to_numeric(top_100_by_turnover['pct'], errors='coerce')
        turnover_records = top_100_by_turnover[['name', 'code', 'pct', 'amount']].to_dict(orient='records')

    range_data = {'sh_stocks': [], 'cyb_kcb_stocks': []}
    if not today_top_stocks.empty and {'pct', 'amount', 'name', 'code'}.issubset(today_top_stocks.columns):
        top_100_by_range = today_top_stocks.sort_values('pct', ascending=False).head(100)
        code_series = top_100_by_range['code'].astype(str)
        sh_df = top_100_by_range[
            (code_series.str[2:].str.startswith('6') | code_series.str[2:].str.startswith('0')) & ~code_series.str[2:].str.startswith('688')
        ]
        cyb_kcb_df = top_100_by_range[
            code_series.str[2:].str.startswith('3') |
            code_series.str[2:].str.startswith('688')
        ]
        range_data = {
            'sh_stocks': sh_df[['name', 'code', 'pct', 'amount']].to_dict(orient='records'),
            'cyb_kcb_stocks': cyb_kcb_df[['name', 'code', 'pct', 'amount']].to_dict(orient='records')
        }

    return {'top_100_turnover': turnover_records, 'top_100_range': range_data}, source_df

def is_review_data_complete(review_data):
    if not review_data:
        return False
    indices = review_data.get('indices') or {}
    if not indices:
        return False
    if not indices.get('sh_df') or not indices.get('cyb_df') or not indices.get('kcb_df'):
        return False
    if not review_data.get('market_overview'):
        return False
    if not review_data.get('top_100_turnover'):
        return False
    top_100_range = review_data.get('top_100_range') or {}
    if not top_100_range.get('sh_stocks') and not top_100_range.get('cyb_kcb_stocks'):
        return False
    return True


def build_review_data(select_date, show_modules=None):
    review_data = {'date': select_date.strftime('%Y-%m-%d')}

    def _should(key):
        return show_modules is None or show_modules.get(key, True)

    all_stocks_df = None

    if _should("external"):
        review_data["external"] = build_external_section()
    else:
        review_data["external"] = {}

    if _should("market"):
        market_section, all_stocks_df = build_market_section(select_date, all_stocks_df)
        review_data.update(market_section)
        review_data["financing_series"] = []
        review_data["gem_pe_series"] = []
    else:
        review_data['indices'] = {'sh_df': [], 'cyb_df': [], 'kcb_df': []}
        review_data['market_overview'] = {}
        review_data["financing_series"] = []
        review_data["gem_pe_series"] = []

    if _should("top100"):
        top_section, all_stocks_df = build_top100_section(select_date, all_stocks_df)
        review_data.update(top_section)

        gainers = []
        losers = []
        if all_stocks_df is not None and not all_stocks_df.empty and "pct" in all_stocks_df.columns:
            view = all_stocks_df.copy()
            view["pct"] = pd.to_numeric(view["pct"], errors="coerce")
            if "amount" in view.columns:
                view["amount"] = pd.to_numeric(view["amount"], errors="coerce")
            if "mkt_cap" in view.columns:
                view["mkt_cap"] = pd.to_numeric(view["mkt_cap"], errors="coerce")
            if "code" in view.columns:
                view["code"] = view["code"].astype(str)
            if "name" in view.columns:
                view["name"] = view["name"].astype(str)

            view = view.dropna(subset=[col for col in ["pct", "code", "name"] if col in view.columns])

            # 去除 ST + 北交（4/8开头或 bj 前缀）
            if {"code", "name"}.issubset(view.columns):
                code_str = view["code"].astype(str).str.lower().str.strip()
                code6 = code_str.str.extract(r"(\d{6})", expand=False).fillna("")
                is_bj = code_str.str.startswith("bj") | code6.str.startswith(("4", "8"))
                is_st = view["name"].astype(str).str.upper().str.contains("ST", na=False)
                view = view[~is_bj & ~is_st]

            gainers_df = view.sort_values("pct", ascending=False).head(100)
            losers_df = view.sort_values("pct", ascending=True).head(100)
            keep_cols = [col for col in ["code", "name", "pct", "amount", "mkt_cap"] if col in view.columns]
            if keep_cols:
                gainers = gainers_df[keep_cols].to_dict(orient="records")
                losers = losers_df[keep_cols].to_dict(orient="records")
        review_data["top_100_gainers"] = gainers
        review_data["top_100_losers"] = losers
    else:
        review_data['top_100_turnover'] = [] 
        review_data['top_100_range'] = {'sh_stocks': [], 'cyb_kcb_stocks': []}
        review_data["top_100_gainers"] = []
        review_data["top_100_losers"] = []

    return review_data


def build_review_data_from_mysql(select_date, show_modules=None):
    review_data = {"date": select_date.strftime("%Y-%m-%d")}

    def _should(key):
        return show_modules is None or show_modules.get(key, True)

    if _should("external"):
        review_data["external"] = build_external_section_from_mysql(select_date)
    else:
        review_data["external"] = {}

    if _should("market"):
        review_data["indices"] = _load_realtime_indices(select_date)
        review_data["market_overview"] = _load_market_overview_from_mysql(select_date)
        # 融资净买入和创业板PE前端统一走实时，不从MySQL读取
        review_data["financing_series"] = []
        review_data["gem_pe_series"] = []
    else:
        review_data["indices"] = {"sh_df": [], "cyb_df": [], "kcb_df": []}
        review_data["market_overview"] = {}
        review_data["financing_series"] = []
        review_data["gem_pe_series"] = []

    if _should("top100"):
        review_data.update(build_top100_section_from_mysql(select_date))
    else:
        review_data["top_100_turnover"] = []
        review_data["top_100_range"] = {"sh_stocks": [], "cyb_kcb_stocks": []}
        review_data["top_100_gainers"] = []
        review_data["top_100_losers"] = []

    return review_data

def display_review_data(review_data, show_modules=None):
    show_modules = show_modules or {}
    date_str = review_data.get('date')
    if isinstance(date_str, (datetime.date, datetime.datetime)):
        zt_date = date_str.strftime('%Y%m%d')
    elif isinstance(date_str, str) and date_str:
        zt_date = date_str.replace('-', '')
    else:
        zt_date = datetime.datetime.now().strftime('%Y%m%d')
    def _show(key):
        return show_modules.get(key, True)
    if _show("external"):
        _section_title("\u5916\u56f4\u6307\u6807")
        external = review_data.get("external") or {}

        def _format_date(value):
            if isinstance(value, (pd.Timestamp, datetime.date, datetime.datetime)):
                return value.strftime("%Y-%m-%d")
            if isinstance(value, str):
                return value
            return ""

        def _format_value(value, fmt, default="\u2014"):
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return default
            return fmt.format(value)

        def _format_delta(value, fmt):
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            return fmt.format(value)

        btc = external.get("btc") or {}
        us10y = external.get("us10y") or {}
        xau = external.get("xau") or {}

        def _render_sparkline(series, color):
            if not series:
                return
            df = pd.DataFrame(series)
            if df.empty or "date" not in df.columns or "value" not in df.columns:
                return
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna(subset=["date", "value"])
            if df.empty:
                return
            fig = go.Figure(go.Scatter(
                x=df["date"],
                y=df["value"],
                mode="lines",
                line=dict(color=color, width=2),
            ))
            fig.update_layout(
                height=120,
                width=220,
                margin=dict(l=6, r=6, t=6, b=6),
                xaxis=dict(visible=False),
                yaxis=dict(visible=False),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=False)

        cols = st.columns(3)
        with cols[0]:
            st.metric(
                "\u6bd4\u7279\u5e01 (BTC/USD)",
                _format_value(btc.get("value"), "${:,.0f}"),
                _format_delta(btc.get("change"), "{:+.2f}%"),
            )
            btc_date = _format_date(btc.get("date"))
            if btc_date:
                st.caption(f"\u65e5\u671f: {btc_date}")
            _render_sparkline(btc.get("series") or [], "#f39c12")
        with cols[1]:
            st.metric(
                "XAU 金价 (USD/oz)",
                _format_value(xau.get("value"), "{:,.2f}"),
                _format_delta(xau.get("change"), "{:+.2f}%"),
            )
            xau_date = _format_date(xau.get("date"))
            if xau_date:
                st.caption(f"\u65e5\u671f: {xau_date}")
            _render_sparkline(xau.get("series") or [], "#d4af37")
        with cols[2]:
            st.metric(
                "\u7f8e\u56fd10Y\u56fd\u503a\u6536\u76ca\u7387",
                _format_value(us10y.get("value"), "{:.2f}%"),
                _format_delta(us10y.get("change"), "{:+.1f}bp"),
            )
            us10y_date = _format_date(us10y.get("date"))
            if us10y_date:
                st.caption(f"\u65e5\u671f: {us10y_date}")
            _render_sparkline(us10y.get("series") or [], "#2f80ed")
        st.markdown("---")

    if _show("market"):
        _section_title("今日大盘")
        indices = review_data.get('indices', {})
        sh_df = _records_to_df(indices.get('sh_df', []))
        cyb_df = _records_to_df(indices.get('cyb_df', []))
        kcb_df = _records_to_df(indices.get('kcb_df', []))

        market_overview = review_data.get('market_overview', {})
        up_stocks = market_overview.get('上涨')
        down_stocks = market_overview.get('下跌')
        limit_up = market_overview.get('涨停')
        limit_down = market_overview.get('跌停')
        activity = market_overview.get('活跃度')

        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            st.markdown("上证指数")
            if not sh_df.empty:
                plotK(sh_df)
        with col2:
            st.markdown("创业板指数")
            if not cyb_df.empty:
                plotK(cyb_df)
        with col3:
            st.markdown("科创板指数")
            if not kcb_df.empty:
                plotK(kcb_df)

        import os
        csv_file = os.path.join('datas', 'market_data.csv')
        df_history_mysql = _load_market_history_from_mysql(limit=30)
        if (not df_history_mysql.empty) or os.path.exists(csv_file):
            try:
                df_history = df_history_mysql.copy()
                if df_history.empty:
                    df_history = pd.read_csv(csv_file)
                df_history = df_history.loc[:, ~df_history.columns.str.contains('^Unnamed')]
                if '日期' in df_history.columns:
                    df_history['日期'] = pd.to_datetime(df_history['日期'], errors='coerce')
                    df_history = df_history.dropna(subset=['日期'])
                    df_history = df_history.sort_values('日期')
                    df_history = df_history.tail(30)
                    numeric_cols = ['上涨', '下跌', '涨停', '跌停', '活跃度', '成交额']
                    for col in numeric_cols:
                        if col in df_history.columns:
                            if col == '活跃度':
                                df_history[col] = df_history[col].astype(str).str.replace('%', '').astype(float)
                            else:
                                df_history[col] = pd.to_numeric(df_history[col], errors='coerce')

                    latest_row = df_history.iloc[-1] if not df_history.empty else None
                    if latest_row is not None:
                        up_stocks = latest_row.get('上涨', up_stocks)
                        down_stocks = latest_row.get('下跌', down_stocks)
                        limit_up = latest_row.get('涨停', limit_up)
                        limit_down = latest_row.get('跌停', limit_down)
                        activity = latest_row.get('活跃度', activity)
                    # 这两项固定实时获取，不走MySQL持久化数据
                    fin_series = get_financing_net_buy_series(60)
                    gem_pe_series = get_gem_pe_series(500)

                    first_row = st.columns(3)
                    with first_row[0]:
                        if '成交额' in df_history.columns:
                            fig_amount = go.Figure()
                            fig_amount.add_trace(go.Scatter(
                                x=df_history['日期'],
                                y=df_history['成交额'],
                                mode='lines+markers',
                                name='成交额',
                                line=dict(color='#4c6ef5', width=2),
                                marker=dict(size=4)
                            ))
                            fig_amount.update_layout(
                                title='成交额',
                                xaxis_title='日期',
                                yaxis_title='成交额',
                                height=300,
                                hovermode='x unified'
                            )
                            st.plotly_chart(fig_amount, use_container_width=True)

                    with first_row[1]:
                        if '活跃度' in df_history.columns:
                            fig_activity = go.Figure()
                            fig_activity.add_trace(go.Scatter(
                                x=df_history['日期'],
                                y=df_history['活跃度'],
                                mode='lines+markers',
                                name='情绪指数',
                                line=dict(color='#f39c12', width=2),
                                marker=dict(size=4)
                            ))
                            fig_activity.update_layout(
                                title='情绪指数（活跃度）',
                                xaxis_title='日期',
                                yaxis_title='活跃度 (%)',
                                height=300,
                                hovermode='x unified'
                            )
                            st.plotly_chart(fig_activity, use_container_width=True)

                    with first_row[2]:
                        if fin_series is not None and not fin_series.empty:
                            fin_series = fin_series.sort_values('date')
                            colors = fin_series['融资净买入'].apply(lambda x: '#e74c3c' if x >= 0 else '#2ecc71')
                            fig_financing = go.Figure(go.Bar(
                                x=fin_series['date'],
                                y=fin_series['融资净买入'],
                                marker_color=colors,
                                name='融资净买入',
                                hovertemplate='%{x|%Y-%m-%d}<br>净买入: %{y:.0f}<extra></extra>'
                            ))
                            fig_financing.update_layout(
                                title='融资净买入（近60交易日）',
                                xaxis_title='日期',
                                yaxis_title='金额',
                                height=300,
                                hovermode='x unified',
                                bargap=0.2
                            )
                            st.plotly_chart(fig_financing, use_container_width=True)

                    second_row = st.columns(3)
                    with second_row[0]:
                        if '上涨' in df_history.columns and '下跌' in df_history.columns:
                            fig_up_down = go.Figure()
                            fig_up_down.add_trace(go.Scatter(
                                x=df_history['日期'],
                                y=df_history['上涨'],
                                mode='lines+markers',
                                name='上涨数',
                                line=dict(color='#e74c3c', width=2),
                                marker=dict(size=4)
                            ))
                            fig_up_down.add_trace(go.Scatter(
                                x=df_history['日期'],
                                y=df_history['下跌'],
                                mode='lines+markers',
                                name='下跌数',
                                line=dict(color='#2ecc71', width=2),
                                marker=dict(size=4)
                            ))
                            fig_up_down.update_layout(
                                title='上涨数 vs 下跌数',
                                xaxis_title='日期',
                                yaxis_title='数量',
                                height=300,
                                hovermode='x unified',
                                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                            )
                            st.plotly_chart(fig_up_down, use_container_width=True)
                    with second_row[1]:
                        if '涨停' in df_history.columns and '跌停' in df_history.columns:
                            fig_limit = go.Figure()
                            fig_limit.add_trace(go.Scatter(
                                x=df_history['日期'],
                                y=df_history['涨停'],
                                mode='lines+markers',
                                name='涨停数',
                                line=dict(color='#c0392b', width=2),
                                marker=dict(size=4)
                            ))
                            fig_limit.add_trace(go.Scatter(
                                x=df_history['日期'],
                                y=df_history['跌停'],
                                mode='lines+markers',
                                name='跌停数',
                                line=dict(color='#27ae60', width=2),
                                marker=dict(size=4)
                            ))
                            fig_limit.update_layout(
                                title='涨停数 vs 跌停数',
                                xaxis_title='日期',
                                yaxis_title='数量',
                                height=300,
                                hovermode='x unified',
                                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                            )
                            st.plotly_chart(fig_limit, use_container_width=True)
                    with second_row[2]:
                        if gem_pe_series is not None and not gem_pe_series.empty:
                            gem_pe_series = gem_pe_series.sort_values('date')
                            fig_gem_pe = go.Figure()
                            fig_gem_pe.add_trace(go.Scatter(
                                x=gem_pe_series['date'],
                                y=gem_pe_series['市盈率'],
                                mode='lines+markers',
                                name='创业板市盈率',
                                line=dict(color='#1f77b4', width=2),
                                marker=dict(size=4),
                                hovertemplate='%{x|%Y-%m-%d}<br>PE: %{y:.2f}<extra></extra>'
                            ))
                            fig_gem_pe.update_layout(
                                title='创业板市盈率（近500交易日）',
                                xaxis_title='日期',
                                yaxis_title='PE',
                                height=300,
                                hovermode='x unified'
                            )
                            st.plotly_chart(fig_gem_pe, use_container_width=True)
                        else:
                            st.info("暂无创业板市盈率数据")
            except Exception as e:
                st.warning(f"读取历史市场数据失败: {e}")

        st.markdown("---")

    if _show("top100"):
        _section_title("\u5e02\u573a\u5168\u8c8c\u5206\u6790")

        # 保留原有2张图：当日涨跌幅分布 + 成交额Top100涨跌分布
        top_100_records = review_data.get("top_100_turnover", [])
        market_overview = review_data.get("market_overview", {})
        range_distribution = market_overview.get("range_distribution", [])

        top_100_by_turnover = pd.DataFrame(top_100_records) if top_100_records else pd.DataFrame()
        if not top_100_by_turnover.empty and "pct" in top_100_by_turnover.columns:
            top_100_by_turnover["pct"] = pd.to_numeric(top_100_by_turnover["pct"], errors="coerce")
            up_stocks = top_100_by_turnover[top_100_by_turnover["pct"] > 2]
            down_stocks = top_100_by_turnover[top_100_by_turnover["pct"] < -2]
            shake_stocks = top_100_by_turnover[
                (top_100_by_turnover["pct"] >= -2) & (top_100_by_turnover["pct"] <= 2)
            ]
            shake_count = len(shake_stocks)
        else:
            up_stocks = pd.DataFrame()
            down_stocks = pd.DataFrame()
            shake_count = 0

        overview_cols = st.columns(2)
        with overview_cols[0]:
            if range_distribution:
                dist_df = pd.DataFrame(range_distribution)
                if not dist_df.empty:
                    order_labels = [">20%", "10%~20%", "5%~10%", "3%~5%", "0%~3%", "-3%~0%", "-5%~-3%", "-10%~-5%", "<-10%"]
                    order_map = {label: idx for idx, label in enumerate(order_labels)}
                    dist_df["order"] = dist_df["label"].map(order_map)
                    dist_df = dist_df.sort_values("order")
                    colors = ["#e74c3c" if row["bucket_start"] >= 0 else "#2ecc71" for _, row in dist_df.iterrows()]
                    fig_range = go.Figure(go.Bar(
                        x=dist_df["label"],
                        y=dist_df["count"],
                        text=dist_df["count"],
                        textposition="outside",
                        marker_color=colors,
                        hovertemplate="%{x}<br>\u5bb6\u6570: %{y}<extra></extra>",
                    ))
                    fig_range.update_layout(
                        title="\u5f53\u65e5\u6da8\u8dcc\u5e45\u5206\u5e03",
                        xaxis_title="\u6da8\u8dcc\u5e45\u533a\u95f4(%)",
                        yaxis_title="\u5bb6\u6570",
                        height=400,
                        margin=dict(t=30, l=10, r=10, b=40),
                    )
                    st.plotly_chart(fig_range, use_container_width=True)
                else:
                    st.info("\u6682\u65e0\u6da8\u8dcc\u5e45\u5206\u5e03\u6570\u636e")
            else:
                st.info("\u6682\u65e0\u6da8\u8dcc\u5e45\u5206\u5e03\u6570\u636e")

        with overview_cols[1]:
            if top_100_by_turnover.empty:
                st.info("\u6682\u65e0TOP100\u6570\u636e")
            else:
                categories = [
                    "\u5c0f\u6da8(2-5%)", "\u4e2d\u6da8(5-9%)", "\u5927\u6da8(>9%)",
                    "\u5c0f\u8dcc(-2~-5%)", "\u4e2d\u8dcc(-5~-9%)", "\u5927\u8dcc(<-9%)",
                    "\u9707\u8361(-2%~2%)",
                ]
                small_up = up_stocks[(up_stocks["pct"] >= 2) & (up_stocks["pct"] < 5)]
                medium_up = up_stocks[(up_stocks["pct"] >= 5) & (up_stocks["pct"] < 9)]
                large_up = up_stocks[up_stocks["pct"] >= 9]
                small_down = down_stocks[(down_stocks["pct"] <= -2) & (down_stocks["pct"] > -5)]
                medium_down = down_stocks[(down_stocks["pct"] <= -5) & (down_stocks["pct"] > -9)]
                large_down = down_stocks[down_stocks["pct"] <= -9]
                values = [
                    len(small_up), len(medium_up), len(large_up),
                    len(small_down), len(medium_down), len(large_down),
                    shake_count,
                ]
                colors = ["#c0392b", "#a93226", "#922b21", "#27ae60", "#229954", "#1e8449", "#f39c12"]
                fig_bar = go.Figure(go.Bar(
                    x=categories,
                    y=values,
                    marker_color=colors,
                    text=values,
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>\u6570\u91cf: %{y}<extra></extra>",
                ))
                fig_bar.update_layout(
                    title="\u6210\u4ea4\u989dTop100\u6da8\u8dcc\u5206\u5e03",
                    xaxis_title="\u5206\u7c7b",
                    yaxis_title="\u80a1\u7968\u6570\u91cf",
                    height=400,
                    xaxis=dict(tickangle=-45),
                    yaxis=dict(range=[0, max(values) * 1.2] if values else [0, 10]),
                )
                st.plotly_chart(fig_bar, use_container_width=True)

        top_100_gainers_records = review_data.get("top_100_gainers", [])
        gainers_df = pd.DataFrame(top_100_gainers_records) if top_100_gainers_records else pd.DataFrame()

        if gainers_df.empty:
            st.info("\u6682\u65e0\u6da8\u5e45Top100\u6570\u636e")
        else:
            for col in ["pct", "amount", "mkt_cap"]:
                if col in gainers_df.columns:
                    gainers_df[col] = pd.to_numeric(gainers_df[col], errors="coerce")
            gainers_df = gainers_df.dropna(subset=[c for c in ["code", "name", "pct"] if c in gainers_df.columns])
            gainers_df = gainers_df.sort_values("pct", ascending=False).head(100)

            if gainers_df.empty:
                st.info("\u6682\u65e0\u6ee1\u8db3\u6761\u4ef6\u7684\u6da8\u5e45Top100\u6570\u636e")
            else:
                amount_yi = (gainers_df["amount"] / 1e8) if "amount" in gainers_df.columns else pd.Series(dtype=float)
                mkt_cap_yi = (gainers_df["mkt_cap"] / 1e8) if "mkt_cap" in gainers_df.columns else pd.Series(dtype=float)

                amount_labels = ["<5\u4ebf", "5-50\u4ebf", "50-90\u4ebf", ">90\u4ebf"]
                amount_values = [
                    int((amount_yi < 5).sum()) if not amount_yi.empty else 0,
                    int(((amount_yi >= 5) & (amount_yi < 50)).sum()) if not amount_yi.empty else 0,
                    int(((amount_yi >= 50) & (amount_yi < 90)).sum()) if not amount_yi.empty else 0,
                    int((amount_yi >= 90).sum()) if not amount_yi.empty else 0,
                ]

                mkt_labels = ["<50\u4ebf", "50-100\u4ebf", "100-200\u4ebf", "200-500\u4ebf", ">500\u4ebf"]
                mkt_values = [
                    int((mkt_cap_yi < 50).sum()) if not mkt_cap_yi.empty else 0,
                    int(((mkt_cap_yi >= 50) & (mkt_cap_yi < 100)).sum()) if not mkt_cap_yi.empty else 0,
                    int(((mkt_cap_yi >= 100) & (mkt_cap_yi < 200)).sum()) if not mkt_cap_yi.empty else 0,
                    int(((mkt_cap_yi >= 200) & (mkt_cap_yi < 500)).sum()) if not mkt_cap_yi.empty else 0,
                    int((mkt_cap_yi >= 500).sum()) if not mkt_cap_yi.empty else 0,
                ]

                code_series = gainers_df["code"].astype(str).str.lower().str.strip()
                code6 = code_series.str.extract(r"(\d{6})", expand=False).fillna("")
                kcb_mask = code6.str.startswith("688")
                cyb_mask = code6.str.startswith(("300", "301"))

                board_labels = [
                    "\u4e3b\u677f\uff08\u4e0a\u8bc1+\u6df1\u5733\uff09",
                    "\u521b\u4e1a\u677f",
                    "\u79d1\u521b\u677f",
                ]
                board_values = [
                    int((~kcb_mask & ~cyb_mask).sum()),
                    int(cyb_mask.sum()),
                    int(kcb_mask.sum()),
                ]

                row_cols = st.columns(3)

                with row_cols[0]:
                    fig_amount = go.Figure(go.Bar(
                        x=amount_labels,
                        y=amount_values,
                        marker_color=["#9b59b6", "#3498db", "#f39c12", "#e74c3c"],
                        text=amount_values,
                        textposition="outside",
                        hovertemplate="%{x}<br>\u6570\u91cf: %{y}<extra></extra>",
                    ))
                    fig_amount.update_layout(
                        title="\u6210\u4ea4\u989d\u5206\u5c42\uff08\u6da8\u5e45Top100\uff09",
                        xaxis_title="\u6210\u4ea4\u989d\u533a\u95f4\uff08\u4ebf\u5143\uff09",
                        yaxis_title="\u80a1\u7968\u6570\u91cf",
                        height=350,
                    )
                    st.plotly_chart(fig_amount, use_container_width=True)

                with row_cols[1]:
                    fig_mkt = go.Figure(go.Bar(
                        x=mkt_labels,
                        y=mkt_values,
                        marker_color=["#16a085", "#1abc9c", "#27ae60", "#2ecc71", "#58d68d"],
                        text=mkt_values,
                        textposition="outside",
                        hovertemplate="%{x}<br>\u6570\u91cf: %{y}<extra></extra>",
                    ))
                    fig_mkt.update_layout(
                        title="\u5e02\u503c\u5206\u5c42\uff08\u6da8\u5e45Top100\uff09",
                        xaxis_title="\u5e02\u503c\u533a\u95f4\uff08\u4ebf\u5143\uff09",
                        yaxis_title="\u80a1\u7968\u6570\u91cf",
                        height=350,
                    )
                    st.plotly_chart(fig_mkt, use_container_width=True)

                with row_cols[2]:
                    fig_board = go.Figure(go.Bar(
                        x=board_labels,
                        y=board_values,
                        marker_color=["#2f80ed", "#f2994a", "#eb5757"],
                        text=board_values,
                        textposition="outside",
                        hovertemplate="%{x}<br>\u6570\u91cf: %{y}<extra></extra>",
                    ))
                    fig_board.update_layout(
                        title="\u677f\u5757\u5206\u7c7b\uff08\u6da8\u5e45Top100\uff09",
                        xaxis_title="\u677f\u5757",
                        yaxis_title="\u80a1\u7968\u6570\u91cf",
                        height=350,
                    )
                    st.plotly_chart(fig_board, use_container_width=True)

        st.markdown("---")



st.set_page_config(
    page_title="复盘",
    page_icon="🚀",
    layout="wide"
)

today = datetime.datetime.now()
select_date = st.date_input("选择日期",today)
st.markdown("#### 复盘模块显示")
show_external = st.checkbox("\u5916\u56f4\u6307\u6807", value=True, key="show_external")
show_market = st.checkbox("今日大盘", value=True, key="show_market")
show_top100 = st.checkbox("市场全貌分析", value=True, key="show_top100")
show_modules = {
    "external": show_external,
    "market": show_market,
    "top100": show_top100,
}
btn_cols = st.columns(2)
with btn_cols[0]:
    load_btn = st.button("Load")
with btn_cols[1]:
    realtime_load_btn = st.button("实时Load")

if load_btn or realtime_load_btn:
    if select_date.weekday() >= 5: 
        st.warning("非交易日")
        st.stop()
    
    # date_str = select_date.strftime('%Y-%m-%d')
    # cached_data = load_review_data(date_str)
    # if cached_data:
    #     st.info(f"使用已缓存复盘数据: {date_str}")
    #     display_review_data(cached_data)
    # else:
    if realtime_load_btn:
        review_data = build_review_data(select_date, show_modules)
        st.info("已按实时方案取数")
    else:
        review_data = build_review_data_from_mysql(select_date, show_modules)
        st.info("已按MySQL持久化数据取数（K线实时）")
        if show_modules.get("top100", True) and not review_data.get("top_100_turnover"):
            st.warning("MySQL中未命中TOP100分组数据，可点击“实时Load”兜底。")
    #     if is_review_data_complete(review_data):
    #         save_review_data(date_str, review_data)
    #         st.success(f"复盘数据已保存: {date_str}")
    #     else:
    #         st.warning("复盘数据不完整，未写入缓存")
    display_review_data(review_data, show_modules)
