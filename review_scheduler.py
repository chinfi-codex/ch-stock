#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import datetime as dt
import logging
import time
from pathlib import Path

import pandas as pd

logging.getLogger("streamlit").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime").setLevel(logging.ERROR)

from data_sources import (
    _build_pct_distribution,
    _df_to_records,
    _normalize_top_stocks_df,
    get_dt_pool,
    get_zt_pool,
)
from tools import get_all_stocks, get_longhu_data, get_market_data
from tools.financial_data import EconomicIndicators
from tools.storage_utils import save_review_data


def _parse_date(date_str):
    return dt.datetime.strptime(date_str, "%Y-%m-%d").date()


def _validate_time(time_str):
    dt.datetime.strptime(time_str, "%H:%M")
    return time_str


def _df_to_plain_records(df):
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return []
    view = df.copy()
    for col in view.columns:
        if pd.api.types.is_datetime64_any_dtype(view[col]):
            view[col] = view[col].astype(str)
    return view.to_dict(orient="records")


def _market_items_to_dict(market_data):
    if market_data is None or market_data.empty:
        return {}
    if not {"item", "value"}.issubset(set(market_data.columns)):
        return {}
    view = market_data[["item", "value"]].copy()
    view["item"] = view["item"].astype(str)
    return dict(zip(view["item"], view["value"]))


def _find_market_value(item_map, keywords):
    if not item_map:
        return None
    for key, value in item_map.items():
        text = str(key)
        if any(word in text for word in keywords):
            return value
    return None


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


def build_market_section(select_date, all_stocks_df=None):
    sh_df, cyb_df, kcb_df, market_data = get_market_data()
    item_map = _market_items_to_dict(market_data)
    up_stocks = _find_market_value(item_map, ["上涨"])
    down_stocks = _find_market_value(item_map, ["下跌"])
    limit_up = _find_market_value(item_map, ["涨停"])
    limit_down = _find_market_value(item_map, ["跌停"])
    activity = _find_market_value(item_map, ["活跃", "情绪"])
    if isinstance(activity, str):
        activity = activity.replace("%", "").strip()

    if all_stocks_df is None:
        try:
            all_stocks_df = get_all_stocks(select_date)
        except Exception:
            all_stocks_df = None

    market_overview = {
        "items": item_map,
        "up_stocks": up_stocks,
        "down_stocks": down_stocks,
        "limit_up": limit_up,
        "limit_down": limit_down,
        "activity": activity,
        "range_distribution": _build_pct_distribution(all_stocks_df),
    }
    indices = {
        "sh_df": _df_to_records(sh_df),
        "cyb_df": _df_to_records(cyb_df),
        "kcb_df": _df_to_records(kcb_df),
    }
    return {"indices": indices, "market_overview": market_overview}, all_stocks_df


# concept section disabled (akshare blocked)

def build_top100_section(select_date, all_stocks_df=None):
    source_df = all_stocks_df if all_stocks_df is not None else get_all_stocks(select_date)
    today_top_stocks = _normalize_top_stocks_df(source_df)

    if not today_top_stocks.empty:
        cols = list(today_top_stocks.columns)
        if len(cols) >= 4:
            rename_pos = {cols[0]: "name", cols[1]: "code", cols[2]: "pct", cols[3]: "amount"}
            today_top_stocks = today_top_stocks.rename(columns=rename_pos)

    turnover_records = []
    if not today_top_stocks.empty and {"pct", "amount", "name", "code"}.issubset(today_top_stocks.columns):
        top_100_by_turnover = today_top_stocks.sort_values("amount", ascending=False).head(100)
        top_100_by_turnover["pct"] = pd.to_numeric(top_100_by_turnover["pct"], errors="coerce")
        turnover_records = top_100_by_turnover[["name", "code", "pct", "amount"]].to_dict(orient="records")

    range_data = {"sh_stocks": [], "cyb_kcb_stocks": []}
    if not today_top_stocks.empty and {"pct", "amount", "name", "code"}.issubset(today_top_stocks.columns):
        top_100_by_range = today_top_stocks.sort_values("pct", ascending=False).head(100)
        code_series = top_100_by_range["code"].astype(str)
        sh_df = top_100_by_range[
            (code_series.str[2:].str.startswith("6") | code_series.str[2:].str.startswith("0"))
            & ~code_series.str[2:].str.startswith("688")
        ]
        cyb_kcb_df = top_100_by_range[
            code_series.str[2:].str.startswith("3") | code_series.str[2:].str.startswith("688")
        ]
        range_data = {
            "sh_stocks": sh_df[["name", "code", "pct", "amount"]].to_dict(orient="records"),
            "cyb_kcb_stocks": cyb_kcb_df[["name", "code", "pct", "amount"]].to_dict(orient="records"),
        }

    return {"top_100_turnover": turnover_records, "top_100_range": range_data}, source_df


def build_review_data(select_date):
    review_data = {"date": select_date.strftime("%Y-%m-%d")}
    all_stocks_df = None

    review_data["external"] = build_external_section()

    market_section, all_stocks_df = build_market_section(select_date, all_stocks_df)
    review_data.update(market_section)

    # concept section disabled

    top_section, all_stocks_df = build_top100_section(select_date, all_stocks_df)
    review_data.update(top_section)

    gainers = []
    losers = []
    if all_stocks_df is not None and not all_stocks_df.empty and "pct" in all_stocks_df.columns:
        view = all_stocks_df.copy()
        view["pct"] = pd.to_numeric(view["pct"], errors="coerce")
        view = view.dropna(subset=["pct"])
        gainers_df = view.sort_values("pct", ascending=False).head(100)
        losers_df = view.sort_values("pct", ascending=True).head(100)
        keep_cols = [col for col in ["code", "name", "pct", "amount", "mkt_cap"] if col in view.columns]
        if keep_cols:
            gainers = gainers_df[keep_cols].to_dict(orient="records")
            losers = losers_df[keep_cols].to_dict(orient="records")
    review_data["top_100_gainers"] = gainers
    review_data["top_100_losers"] = losers
    return review_data


def _build_short_section(target_date):
    trade_date = target_date.strftime("%Y%m%d")
    short_section = {
        "trade_date": trade_date,
        "longhu": [],
        "zt_pool": [],
        "dt_pool": [],
    }
    try:
        short_section["longhu"] = _df_to_plain_records(get_longhu_data(trade_date))
    except Exception as exc:
        logging.warning("Get longhu data failed: %s", exc)
    try:
        short_section["zt_pool"] = _df_to_plain_records(get_zt_pool(trade_date))
    except Exception as exc:
        logging.warning("Get zt_pool failed: %s", exc)
    try:
        short_section["dt_pool"] = _df_to_plain_records(get_dt_pool(trade_date))
    except Exception as exc:
        logging.warning("Get dt_pool failed: %s", exc)
    return short_section


def build_full_review_data(target_date):
    review_data = build_review_data(target_date)
    review_data["short"] = _build_short_section(target_date)
    return review_data


def run_once(target_date, output_dir, skip_weekend=False):
    if skip_weekend and target_date.weekday() >= 5:
        logging.info("Skip weekend date: %s", target_date.strftime("%Y-%m-%d"))
        return None

    review_data = build_full_review_data(target_date)
    file_path = save_review_data(
        target_date.strftime("%Y-%m-%d"),
        review_data,
        review_dir=output_dir,
    )
    logging.info("Saved review JSON: %s", file_path)
    return file_path


def run_scheduler(run_time, output_dir, skip_weekend=False, run_immediately=False, poll_seconds=30):
    try:
        import schedule
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "Missing dependency 'schedule'. Install it with: pip install schedule"
        ) from exc

    def _job():
        try:
            run_once(dt.date.today(), output_dir=output_dir, skip_weekend=skip_weekend)
        except Exception:
            logging.exception("Daily review job failed")

    schedule.every().day.at(run_time).do(_job)
    logging.info("Scheduler started. Run time: %s, output dir: %s", run_time, output_dir)

    if run_immediately:
        _job()

    while True:
        schedule.run_pending()
        time.sleep(max(1, int(poll_seconds)))


def build_parser():
    parser = argparse.ArgumentParser(description="Daily review data scheduler")
    parser.add_argument("--time", default="18:30", help="Daily run time, format HH:MM")
    parser.add_argument(
        "--output-dir",
        default=str(Path("Datas") / "Reviews"),
        help="Output directory for JSON files",
    )
    parser.add_argument("--run-once", action="store_true", help="Run once and exit")
    parser.add_argument("--date", help="Target date for --run-once, format YYYY-MM-DD")
    parser.add_argument("--run-immediately", action="store_true", help="Run one time when scheduler starts")
    parser.add_argument("--skip-weekend", action="store_true", help="Skip Saturday/Sunday")
    parser.add_argument("--poll-seconds", type=int, default=30, help="Scheduler polling interval")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.run_once:
        if args.date:
            target_date = _parse_date(args.date)
        else:
            target_date = dt.date.today()
        run_once(
            target_date=target_date,
            output_dir=str(output_dir),
            skip_weekend=args.skip_weekend,
        )
        return

    run_time = _validate_time(args.time)
    run_scheduler(
        run_time=run_time,
        output_dir=str(output_dir),
        skip_weekend=args.skip_weekend,
        run_immediately=args.run_immediately,
        poll_seconds=args.poll_seconds,
    )


if __name__ == "__main__":
    main()
