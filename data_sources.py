import datetime
import re
import os
import pandas as pd
import akshare as ak
import streamlit as st
import tushare as ts
from infra.config import get_tushare_token
from tools.kline_data import get_ak_price_df


def _normalize_top_stocks_df(df):
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()

    def _pick(cols):
        for col in cols:
            if col in df.columns:
                return col
        return None

    name_col = _pick(["名称", "name"])
    code_col = _pick(["代码", "ts_code", "code", "symbol"])
    pct_col = _pick(["润跌平", "pct_chg", "pct", "润跌平(%)"])
    amount_col = _pick(["成交额", "amount", "成交额万", "成交额万元"])
    vol_col = _pick(["成交量", "vol", "volume"])

    if not all([name_col, code_col, pct_col, amount_col]):
        return pd.DataFrame()

    keep_cols = [name_col, code_col, pct_col, amount_col]
    if vol_col:
        keep_cols.append(vol_col)
    out = df[keep_cols].copy()
    out.columns = ["名称", "代码", "润跌平", "成交额"] + (["成交量"] if vol_col else [])

    out["代码"] = out["代码"].astype(str).str.strip()

    def _normalize_code(value):
        if not value:
            return value
        code = str(value).strip().lower()
        if "." in code:
            prefix, suffix = code.split(".", 1)
            if suffix in {"sh", "sz", "bj"}:
                return f"{suffix}{prefix}"
        if code.startswith(("sh", "sz", "bj")) and len(code) >= 8:
            return code
        if len(code) == 6 and code.isdigit():
            if code.startswith(("0", "3")):
                return f"sz{code}"
            if code.startswith(("6", "9")):
                return f"sh{code}"
            if code.startswith("8"):
                return f"bj{code}"
        return code

    out["代码"] = out["代码"].apply(_normalize_code)
    out["润跌平"] = pd.to_numeric(out["润跌平"], errors="coerce")
    out["成交额"] = pd.to_numeric(out["成交额"], errors="coerce")
    if amount_col == "amount" and "mkt_cap" not in df.columns:
        out["成交额"] = out["成交额"] * 1000
    if amount_col == "成交额万元":
        out["成交额"] = out["成交额"] * 10000
    if "成交量" in out.columns:
        out["成交量"] = pd.to_numeric(out["成交量"], errors="coerce")
        if vol_col == "vol":
            out["成交量"] = out["成交量"] * 100
    return out


def _build_pct_distribution(df):
    if df is None or df.empty or "pct" not in df.columns:
        return []
    pct_series = pd.to_numeric(df["pct"], errors="coerce").dropna()
    if pct_series.empty:
        return []
    bins = [-100, -10, -5, -3, 0, 3, 5, 10, 20, 100]
    labels = [
        "<-10%",
        "-10%~-5%",
        "-5%~-3%",
        "-3%~0%",
        "0%~3%",
        "3%~5%",
        "5%~10%",
        "10%~20%",
        ">20%",
    ]
    categories = pd.cut(
        pct_series, bins=bins, labels=labels, right=False, include_lowest=True
    )
    counts = categories.value_counts().reindex(labels, fill_value=0)
    distribution = []
    for idx, (label, count) in enumerate(zip(labels, counts)):
        distribution.append(
            {
                "label": label,
                "count": int(count),
                "bucket_start": bins[idx],
                "bucket_end": bins[idx + 1],
            }
        )
    return distribution


def _normalize_concept_kline(df):
    df = df.rename(
        columns={
            "鏃ユ湡": "date",
            "寮€鐩樹环": "open",
            "鏈€楂樹环": "high",
            "鏈€浣庝环": "low",
            "鏀剁洏浠?": "close",
            "成交量": "volume",
            "成交额": "amount",
        }
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    df = df.set_index("date")
    return df


def _normalize_index_kline(df):
    df = df.rename(
        columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    df = df.set_index("date")
    return df


def _pick_first_column(df, candidates):
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _to_number(series):
    if series is None:
        return None
    s = series.astype(str).str.replace("%", "", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _normalize_spot_df(df):
    if df is None or df.empty:
        return pd.DataFrame()
    if {"code", "name", "pct", "amount", "mkt_cap"}.issubset(df.columns):
        view = df[["code", "name", "pct", "amount", "mkt_cap"]].copy()
        view["pct"] = _to_number(view["pct"])
        view["amount"] = _to_number(view["amount"])
        view["mkt_cap"] = _to_number(view["mkt_cap"])
        view = view.dropna(subset=["code", "name", "pct", "amount", "mkt_cap"])
        return view

    code_col = _pick_first_column(df, ["代码", "鑲＄エ代码", "symbol"])
    name_col = _pick_first_column(df, ["名称", "鑲＄エ名称", "name"])
    pct_col = _pick_first_column(df, ["润跌平", "润跌平(%)", "娑ㄥ箙", "pct_chg"])
    amount_col = _pick_first_column(df, ["成交额", "成交额万", "成交额万元", "amount"])
    mkt_cap_col = _pick_first_column(
        df, ["鎬诲競鍊?", "鎬诲競鍊?鍏?", "鎬诲競鍊?涓囧厓)", "total_mv"]
    )

    if not all([code_col, name_col, pct_col, amount_col, mkt_cap_col]):
        return pd.DataFrame()

    view = df[[code_col, name_col, pct_col, amount_col, mkt_cap_col]].copy()
    view.columns = ["code", "name", "pct", "amount", "mkt_cap"]
    view["pct"] = _to_number(view["pct"])
    view["amount"] = _to_number(view["amount"])
    view["mkt_cap"] = _to_number(view["mkt_cap"])
    if amount_col == "amount":
        view["amount"] = view["amount"] * 1000
    if mkt_cap_col == "total_mv":
        view["mkt_cap"] = view["mkt_cap"] * 10000
    view = view.dropna(subset=["code", "name", "pct", "amount", "mkt_cap"])
    return view


def _normalize_em_kline(df):
    rename_map = {
        "鏃ユ湡": "date",
        "寮€鐩?": "open",
        "寮€鐩樹环": "open",
        "鏈€楂?": "high",
        "鏈€楂樹环": "high",
        "鏈€浣?": "low",
        "鏈€浣庝环": "low",
        "鏀剁洏": "close",
        "鏀剁洏浠?": "close",
        "成交量": "volume",
        "成交额": "amount",
    }
    df = df.rename(columns=rename_map)
    if "date" not in df.columns:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")
    return df


def _safe_market_value(market_data, item_name, default=None):
    if market_data is None or market_data.empty:
        return default
    series = market_data.loc[market_data["item"] == item_name, "value"]
    if series.empty:
        return default
    return series.iloc[0]


def _df_to_records(df):
    if df is None or df.empty:
        return []
    df_reset = df.reset_index()
    if "date" in df_reset.columns:
        df_reset["date"] = df_reset["date"].astype(str)
    return df_reset.to_dict(orient="records")


def _records_to_df(records, date_col="date"):
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.set_index(date_col)
    return df


@st.cache_data(ttl="1d")
def get_concept_kline_data(concept_name, start_date, end_date):
    try:
        df = ak.stock_board_concept_index_ths(
            symbol=concept_name, start_date=start_date, end_date=end_date
        )
        if df is None or df.empty:
            return None
        return _normalize_concept_kline(df)
    except Exception:
        return None


@st.cache_data(ttl="1d")
def get_concept_list():
    try:
        concept_list = ak.stock_board_concept_name_ths()
        return concept_list
    except Exception:
        return None


@st.cache_data(ttl="1d")
def get_benchmark_kline(start_date, end_date, symbol="sh000001"):
    def _as_em_symbol(value):
        if value.startswith(("sh", "sz", "bj", "csi")):
            return value
        if value.isdigit() and len(value) == 6:
            return f"csi{value}"
        return value

    def _normalize_df(raw_df):
        if raw_df is None or raw_df.empty:
            return None
        if "volume" not in raw_df.columns and "amount" in raw_df.columns:
            raw_df = raw_df.copy()
            raw_df["volume"] = raw_df["amount"]
        return _normalize_index_kline(raw_df)

    fetchers = [
        lambda: ak.stock_zh_index_daily(symbol=symbol),
        lambda: ak.stock_zh_index_daily_tx(symbol=symbol),
        lambda: ak.stock_zh_index_daily_em(
            symbol=_as_em_symbol(symbol), start_date=start_date, end_date=end_date
        ),
    ]

    df = None
    for fetch in fetchers:
        try:
            df = _normalize_df(fetch())
        except Exception:
            df = None
        if df is not None and not df.empty:
            break

    if df is None or df.empty:
        return None
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    df = df.loc[start_dt:end_dt]
    return df


@st.cache_data(ttl="1h")
def get_zt_pool(date):
    # 优先 AkShare
    try:
        df = ak.stock_zt_pool_em(date=date)
        if df is not None and not df.empty:
            return df
    except Exception:
        df = None
    # fallback: Tushare limit_list
    try:
        token = get_tushare_token()
        if not token:
            return pd.DataFrame()
        pro = ts.pro_api(token)
        df = pro.limit_list(trade_date=date, limit_type="U")
        if df is not None and not df.empty:
            return df
        # 二级 fallback：用日线 + 涨跌停价推导
        daily = pro.daily(
            trade_date=date, fields="ts_code,trade_date,close,pct_chg,amount"
        )
        limit = pro.stk_limit(
            trade_date=date, fields="ts_code,trade_date,up_limit,down_limit"
        )
        if daily is None or daily.empty or limit is None or limit.empty:
            return pd.DataFrame()
        merged = daily.merge(limit, on=["ts_code", "trade_date"], how="left")
        merged = merged[merged["close"] >= merged["up_limit"]]
        return merged
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl="1h")
def get_dt_pool(date):
    # 优先 AkShare
    try:
        df = ak.stock_zt_pool_dtgc_em(date=date)
        if df is not None and not df.empty:
            return df
    except Exception:
        df = None
    # fallback: Tushare limit_list (跌停)
    try:
        token = get_tushare_token()
        if not token:
            return pd.DataFrame()
        pro = ts.pro_api(token)
        df = pro.limit_list(trade_date=date, limit_type="D")
        if df is not None and not df.empty:
            return df
        # 二级 fallback：用日线 + 涨跌停价推导
        daily = pro.daily(
            trade_date=date, fields="ts_code,trade_date,close,pct_chg,amount"
        )
        limit = pro.stk_limit(
            trade_date=date, fields="ts_code,trade_date,up_limit,down_limit"
        )
        if daily is None or daily.empty or limit is None or limit.empty:
            return pd.DataFrame()
        merged = daily.merge(limit, on=["ts_code", "trade_date"], how="left")
        merged = merged[merged["close"] <= merged["down_limit"]]
        return merged
    except Exception:
        return pd.DataFrame()


def _fetch_kline_df(code, end_date, lookback_days, adjust_mode, include_amount):
    end_str = end_date.strftime("%Y%m%d")
    start_date = (end_date - datetime.timedelta(days=lookback_days * 2)).strftime(
        "%Y%m%d"
    )

    if adjust_mode == "qfq":
        price_df = get_ak_price_df(code, end_date=end_str, count=lookback_days)
        price_df = price_df.copy()
        price_df.index = pd.to_datetime(price_df.index)
    else:
        raw = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_str,
            adjust=adjust_mode,
        )
        price_df = _normalize_em_kline(raw)

    if price_df is None or price_df.empty:
        return pd.DataFrame()

    if include_amount and "amount" not in price_df.columns:
        raw = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_str,
            adjust=adjust_mode,
        )
        amount_df = _normalize_em_kline(raw)
        if not amount_df.empty and "amount" in amount_df.columns:
            price_df = price_df.join(amount_df[["amount"]], how="left")

    price_df = price_df.sort_index().tail(lookback_days)
    price_df["is_trading_day"] = True
    price_df["adj_factor"] = 1.0
    return price_df
