#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
同花顺概念板块量价数据筛选/筛选条件：
1. 板块指数收盘价 > 60日均线
2. 20日均线斜率为正（趋势不只是反弹）
3. 最新收盘价创60日新高
"""

import os
from datetime import datetime, timedelta, date

import akshare as ak
import numpy as np
import pandas as pd
import streamlit as st
import tushare as ts

from tools.market_data import get_all_stocks
from tools.stock_data import plotK, get_ak_price_df


def calculate_ma(prices, period):
    """计算移动平均线"""
    return prices.rolling(window=period).mean()


def calculate_ma_slope(ma_values, period=5):
    """
    计算均线斜率
    使用最近period天的数据进行线性回归，返回斜率
    """
    if len(ma_values) < period:
        return 0

    recent_values = ma_values[-period:].values
    x = np.arange(len(recent_values))

    # 线性回归计算斜率
    slope = np.polyfit(x, recent_values, 1)[0]
    return slope


def _normalize_concept_kline(df):
    df = df.rename(
        columns={
            "日期": "date",
            "开盘价": "open",
            "最高价": "high",
            "最低价": "low",
            "收盘价": "close",
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


@st.cache_data(ttl="1d")
def get_concept_kline_data(concept_name, start_date, end_date):
    """
    获取概念板块K线数据（缓存）
    """
    try:
        df = ak.stock_board_concept_index_ths(
            symbol=concept_name,
            start_date=start_date,
            end_date=end_date,
        )
        if df is None or df.empty:
            return None
        return _normalize_concept_kline(df)
    except Exception as e:
        print(f"获取 {concept_name} 数据失败: {e}")
        return None


@st.cache_data(ttl="1d")
def get_concept_list():
    """
    获取所有概念板块列表
    """
    try:
        return ak.stock_board_concept_name_ths()
    except Exception:
        return None


@st.cache_data(ttl="1d")
def get_em_concept_list():
    """
    获取东方财富概念板块列表（缓存）
    """
    try:
        return ak.stock_board_concept_name_em()
    except Exception:
        return None


@st.cache_data(ttl="1d")
def get_em_concept_cons(concept_name):
    """
    获取东方财富概念板块成分股（缓存）
    """
    try:
        return ak.stock_board_concept_cons_em(symbol=concept_name)
    except Exception:
        return None


@st.cache_data(ttl="1d")
def get_em_concept_hist(concept_name, start_date, end_date):
    """
    获取东方财富概念板块指数K线（缓存）
    """
    try:
        df = ak.stock_board_concept_hist_em(
            symbol=concept_name,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="",
        )
        if df is None or df.empty:
            return None
        return _normalize_em_kline(df)
    except Exception:
        return None


@st.cache_data(ttl="1d")
def get_stock_hist(symbol, start_date, end_date):
    """
    获取个股K线（缓存）
    """
    try:
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="",
        )
        if df is None or df.empty:
            return None
        return _normalize_em_kline(df)
    except Exception:
        return None


def _normalize_em_kline(df):
    rename_map = {
        "日期": "date",
        "开盘": "open",
        "开盘价": "open",
        "最高": "high",
        "最高价": "high",
        "最低": "low",
        "最低价": "low",
        "收盘": "close",
        "收盘价": "close",
        "成交量": "volume",
        "成交额": "amount",
    }
    df = df.rename(columns=rename_map)
    if "date" not in df.columns:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")
    return df


def _pick_first_column(df, candidates):
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def _detect_sector_stage(sector_df):
    close = sector_df["close"]
    amount = sector_df.get("amount")
    if amount is None:
        amount = sector_df.get("volume")

    ma20 = calculate_ma(close, 20)
    ma60 = calculate_ma(close, 60)
    ma120 = calculate_ma(close, 120)
    ma60_slope = calculate_ma_slope(ma60.dropna(), period=10)
    hh_60 = close.rolling(60).max().iloc[-1]
    amount_ratio = 1
    if amount is not None:
        amount_ratio = (amount / amount.rolling(20).mean()).iloc[-1]

    latest = close.iloc[-1]
    if latest >= hh_60 and amount_ratio > 1.2 and ma60_slope > 0:
        return "start"
    if latest > ma20.iloc[-1] and ma60_slope > 0 and latest > ma60.iloc[-1]:
        return "uptrend"
    return "divergence"


@st.cache_data(ttl="1d")
def get_benchmark_kline(start_date, end_date, symbol="sh000001"):
    df = ak.stock_zh_index_daily(symbol=symbol)
    if df is None or df.empty:
        return None
    df = _normalize_index_kline(df)
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    df = df.loc[start_dt:end_dt]
    return df


def calculate_adx(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]

    tr = pd.concat(
        [
            (high - low),
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)

    plus_dm = (high - high.shift(1)).where(
        (high - high.shift(1)) > (low.shift(1) - low), 0.0
    )
    plus_dm = plus_dm.where(plus_dm > 0, 0.0)
    minus_dm = (low.shift(1) - low).where(
        (low.shift(1) - low) > (high - high.shift(1)), 0.0
    )
    minus_dm = minus_dm.where(minus_dm > 0, 0.0)

    tr_smooth = tr.rolling(window=period).sum()
    plus_dm_smooth = plus_dm.rolling(window=period).sum()
    minus_dm_smooth = minus_dm.rolling(window=period).sum()

    plus_di = 100 * (plus_dm_smooth / tr_smooth.replace(0, np.nan))
    minus_di = 100 * (minus_dm_smooth / tr_smooth.replace(0, np.nan))
    dx = (
        100
        * (plus_di - minus_di).abs()
        / (plus_di + minus_di).replace(0, np.nan)
    )
    adx = dx.rolling(window=period).mean()
    return adx


def calculate_atr(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    tr = pd.concat(
        [
            (high - low),
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(window=period).mean()


def calculate_obv(df):
    direction = np.sign(df["close"].diff()).fillna(0)
    return (direction * df["volume"]).cumsum()


def calculate_adl(df):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    volume = df["volume"]
    mfm = ((close - low) - (high - close)) / (high - low).replace(0, np.nan)
    mfv = mfm * volume
    return mfv.cumsum()


def calculate_max_drawdown(close_series, window):
    rolling_max = close_series.rolling(window=window, min_periods=1).max()
    drawdown = close_series / rolling_max - 1
    return drawdown.tail(window).min()


def calculate_downside_vol(returns, window):
    downside = returns.where(returns < 0, np.nan)
    return downside.tail(window).std()


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

    code_col = _pick_first_column(df, ["代码", "股票代码", "symbol"])
    name_col = _pick_first_column(df, ["名称", "股票名称", "name"])
    pct_col = _pick_first_column(df, ["涨跌幅", "涨跌幅(%)", "涨跌幅%", "pct_chg"])
    amount_col = _pick_first_column(df, ["成交额", "成交额(千元)", "成交额(万元)", "amount"])
    mkt_cap_col = _pick_first_column(df, ["总市值", "总市值(千元)", "总市值(万元)", "total_mv"])

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


@st.cache_data(ttl="30m")
def get_spot_pool(base_date=None):
    return get_all_stocks(base_date)


def _fetch_kline_df(code, end_date, lookback_days, adjust_mode, include_amount):
    token = st.secrets.get("tushare_token") or os.environ.get("TUSHARE_TOKEN")
    if not token:
        st.error("Missing TUSHARE_TOKEN; cannot fetch market data")
        return pd.DataFrame()
    pro = ts.pro_api(token)

    def _to_ts_code(c):
        c = str(c).strip().upper()
        if "." in c:
            return c.replace(".SS", ".SH")
        if len(c) == 6 and c.isdigit():
            if c.startswith(("0", "3")):
                return f"{c}.SZ"
            if c.startswith(("6", "9")):
                return f"{c}.SH"
            if c.startswith("8"):
                return f"{c}.BJ"
        return c

    end_str = end_date.strftime("%Y%m%d")
    start_date = (end_date - timedelta(days=lookback_days * 2)).strftime("%Y%m%d")
    ts_code = _to_ts_code(code)

    try:
        raw = pro.daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_str,
            fields="trade_date,open,high,low,close,vol,amount",
        )
    except Exception as e:
        st.warning(f"{code} 拉取 TuShare 日线失败: {e}")
        return pd.DataFrame()

    if raw is None or raw.empty:
        return pd.DataFrame()

    raw = raw.sort_values("trade_date")
    raw["trade_date"] = pd.to_datetime(raw["trade_date"])

    adj = adjust_mode or ""
    if adj in ("qfq", "hfq"):
        try:
            adj_df = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_str)
        except Exception:
            adj_df = pd.DataFrame()
        if not adj_df.empty:
            adj_df = adj_df[["trade_date", "adj_factor"]]
            adj_df["trade_date"] = pd.to_datetime(adj_df["trade_date"])
            merged = raw.merge(adj_df, on="trade_date", how="left")
            merged["adj_factor"] = merged["adj_factor"].ffill().bfill()
            base = merged["adj_factor"].iloc[-1] if adj == "qfq" else merged["adj_factor"].iloc[0]
            factor = merged["adj_factor"] / base if base else 1.0
            for col in ["open", "high", "low", "close"]:
                merged[col] = merged[col] * factor
            raw = merged.drop(columns=["adj_factor"])

    raw = raw.rename(columns={"trade_date": "date", "vol": "volume"})
    raw["volume"] = pd.to_numeric(raw["volume"], errors="coerce")
    raw["amount"] = pd.to_numeric(raw["amount"], errors="coerce")
    raw = raw.dropna(subset=["open", "high", "low", "close"])

    price_df = raw.set_index("date").sort_index()
    cols = ["open", "high", "low", "close", "volume"]
    if include_amount:
        cols.append("amount")
    price_df = price_df[cols].tail(lookback_days)
    price_df["is_trading_day"] = True
    return price_df


def _calc_slope(series, window):
    if window <= 1:
        return series.diff()
    return series.diff(window) / window


def _compute_features(df, cfg):
    df = df.copy()
    df = df.sort_index()

    upper = df["high"].rolling(cfg["W_box"], min_periods=cfg["W_box"]).max()
    lower = df["low"].rolling(cfg["W_box"], min_periods=cfg["W_box"]).min()
    mid = (upper + lower) / 2
    box_width = (upper - lower) / mid

    hit = (df["close"] >= lower * (1 - cfg["box_hit_tol"])) & (
        df["close"] <= upper * (1 + cfg["box_hit_tol"])
    )
    hit_ratio = hit.rolling(cfg["W_box"], min_periods=cfg["W_box"]).mean()

    box_valid = (
        (box_width >= cfg["box_width_min"])
        & (box_width <= cfg["box_width_max"])
        & (hit_ratio >= cfg["box_hit_ratio_min"])
    )

    rolling_max = df["close"].rolling(cfg["L_bottom"], min_periods=cfg["L_bottom"]).max()
    drawdown = 1 - df["close"] / rolling_max
    cond_drawdown = drawdown >= cfg["bottom_drawdown_min"]

    ma20 = df["close"].rolling(20, min_periods=20).mean()
    ma60 = df["close"].rolling(60, min_periods=60).mean()
    ma20_slope = _calc_slope(ma20, cfg["ma_slope_window"])
    ma60_slope = _calc_slope(ma60, cfg["ma_slope_window"])
    cond_ma_flat = (ma20_slope.abs() <= cfg["ma_slope_max"]) & (ma60_slope.abs() <= cfg["ma_slope_max"])
    cond_ma_gap = (ma20 / ma60 - 1).abs() <= cfg["ma_gap_max"]

    vol_avg = df["volume"].rolling(cfg["W_box"], min_periods=cfg["W_box"]).mean()
    vol_prev = vol_avg.shift(cfg["W_box"])
    cond_vol_contract = vol_avg <= vol_prev * cfg["vol_contract_ratio"]

    ret_std = df["close"].pct_change().rolling(cfg["W_box"], min_periods=cfg["W_box"]).std()
    ret_std_base = ret_std.rolling(cfg["L_bottom"], min_periods=cfg["L_bottom"]).mean()
    cond_vol_converge = ret_std <= ret_std_base * cfg["vol_converge_ratio"]

    bottom_score = (
        cond_drawdown.astype(int)
        + cond_ma_flat.astype(int)
        + cond_ma_gap.astype(int)
        + cond_vol_contract.astype(int)
        + cond_vol_converge.astype(int)
    )
    bottom_valid = bottom_score >= cfg["bottom_min_hits"]

    near_upper = df["close"] >= upper * (1 - cfg["eps"])
    break_upper = df["close"] >= upper
    vol_ma = df["volume"].rolling(cfg["V_avg"], min_periods=cfg["V_avg"]).mean()
    vol_spike = df["volume"] >= cfg["vol_spike_mult"] * vol_ma

    close_pos = (df["close"] - df["low"]) / (df["high"] - df["low"]).replace(0, np.nan)
    upper_shadow = (df["high"] - df["close"]) / df["close"].replace(0, np.nan)
    ret_1d = df["close"].pct_change()

    df["Upper"] = upper
    df["Lower"] = lower
    df["BoxWidth"] = box_width
    df["HitRatio"] = hit_ratio
    df["BoxValid"] = box_valid
    df["Drawdown"] = drawdown
    df["BottomValid"] = bottom_valid
    df["NearUpper"] = near_upper
    df["BreakUpper"] = break_upper
    df["VolSpike"] = vol_spike
    df["ClosePos"] = close_pos
    df["UpperShadow"] = upper_shadow
    df["Ret1d"] = ret_1d
    df["MA20"] = ma20
    df["MA60"] = ma60
    return df


def _evaluate_stock(code, name, cfg, end_date):
    df = _fetch_kline_df(
        code,
        end_date=end_date,
        lookback_days=cfg["lookback_days"],
        adjust_mode=cfg["adjust_mode"],
        include_amount=cfg["use_amount"],
    )
    if df is None or df.empty:
        return None, None

    min_len = max(cfg["W_box"], cfg["L_bottom"], cfg["V_avg"], 60)
    if len(df) < min_len:
        return None, None

    df = df.dropna(subset=["open", "high", "low", "close", "volume"])
    if df.empty:
        return None, None

    df = _compute_features(df, cfg)
    latest = df.iloc[-1]

    if pd.isna(latest["Upper"]) or pd.isna(latest["Lower"]):
        return None, None

    amount_ok = True
    if cfg["use_amount"]:
        amount_ok = not pd.isna(latest.get("amount", np.nan))

    shape_ok = True
    if cfg["use_shape_filter"]:
        shape_ok = (
            latest["ClosePos"] >= cfg["close_pos_min"]
            and latest["UpperShadow"] <= cfg["upper_shadow_max"]
        )

    ret_ok = True
    if cfg["use_ret_filter"]:
        ret_ok = cfg["ret_min"] <= latest["Ret1d"] <= cfg["ret_max"]

    candidate = bool(
        latest["BoxValid"]
        and latest["BottomValid"]
        and latest["NearUpper"]
        and latest["VolSpike"]
        and amount_ok
        and shape_ok
        and ret_ok
    )

    strong = bool(candidate and latest["BreakUpper"])

    result = {
        "代码": code,
        "名称": name,
        "日期": latest.name.strftime("%Y-%m-%d"),
        "收盘": round(float(latest["close"]), 2),
        "成交额": round(float(latest.get("amount", np.nan)), 2) if cfg["use_amount"] else np.nan,
        "BoxWidth": round(float(latest["BoxWidth"]), 4),
        "HitRatio": round(float(latest["HitRatio"]), 3),
        "Drawdown": round(float(latest["Drawdown"]), 3),
        "BoxValid": bool(latest["BoxValid"]),
        "BottomValid": bool(latest["BottomValid"]),
        "VolSpike": bool(latest["VolSpike"]),
        "NearUpper": bool(latest["NearUpper"]),
        "BreakUpper": bool(latest["BreakUpper"]),
        "Candidate": candidate,
        "Strong": strong,
        "ClosePos": round(float(latest["ClosePos"]), 3) if not pd.isna(latest["ClosePos"]) else np.nan,
        "UpperShadow": round(float(latest["UpperShadow"]), 3) if not pd.isna(latest["UpperShadow"]) else np.nan,
    }
    return result, df


def _build_plot_df(df, code, highlight_date, plot_window, future_days=0):
    if highlight_date is None or not isinstance(highlight_date, (datetime, date)) or future_days <= 0:
        return df.tail(plot_window)
    highlight_dt = (
        datetime.combine(highlight_date, datetime.min.time()) if isinstance(highlight_date, date) else highlight_date
    )
    today_date = datetime.now().date()
    if highlight_dt.date() >= today_date:
        return df.tail(plot_window)

    display_end_date = min(highlight_dt.date() + timedelta(days=future_days), today_date)
    try:
        ext_df = get_ak_price_df(
            code,
            end_date=datetime.combine(display_end_date, datetime.min.time()).strftime("%Y%m%d"),
            count=plot_window + future_days + 20,
        )
    except Exception:
        ext_df = pd.DataFrame()

    if ext_df is None or ext_df.empty:
        return df.tail(plot_window)

    ext_df = ext_df.sort_index()
    pos_arr = ext_df.index.get_indexer([highlight_dt], method="pad")
    pos = pos_arr[0] if len(pos_arr) else -1
    if pos < 0:
        return ext_df.tail(plot_window)

    start = max(0, pos - max(plot_window - future_days - 1, 0))
    end = min(len(ext_df), pos + future_days + 1)
    subset = ext_df.iloc[start:end]
    if len(subset) < plot_window:
        subset = ext_df.iloc[max(0, end - plot_window) : end]
    return subset


def _render_box_kline_grid(title, items, plot_window, highlight_date=None, future_days=0):
    st.markdown(f"**{title}**")
    if not items:
        st.info("暂无可展示的图表")
        return

    cols_per_row = 4
    rows = len(items) // cols_per_row + int(len(items) % cols_per_row > 0)
    idx = 0
    for _ in range(rows):
        cols = st.columns(cols_per_row)
        for col in cols:
            if idx >= len(items):
                break
            label, df, _highlight_date, code = items[idx]
            col.markdown(f"**{label}**")
            plot_df = _build_plot_df(df, code, highlight_date, plot_window, future_days)
            highlight_for_plot = highlight_date
            if highlight_date is not None and highlight_date not in plot_df.index:
                idx_candidates = plot_df.index[plot_df.index <= pd.to_datetime(highlight_date)]
                if len(idx_candidates):
                    highlight_for_plot = idx_candidates[-1]
                else:
                    highlight_for_plot = None
            plotK(plot_df, container=col, highlight_date=highlight_for_plot)
            idx += 1


def _render_range_volume_breakout():
    st.title("区间放量突破")

    st.markdown("#### 参数配置")
    with st.form("box_breakout_params"):
        select_date = st.date_input("筛选日期", value=datetime.now().date(), max_value=datetime.now().date())
        cols = st.columns(3)
        with cols[0]:
            W_box = st.number_input("W_box 箱体窗口(天)", min_value=10, max_value=120, value=30)
            L_bottom = st.number_input("L_bottom 底部窗口(天)", min_value=60, max_value=400, value=120)
            V_avg = st.number_input("V_avg 量能窗口(天)", min_value=5, max_value=60, value=20)
            eps = st.number_input("eps 上沿容差", min_value=0.002, max_value=0.03, value=0.01, step=0.001)

        with cols[1]:
            box_width_min = st.number_input("箱体宽度下限", min_value=0.02, max_value=0.15, value=0.06, step=0.01)
            box_width_max = st.number_input("箱体宽度上限", min_value=0.12, max_value=0.4, value=0.25, step=0.01)
            box_hit_ratio_min = st.number_input("箱体命中率下限", min_value=0.3, max_value=0.95, value=0.6, step=0.05)
            box_hit_tol = st.number_input("箱体命中容差", min_value=0.002, max_value=0.03, value=0.01, step=0.001)

        with cols[2]:
            vol_spike_mult = st.number_input("量能放大倍数", min_value=1.2, max_value=4.0, value=1.8, step=0.1)
            lookback_days = st.number_input("历史回看天数", min_value=120, max_value=600, value=260, step=20)
            max_stocks = st.number_input("最大处理股票数", min_value=50, max_value=800, value=200, step=50)

        st.markdown("**底部条件**")
        bottom_cols = st.columns(3)
        with bottom_cols[0]:
            bottom_drawdown_min = st.number_input("回撤阈值", min_value=0.05, max_value=0.6, value=0.2, step=0.05)
            bottom_min_hits = st.number_input("底部最少满足条数", min_value=1, max_value=5, value=2, step=1)
            ma_gap_max = st.number_input("MA20/MA60 最大偏离", min_value=0.01, max_value=0.3, value=0.08, step=0.01)
        with bottom_cols[1]:
            ma_slope_window = st.number_input("均线斜率窗口", min_value=3, max_value=20, value=5, step=1)
            ma_slope_max = st.number_input("均线斜率上限", min_value=0.0, max_value=0.02, value=0.003, step=0.001)
            vol_contract_ratio = st.number_input("缩量倍数", min_value=0.5, max_value=1.0, value=0.85, step=0.05)
        with bottom_cols[2]:
            vol_converge_ratio = st.number_input("波动收敛倍数", min_value=0.5, max_value=1.0, value=0.85, step=0.05)
            use_shape_filter = st.checkbox("启用K线形态过滤", value=True)
            use_ret_filter = st.checkbox("启用单日涨跌幅过滤", value=False)

        shape_cols = st.columns(3)
        with shape_cols[0]:
            close_pos_min = st.number_input("收盘位置下限", min_value=0.4, max_value=0.9, value=0.65, step=0.05)
        with shape_cols[1]:
            upper_shadow_max = st.number_input("上影线比例上限", min_value=0.05, max_value=0.5, value=0.25, step=0.05)
        with shape_cols[2]:
            ret_min = st.number_input("单日涨跌幅下限", min_value=-0.05, max_value=0.1, value=-0.02, step=0.01)
            ret_max = st.number_input("单日涨跌幅上限", min_value=0.02, max_value=0.2, value=0.12, step=0.01)

        st.markdown("**数据与展示**")
        data_cols = st.columns(3)
        with data_cols[0]:
            use_amount = st.checkbox("使用成交额过滤", value=True)
            adjust_mode = st.selectbox("复权方式", ["qfq", "hfq", ""])
        with data_cols[1]:
            plot_window = st.number_input("绘图K线长度", min_value=60, max_value=250, value=90, step=10)
        with data_cols[2]:
            max_plots = st.number_input("最大展示数量(分类)", min_value=8, max_value=80, value=32, step=4)

        run = st.form_submit_button("开始筛选")

    if not run:
        st.info("请配置参数后点击“开始筛选”。")
        return

    end_date = datetime.combine(select_date, datetime.min.time())

    with st.spinner("获取股票池..."):
        raw_spot = get_spot_pool(select_date)
        if raw_spot is None or raw_spot.empty:
            token = st.secrets.get("tushare_token") or os.environ.get("TUSHARE_TOKEN")
            if not token:
                st.error("未配置 Tushare Token，请在 `.streamlit/secrets.toml` 或环境变量 `TUSHARE_TOKEN` 中设置")
            else:
                st.error("未能获取到 Tushare 股票池数据")
            return
        spot_df = _normalize_spot_df(raw_spot)
        if spot_df.empty:
            st.error("无法识别股票池字段")
            return

        spot_df = spot_df[~spot_df["code"].astype(str).str.startswith(("8", "9"))]

        min_mkt_cap = 50 * 1e8
        spot_df = spot_df[spot_df["mkt_cap"] >= min_mkt_cap]
        spot_df = spot_df.sort_values("amount", ascending=False).head(int(max_stocks))

    cfg = {
        "W_box": int(W_box),
        "L_bottom": int(L_bottom),
        "V_avg": int(V_avg),
        "eps": float(eps),
        "box_width_min": float(box_width_min),
        "box_width_max": float(box_width_max),
        "box_hit_ratio_min": float(box_hit_ratio_min),
        "box_hit_tol": float(box_hit_tol),
        "vol_spike_mult": float(vol_spike_mult),
        "lookback_days": int(lookback_days),
        "bottom_drawdown_min": float(bottom_drawdown_min),
        "bottom_min_hits": int(bottom_min_hits),
        "ma_gap_max": float(ma_gap_max),
        "ma_slope_window": int(ma_slope_window),
        "ma_slope_max": float(ma_slope_max),
        "vol_contract_ratio": float(vol_contract_ratio),
        "vol_converge_ratio": float(vol_converge_ratio),
        "use_shape_filter": bool(use_shape_filter),
        "close_pos_min": float(close_pos_min),
        "upper_shadow_max": float(upper_shadow_max),
        "use_ret_filter": bool(use_ret_filter),
        "ret_min": float(ret_min),
        "ret_max": float(ret_max),
        "use_amount": bool(use_amount),
        "adjust_mode": adjust_mode if adjust_mode else "",
    }

    highlight_date = select_date if select_date < datetime.now().date() else None
    future_days = 20 if highlight_date else 0

    progress = st.progress(0.0)
    status = st.empty()

    candidate_rows = []
    strong_rows = []
    candidate_plots = []
    strong_plots = []
    non_break_plots = []

    codes = spot_df["code"].tolist()
    total = len(codes)
    for idx, row in enumerate(spot_df.itertuples(index=False)):
        progress.progress((idx + 1) / total if total else 1)
        status.text(f"处理 {row.code} {row.name} ({idx + 1}/{total})")

        result, df = _evaluate_stock(row.code, row.name, cfg, end_date)
        if result is None or df is None:
            continue

        if result["Strong"]:
            strong_rows.append(result)
            if len(strong_plots) < int(max_plots):
                strong_plots.append((f"{row.code} {row.name}", df, df.index[-1], row.code))
        elif result["Candidate"]:
            candidate_rows.append(result)
            if len(candidate_plots) < int(max_plots):
                candidate_plots.append((f"{row.code} {row.name}", df, df.index[-1], row.code))
        else:
            if len(non_break_plots) < int(max_plots):
                non_break_plots.append((f"{row.code} {row.name}", df, df.index[-1], row.code))

    progress.empty()
    status.empty()

    st.markdown("#### 结果汇总")
    st.write(f"股票池数量: {len(spot_df)}")
    st.write(f"箱体候选数: {len(candidate_rows)}")
    st.write(f"箱体上沿突破数: {len(strong_rows)}")

    if candidate_rows:
        st.markdown("#### 箱体候选（待突破）")
        st.dataframe(pd.DataFrame(candidate_rows), use_container_width=True, hide_index=True)
        _render_box_kline_grid(
            "候选示例",
            candidate_plots,
            plot_window,
            highlight_date=highlight_date,
            future_days=future_days,
        )
    else:
        st.info("暂无满足候选条件的股票")

    if strong_rows:
        st.markdown("#### 箱体上沿突破（强势）")
        st.dataframe(pd.DataFrame(strong_rows), use_container_width=True, hide_index=True)
        _render_box_kline_grid(
            "强势示例",
            strong_plots,
            plot_window,
            highlight_date=highlight_date,
            future_days=future_days,
        )
    else:
        st.info("暂无满足突破条件的股票")

    st.markdown("#### 未满足条件的股票")
    _render_box_kline_grid(
        "未满足条件",
        non_break_plots,
        plot_window,
        highlight_date=highlight_date,
        future_days=future_days,
    )

def _render_concept_momentum():
    """
    筛选符合条件的概念板块
    """
    st.title("📊 同花顺概念板块筛选器")

    st.markdown("#### 参数配置（点击确认后开始筛选）")
    with st.form("filter_params"):
        st.markdown("**基准指数**：用于计算相对强弱RS（默认上证指数）。")
        bench_symbol = st.text_input(
            "基准指数代码", value="sh000001", help="如: sh000001 / sz399300"
        )

        st.markdown("**均线与新高**：控制趋势与新高判断窗口。")
        cols_ma = st.columns(3)
        with cols_ma[0]:
            enable_ma60 = st.checkbox("启用 收盘价MA60", value=True, help="过滤弱势板块")
        with cols_ma[1]:
            enable_ma20_slope = st.checkbox("启用 MA20斜率", value=True, help="均线向上更强")
            ma20_slope_period = st.number_input(
                "MA20斜率窗口", min_value=3, max_value=30, value=5, help="斜率回归天数"
            )
        with cols_ma[2]:
            enable_new_high = st.checkbox("启用 新高筛选", value=True, help="创阶段新高优先")
            new_high_window = st.number_input(
                "新高窗口(日)", min_value=20, max_value=250, value=60, help="新高统计窗口"
            )

        st.markdown("**RS 强弱**：RS 创新高或不破位优先。")
        cols_rs = st.columns(3)
        with cols_rs[0]:
            enable_rs = st.checkbox("启用 RS 强弱", value=True, help="相对市场更强")
            rs_high_window = st.number_input(
                "RS新高窗口(日)", min_value=20, max_value=250, value=60, help="RS新高判断窗口"
            )
        with cols_rs[1]:
            rs_slope_20_window = st.number_input(
                "RS斜率(短)窗口", min_value=10, max_value=60, value=20, help="RS短期斜率窗口"
            )
        with cols_rs[2]:
            rs_slope_60_window = st.number_input(
                "RS斜率(长)窗口", min_value=20, max_value=120, value=60, help="RS长期斜率窗口"
            )

        st.markdown("**趋势强度**：趋势强弱与结构性上行确认。")
        cols_trend = st.columns(3)
        with cols_trend[0]:
            enable_adx = st.checkbox("启用 ADX", value=True, help="趋势强度上升且足够强")
            adx14_threshold = st.slider(
                "ADX14阈值", min_value=10, max_value=40, value=20, help="ADX14阈值"
            )
            adx20_threshold = st.slider(
                "ADX20阈值", min_value=10, max_value=50, value=25, help="ADX20阈值"
            )
        with cols_trend[1]:
            enable_ma_align = st.checkbox("启用 多头排列", value=True, help="MA20>MA60>MA120")
            ma_align_ratio_threshold = st.slider(
                "多头排列占比阈值", min_value=0.0, max_value=1.0, value=0.6, help="近60日占比"
            )
        with cols_trend[2]:
            enable_hhhl = st.checkbox("启用 HH/HL", value=True, help="更高高点/低点结构")
            hhhl_window = st.number_input(
                "HH/HL统计窗口(日)", min_value=10, max_value=60, value=20, help="统计窗口"
            )
            hhhl_count_threshold = st.number_input(
                "HH/HL最小次数", min_value=1, max_value=20, value=8, help="窗口内次数"
            )

        st.markdown("**量能确认**：放量突破与成交结构。")
        cols_vol = st.columns(2)
        with cols_vol[0]:
            enable_volume = st.checkbox("启用 量能确认", value=True, help="量价配合更可靠")
            volume_ratio_threshold = st.slider(
                "放量突破量比阈值", min_value=1.0, max_value=3.0, value=1.5, help="当日量/20日均量"
            )
        with cols_vol[1]:
            up_volume_ratio_threshold = st.slider(
                "上涨成交占比阈值", min_value=0.5, max_value=0.8, value=0.55, help="上行成交占比"
            )

        st.markdown("**风险维度**：回撤/下行波动/ATR 限制波动风险。")
        cols_risk = st.columns(3)
        with cols_risk[0]:
            enable_max_dd = st.checkbox("启用 最大回撤", value=True, help="控制阶段回撤")
            max_dd_60_threshold = st.slider(
                "最大回撤(60D)阈值", min_value=0.05, max_value=0.4, value=0.2, help="更小更稳"
            )
        with cols_risk[1]:
            enable_downside_vol = st.checkbox("启用 下行波动", value=True, help="负收益波动")
            downside_vol_60_threshold = st.slider(
                "下行波动(60D)阈值", min_value=0.01, max_value=0.1, value=0.035, help="更小更稳"
            )
        with cols_risk[2]:
            enable_atr = st.checkbox("启用 ATR%", value=True, help="日内波动")
            atr_pct_threshold = st.slider(
                "ATR%阈值", min_value=0.01, max_value=0.2, value=0.06, help="更小更稳"
            )

        run_filter = st.form_submit_button("确认开始筛选")

    if not run_filter:
        st.info("请在上方配置参数后点击“确认开始筛选”。")
        return

    with st.spinner("正在获取概念板块列表..."):
        concept_list = get_concept_list()
        if concept_list is None or concept_list.empty:
            st.error("获取概念板块列表失败")
            return
        st.success(f"成功获取 {len(concept_list)} 个概念板块")

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=420)).strftime("%Y%m%d")

    st.info(f"数据时间范围: {start_date} 至 {end_date}")

    progress_bar = st.progress(0)
    status_text = st.empty()

    qualified_concepts = []
    plot_items = []

    benchmark_df = get_benchmark_kline(start_date, end_date, symbol=bench_symbol)
    if benchmark_df is None or len(benchmark_df) < 120:
        st.error("基准指数数据不足，无法计算相对强弱")
        return

    total_concepts = len(concept_list)

    for idx, row in concept_list.iterrows():
        concept_name = row["name"]

        progress = (idx + 1) / total_concepts
        progress_bar.progress(progress)
        status_text.text(f"正在分析: {concept_name} ({idx + 1}/{total_concepts})")

        hist_data = get_concept_kline_data(concept_name, start_date, end_date)

        if hist_data is None or len(hist_data) < 120:
            continue

        aligned = hist_data.join(
            benchmark_df[["close"]].rename(columns={"close": "benchmark_close"}), how="inner"
        )
        if len(aligned) < 120:
            continue

        aligned["MA20"] = calculate_ma(aligned["close"], 20)
        aligned["MA60"] = calculate_ma(aligned["close"], 60)
        aligned["MA120"] = calculate_ma(aligned["close"], 120)

        latest = aligned.iloc[-1]

        condition1 = (not enable_ma60) or (latest["close"] > latest["MA60"])

        ma20_slope = calculate_ma_slope(aligned["MA20"].dropna(), period=int(ma20_slope_period))
        condition2 = (not enable_ma20_slope) or (ma20_slope > 0)

        recent_60_high = aligned["close"].tail(int(new_high_window)).max()
        condition3 = (not enable_new_high) or (latest["close"] >= recent_60_high)

        aligned["RS"] = aligned["close"] / aligned["benchmark_close"]
        rs_latest = aligned["RS"].iloc[-1]
        rs_60_high = aligned["RS"].tail(int(rs_high_window)).max()
        rs_ma60 = aligned["RS"].rolling(60).mean().iloc[-1]
        rs_slope_20 = calculate_ma_slope(aligned["RS"].dropna(), period=int(rs_slope_20_window))
        rs_slope_60 = calculate_ma_slope(aligned["RS"].dropna(), period=int(rs_slope_60_window))
        rs_condition = (rs_latest >= rs_60_high) or (rs_latest >= rs_ma60 and rs_slope_20 >= 0)
        condition4 = (not enable_rs) or rs_condition

        aligned["ADX14"] = calculate_adx(aligned, period=14)
        aligned["ADX20"] = calculate_adx(aligned, period=20)
        adx14_latest = aligned["ADX14"].iloc[-1]
        adx20_latest = aligned["ADX20"].iloc[-1]
        adx14_prev = aligned["ADX14"].iloc[-2] if len(aligned) > 2 else np.nan
        condition5_adx = (not enable_adx) or (
            adx14_latest > adx14_threshold
            and adx20_latest > adx20_threshold
            and adx14_latest > adx14_prev
        )

        ma_align_mask = (aligned["MA20"] > aligned["MA60"]) & (aligned["MA60"] > aligned["MA120"])
        ma_align_ratio = ma_align_mask.tail(60).mean()
        condition5_ma = (not enable_ma_align) or (ma_align_ratio >= ma_align_ratio_threshold)

        hh_hl_count = (
            (aligned["high"] > aligned["high"].shift(1))
            & (aligned["low"] > aligned["low"].shift(1))
        ).tail(int(hhhl_window)).sum()
        condition5_hhhl = (not enable_hhhl) or (hh_hl_count >= hhhl_count_threshold)

        condition5 = condition5_adx and condition5_ma and condition5_hhhl

        volume_ratio = aligned["volume"] / aligned["volume"].rolling(20).mean()
        prev_high_60 = aligned["high"].shift(1).rolling(60).max().iloc[-1]
        breakout_volume = latest["close"] > prev_high_60 and volume_ratio.iloc[-1] > volume_ratio_threshold

        up_volume = aligned.loc[aligned["close"] >= aligned["open"], "volume"].tail(20).sum()
        total_volume = aligned["volume"].tail(20).sum()
        up_volume_ratio = up_volume / total_volume if total_volume > 0 else 0

        obv = calculate_obv(aligned)
        obv_slope = calculate_ma_slope(obv.dropna(), period=20)
        adl = calculate_adl(aligned)
        adl_slope = calculate_ma_slope(adl.dropna(), period=20)

        vol_ma20_slope = calculate_ma_slope(aligned["volume"].rolling(20).mean().dropna(), period=5)
        volume_condition = (
            breakout_volume
            or (up_volume_ratio > up_volume_ratio_threshold and obv_slope > 0)
        ) and adl_slope > 0 and vol_ma20_slope > 0
        condition6 = (not enable_volume) or volume_condition

        returns = aligned["close"].pct_change()
        max_dd_60 = calculate_max_drawdown(aligned["close"], 60)
        downside_vol_60 = calculate_downside_vol(returns, 60)
        atr14 = calculate_atr(aligned, 14)
        atr_pct = (atr14 / aligned["close"]).iloc[-1]
        condition7 = True
        if enable_max_dd:
            condition7 = condition7 and (abs(max_dd_60) < max_dd_60_threshold)
        if enable_downside_vol:
            condition7 = condition7 and (downside_vol_60 < downside_vol_60_threshold)
        if enable_atr:
            condition7 = condition7 and (atr_pct < atr_pct_threshold)

        if condition1 and condition2 and condition3 and condition4 and condition5 and condition6 and condition7:
            qualified_concepts.append(
                {
                    "概念名称": concept_name,
                    "最新收盘价": latest["close"],
                    "MA20": round(latest["MA20"], 2),
                    "MA60": round(latest["MA60"], 2),
                    "MA120": round(latest["MA120"], 2),
                    "MA20斜率": round(ma20_slope, 4),
                    "RS": round(rs_latest, 4),
                    "RS20斜率": round(rs_slope_20, 4),
                    "RS60斜率": round(rs_slope_60, 4),
                    "ADX14": round(adx14_latest, 2),
                    "ADX20": round(adx20_latest, 2),
                    "多头排列占比": round(ma_align_ratio, 2),
                    "HH/HL次数": int(hh_hl_count),
                    "量比": round(volume_ratio.iloc[-1], 2),
                    "上涨成交占比": round(up_volume_ratio, 2),
                    "OBV斜率": round(obv_slope, 2),
                    "ADL斜率": round(adl_slope, 2),
                    "最大回撤(60D)": round(max_dd_60, 4),
                    "下行波动(60D)": round(downside_vol_60, 4),
                    "ATR%": round(atr_pct, 4),
                    "成交量": latest["volume"],
                    "成交额": latest.get("amount", np.nan),
                    "日期": latest.name,
                }
            )
            plot_items.append(
                (concept_name, aligned[["open", "high", "low", "close", "volume"]].tail(100))
            )

    progress_bar.empty()
    status_text.empty()

    if qualified_concepts:
        result_df = pd.DataFrame(qualified_concepts)
        result_df = result_df.sort_values("MA20斜率", ascending=False)

        st.success(f"✅ 找到 {len(result_df)} 个符合条件的概念板块")

        st.dataframe(
            result_df,
            use_container_width=True,
            hide_index=True,
        )

        csv = result_df.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            label="📥 下载筛选结果(CSV)",
            data=csv,
            file_name=f"concept_filter_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

        st.markdown("### 📊 板块K线")
        columns_per_row = 4
        rows = len(plot_items) // columns_per_row + int(len(plot_items) % columns_per_row > 0)
        for i in range(rows):
            cols = st.columns(columns_per_row)
            for j in range(columns_per_row):
                index = i * columns_per_row + j
                if index < len(plot_items):
                    name, plot_df = plot_items[index]
                    cols[j].markdown(f"**{name}**")
                    plotK(plot_df, container=cols[j])
    else:
        st.warning("⚠️ 没有找到符合条件的概念板块")

def _render_stock_momentum():
    st.title("个股动量")
    st.markdown("#### 概念板块筛选")

    concept_df = get_em_concept_list()
    if concept_df is None or concept_df.empty:
        st.error("概念板块列表获取失败")
        return

    name_col = _pick_first_column(concept_df, ["板块名称", "名称", "概念名称", "name"])
    if name_col is None:
        st.error("概念板块名称列缺失，无法筛选")
        return

    search_keyword = st.text_input("按名称搜索", value="")
    display_df = concept_df
    if search_keyword:
        display_df = concept_df[
            concept_df[name_col].astype(str).str.contains(search_keyword, case=False, na=False)
        ]

    options = display_df[name_col].dropna().unique().tolist()
    selected_names = st.multiselect("选择概念板块（可多选）", options=options)

    if not selected_names:
        st.info("请选择一个或多个概念板块")
        return

    with st.spinner("正在拉取成分股并合并去重..."):
        all_rows = []
        members_map = {}
        for name in selected_names:
            df = get_em_concept_cons(name)
            if df is None or df.empty:
                continue
            df = df.copy()
            df["概念板块"] = name
            members_map[name] = df
            all_rows.append(df)

    if not all_rows:
        st.warning("未获取到成分股数据")
        return

    combined_raw = pd.concat(all_rows, ignore_index=True)
    code_col = _pick_first_column(combined_raw, ["代码", "股票代码", "symbol", "code"])
    name_col = _pick_first_column(combined_raw, ["名称", "股票名称", "简称", "name"])
    sector_col = _pick_first_column(combined_raw, ["概念板块", "板块", "概念"])

    concept_map = {}
    if code_col and sector_col:
        for code, group in combined_raw.groupby(code_col):
            concept_map[str(code)] = sorted(set(group[sector_col].dropna().astype(str).tolist()))

    if code_col is not None:
        combined = combined_raw.drop_duplicates(subset=[code_col])
    else:
        combined = combined_raw.drop_duplicates()

    st.dataframe(combined, use_container_width=True, hide_index=True)

    st.markdown("#### 动量筛选")
    with st.expander("参数设置", expanded=True):
        st.markdown(
            "- 基准指数代码\n"
            "- 领导股成交额下限(亿元)\n"
            "- 与合成板块相关性下限(20日)\n"
            "- 领导股数量\n"
            "- 中军数量\n"
            "- 底部启动数量\n"
            "- 底部启动成交额下限(亿元)\n"
            "- 最多处理股票数"
        )
        bench_symbol = st.text_input("基准指数代码", value="sh000001")
        amount_min = st.number_input("领导股成交额下限(亿元)", min_value=0.1, value=5.0)
        corr_min = st.slider("与合成板块相关性下限(20日)", min_value=0.0, max_value=1.0, value=0.2)
        top_leader = st.number_input("领导股数量", min_value=1, value=3, step=1)
        top_core = st.number_input("中军数量", min_value=1, value=5, step=1)
        top_bottom = st.number_input("底部启动数量", min_value=1, value=8, step=1)
        bottom_amount_min = st.number_input("底部启动成交额下限(亿元)", min_value=0.1, value=3.0)
        max_stocks_total = st.number_input("最多处理股票数", min_value=30, max_value=500, value=200, step=10)

    run_filter = st.button("动量筛选")
    if not run_filter:
        return

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=420)).strftime("%Y%m%d")

    benchmark_df = get_benchmark_kline(start_date, end_date, symbol=bench_symbol)
    if benchmark_df is None or benchmark_df.empty:
        st.error("基准指数数据获取失败")
        return

    sector_closes = []
    sector_amounts = []
    for sector_name in selected_names:
        sector_index = get_em_concept_hist(sector_name, start_date, end_date)
        if sector_index is None or sector_index.empty:
            continue
        sector_closes.append(sector_index["close"].rename(sector_name))
        amount_series = sector_index.get("amount")
        if amount_series is None:
            amount_series = sector_index.get("volume")
        if amount_series is not None:
            sector_amounts.append(amount_series.rename(sector_name))

    if not sector_closes:
        st.error("选中板块没有可用的指数数据")
        return

    sector_close_df = pd.concat(sector_closes, axis=1).dropna()
    sector_norm = sector_close_df / sector_close_df.iloc[0]
    sector_index = pd.DataFrame({"close": sector_norm.mean(axis=1)})
    if sector_amounts:
        amount_df = pd.concat(sector_amounts, axis=1).reindex(sector_index.index).dropna()
        sector_index["amount"] = amount_df.mean(axis=1)

    sector_stage = _detect_sector_stage(sector_index)

    codes = []
    if code_col is not None:
        codes = combined[code_col].dropna().astype(str).unique().tolist()
    codes = codes[: int(max_stocks_total)]

    if not codes:
        st.warning("没有可用的股票代码")
        return

    sector_close = sector_index["close"]
    sector_dd_60 = calculate_max_drawdown(sector_close, 60)
    if {"high", "low", "close"}.issubset(sector_index.columns):
        sector_atr_pct = (calculate_atr(sector_index, 20) / sector_close).iloc[-1]
    else:
        sector_atr_pct = sector_close.pct_change().rolling(20).std().iloc[-1]
    if pd.isna(sector_atr_pct):
        sector_atr_pct = 0

    results = []
    with st.spinner("正在计算合并股票池的动量..."):
        for code in codes:
            stock_df = get_stock_hist(code, start_date, end_date)
            if stock_df is None or stock_df.empty or len(stock_df) < 120:
                continue

            aligned = stock_df.join(
                sector_index[["close"]].rename(columns={"close": "sector_close"}), how="inner"
            )
            aligned = aligned.join(
                benchmark_df[["close"]].rename(columns={"close": "bench_close"}), how="inner"
            )
            if len(aligned) < 120:
                continue

            close = aligned["close"]
            amount = aligned.get("amount")
            if amount is None:
                amount = aligned.get("volume")
            ma60 = calculate_ma(close, 60)
            ma60_slope = calculate_ma_slope(ma60.dropna(), period=10)
            avg_amount_20 = amount.rolling(20).mean().iloc[-1] if amount is not None else 0
            avg_amount_60 = amount.rolling(60).mean().iloc[-1] if amount is not None else 0

            stock_ret = close.pct_change().dropna()
            sector_ret_aligned = aligned["sector_close"].pct_change().dropna()
            corr_20 = stock_ret.tail(20).corr(sector_ret_aligned.tail(20)) if len(stock_ret) >= 20 else 0

            if avg_amount_20 < amount_min * 1e8:
                continue
            if close.iloc[-1] <= ma60.iloc[-1] or ma60_slope <= 0:
                continue
            if corr_20 < corr_min:
                continue

            hh_120 = close.rolling(120).max().iloc[-1]
            hh_60 = close.rolling(60).max().iloc[-1]
            breakout_flag = close.iloc[-1] >= hh_60
            new_high_flag = close.iloc[-1] >= hh_120

            rs_sec = close / aligned["sector_close"]
            rs_mkt = close / aligned["bench_close"]
            rs_sec_slope_20 = calculate_ma_slope(rs_sec.dropna(), period=20)
            rs_mkt_slope_20 = calculate_ma_slope(rs_mkt.dropna(), period=20)

            sector_breakout_flag = aligned["sector_close"] >= aligned["sector_close"].rolling(60).max()
            stock_breakout_flag = close >= close.rolling(60).max()
            sector_breakout_date = (
                sector_breakout_flag[sector_breakout_flag].index[-1]
                if sector_breakout_flag.any()
                else aligned.index[-1]
            )
            stock_breakout_date = (
                stock_breakout_flag[stock_breakout_flag].index[-1]
                if stock_breakout_flag.any()
                else aligned.index[-1]
            )
            lead_lag_days = (sector_breakout_date - stock_breakout_date).days

            excess = stock_ret.tail(10).values - sector_ret_aligned.tail(10).values
            consistency = (excess > 0).mean() if len(excess) else 0

            down_mask = sector_ret_aligned < 0
            if down_mask.any():
                resilience = stock_ret[down_mask].mean() - sector_ret_aligned[down_mask].mean()
            else:
                resilience = 0

            atr_pct = (calculate_atr(aligned, 20) / close).iloc[-1]
            max_dd_60 = calculate_max_drawdown(close, 60)

            price_score = (1 if new_high_flag else 0) + (1 if breakout_flag else 0)
            lead_score = max(0, min(10, lead_lag_days)) / 10
            rs_score = (1 if rs_sec_slope_20 > 0 else 0) + (1 if rs_mkt_slope_20 > 0 else 0)
            pricing_score = consistency + max(0, resilience)

            if sector_stage == "start":
                w_price, w_rs, w_ignite, w_pricing = 0.35, 0.25, 0.25, 0.15
            elif sector_stage == "uptrend":
                w_price, w_rs, w_ignite, w_pricing = 0.25, 0.35, 0.15, 0.25
            else:
                w_price, w_rs, w_ignite, w_pricing = 0.2, 0.25, 0.15, 0.4

            score_leader = (
                w_price * price_score
                + w_rs * rs_score
                + w_ignite * lead_score
                + w_pricing * pricing_score
            )

            stock_name = ""
            if name_col:
                name_match = combined_raw.loc[combined_raw[code_col].astype(str) == str(code), name_col]
                if not name_match.empty:
                    stock_name = name_match.iloc[0]

            fire_flag = lead_lag_days > 0
            results.append(
                {
                    "点火": fire_flag,
                    "新高60": bool(breakout_flag),
                    "新高120": bool(new_high_flag),
                    "代码": code,
                    "名称": stock_name,
                    "所属板块": ",".join(concept_map.get(str(code), [])) if concept_map else "",
                    "合成阶段": sector_stage,
                    "avg_amount_20": round(avg_amount_20, 2),
                    "avg_amount_60": round(avg_amount_60, 2),
                    "ma60_slope": round(ma60_slope, 4),
                    "corr_20": round(corr_20, 4),
                    "rs_sec_slope_20": round(rs_sec_slope_20, 4),
                    "rs_mkt_slope_20": round(rs_mkt_slope_20, 4),
                    "lead_lag_days": int(lead_lag_days),
                    "consistency": round(consistency, 3),
                    "resilience": round(resilience, 4),
                    "atr_pct": round(atr_pct, 4),
                    "max_dd_60": round(max_dd_60, 4),
                    "S_leader": round(score_leader, 4),
                }
            )

    if not results:
        st.warning("没有符合条件的个股")
        return

    result_df = pd.DataFrame(results)
    leader_pool = result_df.sort_values("S_leader", ascending=False).head(int(top_leader))

    result_df["liquidity_rank"] = result_df["avg_amount_60"].rank(pct=True)
    result_df["atr_rank"] = result_df["atr_pct"].rank(pct=True)
    result_df["dd_rank"] = result_df["max_dd_60"].rank(pct=True)
    result_df["liquidity_score"] = result_df["liquidity_rank"]
    result_df["stability_score"] = 1 - (result_df["atr_rank"] * 0.5 + result_df["dd_rank"] * 0.5)
    result_df["anchor_score"] = result_df["corr_20"].clip(lower=0)
    result_df["rs_bonus"] = (result_df["rs_sec_slope_20"] > 0).astype(int)

    if sector_stage == "uptrend":
        a, b, c, d = 0.35, 0.25, 0.25, 0.15
    elif sector_stage == "divergence":
        a, b, c, d = 0.3, 0.35, 0.25, 0.1
    else:
        a, b, c, d = 0.35, 0.2, 0.25, 0.2

    result_df["S_core"] = (
        a * result_df["liquidity_score"]
        + b * result_df["stability_score"]
        + c * result_df["anchor_score"]
        + d * result_df["rs_bonus"]
    )

    result_df["core_type"] = np.where(
        (result_df["rs_sec_slope_20"] > 0) & (result_df["max_dd_60"] > sector_dd_60),
        "offensive",
        "defensive",
    )

    core_pool = result_df.sort_values("S_core", ascending=False).head(int(top_core))

    leader_codes = set(leader_pool["代码"].astype(str)) if "代码" in leader_pool.columns else set()
    core_codes = set(core_pool["代码"].astype(str)) if "代码" in core_pool.columns else set()
    excluded_codes = leader_codes | core_codes

    bottom_candidates = result_df.copy()
    if "代码" in bottom_candidates.columns and excluded_codes:
        bottom_candidates = bottom_candidates[~bottom_candidates["代码"].astype(str).isin(excluded_codes)]

    if not bottom_candidates.empty:
        bottom_rows = []
        for _, row in bottom_candidates.iterrows():
            code = str(row.get("代码", "")).strip()
            if not code:
                continue
            stock_df = get_stock_hist(code, start_date, end_date)
            if stock_df is None or stock_df.empty or len(stock_df) < 250:
                continue
            aligned = stock_df.join(
                sector_index[["close"]].rename(columns={"close": "sector_close"}), how="inner"
            )
            if len(aligned) < 250:
                continue
            close = aligned["close"]
            amount = aligned.get("amount")
            if amount is None:
                amount = aligned.get("volume")
            avg_amount_20 = amount.rolling(20).mean().iloc[-1] if amount is not None else 0
            if avg_amount_20 < bottom_amount_min * 1e8:
                continue

            ret_20 = close.pct_change(20).iloc[-1]
            low_60 = close.rolling(60).min()
            no_new_low = (close.tail(20) > low_60.tail(20)).all()

            atr20 = calculate_atr(aligned, 20)
            atr40 = calculate_atr(aligned, 40)
            atr_pct_20 = (atr20 / close).iloc[-1]
            atr_pct_40 = (atr40 / close).iloc[-1]
            vol_contraction = atr_pct_20 < atr_pct_40

            ma20 = calculate_ma(close, 20)
            ma60 = calculate_ma(close, 60)
            ma20_slope = calculate_ma_slope(ma20.dropna(), period=10)
            ma60_slope = calculate_ma_slope(ma60.dropna(), period=10)
            ma_turn = ma20_slope >= 0 and ma60_slope > -0.0001

            higher_low_flag = close.tail(20).min() > close.tail(40).min()

            range_250_high = close.rolling(250).max().iloc[-1]
            range_250_low = close.rolling(250).min().iloc[-1]
            range_pct_250 = 0
            if range_250_high > range_250_low:
                range_pct_250 = (close.iloc[-1] - range_250_low) / (range_250_high - range_250_low)
            drawdown_250h = close.iloc[-1] / range_250_high - 1 if range_250_high else 0
            dist_120l = close.iloc[-1] / close.rolling(120).min().iloc[-1] - 1

            box_high = close.tail(60).max()
            box_break = close.iloc[-1] > box_high
            vol_ratio = amount.iloc[-1] / amount.rolling(20).mean().iloc[-1] if amount is not None else 0
            trigger_flag = box_break and vol_ratio > 1.2
            confirm_flag = (close.tail(2) > box_high).all() if len(close) >= 2 else False

            rs_sec = close / aligned["sector_close"]
            rs_sec_slope_20 = calculate_ma_slope(rs_sec.dropna(), period=20)
            rs_turn = rs_sec_slope_20 > 0

            risk_noise = atr_pct_20 > sector_atr_pct * 1.5
            risk_penalty = 1 if risk_noise else 0

            if not (ret_20 > -0.1 or no_new_low or vol_contraction):
                continue
            if not rs_turn:
                continue

            position_score = (1 - range_pct_250) + max(0, -drawdown_250h) + max(0, 0.05 - dist_120l)
            base_score = (1 if vol_contraction else 0) + (1 if ma_turn else 0) + (1 if higher_low_flag else 0)
            trigger_score = (1 if trigger_flag else 0) + (1 if confirm_flag else 0)
            rs_score = 1 if rs_turn else 0
            liquidity_score = 1 if avg_amount_20 >= bottom_amount_min * 1e8 else 0

            if sector_stage == "divergence":
                w_p, w_b, w_t, w_r, w_l, w_k = 0.3, 0.35, 0.15, 0.15, 0.1, 0.2
            else:
                w_p, w_b, w_t, w_r, w_l, w_k = 0.25, 0.25, 0.25, 0.2, 0.1, 0.15

            s_bottom = (
                w_p * position_score
                + w_b * base_score
                + w_t * trigger_score
                + w_r * rs_score
                + w_l * liquidity_score
                - w_k * risk_penalty
            )

            bottom_rows.append(
                {
                    "代码": code,
                    "名称": row.get("名称", ""),
                    "所属板块": row.get("所属板块", ""),
                    "S_bottom": round(s_bottom, 4),
                    "range_pct_250": round(range_pct_250, 4),
                    "drawdown_250h": round(drawdown_250h, 4),
                    "dist_120l": round(dist_120l, 4),
                    "vol_contraction": bool(vol_contraction),
                    "ma_turn": bool(ma_turn),
                    "higher_low": bool(higher_low_flag),
                    "trigger": bool(trigger_flag),
                    "confirm": bool(confirm_flag),
                    "rs_sec_slope_20": round(rs_sec_slope_20, 4),
                    "risk_penalty": int(risk_penalty),
                }
            )

        bottom_pool = pd.DataFrame(bottom_rows)
    else:
        bottom_pool = pd.DataFrame()

    def _render_kline_grid(title, pool_df, desc_builder):
        st.markdown(f"**{title}**")
        items = []
        for _, row in pool_df.iterrows():
            code = str(row.get("代码", "")).strip()
            if not code:
                continue
            hist = get_stock_hist(code, start_date, end_date)
            if hist is None or hist.empty:
                continue
            kline_df = hist[["open", "high", "low", "close", "volume"]].tail(100)
            name = str(row.get("名称", "")).strip()
            label = f"{code} {name}".strip()
            desc = desc_builder(row)
            items.append((label, kline_df, desc))

        if not items:
            st.info("暂无可展示的K线数据")
            return

        columns_per_row = 4
        rows = len(items) // columns_per_row + int(len(items) % columns_per_row > 0)
        idx = 0
        for _ in range(rows):
            cols = st.columns(columns_per_row)
            for col in cols:
                if idx >= len(items):
                    break
                label, kline_df, desc = items[idx]
                col.markdown(f"**{label}**")
                col.caption(desc)
                plotK(kline_df, container=col)
                idx += 1

    st.subheader(f"合并股票池 | 合成阶段: {sector_stage}")

    _render_kline_grid(
        "领导股池",
        leader_pool,
        lambda row: (
            f"动量: S_leader {row.get('S_leader', 0):.2f} | "
            f"RS(板块) {row.get('rs_sec_slope_20', 0):.2f} | "
            f"RS(市场) {row.get('rs_mkt_slope_20', 0):.2f} "
            f"{'🔥' if row.get('点火') else ''}{'60H' if row.get('新高60') else ''}{'120H' if row.get('新高120') else ''}"
        ),
    )

    _render_kline_grid(
        "容量中军池",
        core_pool,
        lambda row: (
            f"动量: S_core {row.get('S_core', 0):.2f} | "
            f"类型 {row.get('core_type', '')} | "
            f"liquidity {row.get('liquidity_score', 0):.2f}"
        ),
    )

    _render_kline_grid(
        "底部启动池",
        bottom_pool.sort_values("S_bottom", ascending=False).head(int(top_bottom))
        if not bottom_pool.empty
        else bottom_pool,
        lambda row: (
            f"动量: S_bottom {row.get('S_bottom', 0):.2f} | "
            f"RS(板块) {row.get('rs_sec_slope_20', 0):.2f} | "
            f"触发 {'是' if row.get('trigger') else '否'}"
        ),
    )


def filter_concepts():
    tabs = st.tabs(["板块动量", "个股动量", "区间放量突破"])
    with tabs[0]:
        _render_concept_momentum()
    with tabs[1]:
        _render_stock_momentum()
    with tabs[2]:
        _render_range_volume_breakout()


if __name__ == "__main__":
    filter_concepts()
