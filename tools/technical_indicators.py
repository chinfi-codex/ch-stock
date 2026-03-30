#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
常用技术指标本地计算模块
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd


FULL_GROUP_FIELDS = {
    "trend": [
        "ma_qfq_5",
        "ma_qfq_10",
        "ma_qfq_20",
        "ma_qfq_30",
        "ma_qfq_60",
        "ma_qfq_90",
        "ma_qfq_250",
        "ema_qfq_5",
        "ema_qfq_10",
        "ema_qfq_20",
        "ema_qfq_30",
        "ema_qfq_60",
        "ema_qfq_90",
        "ema_qfq_250",
        "bbi_qfq",
        "macd_qfq",
        "macd_dif_qfq",
        "macd_dea_qfq",
    ],
    "momentum": [
        "rsi_qfq_6",
        "rsi_qfq_12",
        "rsi_qfq_24",
        "roc_qfq",
        "maroc_qfq",
        "mtm_qfq",
        "mtmma_qfq",
        "trix_qfq",
        "trma_qfq",
    ],
    "oscillation": [
        "kdj_k_qfq",
        "kdj_d_qfq",
        "kdj_qfq",
        "wr_qfq",
        "wr1_qfq",
        "cci_qfq",
        "bias1_qfq",
        "bias2_qfq",
        "bias3_qfq",
    ],
    "volatility": [
        "boll_upper_qfq",
        "boll_mid_qfq",
        "boll_lower_qfq",
        "atr_qfq",
    ],
    "sentiment_volume": [
        "obv_qfq",
        "mfi_qfq",
        "vr_qfq",
        "psy_qfq",
        "psyma_qfq",
    ],
    "trend_strength": [
        "dmi_pdi_qfq",
        "dmi_mdi_qfq",
        "dmi_adx_qfq",
        "dmi_adxr_qfq",
    ],
    "state": [
        "updays",
        "downdays",
        "topdays",
        "lowdays",
    ],
}

SUMMARY_FIELDS = [
    "ma_qfq_20",
    "ma_qfq_60",
    "ma_qfq_250",
    "macd_qfq",
    "macd_dif_qfq",
    "rsi_qfq_12",
    "kdj_k_qfq",
    "boll_mid_qfq",
    "atr_qfq",
    "mfi_qfq",
    "dmi_adx_qfq",
    "updays",
    "downdays",
    "topdays",
    "lowdays",
]

REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume"]


def _ensure_price_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        raise ValueError("price dataframe is empty")

    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"missing required columns: {missing_cols}")

    result = df.copy()
    if "date" in result.columns:
        result["date"] = pd.to_datetime(result["date"])
        result = result.set_index("date")
    elif not isinstance(result.index, pd.DatetimeIndex):
        result.index = pd.to_datetime(result.index)

    result = result.sort_index()
    for col in REQUIRED_COLUMNS:
        result[col] = pd.to_numeric(result[col], errors="coerce")
    return result


def _serialize_value(value):
    if isinstance(value, (np.floating, float)):
        if pd.isna(value):
            return None
        return float(value)
    if isinstance(value, (np.integer, int)):
        return int(value)
    if pd.isna(value):
        return None
    return value


def _rolling_rma(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(alpha=1 / period, adjust=False).mean()


def calculate_ma(df: pd.DataFrame, windows: Iterable[int]) -> pd.DataFrame:
    result = pd.DataFrame(index=df.index)
    for window in windows:
        result[f"ma_qfq_{window}"] = df["close"].rolling(window=window, min_periods=window).mean()
    return result


def calculate_ema(df: pd.DataFrame, windows: Iterable[int]) -> pd.DataFrame:
    result = pd.DataFrame(index=df.index)
    for window in windows:
        result[f"ema_qfq_{window}"] = df["close"].ewm(span=window, adjust=False).mean()
    return result


def calculate_macd(
    df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9
) -> pd.DataFrame:
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    return pd.DataFrame(
        {
            "macd_dif_qfq": dif,
            "macd_dea_qfq": dea,
            "macd_qfq": (dif - dea) * 2,
        },
        index=df.index,
    )


def calculate_rsi(df: pd.DataFrame, windows: Iterable[int]) -> pd.DataFrame:
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    result = pd.DataFrame(index=df.index)
    for window in windows:
        avg_gain = _rolling_rma(gain, window)
        avg_loss = _rolling_rma(loss, window)
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        result[f"rsi_qfq_{window}"] = rsi.fillna(100).where(avg_loss.ne(0), 100)
        result.loc[(avg_gain == 0) & (avg_loss == 0), f"rsi_qfq_{window}"] = 50
    return result


def calculate_kdj(
    df: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3
) -> pd.DataFrame:
    low_min = df["low"].rolling(window=n, min_periods=n).min()
    high_max = df["high"].rolling(window=n, min_periods=n).max()
    rsv = (df["close"] - low_min) / (high_max - low_min).replace(0, np.nan) * 100
    k = rsv.ewm(alpha=1 / m1, adjust=False).mean()
    d = k.ewm(alpha=1 / m2, adjust=False).mean()
    j = 3 * k - 2 * d
    return pd.DataFrame(
        {"kdj_k_qfq": k, "kdj_d_qfq": d, "kdj_qfq": j},
        index=df.index,
    )


def calculate_boll(df: pd.DataFrame, n: int = 20, p: int = 2) -> pd.DataFrame:
    mid = df["close"].rolling(window=n, min_periods=n).mean()
    std = df["close"].rolling(window=n, min_periods=n).std(ddof=0)
    return pd.DataFrame(
        {
            "boll_mid_qfq": mid,
            "boll_upper_qfq": mid + p * std,
            "boll_lower_qfq": mid - p * std,
        },
        index=df.index,
    )


def calculate_atr(df: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return pd.DataFrame({"atr_qfq": tr.rolling(window=n, min_periods=n).mean()}, index=df.index)


def calculate_cci(df: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    ma = typical_price.rolling(window=n, min_periods=n).mean()
    md = typical_price.rolling(window=n, min_periods=n).apply(
        lambda x: np.mean(np.abs(x - np.mean(x))), raw=True
    )
    cci = (typical_price - ma) / (0.015 * md.replace(0, np.nan))
    return pd.DataFrame({"cci_qfq": cci}, index=df.index)


def calculate_wr(df: pd.DataFrame, n: int = 10, n1: int = 6) -> pd.DataFrame:
    high_n = df["high"].rolling(window=n, min_periods=n).max()
    low_n = df["low"].rolling(window=n, min_periods=n).min()
    wr = (high_n - df["close"]) / (high_n - low_n).replace(0, np.nan) * 100

    high_n1 = df["high"].rolling(window=n1, min_periods=n1).max()
    low_n1 = df["low"].rolling(window=n1, min_periods=n1).min()
    wr1 = (high_n1 - df["close"]) / (high_n1 - low_n1).replace(0, np.nan) * 100
    return pd.DataFrame({"wr_qfq": wr, "wr1_qfq": wr1}, index=df.index)


def calculate_bias(df: pd.DataFrame, windows: Iterable[int] = (6, 12, 24)) -> pd.DataFrame:
    result = pd.DataFrame(index=df.index)
    for idx, window in enumerate(windows, start=1):
        ma = df["close"].rolling(window=window, min_periods=window).mean()
        result[f"bias{idx}_qfq"] = (df["close"] - ma) / ma.replace(0, np.nan) * 100
    return result


def calculate_roc(df: pd.DataFrame, n: int = 12, m: int = 6) -> pd.DataFrame:
    roc = (df["close"] - df["close"].shift(n)) / df["close"].shift(n).replace(0, np.nan) * 100
    maroc = roc.rolling(window=m, min_periods=m).mean()
    return pd.DataFrame({"roc_qfq": roc, "maroc_qfq": maroc}, index=df.index)


def calculate_mtm(df: pd.DataFrame, n: int = 12, m: int = 6) -> pd.DataFrame:
    mtm = df["close"] - df["close"].shift(n)
    mtmma = mtm.rolling(window=m, min_periods=m).mean()
    return pd.DataFrame({"mtm_qfq": mtm, "mtmma_qfq": mtmma}, index=df.index)


def calculate_trix(df: pd.DataFrame, m1: int = 12, m2: int = 20) -> pd.DataFrame:
    ema1 = df["close"].ewm(span=m1, adjust=False).mean()
    ema2 = ema1.ewm(span=m1, adjust=False).mean()
    ema3 = ema2.ewm(span=m1, adjust=False).mean()
    trix = ema3.pct_change() * 100
    trma = trix.rolling(window=m2, min_periods=m2).mean()
    return pd.DataFrame({"trix_qfq": trix, "trma_qfq": trma}, index=df.index)


def calculate_obv(df: pd.DataFrame) -> pd.DataFrame:
    direction = np.sign(df["close"].diff()).fillna(0)
    obv = (direction * df["volume"]).cumsum()
    return pd.DataFrame({"obv_qfq": obv}, index=df.index)


def calculate_mfi(df: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    money_flow = typical_price * df["volume"]
    positive = money_flow.where(typical_price > typical_price.shift(1), 0.0)
    negative = money_flow.where(typical_price < typical_price.shift(1), 0.0)
    pos_sum = positive.rolling(window=n, min_periods=n).sum()
    neg_sum = negative.rolling(window=n, min_periods=n).sum()
    money_ratio = pos_sum / neg_sum.replace(0, np.nan)
    mfi = 100 - (100 / (1 + money_ratio))
    mfi = mfi.fillna(100).where(neg_sum.ne(0), 100)
    mfi.loc[(pos_sum == 0) & (neg_sum == 0)] = 50
    return pd.DataFrame({"mfi_qfq": mfi}, index=df.index)


def calculate_vr(df: pd.DataFrame, m1: int = 26) -> pd.DataFrame:
    prev_close = df["close"].shift(1)
    av = df["volume"].where(df["close"] > prev_close, 0.0)
    bv = df["volume"].where(df["close"] < prev_close, 0.0)
    cv = df["volume"].where(df["close"] == prev_close, 0.0)
    avs = av.rolling(window=m1, min_periods=m1).sum()
    bvs = bv.rolling(window=m1, min_periods=m1).sum()
    cvs = cv.rolling(window=m1, min_periods=m1).sum()
    vr = (avs + 0.5 * cvs) / (bvs + 0.5 * cvs).replace(0, np.nan) * 100
    return pd.DataFrame({"vr_qfq": vr}, index=df.index)


def calculate_psy(df: pd.DataFrame, n: int = 12, m: int = 6) -> pd.DataFrame:
    up = (df["close"] > df["close"].shift(1)).astype(float)
    psy = up.rolling(window=n, min_periods=n).mean() * 100
    psyma = psy.rolling(window=m, min_periods=m).mean()
    return pd.DataFrame({"psy_qfq": psy, "psyma_qfq": psyma}, index=df.index)


def calculate_dmi(df: pd.DataFrame, m1: int = 14, m2: int = 6) -> pd.DataFrame:
    high_diff = df["high"].diff()
    low_diff = -df["low"].diff()
    plus_dm = pd.Series(
        np.where((high_diff > low_diff) & (high_diff > 0), high_diff, 0.0),
        index=df.index,
    )
    minus_dm = pd.Series(
        np.where((low_diff > high_diff) & (low_diff > 0), low_diff, 0.0),
        index=df.index,
    )
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = _rolling_rma(tr, m1)
    pdi = 100 * _rolling_rma(plus_dm, m1) / atr.replace(0, np.nan)
    mdi = 100 * _rolling_rma(minus_dm, m1) / atr.replace(0, np.nan)
    dx = (abs(pdi - mdi) / (pdi + mdi).replace(0, np.nan)) * 100
    adx = _rolling_rma(dx, m2)
    adxr = (adx + adx.shift(m2)) / 2
    return pd.DataFrame(
        {
            "dmi_pdi_qfq": pdi,
            "dmi_mdi_qfq": mdi,
            "dmi_adx_qfq": adx,
            "dmi_adxr_qfq": adxr,
        },
        index=df.index,
    )


def calculate_state_metrics(df: pd.DataFrame) -> pd.DataFrame:
    close_diff = df["close"].diff()
    up_mask = close_diff > 0
    down_mask = close_diff < 0

    updays = []
    downdays = []
    current_up = 0
    current_down = 0
    for is_up, is_down in zip(up_mask.fillna(False), down_mask.fillna(False)):
        current_up = current_up + 1 if is_up else 0
        current_down = current_down + 1 if is_down else 0
        updays.append(current_up)
        downdays.append(current_down)

    topdays = []
    lowdays = []
    highs = df["high"].tolist()
    lows = df["low"].tolist()
    for idx in range(len(df)):
        top_count = 0
        for prev_idx in range(idx - 1, -1, -1):
            if highs[prev_idx] <= highs[idx]:
                top_count += 1
            else:
                break
        topdays.append(top_count)

        low_count = 0
        for prev_idx in range(idx - 1, -1, -1):
            if lows[prev_idx] >= lows[idx]:
                low_count += 1
            else:
                break
        lowdays.append(low_count)

    return pd.DataFrame(
        {
            "updays": pd.Series(updays, index=df.index, dtype=float),
            "downdays": pd.Series(downdays, index=df.index, dtype=float),
            "topdays": pd.Series(topdays, index=df.index, dtype=float),
            "lowdays": pd.Series(lowdays, index=df.index, dtype=float),
        },
        index=df.index,
    )


def calculate_common_indicators(df: pd.DataFrame) -> pd.DataFrame:
    price_df = _ensure_price_df(df)
    result = price_df.copy()

    result = result.join(calculate_ma(price_df, [5, 10, 20, 30, 60, 90, 250]), how="left")
    result = result.join(calculate_ema(price_df, [5, 10, 20, 30, 60, 90, 250]), how="left")
    result = result.join(calculate_macd(price_df), how="left")
    result = result.join(calculate_rsi(price_df, [6, 12, 24]), how="left")
    result = result.join(calculate_kdj(price_df), how="left")
    result = result.join(calculate_boll(price_df), how="left")
    result = result.join(calculate_atr(price_df), how="left")
    result = result.join(calculate_cci(price_df), how="left")
    result = result.join(calculate_wr(price_df), how="left")
    result = result.join(calculate_bias(price_df), how="left")
    result = result.join(calculate_roc(price_df), how="left")
    result = result.join(calculate_mtm(price_df), how="left")
    result = result.join(calculate_trix(price_df), how="left")
    result = result.join(calculate_obv(price_df), how="left")
    result = result.join(calculate_mfi(price_df), how="left")
    result = result.join(calculate_vr(price_df), how="left")
    result = result.join(calculate_psy(price_df), how="left")
    result = result.join(calculate_dmi(price_df), how="left")
    result = result.join(calculate_state_metrics(price_df), how="left")

    result["bbi_qfq"] = (
        result["ma_qfq_3"] if "ma_qfq_3" in result.columns else price_df["close"].rolling(3, min_periods=3).mean()
    )
    ma6 = price_df["close"].rolling(6, min_periods=6).mean()
    ma12 = price_df["close"].rolling(12, min_periods=12).mean()
    ma24 = price_df["close"].rolling(24, min_periods=24).mean()
    result["bbi_qfq"] = (
        price_df["close"].rolling(3, min_periods=3).mean() + ma6 + ma12 + price_df["close"].rolling(20, min_periods=20).mean()
    ) / 4

    return result


def build_indicator_full_payload(df_or_series: pd.DataFrame | pd.Series) -> dict:
    if isinstance(df_or_series, pd.DataFrame):
        if df_or_series.empty:
            raise ValueError("indicator dataframe is empty")
        series = df_or_series.iloc[-1]
    else:
        series = df_or_series

    return {
        group: {field: _serialize_value(series.get(field)) for field in fields}
        for group, fields in FULL_GROUP_FIELDS.items()
    }


def build_indicator_summary_payload(df_or_series: pd.DataFrame | pd.Series) -> dict:
    if isinstance(df_or_series, pd.DataFrame):
        if df_or_series.empty:
            raise ValueError("indicator dataframe is empty")
        series = df_or_series.iloc[-1]
    else:
        series = df_or_series

    return {field: _serialize_value(series.get(field)) for field in SUMMARY_FIELDS}
