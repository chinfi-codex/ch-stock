import numpy as np
import pandas as pd


def calculate_ma(prices, period):
    return prices.rolling(window=period).mean()


def calculate_ma_slope(ma_values, period=5):
    if len(ma_values) < period:
        return 0
    recent_values = ma_values[-period:].values
    x = np.arange(len(recent_values))
    return np.polyfit(x, recent_values, 1)[0]


def calculate_adx(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]

    tr = pd.concat([(high - low), (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)

    plus_dm = (high - high.shift(1)).where((high - high.shift(1)) > (low.shift(1) - low), 0.0)
    plus_dm = plus_dm.where(plus_dm > 0, 0.0)
    minus_dm = (low.shift(1) - low).where((low.shift(1) - low) > (high - high.shift(1)), 0.0)
    minus_dm = minus_dm.where(minus_dm > 0, 0.0)

    tr_smooth = tr.rolling(window=period).sum()
    plus_dm_smooth = plus_dm.rolling(window=period).sum()
    minus_dm_smooth = minus_dm.rolling(window=period).sum()

    plus_di = 100 * (plus_dm_smooth / tr_smooth.replace(0, np.nan))
    minus_di = 100 * (minus_dm_smooth / tr_smooth.replace(0, np.nan))
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.rolling(window=period).mean()
    return adx


def calculate_atr(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    tr = pd.concat([(high - low), (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
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

    hit = (df["close"] >= lower * (1 - cfg["box_hit_tol"])) & (df["close"] <= upper * (1 + cfg["box_hit_tol"]))
    hit_ratio = hit.rolling(cfg["W_box"], min_periods=cfg["W_box"]).mean()

    box_valid = (box_width >= cfg["box_width_min"]) & (box_width <= cfg["box_width_max"]) & (
        hit_ratio >= cfg["box_hit_ratio_min"]
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
