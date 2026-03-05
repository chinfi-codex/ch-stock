import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import streamlit as st
import tushare as ts

from .market_data import get_all_stocks


PATTERN_LABELS = {
    "low_breakout": "低位突破",
    "low_first_board": "低位首板",
    "box_breakout_120": "120日新高箱体突破",
    "new_high_500": "历史新高(500D)",
    "box_bottom_start": "箱体底部区间启动",
}


def _get_tushare_token() -> str:
    token = None
    try:
        token = st.secrets.get("tushare_token")
    except Exception:
        token = None
    return token or os.environ.get("TUSHARE_TOKEN", "")


def _get_pro():
    token = _get_tushare_token()
    if not token:
        raise RuntimeError("Missing TUSHARE_TOKEN")
    return ts.pro_api(token)


def _normalize_trade_date(trade_date: Any) -> Tuple[str, date]:
    dt = pd.to_datetime(trade_date, errors="coerce")
    if pd.isna(dt):
        raise ValueError(f"Invalid trade date: {trade_date}")
    return dt.strftime("%Y%m%d"), dt.date()


def _to_ts_code(value: str) -> str:
    raw = str(value).strip().upper()
    if "." in raw:
        return raw
    code = re.sub(r"\D", "", raw)
    if len(code) != 6:
        return raw
    if code.startswith(("0", "3")):
        return f"{code}.SZ"
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    if code.startswith("8"):
        return f"{code}.BJ"
    return raw


def _extract_code6(value: str) -> str:
    m = re.search(r"(\d{6})", str(value))
    return m.group(1) if m else ""


def _classify_board(code6: str) -> str:
    if code6.startswith("688"):
        return "科创板"
    if code6.startswith(("300", "301")):
        return "创业板"
    return "主板"


def validate_trade_day(trade_date: date) -> tuple[bool, str]:
    try:
        trade_date_str, trade_dt = _normalize_trade_date(trade_date)
        pro = _get_pro()
        cal = pro.trade_cal(exchange="", start_date=trade_date_str, end_date=trade_date_str)
        if cal is None or cal.empty or "is_open" not in cal.columns:
            return False, "交易日校验失败: 未获取到交易日历"
        is_open = int(cal.iloc[0]["is_open"]) == 1
        if not is_open:
            return False, f"{trade_dt.strftime('%Y-%m-%d')} 不是交易日"
        return True, ""
    except Exception as e:
        return False, f"交易日校验失败: {e}"


@st.cache_data(ttl="4h", show_spinner=False)
def load_top100_gainers(trade_date: date) -> pd.DataFrame:
    source_df = get_all_stocks(trade_date)
    if source_df is None or source_df.empty:
        return pd.DataFrame()

    view = source_df.copy()
    for col in ["pct", "amount", "mkt_cap"]:
        if col in view.columns:
            view[col] = pd.to_numeric(view[col], errors="coerce")

    view["code"] = view["code"].astype(str).str.strip()
    view["name"] = view["name"].astype(str).fillna("")
    view["code6"] = view["code"].apply(_extract_code6)
    view = view[view["code6"] != ""]

    code_lower = view["code"].str.lower()
    is_bj = code_lower.str.startswith("bj") | view["code6"].str.startswith(("4", "8"))
    is_st = view["name"].str.upper().str.contains("ST", na=False)
    view = view[~is_bj & ~is_st]

    view = view.dropna(subset=["pct", "amount", "mkt_cap"])
    view = view.sort_values("pct", ascending=False).head(100).copy()
    view["rank"] = range(1, len(view) + 1)
    view["ts_code"] = view["code6"].apply(_to_ts_code)
    view["board_type"] = view["code6"].apply(_classify_board)
    return view[
        [
            "rank",
            "code6",
            "ts_code",
            "name",
            "pct",
            "amount",
            "mkt_cap",
            "board_type",
        ]
    ].rename(columns={"code6": "code"})


@st.cache_data(ttl="12h", show_spinner=False)
def fetch_stock_kline_with_limit(ts_code: str, trade_date: str, lookback_days: int = 620) -> pd.DataFrame:
    code = _to_ts_code(ts_code)
    if not code:
        return pd.DataFrame()

    try:
        end_dt = pd.to_datetime(trade_date, errors="coerce")
        if pd.isna(end_dt):
            return pd.DataFrame()

        start_dt = end_dt - timedelta(days=max(int(lookback_days) * 2, 900))
        pro = _get_pro()
        daily = pro.daily(
            ts_code=code,
            start_date=start_dt.strftime("%Y%m%d"),
            end_date=end_dt.strftime("%Y%m%d"),
            fields="trade_date,open,high,low,close,amount",
        )
        if daily is None or daily.empty:
            return pd.DataFrame()

        limit_df = pro.stk_limit(
            ts_code=code,
            start_date=start_dt.strftime("%Y%m%d"),
            end_date=end_dt.strftime("%Y%m%d"),
            fields="trade_date,up_limit",
        )
        if limit_df is None or limit_df.empty:
            limit_df = pd.DataFrame(columns=["trade_date", "up_limit"])

        daily = daily.copy()
        limit_df = limit_df.copy()
        daily["trade_date"] = pd.to_datetime(daily["trade_date"], errors="coerce")
        limit_df["trade_date"] = pd.to_datetime(limit_df["trade_date"], errors="coerce")

        for col in ["open", "high", "low", "close", "amount"]:
            if col in daily.columns:
                daily[col] = pd.to_numeric(daily[col], errors="coerce")
        if "up_limit" in limit_df.columns:
            limit_df["up_limit"] = pd.to_numeric(limit_df["up_limit"], errors="coerce")

        merged = daily.merge(limit_df[["trade_date", "up_limit"]], on="trade_date", how="left")
        merged = merged.dropna(subset=["trade_date", "open", "high", "low", "close"])
        merged = merged.sort_values("trade_date")
        if len(merged) > lookback_days:
            merged = merged.tail(lookback_days)
        return merged.reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def _regression_slope(values: pd.Series) -> float:
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    if len(arr) < 2 or np.isnan(arr).any():
        return np.nan
    x = np.arange(len(arr), dtype=float)
    return float(np.polyfit(x, arr, 1)[0])


def _window_excl_today(series: pd.Series, window: int) -> pd.Series:
    n = len(series)
    if n - 1 < window:
        return pd.Series(dtype=float)
    return series.iloc[n - 1 - window : n - 1]


def _to_py_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        num = float(v)
        if np.isnan(num):
            return None
        return num
    except Exception:
        return None


def compute_single_stock_patterns(
    df: pd.DataFrame,
    asof_date: str,
    require_newhigh_volume: bool = True,
) -> dict[str, bool | float | int]:
    result: Dict[str, Any] = {
        "pos_120": np.nan,
        "amount_ratio_20": np.nan,
        "limit_streak": 0,
        "is_limit_up_today": False,
        "ma20": np.nan,
        "ma20_slope_10": np.nan,
        "upper_120": np.nan,
        "lower_120": np.nan,
        "box_width_120": np.nan,
        "hit_ratio_120": np.nan,
        "is_box_120": False,
        "pattern_low_breakout": False,
        "pattern_low_first_board": False,
        "pattern_box_breakout_120": False,
        "pattern_new_high_500": False,
        "pattern_box_bottom_start": False,
    }

    if df is None or df.empty:
        return result

    asof_dt = pd.to_datetime(asof_date, errors="coerce")
    if pd.isna(asof_dt):
        return result

    view = df.copy()
    view["trade_date"] = pd.to_datetime(view["trade_date"], errors="coerce")
    view = view.dropna(subset=["trade_date"])
    view = view[view["trade_date"] <= asof_dt].sort_values("trade_date")
    if view.empty:
        return result

    close = pd.to_numeric(view["close"], errors="coerce")
    high = pd.to_numeric(view["high"], errors="coerce")
    low = pd.to_numeric(view["low"], errors="coerce")
    amount = pd.to_numeric(view.get("amount"), errors="coerce")
    up_limit = pd.to_numeric(view.get("up_limit"), errors="coerce")

    if close.empty:
        return result

    close_t = close.iloc[-1]
    if pd.isna(close_t):
        return result

    close20 = _window_excl_today(close, 20)
    high120 = _window_excl_today(high, 120)
    low120 = _window_excl_today(low, 120)
    high500 = _window_excl_today(high, 500)
    amount20 = _window_excl_today(amount, 20)

    hhv_close_20 = close20.max() if len(close20) == 20 else np.nan
    upper_120 = high120.max() if len(high120) == 120 else np.nan
    lower_120 = low120.min() if len(low120) == 120 else np.nan
    hhv_high_500 = high500.max() if len(high500) == 500 else np.nan

    amount_ratio_20 = np.nan
    if len(amount20) == 20:
        avg_amount20 = amount20.mean()
        if pd.notna(avg_amount20) and avg_amount20 > 0 and pd.notna(amount.iloc[-1]):
            amount_ratio_20 = float(amount.iloc[-1] / avg_amount20)
    result["amount_ratio_20"] = amount_ratio_20

    ma20_series = close.rolling(20).mean()
    ma20 = ma20_series.iloc[-1] if len(close) >= 20 else np.nan
    result["ma20"] = ma20
    ma20_slope_10 = _regression_slope(ma20_series.iloc[-10:]) if len(ma20_series) >= 10 else np.nan
    result["ma20_slope_10"] = ma20_slope_10

    pos_120 = np.nan
    box_width_120 = np.nan
    hit_ratio_120 = np.nan
    is_box_120 = False
    if pd.notna(upper_120) and pd.notna(lower_120) and lower_120 > 0 and upper_120 > lower_120:
        pos_120 = float((close_t - lower_120) / (upper_120 - lower_120 + 1e-9))
        box_width_120 = float((upper_120 - lower_120) / lower_120)

        prev_close_120 = _window_excl_today(close, 120)
        if len(prev_close_120) == 120:
            norm_pos = (prev_close_120 - lower_120) / (upper_120 - lower_120 + 1e-9)
            hit_ratio_120 = float(((norm_pos >= 0.15) & (norm_pos <= 0.85)).mean())
            is_box_120 = (0.08 <= box_width_120 <= 0.35) and (hit_ratio_120 >= 0.70)

    result["pos_120"] = pos_120
    result["upper_120"] = upper_120
    result["lower_120"] = lower_120
    result["box_width_120"] = box_width_120
    result["hit_ratio_120"] = hit_ratio_120
    result["is_box_120"] = bool(is_box_120)

    is_limit_up_day = (up_limit.notna()) & (close >= up_limit * 0.999)
    limit_streak = 0
    for i in range(len(is_limit_up_day) - 1, -1, -1):
        if bool(is_limit_up_day.iloc[i]):
            limit_streak += 1
        else:
            break
    is_limit_up_today = bool(is_limit_up_day.iloc[-1]) if len(is_limit_up_day) > 0 else False
    result["limit_streak"] = int(limit_streak)
    result["is_limit_up_today"] = is_limit_up_today

    low_breakout = (
        pd.notna(pos_120)
        and pd.notna(hhv_close_20)
        and pd.notna(amount_ratio_20)
        and (pos_120 <= 0.30)
        and (close_t > hhv_close_20 * (1 + 0.005))
        and (amount_ratio_20 >= 1.5)
    )

    low_first_board = low_breakout and is_limit_up_today and limit_streak == 1

    box_breakout_120 = (
        is_box_120
        and pd.notna(upper_120)
        and pd.notna(amount_ratio_20)
        and (close_t > upper_120 * (1 + 0.005))
        and (amount_ratio_20 >= 1.5)
    )

    new_high_500 = pd.notna(hhv_high_500) and (close_t > hhv_high_500 * (1 + 0.003))
    if require_newhigh_volume:
        new_high_500 = new_high_500 and pd.notna(amount_ratio_20) and (amount_ratio_20 >= 1.2)

    box_bottom_start = (
        is_box_120
        and pd.notna(pos_120)
        and pd.notna(ma20)
        and pd.notna(ma20_slope_10)
        and pd.notna(amount_ratio_20)
        and (pos_120 <= 0.25)
        and (close_t > ma20)
        and (ma20_slope_10 > 0)
        and (amount_ratio_20 >= 1.3)
    )

    result["pattern_low_breakout"] = bool(low_breakout)
    result["pattern_low_first_board"] = bool(low_first_board)
    result["pattern_box_breakout_120"] = bool(box_breakout_120)
    result["pattern_new_high_500"] = bool(new_high_500)
    result["pattern_box_bottom_start"] = bool(box_bottom_start)

    return result


def _build_bucket_stats(values: pd.Series, buckets: List[Tuple[str, float, float]], sample_size: int) -> List[Dict[str, Any]]:
    stats: List[Dict[str, Any]] = []
    safe_values = pd.to_numeric(values, errors="coerce")
    for label, low, high in buckets:
        if high == float("inf"):
            count = int((safe_values >= low).sum())
        else:
            count = int(((safe_values >= low) & (safe_values < high)).sum())
        ratio = (count / sample_size) if sample_size else 0.0
        stats.append({"label": label, "count": count, "ratio": ratio})
    return stats


def _analyze_single_stock(row: Dict[str, Any], trade_date_str: str, require_newhigh_volume: bool) -> Dict[str, Any]:
    kline = fetch_stock_kline_with_limit(row.get("ts_code", ""), trade_date_str, lookback_days=620)
    pattern_metrics = compute_single_stock_patterns(kline, trade_date_str, require_newhigh_volume=require_newhigh_volume)
    return {"code": row.get("code"), **pattern_metrics}


def _compute_top100_profit_effect_core(trade_date_str: str, require_newhigh_volume: bool) -> Dict[str, Any]:
    ok, msg = validate_trade_day(trade_date_str)
    if not ok:
        raise ValueError(msg)

    top100_df = load_top100_gainers(pd.to_datetime(trade_date_str).date())
    if top100_df is None or top100_df.empty:
        raise ValueError("未获取到Top100涨幅样本")
    if len(top100_df) < 100:
        raise ValueError(f"样本不足100只，当前仅 {len(top100_df)} 只")

    top100_df = top100_df.sort_values("rank").reset_index(drop=True)

    result_rows: Dict[str, Dict[str, Any]] = {}
    workers = 8
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(_analyze_single_stock, row._asdict() if hasattr(row, "_asdict") else row.to_dict(), trade_date_str, require_newhigh_volume): row["code"]
            for _, row in top100_df.iterrows()
        }
        for future in as_completed(future_map):
            code = future_map[future]
            try:
                result_rows[code] = future.result()
            except Exception:
                result_rows[code] = {"code": code}

    metrics_df = pd.DataFrame(result_rows.values()) if result_rows else pd.DataFrame(columns=["code"])
    details = top100_df.merge(metrics_df, on="code", how="left")

    for col in [
        "pattern_low_breakout",
        "pattern_low_first_board",
        "pattern_box_breakout_120",
        "pattern_new_high_500",
        "pattern_box_bottom_start",
        "is_limit_up_today",
        "is_box_120",
    ]:
        if col in details.columns:
            details[col] = details[col].fillna(False).astype(bool)

    details["limit_streak"] = pd.to_numeric(details.get("limit_streak"), errors="coerce").fillna(0).astype(int)
    details["amount_yi"] = pd.to_numeric(details["amount"], errors="coerce") / 1e8
    details["mkt_cap_yi"] = pd.to_numeric(details["mkt_cap"], errors="coerce") / 1e8

    sample_size = int(len(details))
    amount_buckets = [
        ("<5亿", 0, 5),
        ("5-50亿", 5, 50),
        ("50-90亿", 50, 90),
        (">=90亿", 90, float("inf")),
    ]
    mkt_buckets = [
        ("<50亿", 0, 50),
        ("50-100亿", 50, 100),
        ("100-200亿", 100, 200),
        ("200-500亿", 200, 500),
        (">=500亿", 500, float("inf")),
    ]

    amount_stats = _build_bucket_stats(details["amount_yi"], amount_buckets, sample_size)
    mktcap_stats = _build_bucket_stats(details["mkt_cap_yi"], mkt_buckets, sample_size)

    board_order = ["主板", "创业板", "科创板"]
    board_counts = details["board_type"].value_counts().to_dict()
    board_stats = [
        {
            "label": b,
            "count": int(board_counts.get(b, 0)),
            "ratio": (int(board_counts.get(b, 0)) / sample_size) if sample_size else 0.0,
        }
        for b in board_order
    ]

    pattern_defs = [
        ("low_breakout", "pattern_low_breakout"),
        ("low_first_board", "pattern_low_first_board"),
        ("box_breakout_120", "pattern_box_breakout_120"),
        ("new_high_500", "pattern_new_high_500"),
        ("box_bottom_start", "pattern_box_bottom_start"),
    ]
    pattern_stats: List[Dict[str, Any]] = []
    for key, col in pattern_defs:
        count = int(details[col].sum()) if col in details.columns else 0
        pattern_stats.append(
            {
                "key": key,
                "label": PATTERN_LABELS[key],
                "count": count,
                "ratio": (count / sample_size) if sample_size else 0.0,
            }
        )

    details = details.sort_values("rank")
    numeric_cols = [
        "pct",
        "amount",
        "mkt_cap",
        "amount_yi",
        "mkt_cap_yi",
        "pos_120",
        "amount_ratio_20",
        "ma20",
        "ma20_slope_10",
        "upper_120",
        "lower_120",
        "box_width_120",
        "hit_ratio_120",
    ]
    for col in numeric_cols:
        if col in details.columns:
            details[col] = pd.to_numeric(details[col], errors="coerce")

    for col in ["pos_120", "amount_ratio_20", "ma20", "ma20_slope_10", "upper_120", "lower_120", "box_width_120", "hit_ratio_120"]:
        if col in details.columns:
            details[col] = details[col].apply(_to_py_float)

    return {
        "trade_date": pd.to_datetime(trade_date_str).strftime("%Y-%m-%d"),
        "sample_size": sample_size,
        "amount_stats": amount_stats,
        "mktcap_stats": mktcap_stats,
        "board_stats": board_stats,
        "pattern_stats": pattern_stats,
        "details": details.to_dict(orient="records"),
    }


@st.cache_data(ttl="2h", show_spinner=False)
def _compute_top100_profit_effect_cached(trade_date_str: str, require_newhigh_volume: bool) -> Dict[str, Any]:
    return _compute_top100_profit_effect_core(trade_date_str, require_newhigh_volume)


def compute_top100_profit_effect(trade_date: date, require_newhigh_volume: bool = True) -> dict:
    trade_date_str, _ = _normalize_trade_date(trade_date)
    return _compute_top100_profit_effect_cached(trade_date_str, require_newhigh_volume)


@st.cache_data(ttl="30m", show_spinner=False)
def fetch_top100_layered_features_by_date(limit_days: int = 20) -> pd.DataFrame:
    try:
        from database.db_manager import get_db
    except Exception:
        return pd.DataFrame()

    try:
        safe_limit = max(1, min(int(limit_days), 250))
    except Exception:
        safe_limit = 20

    sql = f"""
        SELECT
            trade_date,
            turnover_lt_5e8,
            turnover_5e8_to_50,
            turnover_50e8_to_90,
            turnover_gt_90e8,
            mktcap_lt_5e9,
            mktcap_5e9_to_10,
            mktcap_10e9_to_20,
            mktcap_20e9_to_50,
            mktcap_gt_50e9,
            board_main,
            board_gem,
            board_star
        FROM gainer_feature_summary
        ORDER BY trade_date DESC
        LIMIT {safe_limit}
    """

    try:
        with get_db() as db:
            rows = db.query(sql)
    except Exception:
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df = df.dropna(subset=["trade_date"]).sort_values("trade_date", ascending=False)
        df["trade_date"] = df["trade_date"].dt.strftime("%Y-%m-%d")
    return df.reset_index(drop=True)
