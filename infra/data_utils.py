#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
通用数据处理模块。
"""

from typing import Any, Optional, Union

import pandas as pd


def convert_to_ts_code(code: Optional[str]) -> str:
    """将股票代码转换为 Tushare ts_code 格式。"""
    if code is None:
        raise ValueError("股票代码不能为空")

    code = str(code).strip()
    if not code:
        raise ValueError("股票代码不能为空")

    upper_code = code.upper()
    if "." in upper_code:
        prefix, suffix = upper_code.split(".", 1)
        suffix = suffix.replace("SS", "SH")
        if suffix in {"SH", "SZ", "BJ"}:
            return f"{prefix}.{suffix}"

    if upper_code.startswith(("SZ", "SH", "BJ")) and len(upper_code) >= 8:
        body = upper_code[2:]
        suffix = upper_code[:2]
        return f"{body}.{suffix}"

    if len(code) == 6 and code.isdigit():
        if code.startswith(("0", "3")):
            return f"{code}.SZ"
        if code.startswith(("6", "9")):
            return f"{code}.SH"
        if code.startswith("8"):
            return f"{code}.BJ"

    return upper_code


def convert_to_ak_code(code: str) -> str:
    """将股票代码转换为 AKShare 格式。"""
    code = str(code).strip()
    if code.lower().startswith(("sh", "sz", "bj")) and len(code) >= 8:
        return code.lower()

    if "." in code:
        parts = code.split(".")
        if len(parts) == 2 and parts[1].upper() in ("SH", "SZ", "BJ"):
            return f"{parts[1].lower()}{parts[0]}"

    if len(code) == 6 and code.isdigit():
        if code.startswith(("0", "3")):
            return f"sz{code}"
        if code.startswith(("6", "9")):
            return f"sh{code}"
        if code.startswith("8"):
            return f"bj{code}"

    return code.lower()


def to_number(series: Union[pd.Series, Any]) -> Optional[pd.Series]:
    """将 Series 转为数值类型。"""
    if series is None:
        return None
    s = series.astype(str).str.replace("%", "", regex=False)
    return pd.to_numeric(s, errors="coerce")


def latest_metric_from_df(
    df: pd.DataFrame, value_col: str, date_col: str = "date"
) -> Optional[dict]:
    """从 DataFrame 中获取最新值与前值。"""
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
    prev_value = view.iloc[1][value_col] if len(view) > 1 else None
    return {
        "date": latest[date_col] if date_col in view.columns else None,
        "value": float(latest[value_col]),
        "prev_value": float(prev_value)
        if prev_value is not None and not pd.isna(prev_value)
        else None,
    }


def calc_pct_change(current: float, previous: float) -> Optional[float]:
    """计算百分比变化。"""
    if current is None or previous is None or previous == 0:
        return None
    return (current / previous - 1) * 100


def series_from_df(df: pd.DataFrame, value_col: str, days: int) -> list:
    """从 DataFrame 提取时间序列。"""
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
