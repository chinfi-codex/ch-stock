#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
市场历史 CSV 仓储。
"""

import os

import pandas as pd


def get_market_history_csv_path() -> str:
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "datas", "market_data.csv"
    )


def upsert_market_history_row(row: dict, columns: list[str]) -> None:
    """写入或更新单日市场历史。"""
    csv_file = get_market_history_csv_path()
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)

    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
        if "日期" not in df.columns:
            if len(df.columns) == len(columns):
                df.columns = columns
            else:
                first_cols = list(df.columns)
                first_cols[0] = "日期"
                df.columns = first_cols

        for col in ["成交额", "上涨", "下跌"]:
            if col not in df.columns:
                df[col] = ""

        if not df[df["日期"] == row["日期"]].empty:
            idx = df.index[df["日期"] == row["日期"]][0]
            for col in ["成交额", "上涨", "下跌"]:
                if col in df.columns and (
                    pd.isna(df.at[idx, col]) or str(df.at[idx, col]).strip() == ""
                ):
                    df.at[idx, col] = row.get(col, "")
            df.to_csv(csv_file, index=False)
            return

        df = pd.concat([pd.DataFrame([row], columns=columns), df], ignore_index=True)
        df.to_csv(csv_file, index=False)
        return

    pd.DataFrame([row], columns=columns).to_csv(csv_file, index=False)


def load_market_history(days: int = 30) -> pd.DataFrame:
    """读取市场历史。"""
    safe_days = max(1, int(days))
    csv_file = get_market_history_csv_path()
    if not os.path.exists(csv_file):
        return pd.DataFrame()

    try:
        df = pd.read_csv(csv_file)
        if df is None or df.empty:
            return pd.DataFrame()
        cols = list(df.columns)
        if len(cols) < 13:
            return pd.DataFrame()

        out = pd.DataFrame()
        out["日期"] = df[cols[0]]
        out["上涨"] = df[cols[1]]
        out["涨停"] = df[cols[2]]
        out["下跌"] = df[cols[5]]
        out["跌停"] = df[cols[6]]
        out["活跃度"] = df[cols[11]]
        out["成交额"] = df[cols[12]]
        out["日期"] = pd.to_datetime(out["日期"], errors="coerce")
        out = out.dropna(subset=["日期"]).sort_values("日期").tail(safe_days)
        for col in ["上涨", "下跌", "涨停", "跌停", "成交额"]:
            out[col] = pd.to_numeric(out[col], errors="coerce")
        out["活跃度"] = pd.to_numeric(
            out["活跃度"].astype(str).str.replace("%", "", regex=False),
            errors="coerce",
        )
        return out
    except Exception:
        return pd.DataFrame()
