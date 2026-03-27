#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
K线数据模块
包含K线数据获取（日线、周线、月线、分时）和K线绘图功能
"""

import datetime
import logging
import os

import akshare as ak
import mplfinance as mpf
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st
import tushare as ts

from infra.data_utils import convert_to_ts_code

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# tushare配置
TS_TOKEN = os.getenv(
    "TUSHARE_TOKEN", "943ca25f1428e5ed6d7d752b6b1496e6afdcac48ace4cf54e0d82a6e"
)
_ts_pro_client = None


def _get_ts_client():
    """惰性初始化tushare客户端"""
    global _ts_pro_client
    if _ts_pro_client is None:
        if not TS_TOKEN:
            raise ValueError("未配置 Tushare Token，无法使用兜底数据源")
        ts.set_token(TS_TOKEN)
        _ts_pro_client = ts.pro_api()
    return _ts_pro_client


@st.cache_data(ttl="0.5d")
def get_tushare_price_df(code, end_date=None, count=60):
    """使用tushare获取股票日K线数据"""
    if end_date is None:
        end_date = datetime.datetime.now().strftime("%Y%m%d")

    ts_code = convert_to_ts_code(code)
    pro = _get_ts_client()

    end_dt = datetime.datetime.strptime(end_date, "%Y%m%d")
    start_dt = end_dt - datetime.timedelta(days=max(count * 3, 120))
    start_date = start_dt.strftime("%Y%m%d")

    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        raise ValueError(f"Tushare未返回 {ts_code} 的日线数据")

    df = df.sort_values("trade_date")
    if len(df) > count:
        df = df.tail(count)

    df = df[["trade_date", "open", "close", "high", "low", "vol"]].copy()
    df = df.rename(columns={"trade_date": "date", "vol": "volume"})
    df["volume"] = df["volume"] * 100  # tushare单位为手，需要换算为股以匹配akshare
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    return df


@st.cache_data(ttl="0.5d")
def get_ak_price_df(code, end_date=None, count=60):
    """获取股票日K线数据，统一使用 TuShare 数据源"""
    if end_date is None:
        end_date = datetime.datetime.now().strftime("%Y%m%d")

    try:
        return get_tushare_price_df(code, end_date, count)
    except Exception as ts_error:
        logger.error(f"tushare获取 {code} 日线失败: {ts_error}")
        raise


@st.cache_data(ttl="0.5d")
def get_tushare_weekly_df(code, end_date=None, count=60):
    """使用tushare获取股票周K线数据"""
    if end_date is None:
        end_date = datetime.datetime.now().strftime("%Y%m%d")

    ts_code = convert_to_ts_code(code)
    pro = _get_ts_client()

    # 周线需要更多天数来获取足够的数据
    end_dt = datetime.datetime.strptime(end_date, "%Y%m%d")
    start_dt = end_dt - datetime.timedelta(days=max(count * 10, 500))
    start_date = start_dt.strftime("%Y%m%d")

    df = pro.weekly(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        raise ValueError(f"Tushare未返回 {ts_code} 的周线数据")

    df = df.sort_values("trade_date")
    if len(df) > count:
        df = df.tail(count)

    df = df[["trade_date", "open", "close", "high", "low", "vol"]].copy()
    df = df.rename(columns={"trade_date": "date", "vol": "volume"})
    df["volume"] = df["volume"] * 100  # tushare单位为手，换算为股
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    return df


@st.cache_data(ttl="0.5d")
def get_tushare_monthly_df(code, end_date=None, count=60):
    """使用tushare获取股票月K线数据"""
    if end_date is None:
        end_date = datetime.datetime.now().strftime("%Y%m%d")

    ts_code = convert_to_ts_code(code)
    pro = _get_ts_client()

    # 月线需要更多天数来获取足够的数据
    end_dt = datetime.datetime.strptime(end_date, "%Y%m%d")
    start_dt = end_dt - datetime.timedelta(days=max(count * 35, 1200))
    start_date = start_dt.strftime("%Y%m%d")

    df = pro.monthly(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        raise ValueError(f"Tushare未返回 {ts_code} 的月线数据")

    df = df.sort_values("trade_date")
    if len(df) > count:
        df = df.tail(count)

    df = df[["trade_date", "open", "close", "high", "low", "vol"]].copy()
    df = df.rename(columns={"trade_date": "date", "vol": "volume"})
    df["volume"] = df["volume"] * 100  # tushare单位为手，换算为股
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    return df


@st.cache_data(ttl="0.5d")
def get_ak_interval_price_df(code, end_date=None, count=241):
    """获取股票分时数据（带重试机制）"""
    if end_date is None:
        end_date = datetime.datetime.now().strftime("%Y%m%d")

    df = ak.stock_zh_a_hist_min_em(symbol=code, end_date=end_date, period="1").tail(
        count
    )

    df.columns = [
        "date",
        "open",
        "close",
        "high",
        "low",
        "volume_",
        "volume",
        "lastprice",
    ]
    df = df[["date", "open", "close", "high", "low", "volume"]]
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    return df


def calculate_macd(df, fast=12, slow=26, signal=9):
    """
    计算MACD指标

    参数:
        df: DataFrame with 'close' column
        fast: 快线周期，默认12
        slow: 慢线周期，默认26
        signal: 信号线周期，默认9

    返回:
        DataFrame with 'macd', 'signal', 'histogram' columns
    """
    df = df.copy()
    # 计算EMA
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    # DIF线
    df["macd"] = ema_fast - ema_slow
    # DEA信号线
    df["signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
    # MACD柱状图
    df["histogram"] = (df["macd"] - df["signal"]) * 2
    return df


def plotK(
    df,
    k="d",
    plot_type="candle",
    ma_line=None,
    fail_zt=False,
    container=st,
    highlight_date=None,
    show_macd=False,
):
    """绘制K线图（mplfinance版本）

    参数:
        show_macd: 是否显示MACD指标，默认False
    """
    # 确保df是副本，避免修改原始数据
    df = df.copy()

    # 处理索引
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
    elif not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    # 处理周/月线重采样
    if k == "w":
        df = df.resample("W").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
    elif k == "m":
        df = df.resample("M").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )

    # 移除NaN值
    df = df.dropna()

    # 计算MACD
    if show_macd and "close" in df.columns:
        df = calculate_macd(df)

    # 设置颜色方案
    if fail_zt:
        # 炸板模式：黑色/灰色
        mc = mpf.make_marketcolors(
            up="black", down="gray", edge="inherit", wick="inherit", volume="inherit"
        )
    else:
        # 标准模式：红涨绿跌（中国A股习惯）
        mc = mpf.make_marketcolors(
            up="#ff4d4f",  # 红色（上涨）
            down="#52c41a",  # 绿色（下跌）
            edge="inherit",
            wick="inherit",
            volume="inherit",
        )

    # 计算均线
    if ma_line is not None:
        ma_periods = ma_line if isinstance(ma_line, (list, tuple)) else [ma_line]
    else:
        ma_periods = [5, 10, 20, 60, 144, 250]

    added_mas = []
    for period in ma_periods:
        if len(df) >= period:
            col_name = f"MA{period}"
            df[col_name] = df["close"].rolling(window=period, min_periods=1).mean()
            added_mas.append(col_name)

    # 准备额外的plot（用于标注）
    addplot = []

    # 处理标注日期
    if highlight_date is not None:
        if isinstance(highlight_date, str):
            highlight_date = pd.to_datetime(highlight_date)
        elif isinstance(highlight_date, datetime.datetime):
            highlight_date = pd.to_datetime(highlight_date)

        # 找到最近的交易日
        available_dates = df.index
        if highlight_date in available_dates:
            # 找到该日期的最低价用于标注
            low_price = df.loc[highlight_date, "low"]

            # 创建标注数据（只在高亮日期有值，其他为NaN）
            annotate_data = pd.Series(index=df.index, dtype=float)
            annotate_data[highlight_date] = low_price * 0.95  # 在最低价下方一点标注

            # 添加散点标注
            addplot.append(
                mpf.make_addplot(
                    annotate_data,
                    type="scatter",
                    marker="^",
                    markersize=150,
                    color="red",
                )
            )

    # 添加MACD指标（如果需要）
    if show_macd and "macd" in df.columns:
        # MACD线
        addplot.append(
            mpf.make_addplot(df["macd"], panel=2, color="blue", width=1, title="MACD")
        )
        # 信号线
        addplot.append(mpf.make_addplot(df["signal"], panel=2, color="orange", width=1))
        # MACD柱状图
        histogram_colors = ["red" if h >= 0 else "green" for h in df["histogram"]]
        addplot.append(
            mpf.make_addplot(
                df["histogram"],
                panel=2,
                type="bar",
                color=histogram_colors,
                alpha=0.7,
            )
        )

    # 创建样式
    s = mpf.make_mpf_style(
        marketcolors=mc,
        gridstyle="-",
        gridcolor="lightgray",
        rc={"font.size": 10, "figure.figsize": (12, 10)},
    )

    # 确定图表类型
    if plot_type == "line":
        chart_type = "line"
        mav = None  # 线图不需要均线
    else:
        chart_type = "candle"
        # 过滤掉数据不足的均线周期
        mav = tuple([p for p in ma_periods if len(df) >= p])

    # 确定面板数量
    if show_macd and "macd" in df.columns:
        panel_ratios = (4, 1, 1)
    else:
        panel_ratios = (3, 1)

    # 绘制图表
    fig, axes = mpf.plot(
        df,
        type=chart_type,
        mav=mav if chart_type == "candle" else None,
        volume=True,
        style=s,
        returnfig=True,
        figsize=(12, 10),
        panel_ratios=panel_ratios,
        addplot=addplot if addplot else [],
    )

    # 调整布局
    plt.tight_layout()

    # 在Streamlit中显示
    container.pyplot(fig, use_container_width=True)

    # 关闭图形以释放内存
    plt.close(fig)
