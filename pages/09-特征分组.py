#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
特征分组页面
展示容量上涨股票和10:30前涨停股票
"""

import os
import json
import datetime
import time

import streamlit as st
import pandas as pd
import requests

from tools import plotK
from tools.stock_data import get_ak_price_df
from tools.ai_analysis import analyze_stock_classification


def _section_title(title):
    st.markdown(
        f"<div style='font-size:26px;font-weight:700;margin:8px 0 8px 0;'>{title}</div>",
        unsafe_allow_html=True,
    )


@st.cache_data(ttl="10m")
def fetch_zt_list_from_jrj(trade_date: str = ""):
    """
    从 JRJ 获取涨停股票列表

    Args:
        trade_date: 交易日期，格式 "YYYYMMDD"，空字符串表示当天

    Returns:
        list: 涨停股票列表，按封板时间排序
    """
    url = "https://gateway.jrj.com/quot-dc/zdt/v1/record"

    headers = {
        "authority": "gateway.jrj.com",
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9",
        "deviceinfo": json.dumps(
            {
                "productId": "6000021",
                "version": "1.0.0",
                "device": "Mozilla/5.0",
                "sysName": "Chrome",
                "sysVersion": ["chrome/145.0.0.0"],
            }
        ),
        "origin": "https://summary.jrj.com.cn",
        "productid": "6000021",
        "referer": "https://summary.jrj.com.cn/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    all_records = []
    page_num = 1
    page_size = 100

    while True:
        payload = {
            "td": trade_date,
            "zdtType": "zt",
            "pageNum": page_num,
            "pageSize": page_size,
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and data.get("code") == 20000:
                records = data.get("data", {}).get("list", [])
                if not records:
                    break
                all_records.extend(records)

                if len(records) < page_size:
                    break
                page_num += 1

                if page_num > 10:
                    st.warning(f"fetch_zt_list_from_jrj: 安全限制触发，page={page_num}")
                    break
            else:
                break
        except Exception as e:
            st.error(f"获取涨停列表失败: {e}")
            break

    # 过滤 ST 股票并排序
    def is_st(name):
        return "ST" in name.upper() if name else False

    filtered = [r for r in all_records if not is_st(r.get("name", ""))]

    # 按封板时间排序（zdttm 是 HHMMSS 格式）
    filtered.sort(key=lambda x: x.get("zdttm", 999999))

    return filtered


def parse_zdt_time(zdttm):
    """解析封板时间 HHMMSS -> HH:MM"""
    if not zdttm:
        return "--:--"
    zdttm_str = str(int(zdttm)).zfill(6)
    return f"{zdttm_str[:2]}:{zdttm_str[2:4]}"


@st.cache_data(ttl="1h")
def get_stock_total_mv(code: str) -> float:
    """
    使用 tushare 获取股票总市值（亿元）

    Args:
        code: 股票代码，如 "000001"

    Returns:
        float: 总市值（亿元），失败返回 0
    """
    try:
        import tushare as ts

        token = st.secrets.get("tushare_token") or os.environ.get("TUSHARE_TOKEN")
        if not token:
            return 0

        ts.set_token(token)
        pro = ts.pro_api()

        # 获取股票基本信息
        df = pro.daily_basic(
            ts_code=f"{code}.SZ" if code.startswith(("0", "3")) else f"{code}.SH",
            fields="ts_code,total_mv",
        )
        if df is not None and not df.empty:
            # total_mv 单位是万元，转换为亿元
            return float(df["total_mv"].iloc[0]) / 10000
        return 0
    except Exception:
        return 0


def enrich_zt_with_mv(zt_records: list) -> list:
    """
    为涨停记录添加总市值数据

    Returns:
        list: 添加 total_mv 字段的记录列表
    """
    enriched = []
    for r in zt_records:
        code = r.get("code", "")
        if code and len(code) == 6:
            total_mv = get_stock_total_mv(code)
            r_copy = r.copy()
            r_copy["total_mv"] = total_mv
            enriched.append(r_copy)
        else:
            r_copy = r.copy()
            r_copy["total_mv"] = 0
            enriched.append(r_copy)
    return enriched


def is_bse_stock(code: str) -> bool:
    """判断是否为北交所股票（代码以 4、8 或 9 开头）"""
    if not code:
        return False
    return code.startswith(("4", "8", "9"))


@st.cache_data(ttl="10m")
def get_capacity_stocks():
    """
    获取当日容量上涨股票（去除北交所）
    条件：
    - 当日成交金额 > 5亿
    - 涨幅 > 8%
    - 市值 50-200亿
    - 5日涨幅 < 25%
    - 去除北交所

    Returns:
        list: 符合条件的股票列表
    """
    try:
        import tushare as ts

        token = st.secrets.get("tushare_token") or os.environ.get("TUSHARE_TOKEN")
        if not token:
            return []

        ts.set_token(token)
        pro = ts.pro_api()

        # 获取当日日期
        today = datetime.datetime.now().strftime("%Y%m%d")

        # 获取当日行情数据
        df_daily = pro.daily(
            trade_date=today, fields="ts_code,open,high,low,close,pct_chg,amount,vol"
        )
        if df_daily is None or df_daily.empty:
            return []

        # 获取每日基础数据（包含总市值）
        df_basic = pro.daily_basic(trade_date=today, fields="ts_code,total_mv")
        if df_basic is None or df_basic.empty:
            return []

        # 合并数据
        df = df_daily.merge(df_basic, on="ts_code", how="left")

        # 提取代码（不含后缀）
        df["code"] = df["ts_code"].str.split(".").str[0]

        # 去除北交所（代码以 4、8 或 9 开头）
        df = df[~df["code"].str.startswith(("4", "8", "9"))]

        if df.empty:
            return []

        # 计算5日涨幅（需要获取前5个交易日数据）
        trade_cal = pro.trade_cal(
            exchange="SSE",
            start_date=(datetime.datetime.now() - datetime.timedelta(days=10)).strftime(
                "%Y%m%d"
            ),
            end_date=today,
        )
        trade_cal = trade_cal[trade_cal["is_open"] == 1].sort_values("cal_date")
        if len(trade_cal) >= 6:
            prev_5d_date = trade_cal.iloc[-6]["cal_date"]
            df_prev = pro.daily(trade_date=prev_5d_date, fields="ts_code,close")
            df_prev.columns = ["ts_code", "close_5d_ago"]
            df = df.merge(df_prev, on="ts_code", how="left")
            df["chg_5d"] = (df["close"] - df["close_5d_ago"]) / df["close_5d_ago"] * 100
        else:
            df["chg_5d"] = 0

        # 转换单位
        df["amount_yi"] = df["amount"] / 100000  # amount单位是千元，转为亿元
        df["total_mv_yi"] = df["total_mv"] / 10000  # total_mv单位是万元，转为亿元

        # 应用筛选条件
        filtered = df[
            (df["amount_yi"] > 5)  # 成交金额 > 5亿
            & (df["pct_chg"] > 8)  # 涨幅 > 8%
            & (df["total_mv_yi"] >= 50)  # 市值 >= 50亿
            & (df["total_mv_yi"] <= 200)  # 市值 <= 200亿
            & (df["chg_5d"] < 25)  # 5日涨幅 < 25%
        ].copy()

        if filtered.empty:
            return []

        # 按成交金额排序
        filtered = filtered.sort_values("amount_yi", ascending=False)

        # 转换为字典列表
        result = []
        for _, row in filtered.iterrows():
            code = row["code"]
            result.append(
                {
                    "code": code,
                    "name": "",  # 名称需要另外获取
                    "close": row["close"],
                    "pct_chg": row["pct_chg"],
                    "amount_yi": round(row["amount_yi"], 2),
                    "total_mv_yi": round(row["total_mv_yi"], 2),
                    "chg_5d": round(row["chg_5d"], 2),
                }
            )

        # 获取股票名称
        try:
            stock_basic = pro.stock_basic(
                exchange="", list_status="L", fields="ts_code,name"
            )
            code_to_name = dict(
                zip(stock_basic["ts_code"].str.split(".").str[0], stock_basic["name"])
            )
            for r in result:
                r["name"] = code_to_name.get(r["code"], "")
        except Exception as e:
            st.warning(f"获取股票名称失败: {e}")

        return result
    except Exception as e:
        st.error(f"获取容量股票失败: {e}")
        return []


# ========== 页面主函数 ==========


def main():
    st.set_page_config(page_title="特征分组", page_icon="📊", layout="wide")

    _section_title("📊 特征分组")

    # AI分析开关
    enable_ai = st.checkbox(
        "开启AI分析", value=False, help="开启后将使用AI对股票进行分类分析"
    )

    # ---- 分组1: 容量上涨股票 ----
    st.markdown(
        "### 💪 容量上涨（成交>5亿，涨幅>8%，市值50-200亿，5日涨幅<25%，去除北交所）"
    )

    with st.spinner("正在筛选容量股票..."):
        capacity_stocks = get_capacity_stocks()

    if not capacity_stocks:
        st.info("暂无符合条件的容量上涨股票")
    else:
        st.caption(f"共 {len(capacity_stocks)} 只")

        # AI 分析 - 容量上涨股票分类
        if enable_ai:
            analyze_stock_classification(
                stock_list=capacity_stocks,
                group_name="容量上涨股票",
                show_ui=True,
            )

        # 一行4列展示 K 线图
        stocks_per_row = 4
        for i in range(0, len(capacity_stocks), stocks_per_row):
            cols = st.columns(stocks_per_row)
            for j, col in enumerate(cols):
                idx = i + j
                if idx < len(capacity_stocks):
                    stock = capacity_stocks[idx]
                    code = stock.get("code", "")
                    name = stock.get("name", "")
                    pct = stock.get("pct_chg", 0)
                    amt = stock.get("amount_yi", 0)
                    mv = stock.get("total_mv_yi", 0)

                    with col:
                        st.markdown(
                            f"**{name}** ({code}) 涨:{pct:.1f}% 额:{amt}亿 市:{mv}亿"
                        )

                        # 获取 K 线数据（添加延时避免请求过快）
                        try:
                            time.sleep(0.3)  # 300ms 延时
                            price_df = get_ak_price_df(code, count=60)
                            if price_df is not None and not price_df.empty:
                                plotK(
                                    price_df,
                                    k="d",
                                    plot_type="candle",
                                    ma_line=(5, 10, 20),
                                    container=st,
                                )
                            else:
                                st.warning(f"{name} 无 K 线数据")
                        except Exception as e:
                            st.warning(f"{name} 获取 K 线失败")

    st.markdown("---")

    # ---- 分组2: 10:30前涨停 ----
    st.markdown("### 📈 10:30前涨停（JRJ，总市值≥50亿，已过滤ST）")

    # 获取涨停列表
    with st.spinner("正在获取涨停数据..."):
        zt_records = fetch_zt_list_from_jrj("")

    if not zt_records:
        st.info("暂无涨停数据")
    else:
        # 获取总市值数据
        with st.spinner("正在获取市值数据..."):
            zt_records = enrich_zt_with_mv(zt_records)

        # 筛选 10:30 前涨停且总市值 >= 50亿的股票
        early_zt = [
            r
            for r in zt_records
            if r.get("zdttm", 0) <= 103000 and r.get("total_mv", 0) >= 50
        ]

        if not early_zt:
            st.info("暂无符合条件的涨停股票")
        else:
            st.caption(f"共 {len(early_zt)} 只")

            # AI 分析 - 10:30前涨停股票分类
            if enable_ai:
                analyze_stock_classification(
                    stock_list=early_zt,
                    group_name="10:30前涨停",
                    show_ui=True,
                )

            # 一行4列展示 K 线图
            stocks_per_row = 4
            for i in range(0, len(early_zt), stocks_per_row):
                cols = st.columns(stocks_per_row)
                for j, col in enumerate(cols):
                    idx = i + j
                    if idx < len(early_zt):
                        stock = early_zt[idx]
                        code = stock.get("code", "")
                        name = stock.get("name", "")
                        zdt_time = parse_zdt_time(stock.get("zdttm"))
                        total_mv = stock.get("total_mv", 0)

                        with col:
                            st.markdown(
                                f"**{name}** ({code}) 封板:{zdt_time} 市:{total_mv:.0f}亿"
                            )

                            # 获取 K 线数据（添加延时避免请求过快）
                            try:
                                time.sleep(0.3)  # 300ms 延时
                                price_df = get_ak_price_df(code, count=60)
                                if price_df is not None and not price_df.empty:
                                    plotK(
                                        price_df,
                                        k="d",
                                        plot_type="candle",
                                        ma_line=(5, 10, 20),
                                        container=st,
                                    )
                                else:
                                    st.warning(f"{name} 无 K 线数据")
                            except Exception as e:
                                st.warning(f"{name} 获取 K 线失败")


if __name__ == "__main__":
    main()
