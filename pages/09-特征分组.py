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

from infra.config import get_tushare_token
from tools import plotK
from tools.kline_data import get_ak_price_df
from services.ai_analysis import analyze_stock_classification
from services.daily_basic_service import get_daily_basic_smart


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

        token = get_tushare_token()
        if not token:
            return 0

        ts.set_token(token)
        pro = ts.pro_api()

        # 获取股票基本信息（优先本地库）
        ts_code = f"{code}.SZ" if code.startswith(("0", "3")) else f"{code}.SH"
        
        df = get_daily_basic_smart(
            trade_date=datetime.datetime.now().strftime("%Y%m%d"),
            fields=["ts_code", "total_mv"],
            use_cache=True
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

        token = get_tushare_token()
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

        # 获取每日基础数据（包含总市值，优先本地库）
        df_basic = get_daily_basic_smart(
            trade_date=today,
            fields=["ts_code", "total_mv"],
            use_cache=True
        )
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


# ========== 关注列表管理 ==========

# 关注列表数据文件路径
WATCHLIST_FILE = "datas/watchlist.json"


def _init_watchlist_state():
    """初始化关注列表到 session_state"""
    if "watchlist_data" not in st.session_state:
        if not os.path.exists(WATCHLIST_FILE):
            st.session_state.watchlist_data = {"watchlist": []}
        else:
            try:
                with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
                    st.session_state.watchlist_data = json.load(f)
            except Exception as e:
                st.session_state.watchlist_data = {"watchlist": []}


def get_watchlist():
    """
    获取关注列表（从 session_state）

    Returns:
        dict: 包含 watchlist 列表的字典
    """
    _init_watchlist_state()
    return st.session_state.watchlist_data


def save_watchlist(watchlist_data):
    """
    保存关注列表到文件和 session_state

    Args:
        watchlist_data: 包含 watchlist 列表的字典

    Returns:
        bool: 保存是否成功
    """
    try:
        os.makedirs("datas", exist_ok=True)
        with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
            json.dump(watchlist_data, f, ensure_ascii=False, indent=2)
        # 同时更新 session_state
        st.session_state.watchlist_data = watchlist_data
        return True
    except Exception as e:
        st.error(f"保存关注列表失败: {e}")
        return False


def add_stock_to_watchlist(code, name):
    """
    添加股票到关注列表

    Args:
        code: 股票代码
        name: 股票名称

    Returns:
        tuple: (bool, str) - 是否成功，消息
    """
    _init_watchlist_state()
    watchlist_data = get_watchlist()

    if "watchlist" not in watchlist_data:
        watchlist_data["watchlist"] = []

    # 检查是否已存在
    watched_codes = [item.get("code") for item in watchlist_data["watchlist"]]
    if code in watched_codes:
        return False, "已关注"

    # 添加新股票
    watchlist_data["watchlist"].append(
        {
            "code": code,
            "name": name,
            "add_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )

    if save_watchlist(watchlist_data):
        return True, "关注成功"
    return False, "保存失败"


def is_watched(code, watchlist_data=None):
    """
    检查股票是否已关注

    Args:
        code: 股票代码
        watchlist_data: 关注列表数据（可选）

    Returns:
        bool: 是否已关注
    """
    if watchlist_data is None:
        watchlist_data = get_watchlist()

    if "watchlist" not in watchlist_data:
        return False

    watched_codes = [item.get("code") for item in watchlist_data["watchlist"]]
    return code in watched_codes


@st.fragment
def stock_card_with_watch(stock, stock_type="capacity"):
    """
    股票卡片，包含关注功能（使用 fragment 实现局部刷新）

    Args:
        stock: 股票数据字典
        stock_type: 股票类型（"capacity" 或 "zt"）
    """
    code = stock.get("code", "")
    name = stock.get("name", "")

    # 判断是否已关注
    watchlist_data = get_watchlist()
    watched = is_watched(code, watchlist_data)

    with st.container():
        # 股票信息行
        if stock_type == "capacity":
            pct = stock.get("pct_chg", 0)
            amt = stock.get("amount_yi", 0)
            mv = stock.get("total_mv_yi", 0)
            title = f"**{name}** ({code}) 涨:{pct:.1f}% 额:{amt}亿 市:{mv}亿"
        else:
            zdt_time = parse_zdt_time(stock.get("zdttm"))
            total_mv = stock.get("total_mv", 0)
            title = f"**{name}** ({code}) 封板:{zdt_time} 市:{total_mv:.0f}亿"

        st.markdown(title)

        # 关注按钮行
        if not watched:
            if st.button(
                "⭐ 关注", key=f"watch_{stock_type}_{code}", use_container_width=True
            ):
                success, msg = add_stock_to_watchlist(code, name)
                if success:
                    st.toast(f"{name} {msg}", icon="✅")
                else:
                    st.toast(f"{name} {msg}", icon="⚠️")
                st.rerun(scope="fragment")
        else:
            st.button(
                "✅ 已关注",
                key=f"watched_{stock_type}_{code}",
                use_container_width=True,
                disabled=True,
            )

        # K 线图
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


# ========== 页面主函数 ==========


def main():
    st.set_page_config(page_title="特征分组", page_icon="📊", layout="wide")

    # 初始化关注列表
    _init_watchlist_state()

    _section_title("📊 特征分组")

    # AI分析开关
    enable_ai = st.checkbox(
        "开启AI分析", value=False, help="开启后将使用AI对股票进行分类分析"
    )

    # ---- 分组1: 容量上涨股票 ----
    st.markdown("---")
    col1, col2 = st.columns([0.7, 0.3])
    with col1:
        st.markdown(
            "### 💪 容量上涨（成交>5亿，涨幅>8%，市值50-200亿，5日涨幅<25%，去除北交所）"
        )
    with col2:
        if st.button("📊 查看详情", key="btn_capacity", use_container_width=True):
            st.session_state.show_capacity = True

    # 使用session_state控制数据加载和显示
    if "show_capacity" not in st.session_state:
        st.session_state.show_capacity = False
    if "capacity_stocks" not in st.session_state:
        st.session_state.capacity_stocks = None
    if "capacity_loading" not in st.session_state:
        st.session_state.capacity_loading = False

    # 点击按钮后加载数据
    if (
        st.session_state.show_capacity
        and st.session_state.capacity_stocks is None
        and not st.session_state.capacity_loading
    ):
        st.session_state.capacity_loading = True
        with st.spinner("正在筛选容量股票..."):
            st.session_state.capacity_stocks = get_capacity_stocks()
        st.session_state.capacity_loading = False
        st.rerun()

    # 显示内容
    if st.session_state.show_capacity:
        capacity_stocks = st.session_state.capacity_stocks

        if capacity_stocks is None:
            st.info("点击上方按钮加载数据")
        elif not capacity_stocks:
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

            # 一行4列展示股票卡片（带关注功能）
            stocks_per_row = 4
            for i in range(0, len(capacity_stocks), stocks_per_row):
                cols = st.columns(stocks_per_row)
                for j, col in enumerate(cols):
                    idx = i + j
                    if idx < len(capacity_stocks):
                        stock = capacity_stocks[idx]
                        with col:
                            stock_card_with_watch(stock, stock_type="capacity")

    # ---- 分组2: 10:30前涨停 ----
    st.markdown("---")
    col1, col2 = st.columns([0.7, 0.3])
    with col1:
        st.markdown("### 📈 10:30前涨停（JRJ，总市值≥50亿，已过滤ST）")
    with col2:
        if st.button("📈 查看详情", key="btn_zt", use_container_width=True):
            st.session_state.show_zt = True

    # 使用session_state控制数据加载和显示
    if "show_zt" not in st.session_state:
        st.session_state.show_zt = False
    if "zt_stocks" not in st.session_state:
        st.session_state.zt_stocks = None
    if "zt_loading" not in st.session_state:
        st.session_state.zt_loading = False

    # 点击按钮后加载数据
    if (
        st.session_state.show_zt
        and st.session_state.zt_stocks is None
        and not st.session_state.zt_loading
    ):
        st.session_state.zt_loading = True
        with st.spinner("正在获取涨停数据..."):
            zt_records = fetch_zt_list_from_jrj("")
            if zt_records:
                with st.spinner("正在获取市值数据..."):
                    zt_records = enrich_zt_with_mv(zt_records)
                # 筛选 10:30 前涨停且总市值 >= 50亿的股票
                early_zt = [
                    r
                    for r in zt_records
                    if r.get("zdttm", 0) <= 103000 and r.get("total_mv", 0) >= 50
                ]
                st.session_state.zt_stocks = early_zt
            else:
                st.session_state.zt_stocks = []
        st.session_state.zt_loading = False
        st.rerun()

    # 显示内容
    if st.session_state.show_zt:
        early_zt = st.session_state.zt_stocks

        if early_zt is None:
            st.info("点击上方按钮加载数据")
        elif not early_zt:
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

            # 一行4列展示股票卡片（带关注功能）
            stocks_per_row = 4
            for i in range(0, len(early_zt), stocks_per_row):
                cols = st.columns(stocks_per_row)
                for j, col in enumerate(cols):
                    idx = i + j
                    if idx < len(early_zt):
                        stock = early_zt[idx]
                        with col:
                            stock_card_with_watch(stock, stock_type="zt")


if __name__ == "__main__":
    main()
