#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
关注分组页面
展示用户关注的股票列表，以表格形式显示
"""

import os
import json
import datetime
import time

import streamlit as st
import pandas as pd

from tools import plotK
from tools.kline_data import get_ak_price_df


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


def remove_stock_from_watchlist(code):
    """
    从关注列表移除股票

    Args:
        code: 股票代码

    Returns:
        tuple: (bool, str) - 是否成功，消息
    """
    _init_watchlist_state()
    watchlist_data = get_watchlist()

    if "watchlist" not in watchlist_data or not watchlist_data["watchlist"]:
        return False, "列表为空"

    # 查找股票名称
    stock_name = ""
    for item in watchlist_data["watchlist"]:
        if item.get("code") == code:
            stock_name = item.get("name", "")
            break

    # 移除股票
    watchlist_data["watchlist"] = [
        item for item in watchlist_data["watchlist"] if item.get("code") != code
    ]

    if save_watchlist(watchlist_data):
        return True, f"已移除 {stock_name}"
    return False, "保存失败"


def _section_title(title):
    """页面标题样式"""
    st.markdown(
        f"<div style='font-size:26px;font-weight:700;margin:8px 0 8px 0;'>{title}</div>",
        unsafe_allow_html=True,
    )


def main():
    """主函数"""
    st.set_page_config(page_title="关注分组", page_icon="⭐", layout="wide")

    # 初始化关注列表
    _init_watchlist_state()

    _section_title("⭐ 关注分组")

    # 加载关注列表
    watchlist_data = get_watchlist()
    watchlist = watchlist_data.get("watchlist", [])

    # 显示关注列表
    if not watchlist:
        st.info("暂无关注的股票，请在「特征分组」页面点击「⭐ 关注」添加")
    else:
        st.caption(f"共 {len(watchlist)} 只股票")

        # 转换为 DataFrame 显示
        df_watch = pd.DataFrame(watchlist)

        if not df_watch.empty:
            # 显示表格
            st.dataframe(
                df_watch[["code", "name", "add_time"]],
                column_config={
                    "code": st.column_config.TextColumn("代码", width="small"),
                    "name": st.column_config.TextColumn("名称", width="medium"),
                    "add_time": st.column_config.TextColumn("添加时间", width="medium"),
                },
                use_container_width=True,
                hide_index=True,
                key="watchlist_table",
            )

        st.markdown("---")

        # 显示每只股票的详细信息
        st.markdown("### 📈 股票详情")

        for idx, stock in enumerate(watchlist):
            code = stock.get("code", "")
            name = stock.get("name", "")
            add_time = stock.get("add_time", "")

            # 使用 expander 展示
            with st.expander(
                f"**{name}** ({code}) - 添加时间：{add_time}", expanded=False
            ):
                # 删除按钮
                if st.button(
                    f"❌ 移除关注", key=f"remove_{code}", use_container_width=True
                ):
                    success, msg = remove_stock_from_watchlist(code)
                    if success:
                        st.toast(msg, icon="✅")
                        st.rerun()
                    else:
                        st.toast(msg, icon="⚠️")

                # K 线图
                try:
                    with st.spinner(f"正在加载 {name} 的 K 线数据..."):
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
                        st.warning(f"{name} 暂无 K 线数据")
                except Exception as e:
                    st.warning(f"{name} 获取 K 线失败: {str(e)}")


if __name__ == "__main__":
    main()
