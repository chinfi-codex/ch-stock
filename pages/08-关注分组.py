#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
关注分组页面
展示用户关注的股票列表，以表格形式显示
"""

import streamlit as st
import pandas as pd
from services.watchlist_service import get_watchlist, init_watchlist_state


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
    init_watchlist_state()

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
                df_watch[["code", "name", "source_group", "add_time"]],
                column_config={
                    "code": st.column_config.TextColumn("代码", width="small"),
                    "name": st.column_config.TextColumn("名称", width="medium"),
                    "source_group": st.column_config.TextColumn("来源分组", width="medium"),
                    "add_time": st.column_config.TextColumn("添加时间", width="medium"),
                },
                use_container_width=True,
                hide_index=True,
                key="watchlist_table",
            )


if __name__ == "__main__":
    main()
