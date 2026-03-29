#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
市场专题分析服务。
"""

import akshare as ak
import pandas as pd
import streamlit as st

from tools.utils import get_stock_list


@st.cache_data(ttl="1d")
def get_longhu_data(date):
    """龙虎榜游资数据。"""
    chairs_set = {
        "毛老板": ["上海东方路", "深圳金田路", "成都通盈街", "北京光华路"],
        "章盟主": [
            "中信证券杭州延安路",
            "上海江苏路",
            "宁波彩虹北路",
            "上海建国西路",
            "杭州四季路",
        ],
        "赵老哥": ["浙商证券绍兴分", "北京阜成路", "上海嘉善路"],
        "方新侠": ["朱雀大街", "兴业证券陕西分公司"],
    }
    all_business_names = [name for names in chairs_set.values() for name in names]

    lh_yz_df = ak.stock_lhb_hyyyb_em(start_date=date, end_date=date)
    lh_yz_df = lh_yz_df[
        lh_yz_df["营业部名称"].str.contains("|".join(all_business_names), na=False)
    ]
    if lh_yz_df.empty:
        return lh_yz_df

    def get_chair_name(business_name):
        for chair, names in chairs_set.items():
            if any(b in business_name for b in names):
                return chair
        return None

    lh_yz_df["游资"] = lh_yz_df["营业部名称"].apply(get_chair_name)
    s = (
        lh_yz_df["买入股票"]
        .str.split(" ")
        .apply(pd.Series, 1)
        .stack()
        .reset_index(level=1, drop=True)
    )
    s.name = "买入股票_new"
    lh_yz_df = lh_yz_df.drop("买入股票", axis=1).join(s)
    lh_yz_df = lh_yz_df.rename(columns={"买入股票_new": "买入股票"}).reset_index()

    stocks_df = get_stock_list()
    for i, row in lh_yz_df.iterrows():
        try:
            symbol = stocks_df.loc[stocks_df["zwjc"] == row["买入股票"], "code"].iloc[0]
            detail_buy_df = ak.stock_lhb_stock_detail_em(symbol=symbol, date=date, flag="买入")
            detail_sell_df = ak.stock_lhb_stock_detail_em(symbol=symbol, date=date, flag="卖出")
            detail_df_yz = pd.concat([detail_buy_df, detail_sell_df], axis=0)
            detail_df_yz.fillna(0, inplace=True)
            index = 1 if len(detail_df_yz) > 1 and detail_df_yz["类型"].nunique() == 1 else 0
            lh_yz_df.loc[i, "买入金额"] = detail_df_yz["买入金额"].iloc[0]
            lh_yz_df.loc[i, "卖出金额"] = detail_df_yz["卖出金额"].iloc[index]
        except Exception:
            pass

    lh_yz_df["净买入"] = lh_yz_df["买入金额"] - lh_yz_df["卖出金额"]
    lh_yz_df = lh_yz_df[
        (lh_yz_df["买入金额"] > 10000000) | (lh_yz_df["卖出金额"] > 10000000)
    ].sort_values("净买入", ascending=False)
    return lh_yz_df
