#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import streamlit as st
import json
import akshare as ak

from datas import spider, storager
from datas.findata import EconomicIndicators
from datas.cninfo import cninfo_announcement_spider, get_stock_list
from analysts import NewsTrends
from tools.llm import get_chatgpt_chat


st.set_page_config(
    page_title="FinNews",
    page_icon="🚀",
    layout="wide",
)

news_tab, aly_tab, marco_tab = st.tabs(['电报200','新闻分析','宏观数据'])
with news_tab:
    @st.cache_data(ttl="5m",show_spinner="加载中...")
    def write_news():
        spider.NewsSpider().run()
        news_df = storager.mysql_retriever("SELECT * FROM NEWS_CLS ORDER BY date DESC, time DESC LIMIT 200")
        return news_df
    news_df = write_news()

    hl = st.checkbox("🗞只看高亮")
    if hl:
        news_df = news_df[news_df['degree'] == 'B']

    for i,row in news_df.iterrows():
        if row['degree'] == 'B' or row['degree'] == 'A':
            content = f"<font color='red'> {row['content']} </font>"
        else:
            content = row['content']
        st.markdown(content,unsafe_allow_html=True)
        st.write(row['time'])
        st.divider()
        

# with aly_tab:
#     newsTrends = NewsTrends()
#     n1,n2 = st.columns([2,1])
#     with n2:
#         img = newsTrends.tag_cloud_img(height=800)
#         st.image(img)
    # with n1:
    #     subjects = newsTrends.llm_tags_cluster()
    #     subjects = subjects.sort_values('Word Count',ascending=False)
    #     for i,row in subjects.iterrows():
    #         st.write(row['Word Count'],row['Top Words'])
    #         q_btn = st.button('Query',key=row['Topic'])
    #         if q_btn:
    #             rs = newsTrends.vector_query(row['Top Words'])
    #             st.write(rs)

with marco_tab:
    col1,col2,col3,col4,col5=st.columns(5)
    with col1:
        try:
            rate = EconomicIndicators.get_exchangerates_realtime()
            rate = rate['5. Exchange Rate']
            rate = "{:.3f}".format(float(rate))
            st.metric('汇率',value=rate)
            #history_rates = EconomicIndicators.get_exchangerates_daily(curDate=90)
        except Exception as e:
            st.write(e)
    with col2:
        try:
            ty_df = EconomicIndicators.get_treasury_yield()
            ty_0 = ty_df.loc[0,'value']
            st.metric('美10债',ty_0, delta_color='inverse')
        except Exception as e:
            st.write(e)
    with col3:
        try:
            federal_df = EconomicIndicators.get_federal_rate()
            federal_rate1 = federal_df.loc[0, 'value']
            st.metric('美利率',federal_rate1, delta_color='inverse')
        except Exception as e:
            st.write(e)
    with col4:
        try:
            oils = EconomicIndicators.get_commodities('WTI')
            oil1 = oils.loc[0,'value']
            st.metric('WTI原油',oil1, delta_color='inverse')
        except Exception as e:
            st.write(e)
    with col5:
        try:
            gas = EconomicIndicators.get_commodities('natural_gas')
            gas1 = gas.loc[0, 'value']
            st.metric('天然气',gas1, delta_color='inverse')
        except Exception as e:
            st.write(e)


    macro_ec = ak.macro_china_qyspjg().iloc[:12] # 国民经济
    macro_financing = ak.macro_china_shrzgm().tail(30).iloc[::-1] #社融
    macro_pmi = ak.macro_china_cx_pmi_yearly().tail(12).iloc[::-1] #财新PMI
    st.write(macro_ec)
    st.write(macro_financing)
    st.write(macro_pmi)

    

