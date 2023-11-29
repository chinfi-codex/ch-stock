#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import streamlit as st
import json
import akshare as ak
import datetime

from datas import spider, storager
from datas.findata import EconomicIndicators
from datas.cninfo import cninfo_announcement_spider, get_stock_list
from tools.llm import get_chatgpt_chat


st.set_page_config(
    page_title="FinNews",
    page_icon="🚀",
    layout="wide",
)

news_tab, follow_tab = st.tabs(['电报200','跟踪信息'])
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
        

with follow_tab:
    col1,col2,col3,col4,col5=st.columns(5)
    with col1:
        try:
            rate = EconomicIndicators.get_exchangerates_realtime()
            rate = rate['5. Exchange Rate']
            rate = "{:.3f}".format(float(rate))
            st.metric('汇率',value=rate)
            history_rates = EconomicIndicators.get_exchangerates_daily(curDate=3)
            st.write(history_rates)
        except Exception as e:
            st.write(e)
    with col2:
        try:
            ty_df = EconomicIndicators.get_treasury_yield()
            ty_0 = ty_df.loc[0,'value']
            st.metric('美10债',ty_0, delta_color='inverse')
            st.write(ty_df.head(3))
        except Exception as e:
            st.write(e)
    with col3:
        try:
            oils = EconomicIndicators.get_commodities('WTI')
            oil1 = oils.loc[0,'value']
            st.metric('WTI原油',oil1, delta_color='inverse')
            st.write(oils.head(3))
        except Exception as e:
            st.write(e)
    with col4:
        try:
            gas = EconomicIndicators.get_commodities('natural_gas')
            gas1 = gas.loc[0, 'value']
            st.metric('天然气',gas1, delta_color='inverse')
            st.write(gas.head(3))
        except Exception as e:
            st.write(e)
    with col5:
        try:
            au99 = ak.spot_hist_sge(symbol='Au99.99').iloc[::-1].reset_index(drop=True)
            au991 = au99.loc[0, 'close']
            st.metric('沪金',au991, delta_color='inverse')
            st.write(au99.head(3)[['date','close']])
        except Exception as e:
            st.write(e)

    tag_col,keyword_col = st.columns([1,1])
    with tag_col:
        def write_tags_content(startdate, tags):
            for tag in tags:
                sql = f"""
                SELECT * FROM NEWS_CLS
                WHERE tags LIKE "%{tag}%" AND date BETWEEN '{startdate}' AND CURRENT_DATE
                ORDER BY date DESC, time DESC
                """
                news = storager.mysql_retriever(sql)
                with st.expander(tag):
                    for i,row in news.iterrows():
                        st.write(row['date'])
                        st.write(row['content'])

        tags = ["经济数据","美国经济","俄乌","巴以冲突"]
        startdate = datetime.date.today() - datetime.timedelta(days=5)
        startdate_str = startdate.strftime("%Y-%m-%d")
        write_tags_content(startdate_str, tags)

    with keyword_col:
        def write_keyword_content(startdate,keyword):
            sql = f"""
            SELECT * FROM NEWS_CLS
            WHERE content LIKE '%{keyword}%' AND date BETWEEN '{startdate}' AND CURRENT_DATE
            ORDER BY date DESC, time DESC
            """
            news = storager.mysql_retriever(sql)
            with st.expander(keyword):
                for i,row in news.iterrows():
                    st.write(row['date'])
                    st.write(row['content'])

        keywords = ["华为","特斯拉","英伟达","苹果","马斯克"]
        startdate = datetime.date.today() - datetime.timedelta(days=2)
        startdate_str = startdate.strftime("%Y-%m-%d")
        write_tags_content(startdate_str, keywords)


    

