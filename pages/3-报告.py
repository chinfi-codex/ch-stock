import streamlit as st
import pandas as pd
import numpy as np
import json
import requests
import datetime
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS'] 

from langchain.text_splitter import RecursiveCharacterTextSplitter
import akshare as ak

from tools.llm import get_chatgpt_chat
from datas.findata import EconomicIndicators
from datas import spider
from datas.storager import mysql_retriever
from tools.SparkApi import get_spark_chat


st.set_page_config(
    page_title="周报",
    page_icon="🚀",
    layout="wide"
)

@st.cache_data(ttl="1day")
def plot_exchange():
    rates_df = EconomicIndicators.get_exchangerates_daily(curDate=30)
    rates_str = rates_df.to_dict()['4. close']

    sorted_data = dict(sorted(rates_str.items()))
    dates = list(sorted_data.keys())
    values = [float(v) for v in sorted_data.values()]

    # Extracting the last week's data
    last_week_dates = dates[-7:]
    last_week_values = values[-7:]

    # Plotting the curve with the last week shaded, but keeping the y-axis limits the same as the previous chart
    plt.figure(figsize=(15, 6))
    plt.plot(dates, values, marker='o', linestyle='-', color='b')
    plt.fill_between(last_week_dates, last_week_values, color='gray', alpha=0.5)
    plt.xticks(rotation=45)
    plt.ylim(min(values), max(values))  # Setting the y-axis limits to the previous chart's limits
    plt.xlabel("Date")
    plt.ylabel("Value")
    plt.title("Curve with the last week shaded (consistent y-axis)")
    plt.tight_layout()
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    st.pyplot(plt)


def write_tags_content(startdate, tags):
    def news_fetcher(tag,startdate):
        sql = f"""
        SELECT * FROM NEWS_CLS
        WHERE tags LIKE "%{tag}%" AND date BETWEEN '{startdate}' AND CURRENT_DATE
        ORDER BY date DESC, time DESC
        """
        news_df = mysql_retriever(sql)
        return news_df

    for t in tags:
        with st.expander(t):
            news = news_fetcher(t,startdate)
            for i,row in news.iterrows():
                st.write(row['date'])
                st.write(row['content'])


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


spider.NewsSpider().run()
tags = ["经济数据","美国经济","俄乌","巴以冲突"]
startdate = datetime.date.today() - datetime.timedelta(days=5)
startdate_str = startdate.strftime("%Y-%m-%d")
write_tags_content(startdate_str, tags)
