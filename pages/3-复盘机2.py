import streamlit as st
import pandas as pd
import json
import datetime
import requests

import akshare as ak
import mplfinance as mpf
from datas.storager import mysql_retriever,mysql_storager
from qmt.kdata import *
from qmt.boardData import *


st.set_page_config(layout="wide")
with st.container():
    c1,c2 = st.columns([1,1])
    with c1:
        select_date = st.date_input('日期')
    with c2:
        concepts, industries = get_all_boards()
        concept_names = [c['name'] for c in concepts]
        select_board = st.selectbox('板块',options=concept_names)
date_str = str(select_date).replace('-','')


def day_high_time(code):
    df = get_ak_interval_price_df(code,date_str)
    day_high_time = df['close'].idxmax()
    return day_high_time


def plot_kline_byrow(df,columns_per_row=3):
    rows = len(df) // columns_per_row + int(len(df) % columns_per_row > 0)
    for i in range(rows):
        cols = st.columns(columns_per_row)
        for j in range(columns_per_row):
            index = i * columns_per_row + j
            if index < len(df):
                code = df.loc[index,'代码']
                name = df.loc[index,'名称']
                cols[j].write(name)
                price = get_ak_price_df(code,str(select_date).replace('-',''))
                plotK(price,container=cols[j])


if st.button('查询'):
    a1,a2 = st.columns([0.6,0.4])
    board_df = get_concept_board_index(select_board,count=100)
    with a1:
        for i in concepts:
            if i['name'] == select_board:
                code = i['code']
                break    
        board_stocks_df = ak.stock_board_cons_ths(code)
        board_stocks_df = board_stocks_df[['代码','名称','涨跌幅','流通市值','成交额']]
        board_stocks_df['日内高点'] = board_stocks_df['代码'].apply(day_high_time)
        board_stocks_df['涨跌幅'] = board_stocks_df['涨跌幅'].replace('--','0')
        #board_stocks_df['涨停统计'] = board_stocks_df['代码'].apply(get_zt_counts)
        board_stocks_df = board_stocks_df[board_stocks_df['涨跌幅'].astype(float) >= 5].sort_values('日内高点')

        st.dataframe(board_stocks_df,hide_index=True)
    with a2:
        board_df['pct'] = board_df['close'].pct_change()
        st.write('今日涨幅:',round(board_df['pct'].tolist()[-1],2))
        plotK(board_df)

    plot_kline_byrow(board_stocks_df)


if st.button('今日大涨'):
    zt_all_df = ak.stock_zt_pool_em(date=date_str)
    df = get_top_df()
    df['日内高点'] = df['代码'].apply(day_high_time)
    mapping = zt_all_df.set_index('名称')['涨停统计'].to_dict()
    df['涨停统计'] = df['名称'].map(mapping)
    df['涨停统计'] = df['涨停统计'].astype(str).apply(lambda x:x.replace('/','~'))
    df = df.sort_values('日内高点')
    st.dataframe(df,hide_index=True)

    plot_kline_byrow(df)

