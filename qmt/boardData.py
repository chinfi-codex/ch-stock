import streamlit as st
import pandas as pd
import numpy as np
import json

import akshare as ak
import mplfinance as mpf


@st.cache_data(ttl='1d')
def get_all_boards():
    concept_df = ak.stock_board_concept_name_ths()
    concept_df = concept_df[['概念名称','代码']]
    concept_df.columns = ['name', 'code']

    industry_df = ak.stock_board_industry_summary_ths()
    industry_df = industry_df[['板块']]
    industry_df.columns = ['name']
    return concept_df.to_dict('records'), industry_df.to_dict('records')


@st.cache_data(ttl='0.5d')
def get_concept_board_index(concept_name,count=181):
    df = ak.stock_board_concept_hist_ths(symbol=concept_name).tail(count)
    df.columns = ['date','open','high','low','close','volume_','volume']
    df = df[['date','open','high','low','close','volume']]
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date',inplace=True)
    return df


@st.cache_data(ttl='0.5d')
def get_top_df():
    df = ak.stock_board_cons_ths(symbol="883421").head(100)
    df = df[df['涨跌幅'].astype(float) >= 9.85]
    df = df[['代码','名称','涨跌幅']]
    return df