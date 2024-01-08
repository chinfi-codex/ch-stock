import streamlit as st
import pandas as pd
import numpy as np
import json

import akshare as ak
import mplfinance as mpf


@st.cache_data(ttl='0.5d')
def get_ak_price_df(code,end_date,count=60):
    df = ak.stock_zh_a_hist(code,period="daily",end_date=end_date,adjust='').tail(count)
    df.columns = ['date','open','close','high','low','volume_','volume','range','pct','pct_','change']
    df = df[['date','open','close','high','low','volume']]
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date',inplace=True)
    return df


@st.cache_data(ttl='0.5d')
def get_ak_interval_price_df(code,end_date,count=241):
    df = ak.stock_zh_a_hist_min_em(code,end_date=end_date,period='1').tail(count)
    df.columns = ['date','open','close','high','low','volume_','volume','lastprice']
    df = df[['date','open','close','high','low','volume']]
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date',inplace=True)
    return df


def plotK(
    df,
    k='d', #日/周/月
    ma_line_on=False,
    fail_zt=False,
    container=st
    ):
    if k == 'w':
        df = df.resample('W').agg({'open': 'first', 
                                          'high': 'max', 
                                          'low': 'min', 
                                          'close': 'last',
                                          'volume': 'sum'})
    if k == 'm':
        df = df.resample('M').agg({'open': 'first', 
                                       'high': 'max', 
                                       'low': 'min', 
                                       'close': 'last',
                                       'volume': 'sum'})

    if fail_zt:
        mc = mpf.make_marketcolors(up='black', down='darkgray', inherit=True)
    else:
        mc = mpf.make_marketcolors(up='r',down='g',inherit=True)
    s  = mpf.make_mpf_style(marketcolors=mc,gridaxis='horizontal',gridstyle='dashed')
    plot_args = {
        'type': 'candle',
        'style': s,
        'volume': True,
        'returnfig': True
    }
    if ma_line_on: plot_args['mav'] = (5, 10, 20, 144, 250)

    fig, axe = mpf.plot(df, **plot_args)
    container.pyplot(fig)


