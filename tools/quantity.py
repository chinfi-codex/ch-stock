import streamlit as st
import pandas as pd
import numpy as np
import json

import akshare as ak
import mplfinance as mpf

    
def get_ak_df(code,count=100, **kwargs):
    df = ak.stock_zh_a_hist(symbol=code, **kwargs, adjust="qfq")
    df = df.tail(count)

    df.columns = ['date','open','close','high','low','volume_','volume','range','pct','pct_','change']
    df = df[['date','open','close','high','low','volume']]
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date',inplace=True)
    return df


def plotK(df,k='d',container=st):
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

    mc = mpf.make_marketcolors(up='r',down='g',inherit=True)
    s  = mpf.make_mpf_style(marketcolors=mc,gridaxis='horizontal',gridstyle='dashed')
    fig, axe = mpf.plot(df, type='candle', style=s,
             volume=True,
             mav=(5,10,20,30,60,100),
             returnfig=True)
    container.pyplot(fig)



