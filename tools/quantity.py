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



# 函数来计算EMA
def ema(values, period):
    """ Calculate Exponential Moving Average """
    return values.ewm(span=period, adjust=False).mean()

# 函数来计算MACD
def macd(values, fastperiod=12, slowperiod=26, signalperiod=9):
    """ Calculate MACD """
    ema_fast = ema(values, fastperiod)
    ema_slow = ema(values, slowperiod)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signalperiod)
    macd_hist = macd_line - signal_line
    return macd_line, signal_line, macd_hist

# 函数来计算RSI
def rsi(close, period=14):
    """ Calculate Relative Strength Index (RSI) """
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# 函数来计算布林带
def bollinger_bands(close, period=20, num_std=2):
    """ Calculate Bollinger Bands """
    sma = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()
    upper_band = sma + (std * num_std)
    lower_band = sma - (std * num_std)
    return upper_band, sma, lower_band

# 评估股票是否符合特定特征的函数
def evaluate_stock_characteristics(stock_df):
    # Last available data point for the stock
    latest_data = stock_df.iloc[-1]

    # Check MACD
    macd_positive = latest_data['MACD'] > 0

    # Check EMA
    ema_trend = latest_data['EMA_short'] > latest_data['EMA_long']

    # Check RSI
    rsi_not_overbought = latest_data['RSI'] < 70

    # Check Bollinger Bands
    price_near_upper_band = latest_data['close'] >= latest_data['Boll_Upper']

    # All conditions must be true for the stock to fit the common characteristics
    return all([macd_positive, ema_trend, rsi_not_overbought, price_near_upper_band])


