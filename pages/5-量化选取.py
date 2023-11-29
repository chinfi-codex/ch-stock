import streamlit as st
import pandas as pd
import json
import datetime
import requests

import akshare as ak
import mplfinance as mpf
from tools.quantity import *
from datas.cninfo import get_stock_list


st.set_page_config(
    page_icon="🚀",
    layout="wide"
)


if st.button('指标选股'):
    codes = []
    all_stocks = get_stock_list()
    all_codes = all_stocks['code'].tolist()
    for code in all_codes:
        try:
            df = get_ak_price_df(code,datetime.datetime.today().strftime('%Y%m%d'))
            df['MACD'], df['MACD_signal'], df['MACD_hist'] = macd(df['close'])
            df['EMA_short'] = ema(df['close'], 12)
            df['EMA_long'] = ema(df['close'], 26)
            df['RSI'] = rsi(df['close'])
            df['Boll_Upper'], df['Boll_Middle'], df['Boll_Lower'] = bollinger_bands(df['close'])

            is_characteristic = evaluate_stock_characteristics(df)
            if is_characteristic:
                st.write(code)
                codes.append(code)
        except Exception as e:
            print (code)
            pass
    selected_stocks = all_stocks[all_stocks['code'].isin(codes)]
    st.write(selected_stocks)


if st.button('概念板块'):
    @st.cache_data(ttl='1d')
    def get_ths_concepts():
        df = ak.stock_board_concept_name_ths()
        df = df[['概念名称','代码']]
        return df.to_dict('records')

    concepts = get_ths_concepts()
    df = ak.stock_board_concept_hist_ths(start_year='2023',symbol=concepts[1]['概念名称'])
    st.write(df)




