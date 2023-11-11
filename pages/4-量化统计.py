import streamlit as st
import pandas as pd
import numpy as np
import json
import datetime
from io import StringIO
import re
import requests

import akshare as ak
import mplfinance as mpf
from tools.Ashare import *
from datas.storager import mysql_retriever,mysql_storager

st.set_page_config(
    page_icon="🚀",
    layout="wide"
)


def get_next_tradeday(date):
    if date == datetime.datetime.today():
        return date
    day_of_week = date.weekday()

    if day_of_week >= 0 and day_of_week <= 3:  # 周一到周四
        next_date = date + datetime.timedelta(days=1)
    elif day_of_week == 4:  # 周五
        next_date = date + datetime.timedelta(days=3)  # 加3天以跳过周末
    else:  # 周六或周日
        return None
    return next_date.strftime('%Y-%m-%d')


class PriceData:
    def __init__(self, code):
        if code.startswith('00') or code.startswith('3'):
            self.code = ('sz' + code)
        elif code.startswith('6'):
            self.code = ('sh' + code)
        else:
            self.code = code


    def buy_date_price(self, buy_date):
        df = get_price(self.code,end_date=buy_date,count=1,frequency='1d')
        return df.to_dict('records')


    def next_tradeday_price(self, buy_date):
        next_tradeday = get_next_tradeday(buy_date)
        hour_df = get_price(self.code,end_date=next_tradeday,count=8,frequency='30m')
        day_df = get_price(self.code,end_date=next_tradeday,count=1,frequency='1d')
        price_dict = {
            'day': day_df.to_dict('records'),
            'hours': hour_df.to_dict('records') 
        }
        return price_dict


    def plotDayK(self, buy_date, container):
        try:
            next_tradeday = get_next_tradeday(buy_date)
            df = get_price(self.code,end_date=next_tradeday,count=60,frequency='1d')
            
            # 隔日为买入当日，或隔日小于今天（周末情况）
            if next_tradeday == buy_date or datetime.datetime.strptime(next_tradeday,'%Y-%m-%d') > datetime.datetime.today():
                fail_zt = (df.iloc[-1]['close'] < df.iloc[-1]['high'])
                next_high_pct = None
                plot_df = df
            # 正常：隔日大于买入当日
            else:
                fail_zt = (df.iloc[-2]['close'] < df.iloc[-2]['high'])
                next_high_pct = round((df.iloc[-1]['high']/df.iloc[-2]['close'] -1),5)*100
                plot_df = df.iloc[:-1]
            
            if fail_zt:
                mc = mpf.make_marketcolors(up='black', down='darkgray', inherit=True)
                s = mpf.make_mpf_style(marketcolors=mc, gridaxis='horizontal', gridstyle='dashed')
            else:
                mc = mpf.make_marketcolors(up='r',down='g',inherit=True)
                s  = mpf.make_mpf_style(marketcolors=mc,gridaxis='horizontal',gridstyle='dashed')
            fig, axe = mpf.plot(plot_df, type='candle', style=s,
                     volume=True,
                     returnfig=True)
            if next_high_pct: container.write(f'隔日最高溢价:{next_high_pct}%')
            container.pyplot(fig)
        except Exception as e:
            print(e)


with st.expander('数据录入'):
    data_input = st.text_area('Input DATA:')
    if st.button('save'):
        df = pd.read_csv(StringIO(data_input), delimiter='\t', engine='python').drop_duplicates()
        df = df[['证券代码','时间']]
        df.columns = ['o_code','buy_time']
        df['code'] = df['o_code'].apply(lambda x:x.split('.')[0])
        df['name'] = df['o_code'].apply(lambda x:re.search(r'\(([^)]*)\)', x).group(1))
        df['buy_date'] = datetime.datetime.today().strftime('%Y-%m-%d')
        df.drop('o_code', axis=1, inplace=True)
        st.write(df)

        s = mysql_storager(df,'QT_DBLOG')
        st.write(s)


st.markdown('#### 数据分析')
buy_date_select = st.date_input('买入日期')
if st.button('当日买入数据'):
    def fail_zt(code):
        price = PriceData(code).buy_date_price(buy_date_select)
        if len(price) > 1: 
            i=1
        else:
            i = 0
        return (price[i]['close'] < price[i]['high'])

    with st.spinner(''):
        sql = f"SELECT * FROM QT_DBLOG WHERE buy_date='{buy_date_select}'"
        buy_stocks_df = mysql_retriever(sql)
        buy_stocks_df['fail_zt'] = buy_stocks_df['code'].apply(lambda x:fail_zt(x))
        fail_df = buy_stocks_df[buy_stocks_df['fail_zt'] == True]
        fail_pct = round(len(fail_df) / len(buy_stocks_df),2)*100
        
        date = str(buy_date_select).replace('-','')
        zt_all = ak.stock_zt_pool_em(date=date)
        zt_fail_pool = ak.stock_zt_pool_zbgc_em(date=date)

        all_names = zt_all['名称'].tolist()
        buy_names = buy_stocks_df['name'].tolist()
        miss_buy = list(set(all_names)-set(buy_names))
        miss_buy_df = zt_all[zt_all['名称'].isin(miss_buy)]
        miss_buy_df = miss_buy_df[['名称','成交额','封板资金','首次封板时间','最后封板时间']]

        buy_pct = round(len(buy_stocks_df) / (len(zt_all)+len(zt_fail_pool)),2)*100
        st.markdown(f'''
            #### 买入数量:{len(buy_stocks_df)}\n
            * 未封板率:{fail_pct}%\n
            * 占全天收盘涨停比例:{str(buy_pct)}%\n
            ''')
        st.code(buy_stocks_df)
        st.markdown('未买入涨停股:')
        st.write(miss_buy_df)


if st.button('当日买入图形'):
    sql = f"SELECT * FROM QT_DBLOG WHERE buy_date='{buy_date_select}'"
    df = mysql_retriever(sql)

    columns_per_row = 3
    rows = len(df) // columns_per_row + int(len(df) % columns_per_row > 0)

    for i in range(rows):
        # 创建一行列
        cols = st.columns(columns_per_row)
        st.divider()
        for j in range(columns_per_row):
            # 计算当前元素在data_list中的索引
            index = i * columns_per_row + j
            # 如果索引有效，则在当前列中显示数据
            if index < len(df):
                code = df.loc[index,'code']
                name = df.loc[index,'name']
                price_data = PriceData(code)
                cols[j].write(name)
                price_data.plotDayK(buy_date_select,cols[j])


if st.button('买入次日表现'):
    @st.cache_data(ttl='5m')
    def caculate_hour_pct(code,buy_date,hour_index):
        price_data = PriceData(code)
        if i == 0:
            hour_price = price_data.next_tradeday_price(buy_date_select)['day'][0]['open']
        else:
            hour_price = price_data.next_tradeday_price(buy_date_select)['hours'][i-1]['close']
        lastclose_price = price_data.buy_date_price(buy_date_select)[0]['close']
        return round((hour_price/lastclose_price - 1),5)*100


    next_tradeday = get_next_tradeday(buy_date_select)
    if next_tradeday is None:
        st.stop()
        st.warning('NOT TRADE DAY')

    with st.spinner(''):
        sql = f"SELECT * FROM QT_DBLOG WHERE buy_date='{buy_date_select}'"
        df = mysql_retriever(sql)

        # 计算分时涨幅
        hours = ['0925','1000','1030','1100','1130','1330','1400','1430','1500']
        codes = df['code'].tolist()
        for i in range(9):
            df[hours[i]] = [caculate_hour_pct(code,buy_date_select,i) for code in codes]
        st.code(df)

        # 计算分时涨幅均值
        mean_list = [dict(hour=h,mean=df[h].mean()) for h in hours]
        mean_df = pd.DataFrame(mean_list)
        mean_df.set_index('hour',inplace=True)
        c1,c2 = st.columns([1,1])
        with c1:
            st.bar_chart(mean_df)





