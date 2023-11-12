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


@st.cache_data(ttl='0.5d')
def get_buy_datas(buy_date=None):
    if buy_date is None:
        sql = f"SELECT * FROM QT_DBLOG"
    else:
        sql = f"SELECT * FROM QT_DBLOG WHERE buy_date='{buy_date_select}'"
    df = mysql_retriever(sql)
    return df


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
        day_df = get_price(self.code,end_date=next_tradeday,count=1,frequency='1d')
        hour_df = ak.stock_zh_a_hist_min_em(self.code[2:],start_date=next_tradeday,end_date=next_tradeday,period='30')
        #hour_df = get_price(self.code,end_date=next_tradeday,count=8,frequency='30m')
        price_dict = {
            'code': self.code,
            'day': day_df.to_dict('records'),
            'hours': hour_df.to_dict('records') 
        }
        print (price_dict)
        return price_dict

    def plotDayK(self, buy_date, container):
        next_tradeday = get_next_tradeday(buy_date)
        df = get_price(self.code,end_date=next_tradeday,count=60,frequency='1d')
        try:  
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
            print(df)


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


data_tab, yz_tab = st.tabs(['数据分析','游资研究'])
with data_tab:
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
            buy_stocks_df = get_buy_datas(buy_date_select)
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
        df = get_buy_datas(buy_date_select)

        # 按每列格数计算需要多少行
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


    @st.cache_data(ttl='5m')
    def caculate_hour_pct(code,buy_date,hour_index):
        price_data = PriceData(code)
        if i == 0:
            hour_price = price_data.next_tradeday_price(buy_date)['day'][0]['open']
        else:
            hour_price = price_data.next_tradeday_price(buy_date)['hours'][i-1]['收盘']
        lastclose_price = price_data.buy_date_price(buy_date)[0]['close']
        return round((hour_price/lastclose_price - 1),5)*100

    if st.button('买入次日表现'):
        with st.spinner(''):
            df = get_buy_datas(buy_date_select)
            #df = get_buy_datas(None)

            # 计算分时涨幅
            hours = ['0925','1000','1030','1100','1130','1330','1400','1430','1500']
            for i in range(len(hours)):
                df[hours[i]] = [caculate_hour_pct(row['code'], row['buy_date'], i) for index, row in df.iterrows()]
            st.code(df)

            # 计算分时涨幅均值
            mean_list = [dict(hour=h,mean=df[h].mean()) for h in hours]
            mean_df = pd.DataFrame(mean_list)
            mean_df.set_index('hour',inplace=True)
            c1,c2 = st.columns([1,1])
            with c1:
                st.bar_chart(mean_df)


with yz_tab:
    chair = st.selectbox('游资',['上塘路','养家','呼家楼','小鳄鱼'])
    search_btn = st.button('go')
    if search_btn:
        df = pd.read_excel('datas/youzi.xlsx',sheet_name=chair)
        df = df[~df['上榜原因'].str.contains('三个')]
        df = df.drop_duplicates(subset=['名称', '买入'])
        df['date'] = pd.to_datetime(df['日期'], format='%Y/%m/%d')
        
        for i,row in df.iterrows():
            code = str(row.get('代码'))
            name = row.get('名称')
            date = row.get('date')
            buy_amount = row.get('买入')
            sell_amount = row.get('卖出')
            net_buy = row.get('净买入')

            c1,c2 = st.columns([2,1])
            if net_buy > 0:
                with c1: st.write(date,name,net_buy)
                with c2:
                    price_data = PriceData(code)
                    price_data.plotDayK(date,st)
                st.divider()



