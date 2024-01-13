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
from datas.storager import mysql_retriever,mysql_storager
from qmt.kdata import *
from datas.cninfo import get_stock_list
from datas.useful import get_stock_topics


st.set_page_config(
    page_icon="🚀",
    layout="wide"
)


@st.cache_data(ttl='0.5d')
def get_buy_datas(buy_date=None):
    if buy_date is None:
        sql = f"SELECT * FROM QT_DBLOG"
    else:
        sql = f"SELECT * FROM QT_DBLOG WHERE buy_date='{buy_date_select}'"
    df = mysql_retriever(sql)
    return df


data_tab, yz_tab, bt_tab = st.tabs(['数据分析','游资研究','回测数据'])
with data_tab:
    buy_date_select = st.date_input('买入日期')
    if st.button('当日涨停数据'):
        def highlight_buy_true(s):
            return ['background-color: yellow' if s['涨停统计'] == '1/1' else '' for _ in s]

        with st.spinner(''):            
            date = str(buy_date_select).replace('-','')
            zt_all_df = ak.stock_zt_pool_em(date=date)
            zt_all_df = zt_all_df[['代码','名称','首次封板时间','炸板次数','涨停统计','连板数']]
            styled_df = zt_all_df.style.apply(highlight_buy_true, axis=1)
            st.markdown('当日全市场封涨停股:')
            st.dataframe(styled_df,hide_index=True)


    if st.button('当日涨停图形'):
        df = ak.stock_zt_pool_em(date=str(buy_date_select).replace('-',''))

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
                    code = df.loc[index,'代码']
                    name = df.loc[index,'名称']
                    price_data = PriceData(code)
                    cols[j].write(name)
                    price_data.plotDayK(buy_date_select,cols[j])

                    stock_data = StockTechnical(code)
                    vol_rate = stock_data.volume()
                    day5_range = stock_data.day_range(5)
                    day60_high = stock_data.is_period_newhigh(60)
                    zts_counts = stock_data.zts_counts()
                    cols[j].write(f'量能比,{vol_rate}')
                    cols[j].write(f'前5日涨幅,{day5_range}')
                    cols[j].write(f'是否60日新高,{day60_high}')
                    cols[j].write(f'前连板数,{zts_counts}')
                    is_buy = (0.3<vol_rate<2.5) and (day5_range < 30) and day60_high and (zts_counts <2)
                    cols[j].write(f'符合买入,{is_buy}')


    if st.button('涨停次日表现'):
        @st.cache_data(ttl='5m')
        def caculate_hour_pct(code,buy_date,hour_index):
            price_data = PriceData(code)
            if i == 0:
                hour_price = price_data.next_tradeday_price(buy_date)['day'][-1]['open']
            else:
                hour_price = price_data.next_tradeday_price(buy_date)['hours'][i-1]['收盘']
            last_buy_price = price_data.buy_date_price(buy_date)[-1]['high']
            return round((hour_price/last_buy_price - 1),5)*100

        def highlight_greater_than_zero(val):
            color = 'yellow' if val > 0 else ''
            return f'background-color: {color}'

        with st.spinner(''):
            df = ak.stock_zt_pool_em(date=str(buy_date_select).replace('-',''))
            #df['buy_time'] = df['buy_time'].astype(str)
            # 计算分时涨幅
            hours = ['0925','1000','1030','1100','1130','1330','1400','1430','1500']
            for i in range(len(hours)):
                df[hours[i]] = [caculate_hour_pct(row['代码'], buy_date_select, i) for index, row in df.iterrows()]
            hl_df = df.style.applymap(highlight_greater_than_zero, subset=hours)
            st.dataframe(hl_df,hide_index=True)

            # 计算分时涨幅均值
            mean_list = [dict(hour=h,mean=df[h].mean()) for h in hours]
            mean_df = pd.DataFrame(mean_list)
            mean_df.set_index('hour',inplace=True)
            c1,c2 = st.columns([1,1])
            with c1:
                st.bar_chart(mean_df)


with yz_tab:
    chair = st.selectbox('游资',['上塘路','养家','呼家楼','小鳄鱼','陈小群'])
    search_btn = st.button('go')
    if search_btn:
        df = pd.read_excel('datas/youzi.xlsx',sheet_name=chair)
        df = df[~df['上榜原因'].str.contains('三个')]
        df = df.drop_duplicates(subset=['名称', '买入'])
        df['date'] = pd.to_datetime(df['日期'], format='%Y/%m/%d')
        
        for i,row in df.iterrows():
            code = str(row.get('代码'))
            if len(code) < 6:
                code = (6-len(code))*'0'+code
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
 

with bt_tab:
    if st.button('backtrade'):
        
        # 起始日期和结束日期
        start_date = '2023-12-12'
        end_date_str = '20231218'
        end_date = pd.to_datetime(end_date_str, format='%Y%m%d')

        # 生成日期范围
        date_range = pd.date_range(start=start_date, end=end_date)

        low_opens = []
        for current_date in date_range:
            try:
                date_str = current_date.strftime('%Y%m%d')
                print (date_str)
                df = ak.stock_zt_pool_em(date=date_str)
                nd_str = get_next_tradeday(datetime.datetime.strptime(date_str,'%Y%m%d')).replace('-','')

                for i,row in df.iterrows():
                    price_df = get_ak_price_df(row.get('代码'),nd_str,2)
                    next_open_price = price_df['open'].tolist()[1]
                    day_close_price = price_df['close'].tolist()[0]
                    next_open_rate = next_open_price/day_close_price -1
                    #if next_open_rate <= 0:
                    next_df = get_ak_interval_price_df(row.get('代码'),get_next_tradeday(datetime.datetime.strptime(date_str,'%Y%m%d')))
                    print (next_df)
                    day_high_time = next_df['close'].idxmax()
                    day_high_rate = next_df['close'].max() / day_close_price -1
                    d = dict(
                        date = date_str,
                        code = row['代码'],
                        name = row['名称'],
                        zts = row['涨停统计'],
                        low_rate = next_open_rate,
                        day_high_time = day_high_time,
                        day_high_rate = day_high_rate
                        )
                    low_opens.append(d)
                        #print (low_opens)
            except Exception as e:
                print(e)
                pass
        output_df = pd.DataFrame(low_opens)
        output_df.to_excel('bt.xlsx')
        
        





