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
from tools.quantity import *
from datas.cninfo import get_stock_list


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
        self.code = code

    def buy_date_price(self, buy_date):
        df = get_ak_price_df(self.code,buy_date.strftime('%Y%m%d'),count=2)
        return df.to_dict('records')

    def next_tradeday_price(self, buy_date):
        next_tradeday = get_next_tradeday(buy_date).replace('-','')
        day_df = get_ak_price_df(self.code,next_tradeday,count=1)
        hour_df = ak.stock_zh_a_hist_min_em(self.code,start_date=next_tradeday,end_date=next_tradeday,period='30')
        price_dict = {
            'code': self.code,
            'day': day_df.to_dict('records'),
            'hours': hour_df.to_dict('records') 
        }
        return price_dict

    def plotDayK(self, buy_date, container):
        next_tradeday = get_next_tradeday(buy_date)
        df = get_ak_price_df(self.code,next_tradeday.replace('-',''))
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
            #plot_df.to_csv(f'{self.code}.csv')
        except Exception as e:
            print(e)
            print(df)
            pass


with st.expander('数据录入'):
    data_input = st.text_area('Input DATA:')
    if st.button('save'):
        df = pd.read_csv(StringIO(data_input), delimiter='\t', engine='python').drop_duplicates(subset='证券代码')
        df = df[['证券代码','时间']]
        df.columns = ['o_code','buy_time']
        df['code'] = df['o_code'].apply(lambda x:x.split('.')[0])
        df['name'] = df['o_code'].apply(lambda x:re.search(r'\(([^)]*)\)', x).group(1))
        df['buy_date'] = datetime.datetime.today().strftime('%Y-%m-%d')
        df.drop('o_code', axis=1, inplace=True)
        st.write(df)

        s = mysql_storager(df,'QT_DBLOG')
        st.write(s)


data_tab, yz_tab, backtrade_tab = st.tabs(['数据分析','游资研究','回测分析'])
with data_tab:
    buy_date_select = st.date_input('买入日期')
    if st.button('当日买入数据'):
        def success_zt(code):
            price = PriceData(code).buy_date_price(buy_date_select)
            return (price[-1]['close'] == price[-1]['high'])

        def highlight_buy_true(s):
                return ['background-color: yellow' if s['buy'] else '' for _ in s]

        with st.spinner(''):
            buy_stocks_df = get_buy_datas(buy_date_select)
            buy_stocks_df['buy_time'] = buy_stocks_df['buy_time'].astype(str)

            buy_stocks_df['success_zt'] = buy_stocks_df['code'].apply(lambda x:success_zt(x))
            success_df = buy_stocks_df[buy_stocks_df['success_zt'] == True]
            success_pct = round(len(success_df) / len(buy_stocks_df),2)*100
            
            date = str(buy_date_select).replace('-','')
            zt_all_df = ak.stock_zt_pool_em(date=date)
            zt_all_df = zt_all_df[['代码','名称','首次封板时间','炸板次数','涨停统计','连板数']]
            #zt_all_df['一字板'] = 
            zt_all_df['buy'] = zt_all_df['代码'].apply(lambda x:x in buy_stocks_df['code'].tolist())

            hl_zt_all_df = zt_all_df.style.apply(highlight_buy_true, axis=1)


            success_zt_buy_pct = round(len(success_df) / len(zt_all_df),2)*100
            st.markdown(f'''
                #### 买入数量:{len(buy_stocks_df)}\n
                * 封板成功率:{success_pct}%\n
                * 封涨停股买入比例:{success_zt_buy_pct}%\n
                ''')
            st.dataframe(buy_stocks_df,hide_index=True)
            st.markdown('当日全市场封涨停股:')
            st.dataframe(hl_zt_all_df,hide_index=True)


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


    if st.button('买入次日表现'):
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
            df = get_buy_datas(buy_date_select)
            df['buy_time'] = df['buy_time'].astype(str)
            # 计算分时涨幅
            hours = ['0925','1000','1030','1100','1130','1330','1400','1430','1500']
            for i in range(len(hours)):
                df[hours[i]] = [caculate_hour_pct(row['code'], row['buy_date'], i) for index, row in df.iterrows()]
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
    chair = st.selectbox('游资',['上塘路','养家','呼家楼','小鳄鱼'])
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


with backtrade_tab:
    @st.cache_data(ttl='1d')
    def get_ths_concepts():
        df = ak.stock_board_concept_name_ths()
        df = df[['概念名称','代码']]
        return df.to_dict('records')

    def get_zt_stocks(date):
        zt_pool = ak.stock_zt_pool_em(date=date)
        zt_pool = zt_pool[['代码','名称','成交额','总市值','换手率','首次封板时间']]
        zt_fail_pool = ak.stock_zt_pool_zbgc_em(date=date)
        zt_fail_pool = zt_fail_pool[['代码','名称','成交额','总市值','换手率','首次封板时间']]
        zt_all_pool = pd.concat([zt_pool,zt_fail_pool])
        
        next_tradeday = get_next_tradeday(datetime.datetime.strptime(date,'%Y%m%d'))
        print (next_tradeday)
        if next_tradeday is not None:
            next_tradeday = next_tradeday.replace('-','')
        else:
            return None
        zt_all_pool['date'] = date
        zt_all_pool.reset_index(inplace=True)
        for index, row in zt_all_pool.iterrows():
            dict_data = get_ak_price_df(row['代码'],next_tradeday,count=3).to_dict('records')
            for key, value in dict_data[0].items():
                zt_all_pool.at[index, 't-1 '+key] = value
            for key, value in dict_data[1].items():
                zt_all_pool.at[index, 't+0 '+key] = value
            for key, value in dict_data[2].items():
                zt_all_pool.at[index, 't+1 '+key] = value

        return zt_all_pool

    if st.button('每日涨停板统计'):
        start_date = '20231101'
        zt_pool = ak.stock_zt_pool_em(date=start_date)

        end_date = datetime.datetime.today().strftime('%Y%m%d')
        date_range = pd.date_range(start=start_date, end=end_date, freq='D').strftime('%Y%m%d')

        dfs = []
        for date in date_range:
            try:
                result = get_zt_stocks(date)
                if result is not None:
                    dfs.append(result)
            except Exception as e:
                break

            

        all_pool = pd.concat(dfs)
        st.write(all_pool)
        all_pool.to_csv('backtrade_all_pool.csv')

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








