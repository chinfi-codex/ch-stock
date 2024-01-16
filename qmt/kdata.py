import streamlit as st
import pandas as pd
import numpy as np
import json
import datetime

import akshare as ak
import mplfinance as mpf


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
def get_ak_price_df(code,end_date=None,count=60):
    if end_date is None:
        end_date = datetime.datetime.now().strftime('%Y%m%d')
    df = ak.stock_zh_a_hist(code,period="daily",end_date=end_date,adjust='').tail(count)
    df.columns = ['date','open','close','high','low','volume_','volume','range','pct','pct_','change']
    df = df[['date','open','close','high','low','volume']]
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date',inplace=True)
    return df


@st.cache_data(ttl='0.5d')
def get_ak_interval_price_df(code,end_date=None,count=241):
    if end_date is None:
        end_date = datetime.datetime.now().strftime('%Y%m%d')
    df = ak.stock_zh_a_hist_min_em(code,end_date=end_date,period='1').tail(count)
    df.columns = ['date','open','close','high','low','volume_','volume','lastprice']
    df = df[['date','open','close','high','low','volume']]
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date',inplace=True)
    return df


def plotK(df,k='d',plot_type='candle',ma_line=None,fail_zt=False,container=st):
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
        'type': plot_type,
        'style': s,
        'volume': True,
        'returnfig': True
    }
    if ma_line is not None: 
        plot_args['mav'] = (5, 10, 20, 144, 250)

    fig, axe = mpf.plot(df, **plot_args)
    container.pyplot(fig)


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

    def plotDayK(self, buy_date, container=st):
        next_tradeday = get_next_tradeday(buy_date)
        if next_tradeday is not None:
            df = get_ak_price_df(self.code,next_tradeday.replace('-',''))
        else:
            st.warning('周末')
            return
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
            
            if next_high_pct: container.write(f'隔日最高溢价:{next_high_pct}%')
            plotK(plot_df, ma_line=(5, 10, 20),fail_zt=fail_zt, container=container)
        except Exception as e:
            pass

    def plotIntervalK(self, buy_date, container=st):
        df = get_ak_interval_price_df(self.code)
        plotK(df,plot_type='line',container=container)


class StockTechnical:
    def __init__(self, code):
        self.code = code
        self.day_df = get_ak_price_df(self.code,count=120)

    @staticmethod
    def get_upStopPrice(code,date):
        pass

    def volume(self, buy_time=None):
        # 今日/昨日量能
        vols = self.day_df['volume'].tolist()
        interval_df = get_ak_interval_price_df(self.code)
        buy_time = buy_time[:-2] + '00'
        buy_row = interval_df[interval_df.index.astype(str).str.contains(buy_time)]
        return buy_row['volume'].tolist()[-1]/vols[-2]

    def day_range(self,days):
        # 区间涨幅
        df = self.day_df
        df = df.drop(df.index[-1])
        df['pct'] = df['close'].pct_change()*100
        return df['pct'].tail(days).sum()
        
    def is_period_newhigh(self,period_range,price_type='upstop'):
        # 是否区间新高：涨停/收盘/最高
        high_price = self.day_df['high'].tolist()[-1]
        df = self.day_df
        df = df.drop(df.index[-1]).tail(period_range)
        is_newhigh = high_price >= df['high'].max()
        return is_newhigh

    def zts_counts(self):
        # 是否昨日一字板
        # 连板数
        code = self.code
        if code[0] == '3' or code[:3] =='688':
            zt_high = 1.2
        elif code[0] == '8':
            zt_high = 1.3
        else:
            zt_high = 1.1
            
        close_ls = self.day_df.tail(10)['close'].tolist()
        zts_counts = 0
        for i in range(len(close_ls)-1,0,-1):
            if i>1 and close_ls[i-1] > (close_ls[i-2]*zt_high*0.99): 
                zts_counts+=1
            else:
                break
        return zts_counts

    def is_direct_zt(self):
        # 一字板
        row_today = self.day_df.to_dict('records')[-1]
        row_yesterday = self.day_df.to_dict('records')[-2]
        return (row_today['open'] == row_today['high']) or (row_yesterday['open'] == row_yesterday['close'] == row_yesterday['high'])

    def is_fail_zt(self):
        # 是否炸板
        row_today = self.day_df.to_dict('records')[-1]
        return row_today['close'] < row_today['high']


