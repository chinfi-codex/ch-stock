import streamlit as st
import pandas as pd
import json
import datetime
import requests
from io import StringIO

import akshare as ak
import mplfinance as mpf
from datas.storager import mysql_retriever,mysql_storager
from qmt.kdata import *
from qmt.boardData import *


st.set_page_config(
    page_icon="🚀",
    layout="wide"
)


concepts, industries = get_all_boards()
sql = f"SELECT * FROM QT_SETTINGS ORDER BY last_updated DESC LIMIT 1"
loaded_settings = mysql_retriever(sql).to_dict('records')[0]
buy_params = eval(loaded_settings['buy_params'])
#st.write(loaded_settings)

with st.expander('',expanded=True):
    c1,c2,c3 = st.columns([1,1,2])
    with c1:
        st.markdown('##### 仓位参数')
        buy_pct = st.number_input('买入仓位上限',min_value=0,max_value=100,
            value=int(buy_params['buy_pct'])
            )
        buy_single_position_pct = st.number_input('单股仓位比例',min_value=0,max_value=100,
            value=int(buy_params['buy_single_position_pct'])
            )
        
        
    with c2:
        st.markdown('##### 买入参数')
        buy_zts_limit = st.number_input('买入最高连板数',min_value=0,max_value=100,
            value=int(buy_params['buy_zts_limit'])
            )
        st.caption('前连续涨停大于，则不买入')
        buy_time_end = st.time_input('买入停止时间', buy_params['buy_time_end'])
        buy_volume_upper = st.number_input('今/昨量能比值上限',min_value=0.0,max_value=10.0,
            #value=buy_params['buy_volume_upper']
            value=2.5
            )
        buy_volume_lower = st.number_input('今/昨量能比值下限',min_value=0.0,max_value=10.0,
            #value=buy_params['buy_volume_lower']
            value=0.3
            )
        buy_5day_range = st.number_input('5日涨幅上限%',min_value=0,max_value=100,
            #value=buy_params['buy_5day_range']
            value=30
            )
        buy_60high_on = st.toggle('只买60日新高',
            #value=buy_params['buy_60high_on']
            value=1
            )


    with c3:
        st.markdown('##### 买入板块')
        selected_concept_boards = st.multiselect(
            '概念板块',
            options = [c['name'] for c in concepts],
            default = loaded_settings['buy_concepts'].split(',')
            )
        selected_industry_boards = st.multiselect(
            '行业板块',
            options = [i['name'] for i in industries],
            default = loaded_settings['buy_industries'].split(','))

        if st.button('板块今日买入'):
            concept_codes = []
            for concept in selected_concept_boards:
                for i in concepts:
                    if i['name'] == concept:
                        code = i['code']
                        break    
                codes = ak.stock_board_cons_ths(symbol=code)['代码'].tolist()
                concept_codes += codes

            industry_codes = []
            for board in selected_industry_boards:
                codes = ak.stock_board_industry_cons_ths(symbol=board)['代码'].tolist()
                industry_codes += codes
            buy_codes = concept_codes + industry_codes
            
            st.write('板块股票数:',len(buy_codes))


    if st.button('保存配置'):
        buy_params_dict = {
            'buy_pct': buy_pct,
            'buy_single_position_pct': buy_single_position_pct,
            'buy_zts_limit': buy_zts_limit,
            'buy_time_end': buy_time_end,
            'buy_volume_upper': buy_volume_upper,
            'buy_volume_lower': buy_volume_lower,
            'buy_5day_range': buy_5day_range,
            'buy_60high_on': buy_60high_on,
        }
        settings = {
            'buy_params': str(buy_params_dict),
            'buy_concepts': ','.join(selected_concept_boards),
            'buy_industries': ','.join(selected_industry_boards),
            'last_updated': datetime.datetime.now()
        }
        resp = mysql_storager(pd.DataFrame(settings,index=[0]),'QT_SETTINGS')
        st.toast(resp)






with st.expander('数据录入'):
    data_input = st.text_area('Input DATA:')
    if data_input:
        df = pd.read_csv(StringIO(data_input), delimiter='\t', engine='python').drop_duplicates(subset='证券名称')
        df = df[['证券代码','证券名称','操作','成交价格','成交日期','成交时间']]
        buy_df = df[df['操作'] == '买入'].sort_values('成交时间')
        sell_df = df[df['操作'] == '卖出'].sort_values('成交时间')

    if st.button('买入核对'):
        def highlight_to_buy(s):
            return ['background-color: yellow' if s['符合买入'] else '' for _ in s]

        def merge_tech_columns(orginal_df, fix_column):
            new_data_df = pd.DataFrame()
            codes = orginal_df[fix_column]
            for code in codes:
                stock_data = StockTechnical(code)
                vol_rate = stock_data.volume()
                day5_range = stock_data.day_range(5)
                day60_high = stock_data.is_period_newhigh(60)
                zts_counts = stock_data.zts_counts()
                is_direct_zt = stock_data.is_direct_zt()
                to_buy = (0.3<vol_rate<2.5) and (day5_range < 30) and day60_high and (zts_counts <2) and not is_direct_zt
                
                new_data_df = new_data_df._append({
                    fix_column: code,
                    '量能比': vol_rate,
                    '前5日涨幅': day5_range,
                    '是否60日新高': day60_high,
                    '前连板数': zts_counts,
                    '是否一字板': is_direct_zt,
                    '符合买入': to_buy,
                }, ignore_index=True)
                result_df = pd.merge(orginal_df, new_data_df, on=fix_column)
            return result_df

        zt_all_df = ak.stock_zt_pool_em(date=datetime.datetime.now().strftime('%Y%m%d'))
        zt_all_df = zt_all_df[['代码','名称','涨跌幅','首次封板时间','涨停统计']]
        zt_all_df = merge_tech_columns(zt_all_df,'代码')
        
        if 'buy_df' in locals():
            zt_all_df['是否买入'] = zt_all_df[zt_all_df['代码'] in buy_df['代码'].tolist()]
            buy_df = merge_tech_columns(buy_df,'证券代码')

        zt_all_df = zt_all_df.style.apply(highlight_to_buy, axis=1)
        st.dataframe(zt_all_df,hide_index=True)


    if st.button('买入分析'):
        def plot_df_data(df):
            df['date'] = pd.to_datetime(df['成交日期'], format='%Y%m%d')
            df['date'] = df['date'] + pd.to_timedelta('23:59:59')

            for i,row in df.iterrows():
                code = str(row.get('证券代码'))
                name = row['证券名称']
                time = row['成交时间']
                price = row['成交价格']
                date = row['date']

                if len(code) < 6:
                    code = (6-len(code))*'0'+code
                price_data = PriceData(code)
                c1,c2,c3 = st.columns([1,1,1])
                with c1: 
                    st.markdown(f' ##### {name}')
                    st.write('当日成交:')
                    ddf = price_data.buy_date_price(date)
                    trade_pct = round(price/ddf[0]['close'] - 1,2)
                    st.write(time,price,trade_pct)
                    st.write('当日最高:')
                    idf = get_ak_interval_price_df(code,date)
                    st.write(str(idf['close'].idxmax()),idf['close'].max(),round(idf['close'].max()/ddf[0]['close']-1,2))

                with c2:
                    price_data.plotDayK(row['date'])
                with c3:
                    price_data.plotIntervalK(row['date'])
                st.divider()

        st.markdown('### 买入数据')
        plot_df_data(buy_df)
        
        st.markdown('### 卖出数据')
        plot_df_data(sell_df)