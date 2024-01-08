import streamlit as st
import pandas as pd
import json
import datetime
import requests

import akshare as ak
import mplfinance as mpf
from datas.storager import mysql_retriever,mysql_storager
from qmt.kdata import *
from datas.cninfo import get_stock_list


st.set_page_config(
    page_icon="🚀",
    layout="wide"
)

@st.cache_data(ttl='1d')
def init_boards():
    concept_df = ak.stock_board_concept_name_ths()
    concept_df = concept_df[['概念名称','代码']]
    concept_df.columns = ['name', 'code']

    industry_df = ak.stock_board_industry_summary_ths()
    industry_df = industry_df[['板块']]
    industry_df.columns = ['name']
    return concept_df.to_dict('records'), industry_df.to_dict('records')


concepts, industries = init_boards()
sql = f"SELECT * FROM QT_SETTINGS ORDER BY last_updated DESC LIMIT 1"
loaded_settings = mysql_retriever(sql).to_dict('records')[0]
#st.write(loaded_settings)

with st.expander('',expanded=True):
    c1,c2 = st.columns([1,1])
    with c1:
        st.markdown('##### 买入参数')
        buy_pct = st.number_input('买入仓位上限',min_value=0,max_value=100,
            value=int(eval(loaded_settings['buy_params'])['buy_pct'])
            )
        buy_single_position_pct = st.number_input('单股仓位比例',min_value=0,max_value=100,
            value=int(eval(loaded_settings['buy_params'])['buy_single_position_pct'])
            )
        buy_zts_limit = st.number_input('买入最高连板数',min_value=0,max_value=100,
            value=int(eval(loaded_settings['buy_params'])['buy_zts_limit'])
            )
        st.caption('前连续涨停大于，则不买入')
        buy_time_end = st.time_input('买入停止时间', eval(loaded_settings['buy_params'])['buy_time_end'])
        #buy_time_range
        # A.buy_pct = 1
        # A.hs_zts_limit = 1
        # A.bj_zts_limit = 0
        

    with c2:
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

        # if st.button('板块股票'):
        #     concept_codes = []
        #     for concept in selected_concept_boards:
        #         for i in concepts:
        #             if i['name'] == concept:
        #                 code = i['code']
        #                 break    
        #         codes = ak.stock_board_cons_ths(symbol=code)['代码'].tolist()
        #         concept_codes += codes

        #     industry_codes = []
        #     for board in selected_industry_boards:
        #         codes = ak.stock_board_industry_cons_ths(symbol=board)['代码'].tolist()
        #         industry_codes += codes
        #     buy_codes = concept_codes + industry_codes
            
        #     st.write('板块股票数:',len(buy_codes))


    if st.button('保存配置'):
        buy_params_dict = {
            'buy_pct': buy_pct,
            'buy_single_position_pct': buy_single_position_pct,
            'buy_zts_limit': buy_zts_limit,
            'buy_time_end': buy_time_end
        }
        settings = {
            'buy_params': str(buy_params_dict),
            'buy_concepts': ','.join(selected_concept_boards),
            'buy_industries': ','.join(selected_industry_boards),
            'last_updated': datetime.datetime.now()
        }
        mysql_storager(pd.DataFrame(settings,index=[0]),'QT_SETTINGS')



