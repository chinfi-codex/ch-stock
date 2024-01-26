import streamlit as st
import pandas as pd
import json
import datetime
import time

import akshare as ak
from datas.storager import mysql_retriever,mysql_storager
from datas import spider
from tools.llm import get_chatgpt_chat, get_baichuan_chat
from tools.SparkApi import get_spark_chat
from tools.tools import notify_pushplus
from datas.cninfo import get_stock_list
from qmt.kdata import *
from qmt.boardData import *


st.set_page_config(
    page_title="复盘机",
    page_icon="🚀",
    layout="wide"
)


today = datetime.datetime.now()
review_dict = {}
review_df = None
select_date = st.date_input("选择日期",today)


load_btn = st.button('Load')
with st.expander("设置项"):
    reload_btn = st.button('Reload')
    notify_btn = st.button('Push')
    emotion_index = st.text_input('情绪指数')
    jiuyan_picurl = st.text_input('涨停简图')

def write_review_from_data(review_dict):
    def write_review(trade_sum_str,zt_str,jiuyan_picurl,hk_volumn_df,cashflow_in_df,cashflow_out_df,lh_jg_in_df,lh_jg_out_df,lh_yz_df):
        def write_df(data):
            df = pd.DataFrame(json.loads(data))
            if not df.empty:
                st.dataframe(df,hide_index=True)
            else:
                st.markdown('无符合条件数据')
        
        st.markdown("#### 今日成交概况")
        st.markdown(trade_sum_str)

        st.markdown("#### 涨停、连板指数")
        st.markdown(zt_str)
        st.image(jiuyan_picurl,width=600)

        st.markdown("#### 港股成交排名")
        write_df(hk_volumn_df)

        st.markdown("#### 行业资金流向")
        write_df(cashflow_in_df)
        write_df(cashflow_out_df)

        st.markdown("#### 龙虎榜-机构")
        st.markdown('机构买入')
        write_df(lh_jg_in_df)
        st.markdown('机构卖出')
        write_df(lh_jg_out_df) 

        st.markdown("#### 龙虎榜-游资")
        yz_df = pd.DataFrame(json.loads(lh_yz_df))
        st.table(yz_df)

    review_args = [v for k, v in review_dict.items() if k != "date"]
    write_review(*review_args)


def collect_review_data(select_date):
    def format_cash_str(df,column):
        df[column] = df[column].apply(lambda x: f"{x/100000000:.2f}亿元" if x>100000000 else f"{x/10000:.1f}万元")
    def format_pct(num):
        percentage = round(num, 2)
        return f"{percentage}%"

    date = str(select_date).replace('-','')
    review_dict = {'date': select_date}


    #今日成交
    sql = f"""
        SELECT * FROM NEWS_CLS
        WHERE (tags LIKE "%A股盘面直播%" OR tags LIKE "%港股动态%") AND date = '{str(select_date)}'
        ORDER BY date DESC, time DESC
        """
    news_df = mysql_retriever(sql)
    close_comments = news_df[news_df['content'].str.contains("收评")].to_json()
    trade_sum_str = get_baichuan_chat("""
        任务:提取数据
        提取要求：提取：两市成交总量、北向资金全天交易金额
        输出格式要求: markdown
        * 今日成交总量:
        * 北向交易金额:xx（净买入/净流出/净卖出
        \n
        """ +
        close_comments)
    hk_comments = news_df[news_df['content'].str.contains("南向资金今日")].to_json()
    hk_sum_str = get_baichuan_chat("""
        提取全天南向资金净买入或净卖出金额.
        输出格式要求: markdown
        * 南向交易金额: xx（净买入/净流出/净卖出）
        \n
        """+
        hk_comments)
    trade_sum_str += f"\n{hk_sum_str}"
    review_dict['trade_sum_str'] = trade_sum_str


    #今日涨停
    zt_pool = ak.stock_zt_pool_em(date=date)
    zt_300_pool = zt_pool[zt_pool['代码'].str.startswith('30')]
    zt_688_pool = zt_pool[zt_pool['代码'].str.startswith('688')]
    
    zt_fail_pool = ak.stock_zt_pool_zbgc_em(date=date)
    zt_total_count = len(zt_pool)+len(zt_fail_pool)
    zt_fail_count = len(zt_fail_pool)
    zt_str = f'##### 今日情绪指数:{emotion_index}\n'
    zt_str += f'* 今日涨停数:{zt_total_count}\n'
    zt_str += f'  * 创业板涨停数:{len(zt_300_pool)}\n'
    zt_str += f'  * 科创板涨停数:{len(zt_688_pool)}\n'
    zt_str += f'  * 炸板数:{zt_fail_count}，炸板率:{format_pct(zt_fail_count/zt_total_count*100)}\n'

    # dt_pool = ak.stock_zt_pool_dtgc_em(date=date)
    # zt_str += f"* 今日跌停数:{len(dt_pool)}\n"
    sb_pre = ak.stock_board_cons_ths(symbol="883979")
    sb_pre = sb_pre[sb_pre['涨跌幅'] != '--']
    lb_pre = ak.stock_board_cons_ths(symbol="883958")
    lb_pre = lb_pre[lb_pre['涨跌幅'] != '--']
    zt_str += f"* 昨日首版表现:{format_pct(sb_pre['涨跌幅'].astype(float).mean())}\n"
    zt_str += f"* 昨日连板表现:{format_pct(lb_pre['涨跌幅'].astype(float).mean())}\n"
    review_dict['zt_str'] = zt_str
    review_dict['jiuyan_picurl'] = jiuyan_picurl


    #港股成交前十中涨幅最大的三个
    hk_volumn_df = ak.stock_hk_spot_em()
    hk_volumn_df = hk_volumn_df.sort_values('成交额',ascending=False).head(10).sort_values('涨跌幅',ascending=False).head(3)
    hk_volumn_df = hk_volumn_df[['名称','成交额','涨跌幅']]
    format_cash_str(hk_volumn_df,'成交额')
    review_dict['hk_volumn_df'] = hk_volumn_df.to_json()
    

    # 行业资金走向
    cashflow_df = ak.stock_fund_flow_industry(symbol="即时").sort_values("净额",ascending=False)
    
    @st.cache_data(ttl="0.5day")
    def sort_board_stocks(board):
        boards_df = ak.stock_board_industry_cons_ths(symbol=board)
        stocks_flow_df = ak.stock_fund_flow_individual(symbol="即时")
        result_df = stocks_flow_df[stocks_flow_df['股票简称'].isin(boards_df['名称'])]
        result_df['净额'] = result_df['净额'].apply(lambda x: float(x.replace('万', ''))/10000 if '万' in x else float(x.replace('亿', '')))
        result_df = result_df.sort_values("净额",ascending=False)
        return result_df

    def get_boards_flow(sort="head"):
        cf_dict = {
            "行业": [],
            "净额": [],
            "个股": []
        }
        if sort == "head": df = cashflow_df.head(3)
        if sort == "tail": df = cashflow_df.tail(3).iloc[::-1]
        for i,row in df.iterrows():
            cf_dict['行业'].append(row['行业'])
            cf_dict['净额'].append(row['净额'])

            bs_df = sort_board_stocks(row['行业'])
            if sort == "head": bs_df = bs_df.head(3)
            if sort == "tail": bs_df = bs_df.tail(3).iloc[::-1]
            bs_str = [f"{row['股票简称']}:{row['净额']}\n" for i,row in bs_df.iterrows()]
            cf_dict['个股'].append(bs_str)
        cf_df = pd.DataFrame(cf_dict)
        return cf_df.to_json()
    review_dict['cashflow_in_df'] = get_boards_flow()
    review_dict['cashflow_out_df'] = get_boards_flow("tail")


    #龙虎-机构
    jg_lh = ak.stock_lhb_jgmmtj_em(start_date=date, end_date=date)

    lh_jg_in_df = jg_lh[(jg_lh['买方机构数'] > 0) & (jg_lh['机构买入总额'] > 100000000 )]
    lh_jg_in_df = lh_jg_in_df[['名称','涨跌幅','买方机构数','机构买入总额']].sort_values('机构买入总额',ascending=False)
    format_cash_str(lh_jg_in_df,'机构买入总额')

    lh_jg_out_df = jg_lh[(jg_lh['卖方机构数'] > 0) & (jg_lh['机构卖出总额'] > 100000000 )]
    lh_jg_out_df = lh_jg_out_df[['名称','涨跌幅','卖方机构数','机构卖出总额']].sort_values('机构卖出总额',ascending=False)
    format_cash_str(lh_jg_out_df,'机构卖出总额')
    review_dict['lh_jg_in_df'] = lh_jg_in_df.to_json()
    review_dict['lh_jg_out_df'] = lh_jg_out_df.to_json()


    #龙虎-游资
    chairs_set = {
        "毛老板": ['上海东方路','深圳金田路','成都通盈街','北京光华路'],
        "章盟主": ['中信证券杭州延安路','上海江苏路','宁波彩虹北路','上海建国西路','杭州四季路'],
        "赵老哥": ["浙商证券绍兴分","北京阜成路","上海嘉善路"],
        "方新侠": ["朱雀大街","兴业证券陕西分公司"],
        "小鳄鱼": ["南京大钟亭","中投证券南京太平南路","长江证券股份有限公司上海世纪大道","上海兰花路","源深路"],
        "作手新一": ["国泰君安证券股份有限公司南京太平南"],
        "炒股养家": ["上海宛平南路","上海茅台路","西安西大街","海口海德路","上海红宝石路"],
        "陈小群": ["金马路","黄河路"],
        "思明南路": ["东亚前海证券有限责任公司上海","东莞证券股份有限公司湖北分公司"],
        "湖里大道": ["湖里大道"],
        "呼家楼": ["呼家楼"],
        "小棉袄": ["上海证券有限责任公司上海分"],
        "小余余": ["申港证券浙江分公司","甬兴证券青岛同安路"],
        "西湖国贸": ["西湖国贸"],
        "桑田路": ["桑田路"],
        "上塘路": ["上塘路"],
        "章盟主": ["杭州延安路","上海江苏路","宁波彩虹北路","上海建国西路","杭州四季路"],
        "金开大道": ["金开大道"],
        "金田路": ["金田路"],
        "小棉袄": ["上海证券上海分"],
        "珍珠路": ["珍珠路"],
        "上海超短帮": ['上海新闸路','上海银城中路','泰闸路','浦东新区银城中路','东川路'],
        "徐晓": ['上海虹桥路'],
        "劳动路": ['中信证券股份有限公司北京总部'],
    }
    all_business_names = [name for names in chairs_set.values() for name in names]

    lh_yz_df = ak.stock_lhb_hyyyb_em(start_date=date, end_date=date)
    lh_yz_df = lh_yz_df[lh_yz_df['营业部名称'].str.contains('|'.join(all_business_names))]
    if not lh_yz_df.empty:
        def get_chair_name(business_name):
            for chair, names in chairs_set.items():
                if any(b in business_name for b in names):
                    return chair
            return None
        lh_yz_df['游资'] = lh_yz_df['营业部名称'].apply(get_chair_name)
        
        s = lh_yz_df['买入股票'].str.split(' ').apply(pd.Series, 1).stack().reset_index(level=1, drop=True)
        s.name = '买入股票_new'
        lh_yz_df = lh_yz_df.drop('买入股票', axis=1).join(s)
        lh_yz_df = lh_yz_df.rename(columns={'买入股票_new': '买入股票'}).reset_index()
    
        #stocks_df = ak.stock_zh_a_spot_em()
        stocks_df = get_stock_list()
        for i,row in lh_yz_df.iterrows():
            try:
                symbol = stocks_df.loc[stocks_df['zwjc'] == row['买入股票'], 'code'].iloc[0]
                detail_buy_df = ak.stock_lhb_stock_detail_em(symbol=symbol, date=date,flag="买入")
                detail_buy_df = detail_buy_df[(detail_buy_df['交易营业部名称'] == row['营业部名称']) & ~(detail_buy_df['类型'].str.contains('三个交易日'))]

                detail_sell_df = ak.stock_lhb_stock_detail_em(symbol=symbol, date=date,flag="卖出")
                detail_sell_df = detail_sell_df[(detail_sell_df['交易营业部名称'] == row['营业部名称']) & ~(detail_sell_df['类型'].str.contains('三个交易日'))]
                detail_df_yz = pd.concat([detail_buy_df,detail_sell_df],axis=0)
                detail_df_yz.fillna(0,inplace=True)

                if len(detail_df_yz)>1 and detail_df_yz['类型'].nunique()==1:
                    index = 1
                else:
                    index = 0
                lh_yz_df.loc[i, '买入金额'] = detail_df_yz['买入金额'].iloc[0]
                lh_yz_df.loc[i, '卖出金额'] = detail_df_yz['卖出金额'].iloc[index]
            except Exception as e:
                pass

        lh_yz_df['净买入'] = lh_yz_df['买入金额'] - lh_yz_df['卖出金额']
        lh_yz_df = lh_yz_df[['游资','买入股票','净买入','买入金额','卖出金额']]
        lh_yz_df = lh_yz_df[(lh_yz_df['买入金额']>10000000) | (lh_yz_df['卖出金额']>10000000)].sort_values('净买入',ascending=False)
        format_cash_str(lh_yz_df,'买入金额')
        format_cash_str(lh_yz_df,'卖出金额')
        format_cash_str(lh_yz_df,'净买入')
    review_dict['lh_yz_df'] = lh_yz_df.to_json()

    return review_dict


if select_date:
    sql = f"SELECT * FROM REVIEW_DAILY_V WHERE date = '{str(select_date)}'"
    review_df = mysql_retriever(sql)
    if not review_df.empty:
        review_dict = review_df.tail(1).to_dict('records')[0]

def load_data(review_dict,load_style=1):
    if select_date.weekday() >= 5: 
        st.warning("非交易日")
        st.stop()

    try:
        if review_df.empty:
            if load_style == 1:
                spider.NewsSpider().run()
                review_dict = collect_review_data(select_date)
                write_review_from_data(review_dict)
                mysql_storager(pd.DataFrame(review_dict,index=[0]),'REVIEW_DAILY_V',if_exists='append')
            else:
                return
        else:
            write_review_from_data(review_dict)
    except Exception as e:
        raise e
load_data(review_dict,2)

if load_btn:
    load_data(review_dict)

if reload_btn and select_date == today.date():
    spider.NewsSpider().run()
    review_dict = collect_review_data(select_date)
    write_review_from_data(review_dict)
    mysql_storager(pd.DataFrame(review_dict,index=[0]),'REVIEW_DAILY_V',if_exists='append')

if notify_btn:
    date = datetime.date.today().strftime("%Y-%m-%d")
    sql = f"SELECT * FROM REVIEW_DAILY_V WHERE date = '{date}'"
    review_df = mysql_retriever(sql)
    if not review_df.empty:
        title = f"复盘机更新啦:{date}"
        content = f"<a href='http://app.vervefunds.com:8501/复盘机'>查看</a>"
        notify_resp = notify_pushplus(title, content, "zhangting_vdata")
        print (notify_resp)

