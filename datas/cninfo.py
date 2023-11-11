#!/usr/bin/env python
# coding: utf-8

import streamlit as st
import requests
from datetime import date
import datetime
import pandas as pd
import json

yesterday = datetime.date.today() - datetime.timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")
yesterday_sedate = yesterday_str + '~' +yesterday_str

@st.cache_data(ttl="15day")
def get_stock_list()->pd.DataFrame:
    url = "http://www.cninfo.com.cn/new/data/szse_stock.json"
    resp = requests.get(url).json()['stockList']
    df = pd.DataFrame(resp)
    return df


@st.cache_data(ttl="1day")
def cninfo_announcement_spider(pageNum, tabType, stock='', searchkey='', category='', seDate=yesterday_sedate):
    '''
        tab类型：fulltext 公告；relation  调研
        searchkey: 标题关键字
        trade: 行业
        category: 
        - 业绩预告 category_yjygjxz_szsh 
        - 年报 category_ndbg_szsh
        - 半年报 category_bndbg_szsh
        - 一季报 category_yjdbg_szsh
        - 三季报 category_sjdbg_szsh
        - 日常经营 category_rcjy_szsh 合同，合作，协议，进展
        - 首发 category_sf_szsh 招股书
        - 股权激励 category_gqjl_szsh
    '''
    url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
    pageNum = int(pageNum)
    data = {'pageNum': pageNum,
            'pageSize': 30,
            'column': 'szse',
            'tabName': tabType,
            'plate': '',
            'stock': stock,
            'searchkey': searchkey,
            'secid': '',
            'category': category,
            'trade': '',
            'seDate': seDate,
            'sortName': '',
            'sortType': '',
            'isHLtitle': 'true'}

    headers = {'Accept': '*/*',
               'Accept-Encoding': 'gzip, deflate',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Connection': 'keep-alive',
               'Content-Length': '181',
               'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
               'Host': 'www.cninfo.com.cn',
               'Origin': 'http://www.cninfo.com.cn',
               'Referer': 'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
               'X-Requested-With': 'XMLHttpRequest'}

    results = requests.post(url, data=data, headers=headers).json()
    if results.get('announcements'):
        df = pd.DataFrame(results['announcements'])
        df = df[['announcementTime','secName','secCode','announcementTitle','adjunctUrl']]
        df['announcementTime'] = df['announcementTime'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
        df['adjunctUrl'] = df['adjunctUrl'].apply(lambda x: 'http://static.cninfo.com.cn/' + x)
        return df
    else:
        return None


@st.cache_data(ttl="1day")
def hudong_spider(page,stock=None,keywords=''):
    headers = {'Accept': '*/*',
       'Accept-Encoding': 'gzip, deflate',
       'Accept-Language': 'zh-CN,zh;q=0.9',
       'Connection': 'keep-alive',
       'Content-Length': '181',
       'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
       'Host': 'www.cninfo.com.cn',
       'Origin': 'http://www.cninfo.com.cn',
       'Referer': 'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search',
       'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
       'X-Requested-With': 'XMLHttpRequest'
    }

    if stock is None:
        data = {'isPagination': 1,
                'keyWords': keywords,
                'companyCode': '',
                'companyBaseinfoId': '',
                'page': int(page),
                'rows': 10,
        }
    else:
        valid_url = "https://ir.p5w.net/company/validCompanyJson.shtml"
        res = requests.post(valid_url,data={'keyword':stock},headers=headers).json()
        obj = res.get('obj')[0]
        code = obj['companyCode']
        pid = obj['pid']
        data = {'isPagination': 1,
                'keyWords': keywords,
                'companyCode': code,
                'companyBaseinfoId': pid,
                'page': int(page),
                'rows': 10,
        }

    qs_url = 'https://ir.p5w.net/interaction/getNewSearchR.shtml'
    results = requests.post(qs_url, data=data, headers=headers).json()
    if results.get('rows'):
        df = pd.DataFrame(results['rows'])
        df = df[['companyShortname','questionerTimeStr','content','replyContent','replyerTimeStr']]
        return df
    else:
        return None

