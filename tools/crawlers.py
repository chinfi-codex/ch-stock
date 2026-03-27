"""
爬虫模块
包含新闻爬虫、公告爬虫、电报爬虫等功能
"""

import time
import datetime
import re
import pandas as pd
import json
import requests
import hashlib
from bs4 import BeautifulSoup
import logging
import os
from urllib.parse import urljoin
from infra.web_scraper import scrape_with_jina_reader
from infra.storage import clean_filename

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)


def cls_telegraphs():
    """
    财联社-电报 https://www.cls.cn/telegraph
    返回dataframe 对象
    """
    current_time = int(time.time())
    url = "https://www.cls.cn/nodeapi/telegraphList"
    params = {
        "app": "CailianpressWeb",
        "category": "",
        "lastTime": current_time,
        "last_time": current_time,
        "os": "web",
        "refresh_type": "1",
        "rn": "2000",
        "sv": "7.7.5",
    }
    text = requests.get(url, params=params).url.split("?")[1]
    if not isinstance(text, bytes):
        text = bytes(text, "utf-8")
    sha1 = hashlib.sha1(text).hexdigest()
    code = hashlib.md5(sha1.encode()).hexdigest()

    params = {
        "app": "CailianpressWeb",
        "category": "",
        "lastTime": current_time,
        "last_time": current_time,
        "os": "web",
        "refresh_type": "1",
        "rn": "2000",
        "sv": "7.7.5",
        "sign": code,
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Type": "application/json;charset=utf-8",
        "Host": "www.cls.cn",
        "Pragma": "no-cache",
        "Referer": "https://www.cls.cn/telegraph",
        "sec-ch-ua": '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
    }
    data = requests.get(url, headers=headers, params=params).json()
    df = pd.DataFrame(data["data"]["roll_data"])

    df = df[["title", "content", "level", "subjects", "ctime"]]
    df["ctime"] = pd.to_datetime(df["ctime"], unit="s", utc=True).dt.tz_convert(
        "Asia/Shanghai"
    )
    df.columns = ["标题", "内容", "等级", "标签", "发布时间"]
    df.sort_values(["发布时间"], ascending=False, inplace=True)
    df.reset_index(inplace=True, drop=True)
    df_tags = df["标签"].to_numpy()
    tags_data = []
    for tags in df_tags:
        if tags:
            ts = ",".join([t["subject_name"] for t in tags])
        else:
            ts = ""
        tags_data.append(ts)
    df["标签"] = tags_data
    df["发布日期"] = df["发布时间"].dt.date
    df["发布时间"] = df["发布时间"].dt.time

    return df


def get_cninfo_orgid(stock_code):
    """
    根据股票代码获取巨潮资讯网的 orgId

    Args:
        stock_code: 股票代码，如 '300017'

    Returns:
        orgId 字符串，如 '9900008387'
    """
    url = "http://www.cninfo.com.cn/new/information/topSearch/query"
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.cninfo.com.cn",
        "Origin": "http://www.cninfo.com.cn",
        "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    data = {"keyWord": stock_code, "maxNum": 10}

    try:
        resp = requests.post(url, data=data, headers=headers, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        if result and len(result) > 0:
            # 匹配精确的股票代码
            for item in result:
                if item.get("code") == stock_code:
                    return item.get("orgId")
            # 如果没有精确匹配，返回第一个
            return result[0].get("orgId")
    except Exception as e:
        logger.error(f"获取 orgId 失败: {stock_code}, error: {e}")

    return None


def cninfo_announcement_spider(
    pageNum, tabType, stock="", searchkey="", category="", trade="", seDate=None
):
    """
    巨潮资讯网公告爬虫
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

    stock 参数格式支持:
    - 单独股票代码: '300017'
    - code,orgId 格式: '300017,9900008387' (推荐，更精确)
    """
    if seDate is None:
        today_str = datetime.date.today().strftime("%Y-%m-%d")
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        seDate = yesterday_str + "~" + today_str

    url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
    pageNum = int(pageNum)
    data = {
        "pageNum": pageNum,
        "pageSize": 30,
        "column": "szse",
        "tabName": tabType,
        "plate": "",
        "stock": stock,
        "searchkey": searchkey,
        "secid": "",
        "category": category,
        "trade": trade,
        "seDate": seDate,
        "sortName": "",
        "sortType": "",
        "isHLtitle": "true",
    }

    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.cninfo.com.cn",
        "Origin": "http://www.cninfo.com.cn",
        "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        results = requests.post(url, data=data, headers=headers, timeout=10).json()
        if results.get("announcements"):
            df = pd.DataFrame(results["announcements"])
            df = df[
                [
                    "announcementTime",
                    "secName",
                    "secCode",
                    "announcementTitle",
                    "adjunctUrl",
                ]
            ]
            df["announcementTime"] = df["announcementTime"].apply(
                lambda x: datetime.datetime.fromtimestamp(x / 1000).strftime("%Y-%m-%d")
            )
            df["adjunctUrl"] = df["adjunctUrl"].apply(
                lambda x: "http://static.cninfo.com.cn/" + x
            )
            return df
    except Exception as e:
        logger.error(f"cninfo_announcement_spider error: {e}")

    return None
