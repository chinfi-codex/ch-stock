"""
爬虫模块
包含新闻爬虫、公告爬虫、微信爬虫、电报爬虫等功能
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
from .utils import scrape_with_jina_reader, clean_filename

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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

    df = df[["title", "content", 'level', "subjects", "ctime"]]
    df["ctime"] = pd.to_datetime(
        df["ctime"], unit="s", utc=True
    ).dt.tz_convert("Asia/Shanghai")
    df.columns = ["标题", "内容", "等级", "标签", "发布时间"]
    df.sort_values(["发布时间"], ascending=False, inplace=True)
    df.reset_index(inplace=True, drop=True)
    df_tags = df["标签"].to_numpy()
    tags_data = []
    for tags in df_tags:
        if tags: 
            ts = ','.join([t['subject_name'] for t in tags])
        else: 
            ts = ''
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
    url = 'http://www.cninfo.com.cn/new/information/topSearch/query'
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'www.cninfo.com.cn',
        'Origin': 'http://www.cninfo.com.cn',
        'Referer': 'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    data = {
        'keyWord': stock_code,
        'maxNum': 10
    }
    
    try:
        resp = requests.post(url, data=data, headers=headers, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        if result and len(result) > 0:
            # 匹配精确的股票代码
            for item in result:
                if item.get('code') == stock_code:
                    return item.get('orgId')
            # 如果没有精确匹配，返回第一个
            return result[0].get('orgId')
    except Exception as e:
        logger.error(f"获取 orgId 失败: {stock_code}, error: {e}")
    
    return None


def cninfo_announcement_spider(pageNum, tabType, stock='', searchkey='', category='',trade='', seDate=None):
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
        seDate = yesterday_str + '~' + today_str
        
    url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
    pageNum = int(pageNum)
    data = {
        'pageNum': pageNum,
        'pageSize': 30,
        'column': 'szse',
        'tabName': tabType,
        'plate': '',
        'stock': stock,
        'searchkey': searchkey,
        'secid': '',
        'category': category,
        'trade': trade,
        'seDate': seDate,
        'sortName': '',
        'sortType': '',
        'isHLtitle': 'true'
    }

    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'www.cninfo.com.cn',
        'Origin': 'http://www.cninfo.com.cn',
        'Referer': 'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }

    try:
        results = requests.post(url, data=data, headers=headers, timeout=10).json()
        if results.get('announcements'):
            df = pd.DataFrame(results['announcements'])
            df = df[['announcementTime', 'secName', 'secCode', 'announcementTitle', 'adjunctUrl']]
            df['announcementTime'] = df['announcementTime'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
            df['adjunctUrl'] = df['adjunctUrl'].apply(lambda x: 'http://static.cninfo.com.cn/' + x)
            return df
    except Exception as e:
        logger.error(f"cninfo_announcement_spider error: {e}")
    
    return None


def wxmp_post_list(fakeid, begin=0, max_retries=3):
    """
    微信文章列表爬虫 - 优化版本
    
    Args:
        fakeid (str): 公众号的fakeid
        begin (int): 起始位置
        max_retries (int): 最大重试次数
    
    Returns:
        pd.DataFrame or str: 文章列表DataFrame或错误信息
    """
    try:
        # 读取配置文件
        config_path = "tools/weixin_config.txt"
        if not os.path.exists(config_path):
            return "配置文件不存在: tools/weixin_config.txt"
        
        with open(config_path, "r", encoding='utf-8') as file:
            lines = [line.strip() for line in file.readlines() if line.strip()]
            if len(lines) < 2:
                return "配置文件格式错误，需要至少2行：cookie和token"
            
            cookie = lines[0]
            token = lines[1]
        
        # 优化的User-Agent
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

        # 完整的请求头
        headers = {
            "Cookie": cookie,
            "User-Agent": user_agent,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://mp.weixin.qq.com/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }

        url = "https://mp.weixin.qq.com/cgi-bin/appmsgpublish"
        params = {
            "sub": "list",
            "search_field": "null",
            "begin": str(begin),
            "count": "10",  # 增加每页数量
            "query": "",
            "fakeid": fakeid,
            "type": "101_1",
            "free_publish_type": "1",
            "sub_action": "list_ex",
            "token": token,
            "lang": "zh_CN",
            "f": "json",
            "ajax": "1"
        }

        # 重试机制
        for attempt in range(max_retries):
            try:
                logger.info(f"尝试获取微信文章列表，第{attempt + 1}次尝试")
                resp = requests.get(url, params=params, headers=headers, timeout=10)
                resp.raise_for_status()
                resp_json = resp.json()
                
                # 检查响应状态
                if 'base_resp' not in resp_json:
                    logger.error(f"响应格式错误: {resp_json}")
                    continue
                
                ret = resp_json['base_resp'].get('ret', 0)
                
                # 处理各种错误码
                if ret == 200013:
                    logger.warning("频率限制，等待后重试")
                    time.sleep(5)
                    continue
                elif ret == 200001:
                    return "token已过期，需要重新获取"
                elif ret == 200002:
                    return "cookie已过期，需要重新登录"
                elif ret != 0:
                    logger.error(f"微信API返回错误: {ret} - {resp_json['base_resp'].get('err_msg', '未知错误')}")
                    continue
                
                # 解析文章数据
                if "publish_page" in resp_json:
                    publish_page = json.loads(resp_json['publish_page'])
                    if "publish_list" in publish_page:
                        articles = []
                        for item in publish_page['publish_list']:
                            if item.get('publish_type') == 1:  # 只处理普通文章
                                try:
                                    publish_info = json.loads(item['publish_info'])
                                    if 'appmsgex' in publish_info:
                                        for article in publish_info['appmsgex']:
                                            articles.append({
                                                'appmsgid': article.get('appmsgid', ''),
                                                'create_time': article.get('create_time', 0),
                                                'title': article.get('title', ''),
                                                'digest': article.get('digest', ''),
                                                'cover': article.get('cover', ''),
                                                'link': article.get('link', ''),
                                                'author': article.get('author', ''),
                                                'content_url': article.get('content_url', ''),
                                                'copyright_stat': article.get('copyright_stat', 0),
                                                'digest': article.get('digest', ''),
                                                'fileid': article.get('fileid', 0),
                                                'is_multi': article.get('is_multi', 0),
                                                'multi_app_msg_item_list': article.get('multi_app_msg_item_list', [])
                                            })
                                except (json.JSONDecodeError, KeyError) as e:
                                    logger.warning(f"解析文章信息失败: {e}")
                                    continue
                        
                        if articles:
                            df = pd.DataFrame(articles)
                            # 转换时间戳
                            df['create_time'] = pd.to_datetime(df['create_time'], unit='s')
                            df['create_date'] = df['create_time'].dt.date
                            df['create_time_str'] = df['create_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
                            
                            # 添加来源信息
                            df['fakeid'] = fakeid
                            df['scrape_time'] = datetime.datetime.now()
                            
                            logger.info(f"成功获取 {len(df)} 篇文章")
                            return df
                        else:
                            logger.info("未找到文章数据")
                            return pd.DataFrame()
                
                logger.warning("响应中未找到publish_page数据")
                continue
                
            except requests.exceptions.RequestException as e:
                logger.error(f"网络请求失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # 指数退避
                continue
            except json.JSONDecodeError as e:
                logger.error(f"JSON解析失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                continue
        
        return f"重试{max_retries}次后仍然失败"
        
    except Exception as e:
        logger.error(f"微信文章列表爬取失败: {e}")
        return f"爬取失败: {str(e)}"


class InfoSpider:
    """公告爬虫类"""
    
    def info_relations_spider(self):
        """公告：调研活动"""
        for pageNum in range(1, 20):
            try:
                infos = cninfo_announcement_spider(pageNum=pageNum, tabType='relation')
                if len(infos) > 0:
                    # mysql_result = storager.mysql_storager(infos, 'CNINFO_RELATIONS')
                    print(f'RELATION SAVE: page{pageNum}, {len(infos)} records')
                    
                    # 这里需要导入PyPDFLoader和PineconeConnection
                    # pc = storager.PineconeConnection()
                    # for i, row in infos.iterrows():
                    #     loader = PyPDFLoader(row['adjunctUrl'])
                    #     docs = loader.load()
                    #     for d in docs:
                    #         d.metadata['source'] = row['adjunctUrl']
                    #         d.metadata['date'] = row['announcementTime']
                    #         d.metadata['secName'] = row['secName']
                    #     pinecone_result = pc.storager(docs, dataType="docs", namespace="relation")
                    #     logger.info(f'RELATION TO VECTOR: {pinecone_result}')
                else:
                    logger.info('NO NEW DATA')
            except Exception as e:
                logger.warning(e)
                pass

    def info_operation_spider(self):
        """公告：日常经营、股权激励"""
        for pageNum in range(1, 20):
            try:
                infos = cninfo_announcement_spider(pageNum=pageNum, tabType='fulltext', category='category_rcjy_szsh;category_gqjl_szsh')
                filter_keywords = '激励,期权,减持,合同,合作,协议,进展'
                fk = '|'.join(filter_keywords.split(','))
                filtered_infos = infos[infos['announcementTitle'].str.contains(fk)]
                # s = storager.mysql_storager(filtered_infos, 'CNINFO_OPERATION')
                print(f'OPERATION SAVE: page{pageNum}, {len(filtered_infos)} records')
            except Exception as e:
                print(e)
                pass

    def info_reports_spider(self):
        """公告：业绩预告、季度报告"""
        for pageNum in range(1, 20):
            try:
                infos = cninfo_announcement_spider(pageNum=pageNum, tabType='fulltext', category='category_yjygjxz_szsh;category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh')
                # s = storager.mysql_storager(infos, 'CNINFO_REPORTS')
                print(f'REPORTS SAVE: page{pageNum}, {len(infos)} records')
            except Exception as e:
                pass

    def info_ipo_spider(self):
        """公告： IPO、招股书"""
        for pageNum in range(1, 20):
            try:
                infos = cninfo_announcement_spider(pageNum=pageNum, tabType='fulltext', category='category_sf_szsh')
                filter_keywords = '招股说明,招股意向'
                fk = '|'.join(filter_keywords.split(','))
                filtered_infos = infos[infos['announcementTitle'].str.contains(fk)]
                # s = storager.mysql_storager(filtered_infos, 'CNINFO_IPO')
                print(f'IPO SAVE: page{pageNum}, {len(filtered_infos)} records')
            except Exception as e:
                pass


class ISWSpider:
    """Understanding War 网站爬虫类"""
    
    def __init__(self):
        self.base_url = "https://www.understandingwar.org"
        self.publications_url = "https://www.understandingwar.org/publications"
        self.backgrounder_url = "https://www.understandingwar.org/backgrounder"
        self.output_dir = "datas/isw"
        self.images_dir = "datas/isw/images"
        
        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.images_dir, exist_ok=True)
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

    def download_image(self, img_url, filename):
        """下载图片到本地"""
        try:
            if not img_url.startswith('http'):
                img_url = urljoin(self.base_url, img_url)
            
            img_path = os.path.join(self.images_dir, filename)
            
            # 如果文件已存在，跳过下载
            if os.path.exists(img_path):
                return f"images/{filename}"
            
            response = requests.get(img_url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            with open(img_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded image: {filename}")
            return f"images/{filename}"
        except Exception as e:
            logger.error(f"Failed to download image {img_url}: {e}")
            return img_url

    def extract_backgrounder_links_from_page(self, page_num=0):
        """从指定页面提取backgrounder文章链接"""
        try:
            # 构建页面URL
            if page_num == 0:
                page_url = self.publications_url
            else:
                page_url = f"{self.publications_url}?page={page_num + 1}"
            
            logger.info(f"Extracting links from page: {page_url}")
            
            response = requests.get(page_url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 查找所有backgrounder文章链接
            links = []
            
            # 查找所有链接
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if href and not href.startswith('#'):
                    # 确保是内部链接
                    if not href.startswith('http'):
                        full_url = urljoin(self.base_url, href)
                    else:
                        full_url = href
                    
                    # 检查是否是backgrounder文章页面
                    if '/backgrounder/' in full_url and full_url != self.backgrounder_url:
                        title = link.get_text(strip=True)
                        if title and len(title) > 5:  # 确保有有意义的标题
                            # 过滤掉导航链接
                            if not any(nav_word in title.lower() for nav_word in ['home', 'about', 'contact', 'donate', 'search', 'login', 'subscribe']):
                                links.append({
                                    'url': full_url,
                                    'title': title
                                })
            
            # 去重
            unique_links = []
            seen_urls = set()
            for link in links:
                if link['url'] not in seen_urls:
                    unique_links.append(link)
                    seen_urls.add(link['url'])
            
            logger.info(f"Found {len(unique_links)} unique backgrounder links on page {page_num}")
            return unique_links
            
        except Exception as e:
            logger.error(f"Failed to extract backgrounder links from page {page_num}: {e}")
            return []

    def scrape_article_with_jina(self, url, title):
        """使用Jina Reader爬取单个文章页面"""
        try:
            # 使用utils中的Jina Reader方法
            result = scrape_with_jina_reader(
                url=url,
                title=title,
                output_dir=self.output_dir,
                save_to_file=True
            )
            
            if result['success']:
                logger.info(f"Successfully scraped article with Jina Reader: {title}")
                return result['filepath']
            else:
                logger.error(f"Failed to scrape article with Jina Reader: {result['error']}")
                return None
            
        except Exception as e:
            logger.error(f"Failed to scrape article with Jina Reader {url}: {e}")
            return None

    def run(self, max_pages=5, max_articles_per_page=10):
        """运行爬虫，支持分页"""
        logger.info(f"Starting ISW backgrounder spider with max_pages={max_pages}...")
        
        total_successful_scrapes = 0
        
        for page_num in range(max_pages):
            logger.info(f"Processing page {page_num + 1}/{max_pages}")
            
            # 提取当前页面的backgrounder文章链接
            links = self.extract_backgrounder_links_from_page(page_num)
            
            if not links:
                logger.warning(f"No backgrounder links found on page {page_num}")
                continue
            
            # 限制每页爬取的文章数量
            links = links[:max_articles_per_page]
            
            page_successful_scrapes = 0
            for i, link in enumerate(links, 1):
                logger.info(f"Scraping {i}/{len(links)} on page {page_num + 1}: {link['title']}")
                
                result = self.scrape_article_with_jina(link['url'], link['title'])
                if result:
                    page_successful_scrapes += 1
                    total_successful_scrapes += 1
                
                # 添加延迟避免被封
                time.sleep(2)
            
            logger.info(f"Page {page_num + 1} finished. Successfully scraped {page_successful_scrapes}/{len(links)} articles")
            
            # 页面间延迟
            if page_num < max_pages - 1:
                time.sleep(5)
        
        logger.info(f"ISW backgrounder spider finished. Total successfully scraped {total_successful_scrapes} articles across {max_pages} pages") 