"""
工具函数模块
包含通用的工具函数和辅助方法
"""

import streamlit as st
import requests
import pandas as pd
import os
import datetime
import re
import logging
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)


def scrape_with_jina_reader(url: str, title: str = "", output_dir: str = "", save_to_file: bool = True) -> dict:
    """
    使用Jina Reader爬取网页内容
    
    Args:
        url (str): 要爬取的网页URL
        title (str): 文章标题，用于生成文件名
        output_dir (str): 输出目录，如果为空则不保存文件
        save_to_file (bool): 是否保存到文件
    
    Returns:
        dict: 包含爬取结果的字典
        {
            'success': bool,
            'content': str,
            'filepath': str,
            'error': str
        }
    """
    try:
        # 使用Jina Reader API
        jina_url = f"https://r.jina.ai/{url}"
        
        # 设置Jina Reader的请求头
        jina_headers = {
            "Authorization": "Bearer jina_045be800274949e78721c55b34acf1b2qKdRArT9C8sCsFPoakPTpVObepOp",
            "X-Return-Format": "markdown",
            "X-With-Images-Summary": "true",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        logger.info(f"使用Jina Reader爬取: {url}")
        response = requests.get(jina_url, headers=jina_headers, timeout=30)
        response.raise_for_status()
        
        result = {
            'success': False,
            'content': '',
            'filepath': '',
            'error': ''
        }
        
        # 如果返回200，处理返回的markdown内容
        if response.status_code == 200:
            result['success'] = True
            result['content'] = response.text
            
            # 如果需要保存到文件
            if save_to_file and output_dir and title:
                try:
                    # 确保输出目录存在
                    os.makedirs(output_dir, exist_ok=True)
                    
                    # 清理文件名
                    safe_title = clean_filename(title)
                    filename = f"{safe_title}.md"
                    filepath = os.path.join(output_dir, filename)
                    
                    # 构建完整的markdown内容
                    content = []
                    content.append(f"# {title}\n")
                    content.append(f"Source: {url}\n")
                    content.append(f"Scraped with Jina Reader: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                    content.append(response.text)
                    
                    # 写入文件
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(''.join(content))
                    
                    result['filepath'] = filepath
                    logger.info(f"Jina Reader爬取成功并保存到: {filepath}")
                    
                except Exception as e:
                    logger.error(f"保存文件失败: {e}")
                    result['error'] = f"保存文件失败: {str(e)}"
            else:
                logger.info("Jina Reader爬取成功")
        else:
            result['error'] = f"Jina Reader返回状态码: {response.status_code}"
            logger.error(f"Jina Reader返回状态码: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        result['error'] = f"网络请求失败: {str(e)}"
        logger.error(f"Jina Reader网络请求失败: {e}")
    except Exception as e:
        result['error'] = f"爬取失败: {str(e)}"
        logger.error(f"Jina Reader爬取失败: {e}")
    
    return result


def clean_filename(filename: str) -> str:
    """
    清理文件名，移除非法字符
    
    Args:
        filename (str): 原始文件名
    
    Returns:
        str: 清理后的文件名
    """
    # 移除或替换非法字符
    illegal_chars = '<>:"/\\|?*'
    for char in illegal_chars:
        filename = filename.replace(char, '_')
    
    # 限制文件名长度
    if len(filename) > 100:
        filename = filename[:100]
    
    return filename


@st.cache_data(ttl="15day")
def get_stock_list():
    """获取股票列表"""
    url = "http://www.cninfo.com.cn/new/data/szse_stock.json"
    resp = requests.get(url).json()['stockList']
    df = pd.DataFrame(resp)
    return df


def df_drop_duplicated(df, subset=None, keep='first'):
    """删除重复数据"""
    return df.drop_duplicates(subset=subset, keep=keep)


def notify_pushplus(title, content, topic):
    """推送消息到PushPlus"""
    url = 'http://www.pushplus.plus/send'
    payload = {
       "token": "349218916e154f048fbafc4a7edd9563",
       "title": title,
       "content": content, 
       "topic": topic,
       "template": "html"
    }
    headers = {
       'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
       'Content-Type': 'application/json'
    }
    resp = requests.post(url, payload, headers)
    return resp


def get_xueqiu_stock_topics(stock_code, cookie, page_id=3):
    """获取雪球股票话题"""
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
        "Connection": "keep-alive",
        "Cookie": cookie,  # 仍为变量
        "Host": "xueqiu.com",
        "Referer": "https://xueqiu.com/S/SH688372",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "elastic-apm-traceparent": "00-db43983b4c5505f4d4a674fd89b785f0-574b28f413051bda-00",
        "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"'
    }
  
    if stock_code.startswith('6'):
        stock_code = 'SH' + stock_code
    else:
        stock_code = 'SZ' + stock_code

    topic_texts = []
    for i in range(1, page_id):
        url = f'https://xueqiu.com/query/v1/symbol/search/status.json?count=10&comment=0&symbol={stock_code}&hl=0&source=all&sort=time&page={i}&q=&type=82'
        try:
            resp = requests.get(url=url, headers=headers, timeout=8)
            resp.raise_for_status()
            data = resp.json()
            topics = data.get('list', [])
        except Exception as e:
            # 可根据需要打印或记录异常
            break
        if not topics:
            break
        for topic in topics:
            if 'text' in topic:
                txt = topic.get('text')
                # 去除<img ...>标签
                txt = re.sub(r'<img.*?>', '', txt, flags=re.DOTALL)
                # 去除<a ...>...</a>标签及其内容
                txt = re.sub(r'<a.*?>.*?</a>', '', txt, flags=re.DOTALL)
                topic_texts.append(txt)
    return topic_texts


def weibo_comments(wid):
    """获取微博评论"""
    url = f'https://weibo.com/ajax/statuses/show?id={wid}'
    header = {
        'user-agent': UserAgent().random
    }
    res = requests.get(url=url, headers=header)
    json_data = res.json()
    id = json_data['id']
    user_id = json_data['user']['idstr']

    # 获取评论
    comments = []
    max_id = ''
    while max_id != 0:
        pl_url = f'https://weibo.com/ajax/statuses/buildComments?is_reload=1&id={id}&is_show_bulletin=2&is_mix=0&max_id={max_id}&count=10&uid={user_id}'
        resp = requests.get(url=pl_url, headers=header)
        json_data = resp.json()
        max_id = json_data['max_id']
        lis = json_data['data']
        for li in lis:
            text_raw = li['text_raw']
            comments.append(text_raw)
    return comments


class FileInfo:
    """文件信息类"""
    def __init__(self, filename, created_date, content):
        self.filename = filename
        self.created_date = created_date
        self.content = content


def read_files_by_condition(directory, keyword=None, start_date=None, end_date=None, encoding='utf-8'):
    """
    从本地指定目录中按条件读取文件内容，条件包括关键词、创建日期

    参数:
        directory (str): 目录路径
        keyword (str, optional): 文件名或内容需包含的关键词
        start_date (str or datetime.date, optional): 文件创建日期起始（包含），格式'YYYY-MM-DD'或date对象
        end_date (str or datetime.date, optional): 文件创建日期结束（包含），格式'YYYY-MM-DD'或date对象
        encoding (str): 文件编码，默认为'utf-8'

    返回:
        list of FileInfo: 每个FileInfo对象包含文件名称、创建日期、文件内容文本
    """
    results = []
    if isinstance(start_date, str):
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    for root, _, files in os.walk(directory):
        for fname in files:
            fpath = os.path.join(root, fname)
            try:
                stat = os.stat(fpath)
                created_date = datetime.date.fromtimestamp(stat.st_ctime)
                # 日期条件判断
                if start_date and created_date < start_date:
                    continue
                if end_date and created_date > end_date:
                    continue
                # 关键词条件判断（文件名或内容）
                content = None
                if keyword:
                    if keyword not in fname:
                        with open(fpath, 'r', encoding=encoding, errors='ignore') as f:
                            file_content = f.read()
                        if keyword not in file_content:
                            continue
                        content = file_content
                    else:
                        with open(fpath, 'r', encoding=encoding, errors='ignore') as f:
                            content = f.read()
                else:
                    with open(fpath, 'r', encoding=encoding, errors='ignore') as f:
                        content = f.read()
                results.append(FileInfo(filename=fpath, created_date=created_date, content=content))
            except Exception:
                continue
    return results 