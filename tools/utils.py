"""
工具函数模块
包含通用的工具函数和辅助方法
"""

import os
import re
import logging
from datetime import datetime, timedelta, date
from typing import Any, Optional, Union

import pandas as pd
import streamlit as st
import requests
import tushare as ts
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)


# =============================================================================
# Tushare 相关工具函数
# =============================================================================

def get_tushare_token() -> str:
    """
    获取 Tushare Token
    优先级：环境变量 > streamlit secrets > .env文件
    
    Returns:
        str: Tushare token，如果未找到则返回空字符串
    """
    # 1. 尝试从环境变量获取
    token = os.environ.get("TUSHARE_TOKEN", "").strip()
    if token:
        return token
    
    # 2. 尝试从 streamlit secrets 获取
    try:
        token = st.secrets.get("tushare_token", "")
        if token:
            return token.strip()
    except Exception:
        pass
    
    # 3. 尝试从 .env 文件获取
    try:
        env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s or s.startswith("#") or "=" not in s:
                        continue
                    k, v = s.split("=", 1)
                    if k.strip() == "TUSHARE_TOKEN":
                        token = v.strip().strip('"').strip("'")
                        if token:
                            return token
    except Exception:
        pass
    
    return ""


def get_tushare_pro() -> ts.ProApi:
    """
    获取 Tushare Pro API 客户端
    
    Returns:
        ts.ProApi: Tushare Pro API 客户端
        
    Raises:
        RuntimeError: 如果无法获取 TUSHARE_TOKEN
    """
    token = get_tushare_token()
    if not token:
        raise RuntimeError("Missing TUSHARE_TOKEN: 请设置环境变量或在 .streamlit/secrets.toml 中配置")
    return ts.pro_api(token)


# =============================================================================
# 股票代码转换工具函数
# =============================================================================

def convert_to_ts_code(code: Optional[str]) -> str:
    """
    将多种股票代码格式转换为 Tushare ts_code 格式 (xxxxxx.SH/SZ/BJ)
    
    支持的输入格式：
    - 纯数字: 000001, 600000
    - 带前缀: sz000001, sh600000, SZ000001, SH600000
    - ts_code: 000001.SZ, 600000.SH
    
    Args:
        code: 股票代码
        
    Returns:
        str: 标准 ts_code 格式
        
    Raises:
        ValueError: 如果 code 为 None 或空字符串
    """
    if code is None:
        raise ValueError("股票代码不能为空")
    
    code = str(code).strip()
    if not code:
        raise ValueError("股票代码不能为空")
    
    upper_code = code.upper()
    
    # 已经是 ts_code 格式
    if "." in upper_code:
        prefix, suffix = upper_code.split(".", 1)
        suffix = suffix.replace("SS", "SH")  # 兼容 SS 后缀
        if suffix in {"SH", "SZ", "BJ"}:
            return f"{prefix}.{suffix}"
    
    # 带前缀格式 (szxxxxxx, shxxxxxx, bjxxxxxx)
    if upper_code.startswith(("SZ", "SH", "BJ")) and len(upper_code) >= 8:
        body = upper_code[2:]
        suffix = upper_code[:2]
        return f"{body}.{suffix}"
    
    # 纯数字格式
    if len(code) == 6 and code.isdigit():
        if code.startswith(("0", "3")):
            return f"{code}.SZ"
        elif code.startswith(("6", "9")):
            return f"{code}.SH"
        elif code.startswith("8"):
            return f"{code}.BJ"
    
    # 无法识别的格式，原样返回
    return upper_code


def convert_to_ak_code(code: str) -> str:
    """
    将股票代码转换为 AKShare 格式 (shxxxxxx/szxxxxxx/bjxxxxxx)
    
    Args:
        code: 股票代码
        
    Returns:
        str: AKShare 格式代码
    """
    code = str(code).strip()
    
    # 已经是 ak_code 格式
    if code.lower().startswith(("sh", "sz", "bj")) and len(code) >= 8:
        return code.lower()
    
    # ts_code 格式
    if "." in code:
        parts = code.split(".")
        if len(parts) == 2 and parts[1].upper() in ("SH", "SZ", "BJ"):
            return f"{parts[1].lower()}{parts[0]}"
    
    # 纯数字格式
    if len(code) == 6 and code.isdigit():
        if code.startswith(("0", "3")):
            return f"sz{code}"
        elif code.startswith(("6", "9")):
            return f"sh{code}"
        elif code.startswith("8"):
            return f"bj{code}"
    
    return code.lower()


def extract_code6(value: str) -> str:
    """
    从字符串中提取6位股票代码
    
    Args:
        value: 包含股票代码的字符串
        
    Returns:
        str: 6位数字股票代码，如果未找到则返回空字符串
    """
    m = re.search(r"(\d{6})", str(value))
    return m.group(1) if m else ""


def classify_board(code6: str) -> str:
    """
    根据6位股票代码判断所属板块
    
    Args:
        code6: 6位股票代码
        
    Returns:
        str: 板块名称 (主板/创业板/科创板/北交所)
    """
    if not code6 or len(code6) != 6:
        return "未知"
    
    if code6.startswith("688") or code6.startswith("689"):
        return "科创板"
    if code6.startswith(("300", "301")):
        return "创业板"
    if code6.startswith("8") or code6.startswith("4"):
        return "北交所"
    if code6.startswith(("0", "3", "6")):
        return "主板"
    
    return "未知"


# =============================================================================
# 数据处理工具函数
# =============================================================================

def pick_first_column(df: pd.DataFrame, candidates: list) -> Optional[str]:
    """
    从候选列名中选择第一个存在于 DataFrame 中的列名
    
    Args:
        df: DataFrame
        candidates: 候选列名列表
        
    Returns:
        Optional[str]: 存在的列名，如果都不存在则返回 None
    """
    if df is None or df.empty:
        return None
    for name in candidates:
        if name in df.columns:
            return name
    return None


def to_number(series: Union[pd.Series, Any]) -> Optional[pd.Series]:
    """
    将 Series 转换为数值类型，移除百分号
    
    Args:
        series: 输入数据
        
    Returns:
        Optional[pd.Series]: 数值 Series，如果输入为 None 则返回 None
    """
    if series is None:
        return None
    s = series.astype(str).str.replace("%", "", regex=False)
    return pd.to_numeric(s, errors="coerce")


def normalize_trade_date(trade_date: Any) -> tuple:
    """
    标准化交易日期
    
    Args:
        trade_date: 日期（支持多种格式）
        
    Returns:
        tuple: (yyyyMMdd 格式字符串, date 对象)
        
    Raises:
        ValueError: 如果日期格式无效
    """
    dt = pd.to_datetime(trade_date, errors="coerce")
    if pd.isna(dt):
        raise ValueError(f"Invalid trade date: {trade_date}")
    return dt.strftime("%Y%m%d"), dt.date()


# 为兼容旧代码，保留别名
_to_ts_code = convert_to_ts_code
_to_number = to_number
_pick_first_column = pick_first_column
_normalize_trade_date = normalize_trade_date


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
        
        # 获取 API Key
        jina_api_key = _get_jina_api_key()
        if not jina_api_key:
            result = {
                'success': False,
                'content': '',
                'filepath': '',
                'error': "Missing JINA_API_KEY: 请设置环境变量或在 .streamlit/secrets.toml 中配置"
            }
            logger.error(result['error'])
            return result
        
        # 设置Jina Reader的请求头
        jina_headers = {
            "Authorization": f"Bearer {jina_api_key}",
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


def _get_pushplus_token() -> str:
    """获取 PushPlus Token，优先级：环境变量 > streamlit secrets"""
    token = os.environ.get("PUSHPLUS_TOKEN", "").strip()
    if token:
        return token
    try:
        token = st.secrets.get("pushplus_token", "")
        if token:
            return token
    except Exception:
        pass
    return ""


def notify_pushplus(title, content, topic):
    """推送消息到PushPlus"""
    token = _get_pushplus_token()
    if not token:
        logger.error("Missing PUSHPLUS_TOKEN: 请设置环境变量或在 .streamlit/secrets.toml 中配置")
        return None
        
    url = 'http://www.pushplus.plus/send'
    payload = {
       "token": token,
       "title": title,
       "content": content, 
       "topic": topic,
       "template": "html"
    }
    headers = {
       'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
       'Content-Type': 'application/json'
    }
    resp = requests.post(url, json=payload, headers=headers)
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