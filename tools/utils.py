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
        env_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", ".env")
        )
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


def get_tushare_pro():
    """
    获取 Tushare Pro API 客户端

    Returns:
        ts.ProApi: Tushare Pro API 客户端

    Raises:
        RuntimeError: 如果无法获取 TUSHARE_TOKEN
    """
    token = get_tushare_token()
    if not token:
        raise RuntimeError(
            "Missing TUSHARE_TOKEN: 请设置环境变量或在 .streamlit/secrets.toml 中配置"
        )
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


# =============================================================================
# 数据处理工具函数
# =============================================================================


def latest_metric_from_df(
    df: pd.DataFrame, value_col: str, date_col: str = "date"
) -> Optional[dict]:
    """从 DataFrame 中获取最新和前一行的指标值

    Args:
        df: DataFrame
        value_col: 数值列名
        date_col: 日期列名，默认为 "date"

    Returns:
        dict: 包含 date, value, prev_value 的字典，如果数据为空则返回 None
    """
    if df is None or df.empty or value_col not in df.columns:
        return None
    view = df.copy()
    if date_col in view.columns:
        view[date_col] = pd.to_datetime(view[date_col], errors="coerce")
    view[value_col] = pd.to_numeric(view[value_col], errors="coerce")
    view = view.dropna(subset=[value_col])
    if date_col in view.columns:
        view = view.dropna(subset=[date_col]).sort_values(date_col, ascending=False)
    if view.empty:
        return None
    latest = view.iloc[0]
    prev_value = None
    if len(view) > 1:
        prev_value = view.iloc[1][value_col]
    return {
        "date": latest[date_col] if date_col in view.columns else None,
        "value": float(latest[value_col]),
        "prev_value": float(prev_value)
        if prev_value is not None and not pd.isna(prev_value)
        else None,
    }


def calc_pct_change(current: float, previous: float) -> Optional[float]:
    """计算百分比变化

    Args:
        current: 当前值
        previous: 前一值

    Returns:
        float: 百分比变化，如果无法计算则返回 None
    """
    if current is None or previous is None or previous == 0:
        return None
    return (current / previous - 1) * 100


def series_from_df(df: pd.DataFrame, value_col: str, days: int) -> list:
    """从 DataFrame 提取时间序列数据

    Args:
        df: DataFrame
        value_col: 数值列名
        days: 返回的天数

    Returns:
        list: 包含 date 和 value 的字典列表
    """
    if df is None or df.empty or value_col not in df.columns:
        return []
    view = df.copy()
    if "date" in view.columns:
        view["date"] = pd.to_datetime(view["date"], errors="coerce")
    view[value_col] = pd.to_numeric(view[value_col], errors="coerce")
    view = view.dropna(subset=[value_col])
    if "date" in view.columns:
        view = view.dropna(subset=["date"]).sort_values("date")
    if view.empty:
        return []
    view = view.tail(int(days))
    if "date" in view.columns:
        view["date"] = view["date"].dt.strftime("%Y-%m-%d")
    view = view[["date", value_col]].rename(columns={value_col: "value"})
    return view.to_dict(orient="records")


def filter_st_bj_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """过滤掉 ST 和北交所股票

    Args:
        df: 包含 code 和 name 列的 DataFrame

    Returns:
        pd.DataFrame: 过滤后的 DataFrame
    """
    if df is None or df.empty:
        return df
    if {"code", "name"}.issubset(df.columns):
        view = df.copy()
        code_str = view["code"].astype(str).str.lower().str.strip()
        is_bj = code_str.str.startswith("bj") | code_str.str.startswith(("4", "8"))
        is_st = view["name"].astype(str).str.upper().str.contains("ST", na=False)
        return view[~is_bj & ~is_st]
    return df


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


def scrape_with_jina_reader(
    url: str, title: str = "", output_dir: str = "", save_to_file: bool = True
) -> dict:
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
                "success": False,
                "content": "",
                "filepath": "",
                "error": "Missing JINA_API_KEY: 请设置环境变量或在 .streamlit/secrets.toml 中配置",
            }
            logger.error(result["error"])
            return result

        # 设置Jina Reader的请求头
        jina_headers = {
            "Authorization": f"Bearer {jina_api_key}",
            "X-Return-Format": "markdown",
            "X-With-Images-Summary": "true",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

        logger.info(f"使用Jina Reader爬取: {url}")
        response = requests.get(jina_url, headers=jina_headers, timeout=30)
        response.raise_for_status()

        result = {"success": False, "content": "", "filepath": "", "error": ""}

        # 如果返回200，处理返回的markdown内容
        if response.status_code == 200:
            result["success"] = True
            result["content"] = response.text

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
                    content.append(
                        f"Scraped with Jina Reader: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    )
                    content.append(response.text)

                    # 写入文件
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write("".join(content))

                    result["filepath"] = filepath
                    logger.info(f"Jina Reader爬取成功并保存到: {filepath}")

                except Exception as e:
                    logger.error(f"保存文件失败: {e}")
                    result["error"] = f"保存文件失败: {str(e)}"
            else:
                logger.info("Jina Reader爬取成功")
        else:
            result["error"] = f"Jina Reader返回状态码: {response.status_code}"
            logger.error(f"Jina Reader返回状态码: {response.status_code}")

    except requests.exceptions.RequestException as e:
        result["error"] = f"网络请求失败: {str(e)}"
        logger.error(f"Jina Reader网络请求失败: {e}")
    except Exception as e:
        result["error"] = f"爬取失败: {str(e)}"
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
        filename = filename.replace(char, "_")

    # 限制文件名长度
    if len(filename) > 100:
        filename = filename[:100]

    return filename


@st.cache_data(ttl="15day")
def get_stock_list():
    """获取股票列表"""
    url = "http://www.cninfo.com.cn/new/data/szse_stock.json"
    resp = requests.get(url).json()["stockList"]
    df = pd.DataFrame(resp)
    return df


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
        "sec-ch-ua-platform": '"Windows"',
    }

    if stock_code.startswith("6"):
        stock_code = "SH" + stock_code
    else:
        stock_code = "SZ" + stock_code

    topic_texts = []
    for i in range(1, page_id):
        url = f"https://xueqiu.com/query/v1/symbol/search/status.json?count=10&comment=0&symbol={stock_code}&hl=0&source=all&sort=time&page={i}&q=&type=82"
        try:
            resp = requests.get(url=url, headers=headers, timeout=8)
            resp.raise_for_status()
            data = resp.json()
            topics = data.get("list", [])
        except Exception as e:
            # 可根据需要打印或记录异常
            break
        if not topics:
            break
        for topic in topics:
            if "text" in topic:
                txt = topic.get("text")
                # 去除<img ...>标签
                txt = re.sub(r"<img.*?>", "", txt, flags=re.DOTALL)
                # 去除<a ...>...</a>标签及其内容
                txt = re.sub(r"<a.*?>.*?</a>", "", txt, flags=re.DOTALL)
                topic_texts.append(txt)
    return topic_texts


def weibo_comments(wid):
    """获取微博评论"""
    url = f"https://weibo.com/ajax/statuses/show?id={wid}"
    header = {"user-agent": UserAgent().random}
    res = requests.get(url=url, headers=header)
    json_data = res.json()
    id = json_data["id"]
    user_id = json_data["user"]["idstr"]

    # 获取评论
    comments = []
    max_id = ""
    while max_id != 0:
        pl_url = f"https://weibo.com/ajax/statuses/buildComments?is_reload=1&id={id}&is_show_bulletin=2&is_mix=0&max_id={max_id}&count=10&uid={user_id}"
        resp = requests.get(url=pl_url, headers=header)
        json_data = resp.json()
        max_id = json_data["max_id"]
        lis = json_data["data"]
        for li in lis:
            text_raw = li["text_raw"]
            comments.append(text_raw)
    return comments


# =============================================================================
# 数据存储工具函数（从 storage_utils.py 迁移）
# =============================================================================

import json
from datetime import date, datetime

REVIEW_DIR = os.path.join("datas", "reviews")


def _json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass
    return str(obj)


def _normalize_date_str(value):
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def _resolve_review_dir(review_dir=None):
    return os.fspath(review_dir) if review_dir else REVIEW_DIR


def ensure_review_dir(review_dir=None):
    target_dir = _resolve_review_dir(review_dir)
    os.makedirs(target_dir, exist_ok=True)
    return target_dir


def _save_review_file(date_str, payload, review_dir=None):
    target_dir = ensure_review_dir(review_dir)
    file_path = os.path.join(target_dir, f"{date_str}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=_json_default)
    return file_path


def save_review_data(date, data, review_dir=None):
    date_str = _normalize_date_str(date)
    if not date_str:
        raise ValueError(f"Invalid review date: {date}")

    payload = dict(data or {})
    payload["saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload["date"] = date_str

    return _save_review_file(date_str, payload, review_dir=review_dir)
