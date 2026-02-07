#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据存储工具模块
用于保存和加载每日复盘数据
"""
import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path


# 存储目录配置
REVIEW_DIR = os.path.join('datas', 'reviews')


def ensure_review_dir():
    """确保复盘数据存储目录存在"""
    if not os.path.exists(REVIEW_DIR):
        os.makedirs(REVIEW_DIR)
        print(f"创建目录: {REVIEW_DIR}")


def save_review_data(date, data):
    """
    保存复盘数据到JSON文件
    
    Args:
        date: 日期对象或字符串 (格式: YYYY-MM-DD)
        data: 要保存的数据字典
    
    Returns:
        str: 保存的文件路径
    """
    ensure_review_dir()
    
    # 格式化日期
    if isinstance(date, str):
        date_str = date
    else:
        date_str = date.strftime('%Y-%m-%d')
    
    # 构建文件路径
    file_path = os.path.join(REVIEW_DIR, f"{date_str}.json")
    
    # 添加保存时间戳
    data['saved_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data['date'] = date_str
    
    # 保存数据
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return file_path


def load_review_data(date):
    """
    加载指定日期的复盘数据
    
    Args:
        date: 日期对象或字符串 (格式: YYYY-MM-DD)
    
    Returns:
        dict: 复盘数据字典，如果文件不存在返回None
    """
    # 格式化日期
    if isinstance(date, str):
        date_str = date
    else:
        date_str = date.strftime('%Y-%m-%d')
    
    # 构建文件路径
    file_path = os.path.join(REVIEW_DIR, f"{date_str}.json")
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        return None
    
    # 加载数据
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data


def list_review_dates():
    """
    列出所有已保存的复盘日期
    
    Returns:
        list: 日期字符串列表，按日期降序排列
    """
    ensure_review_dir()
    
    # 获取所有JSON文件
    files = [f for f in os.listdir(REVIEW_DIR) if f.endswith('.json')]
    
    # 提取日期并排序
    dates = [f.replace('.json', '') for f in files]
    dates.sort(reverse=True)
    
    return dates


def df_to_dict(df):
    """
    将DataFrame转换为可JSON序列化的字典
    
    Args:
        df: pandas DataFrame
    
    Returns:
        dict: 可序列化的字典
    """
    if df is None or df.empty:
        return None
    
    # 转换为字典格式
    return df.to_dict(orient='records')


def prepare_review_data(market_data, top_stocks_stats, top_range_data, longhu_data):
    """
    准备要保存的复盘数据
    
    Args:
        market_data: 市场概况数据字典
        top_stocks_stats: 成交额TOP100统计数据
        top_range_data: 涨幅TOP100数据
        longhu_data: 龙虎榜数据
    
    Returns:
        dict: 格式化的复盘数据
    """
    review_data = {
        'market_overview': market_data,
        'top_100_turnover': top_stocks_stats,
        'top_100_range': top_range_data,
        'longhu': longhu_data
    }
    
    return review_data
