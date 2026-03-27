"""
数据存储模块
包含文件处理等基础设施
"""

import os


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
