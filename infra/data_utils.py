"""
通用数据处理模块
包含数据转换、格式化等基础设施
"""

from typing import Any, Optional, Union
import pandas as pd


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
