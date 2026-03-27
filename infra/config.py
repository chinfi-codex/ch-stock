"""
配置管理模块
包含Token获取、配置读取等基础设施
"""

import os
import streamlit as st
import tushare as ts


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
