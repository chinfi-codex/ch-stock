#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
统一配置管理入口。
"""

import os

import streamlit as st


_PLACEHOLDER_VALUES = {
    "",
    "your-alpha-vantage-key",
    "your-kimi-api-key",
}


def _read_env_file_value(key: str) -> str:
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
    if not os.path.exists(env_path):
        return ""

    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#") or "=" not in s:
                    continue
                k, v = s.split("=", 1)
                if k.strip() == key:
                    return v.strip().strip('"').strip("'")
    except Exception:
        return ""
    return ""


def _get_config_value(env_key: str, secret_key: str) -> str:
    value = os.environ.get(env_key, "").strip()
    if value:
        return value

    try:
        value = st.secrets.get(secret_key, "")
        if value:
            return str(value).strip()
    except Exception:
        pass

    return _read_env_file_value(env_key)


def get_tushare_token() -> str:
    """获取 Tushare Token。"""
    return _get_config_value("TUSHARE_TOKEN", "tushare_token")


def get_alpha_vantage_key() -> str:
    """获取 Alpha Vantage API Key。"""
    value = _get_config_value("ALPHAVANTAGE_API_KEY", "alpha_vantage_key").strip()
    if value.lower() in _PLACEHOLDER_VALUES:
        return ""
    return value


def get_jina_api_key() -> str:
    """获取 Jina API Key。"""
    value = _get_config_value("JINA_API_KEY", "jina_api_key")
    if value:
        return value

    value = _get_config_value("JINA_KEY", "jina_key")
    if value:
        return value

    return ""


def get_xueqiu_cookie() -> str:
    """获取雪球 Cookie。"""
    return _get_config_value("XUEQIU_COOKIE", "xueqiu_cookie")


def get_jin10_cookie() -> str:
    """获取 Jin10 Cookie。"""
    return _get_config_value("JIN10_COOKIE", "jin10_cookie")


def get_llm_api_key(provider: str) -> str:
    """按 provider 获取 LLM API Key。"""
    env_var_map = {
        "doubao": "DOUBAO_API_KEY",
        "siliconflow": "SILICONFLOW_API_KEY",
        "kimi": "KIMI_API_KEY",
    }
    secret_key_map = {
        "doubao": "doubao_api_key",
        "siliconflow": "siliconflow_api_key",
        "kimi": "kimi_api_key",
    }

    if provider not in env_var_map:
        raise ValueError(f"不支持的 LLM 提供商: {provider}")

    api_key = _get_config_value(env_var_map[provider], secret_key_map[provider])
    if api_key:
        return api_key

    raise ValueError(
        f"未找到 {provider} 的 API 密钥。请配置 {env_var_map[provider]} "
        f"或在 .streamlit/secrets.toml 中配置 {secret_key_map[provider]}"
    )


def get_zsxq_cookie() -> str:
    """获取知识星球 Cookie。"""
    return _get_config_value("ZSXQ_COOKIE", "zsxq_cookie")


def get_zsxq_group_ids() -> str:
    """获取知识星球 Group IDs。"""
    return _get_config_value("ZSXQ_GROUP_IDS", "zsxq_group_ids")


def get_zsxq_api_timeout() -> float:
    """获取知识星球 API 超时时间。"""
    value = _get_config_value("ZSXQ_API_TIMEOUT", "zsxq_api_timeout")
    if not value:
        return 10.0

    try:
        return float(value)
    except ValueError:
        return 10.0


def get_mysql_host() -> str:
    """获取 MySQL 主机。"""
    value = _get_config_value("MYSQL_HOST", "mysql_host")
    return value or "127.0.0.1"


def get_mysql_port() -> int:
    """获取 MySQL 端口。"""
    value = _get_config_value("MYSQL_PORT", "mysql_port")
    if not value:
        return 3306

    try:
        return int(value)
    except ValueError:
        return 3306


def get_mysql_database() -> str:
    """获取 MySQL 数据库名。"""
    return _get_config_value("MYSQL_DATABASE", "mysql_database")


def get_mysql_user() -> str:
    """获取 MySQL 用户名。"""
    return _get_config_value("MYSQL_USER", "mysql_user")


def get_mysql_password() -> str:
    """获取 MySQL 密码。"""
    return _get_config_value("MYSQL_PASSWORD", "mysql_password")


def get_sync_ssh_host() -> str:
    """获取同步 SSH 主机。"""
    return _get_config_value("SYNC_SSH_HOST", "sync_ssh_host")


def get_sync_ssh_port() -> int:
    """获取同步 SSH 端口。"""
    value = _get_config_value("SYNC_SSH_PORT", "sync_ssh_port")
    if not value:
        return 22

    try:
        return int(value)
    except ValueError:
        return 22


def get_sync_ssh_user() -> str:
    """获取同步 SSH 用户。"""
    return _get_config_value("SYNC_SSH_USER", "sync_ssh_user")


def get_sync_ssh_password() -> str:
    """获取同步 SSH 密码。"""
    return _get_config_value("SYNC_SSH_PASSWORD", "sync_ssh_password")
