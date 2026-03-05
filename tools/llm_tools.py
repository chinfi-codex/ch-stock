"""
大模型工具模块
包含多种LLM提供商的统一调用接口
"""

import os
import streamlit as st
import openai


def _get_api_key(provider: str) -> str:
    """
    获取LLM提供商的API密钥
    优先级：环境变量 > Streamlit secrets
    
    Args:
        provider: 提供商名称 (doubao, siliconflow, kimi)
        
    Returns:
        str: API密钥
        
    Raises:
        ValueError: 如果未找到对应提供商的API密钥
    """
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
        raise ValueError(f"不支持的LLM提供商: {provider}")
    
    # 1. 尝试从环境变量获取
    api_key = os.environ.get(env_var_map[provider], "").strip()
    if api_key:
        return api_key
    
    # 2. 尝试从 Streamlit secrets 获取
    try:
        api_key = st.secrets.get(secret_key_map[provider], "")
        if api_key:
            return api_key
    except Exception:
        pass
    
    raise ValueError(
        f"未找到 {provider} 的API密钥。"
        f"请设置环境变量 {env_var_map[provider]} "
        f"或在 .streamlit/secrets.toml 中配置 {secret_key_map[provider]}"
    )


def _create_completion(api_key, base_url, model_id, sys_msg, query):
    """兼容 openai v0.x / v1.x 两种 SDK 调用方式。"""
    messages = [
        {"role": "system", "content": sys_msg},
        {"role": "user", "content": query},
    ]

    # openai>=1.x
    if hasattr(openai, "OpenAI"):
        client = openai.OpenAI(api_key=api_key, base_url=base_url)
        completion = client.chat.completions.create(
            model=model_id,
            messages=messages,
            temperature=0,
            top_p=0.8,
        )
        if hasattr(completion, "choices") and completion.choices:
            return completion.choices[0].message.content
        return "请求失败，请检查API密钥或网络连接"

    # openai<1.x
    openai.api_key = api_key
    openai.api_base = base_url
    completion = openai.ChatCompletion.create(
        model=model_id,
        messages=messages,
        temperature=0,
        top_p=0.8,
    )
    choices = completion.get("choices", []) if isinstance(completion, dict) else []
    if choices:
        return choices[0]["message"]["content"]
    return "请求失败，请检查API密钥或网络连接"


@st.cache_data(ttl="1day", show_spinner="Thinking...")
def get_llm_response(query, provider="doubao", model=None, system_message=None):
    """
    统一的LLM调用接口（兼容 openai v0.x / v1.x）
    
    Args:
        query: 用户查询内容
        provider: LLM提供商 (doubao, siliconflow, kimi)
        model: 模型ID（仅 siliconflow 需要）
        system_message: 自定义系统消息
        
    Returns:
        str: LLM响应内容
    """
    provider_config = {
        "doubao": {
            "base_url": "https://ark.cn-beijing.volces.com/api/v3",
            "default_system_message": "你是豆包，是由字节跳动开发的 AI 人工智能助手",
            "default_model": "ep-20260103112951-vxd7j",
        },
        "siliconflow": {
            "base_url": "https://api.siliconflow.cn/v1",
            "default_system_message": "你是一个AI助手。",
            "default_model": model or "deepseek-ai/DeepSeek-R1",
        },
        "kimi": {
            "base_url": "https://api.moonshot.cn/v1",
            "default_system_message": "你是Kimi，一个由Moonshot AI开发的人工智能助手",
            "default_model": "moonshot-v1-8k",
        },
    }
    
    if provider not in provider_config:
        return "不支持的LLM提供商"
    
    config = provider_config[provider]
    
    try:
        api_key = _get_api_key(provider)
    except ValueError as e:
        return str(e)
    
    sys_msg = system_message if system_message is not None else config["default_system_message"]
    return _create_completion(api_key, config["base_url"], config["default_model"], sys_msg, query)
