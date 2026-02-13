"""
大模型工具模块
包含多种LLM提供商的统一调用接口
"""

import streamlit as st
import openai


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
    """统一的LLM调用接口（兼容 openai v0.x / v1.x）。"""
    if provider == "doubao":
        api_key = "7c45a349-5a95-4885-a7b6-df6ed599ed5e"
        base_url = "https://ark.cn-beijing.volces.com/api/v3"
        default_system_message = "你是豆包，是由字节跳动开发的 AI 人工智能助手"
        model_id = "ep-20260103112951-vxd7j"

    elif provider == "siliconflow":
        api_key = "sk-elravmhyqnplxmmkbpvaoxgogonbidpvyqdqqkgdshssdgyy"
        base_url = "https://api.siliconflow.cn/v1"
        default_system_message = "你是一个AI助手。"
        model_id = model or "deepseek-ai/DeepSeek-R1"

    elif provider == "kimi":
        api_key = "sk-owmdCPn9ix1hiTI9ySevMb1OblJ6Ezl08MKwVE93iE0uqgRF"
        base_url = "https://api.moonshot.cn/v1"
        default_system_message = "你是Kimi，一个由Moonshot AI开发的人工智能助手"
        model_id = "moonshot-v1-8k"

    else:
        return "不支持的LLM提供商"

    sys_msg = system_message if system_message is not None else default_system_message
    return _create_completion(api_key, base_url, model_id, sys_msg, query)
