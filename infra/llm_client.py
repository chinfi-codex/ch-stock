"""
大模型客户端模块
包含多种LLM提供商的统一调用接口
"""

import os
import re
import subprocess
import hashlib
import streamlit as st
import openai
from infra.config import get_llm_api_key


def _get_api_key(provider: str) -> str:
    """????????? LLM API Key?"""
    return get_llm_api_key(provider)


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

    sys_msg = (
        system_message
        if system_message is not None
        else config["default_system_message"]
    )
    return _create_completion(
        api_key, config["base_url"], config["default_model"], sys_msg, query
    )


@st.cache_data(ttl="1h", show_spinner=False)
def _cached_kimi_call(prompt_hash: str, prompt: str, timeout: int = 60) -> str:
    """
    内部缓存函数 - 使用st.cache_data自动管理缓存

    参数:
        prompt_hash: 提示词的hash值，用于缓存键
        prompt: 实际的提示词内容
        timeout: 超时时间（秒）

    返回:
        AI分析结果文本
    """
    try:
        import shutil

        kimi_path = shutil.which("kimi")
        if kimi_path is None:
            return "AI分析暂时不可用 (kimi-cli 未安装)"

        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["KIMI_OUTPUT_ENCODING"] = "utf-8"

        result = subprocess.run(
            ["kimi", "--print", "--final-message-only", "--output-format", "text"],
            input=prompt,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=timeout,
            env=env,
        )

        if result.returncode == 0:
            return result.stdout.strip()
        else:
            error_msg = (
                result.stderr.strip()
                if result.stderr
                else f"退出码: {result.returncode}"
            )
            print(f"kimi-cli 执行失败: {error_msg}")
            return "AI分析暂时不可用"

    except subprocess.TimeoutExpired:
        return "AI分析暂时不可用 (请求超时)"
    except Exception as e:
        print(f"kimi-cli 调用异常: {str(e)}")
        return "AI分析暂时不可用"


def call_kimi_print(prompt: str, cache_key: str = "", timeout: int = 60) -> str:
    """
    调用 kimi-cli --print 模式获取AI分析结果

    参数:
        prompt: 提示词
        cache_key: 缓存键（用于识别，实际缓存使用prompt的hash）
        timeout: 超时时间（秒），默认60秒

    返回:
        AI分析结果文本

    示例:
        result = call_kimi_print("分析这只股票", "stock_analysis_000001")
    """
    prompt_hash = hashlib.md5(prompt.encode()).hexdigest()
    return _cached_kimi_call(prompt_hash, prompt, timeout)


def clean_ai_output(raw_result: str) -> str:
    """
    清理AI输出结果，移除think标签和内部格式

    参数:
        raw_result: 原始AI输出

    返回:
        清理后的文本
    """
    cleaned = raw_result

    # 移除 think 标签
    for prefix in ["<think>", "</thinking>", "<thinking>", "</thinking>"]:
        cleaned = cleaned.replace(prefix, "")

    # 提取 TextPart 内容
    text_match = re.search(r"TextPart\([^)]*text='([^']*)',?", cleaned, re.DOTALL)
    if text_match:
        cleaned = text_match.group(1)

    # 移除内部格式标签行
    lines = cleaned.split("\n")
    filtered_lines = []
    for line in lines:
        line_stripped = line.strip()
        if any(
            tag in line_stripped
            for tag in [
                "TurnBegin(",
                "StepBegin(",
                "ThinkPart(",
                "TextPart(",
                "StatusUpdate(",
                "TurnEnd()",
                "StepEnd()",
                "context_usage=",
                "token_usage=",
                "message_id=",
                "context_tokens=",
                "type='think'",
                "type='text'",
                "encrypted=",
            ]
        ):
            continue
        filtered_lines.append(line)

    cleaned = "\n".join(filtered_lines).strip()
    cleaned = cleaned.replace("\\n", "\n")
    cleaned = cleaned.replace("\\'", "'")

    return cleaned


def ai_summarize_cached(text: str, prompt_template: str, cache_key: str) -> str:
    """
    带缓存的AI总结函数 - 自动使用st.cache_data缓存

    参数:
        text: 要总结的文本
        prompt_template: 提示词模板，需包含{text}占位符
        cache_key: 缓存键（用于日志/调试，实际缓存基于内容hash）

    返回:
        清理后的AI总结结果
    """
    full_prompt = prompt_template.format(text=text)
    raw_result = call_kimi_print(full_prompt, cache_key)
    return clean_ai_output(raw_result)
