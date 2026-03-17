"""
Moonshot API 封装
直接调用 Moonshot API，绕过 kimi CLI 的编码问题
"""
import os
import requests
from typing import Optional


def call_moonshot(prompt: str, model: str = "moonshot-v1-8k", timeout: int = 60) -> str:
    """
    直接调用 Moonshot API
    
    Args:
        prompt: 提示词
        model: 模型名称
        timeout: 超时时间
    
    Returns:
        AI 生成的文本
    """
    api_key = os.environ.get("MOONSHOT_API_KEY") or os.environ.get("KIMI_API_KEY")
    if not api_key:
        return "AI分析暂时不可用 (未配置 MOONSHOT_API_KEY)"
    
    url = "https://api.moonshot.cn/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    data = {
        "model": model,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 500
    }
    
    try:
        resp = requests.post(url, headers=headers, json=data, timeout=timeout)
        resp.raise_for_status()
        result = resp.json()
        return result["choices"][0]["message"]["content"]
    except Exception as e:
        return f"AI分析暂时不可用"
