"""
AI工具模块
提供统一的AI分析调用接口
"""

import os
import subprocess
import hashlib
import streamlit as st


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
        # 检查 kimi 命令是否可用
        import shutil
        kimi_path = shutil.which("kimi")
        if kimi_path is None:
            return "AI分析暂时不可用 (kimi-cli 未安装)"
        
        # 设置环境变量解决 Windows 编码问题
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        env['KIMI_OUTPUT_ENCODING'] = 'utf-8'
        
        result = subprocess.run(
            ["kimi", "--print", "--final-message-only", "--output-format", "text"],
            input=prompt,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='ignore',
            timeout=timeout,
            env=env
        )
        
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            error_msg = result.stderr.strip() if result.stderr else f"退出码: {result.returncode}"
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
    # 使用prompt的hash作为缓存键，确保相同prompt使用相同缓存
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
    import re
    
    cleaned = raw_result
    
    # 移除 think 标签
    for prefix in ["<think>", "</thinking>", "<thinking>", "</thinking>"]:
        cleaned = cleaned.replace(prefix, "")
    
    # 提取 TextPart 内容
    text_match = re.search(r"TextPart\([^)]*text='([^']*)',?", cleaned, re.DOTALL)
    if text_match:
        cleaned = text_match.group(1)
    
    # 移除内部格式标签行
    lines = cleaned.split('\n')
    filtered_lines = []
    for line in lines:
        line_stripped = line.strip()
        if any(tag in line_stripped for tag in [
            'TurnBegin(', 'StepBegin(', 'ThinkPart(', 'TextPart(',
            'StatusUpdate(', 'TurnEnd()', 'StepEnd()', 'context_usage=',
            'token_usage=', 'message_id=', 'context_tokens=',
            "type='think'", "type='text'", 'encrypted='
        ]):
            continue
        filtered_lines.append(line)
    
    cleaned = '\n'.join(filtered_lines).strip()
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


def clear_ai_cache():
    """清除AI缓存 - 调用streamlit的缓存清除"""
    st.cache_data.clear()


def get_cache_stats() -> dict:
    """获取缓存统计信息（简化版，streamlit不直接暴露缓存大小）"""
    return {
        "status": "使用st.cache_data自动管理",
        "ttl": "1小时"
    }
