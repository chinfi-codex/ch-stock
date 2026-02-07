"""
大模型工具模块
包含多种LLM提供商的统一调用接口
"""

import streamlit as st
import openai


@st.cache_data(ttl="1day", show_spinner="Thinking...")
def get_llm_response(query, provider="doubao", model=None, system_message=None):
    """
    统一的LLM调用接口
    
    参数:
        query: 用户查询内容
        provider: LLM提供商，可选值: "doubao", "siliconflow", "kimi"
        model: 模型名称，如果为None则使用默认模型
        system_message: 系统提示信息，如果为None则使用默认提示
    
    返回:
        LLM的回复内容
    """
    if provider == "doubao":
        client = openai.OpenAI(
            api_key="7c45a349-5a95-4885-a7b6-df6ed599ed5e",
            base_url="https://ark.cn-beijing.volces.com/api/v3",
        )
        
        default_system_message = "你是豆包，是由字节跳动开发的 AI 人工智能助手"
        model_id = "ep-20260103112951-vxd7j"
        
    elif provider == "siliconflow":
        client = openai.OpenAI(
            api_key="sk-elravmhyqnplxmmkbpvaoxgogonbidpvyqdqqkgdshssdgyy",
            base_url="https://api.siliconflow.cn/v1",
        )
        
        default_system_message = "你是一个AI助手。"
        model_id = model or "deepseek-ai/DeepSeek-R1"
        
    elif provider == "kimi":
        client = openai.OpenAI(
            api_key='sk-owmdCPn9ix1hiTI9ySevMb1OblJ6Ezl08MKwVE93iE0uqgRF',
            base_url="https://api.moonshot.cn/v1",
        )
        
        default_system_message = "你是Kimi，一个由Moonshot AI开发的人工智能助手"
        model_id = "moonshot-v1-8k"
        
    else:
        return "不支持的LLM提供商"
    
    # 使用提供的系统消息或默认消息
    sys_msg = system_message if system_message is not None else default_system_message
    
    # 创建聊天完成请求
    completion = client.chat.completions.create(
        model=model_id,
        messages=[
            {"role": "system", "content": sys_msg},
            {"role": "user", "content": query},
        ],
        temperature=0,
        top_p=0.8,
    )
    
    # 返回结果
    if hasattr(completion, 'choices') and len(completion.choices) > 0:
        return completion.choices[0].message.content
    else:
        return "请求失败，请检查API密钥或网络连接" 