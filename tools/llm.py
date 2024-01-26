import streamlit as st
import pandas as pd
import json
import requests
import random
import openai


def get_key(is_paykey=False):
    if is_paykey:
        return "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"
    else:
        keys = [
            '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',
            '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',
            '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',
            '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',
            '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',
            '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',
            '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'
            ]
        return random.choice(keys)


def get_baichuan_chat(query):
    url = "https://api.baichuan-ai.com/v1/chat/completions"
    api_key = 'sk-9fa2e18c65e311c1a560fee74a000d60'

    data = {
        "model": "Baichuan2-Turbo",
        "messages": [
            {
                "role": "user",
                "content": query
            }
        ],
        "stream": False
    }
    json_data = json.dumps(data)
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + api_key
    }
    response = requests.post(url, data=json_data, headers=headers, timeout=60)

    if response.status_code == 200:
        resp = json.loads(response.text)
        content = resp['choices'][0]['message']['content']
        print(resp['usage'])
        return content
    else:
        print("请求失败，状态码:", response.status_code)
        print("请求失败，body:", response.text)
        print("请求失败，X-BC-Request-Id:", response.headers.get("X-BC-Request-Id"))


@st.cache_data(ttl="1day",show_spinner="Thinking...")
def get_chatgpt_chat(
        sys_message:str, 
        human_message:str, 
        temperature:int=0, 
        model:str='gpt-3.5-turbo-0613',
        is_paykey:bool=True,
        max_tokens:int=500,
        )->str:
    openai.api_key = get_key(is_paykey)
    completion = openai.ChatCompletion.create(
      model=model,
      messages=[
        {"role": "system", "content": sys_message},
        {"role": "user", "content": human_message}
      ],
      temperature=temperature,
      max_tokens=max_tokens,
    )

    content = completion.choices[0].message['content']
    usage = completion.usage
    print (usage)
    return content


@st.cache_data(ttl="1day",show_spinner="Thinking...")
def get_chatgpt_functioncall(query, functions):
    openai.api_key = "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"
    messages = [{"role": "user", "content": query}]
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-0613",
        messages=messages,
        functions=functions,
        function_call="auto",
    )
    response_message = response["choices"][0]["message"]
    return response_message

