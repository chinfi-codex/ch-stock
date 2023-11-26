import streamlit as st
import pandas as pd
import json
import requests
import datetime

from tools.chatroom import ChatroomExtracter
from tools.notion import NotionAgent
from datas.wxmp import WXSpider
from datas.cninfo import hudong_spider, cninfo_announcement_spider
from datas.useful import weibo_comments
from tools.llm import get_chatgpt_chat


today = datetime.datetime.now().date()

st.markdown('#### 聊天记录上传同步')
def notion_data(content):
    data = {
        "rich_text": [
            {
                "type": "text",
                "text": {"content": content}
            }
        ]
    }
    return data

def write_tag_contents(title_page_id, tag_contents):
    notionAgent.append_block(title_page_id,'paragraph',notion_data(str(date)))
    for content in tag_contents:
        notionAgent.append_block(title_page_id,'paragraph',notion_data(content))

chatroom_file = st.file_uploader("上传聊天记录文件",type='md')
if chatroom_file:
    notionAgent = NotionAgent()
    date = chatroom_file.name.split('.')[0]
    extracter = ChatroomExtracter(chatroom_file)
    tag_infos = extracter.extract_tags()

    review_page_url = '91bbb86963914a5db20e9409c0e03b2f'
    tag_contents = []
    for tag_info in tag_infos:
        tag = tag_info['tag']
        content = tag_info['content']
        if tag in ['#早会#','#每日复盘#']:
            tag_contents.append(content)
    if len(tag_contents) > 0:
        write_tag_contents(review_page_url, tag_contents)
        st.write(tag_contents)


    v_page_id = 'b674dd6768a34af3afbb5ddabaa4d752'
    page_children = notionAgent.read_page(v_page_id)['child_blocks']
    for page in page_children:
        title_page_id = page['id']
        title_page = page['child_page']['title']
        
        tag_contents = []
        for tag_info in tag_infos:
            tag = tag_info['tag']
            content = tag_info['content']
            if tag == f'#{title_page}#':
                tag_contents.append(content)
        if len(tag_contents) > 0:
            write_tag_contents(title_page_id, tag_contents)
            st.write(tag_contents)
        else:
            st.write(f"#{title_page}# 无内容")

                

# st.markdown('#### 公众号爬取')
# wxmp_btn = st.button('公众号')
# if wxmp_btn:
#     st.stop()

#     def summary_post(content):
#         summary_prompt = """
#             角色: 文章摘要专家
#             任务: 根据提供的文章内容，提炼找出核心观点和主要信息，进行简洁总结
#             输出要求: markdown格式list列出
#             """
#         return get_chatgpt_chat(summary_prompt,content,model="gpt-3.5-turbo-16k")

#     database_id = 'a9a1f702da11458ca43802318160edac'
#     sources = {
#         "证监会发布": "MzA4NzAzMDgwMw",
#         "求是网": "MjM5NjQ1NjY4MQ",
#         "锐科技": "MzAxMzEzNDAxOQ",
#         "经济日报": "MjM5NjEyMzYxMg",
#     }
#     for key,value in sources.items():
#         posts = WXSpider.wxmp_post_list(f"{value}==")
#         try:
#             for i,row in posts.iterrows():
#                 date = row['create_time']
#                 title = row['title']
#                 link = row['link']
#                 content = WXSpider.wxmp_post_parser(link)['content']
#                 summary = summary_post(content)
#                 properties = {
#                     "公众号": {
#                         "title": [{"text": {"content": key}}]
#                     },
#                     "标题": {
#                         "rich_text": [{"text": {"content": title}}]
#                     },
#                     "日期": {
#                         "rich_text": [{"text": {"content": date}}]
#                     },
#                     "链接": {
#                         "url": link
#                     },
#                     "总结": {
#                         "rich_text": [{"text": {"content": summary}}]
#                     },
#                 }
#                 update_res = NotionAgent().update_database(database_id,properties)
#         except Exception as e:
#             pass       


query = st.text_input('')
from gradio_client import Client
import time
def get_resp(query):
    url = 'http://47.103.122.100:9412'
    #url = "https://referqa.arslantu.xyz"
    client = Client(url)
    response = client.predict(
        query, 
        "ada",
        "123", 
        '',
        api_name="/chat",
    )
    return response


if st.button('test case'):
    df = pd.read_excel('test.xlsx')
    prompts = df['Prompt'].tolist()
    answers = []
    ms = []
    for p in prompts:
        t0 = time.time()
        resp = get_resp(p)
        ms.append(time.time()-t0)
        answers.append(resp)

    df['answer'] = answers
    df['ms'] = ms
    df.to_csv('test_case.csv')
    st.write(answers)
    st.write(ms)


if st.button('fewshot test'):
    df = pd.read_excel('test.xlsx')
    dicts = df.to_dict('records')
    for d in dicts:
        qs = d.get('Prompt')
        chuck = d.get('chuck')
        prompt = f'''
        你的背景知识:{chuck}

        对话要求：
        1. 背景知识是最新的
        2. 使用背景知识回答问题。
        3. 使用对话的风格回答我的问题，答案要和背景知识表述一致。
        4. 回答字数不超过50个字
        我的问题是:"{qs}"
        ''' 
        print(prompt)
        t0 = time.time()
        resp = get_resp(prompt)
        st.write(time.time()-t0)
        st.write(resp)
        


