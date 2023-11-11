import pandas as pd
import json
import time
import requests
import streamlit as st


class NotionAgent:
    def __init__(self):
        secret = "secret_081DkYDYw98pMHw99e4OswDObvbT989hkdPytIj7jFN"
        self.headers = {
            "Authorization": "Bearer " + secret,
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28",
        }
        self.pages = {}


    def _retrive_plaintext(self,data):
        def extract_plain_text(item):
            result = []
            if isinstance(item, dict):
                for key, value in item.items():
                    if key == "plain_text":
                        result.append(value)
                    else:
                        result.extend(extract_plain_text(value))
            elif isinstance(item, list):
                for i in item:
                    result.extend(extract_plain_text(i))
            return result
        texts = extract_plain_text(data)
        result = "\n\n".join(texts)
        return result


    def read_page(self,page_id):
        BLOCK_CHILD_URL = "https://api.notion.com/v1/blocks/{block_id}/children"
        url = BLOCK_CHILD_URL.format(block_id=page_id)
        res = requests.get(url, headers=self.headers).json()

        block_results = res['results']
        page_child_blocks = [block for block in block_results if block['type'] == 'child_page']
        page_texts = self._retrive_plaintext(block_results)
        return {'child_blocks': page_child_blocks, 'texts': page_texts}


    def append_block(self,page_id,content_type,content_data):
        BLOCK_URL = 'https://api.notion.com/v1/blocks/{block_id}/children'
        url = BLOCK_URL.format(block_id=page_id)
        data = {
            "children": [
                {
                    "object": "block",
                    "type": content_type,
                    content_type: content_data
                }
            ]
        }
        res = requests.patch(url=url,headers=self.headers,json=data)


    def update_database(self,database_id,properties):
        data = {
            "parent": {"database_id": database_id},
            "properties": properties
        }
        res = requests.post("https://api.notion.com/v1/pages", headers=self.headers, json=data)
        return res









