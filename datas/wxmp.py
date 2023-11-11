import json
import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup
import yaml


class WXSpider:
    @staticmethod
    def wxmp_post_list(fakeid):
        with open("datas/wxmp_config.yaml", "r") as file:
            file_data = file.read()
        config = yaml.safe_load(file_data) 

        headers = {
            "Cookie": config['cookie'],
            "User-Agent": config['user_agent']
        }

        url = "https://mp.weixin.qq.com/cgi-bin/appmsg"
        begin = "0"
        params = {
            "action": "list_ex",
            "begin": begin,
            "count": "5",
            "fakeid": fakeid,
            "type": "9",
            "token": config['token'],
            "lang": "zh_CN",
            "f": "json",
            "ajax": "1"
        }

        resp = requests.get(url, params = params, headers=headers)
        resp_json = resp.json()

        # frequencey control
        ret = resp_json['base_resp']['ret']
        if ret == 200013:
            return f'frequencey control:{ret}'

        # msgs phrase
        if "app_msg_list" in resp_json: 
            df = pd.DataFrame(resp_json['app_msg_list'])
            df = df[['appmsgid','create_time','title','digest','cover','link']]
            df['create_time'] = df['create_time'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d'))
            return df
        else:
            return resp_json['base_resp']['err_msg']

    @staticmethod
    def wxmp_post_parser(url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        resq = requests.get(url, headers=headers)
        soup = BeautifulSoup(resq.text, 'html.parser')
        target_div = soup.find('div', class_='rich_media_area_primary_inner')
        if target_div:
            result = {}
            author = target_div.find('a', id='js_name')
            if author: result['author'] = author.get_text(strip=True)
            content = target_div.find('div', class_='rich_media_content')
            if content: result['content'] = content.get_text(strip=True)
            return result
        else:
            return 'nodata'





