import streamlit as st
import pandas as pd
import numpy as np
import json
import requests
import datetime
import re
from io import StringIO


class ChatroomExtracter:
    def __init__(self, file):
        stringio = StringIO(file.getvalue().decode("utf-8"))
        texts = stringio.read()
        _lines = texts.strip().split('\n\n')
        _lines = [line for line in _lines if line !='' and 'sysmsg' not in line]
        self.lines = _lines


    def split_data_by_discussion(self):
        discussion_chunks = []
        current_group = []
        for item in self.datas:
            if "**" in item:
                if current_group:
                    discussion_chunks.append(current_group)
                current_group = [item]
            else:
                current_group.append(item)
        if current_group:
            discussion_chunks.append(current_group)
        return discussion_chunks


    def extract_tags(self):
        def extract_info(line):
            try:
                speaker = line.split("::")[0].split("-")[-1].strip()
                tag = re.search(pattern, line).group(0)
                content = line.split("::")[1].strip()
                return {"speaker": speaker, "tag": tag, "content": content}
            except Exception as e:
                print (line)
                return {"speaker": '', "tag": '', "content": ''}
            

        pattern = r'#(.*?)#'
        tag_lines = [line for line in self.lines if re.search(pattern, line)]
        results = [extract_info(line) for line in tag_lines]
        return results


    def extract_point_by_speaker(self,speaker):
        discussion_chunks = self.split_data_by_discussion()
        speaker_chunks = []
        for chunk in discussion_chunks:
            for line in chunk:
                if f'{speaker}::' in line: 
                    speaker_chunks.append(chunk)
                    break

        output_format = """{
            "time": ...,
            "topic": ...,
            "speaker_opinion": ...
        }
        """
        extract_chunk_prompt = f"""
        提取聊天记录中的:
         - 时间
         - 讨论主题总结
         - 发言人<{speaker}>在讨论中的意见总结
        输出要求: JSON数据
        格式: {output_format}
        """


