import pandas as pd
import streamlit as st
import datetime
import json

from wordcloud import WordCloud
from sklearn.decomposition import NMF
from sklearn.feature_extraction.text import TfidfVectorizer

from tools.llm import get_chatgpt_chat
from tools.tools import VectorStore
from datas.storager import mysql_retriever


class NewsTrends:
    def __init__(self):
        def exclude_tags(tag_str):
            exclude_list = ['A股盘面直播','A股盘面','A股公告速递','环球市场情报','港股动态','美股动态','','A股监管','A股监管动']
            tag_list = tag_str.split(',')
            return not any(tag in exclude_list for tag in tag_list)

        sql = f"""
            SELECT * FROM NEWS_CLS
            WHERE date = CURRENT_DATE
            ORDER BY date DESC, time DESC
            """
        df = mysql_retriever(sql)
        df = df.fillna('')
        df = df[df['tags'].apply(exclude_tags)]
        self.news_df = df

    #词云
    def tag_cloud_img(self,height=300):
        df = self.news_df['llm_tags']
        df.dropna(inplace=True)
        tags = []
        for n in df:
            ns = n.split(',')
            ts = [item for item in ns]
            tags += ts

        tag_counts = pd.Series(tags).value_counts()
        wordcloud = WordCloud(
            width=600, 
            height=height,
            background_color='white',
            font_path='ziyst.ttf'
            )
        cloud_image = wordcloud.generate_from_frequencies(tag_counts)
        image = cloud_image.to_image()
        return image

    #向量搜索
    def vector_query(self,query):
        texts = self.news_df['content'].tolist()
        v = VectorStore('news')
        texts = v.add_texts_and_query(texts,query)
        return texts

    #主题聚类
    def llm_tags_cluster(self):
        tags = self.news_df
        tags['llm_tags'] = tags['llm_tags'].fillna("")
        tags['llm_tags'] = tags['llm_tags'].apply(lambda x: x.replace(' ', '').replace('，',',').split(','))

        # Vectorize the tags
        vectorizer = TfidfVectorizer(analyzer='word', max_features=5000)
        X = vectorizer.fit_transform([' '.join(tag) for tag in tags['llm_tags']])

        # Perform NMF
        nmf = NMF(n_components=12, random_state=42)
        W = nmf.fit_transform(X)
        H = nmf.components_

        # Get the feature names (words/phrases)
        feature_names = vectorizer.get_feature_names_out()

        # Get the top words for each topic
        num_top_words = 10
        topic_summaries = []
        for topic_idx, topic in enumerate(H):
            top_words = [feature_names[i] for i in topic.argsort()[:-num_top_words - 1:-1]]
            topic_summaries.append(' '.join(top_words))

        # Concatenate all tags into a single text
        all_tags_text = ' '.join([' '.join(tag) for tag in tags['llm_tags']])

        # Calculate word counts
        word_counts = []
        for topic_idx, topic in enumerate(H):
            top_words = [feature_names[i] for i in topic.argsort()[:-num_top_words - 1:-1]]
            word_counts.append(sum([all_tags_text.count(word) for word in top_words]))

        # Create a dataframe with the topic summaries and word counts
        topics_df = pd.DataFrame({'Topic': range(1, 13), 'Top Words': topic_summaries, 'Word Count': word_counts})
        return topics_df





 





