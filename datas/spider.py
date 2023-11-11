import schedule
import time
import datetime
from . import cninfo, telegraph, storager, wxmp
import pandas as pd
from langchain.document_loaders import PyPDFLoader

import sys
sys.path.append('..') 
from tools.llm import get_chatgpt_chat
from tools.SparkApi import get_spark_chat

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)


def df_drop_duplicated(df1, df2, value_key):
    for i, row in df1.iterrows():
        content = row[value_key]
        if content in df2[value_key].values:
            df1 = df1.drop(i)
    return df1


class NewsSpider:
    # 财联社电报
    def cls_spider(self):
        def llm_tags(content):
            prompt = """
            你是识别新闻信息的专家，根据输入的新闻提取关键词标签。
            要求：提取最关键的5个关键词、记住每个关键词最多不超过5个汉字。输出：只输出关键词字符，关键词以“,”隔开
            """
            try:
                tags = get_spark_chat(prompt+':   '+content)
                tags = tags.replace('，',',')
                return tags
            except Exception as e:
                logger.error(e)
                return ''

        news = telegraph.cls_telegraphs()
        news.columns = ['title', 'content', 'degree', 'tags', 'time', 'date']
        saved_news = storager.mysql_retriever('SELECT title, content, degree, tags, time, date FROM NEWS_CLS ORDER BY date DESC,time DESC LIMIT 500')
        unique_rows= df_drop_duplicated(news, saved_news, 'content')

        if len(unique_rows) > 0:
            logger.info (unique_rows)
            #unique_rows['llm_tags'] = unique_rows['content'].apply(llm_tags)
            mysql_result = storager.mysql_storager(unique_rows, 'NEWS_CLS')
            return mysql_result
        else:
            logger.info('NO NEW DATA')

    def run(self):
        self.cls_spider()
        logger.info ('NEWS SPIDER: CLS JOB FINISHED')


class InfoSpider:
    # 公告：调研活动
    def info_relations_spider(self):
        for pageNum in range(1,20):
            try:
                infos = cninfo.cninfo_announcement_spider(pageNum=pageNum,tabType='relation')
                if len(infos) > 0:
                    mysql_result = storager.mysql_storager(infos, 'CNINFO_RELATIONS')
                    print (f'RELATION SAVE: page{pageNum},{mysql_result}')

                    pc = storager.PineconeConnection()
                    for i,row in infos.iterrows():
                        loader = PyPDFLoader(row['adjunctUrl'])
                        docs = loader.load()
                        for d in docs:
                            d.metadata['source'] = row['adjunctUrl']
                            d.metadata['date'] = row['announcementTime']
                            d.metadata['secName'] = row['secName']
                        pinecone_result = pc.storager(docs, dataType="docs",namespace="relation")
                        logger.info (f'RELATION TO VECTOR: {pinecone_result}')
                else:
                    logger.info('NO NEW DATA')
            except Exception as e:
                logger.warning (e)
                pass

    # 公告：日常经营、股权激励
    def info_operation_spider(self):
        for pageNum in range(1,20):
            try:
                infos = cninfo.cninfo_announcement_spider(pageNum=pageNum,tabType='fulltext',category='category_rcjy_szsh;category_gqjl_szsh')
                filter_keywords = '激励,期权,减持,合同,合作,协议,进展'
                fk = '|'.join(filter_keywords.split(','))
                filtered_infos = infos[infos['announcementTitle'].str.contains(fk)]
                s = storager.mysql_storager(filtered_infos, 'CNINFO_OPERATION')
            except Exception as e:
                print (e)
                pass

    # 公告：业绩预告、季度报告
    ### TOFIX: 业绩公告日期可能是发布日+ 1 ### 
    def info_reports_spider(self):
        for pageNum in range(1,20):
            try:
                infos = cninfo.cninfo_announcement_spider(pageNum=pageNum,tabType='fulltext',category='category_yjygjxz_szsh;category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh')
                s = storager.mysql_storager(infos, 'CNINFO_REPORTS')
            except Exception as e:
                pass

    # 公告： IPO、招股书
    def info_ipo_spider(self):
        for pageNum in range(1,20):
            try:
                infos = cninfo.cninfo_announcement_spider(pageNum=pageNum,tabType='fulltext',category='category_sf_szsh')
                filter_keywords = '招股说明,招股意向'
                fk = '|'.join(filter_keywords.split(','))
                filtered_infos = infos[infos['announcementTitle'].str.contains(fk)]
                s = storager.mysql_storager(filtered_infos, 'CNINFO_IPO')
            except Exception as e:
                pass

    def run(self):
        self.info_relations_spider()
        logger.info ('INFO SPIDER: RELATIONS JOB FINISHED')
        time.sleep(2)
        self.info_operation_spider()
        logger.info ('INFO SPIDER: OPERATION JOB FINISHED')
        time.sleep(2)
        self.info_reports_spider()
        logger.info ('INFO SPIDER: REPORT JOB FINISHED')
        time.sleep(2)
        self.info_ipo_spider()
        logger.info ('INFO SPIDER: IPO JOB FINISHED')


class WxmpSpider:
    def __init__(self):
        q = 'SELECT * FROM WXMP_SOURCES'
        self.sources = storager.mysql_retriever(q)

    def wxmp_spider(self, sources):
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y-%m-%d")

        for i,row in sources.iterrows():
            fakeid = row['fakeid'] + '=='
            posts_resp = wxmp.wxmp_post_list(fakeid)
            if type(posts_resp) == pd.DataFrame:
                posts = posts_resp[posts_resp['create_time'] == yesterday_str]
                s = storager.mysql_storager(posts, 'WXMP_POSTS')
            else:
                logger.info (posts_resp)

    def run(self):
        '''sourceType：1，大v 2，卖方券商  3，行业知识  4，宏观分析'''
        self.wxmp_spider(self.sources)
        logger.info ('WXMP SPIDER: FINISHED')



if __name__ == '__main__':
    spider = NewsSpider()
    spider.run()