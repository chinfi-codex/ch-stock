import streamlit as st
import requests

from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import Chroma
from langchain.document_loaders import PyPDFLoader


def notify_pushplus(title,content,topic):
    url = 'http://www.pushplus.plus/send'
    payload = {
       "token": "349218916e154f048fbafc4a7edd9563",
       "title": title,
       "content": content, 
       "topic": topic,
       "template": "html"
    }
    headers = {
       'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
       'Content-Type': 'application/json'
    }
    resp = requests.post(url, payload, headers)
    return resp


class FileLoader:
    def __init__(self,token_size=500):
        self.text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
            chunk_size=token_size, 
            chunk_overlap=0,
        )

    def pdf_to_docs(self,df_instance):
        docs = []
        for i,row in df_instance.iterrows():
            url = row['adjunctUrl']
            date = row['announcementTime']
            company = row['secName']
            title = row['announcementTitle']

            loader = PyPDFLoader(url)
            ds = loader.load_and_split(self.text_splitter)
            for d in ds:
                metadata = d.metadata
                metadata['source'] = url
                metadata['date'] = date
                metadata['company'] = company
                metadata['title'] = title
            docs += ds
        return docs


class VectorStore:
    def __init__(self, collection):
        self.embeddings = OpenAIEmbeddings(openai_api_key="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")
        self.vectorstore = Chroma(collection,self.embeddings)

    def add_and_query(self, docs, query, stock,k=10):
        try:
            self.vectorstore.add_documents(docs)
            self.vectorstore.get(where={"company": stock})
            docs = self.vectorstore.similarity_search(query,k)
            return docs
        except Exception as e:
            return e

    def add_texts_and_query(self,texts,query,k=20):
        try:
            self.vectorstore.add_texts(texts)
            texts = self.vectorstore.similarity_search(query,k)
            return texts
        except Exception as e:
            return e



