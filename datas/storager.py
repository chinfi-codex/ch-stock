#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import Pinecone
import pinecone


def mysql_retriever(query):
    mysql_url = st.secrets['mysql_url']
    engine = create_engine(mysql_url)
    df = pd.read_sql_query(text(query), engine.connect())
    return df


def mysql_storager(dataFrame, dataTable, if_exists='append'):
    mysql_url = st.secrets['mysql_url']
    engine = create_engine(mysql_url)
    try:
        dataFrame.to_sql(name=dataTable, con=engine, if_exists=if_exists, index=False)
        return 'success'
    except Exception as e:
        return e


def mysql_updater(query):
    mysql_url = st.secrets['mysql_url']
    engine = create_engine(mysql_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    session.execute(text(query))
    session.commit()
    session.close()
    

class PineconeConnection:
    def __init__(self):
        PINECONE_API_KEY = st.secrets['PINECONE_API_KEY']
        PINECONE_ENV = st.secrets['PINECONE_ENV']
        pinecone.init(
            api_key=PINECONE_API_KEY,
            environment=PINECONE_ENV
        )
        self.embeddings = OpenAIEmbeddings(openai_api_key=st.secrets['openai_key'])
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000, 
            chunk_overlap=0,
            separators=["\n\n", "\n"]
        )
        self.index_name = "anindex-index"

    def storager(self,data,namespace=None, **kwargs):
        try:
            if kwargs['dataType'] == 'text':
                Pinecone.from_texts(data, self.embeddings, metadatas=kwargs['metadatas'], index_name=self.index_name,namespace=namespace)
            if kwargs['dataType'] == 'docs':
                docs = self.text_splitter.split_documents(data)
                Pinecone.from_documents(docs, self.embeddings, index_name=self.index_name,namespace=namespace)
            return 'success'
        except Exception as e:
            return e

    def retriever(self, query,k=5,_filter=None,namespace=None):
        try:
            vec = Pinecone.from_existing_index(
                self.index_name, 
                self.embeddings,
                namespace=namespace
                )
            result = vec.similarity_search(
                query, 
                filter=_filter,
                k=k)
            return result
        except Exception as e:
            return e
    




