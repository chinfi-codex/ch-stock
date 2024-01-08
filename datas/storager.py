#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


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




