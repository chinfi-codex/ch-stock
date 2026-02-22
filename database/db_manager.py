#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据库管理模块 - MySQL连接和操作
"""
import os
import pymysql
from pymysql.cursors import DictCursor
from contextlib import contextmanager
from typing import List, Dict, Any, Optional
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DBManager:
    """MySQL数据库管理类"""
    
    def __init__(self):
        self.host = os.getenv('DB_HOST', '47.90.205.168')
        self.port = int(os.getenv('DB_PORT', '3306'))
        self.user = os.getenv('DB_USER', 'boss_remote')
        self.password = os.getenv('DB_PASSWORD', '0V1-fE4Aui_G8G@XY@_h')
        self.database = os.getenv('DB_NAME', 'stock_review')
        self.connection = None
        self.connect()
    
    def connect(self):
        """建立数据库连接"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database if self.database else None,
                charset='utf8mb4',
                cursorclass=DictCursor,
                autocommit=False
            )
            logger.info(f"✓ 数据库连接成功: {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"✗ 数据库连接失败: {e}")
            raise
    
    def ensure_connection(self):
        """确保连接有效，如果断开则重连"""
        try:
            self.connection.ping(reconnect=True)
        except Exception:
            self.connect()
    
    def execute(self, sql: str, params: tuple = None) -> int:
        """执行SQL语句"""
        self.ensure_connection()
        try:
            with self.connection.cursor() as cursor:
                affected = cursor.execute(sql, params)
                self.connection.commit()
                return affected
        except Exception as e:
            self.connection.rollback()
            logger.error(f"SQL执行错误: {e}\nSQL: {sql[:200]}")
            raise
    
    def query(self, sql: str, params: tuple = None) -> List[Dict]:
        """查询数据"""
        self.ensure_connection()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"查询错误: {e}\nSQL: {sql[:200]}")
            raise
    
    def query_one(self, sql: str, params: tuple = None) -> Optional[Dict]:
        """查询单条数据"""
        self.ensure_connection()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchone()
        except Exception as e:
            logger.error(f"查询错误: {e}\nSQL: {sql[:200]}")
            raise
    
    def insert(self, table: str, data: Dict[str, Any]) -> int:
        """插入单条数据"""
        columns = ', '.join([f"`{k}`" for k in data.keys()])
        placeholders = ', '.join(['%s'] * len(data))
        sql = f"INSERT INTO `{table}` ({columns}) VALUES ({placeholders})"
        return self.execute(sql, tuple(data.values()))
    
    def insert_many(self, table: str, data_list: List[Dict[str, Any]]) -> int:
        """批量插入数据"""
        if not data_list:
            return 0
        
        columns = ', '.join([f"`{k}`" for k in data_list[0].keys()])
        placeholders = ', '.join(['%s'] * len(data_list[0]))
        sql = f"INSERT INTO `{table}` ({columns}) VALUES ({placeholders})"
        
        self.ensure_connection()
        try:
            with self.connection.cursor() as cursor:
                values = [tuple(d.values()) for d in data_list]
                affected = cursor.executemany(sql, values)
                self.connection.commit()
                return affected
        except Exception as e:
            self.connection.rollback()
            logger.error(f"批量插入错误: {e}")
            raise
    
    def upsert(self, table: str, data: Dict[str, Any], unique_keys: List[str]) -> int:
        """插入或更新数据"""
        columns = ', '.join([f"`{k}`" for k in data.keys()])
        placeholders = ', '.join(['%s'] * len(data))
        
        # 构建更新部分（排除唯一键）
        update_cols = [k for k in data.keys() if k not in unique_keys]
        if update_cols:
            update_clause = ', '.join([f"`{k}` = VALUES(`{k}`)" for k in update_cols])
            sql = f"""INSERT INTO `{table}` ({columns}) VALUES ({placeholders})
                     ON DUPLICATE KEY UPDATE {update_clause}"""
        else:
            sql = f"INSERT IGNORE INTO `{table}` ({columns}) VALUES ({placeholders})"
        
        return self.execute(sql, tuple(data.values()))
    
    def upsert_many(self, table: str, data_list: List[Dict[str, Any]], unique_keys: List[str]) -> int:
        """批量插入或更新数据"""
        if not data_list:
            return 0
        
        columns = ', '.join([f"`{k}`" for k in data_list[0].keys()])
        placeholders = ', '.join(['%s'] * len(data_list[0]))
        
        # 构建更新部分（排除唯一键）
        update_cols = [k for k in data_list[0].keys() if k not in unique_keys]
        if update_cols:
            update_clause = ', '.join([f"`{k}` = VALUES(`{k}`)" for k in update_cols])
            sql = f"INSERT INTO `{table}` ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
        else:
            sql = f"INSERT IGNORE INTO `{table}` ({columns}) VALUES ({placeholders})"
        
        self.ensure_connection()
        try:
            with self.connection.cursor() as cursor:
                values = [tuple(d.values()) for d in data_list]
                affected = cursor.executemany(sql, values)
                self.connection.commit()
                return affected
        except Exception as e:
            self.connection.rollback()
            logger.error(f"批量upsert错误: {e}")
            raise
    
    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")


@contextmanager
def get_db():
    """数据库连接上下文管理器"""
    db = DBManager()
    try:
        yield db
    finally:
        db.close()
