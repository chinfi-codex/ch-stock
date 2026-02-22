#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
导入 market_data.csv 到 MySQL
"""
import os
import sys
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database.db_manager import get_db

def parse_date(date_str):
    """解析日期格式 2026/02/12 -> 20260212"""
    try:
        dt = datetime.strptime(date_str, '%Y/%m/%d')
        return dt.strftime('%Y%m%d')
    except:
        return None

def parse_pct(value):
    """解析百分比 38.44% -> 38.44"""
    if pd.isna(value):
        return None
    try:
        return float(str(value).replace('%', ''))
    except:
        return None

def import_market_data():
    """导入市场数据"""
    csv_path = 'datas/market_data.csv'
    
    if not os.path.exists(csv_path):
        print(f'错误: 文件不存在 {csv_path}')
        return
    
    # 读取CSV
    df = pd.read_csv(csv_path)
    print(f'读取到 {len(df)} 行数据')
    print(f'列名: {df.columns.tolist()}')
    print()
    
    # 连接数据库
    with get_db() as db:
        success_count = 0
        error_count = 0
        
        for idx, row in df.iterrows():
            try:
                # 解析日期
                trade_date = parse_date(row['日期'])
                if not trade_date:
                    print(f'跳过无效日期: {row["日期"]}')
                    continue
                
                # 构建数据
                data = {
                    'trade_date': trade_date,
                    'up_count': int(row['上涨']) if pd.notna(row['上涨']) else 0,
                    'down_count': int(row['下跌']) if pd.notna(row['下跌']) else 0,
                    'zt_count': int(row['涨停']) if pd.notna(row['涨停']) else 0,
                    'dt_count': int(row['跌停']) if pd.notna(row['跌停']) else 0,
                    'activity_index': parse_pct(row['活跃度']),
                    'total_amount': float(row['成交额']) / 1e8 if pd.notna(row['成交额']) else 0,  # 转为亿元
                }
                
                # 检查是否已存在
                existing = db.query_one(
                    'SELECT id FROM market_activity_daily WHERE trade_date = %s',
                    (trade_date,)
                )
                
                if existing:
                    # 更新
                    db.execute(
                        '''UPDATE market_activity_daily 
                           SET up_count=%s, down_count=%s, zt_count=%s, dt_count=%s, 
                               activity_index=%s, total_amount=%s
                           WHERE trade_date=%s''',
                        (data['up_count'], data['down_count'], data['zt_count'], 
                         data['dt_count'], data['activity_index'], data['total_amount'],
                         trade_date)
                    )
                    print(f'更新 {trade_date}: 涨{data["up_count"]}/跌{data["down_count"]}/涨停{data["zt_count"]}/跌停{data["dt_count"]}')
                else:
                    # 插入
                    db.insert('market_activity_daily', data)
                    print(f'插入 {trade_date}: 涨{data["up_count"]}/跌{data["down_count"]}/涨停{data["zt_count"]}/跌停{data["dt_count"]}')
                
                success_count += 1
                
            except Exception as e:
                error_count += 1
                print(f'错误 [{idx+1}]: {e}')
                import traceback
                traceback.print_exc()
        
        print()
        print('='*60)
        print(f'导入完成: 成功 {success_count}, 失败 {error_count}')
        print('='*60)

if __name__ == '__main__':
    import_market_data()
