#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
获取每日市场数据并写入 CSV
用于 GitHub Actions 定时任务
"""
import os
import sys
import pandas as pd
import akshare as ak
import tushare as ts
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_tushare_token():
    """获取 Tushare Token"""
    # 优先从环境变量获取（GitHub Actions 使用）
    token = os.environ.get('TUSHARE_TOKEN')
    if token:
        return token
    
    # 其次尝试从 secrets 文件获取（本地开发使用）
    try:
        import streamlit as st
        token = st.secrets.get("tushare_token")
        if token:
            return token
    except Exception:
        pass
    
    return None


def fetch_and_save_market_data():
    """获取市场数据并保存到 CSV"""
    print(f"开始获取市场数据: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 获取 AkShare 市场数据
    try:
        market_data = ak.stock_market_activity_legu()
        print("✓ AkShare 市场数据获取成功")
    except Exception as e:
        print(f"✗ AkShare 数据获取失败: {e}")
        return False
    
    # 提取日期
    stat_date = None
    if '统计日期' in market_data['item'].values:
        stat_date = market_data.loc[market_data['item'] == '统计日期', 'value'].values[0]
        try:
            stat_date = pd.to_datetime(stat_date).strftime('%Y/%m/%d')
        except:
            stat_date = pd.Timestamp.now().strftime('%Y/%m/%d')
    else:
        stat_date = pd.Timestamp.now().strftime('%Y/%m/%d')
    
    print(f"统计日期: {stat_date}")
    
    # 构造数据行
    row = {'日期': stat_date}
    
    # 提取前11个字段（AkShare 标准字段）
    for idx in range(0, min(11, len(market_data))):
        item = str(market_data.iloc[idx]['item'])
        value = market_data.iloc[idx]['value']
        row[item] = value
    
    # 从 Tushare 获取成交额、上涨家数、下跌家数
    total_amount = 0.0
    up_count = None
    down_count = None
    
    token = get_tushare_token()
    if token:
        try:
            pro = ts.pro_api(token)
            trade_date = pd.to_datetime(stat_date).strftime("%Y%m%d")
            
            # 获取当日所有股票的日线数据
            daily = pro.daily(trade_date=trade_date, fields="ts_code,trade_date,amount,pct_chg")
            
            if daily is not None and not daily.empty:
                # 计算成交额（千元）
                if "amount" in daily.columns:
                    total_amount = pd.to_numeric(daily["amount"], errors="coerce").sum()
                    print(f"✓ 成交额计算成功: {total_amount:,.0f} 千元 ({total_amount/1e9:.2f} 万亿)")
                
                # 计算上涨和下跌家数
                if "pct_chg" in daily.columns:
                    daily['pct_chg'] = pd.to_numeric(daily['pct_chg'], errors='coerce')
                    up_count = int((daily['pct_chg'] > 0).sum())
                    down_count = int((daily['pct_chg'] < 0).sum())
                    print(f"✓ 涨跌家数计算成功: 上涨 {up_count}, 下跌 {down_count}")
            else:
                print(f"⚠ Tushare 返回空数据")
        except Exception as e:
            print(f"✗ Tushare 数据获取失败: {e}")
    else:
        print("⚠ 未找到 TUSHARE_TOKEN，跳过 Tushare 数据获取")
    
    # 设置成交额
    row['成交额'] = total_amount
    
    # 回填上涨/下跌数据（如果 AkShare 没有提供）
    ak_up = row.get('上涨')
    ak_down = row.get('下跌')
    
    if (pd.isna(ak_up) or str(ak_up).strip() == '' or ak_up is None) and up_count is not None:
        row['上涨'] = up_count
        print(f"✓ 使用 Tushare 数据回填: 上涨 = {up_count}")
    
    if (pd.isna(ak_down) or str(ak_down).strip() == '' or ak_down is None) and down_count is not None:
        row['下跌'] = down_count
        print(f"✓ 使用 Tushare 数据回填: 下跌 = {down_count}")
    
    # 保存到 CSV
    datas_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'datas')
    os.makedirs(datas_dir, exist_ok=True)
    csv_file = os.path.join(datas_dir, 'market_data.csv')
    
    # 构建表头
    columns = ['日期'] + [str(market_data.iloc[i]['item']) for i in range(0, min(11, len(market_data)))]
    if '成交额' not in columns:
        columns.append('成交额')
    if '上涨' not in columns:
        columns.append('上涨')
    if '下跌' not in columns:
        columns.append('下跌')
    
    try:
        if os.path.exists(csv_file):
            df = pd.read_csv(csv_file)
            
            # 确保所有列都存在
            for col in columns:
                if col not in df.columns:
                    df[col] = ""
            
            # 检查是否已存在该日期
            if not df[df['日期'] == stat_date].empty:
                idx = df.index[df['日期'] == stat_date][0]
                
                # 更新各字段（仅当缺失时）
                for col in ['成交额', '上涨', '下跌']:
                    if col in row and col in df.columns:
                        current_val = df.at[idx, col]
                        if pd.isna(current_val) or str(current_val).strip() == "":
                            df.at[idx, col] = row[col]
                            print(f"✓ 更新 {col}: {row[col]}")
                
                df.to_csv(csv_file, index=False)
                print(f"✓ 已更新现有日期数据: {stat_date}")
            else:
                # 新数据插入首行
                df = pd.concat([pd.DataFrame([row], columns=columns), df], ignore_index=True)
                df.to_csv(csv_file, index=False)
                print(f"✓ 已添加新日期数据: {stat_date}")
        else:
            # 新建文件
            df = pd.DataFrame([row], columns=columns)
            df.to_csv(csv_file, index=False)
            print(f"✓ 已创建新文件并写入数据: {stat_date}")
        
        print(f"✓ 数据已保存到: {csv_file}")
        return True
        
    except Exception as e:
        print(f"✗ 保存 CSV 失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = fetch_and_save_market_data()
    sys.exit(0 if success else 1)
