#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

# 数据
data = {
    'date': ['2026-02-26', '2026-02-27', '2026-03-02', '2026-03-03', '2026-03-04', '2026-03-05', '2026-03-06', 
             '2026-03-09', '2026-03-10', '2026-03-11', '2026-03-12', '2026-03-13', '2026-03-16', '2026-03-17',
             '2026-03-18', '2026-03-19', '2026-03-20', '2026-03-23', '2026-03-24', '2026-03-25'],
    'close': [4146.6311, 4162.8815, 4182.5909, 4122.6760, 4082.4740, 4108.5670, 4124.1940, 4096.6023, 
              4123.1380, 4133.4331, 4129.1026, 4095.4485, 4084.7858, 4049.9073, 4062.9844, 4006.5523, 
              3957.0527, 3813.2829, 3881.2797, 3931.8375],
    'high': [4152.1926, 4166.2344, 4188.7702, 4197.2280, 4106.0397, 4125.6166, 4129.4647, 4106.5336, 
             4123.9578, 4135.8392, 4141.6488, 4134.0794, 4096.1333, 4108.4000, 4065.3734, 4042.0221, 
             4022.7033, 3906.6220, 3881.4236, 3933.0613],
    'low': [4127.1531, 4128.3600, 4131.3676, 4116.0110, 4055.4125, 4090.6213, 4085.8954, 4052.5454, 
            4098.5869, 4112.7998, 4103.1636, 4086.8469, 4048.0866, 4049.5801, 4023.0295, 3994.1651, 
            3955.7113, 3794.6844, 3807.9937, 3891.8159],
    'volume': [651702826, 682047646, 861579218, 921139170, 765169531, 689746535, 646765760, 797803556, 
               674922194, 707727169, 786151182, 792054764, 732227098, 678771224, 623873102, 667255422, 
               666798387, 804738850, 680622039, 688343626]
}

df = pd.DataFrame(data)
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values('date').reset_index(drop=True)

# 计算移动平均线
df['ma5'] = df['close'].rolling(5).mean()
df['ma10'] = df['close'].rolling(10).mean()

# 计算RSI (14日)
delta = df['close'].diff()
gain = delta.where(delta > 0, 0)
loss = -delta.where(delta < 0, 0)
avg_gain = gain.rolling(14).mean()
avg_loss = loss.rolling(14).mean()
rs = avg_gain / avg_loss
df['rsi'] = 100 - (100 / (1 + rs))

# 计算MACD
exp1 = df['close'].ewm(span=12).mean()
exp2 = df['close'].ewm(span=26).mean()
df['macd'] = exp1 - exp2
df['macd_signal'] = df['macd'].ewm(span=9).mean()
df['macd_hist'] = df['macd'] - df['macd_signal']

# 计算KDJ
low_min = df['low'].rolling(9).min()
high_max = df['high'].rolling(9).max()
rsv = (df['close'] - low_min) / (high_max - low_min) * 100
df['k'] = rsv.ewm(com=2).mean()
df['d'] = df['k'].ewm(com=2).mean()
df['j'] = 3 * df['k'] - 2 * df['d']

# 输出最新值
latest = df.iloc[-1]
prev = df.iloc[-2]
print('=== 最新技术指标 ===')
print(f"日期: {latest['date'].strftime('%Y-%m-%d')}")
print(f"收盘价: {latest['close']:.2f}")
print(f"MA5: {latest['ma5']:.2f}")
print(f"MA10: {latest['ma10']:.2f}")
print(f"RSI(14): {latest['rsi']:.2f}")
print(f"MACD: {latest['macd']:.4f}")
print(f"MACD Signal: {latest['macd_signal']:.4f}")
print(f"MACD Hist: {latest['macd_hist']:.4f}")
print(f"K: {latest['k']:.2f}")
print(f"D: {latest['d']:.2f}")
print(f"J: {latest['j']:.2f}")
print()
print('=== 前一日指标对比 ===')
print(f"MACD Hist变化: {latest['macd_hist']:.4f} vs {prev['macd_hist']:.4f}")
print(f"RSI: {latest['rsi']:.2f} vs {prev['rsi']:.2f}")
print()
print('=== 关键价位 ===')
print(f"近期高点: {df['high'].max():.2f}")
print(f"近期低点: {df['low'].min():.2f}")
print(f"黄金分割0.618: {df['low'].min() + (df['high'].max() - df['low'].min()) * 0.618:.2f}")
print(f"黄金分割0.5: {(df['high'].max() + df['low'].min()) / 2:.2f}")

# 量价分析
print()
print('=== 量价分析 ===')
recent_5d = df.tail(5)
print(f"近5日平均成交量: {recent_5d['volume'].mean():.0f}")
print(f"前5日平均成交量: {df.iloc[-10:-5]['volume'].mean():.0f}")
