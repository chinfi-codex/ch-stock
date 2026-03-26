#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

# 构建数据
data = [
    ['2026-02-24', 4117.4089, 4129.1338, 4131.5499, 4105.9369, 566322249],
    ['2026-02-25', 4147.2305, 4123.7808, 4167.8438, 4122.6987, 724787214],
    ['2026-02-26', 4146.6311, 4151.0680, 4152.1926, 4127.1531, 651702826],
    ['2026-02-27', 4162.8815, 4128.8974, 4166.2344, 4128.3600, 682047646],
    ['2026-03-02', 4182.5909, 4151.8011, 4188.7702, 4131.3676, 861579218],
    ['2026-03-03', 4122.6760, 4189.4079, 4197.2280, 4116.0110, 921139170],
    ['2026-03-04', 4082.4740, 4087.6320, 4106.0397, 4055.4125, 765169531],
    ['2026-03-05', 4108.5670, 4109.7770, 4125.6166, 4090.6213, 689746535],
    ['2026-03-06', 4124.1940, 4085.8954, 4129.4647, 4085.8954, 646765760],
    ['2026-03-09', 4096.6023, 4098.6987, 4106.5336, 4052.5454, 797803556],
    ['2026-03-10', 4123.1380, 4098.5869, 4123.9578, 4098.5869, 674922194],
    ['2026-03-11', 4133.4331, 4123.6663, 4135.8392, 4112.7998, 707727169],
    ['2026-03-12', 4129.1026, 4133.1996, 4141.6488, 4103.1636, 786151182],
    ['2026-03-13', 4095.4485, 4117.5738, 4134.0794, 4086.8469, 792054764],
    ['2026-03-16', 4084.7858, 4092.2491, 4096.1333, 4048.0866, 732227098],
    ['2026-03-17', 4049.9073, 4086.2953, 4108.4000, 4049.5801, 678771224],
    ['2026-03-18', 4062.9844, 4053.3052, 4065.3734, 4023.0295, 623873102],
    ['2026-03-19', 4006.5523, 4028.5420, 4042.0221, 3994.1651, 667255422],
    ['2026-03-20', 3957.0527, 4004.5732, 4022.7033, 3955.7113, 666798387],
    ['2026-03-23', 3813.2829, 3904.9513, 3906.6220, 3794.6844, 804738850],
]

df = pd.DataFrame(data, columns=['date', 'close', 'open', 'high', 'low', 'volume'])
df['date'] = pd.to_datetime(df['date'])
df = df.set_index('date')

# 计算技术指标

# 1. 移动平均线
df['ma5'] = df['close'].rolling(5).mean()
df['ma10'] = df['close'].rolling(10).mean()

# 2. MACD (12,26,9)
exp1 = df['close'].ewm(span=12, adjust=False).mean()
exp2 = df['close'].ewm(span=26, adjust=False).mean()
df['macd'] = exp1 - exp2
df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
df['hist'] = df['macd'] - df['signal']

# 3. RSI (14)
delta = df['close'].diff()
gain = (delta.where(delta > 0, 0)).rolling(14).mean()
loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
rs = gain / loss
df['rsi'] = 100 - (100 / (1 + rs))

# 4. KDJ (9,3,3)
low_min = df['low'].rolling(9).min()
high_max = df['high'].rolling(9).max()
rsv = (df['close'] - low_min) / (high_max - low_min) * 100
df['k'] = rsv.ewm(com=2, adjust=False).mean()
df['d'] = df['k'].ewm(com=2, adjust=False).mean()
df['j'] = 3 * df['k'] - 2 * df['d']

# 5. 布林带 (20,2)
df['ma20'] = df['close'].rolling(20).mean()
df['std20'] = df['close'].rolling(20).std()
df['upper'] = df['ma20'] + 2 * df['std20']
df['lower'] = df['ma20'] - 2 * df['std20']

# 输出最新数据
latest = df.iloc[-1]
prev = df.iloc[-2]

print('='*60)
print('上证指数技术面分析')
print('='*60)
print(f'\n【最新数据】日期: {latest.name.strftime(\"%Y-%m-%d\")}')
print(f'收盘价: {latest[\"close\"]:.2f}')
print(f'涨跌幅: {(latest[\"close\"]/prev[\"close\"]-1)*100:.2f}%')

print('\n【趋势判断】')
high_0302 = 4188.77
low_0323 = 3794.68
print(f'阶段高点: {high_0302:.2f} (2026-03-02)')
print(f'当前点位: {latest[\"close\"]:.2f}')
print(f'阶段跌幅: {(latest[\"close\"]/high_0302-1)*100:.2f}%')

print('\n【移动平均线】')
print(f'MA5:  {latest[\"ma5\"]:.2f}')
print(f'MA10: {latest[\"ma10\"]:.2f}')
print(f'收盘价与MA5: {latest[\"close\"]-latest[\"ma5\"]:.2f}')

print('\n【MACD指标】')
print(f'MACD: {latest[\"macd\"]:.4f}')
print(f'Signal: {latest[\"signal\"]:.4f}')
print(f'Histogram: {latest[\"hist\"]:.4f}')
if latest['hist'] < 0 and latest['hist'] < prev['hist']:
    trend = '空头增强'
elif latest['hist'] < 0:
    trend = '空头减弱'
else:
    trend = '多头'
print(f'趋势: {trend}')

print('\n【RSI指标】')
print(f'RSI(14): {latest[\"rsi\"]:.2f}')
if latest['rsi'] < 30:
    rsi_status = '超卖(<30)'
elif latest['rsi'] < 50:
    rsi_status = '弱势(30-50)'
elif latest['rsi'] < 70:
    rsi_status = '强势(50-70)'
else:
    rsi_status = '超买(>70)'
print(f'状态: {rsi_status}')

print('\n【KDJ指标】')
print(f'K: {latest[\"k\"]:.2f}')
print(f'D: {latest[\"d\"]:.2f}')
print(f'J: {latest[\"j\"]:.2f}')
if latest['k'] < 20:
    kdj_status = '超卖区'
elif latest['k'] < 50:
    kdj_status = '低位'
else:
    kdj_status = '高位'
print(f'状态: {kdj_status}')

print('\n【布林带】')
print(f'上轨: {latest[\"upper\"]:.2f}')
print(f'中轨: {latest[\"ma20\"]:.2f}')
print(f'下轨: {latest[\"lower\"]:.2f}')
print(f'带宽: {latest[\"upper\"]-latest[\"lower\"]:.2f}')
print(f'突破下轨: {\"是\" if latest[\"close\"] < latest[\"lower\"] else \"否\"}')

# 形态识别
print('\n【形态分析】')
print('头部区域: 4188.77 (03/02)')
print('左肩区域: 4167.84 (02/25)')
print('右肩区域: 4135.84 (03/11)')
print('颈线位: ~4100')

# 缺口分析
print('\n【缺口分析】')
print(f'3月23日跳空低开: 前高4022.70 → 今开3904.95')
print('跳空幅度: 118点，属突破性缺口，恐慌盘涌出')

# 支撑压力位
print('\n【关键价位】')
print('压力位: 3906(今日高点) / 3957(上周五) / 4000(心理关口)')
print('支撑位: 3794(今日低点) / 3700(整数关) / 3580(黄金分割0.618)')
