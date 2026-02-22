#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
K线形态识别算法
参考: https://www.notion.so/k-3060e6b664438112b26feea0b5690d2c
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum

class PatternType(Enum):
    REVERSAL = "reversal"           # 反转形态
    CONTINUATION = "continuation"   # 持续形态
    UNDEFINED = "undefined"         # 不确定

@dataclass
class PatternResult:
    """形态识别结果"""
    code: str                       # 形态代码
    name: str                       # 形态名称
    type: PatternType               # 形态类型
    confidence: float               # 置信度 0-1
    description: str                # 描述

class KLinePatternRecognizer:
    """K线形态识别器"""
    
    def __init__(self):
        self.patterns = {
            'hammer': self._recognize_hammer,
            'inverted_hammer': self._recognize_inverted_hammer,
            'doji': self._recognize_doji,
            'engulfing_bull': self._recognize_engulfing_bull,
            'engulfing_bear': self._recognize_engulfing_bear,
            'morning_star': self._recognize_morning_star,
            'evening_star': self._recognize_evening_star,
            'shooting_star': self._recognize_shooting_star,
            'harami': self._recognize_harami,
            'marubozu': self._recognize_marubozu,
            'spinning_top': self._recognize_spinning_top,
            'three_white_soldiers': self._recognize_three_white_soldiers,
            'three_black_crows': self._recognize_three_black_crows,
        }
    
    def _get_body_info(self, open_price: float, close_price: float, high: float, low: float) -> Dict:
        """获取K线实体信息"""
        body = abs(close_price - open_price)
        upper_shadow = high - max(open_price, close_price)
        lower_shadow = min(open_price, close_price) - low
        total_range = high - low if high != low else 0.001  # 避免除零
        
        return {
            'body': body,
            'body_pct': body / total_range if total_range > 0 else 0,
            'upper_shadow': upper_shadow,
            'upper_pct': upper_shadow / total_range if total_range > 0 else 0,
            'lower_shadow': lower_shadow,
            'lower_pct': lower_shadow / total_range if total_range > 0 else 0,
            'is_bull': close_price > open_price,
            'is_bear': close_price < open_price,
            'total_range': total_range
        }
    
    def _recognize_hammer(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        锤子线识别
        特征：下影线长（>=2倍实体），实体小（<=30%总范围），上影线短（<=10%）
        出现在下跌趋势末端
        """
        if len(df) < 1:
            return None
        
        row = df.iloc[-1]
        info = self._get_body_info(row['open'], row['close'], row['high'], row['low'])
        
        # 锤子线条件
        if (info['lower_pct'] >= 0.5 and           # 下影线长
            info['body_pct'] <= 0.3 and            # 实体小
            info['upper_pct'] <= 0.1):             # 上影线短
            
            # 检查是否在下跌趋势（前5天总体下跌）
            trend = self._get_trend(df, 5)
            if trend == 'down':
                return PatternResult(
                    code='hammer',
                    name='锤子线',
                    type=PatternType.REVERSAL,
                    confidence=0.7 + 0.2 * info['lower_pct'],
                    description='下影线长，实体小，出现在下跌趋势末端，可能反转'
                )
        return None
    
    def _recognize_inverted_hammer(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        倒锤子线识别
        特征：上影线长（>=2倍实体），实体小，下影线短
        出现在下跌趋势末端
        """
        if len(df) < 1:
            return None
        
        row = df.iloc[-1]
        info = self._get_body_info(row['open'], row['close'], row['high'], row['low'])
        
        if (info['upper_pct'] >= 0.5 and
            info['body_pct'] <= 0.3 and
            info['lower_pct'] <= 0.1):
            
            trend = self._get_trend(df, 5)
            if trend == 'down':
                return PatternResult(
                    code='inverted_hammer',
                    name='倒锤子线',
                    type=PatternType.REVERSAL,
                    confidence=0.7 + 0.2 * info['upper_pct'],
                    description='上影线长，实体小，出现在下跌趋势末端，可能反转'
                )
        return None
    
    def _recognize_doji(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        十字星识别
        特征：实体极小（<5%总范围），多空力量均衡
        """
        if len(df) < 1:
            return None
        
        row = df.iloc[-1]
        info = self._get_body_info(row['open'], row['close'], row['high'], row['low'])
        
        if info['body_pct'] <= 0.05:
            return PatternResult(
                code='doji',
                name='十字星',
                type=PatternType.UNDEFINED,
                confidence=0.9,
                description='开盘价与收盘价几乎相等，多空力量均衡'
            )
        return None
    
    def _recognize_engulfing_bull(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        看涨吞没识别
        特征：阳线实体完全包住前一日阴线实体
        """
        if len(df) < 2:
            return None
        
        prev = df.iloc[-2]
        curr = df.iloc[-1]
        
        prev_body_high = max(prev['open'], prev['close'])
        prev_body_low = min(prev['open'], prev['close'])
        curr_body_high = max(curr['open'], curr['close'])
        curr_body_low = min(curr['open'], curr['close'])
        
        # 前阴后阳，且后包住前
        if (prev['close'] < prev['open'] and           # 前一日阴线
            curr['close'] > curr['open'] and           # 当日阳线
            curr_body_high >= prev_body_high and       # 上包
            curr_body_low <= prev_body_low):           # 下包
            
            return PatternResult(
                code='engulfing_bull',
                name='看涨吞没',
                type=PatternType.REVERSAL,
                confidence=0.8,
                description='阳线实体完全包住前一日阴线实体，强烈看涨信号'
            )
        return None
    
    def _recognize_engulfing_bear(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        看跌吞没识别
        特征：阴线实体完全包住前一日阳线实体
        """
        if len(df) < 2:
            return None
        
        prev = df.iloc[-2]
        curr = df.iloc[-1]
        
        prev_body_high = max(prev['open'], prev['close'])
        prev_body_low = min(prev['open'], prev['close'])
        curr_body_high = max(curr['open'], curr['close'])
        curr_body_low = min(curr['open'], curr['close'])
        
        if (prev['close'] > prev['open'] and           # 前一日阳线
            curr['close'] < curr['open'] and           # 当日阴线
            curr_body_high >= prev_body_high and
            curr_body_low <= prev_body_low):
            
            return PatternResult(
                code='engulfing_bear',
                name='看跌吞没',
                type=PatternType.REVERSAL,
                confidence=0.8,
                description='阴线实体完全包住前一日阳线实体，强烈看跌信号'
            )
        return None
    
    def _recognize_morning_star(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        早晨之星识别
        特征：三根K线组合 - 长阴 + 小实体（跳空）+ 长阳
        """
        if len(df) < 3:
            return None
        
        d1, d2, d3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        
        # 第一根：长阴线
        d1_info = self._get_body_info(d1['open'], d1['close'], d1['high'], d1['low'])
        d1_is_bear = d1['close'] < d1['open'] and d1_info['body_pct'] > 0.5
        
        # 第二根：小实体（可以是十字星）
        d2_info = self._get_body_info(d2['open'], d2['close'], d2['high'], d2['low'])
        d2_is_small = d2_info['body_pct'] < 0.3
        
        # 第三根：长阳线，收盘价超过第一根中点
        d3_info = self._get_body_info(d3['open'], d3['close'], d3['high'], d3['low'])
        d3_is_bull = d3['close'] > d3['open'] and d3_info['body_pct'] > 0.5
        d3_strong = d3['close'] > (d1['open'] + d1['close']) / 2
        
        if d1_is_bear and d2_is_small and d3_is_bull and d3_strong:
            return PatternResult(
                code='morning_star',
                name='早晨之星',
                type=PatternType.REVERSAL,
                confidence=0.85,
                description='三根K线组合：长阴+小实体+长阳，强烈看涨反转信号'
            )
        return None
    
    def _recognize_evening_star(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        黄昏之星识别
        特征：三根K线组合 - 长阳 + 小实体（跳空）+ 长阴
        """
        if len(df) < 3:
            return None
        
        d1, d2, d3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        
        d1_info = self._get_body_info(d1['open'], d1['close'], d1['high'], d1['low'])
        d1_is_bull = d1['close'] > d1['open'] and d1_info['body_pct'] > 0.5
        
        d2_info = self._get_body_info(d2['open'], d2['close'], d2['high'], d2['low'])
        d2_is_small = d2_info['body_pct'] < 0.3
        
        d3_info = self._get_body_info(d3['open'], d3['close'], d3['high'], d3['low'])
        d3_is_bear = d3['close'] < d3['open'] and d3_info['body_pct'] > 0.5
        d3_strong = d3['close'] < (d1['open'] + d1['close']) / 2
        
        if d1_is_bull and d2_is_small and d3_is_bear and d3_strong:
            return PatternResult(
                code='evening_star',
                name='黄昏之星',
                type=PatternType.REVERSAL,
                confidence=0.85,
                description='三根K线组合：长阳+小实体+长阴，强烈看跌反转信号'
            )
        return None
    
    def _recognize_shooting_star(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        流星线识别
        特征：上影线长，实体小，出现在上涨趋势末端
        """
        if len(df) < 1:
            return None
        
        row = df.iloc[-1]
        info = self._get_body_info(row['open'], row['close'], row['high'], row['low'])
        
        if (info['upper_pct'] >= 0.5 and
            info['body_pct'] <= 0.3 and
            info['lower_pct'] <= 0.1):
            
            trend = self._get_trend(df, 5)
            if trend == 'up':
                return PatternResult(
                    code='shooting_star',
                    name='流星线',
                    type=PatternType.REVERSAL,
                    confidence=0.75,
                    description='上影线长，实体小，出现在上涨趋势末端，可能反转'
                )
        return None
    
    def _recognize_harami(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        孕线识别
        特征：后一日实体完全包含在前一日实体之内
        """
        if len(df) < 2:
            return None
        
        prev = df.iloc[-2]
        curr = df.iloc[-1]
        
        prev_high = max(prev['open'], prev['close'])
        prev_low = min(prev['open'], prev['close'])
        curr_high = max(curr['open'], curr['close'])
        curr_low = min(curr['open'], curr['close'])
        
        if curr_high <= prev_high and curr_low >= prev_low:
            # 前大后小
            prev_body = abs(prev['close'] - prev['open'])
            curr_body = abs(curr['close'] - curr['open'])
            
            if prev_body > curr_body * 2:  # 前一日实体明显大
                pattern_type = PatternType.REVERSAL if prev['close'] > prev['open'] else PatternType.REVERSAL
                return PatternResult(
                    code='harami',
                    name='孕线',
                    type=pattern_type,
                    confidence=0.7,
                    description='后一日实体完全包含在前一日实体之内，趋势可能反转'
                )
        return None
    
    def _recognize_marubozu(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        光头光脚识别
        特征：几乎没有上下影线，趋势强劲
        """
        if len(df) < 1:
            return None
        
        row = df.iloc[-1]
        info = self._get_body_info(row['open'], row['close'], row['high'], row['low'])
        
        if info['body_pct'] >= 0.95:  # 实体占比极高
            is_bull = row['close'] > row['open']
            return PatternResult(
                code='marubozu',
                name='光头光脚',
                type=PatternType.CONTINUATION,
                confidence=0.8,
                description=f'几乎没有上下影线，{"多" if is_bull else "空"}方力量强劲'
            )
        return None
    
    def _recognize_spinning_top(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        纺锤线识别
        特征：上下影线都有，实体很小，多空胶着
        """
        if len(df) < 1:
            return None
        
        row = df.iloc[-1]
        info = self._get_body_info(row['open'], row['close'], row['high'], row['low'])
        
        if (info['body_pct'] <= 0.2 and
            info['upper_pct'] >= 0.2 and
            info['lower_pct'] >= 0.2):
            
            return PatternResult(
                code='spinning_top',
                name='纺锤线',
                type=PatternType.UNDEFINED,
                confidence=0.75,
                description='上下影线都有，实体很小，多空力量胶着，趋势不明'
            )
        return None
    
    def _recognize_three_white_soldiers(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        红三兵识别
        特征：连续三根阳线，收盘价逐步升高
        """
        if len(df) < 3:
            return None
        
        d1, d2, d3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        
        # 三根都是阳线
        if d1['close'] > d1['open'] and d2['close'] > d2['open'] and d3['close'] > d3['open']:
            # 收盘价逐步升高
            if d3['close'] > d2['close'] > d1['close']:
                # 每根都在前一根范围内开盘，但在更高处收盘
                if (d1['open'] < d2['open'] < d1['close'] and
                    d2['open'] < d3['open'] < d2['close']):
                    
                    return PatternResult(
                        code='three_white_soldiers',
                        name='红三兵',
                        type=PatternType.CONTINUATION,
                        confidence=0.8,
                        description='连续三根阳线，收盘价逐步升高，上涨趋势持续'
                    )
        return None
    
    def _recognize_three_black_crows(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        黑三鸦识别
        特征：连续三根阴线，收盘价逐步降低
        """
        if len(df) < 3:
            return None
        
        d1, d2, d3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        
        if d1['close'] < d1['open'] and d2['close'] < d2['open'] and d3['close'] < d3['open']:
            if d3['close'] < d2['close'] < d1['close']:
                if (d1['open'] > d2['open'] > d1['close'] and
                    d2['open'] > d3['open'] > d2['close']):
                    
                    return PatternResult(
                        code='three_black_crows',
                        name='黑三鸦',
                        type=PatternType.CONTINUATION,
                        confidence=0.8,
                        description='连续三根阴线，收盘价逐步降低，下跌趋势持续'
                    )
        return None
    
    def _get_trend(self, df: pd.DataFrame, period: int = 5) -> str:
        """
        判断近期趋势
        Returns: 'up', 'down', 'sideways'
        """
        if len(df) < period:
            return 'sideways'
        
        recent = df.tail(period)
        start_price = recent.iloc[0]['close']
        end_price = recent.iloc[-1]['close']
        change_pct = (end_price - start_price) / start_price if start_price > 0 else 0
        
        if change_pct > 0.03:
            return 'up'
        elif change_pct < -0.03:
            return 'down'
        return 'sideways'
    
    def recognize(self, df: pd.DataFrame) -> Optional[PatternResult]:
        """
        识别K线形态
        按优先级依次识别，返回置信度最高的形态
        """
        if df is None or len(df) < 1:
            return None
        
        # 确保必要的列存在
        required_cols = ['open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required_cols):
            return None
        
        results = []
        for pattern_name, pattern_func in self.patterns.items():
            try:
                result = pattern_func(df)
                if result:
                    results.append(result)
            except Exception as e:
                continue
        
        if not results:
            return None
        
        # 按置信度排序，返回最高的
        results.sort(key=lambda x: x.confidence, reverse=True)
        return results[0]
    
    def recognize_all(self, df: pd.DataFrame) -> List[PatternResult]:
        """识别所有可能的形态"""
        if df is None or len(df) < 1:
            return []
        
        required_cols = ['open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required_cols):
            return []
        
        results = []
        for pattern_name, pattern_func in self.patterns.items():
            try:
                result = pattern_func(df)
                if result:
                    results.append(result)
            except Exception as e:
                continue
        
        # 按置信度排序
        results.sort(key=lambda x: x.confidence, reverse=True)
        return results


# 便捷函数
def recognize_pattern(df: pd.DataFrame) -> Optional[PatternResult]:
    """识别K线形态（便捷函数）"""
    recognizer = KLinePatternRecognizer()
    return recognizer.recognize(df)


def recognize_all_patterns(df: pd.DataFrame) -> List[PatternResult]:
    """识别所有形态（便捷函数）"""
    recognizer = KLinePatternRecognizer()
    return recognizer.recognize_all(df)
