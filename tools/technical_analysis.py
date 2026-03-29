#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
技术分析模块
包含技术指标计算（StockTechnical类）和K线形态识别集成
"""

import numpy as np
import pandas as pd

from .kline_patterns import (
    KLinePatternRecognizer,
    PatternResult,
    PatternType,
    recognize_all_patterns,
    recognize_pattern,
)


class StockTechnical:
    """
    股票技术分析类

    对股票K线数据进行技术分析，包括：
    - 多周期数据预处理（周线、月线）
    - 新高后涨势指标（New High）
    - 高频换手短线情绪指标（High Turnover Sentiment）
    - 周/月线箱体突破形态（Box Breakout）
    - K线形态识别（独立方法）
    """

    def __init__(self, df, date_col="date", stock_id_col=None):
        """
        初始化技术分析类

        参数:
            df: DataFrame，包含K线数据
                必需字段: open, high, low, close, volume, turnover
                可选字段: float_shares, amount
                索引: [date, stock_id] 或按 stock_id 分组后按 date 排序
            date_col: 日期列名（如果不在索引中）
            stock_id_col: 股票ID列名（如果不在索引中，且有多只股票）
        """
        self.df = df.copy()

        # 处理索引和列
        if isinstance(self.df.index, pd.MultiIndex):
            # MultiIndex: [date, stock_id]
            self.df = self.df.sort_index()
            self.multi_stock = True
        elif stock_id_col and stock_id_col in self.df.columns:
            # 单索引，但有stock_id列
            if date_col in self.df.columns:
                self.df[date_col] = pd.to_datetime(self.df[date_col])
                self.df = self.df.set_index([date_col, stock_id_col]).sort_index()
                self.multi_stock = True
            else:
                self.df.index = pd.to_datetime(self.df.index)
                self.multi_stock = True
        else:
            # 单只股票
            if date_col in self.df.columns:
                self.df[date_col] = pd.to_datetime(self.df[date_col])
                self.df = self.df.set_index(date_col).sort_index()
            else:
                self.df.index = pd.to_datetime(self.df.index)
            self.multi_stock = False

        # 验证必需字段
        required_cols = ["open", "high", "low", "close", "volume", "turnover"]
        missing_cols = [col for col in required_cols if col not in self.df.columns]
        if missing_cols:
            raise ValueError(f"缺少必需字段: {missing_cols}")

        # 计算换手率（如果没有）
        if "turnover" not in self.df.columns or self.df["turnover"].isna().all():
            if "float_shares" in self.df.columns:
                self.df["turnover"] = self.df["volume"] / self.df["float_shares"]
            else:
                raise ValueError("缺少 turnover 字段，且无法从 float_shares 计算")

        # 初始化多周期数据
        self.weekly_df = None
        self.monthly_df = None

        # 初始化K线形态识别器
        self._pattern_recognizer = KLinePatternRecognizer()

    def _groupby_stock(self, func, *args, **kwargs):
        """对多只股票应用函数"""
        if self.multi_stock:
            return self.df.groupby(
                level=1 if isinstance(self.df.index, pd.MultiIndex) else "stock_id"
            ).apply(lambda x: func(x, *args, **kwargs))
        else:
            return func(self.df, *args, **kwargs)

    def aggregate_weekly(self):
        """聚合周线数据"""

        def _agg_weekly(group_df):
            group_df = group_df.copy()
            is_multi = isinstance(group_df.index, pd.MultiIndex)

            if is_multi:
                # 保存股票ID
                stock_id = group_df.index.get_level_values(1)[0]
                dates = pd.to_datetime(group_df.index.get_level_values(0))
                group_df.index = dates
            else:
                group_df.index = pd.to_datetime(group_df.index)

            # 构建聚合字典
            agg_dict = {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "turnover": "sum",
            }
            if "float_shares" in group_df.columns:
                agg_dict["float_shares"] = "last"
            if "amount" in group_df.columns:
                agg_dict["amount"] = "sum"

            weekly = group_df.resample("W").agg(agg_dict)
            # 重命名列（float_shares 不重命名）
            rename_dict = {
                col: f"{col}_w" for col in weekly.columns if col != "float_shares"
            }
            weekly = weekly.rename(columns=rename_dict)

            # 如果是多股票，恢复MultiIndex
            if is_multi:
                weekly.index = pd.MultiIndex.from_arrays(
                    [weekly.index, [stock_id] * len(weekly)], names=["date", "stock_id"]
                )

            return weekly

        if self.multi_stock:
            self.weekly_df = self._groupby_stock(_agg_weekly)
        else:
            self.weekly_df = _agg_weekly(self.df)
        return self.weekly_df

    def aggregate_monthly(self):
        """聚合月线数据"""

        def _agg_monthly(group_df):
            group_df = group_df.copy()
            is_multi = isinstance(group_df.index, pd.MultiIndex)

            if is_multi:
                # 保存股票ID
                stock_id = group_df.index.get_level_values(1)[0]
                dates = pd.to_datetime(group_df.index.get_level_values(0))
                group_df.index = dates
            else:
                group_df.index = pd.to_datetime(group_df.index)

            # 构建聚合字典
            agg_dict = {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "turnover": "sum",
            }
            if "float_shares" in group_df.columns:
                agg_dict["float_shares"] = "last"
            if "amount" in group_df.columns:
                agg_dict["amount"] = "sum"

            monthly = group_df.resample("M").agg(agg_dict)
            # 重命名列（float_shares 不重命名）
            rename_dict = {
                col: f"{col}_m" for col in monthly.columns if col != "float_shares"
            }
            monthly = monthly.rename(columns=rename_dict)

            # 如果是多股票，恢复MultiIndex
            if is_multi:
                monthly.index = pd.MultiIndex.from_arrays(
                    [monthly.index, [stock_id] * len(monthly)],
                    names=["date", "stock_id"],
                )

            return monthly

        if self.multi_stock:
            self.monthly_df = self._groupby_stock(_agg_monthly)
        else:
            self.monthly_df = _agg_monthly(self.df)
        return self.monthly_df

    def new_high_analysis(self, N_high=60, min_ret0=0.02, k_vol=10):
        """
        新高后涨势指标分析

        参数:
            N_high: 窗口天数，默认60
            min_ret0: 当日最小涨幅，默认0.02 (2%)
            k_vol: 量能均值窗口，默认10

        返回:
            DataFrame，包含NH_flag和各项特征
        """

        def _calc_new_high(group_df):
            df = group_df.copy()
            df = df.sort_index()

            # 计算滚动最高价
            df["rolling_max_close"] = (
                df["close"].shift(1).rolling(window=N_high, min_periods=1).max()
            )

            # 计算当日涨幅
            df["ret0"] = df["close"] / df["close"].shift(1) - 1

            # 新高标志
            df["NH_flag"] = (df["close"] > df["rolling_max_close"]) & (
                df["ret0"] >= min_ret0
            )

            # 新高当日特征（仅在NH_flag为True时计算）
            df["NH_strength"] = np.where(
                df["NH_flag"],
                (df["close"] - df["rolling_max_close"]) / df["rolling_max_close"],
                np.nan,
            )

            # 量能比率
            df["vol_mean"] = (
                df["volume"].shift(1).rolling(window=k_vol, min_periods=1).mean()
            )
            df["Vol_ratio_NH"] = np.where(
                df["NH_flag"], df["volume"] / df["vol_mean"], np.nan
            )

            # 实体比率
            df["Body_ratio_NH"] = np.where(
                df["NH_flag"],
                abs(df["close"] - df["open"]) / (df["high"] - df["low"] + 1e-8),
                np.nan,
            )

            # 上影线比率
            df["Upper_shadow_NH"] = np.where(
                df["NH_flag"],
                (df["high"] - df[["close", "open"]].max(axis=1))
                / (df["high"] - df["low"] + 1e-8),
                np.nan,
            )

            # MA20
            df["MA20"] = df["close"].rolling(window=20, min_periods=1).mean()
            df["MA20_gap_NH"] = np.where(
                df["NH_flag"], df["close"] / df["MA20"] - 1, np.nan
            )

            return df

        result = self._groupby_stock(_calc_new_high)
        return result

    def turnover_sentiment_analysis(
        self,
        turnover_thr=0.10,
        vol_ratio_thr=2.0,
        range_thr=0.05,
        k_vol=10,
        calc_score=False,
        score_weights=None,
    ):
        """
        高频换手短线情绪指标分析

        参数:
            turnover_thr: 高换手阈值，默认0.10 (10%)
            vol_ratio_thr: 量能放大阈值，默认2.0
            range_thr: 波动幅度阈值，默认0.05 (5%)
            k_vol: 量能均值窗口，默认10
            calc_score: 是否计算情绪打分，默认False
            score_weights: 打分权重 [w1, w2, w3, w4]，默认None

        返回:
            DataFrame，包含各项情绪指标
        """

        def _calc_sentiment(group_df):
            df = group_df.copy()
            df = df.sort_index()

            # 基础指标
            df["Vol_ratio"] = (
                df["volume"]
                / df["volume"].shift(1).rolling(window=k_vol, min_periods=1).mean()
            )
            df["Range"] = (df["high"] - df["low"]) / (df["close"].shift(1) + 1e-8)
            df["Body_ratio"] = abs(df["close"] - df["open"]) / (
                df["high"] - df["low"] + 1e-8
            )
            df["Upper_shadow"] = (df["high"] - df[["close", "open"]].max(axis=1)) / (
                df["high"] - df["low"] + 1e-8
            )

            # 情绪事件
            df["is_emotion"] = (
                (df["turnover"] > turnover_thr)
                & (df["Vol_ratio"] > vol_ratio_thr)
                & (df["Range"] > range_thr)
            )

            # 情绪打分（可选）
            if calc_score:
                if score_weights is None:
                    score_weights = [0.3, 0.3, 0.3, -0.1]  # 默认权重

                # Z-score标准化
                for col in ["turnover", "Vol_ratio", "Range", "Upper_shadow"]:
                    mean_val = df[col].mean()
                    std_val = df[col].std()
                    df[f"z_{col}"] = (df[col] - mean_val) / (std_val + 1e-8)

                df["Sentiment_score"] = (
                    score_weights[0] * df["z_turnover"]
                    + score_weights[1] * df["z_Vol_ratio"]
                    + score_weights[2] * df["z_Range"]
                    + score_weights[3] * df["z_Upper_shadow"]
                )

            return df

        result = self._groupby_stock(_calc_sentiment)
        return result

    def box_breakout_analysis(
        self,
        period="W",
        Lw=12,
        box_width_max=0.2,
        eps=0.01,
        k_vol_w=6,
        vol_ratio_w_thr=1.5,
    ):
        """
        周/月线箱体突破形态分析

        参数:
            period: 周期类型，'W'（周线）或'M'（月线），默认'W'
            Lw: 箱体回看周/月数，默认12
            box_width_max: 箱体最大宽度，默认0.2 (20%)
            eps: 突破高点的最小超出比例，默认0.01 (1%)
            k_vol_w: 周/月量能均值窗口，默认6
            vol_ratio_w_thr: 突破周/月量能放大阈值，默认1.5

        返回:
            DataFrame，包含箱体和突破指标
        """
        # 确保已聚合周/月线数据
        if period == "W":
            if self.weekly_df is None:
                self.aggregate_weekly()
            period_df = self.weekly_df.copy()
            high_col = "high_w"
            low_col = "low_w"
            close_col = "close_w"
            volume_col = "volume_w"
        elif period == "M":
            if self.monthly_df is None:
                self.aggregate_monthly()
            period_df = self.monthly_df.copy()
            high_col = "high_m"
            low_col = "low_m"
            close_col = "close_m"
            volume_col = "volume_m"
        else:
            raise ValueError("period 必须是 'W' 或 'M'")

        def _calc_box_breakout(group_df):
            df = group_df.copy()
            df = df.sort_index()

            # 箱体识别
            df["Box_high"] = df[high_col].rolling(window=Lw, min_periods=1).max()
            df["Box_low"] = df[low_col].rolling(window=Lw, min_periods=1).min()
            df["Box_width"] = df["Box_high"] / df["Box_low"] - 1
            df["is_box"] = df["Box_width"] < box_width_max

            # 箱体突破事件
            df["Box_high_prev"] = df["Box_high"].shift(1)
            df["avg_vol_box"] = (
                df[volume_col].shift(1).rolling(window=Lw, min_periods=1).mean()
            )
            df["Vol_ratio_w"] = df[volume_col] / (df["avg_vol_box"] + 1e-8)

            df["Breakout_up"] = (
                df["is_box"].shift(1)
                & (df[close_col] > df["Box_high_prev"] * (1 + eps))
                & (df["Vol_ratio_w"] > vol_ratio_w_thr)
            )

            df["Breakout_strength"] = np.where(
                df["Breakout_up"],
                (df[close_col] - df["Box_high_prev"]) / df["Box_high_prev"],
                np.nan,
            )

            return df

        if self.multi_stock:
            result = period_df.groupby(
                level=1 if isinstance(period_df.index, pd.MultiIndex) else "stock_id"
            ).apply(_calc_box_breakout)
        else:
            result = _calc_box_breakout(period_df)

        return result
    # =========================================================================
    # K线形态识别方法（独立方法）
    # =========================================================================

    def recognize_pattern(self, df: pd.DataFrame = None) -> PatternResult | None:
        """
        识别K线形态（独立方法）

        参数:
            df: 可选，指定要分析的DataFrame，默认使用self.df

        返回:
            PatternResult | None: 识别到的形态结果，如果没有识别到则返回None
        """
        if df is None:
            df = self.df
        return self._pattern_recognizer.recognize(df)

    def recognize_all_patterns(self, df: pd.DataFrame = None) -> list[PatternResult]:
        """
        识别所有可能的K线形态（独立方法）

        参数:
            df: 可选，指定要分析的DataFrame，默认使用self.df

        返回:
            list[PatternResult]: 所有识别到的形态结果列表，按置信度排序
        """
        if df is None:
            df = self.df
        return self._pattern_recognizer.recognize_all(df)

    def get_pattern_summary(self, df: pd.DataFrame = None) -> dict:
        """
        获取K线形态识别摘要（独立方法）

        参数:
            df: 可选，指定要分析的DataFrame，默认使用self.df

        返回:
            dict: 形态识别摘要信息
        """
        patterns = self.recognize_all_patterns(df)

        if not patterns:
            return {
                "has_pattern": False,
                "top_pattern": None,
                "pattern_count": 0,
                "patterns": [],
            }

        top_pattern = patterns[0]
        return {
            "has_pattern": True,
            "top_pattern": {
                "code": top_pattern.code,
                "name": top_pattern.name,
                "type": top_pattern.type.value,
                "confidence": top_pattern.confidence,
                "description": top_pattern.description,
            },
            "pattern_count": len(patterns),
            "patterns": [
                {
                    "code": p.code,
                    "name": p.name,
                    "type": p.type.value,
                    "confidence": p.confidence,
                    "description": p.description,
                }
                for p in patterns
            ],
        }
