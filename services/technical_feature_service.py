#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
技术特征编排服务。
"""

import pandas as pd


def get_features(
    technical,
    stock_id=None,
    include_new_high=True,
    include_sentiment=True,
    include_box_breakout=True,
    **kwargs,
):
    """
    组合 StockTechnical 的多项特征分析。
    """
    result_df = technical.df.copy()

    if include_new_high:
        nh_params = {
            "N_high": kwargs.get("N_high", 60),
            "min_ret0": kwargs.get("min_ret0", 0.02),
            "k_vol": kwargs.get("k_vol", 10),
        }
        nh_result = technical.new_high_analysis(**nh_params)
        if technical.multi_stock and stock_id:
            nh_result = nh_result.xs(stock_id, level=1)
        result_df = result_df.join(
            nh_result[
                [
                    "NH_flag",
                    "NH_strength",
                    "Vol_ratio_NH",
                    "Body_ratio_NH",
                    "Upper_shadow_NH",
                    "MA20_gap_NH",
                ]
            ],
            how="left",
        )

    if include_sentiment:
        sentiment_params = {
            "turnover_thr": kwargs.get("turnover_thr", 0.10),
            "vol_ratio_thr": kwargs.get("vol_ratio_thr", 2.0),
            "range_thr": kwargs.get("range_thr", 0.05),
            "k_vol": kwargs.get("k_vol", 10),
            "calc_score": kwargs.get("calc_score", False),
            "score_weights": kwargs.get("score_weights", None),
        }
        sentiment_result = technical.turnover_sentiment_analysis(**sentiment_params)
        if technical.multi_stock and stock_id:
            sentiment_result = sentiment_result.xs(stock_id, level=1)
        sentiment_cols = [
            col
            for col in [
                "Vol_ratio",
                "Range",
                "Body_ratio",
                "Upper_shadow",
                "is_emotion",
                "Sentiment_score",
            ]
            if col in sentiment_result.columns
        ]
        result_df = result_df.join(sentiment_result[sentiment_cols], how="left")

    if include_box_breakout:
        box_params = {
            "period": kwargs.get("period", "W"),
            "Lw": kwargs.get("Lw", 12),
            "box_width_max": kwargs.get("box_width_max", 0.2),
            "eps": kwargs.get("eps", 0.01),
            "k_vol_w": kwargs.get("k_vol_w", 6),
            "vol_ratio_w_thr": kwargs.get("vol_ratio_w_thr", 1.5),
        }
        box_result = technical.box_breakout_analysis(**box_params)
        if technical.multi_stock and stock_id:
            box_result = box_result.xs(stock_id, level=1)

        box_cols = [
            col
            for col in [
                "Box_high",
                "Box_low",
                "Box_width",
                "is_box",
                "Breakout_up",
                "Breakout_strength",
            ]
            if col in box_result.columns
        ]
        if isinstance(box_result.index, pd.MultiIndex):
            box_result_daily = (
                box_result[box_cols]
                .groupby(level=1)
                .apply(lambda x: x.droplevel(1).resample("D").ffill())
            )
        else:
            box_result_daily = box_result[box_cols].resample("D").ffill()

        if isinstance(result_df.index, pd.MultiIndex):
            if isinstance(box_result_daily.index, pd.MultiIndex):
                box_result_aligned = box_result_daily.reindex(
                    result_df.index, method="ffill"
                )
            else:
                daily_dates = result_df.index.get_level_values(0)
                stock_ids = result_df.index.get_level_values(1)
                box_result_aligned = box_result_daily.reindex(
                    daily_dates, method="ffill"
                )
                box_result_aligned.index = pd.MultiIndex.from_arrays(
                    [daily_dates, stock_ids], names=result_df.index.names
                )
        else:
            box_result_aligned = box_result_daily.reindex(result_df.index, method="ffill")

        result_df = result_df.join(box_result_aligned, how="left")

    if technical.multi_stock and stock_id:
        return result_df.xs(stock_id, level=1)
    return result_df
