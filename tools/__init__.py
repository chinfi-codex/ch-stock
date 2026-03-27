"""
股票分析工具包
包含业务原子能力：股票数据、爬虫、金融数据等功能模块

注意：通用基础设施已迁移到 infra/ 目录
"""

# K线数据相关
from .kline_data import (
    get_ak_price_df,
    get_ak_interval_price_df,
    get_tushare_price_df,
    get_tushare_weekly_df,
    get_tushare_monthly_df,
    plotK,
    calculate_macd,
)

# 技术分析相关
from .technical_analysis import StockTechnical

# K线形态识别
from .kline_patterns import (
    KLinePatternRecognizer,
    PatternResult,
    PatternType,
    recognize_pattern,
    recognize_all_patterns,
)

# 市场数据相关
from .market_data import (
    get_market_data,
    get_all_stocks,
    get_longhu_data,
    get_dfcf_concept_boards,
    get_concept_board_index,
    get_financing_net_buy_series,
    get_gem_pe_series,
    get_market_history,
    get_market_daily_stats,
    get_market_amount_series,
)

# 金融数据相关
from .financial_data import EconomicIndicators

# 爬虫相关
from .crawlers import (
    cls_telegraphs,
    cninfo_announcement_spider,
)

# 工具函数（业务相关）
from .utils import (
    get_stock_list,
    get_xueqiu_stock_topics,
    weibo_comments,
)

# AI分析原子能力
from .ai_analysis import (
    build_macro_prompt,
    build_market_overview_prompt,
    build_index_analysis_prompt,
    build_stock_classification_prompt,
    run_ai_analysis,
    display_ai_analysis,
    format_series_for_ai,
    format_market_summary_for_ai,
    format_stock_list_for_classification,
)

__all__ = [
    # K线数据
    "get_ak_price_df",
    "get_ak_interval_price_df",
    "get_tushare_price_df",
    "get_tushare_weekly_df",
    "get_tushare_monthly_df",
    "plotK",
    "calculate_macd",
    # 技术分析
    "StockTechnical",
    # K线形态识别
    "KLinePatternRecognizer",
    "PatternResult",
    "PatternType",
    "recognize_pattern",
    "recognize_all_patterns",
    # 市场数据
    "get_market_data",
    "get_all_stocks",
    "get_longhu_data",
    "get_dfcf_concept_boards",
    "get_concept_board_index",
    "get_financing_net_buy_series",
    "get_gem_pe_series",
    "get_market_history",
    "get_market_daily_stats",
    "get_market_amount_series",
    # 金融数据
    "EconomicIndicators",
    # 爬虫
    "cls_telegraphs",
    "cninfo_announcement_spider",
    # 工具函数（业务相关）
    "get_stock_list",
    "get_xueqiu_stock_topics",
    "weibo_comments",
    # AI分析原子能力
    "build_macro_prompt",
    "build_market_overview_prompt",
    "build_index_analysis_prompt",
    "build_stock_classification_prompt",
    "run_ai_analysis",
    "display_ai_analysis",
    "format_series_for_ai",
    "format_market_summary_for_ai",
    "format_stock_list_for_classification",
]
