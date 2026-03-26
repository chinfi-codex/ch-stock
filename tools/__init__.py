"""
股票分析工具包
包含股票数据、爬虫、大模型、金融数据、公告数据、电报数据等功能模块
"""

# K线数据相关（从 stock_data 拆分）
from .kline_data import (
    get_ak_price_df,
    get_ak_interval_price_df,
    get_tushare_price_df,
    get_tushare_weekly_df,
    get_tushare_monthly_df,
    plotK,
    calculate_macd,
)

# 技术分析相关（从 stock_data 拆分，新增K线形态识别）
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

# 大模型相关
from .llm_tools import (
    get_llm_response,
    call_kimi_print,
    clean_ai_output,
    ai_summarize_cached,
)

# 工具函数
from .utils import (
    get_stock_list,
    get_xueqiu_stock_topics,
    weibo_comments,
    scrape_with_jina_reader,
    clean_filename,
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
    # 大模型
    "get_llm_response",
    "call_kimi_print",
    "clean_ai_output",
    "ai_summarize_cached",
    # 工具函数
    "get_stock_list",
    "get_xueqiu_stock_topics",
    "weibo_comments",
    "scrape_with_jina_reader",
    "clean_filename",
]
