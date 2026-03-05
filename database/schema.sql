-- ============================================
-- 股票复盘系统数据库表结构
-- ============================================

-- 1. 任务执行日志表
CREATE TABLE IF NOT EXISTS job_run_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_date DATE NOT NULL COMMENT '运行日期',
    is_trade_day TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否交易日 0-否 1-是',
    status VARCHAR(20) NOT NULL COMMENT '状态: success/failed/running',
    start_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
    end_time DATETIME NULL COMMENT '结束时间',
    message TEXT NULL COMMENT '日志信息/错误信息',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_trade_date (trade_date),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务执行日志';

-- 2. 交易日历表
CREATE TABLE IF NOT EXISTS trade_calendar (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    cal_date DATE NOT NULL UNIQUE COMMENT '日历日期',
    is_open TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否开盘 0-休市 1-开盘',
    pretrade_date DATE NULL COMMENT '上一个交易日',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_date (cal_date),
    INDEX idx_open (is_open)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='交易日历';

-- 3. 外围资产日线表
CREATE TABLE IF NOT EXISTS external_asset_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_date DATE NOT NULL COMMENT '交易日期',
    asset_code VARCHAR(20) NOT NULL COMMENT '资产代码: BTCUSD/XAUUSD/USDCNY/US10Y',
    asset_name VARCHAR(50) NOT NULL COMMENT '资产名称',
    open_price DECIMAL(18, 8) NULL COMMENT '开盘价',
    high_price DECIMAL(18, 8) NULL COMMENT '最高价',
    low_price DECIMAL(18, 8) NULL COMMENT '最低价',
    close_price DECIMAL(18, 8) NULL COMMENT '收盘价',
    pct_change DECIMAL(8, 4) NULL COMMENT '涨跌幅%',
    volume BIGINT NULL COMMENT '成交量',
    raw_payload JSON NULL COMMENT '原始数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_code (trade_date, asset_code),
    INDEX idx_code_date (asset_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='外围资产日线';

-- 4. 三大指数日线表
CREATE TABLE IF NOT EXISTS index_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_date DATE NOT NULL COMMENT '交易日期',
    ts_code VARCHAR(20) NOT NULL COMMENT '指数代码',
    name VARCHAR(50) NULL COMMENT '指数名称',
    open_price DECIMAL(12, 4) NULL COMMENT '开盘价',
    high_price DECIMAL(12, 4) NULL COMMENT '最高价',
    low_price DECIMAL(12, 4) NULL COMMENT '最低价',
    close_price DECIMAL(12, 4) NULL COMMENT '收盘价',
    pre_close DECIMAL(12, 4) NULL COMMENT '昨收价',
    pct_change DECIMAL(8, 4) NULL COMMENT '涨跌幅%',
    pct_vol DECIMAL(8, 4) NULL COMMENT '振幅%',
    volume BIGINT NULL COMMENT '成交量（手）',
    amount DECIMAL(20, 4) NULL COMMENT '成交额（千元）',
    raw_payload JSON NULL COMMENT '原始数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_code (trade_date, ts_code),
    INDEX idx_code_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='三大指数日线';

-- 5. 市场活跃度日统计表
CREATE TABLE IF NOT EXISTS market_activity_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_date DATE NOT NULL UNIQUE COMMENT '交易日期',
    up_count INT NULL DEFAULT 0 COMMENT '上涨家数',
    down_count INT NULL DEFAULT 0 COMMENT '下跌家数',
    zt_count INT NULL DEFAULT 0 COMMENT '涨停家数',
    dt_count INT NULL DEFAULT 0 COMMENT '跌停家数',
    activity_index DECIMAL(8, 4) NULL COMMENT '活跃度/情绪指数',
    total_amount DECIMAL(20, 4) NULL COMMENT '总成交额（亿元）',
    raw_payload JSON NULL COMMENT '原始数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='市场活跃度日统计';

-- 6. 股票主数据表
CREATE TABLE IF NOT EXISTS stock_master (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL UNIQUE COMMENT 'Tushare代码',
    symbol VARCHAR(20) NOT NULL COMMENT '股票代码',
    name VARCHAR(100) NOT NULL COMMENT '股票名称',
    market VARCHAR(20) NULL COMMENT '市场: 主板/创业板/科创板/北交所',
    exchange VARCHAR(10) NULL COMMENT '交易所: SSE/SZSE/BSE',
    is_st TINYINT(1) DEFAULT 0 COMMENT '是否ST 0-否 1-是',
    is_delist TINYINT(1) DEFAULT 0 COMMENT '是否退市 0-否 1-是',
    is_bse TINYINT(1) DEFAULT 0 COMMENT '是否北交所 0-否 1-是',
    list_date DATE NULL COMMENT '上市日期',
    industry VARCHAR(50) NULL COMMENT '所属行业',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_symbol (symbol),
    INDEX idx_market (market),
    INDEX idx_st (is_st)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票主数据';

-- 7. 全市场股票日指标表
CREATE TABLE IF NOT EXISTS stock_daily_basic (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_date DATE NOT NULL COMMENT '交易日期',
    ts_code VARCHAR(20) NOT NULL COMMENT 'Tushare代码',
    symbol VARCHAR(20) NULL COMMENT '股票代码',
    name VARCHAR(100) NULL COMMENT '股票名称',
    close DECIMAL(12, 4) NULL COMMENT '收盘价',
    pct_chg DECIMAL(8, 4) NULL COMMENT '涨跌幅%',
    turnover_rate DECIMAL(8, 4) NULL COMMENT '换手率%',
    turnover_rate_f DECIMAL(8, 4) NULL COMMENT '换手率(自由流通股)%',
    volume_ratio DECIMAL(8, 4) NULL COMMENT '量比',
    pe_ttm DECIMAL(12, 4) NULL COMMENT '市盈率TTM',
    pe DECIMAL(12, 4) NULL COMMENT '市盈率(LYR)',
    pb DECIMAL(12, 4) NULL COMMENT '市净率',
    ps DECIMAL(12, 4) NULL COMMENT '市销率',
    ps_ttm DECIMAL(12, 4) NULL COMMENT '市销率TTM',
    dv_ratio DECIMAL(12, 4) NULL COMMENT '股息率',
    dv_ttm DECIMAL(12, 4) NULL COMMENT '股息率%',
    total_share BIGINT NULL COMMENT '总股本（万股）',
    float_share BIGINT NULL COMMENT '流通股本（万股）',
    free_share BIGINT NULL COMMENT '自由流通股本（万股）',
    total_mv DECIMAL(20, 4) NULL COMMENT '总市值（万元）',
    circ_mv DECIMAL(20, 4) NULL COMMENT '流通市值（万元）',
    amount DECIMAL(20, 4) NULL COMMENT '成交额（千元）',
    raw_payload JSON NULL COMMENT '原始数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_code (trade_date, ts_code),
    INDEX idx_code_date (ts_code, trade_date),
    INDEX idx_pct (pct_chg),
    INDEX idx_amount (amount),
    INDEX idx_mv (total_mv)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='全市场股票日指标';

-- 8. 股票分组结果表（top100）
CREATE TABLE IF NOT EXISTS stock_group_member (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_date DATE NOT NULL COMMENT '交易日期',
    group_type VARCHAR(30) NOT NULL COMMENT '分组类型: top_100_turnover/top_100_gainers/top_100_losers',
    rank_no INT NOT NULL COMMENT '排名',
    ts_code VARCHAR(20) NOT NULL COMMENT 'Tushare代码',
    symbol VARCHAR(20) NULL COMMENT '股票代码',
    name VARCHAR(100) NULL COMMENT '股票名称',
    close_price DECIMAL(12, 4) NULL COMMENT '收盘价',
    pct_change DECIMAL(8, 4) NULL COMMENT '涨跌幅%',
    amount DECIMAL(20, 4) NULL COMMENT '成交额（千元）',
    total_mv DECIMAL(20, 4) NULL COMMENT '总市值（万元）',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_group_rank (trade_date, group_type, rank_no),
    UNIQUE KEY uk_date_group_code (trade_date, group_type, ts_code),
    INDEX idx_date_type (trade_date, group_type),
    INDEX idx_code (ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票分组结果';

-- 9. 涨幅Top100个股特征表
CREATE TABLE IF NOT EXISTS gainer_feature_stock (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_date DATE NOT NULL COMMENT '交易日期',
    ts_code VARCHAR(20) NOT NULL COMMENT 'Tushare代码',
    symbol VARCHAR(20) NULL COMMENT '股票代码',
    name VARCHAR(100) NULL COMMENT '股票名称',
    rank_no INT NULL COMMENT '涨幅排名',
    close_price DECIMAL(12, 4) NULL COMMENT '收盘价',
    pct_change DECIMAL(8, 4) NULL COMMENT '涨跌幅%',
    amount DECIMAL(20, 4) NULL COMMENT '成交额（千元）',
    total_mv DECIMAL(20, 4) NULL COMMENT '总市值（万元）',
    -- 分层字段
    turnover_bucket VARCHAR(20) NULL COMMENT '成交额分层: lt_5e8/e8_5_to_50/e8_50_to_90/gt_9e9',
    mktcap_bucket VARCHAR(20) NULL COMMENT '市值分层: lt_5e9/e9_5_to_10/e9_10_to_20/e9_20_to_50/gt_5e10',
    board_type VARCHAR(20) NULL COMMENT '板块: main/gem/star',
    -- K线形态
    pattern_code VARCHAR(50) NULL COMMENT 'K线形态代码',
    pattern_name VARCHAR(100) NULL COMMENT 'K线形态名称',
    pattern_version VARCHAR(20) NULL COMMENT '形态算法版本',
    pattern_confidence DECIMAL(5, 4) NULL COMMENT '形态置信度 0-1',
    -- K线数据快照（最近20天）
    kline_snapshot JSON NULL COMMENT 'K线数据快照',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_code (trade_date, ts_code),
    INDEX idx_date_bucket (trade_date, turnover_bucket),
    INDEX idx_date_board (trade_date, board_type),
    INDEX idx_date_pattern (trade_date, pattern_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='涨幅Top100个股特征';

-- 10. 涨幅Top100汇总统计表
CREATE TABLE IF NOT EXISTS gainer_feature_summary (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_date DATE NOT NULL UNIQUE COMMENT '交易日期',
    -- 成交额分层统计
    turnover_lt_5e8 INT DEFAULT 0 COMMENT '成交额<5亿数量',
    turnover_5e8_to_50 INT DEFAULT 0 COMMENT '成交额5-50亿数量',
    turnover_50e8_to_90 INT DEFAULT 0 COMMENT '成交额50-90亿数量',
    turnover_gt_90e8 INT DEFAULT 0 COMMENT '成交额>90亿数量',
    -- 市值分层统计
    mktcap_lt_5e9 INT DEFAULT 0 COMMENT '市值<50亿数量',
    mktcap_5e9_to_10 INT DEFAULT 0 COMMENT '市值50-100亿数量',
    mktcap_10e9_to_20 INT DEFAULT 0 COMMENT '市值100-200亿数量',
    mktcap_20e9_to_50 INT DEFAULT 0 COMMENT '市值200-500亿数量',
    mktcap_gt_50e9 INT DEFAULT 0 COMMENT '市值>500亿数量',
    -- 板块分布统计
    board_main INT DEFAULT 0 COMMENT '主板数量',
    board_gem INT DEFAULT 0 COMMENT '创业板数量',
    board_star INT DEFAULT 0 COMMENT '科创板数量',
    -- 形态分布统计 (JSON格式存储各形态数量)
    pattern_distribution JSON NULL COMMENT '形态分布统计',
    pattern_unclassified INT DEFAULT 0 COMMENT '未归类形态数量',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='涨幅Top100汇总统计';

-- 11. K线形态定义表
CREATE TABLE IF NOT EXISTS kline_pattern_dict (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    pattern_code VARCHAR(50) NOT NULL UNIQUE COMMENT '形态代码',
    pattern_name VARCHAR(100) NOT NULL COMMENT '形态名称',
    pattern_type VARCHAR(50) NULL COMMENT '形态类型: reversal/continuation/undefined',
    description TEXT NULL COMMENT '形态描述',
    rules JSON NULL COMMENT '识别规则',
    version VARCHAR(20) NOT NULL DEFAULT '1.0' COMMENT '版本',
    is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='K线形态定义';

-- 12. 融资融券数据表
CREATE TABLE IF NOT EXISTS margin_trade_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_date DATE NOT NULL UNIQUE COMMENT '交易日期',
    rzye DECIMAL(20, 4) NULL COMMENT '融资余额（元）',
    rqye DECIMAL(20, 4) NULL COMMENT '融券余额（元）',
    rzmre DECIMAL(20, 4) NULL COMMENT '融资买入额（元）',
    rqmcl DECIMAL(20, 4) NULL COMMENT '融券卖出量（股）',
    rzche DECIMAL(20, 4) NULL COMMENT '融资偿还额（元）',
    rz_net_buy DECIMAL(20, 4) NULL COMMENT '融资净买入（元）',
    raw_payload JSON NULL COMMENT '原始数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='融资融券日统计';

-- 13. 创业板PE数据表
CREATE TABLE IF NOT EXISTS gem_pe_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_date DATE NOT NULL UNIQUE COMMENT '交易日期',
    ts_code VARCHAR(20) NOT NULL DEFAULT 'SZ_GEM' COMMENT '代码',
    pe_value DECIMAL(12, 4) NULL COMMENT 'PE值',
    ttm_pe DECIMAL(12, 4) NULL COMMENT 'TTM市盈率',
    ly_pe DECIMAL(12, 4) NULL COMMENT 'LYR市盈率',
    pb_value DECIMAL(12, 4) NULL COMMENT 'PB值',
    total_mv DECIMAL(20, 4) NULL COMMENT '总市值（万元）',
    float_mv DECIMAL(20, 4) NULL COMMENT '流通市值（万元）',
    raw_payload JSON NULL COMMENT '原始数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='创业板PE日统计';

-- 插入K线形态定义
INSERT INTO kline_pattern_dict (pattern_code, pattern_name, pattern_type, description, version) VALUES
('hammer', '锤子线', 'reversal', '下影线较长，实体较小，出现在下跌趋势末端', '1.0'),
('inverted_hammer', '倒锤子线', 'reversal', '上影线较长，实体较小，出现在下跌趋势末端', '1.0'),
('doji', '十字星', 'undefined', '开盘价与收盘价几乎相等，多空力量均衡', '1.0'),
('engulfing_bull', '看涨吞没', 'reversal', '阳线实体完全包住前一日阴线实体', '1.0'),
('engulfing_bear', '看跌吞没', 'reversal', '阴线实体完全包住前一日阳线实体', '1.0'),
('morning_star', '早晨之星', 'reversal', '三根K线组合：长阴+小实体（跳空）+长阳', '1.0'),
('evening_star', '黄昏之星', 'reversal', '三根K线组合：长阳+小实体（跳空）+长阴', '1.0'),
('shooting_star', '流星线', 'reversal', '上影线长，实体小，出现在上涨趋势末端', '1.0'),
('harami', '孕线', 'reversal', '后一日实体完全包含在前一日实体之内', '1.0'),
('marubozu', '光头光脚', 'continuation', '没有上下影线，趋势强劲', '1.0'),
('spinning_top', '纺锤线', 'undefined', '上下影线都有，实体很小，多空胶着', '1.0'),
('three_white_soldiers', '红三兵', 'continuation', '连续三根阳线，收盘价逐步升高', '1.0'),
('three_black_crows', '黑三鸦', 'continuation', '连续三根阴线，收盘价逐步降低', '1.0')
ON DUPLICATE KEY UPDATE pattern_name=VALUES(pattern_name);

