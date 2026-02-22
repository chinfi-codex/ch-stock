# 股票每日复盘系统

## 系统架构

```
交易日数据爬取 → 数据落库(MySQL) → 数据特征计算 → 前端展示
```

## 项目结构

```
ch-stock/
├── database/
│   ├── schema.sql          # 数据库表结构定义
│   ├── db_manager.py       # MySQL连接管理
│   └── init_db.py          # 数据库初始化脚本
├── data_updater/
│   ├── __init__.py
│   └── trade_day_updater.py # 数据更新主逻辑
├── tools/
│   └── kline_patterns.py    # K线形态识别算法
├── pages/
│   └── 10-每日复盘.py       # Streamlit前端页面
└── README_REVIEW.md         # 本文档
```

## 快速开始

### 1. 配置环境变量

在项目根目录创建 `.env` 文件或在系统环境变量中设置：

```bash
# MySQL数据库配置
DB_HOST=47.90.205.168
DB_PORT=3306
DB_USER=boss_remote
DB_PASSWORD=0V1-fE4Aui_G8G@XY@_h
DB_NAME=stock_review

# Tushare API Token (必需)
TUSHARE_TOKEN=your_tushare_token_here

# Alpha Vantage API Key (可选，用于外盘数据)
ALPHAVANTAGE_API_KEY=your_alphavantage_key_here
```

### 2. 初始化数据库

```bash
python database/init_db.py
```

### 3. 运行数据更新

手动运行一次数据更新（以2025年2月20日为例）：

```bash
python -c "from data_updater.trade_day_updater import run_daily_update; print(run_daily_update('20250220'))"
```

### 4. 启动前端页面

```bash
streamlit run app.py
```

然后访问 `http://localhost:8501`，在侧边栏选择 "每日复盘" 页面。

## 数据库表说明

| 表名 | 说明 | 更新频率 |
|------|------|---------|
| job_run_log | 任务执行日志 | 每次运行 |
| trade_calendar | 交易日历 | 初始化时 |
| external_asset_daily | 外围资产日线 | 每日 |
| index_daily | 三大指数日线 | 每日 |
| market_activity_daily | 市场活跃度统计 | 每日 |
| stock_daily_basic | 全市场股票日指标 | 每日 |
| stock_group_member | 股票分组结果 | 每日 |
| gainer_feature_stock | 涨幅Top100个股特征 | 每日 |
| gainer_feature_summary | 涨幅Top100汇总统计 | 每日 |
| margin_trade_daily | 融资融券数据 | 每日 |
| gem_pe_daily | 创业板PE数据 | 每日 |

## 数据更新流程

1. **交易日判断**：通过 Tushare `trade_cal` 确认是否为交易日
2. **外围数据**：BTC/USD、XAU/USD、USDCNY、US10Y
3. **大盘数据**：三大指数K线、市场活跃度
4. **全市场数据**：所有股票的 daily_basic 指标
5. **分组生成**：成交额/涨幅/跌幅 Top100
6. **特征计算**：分层统计、板块分布、K线形态识别
7. **汇总统计**：生成各类分布图表数据

## K线形态识别

支持的形态包括：
- 反转形态：锤子线、倒锤子线、看涨吞没、看跌吞没、早晨之星、黄昏之星、流星线、孕线
- 持续形态：光头光脚、红三兵、黑三鸦
- 不确定形态：十字星、纺锤线

## 前端页面结构

### 1. 外盘数据
- 一行四列卡片：BTC、黄金、汇率、美债
- 显示当前价、涨跌幅、120日趋势图

### 2. 大盘数据
- 三大指数K线（含MA5/20/60）
- 成交额、活跃度、融资净买入趋势
- 涨跌家数、涨跌停数量、创业板PE

### 3. 市场全貌
- 全市场涨跌幅分布
- 涨幅Top100：市值分层、成交额分层
- 涨幅Top100：板块分布、K线形态分布

## 定时任务设置

建议设置定时任务，每日收盘后自动运行数据更新：

```bash
# Linux crontab 示例 (每日17:00运行)
0 17 * * * cd /path/to/ch-stock && python -c "from data_updater.trade_day_updater import run_daily_update; run_daily_update()" >> /var/log/stock_review.log 2>&1
```

## 注意事项

1. **数据更新耗时**：首次全量更新可能需要10-30分钟（取决于网络速度和数据量）
2. **API限制**：Tushare有频率限制，免费版每分钟最多调用120次
3. **交易日判断**：系统会自动判断是否为交易日，非交易日不会更新数据

## 故障排查

### 数据库连接失败
- 检查MySQL服务器是否运行
- 确认用户名密码正确
- 检查防火墙是否开放3306端口

### Tushare API 错误
- 确认已设置有效的 TUSHARE_TOKEN
- 检查API调用频率是否超限

### 数据不完整
- 查看 job_run_log 表中的错误信息
- 检查网络连接是否稳定
- 部分数据源（如外盘数据）可能因API限制无法获取
