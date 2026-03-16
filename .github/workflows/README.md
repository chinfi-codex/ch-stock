# GitHub Actions 工作流说明

## Update Market Data

自动获取每日市场数据并更新到 `datas/market_data.csv`。

### 触发时间

- **定时触发**: 北京时间每周一到周五下午 5:00（UTC 09:00）
- **手动触发**: 支持在 GitHub Actions 页面手动运行

### 配置要求

需要在 GitHub 仓库的 Settings > Secrets and variables > Actions 中添加：

- `TUSHARE_TOKEN`: Tushare API Token

### 工作流程

1. 检出代码
2. 设置 Python 3.10 环境
3. 安装依赖（pandas, akshare, tushare, streamlit）
4. 运行 `scripts/fetch_daily_market_data.py` 获取数据
5. 如有数据更新，自动提交到仓库

### 数据字段

| 字段 | 来源 | 说明 |
|------|------|------|
| 日期 | AkShare | 交易日期 |
| 上涨 | AkShare/Tushare | 上涨家数（缺失时用 Tushare 回填） |
| 下跌 | AkShare/Tushare | 下跌家数（缺失时用 Tushare 回填） |
| 成交额 | Tushare | 全市场成交额（千元） |
| 涨停/跌停等 | AkShare | 其他市场统计数据 |

### 注意事项

- 仅在工作日（周一到周五）自动运行
- 如果当天数据已存在，只会更新缺失的字段
- 提交信息格式：`Update market data for YYYY-MM-DD`
