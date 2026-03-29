# ch-stock 当前分层架构快照

更新时间：2026-03-29

## 总览

当前依赖链路：

```text
pages/app.py -> services -> tools -> infra
```

例外：

- `pages` 可以直接调用少量稳定的原子工具
- `tools/kline_data.py` 中的 `plotK()` 暂时保留展示耦合，作为当前特例处理

## infra

职责：

- 统一配置读取
- 通用数据处理
- 仓储与数据库访问
- Prompt 模板基础设施
- LLM 调用封装
- 通用抓取与存储能力

当前模块：

- [config.py](/Users/chenh/Documents/Stocks/ch-stock/infra/config.py)
- [daily_basic_repository.py](/Users/chenh/Documents/Stocks/ch-stock/infra/daily_basic_repository.py)
- [database.py](/Users/chenh/Documents/Stocks/ch-stock/infra/database.py)
- [data_utils.py](/Users/chenh/Documents/Stocks/ch-stock/infra/data_utils.py)
- [llm_client.py](/Users/chenh/Documents/Stocks/ch-stock/infra/llm_client.py)
- [market_history_repository.py](/Users/chenh/Documents/Stocks/ch-stock/infra/market_history_repository.py)
- [prompt_templates.py](/Users/chenh/Documents/Stocks/ch-stock/infra/prompt_templates.py)
- [storage.py](/Users/chenh/Documents/Stocks/ch-stock/infra/storage.py)
- [web_scraper.py](/Users/chenh/Documents/Stocks/ch-stock/infra/web_scraper.py)

## tools

职责：

- 股票领域原子能力
- 单一数据源抓取
- 单一指标计算
- 单一算法识别
- 单次 AI 调用与 prompt 构建

当前模块：

- [ai_analysis.py](/Users/chenh/Documents/Stocks/ch-stock/tools/ai_analysis.py)
- [crawlers.py](/Users/chenh/Documents/Stocks/ch-stock/tools/crawlers.py)
- [financial_data.py](/Users/chenh/Documents/Stocks/ch-stock/tools/financial_data.py)
- [kline_data.py](/Users/chenh/Documents/Stocks/ch-stock/tools/kline_data.py)
- [kline_patterns.py](/Users/chenh/Documents/Stocks/ch-stock/tools/kline_patterns.py)
- [market_data.py](/Users/chenh/Documents/Stocks/ch-stock/tools/market_data.py)
- [p5w_interaction.py](/Users/chenh/Documents/Stocks/ch-stock/tools/p5w_interaction.py)
- [technical_analysis.py](/Users/chenh/Documents/Stocks/ch-stock/tools/technical_analysis.py)
- [utils.py](/Users/chenh/Documents/Stocks/ch-stock/tools/utils.py)
- [zsxq.py](/Users/chenh/Documents/Stocks/ch-stock/tools/zsxq.py)

明确不再放在 tools 的职责：

- 市场总览流程
- 股票池聚合流程
- 龙虎榜专题流程
- `daily_basic` 存储与智能回源
- 技术特征组合编排
- AI 展示与分类流程

## services

职责：

- 组合多个 tool / infra 完成业务流程
- 补数、回源、降级、展示辅助
- 输出可直接给页面使用的结果

当前模块：

- [ai_analysis.py](/Users/chenh/Documents/Stocks/ch-stock/services/ai_analysis.py)
- [daily_basic_service.py](/Users/chenh/Documents/Stocks/ch-stock/services/daily_basic_service.py)
- [daily_basic_sync.py](/Users/chenh/Documents/Stocks/ch-stock/services/daily_basic_sync.py)
- [market_analysis_service.py](/Users/chenh/Documents/Stocks/ch-stock/services/market_analysis_service.py)
- [market_overview_service.py](/Users/chenh/Documents/Stocks/ch-stock/services/market_overview_service.py)
- [stock_universe_service.py](/Users/chenh/Documents/Stocks/ch-stock/services/stock_universe_service.py)
- [technical_feature_service.py](/Users/chenh/Documents/Stocks/ch-stock/services/technical_feature_service.py)

## 页面层当前主链路

- [app.py](/Users/chenh/Documents/Stocks/ch-stock/app.py)
  - 调用市场总览、股票池、AI 分析相关 service
- [09-特征分组.py](/Users/chenh/Documents/Stocks/ch-stock/pages/09-%E7%89%B9%E5%BE%81%E5%88%86%E7%BB%84.py)
  - 调用 [services/ai_analysis.py](/Users/chenh/Documents/Stocks/ch-stock/services/ai_analysis.py)
  - 调用 [services/daily_basic_service.py](/Users/chenh/Documents/Stocks/ch-stock/services/daily_basic_service.py)

## 配置规则

统一从 [infra/config.py](/Users/chenh/Documents/Stocks/ch-stock/infra/config.py) 读取：

- Tushare token
- Alpha Vantage key
- Jina key
- LLM key
- 知识星球配置

禁止在业务代码中直接读取：

- `os.environ.get(...)`
- `st.secrets.get(...)`

## 后续新增代码的判断标准

放进 `tools` 的条件：

- 是单一职责
- 不依赖 service
- 不包含页面展示
- 不负责仓储回源编排

放进 `services` 的条件：

- 需要组合两个及以上原子能力
- 需要处理流程控制、补数、缓存策略、回退逻辑
- 页面会直接把它当“一个完整能力”来调用

放进 `infra` 的条件：

- 与股票业务无关
- 未来可被其他项目复用
- 本质是配置、模板、数据库、存储、HTTP/LLM 客户端等底层能力
