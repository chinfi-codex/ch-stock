# ch-stock 项目架构规范与代码指南

## 架构哲学

### 分层架构

本项目采用四层职责划分：

```text
pages/app.py -> services -> tools -> infra
```

- `pages/` 与 `app.py`
  - Streamlit 展示层
  - 负责页面渲染、交互和调用 service
- `services/`
  - 业务流程编排层
  - 组合多个 tool / infra 完成完整业务场景
- `tools/`
  - 业务原子能力层
  - 只保留单一职责、可复用、可独立调用的股票领域能力
- `infra/`
  - 基础设施层
  - 提供配置、仓储、模板、通用数据处理、数据库等能力

### 依赖方向

依赖只能自上而下：

```text
pages -> services/tools
services -> tools/infra
tools -> infra
```

禁止：

- `infra -> tools/services/pages`
- `tools -> services/pages`
- `services -> pages`
- 循环导入

### 当前定位

`ch-stock` 是 A 股数据复盘中台，当前重点是：

- 多源数据整合
- 市场复盘与专题分析
- AI 辅助分析
- 为后续 API 输出、量化策略与 OpenClaw 对接提供稳定数据层

## 当前项目结构

```text
ch-stock/
├── app.py
├── data_sources.py
├── infra/
│   ├── config.py
│   ├── daily_basic_repository.py
│   ├── database.py
│   ├── data_utils.py
│   ├── llm_client.py
│   ├── market_history_repository.py
│   ├── prompt_templates.py
│   ├── storage.py
│   └── web_scraper.py
├── services/
│   ├── ai_analysis.py
│   ├── daily_basic_service.py
│   ├── daily_basic_sync.py
│   ├── market_analysis_service.py
│   ├── market_overview_service.py
│   ├── stock_universe_service.py
│   └── technical_feature_service.py
├── tools/
│   ├── ai_analysis.py
│   ├── crawlers.py
│   ├── financial_data.py
│   ├── kline_data.py
│   ├── kline_patterns.py
│   ├── market_data.py
│   ├── p5w_interaction.py
│   ├── technical_analysis.py
│   ├── utils.py
│   └── zsxq.py
├── pages/
└── datas/
```

## 分层职责说明

### infra

`infra/` 只放与业务无关、可复用的底层能力。

当前主要模块：

- `config.py`
  - 统一配置读取入口
  - 当前集中管理：
    - `get_tushare_token()`
    - `get_alpha_vantage_key()`
    - `get_jina_api_key()`
    - `get_llm_api_key(provider)`
    - `get_zsxq_cookie()`
    - `get_zsxq_group_ids()`
    - `get_zsxq_api_timeout()`
- `data_utils.py`
  - 通用数据处理
  - 包含代码转换、数值转换、通用序列提取和涨跌幅计算
- `prompt_templates.py`
  - Prompt 模板环境与模板读取
- `daily_basic_repository.py`
  - `daily_basic` 数据仓储
  - 只负责存取，不负责回源流程
- `market_history_repository.py`
  - 市场历史 CSV 读写
- `database.py`
  - SQLite 连接与初始化
- `llm_client.py`
  - 统一 LLM 调用封装
- `web_scraper.py`
  - 通用网页抓取
- `storage.py`
  - 通用文件工具

规则：

- 所有 token / key / secrets / env 读取统一从 `infra.config` 进入
- repository 只做存取，不做业务编排
- infra 不允许依赖 `tools` 或 `services`

### tools

`tools/` 只保留业务原子能力。

#### 已确认属于原子能力的模块

- `tools/kline_data.py`
  - K 线数据获取
  - `calculate_macd()`
  - `plotK()` 作为当前特例保留在 tools，不在本轮继续下沉
- `tools/kline_patterns.py`
  - K 线形态识别纯算法
- `tools/technical_analysis.py`
  - 单项技术分析能力
  - 保留周/月聚合、新高分析、换手情绪、箱体突破、形态识别
  - 不再负责特征组合编排
- `tools/market_data.py`
  - 只保留原子市场数据获取
  - 当前保留：
    - `get_financing_net_buy_series`
    - `get_gem_pe_series`
    - `get_dfcf_concept_boards`
    - `get_concept_board_index`
    - `get_market_daily_stats`
    - `get_market_amount_series`
- `tools/financial_data.py`
  - 汇率、利率、商品、加密资产、宏观指标
- `tools/crawlers.py`
  - 财联社、巨潮资讯等单一数据源抓取
- `tools/ai_analysis.py`
  - 只保留数据格式化、prompt 构建、单次 AI 调用
- `tools/utils.py`
  - 只保留股票业务工具
  - 当前保留：
    - `filter_st_bj_stocks`
    - `get_stock_list`
    - `get_xueqiu_stock_topics`
    - `weibo_comments`
- `tools/p5w_interaction.py`
  - 全景网互动易抓取原子能力
- `tools/zsxq.py`
  - 知识星球主题抓取原子能力
  - 不再在 `tools` 内做配置直读和本地文件落盘

#### 已从 tools 移出的职责

以下职责已迁出，不允许重新放回 `tools/`：

- `display_ai_analysis`
- `analyze_stock_classification`
- `get_market_data`
- `get_market_history`
- `get_all_stocks`
- `get_longhu_data`
- `daily_basic` 存储与智能回源
- `technical_analysis.get_features`
- 通用数据工具：
  - `latest_metric_from_df`
  - `calc_pct_change`
  - `series_from_df`

规则：

- tool 函数只做一件事
- 不在 `tools` 里混入 Streamlit 页面逻辑
- 不在 `tools` 里混入本地存储编排、数据库同步策略、缓存回源流程
- 除 `plotK()` 之外，避免把 UI 渲染逻辑放进 tools

### services

`services/` 负责业务流程编排。

当前模块：

- `services/ai_analysis.py`
  - AI 分析完整流程
  - 包含展示、外围资产分析、市场概览分析、指数技术分析、股票分类分析
- `services/daily_basic_service.py`
  - `daily_basic` 本地优先查询、缺失回源、缺失日期计算
- `services/daily_basic_sync.py`
  - 历史同步、最近 N 天同步、状态查询
- `services/market_overview_service.py`
  - 市场总览流程
  - 组合指数、市场情绪、历史落盘
- `services/stock_universe_service.py`
  - 全市场股票池聚合
- `services/market_analysis_service.py`
  - 龙虎榜等专题市场分析流程
- `services/technical_feature_service.py`
  - 技术特征组合编排

规则：

- 业务流程统一放 service
- service 可以组合多个 tool 和 repository
- service 可以处理降级、流程控制、补数策略、展示辅助
- 页面优先调用 service，而不是直接拼装 tool

### pages / app

页面层只负责：

- 接收用户输入
- 调用 service / 少量原子 tool
- 展示结果

禁止：

- 在页面里直接拼装复杂业务流程
- 在页面里直接读取 secrets / env
- 在页面里直接处理仓储同步逻辑

## 当前推荐调用方式

### 正确示例

```python
from services.market_overview_service import get_market_data
from services.stock_universe_service import get_all_stocks
from services.ai_analysis import analyze_stock_classification
from services.daily_basic_service import get_daily_basic_smart
```

```python
from tools.kline_data import get_ak_price_df, plotK
from tools.market_data import get_market_daily_stats
from tools.ai_analysis import build_macro_prompt, run_ai_analysis
```

```python
from infra.config import get_tushare_token
from infra.data_utils import convert_to_ts_code, calc_pct_change
from infra.daily_basic_repository import query_daily_basic
```

### 错误示例

```python
from tools.market_data import get_market_data  # 错误：已迁到 services
from tools.ai_analysis import analyze_stock_classification  # 错误：已迁到 services
from tools.daily_basic_storage import get_daily_basic_smart  # 错误：模块已删除
```

```python
import os
import streamlit as st

token = os.environ.get("TUSHARE_TOKEN")      # 错误：应走 infra.config
token = st.secrets.get("tushare_token")      # 错误：应走 infra.config
```

## 配置规范

统一要求：

- 所有配置读取都通过 `infra.config`
- 代码中不直接读取：
  - `os.environ.get(...)`
  - `st.secrets.get(...)`
- 新增外部数据源时，先在 `infra.config` 增加读取函数，再在 tool / service 使用

## 命名规范

- 获取数据：`get_*`
- 计算指标：`calculate_*`
- 构建 prompt：`build_*`
- 分析流程：`analyze_*`
- 格式化：`format_*`
- 收集型原子工具：`collect_*` / `fetch_*`
- 私有辅助：`_*`

## 代码风格

- Python 文件使用 UTF-8
- 新文件头部统一：

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
```

- 显式导入，禁止通配符导入
- 补充必要类型注解
- 错误处理显式，不静默吞异常

## 提交与评审要求

以下改动必须走 PR：

- 新增 / 删除整个模块
- 调整跨层依赖关系
- 修改超过 5 个文件
- 大于 300 行的结构性改动
- repository / config / service 边界调整

提交信息格式：

```text
<type>(<scope>): <subject>
```

示例：

- `refactor(tools): remove composite market flows from tools`
- `feat(services): add stock universe orchestration service`
- `docs: refresh architecture guide after layering refactor`

## 根目录清洁要求

- 项目根目录禁止保留临时文件、分析草稿、一次性脚本和测试文件
- 临时分析文件例如 `analysis_tmp.py` 必须在使用后删除，不得留在根目录
- 测试代码不得放在项目根目录；统一放入 `tests/` 或对应模块目录下
- Python 运行产生的缓存目录如 `__pycache__/` 不得提交，发现后应及时清理

## 维护信息

- 维护者：`chenh`
- 架构版本：`v3.1`
- 最后更新：`2026-03-29`
