# ch-stock 项目架构规范与代码指南

## 架构哲学

### 分层架构设计

本项目采用**三层架构**设计，实现职责分离和代码复用：

```
┌─────────────────────────────────────────────┐
│           Presentation Layer                │
│   pages/          Streamlit 页面展示         │
│   app.py          主应用入口                │
└──────────────────┬──────────────────────────┘
                   │ 调用
┌──────────────────▼──────────────────────────┐
│          Business Logic Layer               │
│   services/       业务流程编排（AI分析等）   │
└──────────────────┬──────────────────────────┘
                   │ 组合调用
┌──────────────────▼──────────────────────────┐
│        Atomic Capability Layer              │
│   tools/          业务原子能力               │
│   ├── kline_data.py      K线数据获取        │
│   ├── technical_analysis.py 技术分析        │
│   ├── market_data.py     市场数据           │
│   └── ...                                  │
└──────────────────┬──────────────────────────┘
                   │ 依赖
┌──────────────────▼──────────────────────────┐
│       Infrastructure Layer                  │
│   infra/          基础设施（通用、可复用）   │
│   ├── config.py          配置管理           │
│   ├── llm_client.py      LLM客户端         │
│   ├── data_utils.py      数据处理          │
│   └── ...                                  │
└─────────────────────────────────────────────┘
```

### 核心设计原则

#### 1. 职责分离（Separation of Concerns）

- **infra/**: 通用基础设施，**与业务无关**，可被任何项目复用
- **tools/**: 业务原子能力，**单一职责**，可独立使用
- **services/**: 业务流程编排，**组合原子能力**完成复杂业务场景
- **pages/**: UI展示层，**仅负责渲染**，不包含业务逻辑

#### 2. 原子化能力（Atomic Capabilities）

业务功能拆解为最小可复用单元：
- ✅ `get_tushare_price_df()` - 获取K线数据（原子能力）
- ✅ `calculate_macd()` - 计算MACD指标（原子能力）
- ✅ `build_macro_prompt()` - 构建分析Prompt（原子能力）
- ❌ `analyze_external_assets()` - 这是业务流程（在services层）

#### 3. 依赖方向（Dependency Direction）

依赖只能**自上而下**：
```
services → tools → infra
pages → services/tools
```

**禁止反向依赖**：
- ❌ infra 不能依赖 tools
- ❌ tools 不能依赖 services
- ❌ services 不能依赖 pages

---

## 项目目标与演进方向

### 项目定位

**ch-stock** 是一个 **A股数据复盘中台**，定位为：

> 🎯 **A股市场的数据整合、分析与复盘一体化平台**

核心使命：
- 📊 **数据整合**：聚合多源数据（Tushare、AKShare、东方财富等）
- 🔍 **智能分析**：结合AI技术进行市场分析和个股研究
- 📝 **自动复盘**：支持每日市场复盘、技术指标监控
- 🔌 **开放对接**：为后续量化策略、OpenClaw生态提供数据支撑

### 演进方向

#### 阶段一：数据中台建设 ✅ 当前阶段
- [x] 多源数据接入（Tushare、东方财富、巨潮资讯）
- [x] 数据标准化和清洗
- [x] 基础技术指标计算
- [x] AI辅助分析能力

#### 阶段二：智能分析平台 🚧 进行中
- [ ] 实时数据推送（WebSocket）
- [ ] 多因子选股引擎
- [ ] 策略回测框架
- [ ] 个性化监控预警

#### 阶段三：生态对接与开放 🔮 规划
- [ ] **OpenClaw 数据对接**：标准化数据API输出
- [ ] 量化交易平台集成
- [ ] 社区化功能（策略分享、讨论）
- [ ] 移动端适配

### 与 OpenClaw 的对接规划

```
┌─────────────────────────────────────────────┐
│              OpenClaw 生态                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ 量化策略  │  │ 智能预警  │  │ 组合管理  │  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  │
│       │             │             │         │
│       └─────────────┼─────────────┘         │
│                     │                       │
│              ┌──────▼──────┐               │
│              │ 标准化API   │               │
│              │  (REST)     │               │
│              └──────┬──────┘               │
└─────────────────────┼───────────────────────┘
                      │
┌─────────────────────▼───────────────────────┐
│         ch-stock 数据复盘中台               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ 市场数据  │  │ 个股分析  │  │ 技术指标  │  │
│  └──────────┘  └──────────┘  └──────────┘  │
└─────────────────────────────────────────────┘
```

**对接标准**：
- 输出标准化的 REST API
- 支持 OpenAPI 规范
- 数据格式：JSON / Protobuf
- 认证方式：API Key

---

#### 4. 显式优于隐式（Explicit over Implicit）

- 跨层调用必须**显式导入**，禁止使用通配符导入
- 函数职责**单一且明确**，避免"万能函数"
- 错误处理**显式抛出**，不静默吞掉异常

---

## 项目结构

```
ch-stock/
├── app.py                      # Streamlit 主应用入口
├── requirements.txt            # Python 依赖
├── data_sources.py             # 数据源适配层（兼容旧代码）
│
├── infra/                      # 基础设施层（通用、可复用）
│   ├── __init__.py            # 导出 infra 模块
│   ├── config.py              # 配置管理（Token获取等）
│   ├── llm_client.py          # LLM调用封装
│   ├── data_utils.py          # 数据处理（代码转换等）
│   ├── storage.py             # 文件存储工具
│   └── web_scraper.py         # 网页爬取
│
├── tools/                      # 业务原子能力层
│   ├── __init__.py            # 导出 tools 模块
│   ├── utils.py               # 股票业务工具函数
│   ├── kline_data.py          # K线数据获取 + 绘图
│   ├── technical_analysis.py  # 技术指标计算
│   ├── kline_patterns.py      # K线形态识别
│   ├── market_data.py         # 市场数据获取
│   ├── financial_data.py      # 金融数据获取
│   ├── crawlers.py            # 数据爬虫
│   └── ai_analysis.py         # AI分析原子能力
│
├── services/                   # 业务流程层
│   ├── __init__.py            # 导出 services 模块
│   └── ai_analysis.py         # AI分析业务流程编排
│
├── pages/                      # UI展示层（Streamlit多页面）
│   ├── 08-关注分组.py
│   ├── 09-特征分组.py
│   └── 10-公司投研专家.py
│
└── datas/                      # 数据存储目录
    └── reviews/
```

---

## 分层模块设计说明

### 第一层：Infrastructure（基础设施层）

**定位**：通用、与业务无关的基础设施，可被任何项目复用。

**设计原则**：
- 单一职责，功能原子化
- 不依赖项目业务逻辑
- 提供稳定的抽象接口

#### `infra/config.py` - 配置管理
```python
# 职责：Token获取、配置读取
# 使用方式：
from infra.config import get_tushare_token

token = get_tushare_token()  # 自动处理优先级：环境变量 > secrets > .env
```

**关键函数**：
- `get_tushare_token()` - 获取Tushare Token（支持多源优先级）

#### `infra/llm_client.py` - LLM客户端
```python
# 职责：统一封装LLM调用，支持多种提供商
# 使用方式：
from infra.llm_client import call_kimi_print, clean_ai_output

result = call_kimi_print(prompt, cache_key="analysis_001")
cleaned = clean_ai_output(result)
```

**关键函数**：
- `call_kimi_print()` - 调用Kimi CLI（带缓存）
- `clean_ai_output()` - 清理AI输出格式
- `ai_summarize_cached()` - 带缓存的AI总结（业务封装）

#### `infra/data_utils.py` - 数据处理
```python
# 职责：通用数据处理，股票代码转换等
# 使用方式：
from infra.data_utils import convert_to_ts_code, to_number

ts_code = convert_to_ts_code("000001")  # "000001.SZ"
nums = to_number(df["pct_chg"])         # 转换为数值类型
```

**关键函数**：
- `convert_to_ts_code()` - 转换为Tushare代码格式
- `convert_to_ak_code()` - 转换为AKShare代码格式
- `to_number()` - 转换Series为数值类型

#### `infra/storage.py` - 文件存储
```python
# 职责：文件存储相关工具
# 使用方式：
from infra.storage import clean_filename

safe_name = clean_filename("非法:文件名.txt")  # "非法_文件名.txt"
```

**关键函数**：
- `clean_filename()` - 清理文件名中的非法字符

#### `infra/web_scraper.py` - 网页爬取
```python
# 职责：通用网页内容爬取
# 使用方式：
from infra.web_scraper import scrape_with_jina_reader

result = scrape_with_jina_reader(url, title="文章标题")
```

**关键函数**：
- `scrape_with_jina_reader()` - 使用Jina Reader爬取网页

---

### 第二层：Tools（业务原子能力层）

**定位**：股票分析领域的原子能力，单一职责，可独立使用。

**设计原则**：
- 每个函数只做一件事
- 不依赖其他业务模块（可依赖infra）
- 提供清晰的输入输出接口

#### `tools/utils.py` - 股票业务工具
```python
# 职责：股票业务相关的数据处理工具
# 依赖：infra.data_utils（代码转换）

from tools.utils import (
    get_stock_list,           # 获取股票列表
    get_xueqiu_stock_topics,  # 获取雪球话题
    weibo_comments,           # 获取微博评论
    filter_st_bj_stocks,      # 过滤ST和北交所
    calc_pct_change,          # 计算百分比变化
)
```

#### `tools/kline_data.py` - K线数据
```python
# 职责：K线数据获取和可视化
# 依赖：infra.config, infra.data_utils

from tools.kline_data import (
    get_tushare_price_df,     # 获取日K线
    get_tushare_weekly_df,    # 获取周K线
    get_tushare_monthly_df,   # 获取月K线
    plotK,                    # 绘制K线图
    calculate_macd,           # 计算MACD指标
)
```

#### `tools/technical_analysis.py` - 技术分析
```python
# 职责：技术指标计算和形态识别
# 依赖：tools.kline_patterns

from tools.technical_analysis import StockTechnical

tech = StockTechnical(df)
features = tech.get_features()           # 获取所有技术特征
patterns = tech.recognize_pattern()      # 识别K线形态（独立方法）
```

#### `tools/kline_patterns.py` - K线形态
```python
# 职责：12种K线形态识别算法
# 依赖：无（纯算法模块）

from tools.kline_patterns import (
    KLinePatternRecognizer,
    recognize_pattern,        # 便捷函数：识别单一形态
    recognize_all_patterns,   # 便捷函数：识别所有形态
)
```

#### `tools/market_data.py` - 市场数据
```python
# 职责：大盘、板块、龙虎榜等市场数据获取
# 依赖：infra.config, infra.data_utils

from tools.market_data import (
    get_market_data,          # 获取大盘数据
    get_all_stocks,           # 获取所有股票
    get_longhu_data,          # 获取龙虎榜数据
)
```

#### `tools/financial_data.py` - 金融数据
```python
# 职责：汇率、债券、商品等金融数据
# 依赖：infra.config

from tools.financial_data import EconomicIndicators

# 获取汇率
rate = EconomicIndicators.get_exchangerates_daily("USD", "CNY")
```

#### `tools/crawlers.py` - 数据爬虫
```python
# 职责：财联社、巨潮资讯等数据源爬取
# 依赖：infra.web_scraper, infra.storage

from tools.crawlers import (
    cls_telegraphs,           # 财联社电报
    cninfo_announcement_spider, # 巨潮资讯公告
)
```

#### `tools/ai_analysis.py` - AI分析原子能力
```python
# 职责：AI分析的Prompt构建和数据格式化（原子能力）
# 依赖：infra.llm_client

from tools.ai_analysis import (
    build_macro_prompt,       # 构建宏观分析Prompt
    build_market_overview_prompt,  # 构建市场概况Prompt
    run_ai_analysis,          # 执行AI分析（原子能力）
    display_ai_analysis,      # 显示AI分析结果
    format_series_for_ai,     # 格式化数据序列
)

# 注意：analyze_* 业务流程已迁移到 services.ai_analysis
```

---

### 第三层：Services（业务流程层）

**定位**：业务流程编排，组合多个原子能力完成复杂业务场景。

**设计原则**：
- 协调多个tools完成业务目标
- 处理业务规则和流程控制
- 可调用display方法展示结果

#### `services/ai_analysis.py` - AI分析业务流程
```python
# 职责：AI分析的业务流程编排
# 依赖：tools.ai_analysis（原子能力）

from services.ai_analysis import (
    analyze_external_assets,      # 外围资产分析流程
    analyze_market_overview,      # 市场概况分析流程
    analyze_index_technical,      # 指数技术分析流程
    analyze_stock_classification, # 股票分类分析流程
)

# 使用示例：
result = analyze_external_assets(
    usdcny_series=usdcny_data,
    btc_series=btc_data,
    xau_series=xau_data,
    wti_series=wti_data,
    us10y_series=us10y_data,
    show_ui=True  # 自动在Streamlit中展示结果
)
```

**与 tools.ai_analysis 的区别**：
- `tools.ai_analysis.build_macro_prompt()` - 原子能力：构建Prompt
- `services.ai_analysis.analyze_external_assets()` - 业务流程：构建Prompt + 调用AI + 展示结果

---

## 代码风格规范

### 文件编码
- 所有 Python 文件必须使用 UTF-8 编码
- 文件头部必须包含：
  ```python
  #!/usr/bin/env python
  # -*- coding: utf-8 -*-
  ```

### 导入顺序（强制要求）

```python
# 1. 标准库
import os
import sys
from datetime import datetime, timedelta

# 2. 第三方库
import pandas as pd
import numpy as np
import streamlit as st
import akshare as ak
import tushare as ts

# 3. 基础设施层（跨项目可复用）
from infra.config import get_tushare_token
from infra.data_utils import convert_to_ts_code
from infra.llm_client import call_kimi_print

# 4. 业务原子能力层（股票领域）
from tools.kline_data import get_ak_price_df, plotK
from tools.market_data import get_market_data
from tools.ai_analysis import build_macro_prompt

# 5. 业务流程层（复杂场景编排）
from services.ai_analysis import analyze_external_assets
```

### 跨层调用规范

#### ✅ 正确的调用方式

```python
# services 层调用 tools 层（业务流程组合原子能力）
# services/ai_analysis.py
from tools.ai_analysis import build_macro_prompt, run_ai_analysis
from tools.financial_data import EconomicIndicators

def analyze_external_assets(...):
    # 1. 获取数据（原子能力）
    usdcny_data = EconomicIndicators.get_exchangerates_daily(...)
    
    # 2. 构建Prompt（原子能力）
    prompt = build_macro_prompt(...)
    
    # 3. 执行分析（原子能力）
    result = run_ai_analysis(prompt)
    
    # 4. 业务流程控制
    return display_ai_analysis(...)
```

```python
# tools 层调用 infra 层（业务依赖基础设施）
# tools/kline_data.py
from infra.config import get_tushare_token
from infra.data_utils import convert_to_ts_code

def get_tushare_price_df(code, ...):
    ts_code = convert_to_ts_code(code)  # 基础设施
    pro = get_tushare_pro()             # 基础设施
    # ... 业务逻辑
```

#### ❌ 错误的调用方式

```python
# 错误：tools 调用 services（下层调用上层）
# tools/kline_data.py
from services.ai_analysis import analyze_external_assets  # ❌ 禁止！

# 错误：infra 调用 tools（基础设施依赖业务）
# infra/config.py
from tools.utils import get_stock_list  # ❌ 禁止！

# 错误：循环导入
# tools/a.py
from tools.b import func_b

# tools/b.py
from tools.a import func_a  # ❌ 循环导入！
```

### 函数命名规范

```python
# 获取数据：get_*
def get_tushare_price_df(code: str) -> pd.DataFrame:
    """使用tushare获取股票日K线数据"""
    pass

# 计算指标：calculate_*
def calculate_macd(df: pd.DataFrame) -> pd.DataFrame:
    """计算MACD指标"""
    pass

# 构建Prompt：build_*
def build_macro_prompt(...) -> str:
    """构建宏观分析Prompt"""
    pass

# 分析流程：analyze_*
def analyze_external_assets(...) -> Optional[str]:
    """外围资产分析业务流程"""
    pass

# 格式化：format_*
def format_series_for_ai(...) -> str:
    """格式化数据序列为AI分析文本格式"""
    pass

# 私有方法：_*
def _get_ts_client():
    """内部辅助函数"""
    pass
```

### 类型注解规范

```python
from typing import Optional, List, Dict, Any
import pandas as pd

def get_stock_data(
    ts_code: str,
    start_date: str,
    end_date: str,
    count: int = 60
) -> pd.DataFrame:
    """
    获取股票数据
    
    Args:
        ts_code: 股票代码（支持多种格式）
        start_date: 开始日期（YYYYMMDD）
        end_date: 结束日期（YYYYMMDD）
        count: 返回数据条数，默认60
    
    Returns:
        pd.DataFrame: 包含open, high, low, close, volume的DataFrame
    
    Raises:
        ValueError: 如果股票代码格式无效
        RuntimeError: 如果API调用失败
    
    Example:
        >>> df = get_stock_data("000001.SZ", "20240101", "20241231")
        >>> print(df.head())
    """
    pass
```

---

## 错误处理规范

### 日志配置
```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
```

### 异常处理模式
```python
# 基础设施层：抛出异常，让上层处理
def get_tushare_token() -> str:
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        raise ValueError("TUSHARE_TOKEN not found")
    return token

# 业务原子能力层：捕获并转换为用户友好的错误
def get_tushare_price_df(code: str) -> pd.DataFrame:
    try:
        pro = get_tushare_pro()
        df = pro.daily(ts_code=code)
        return df
    except Exception as e:
        logger.error(f"获取股票数据失败: {code}, error: {e}")
        raise RuntimeError(f"无法获取 {code} 的数据，请检查代码或网络连接")

# 业务流程层：捕获并提供降级方案
def analyze_external_assets(...) -> Optional[str]:
    try:
        prompt = build_macro_prompt(...)
        return run_ai_analysis(prompt)
    except Exception as e:
        logger.error(f"AI分析失败: {e}")
        if show_ui:
            st.error("AI分析暂时不可用，请稍后重试")
        return None
```

---

## Git 工作流

### Git 操作规范

#### 1. 频繁提交原则

**每次操作都必须 commit**：
- ✅ 完成一个功能点 → commit
- ✅ 修复一个 bug → commit  
- ✅ 重构一个模块 → commit
- ✅ 更新文档 → commit

**时机合适时就 push**：
- 每天下班前 push
- 完成一个完整功能后 push
- 多人协作时及时 push（避免冲突）

```bash
# 示例工作流程
git add .
git commit -m "feat(tools): 新增K线形态识别功能"
# ... 继续开发 ...
git add .
git commit -m "fix(tools): 修复锤子线识别逻辑"
# 功能完成，push到远程
git push
```

#### 2. 分支管理策略

**简单修改**（单文件、小于100行）：
- 直接在 `master` 分支操作
- 及时 commit 和 push

**较大改动**（多文件重构、新功能模块）：
- 必须创建 feature 分支
- 走 PR（Pull Request）流程

```bash
# 创建功能分支
git checkout -b feature/kline-patterns

# 开发完成后，push分支
git push -u origin feature/kline-patterns

# 在 GitHub 上创建 Pull Request
# 代码审查通过后合并到 master
```

**需要走 PR 流程的场景**：
- 新增/删除整个模块（infra/tools/services）
- 修改超过 5 个文件
- 改动超过 300 行代码
- 涉及 API 接口变更
- 数据库/配置结构变更
- 跨层调用关系调整

#### 3. 提交前检查清单

```bash
# 1. 检查修改内容
git diff --cached

# 2. 确认没有遗漏的文件
git status

# 3. 运行代码检查（如有）
# python -m pytest tests/
# python -m mypy tools/

# 4. 提交
git commit -m "type(scope): subject"
```

#### 4. PR 流程规范

**创建 PR 前**：
1. 确保分支基于最新的 master
2. 本地测试通过
3. 添加清晰的 PR 描述（做了什么、为什么、如何测试）

**PR 审查要点**：
- 代码是否符合架构规范
- 是否有重复代码
- 错误处理是否完善
- 文档是否同步更新

**合并后**：
1. 删除 feature 分支
2. 更新本地 master

### Commit 消息规范
```bash
# 格式：<type>(<scope>): <subject>
# scope: infra | tools | services | pages | docs

# 示例：
git commit -m "feat(infra): 新增 Jina Reader 网页爬取功能"
git commit -m "fix(tools): 修复巨潮资讯爬虫超时问题"
git commit -m "refactor(services): 优化AI分析业务流程"
git commit -m "docs: 更新架构规范文档"
```

### Commit 类型
- `feat`: 新功能
- `fix`: Bug 修复
- `refactor`: 重构（不改变功能）
- `perf`: 性能优化
- `docs`: 文档更新
- `style`: 代码风格调整
- `test`: 测试相关
- `chore`: 构建系统或依赖变更

---

## 维护信息

**维护者：** chenh

**架构版本：** v3.0 - 三层架构重构版

**最后更新：** 2026-03-26

---

## 相关链接

- Tushare API: https://tushare.pro/document/2
- AkShare 文档: https://akshare.akfamily.xyz/
- Streamlit 文档: https://docs.streamlit.io/
