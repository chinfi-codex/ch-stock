# ch-stock 项目代码规范

## 项目结构

```
ch-stock/
├── app.py                      # Streamlit 主应用入口
├── requirements.txt              # Python 依赖
├── data_sources.py             # 数据源接口（统一适配层）
├── review_scheduler.py         # 自动化任务调度
│
├── tools/                       # 核工具模块
│   ├── __init__.py            # 统一导出接口
│   ├── utils.py                # 核心工具函数（类型转换、日期处理等）
│   ├── kline_data.py          # K线数据获取（日线/周线/月线/分时）+ K线绘图
│   ├── technical_analysis.py    # 技术指标计算（StockTechnical类）+ K线形态识别集成
│   ├── kline_patterns.py      # K线形态识别（12种形态：锤子线、十字星、吞没、早晨之星等）
│   ├── market_data.py         # 市场数据（大盘、龙虎榜、板块、融资数据）
│   ├── financial_data.py        # 金融数据（货币、宏观指标、汇率、经济指标）
│   ├── storage_utils.py       # 存储工具（JSON/CSV 读写）
│   ├── ai_analysis.py         # AI 分析业务逻辑
│   ├── llm_tools.py          # LLM 调用接口（OpenAI 兼容 API + Kimi CLI）
│   └── crawlers.py            # 爬虫工具（公告、微信、财联社）
│
├── pages/                       # Streamlit 多页面
│   ├── 09-特征分组.py
│   └── 10-公司投研专家.py
│
├── datas/                       # 数据存储目录
│   └── reviews/
```

---

## 代码风格规范

### 文件编码
- 所有 Python 文件必须使用 UTF-8 编码
- 文件头部必须包含：
  ```python
  #!/usr/bin/env python
  # -*- coding: utf-8 -*-
  ```

### 导入顺序
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

# 3. 本地模块
from .utils import get_tushare_token, convert_to_ts_code
from .kline_data import get_ak_price_df, plotK
```

### 函数命名
- 使用 `snake_case`：`get_market_data`, `analyze_stock`, `calculate_macd`
- 私有方法以下划线开头：`_get_token`, `_format_data`
- 常量名使用 `UPPER_SNAKE`

### 类型注解
```python
def get_stock_data(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取股票数据
    
    Args:
        ts_code: 股票代码
        start_date: 开始日期
        end_date: 结束日期
    
    Returns:
        pd.DataFrame: 股票数据
    """
    pass
```

### 文档字符串
- 所有公共函数必须包含 docstring
- 使用 Google 风格：
  ```python
  def function_name(param1, param2):
      """
      函数简短描述
      
      详细说明（可选）
      
      Args:
          param1: 参数1说明
          param2: 参数2说明
      
      Returns:
          返回值类型和说明
      
      Example:
          function_name("value1", "value2")
      """
      pass
  ```

---

## 工具模块职责

### `utils.py`
核心工具函数，不依赖任何业务逻辑：
- `get_tushare_token()`: 获取 Tushare API Token
- `convert_to_ts_code(code)`: 转换股票代码为 Tushare 格式
- `to_number(series)`: 类型转换
- `normalize_trade_date(date)`: 日期格式化
- `scrape_with_jina_reader(url, ...)`: 使用 Jina Reader 爬取网页
- `get_xueqiu_stock_topics(...)`: 雪球股票话题爬虫
- `weibo_comments(wid)`: 微博评论爬虫
- `clean_filename(filename)`: 清理文件名

### `kline_data.py`
K线数据获取和绘图：
- `get_tushare_price_df(code, ...)`: 使用 Tushare 获取日K线
- `get_tushare_weekly_df(code, ...)`: 获取周K线
- `get_tushare_monthly_df(code, ...)`: 获取月K线
- `get_ak_price_df(code, ...)`: 获取日K线（统一接口）
- `get_ak_interval_price_df(code, ...)`: 获取分时数据
- `plotK(df, ...)`: 绘制 K线图
- `calculate_macd(df, ...)`: 计算 MACD 指标

### `technical_analysis.py`
技术分析类，包含技术指标计算和形态识别集成：
- `StockTechnical` 类：
  - `new_high_analysis(...)`: 新高后涨势分析
  - `turnover_sentiment_analysis(...)`: 高频换手情绪分析
  - `box_breakout_analysis(...)`: 箱体突破分析
  - `get_features(...)`: 获取所有技术特征
  - `recognize_pattern(df)`: 识别 K线形态（独立方法）
  - `recognize_all_patterns(df)`: 识别所有形态
  - `get_pattern_summary(df)`: 获取形态摘要

### `kline_patterns.py`
K线形态识别算法：
- `KLinePatternRecognizer` 类：识别器，包含 12 种形态识别方法
- `recognize_pattern(df)`: 便捷函数，识别单一形态
- `recognize_all_patterns(df)`: 便捷函数，识别所有形态
- 支持形态：锤子线、倒锤子线、十字星、看涨/看跌吞没、早晨/黄昏之星、流星线、孕线、光头光脚、纺锤线、红三兵、黑三鸦

### `market_data.py`
市场数据获取：
- `get_market_data()`: 获取大盘数据（上证、深证、创业板）
- `get_all_stocks(select_date)`: 获取所有股票数据
- `get_longhu_data(date)`: 获取龙虎榜数据
- `get_dfcf_concept_boards()`: 获取东方财富概念板块
- `get_concept_board_index(...)`: 获取概念板块指数
- `get_financing_net_buy_series(...)`: 获取融资净买入序列
- `get_gem_pe_series(...)`: 获取创业板 PE 序列
- `get_market_history(...)`: 获取市场历史数据
- `get_market_daily_stats(...)`: 获取市场日统计
- `get_market_amount_series(...)`: 获取成交额序列

### `financial_data.py`
金融数据获取：
- `EconomicIndicators` 类：
  - `get_crypto_daily(...)`: 获取加密货币数据（BTC等）
  - `get_treasury_yield(...)`: 获取美债收益率
  - `get_gold_silver_history(...)`: 获取黄金/白银历史
  - `get_exchange_rate(...)`: 获取汇率
  - `get_fed_funds_rate(...)`: 获取联邦基金利率
  - `get_cpi(...)`: 获取 CPI 数据

### `storage_utils.py`
数据存储：
- `save_review_data(date, data, ...)`: 保存回顾数据为 JSON
- `load_review_data(date)`: 加载回顾数据
- `list_review_dates()`: 列出所有可用的回顾日期
- `upsert_market_history(...)`: 更新市场历史 CSV
- `load_market_history_df(limit)`: 加载市场历史 DataFrame
- `df_to_dict(df)`: DataFrame 转 dict

### `ai_analysis.py`
AI 分析业务逻辑：
- 基于 Jinja2 模板系统
- 支持外部资产分析
- 支持指数技术面分析
- 支持市场情绪分析

### `llm_tools.py`
LLM 调用接口：
- `get_llm_response(query, provider="doubao", ...)`: 统一的 LLM 调用（OpenAI 兼容）
  - 支持 doubao、siliconflow、kimi
- `call_kimi_print(prompt, ...)`: 使用 Kimi CLI 命用（通过 kimi 命令）
- `clean_ai_output(raw_result)`: 清理 AI 输出（移除标签和内部格式）
- `ai_summarize_cached(text, ...)`: 带缓存的 AI 总结

### `crawlers.py`
爬虫工具：
- `cls_telegraphs()`: 财联社电报爬虫
- `get_cninfo_orgid(stock_code)`: 获取巨潮资讯 orgId
- `cninfo_announcement_spider(...)`: 巨潮资讯公告爬虫

---

## Streamlit 特定规范

### 缓存装饰器
```python
@st.cache_data(ttl="1h", show_spinner=False)  # 短期缓存，不显示 spinner
def expensive_operation():
    pass
```

### 使用 container 显示图表
```python
def plotK(df, container=st, ...):
    """
    绘制 K线图
    
    Args:
        df: K线数据
        container: Streamlit container，默认 st
        ...
    """
    # ... 绘制代码
    container.pyplot(fig, use_container_width=True)
```

### 数据展示
```python
# 使用 st.dataframe 展示数据
st.dataframe(df, use_container_width=True)

# 使用 st.metric 展示指标
st.metric("收盘价", f"{latest['close']:.2f}", delta=f"{change_pct:.2f}%")
```

---

## 错误处理

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
try:
    result = risky_operation()
except Exception as e:
    logger.error(f"操作失败: {str(e)}")
    raise  # 返回空 DataFrame 或 None
```

### 数据验证
```python
# 检查 DataFrame 空值
if df is None or df.empty:
    return pd.DataFrame()

# 检查必需列
required_cols = ["open", "close", "high", "low", "volume"]
missing_cols = [col for col in required_cols if col not in df.columns]
if missing_cols:
    raise ValueError(f"缺少必需列: {missing_cols}")
```

---

## API 密钥管理

### 环境变量优先级
```python
import os
import streamlit as st

def get_api_key(provider: str) -> str:
    """
    获取 API 密钥
    
    优先级：环境变量 > Streamlit secrets
    
    Args:
        provider: 提供商名称（tushare/doubao/siliconflow/jina/pushplus）
    
    Returns:
        str: API 密钥
    """
    # 1. 尝试环境变量
    key = os.environ.get(f"{provider.upper()}_API_KEY", "").strip()
    if key:
        return key
    
    # 2. 尝试 Streamlit secrets
    try:
        key = st.secrets.get(f"{provider.lower()}_api_key", "")
        if key:
            return key
    except Exception:
        pass
    
    raise ValueError(f"未配置 {provider} 的 API 密钥")
```

---

## Git 工作流

### Commit 消息规范
```bash
# 格式：<type>(<scope>): <subject>

# 示例：
git commit -m "feat(tools): 新增 K线形态识别功能"
git commit -m "fix(crawlers): 修复巨潮资讯爬虫超时问题"
git commit -m "refactor(stock_data): 拆分数据获取和绘图模块"
git commit -m "docs: 更新项目代码规范文档"
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

## 测试指南

### 数据获取函数测试
```python
# 测试 Tushare 数据获取
try:
    df = get_tushare_price_df("600000.SH")
    assert not df.empty
    assert len(df) <= 60  # count 参数生效
except Exception as e:
    logger.error(f"测试失败: {e}")
```

### K线形态识别测试
```python
from tools import recognize_pattern, KLinePatternRecognizer

# 测试锤子线
recognizer = KLinePatternRecognizer()
pattern = recognizer._recognize_hammer(test_df)
assert pattern is not None
assert pattern.code == "hammer"
```

---

## 常见问题解决方案

### Tushare API 限制
- 免费版每分钟 200 次请求
- 使用 `@st.cache_data()` 减少 API 调用
- 设置合理的 TTL（short=5min, medium=1h, long=1d）

### Streamlit 缓存问题
- 缓存使用函数签名作为键
- 避免在函数内部修改参数后再缓存
- 使用 `hashlib.md5(prompt.encode())` 作为缓存键的一部分

### 数据类型转换
- 使用 `pd.to_numeric(series, errors="coerce")` 处理混合类型
- 处理 NaN：`df.dropna()` 或 `df.fillna()`
- 日期转换：`pd.to_datetime(date_str, errors="coerce")`

---

## 后续开发建议

### 可以添加的功能
1. **更多技术指标**
   - BOLL 布林带
   - ATK 指标
   - 威廉指标
   
2. **更多 K线形态**
   - 三只乌鸦
   - 上升三角形/下降三角形
   - 三只喜鹊
   
3. **数据增强**
   - 实时数据获取
   - 板块数据关联分析
   - 龙虎榜席位追踪
   
4. **AI 功能扩展**
   - 新闻情感分析
   - 研报智能摘要
   - 策略报告生成

### 代码优化方向
1. 使用异步请求加速数据获取
2. 添加单元测试覆盖核心功能
3. 考虑使用类型注解提升 IDE 支持
4. 添加性能监控和日志

---

## 维护信息

**维护者：** chenh

**最后更新：** 2026-03-26

**版本：** v2.0 - 重构后版本

---

## 相关链接

- Tushare API: https://tushare.pro/document/2
- AkShare 文档: https://akshare.akfamily.xyz/
- Plotly 文档: https://plotly.com/python/
- Streamlit 文档: https://docs.streamlit.io/
