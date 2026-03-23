# AI 分析模块使用文档

## 概述

`tools/ai_analysis.py` 提供了统一的 AI 宏观分析功能，支持：

1. **外围资产分析**（汇率、BTC、黄金、原油、美债）
2. **市场全貌分析**（涨跌家数、涨跌停、成交额等）
3. **指数技术分析**（K线形态、均线、支撑阻力）

所有功能都支持：**数据格式化 → Prompt构建 → AI调用 → 结果展示** 的完整流程，且完全解耦。

---

## 快速开始

### 1. 外围资产分析

```python
from tools.ai_analysis import analyze_external_assets

# 获取数据序列（从你的数据源）
usdcny_series = [...]  # 人民币汇率
btc_series = [...]     # 比特币
xau_series = [...]     # 黄金
wti_series = [...]     # WTI原油
us10y_series = [...]   # 美债10Y

# 方式一：自动在 Streamlit UI 中展示
analyze_external_assets(
    usdcny_series=usdcny_series,
    btc_series=btc_series,
    xau_series=xau_series,
    wti_series=wti_series,
    us10y_series=us10y_series,
    show_ui=True
)

# 方式二：仅获取结果，不展示UI
result = analyze_external_assets(
    usdcny_series=usdcny_series,
    btc_series=btc_series,
    xau_series=xau_series,
    wti_series=wti_series,
    us10y_series=us10y_series,
    show_ui=False
)
```

### 2. 市场全貌分析

```python
from tools.ai_analysis import analyze_market_overview

market_data = {
    "上涨": 3500,
    "下跌": 1200,
    "涨停": 85,
    "跌停": 5,
    "成交额": "8500亿",
    "活跃度": "72%"
}

analyze_market_overview(
    market_data=market_data,
    date_str="2024-01-15",
    show_ui=True
)
```

### 3. 指数技术分析

```python
from tools.ai_analysis import analyze_index_technical

index_data = {
    "current_price": 3050.25,
    "ma5": 3045.12,
    "ma20": 3028.45,
    "ma60": 2980.33,
    "volume_trend": "放量",
    "rsi": 58.5,
    "macd": "金叉"
}

analyze_index_technical(
    index_data=index_data,
    index_name="上证指数",
    show_ui=True
)
```

---

## 高级用法

### 自定义分析流程

如果你需要完全自定义分析流程，可以使用底层函数：

```python
from tools.ai_analysis import (
    format_series_for_ai,
    format_market_summary_for_ai,
    run_ai_analysis,
    display_ai_analysis
)
import streamlit as st

# 1. 格式化数据
btc_formatted = format_series_for_ai(btc_series, "比特币")

# 2. 自定义 Prompt
my_prompt = f"""
作为交易员，请分析以下比特币数据：
{btc_formatted}

请给出：
1. 趋势判断
2. 关键价位
3. 操作建议
"""

# 3. 执行分析
result = run_ai_analysis(
    prompt=my_prompt,
    cache_key="my_custom_analysis",
    use_cache=True
)

# 4. 自定义展示
display_ai_analysis(
    title="📊 我的自定义分析",
    ai_result=result,
    expanded=True,
    spinner_text="分析中...",
    help_text="*免责声明：仅供参考*"
)
```

### 修改默认 Prompt 模板

```python
from tools.ai_analysis import build_macro_prompt

# 获取默认 prompt
default_prompt = build_macro_prompt(
    usdcny_series, btc_series, xau_series, wti_series, us10y_series
)

# 在默认基础上追加内容
custom_prompt = default_prompt + "\n\n【额外要求】\n请特别关注中美关系对汇率的影响。"

# 使用自定义 prompt
from tools.ai_analysis import run_ai_analysis, display_ai_analysis

result = run_ai_analysis(custom_prompt, cache_key="custom_macro")
display_ai_analysis(title="自定义宏观分析", ai_result=result)
```

---

## API 参考

### 便捷函数

#### `analyze_external_assets(...)`
外围资产 AI 分析

**参数：**
- `usdcny_series`: 人民币汇率序列（列表）
- `btc_series`: 比特币序列（列表）
- `xau_series`: 黄金序列（列表）
- `wti_series`: WTI原油序列（列表）
- `us10y_series`: 美债10Y收益率序列（列表）
- `show_ui`: 是否在 Streamlit 中展示（默认 True）

**返回：** AI 分析结果文本或 None

---

#### `analyze_market_overview(...)`
市场全貌 AI 分析

**参数：**
- `market_data`: 市场数据字典
- `date_str`: 日期字符串（可选）
- `show_ui`: 是否在 Streamlit 中展示（默认 True）

**返回：** AI 分析结果文本或 None

---

#### `analyze_index_technical(...)`
指数技术分析

**参数：**
- `index_data`: 指数数据字典
- `index_name`: 指数名称（可选）
- `show_ui`: 是否在 Streamlit 中展示（默认 True）

**返回：** AI 分析结果文本或 None

---

### 底层函数

#### `format_series_for_ai(series, asset_name, max_items=60, display_count=10)`
格式化数据序列为 AI 可读的文本格式

**参数：**
- `series`: 数据序列列表，每项包含 date 和 value
- `asset_name`: 资产名称
- `max_items`: 最多保留的记录数
- `display_count`: 在 prompt 中展示的数据条数

**返回：** 格式化的字符串

---

#### `run_ai_analysis(prompt, cache_key="", use_cache=True)`
执行 AI 分析

**参数：**
- `prompt`: 分析提示词
- `cache_key`: 缓存标识
- `use_cache`: 是否使用缓存（默认 True）

**返回：** AI 分析结果文本

---

#### `display_ai_analysis(...)`
统一的 AI 分析展示组件

**参数：**
- `title`: 模块标题
- `ai_result`: 已有的 AI 分析结果（可选）
- `prompt`: AI 分析提示词（如果未提供 ai_result）
- `cache_key`: 缓存键
- `expanded`: 是否默认展开（默认 True）
- `spinner_text`: 加载时显示的文本
- `error_message`: 错误时显示的消息
- `help_text`: 底部提示文本

**返回：** AI 分析结果文本或 None

---

## 缓存策略

- **TTL**: 1小时（`ttl="1h"`）
- **缓存键**: 基于 prompt 的 MD5 hash + 自定义 cache_key
- **自动清理**: Streamlit 自动管理

---

## 集成到其他模块

### 今日大盘模块示例

```python
# 在 display_review_data 的 market 部分
if _show("market"):
    _section_title("今日大盘")
    
    # ... 原有图表代码 ...
    
    # 添加 AI 分析
    from tools.ai_analysis import analyze_market_overview
    
    market_data = {
        "上涨": up_stocks,
        "下跌": down_stocks,
        "涨停": limit_up,
        "跌停": limit_down,
        "成交额": total_amount,
        "活跃度": activity
    }
    
    analyze_market_overview(
        market_data=market_data,
        date_str=date_str,
        show_ui=True
    )
```

### 市场全貌分析示例

```python
# 在 top100 部分添加
if _show("top100"):
    _section_title("市场全貌分析")
    
    # ... 原有分布图表 ...
    
    # 添加 AI 分析
    from tools.ai_analysis import analyze_market_overview
    
    # 从 review_data 提取数据
    market_overview = review_data.get("market_overview", {})
    
    analyze_market_overview(
        market_data=market_overview,
        date_str=review_data.get("date"),
        show_ui=True
    )
```

---

## 最佳实践

1. **优先使用便捷函数**：`analyze_external_assets`, `analyze_market_overview` 等
2. **自定义时使用底层函数**：需要完全控制流程时使用 `run_ai_analysis` + `display_ai_analysis`
3. **合理设置缓存键**：不同场景使用不同的 `cache_key`，避免冲突
4. **处理异常**：AI 调用可能失败，始终检查返回值
5. **数据预处理**：确保传入的数据格式正确（包含 date 和 value 字段的列表）

---

## 故障排除

### AI 分析返回 None
- 检查 API key 是否配置正确
- 检查网络连接
- 查看控制台错误日志

### 缓存不生效
- 确保 `cache_key` 在同一 session 中保持一致
- 检查 prompt 内容是否有变化（任何微小变化都会导致缓存失效）

### 数据格式化错误
- 确保 series 是列表，每项是包含 `date` 和 `value` 的字典
- 检查 `value` 是否为数值类型
