# AI 分析模块使用说明

更新时间：2026-03-29

## 当前分层

AI 分析相关代码已经拆成两层：

- [tools/ai_analysis.py](/Users/chenh/Documents/Stocks/ch-stock/tools/ai_analysis.py)
  - 原子能力
  - 负责数据格式化、prompt 构建、单次 AI 调用
- [services/ai_analysis.py](/Users/chenh/Documents/Stocks/ch-stock/services/ai_analysis.py)
  - 业务流程
  - 负责组合数据、调用 AI、处理展示

## 应该怎么用

### 页面和应用层

优先调用 service：

```python
from services.ai_analysis import (
    analyze_external_assets,
    analyze_market_overview,
    analyze_index_technical,
    analyze_stock_classification,
)
```

这些函数适合页面直接使用，因为它们已经包含完整流程控制。

### 底层能力复用

只有在你明确需要自定义流程时，才直接调用 tool：

```python
from tools.ai_analysis import (
    build_macro_prompt,
    build_market_overview_prompt,
    build_index_analysis_prompt,
    build_stock_classification_prompt,
    format_series_for_ai,
    format_market_summary_for_ai,
    format_stock_list_for_classification,
    run_ai_analysis,
)
```

## 当前职责边界

### tools/ai_analysis.py 保留内容

- `format_series_for_ai`
- `format_market_summary_for_ai`
- `format_stock_list_for_classification`
- `build_macro_prompt`
- `build_market_overview_prompt`
- `build_index_analysis_prompt`
- `build_stock_classification_prompt`
- `run_ai_analysis`

### 已迁出 tools 的内容

以下能力不再放在 `tools.ai_analysis`：

- `display_ai_analysis`
- `analyze_external_assets`
- `analyze_market_overview`
- `analyze_index_technical`
- `analyze_stock_classification`

这些都属于业务流程或展示层职责，统一放在 [services/ai_analysis.py](/Users/chenh/Documents/Stocks/ch-stock/services/ai_analysis.py)。

## 典型用法

### 外围资产分析

```python
from services.ai_analysis import analyze_external_assets

result = analyze_external_assets(
    usdcny_series=usdcny_series,
    btc_series=btc_series,
    xau_series=xau_series,
    wti_series=wti_series,
    us10y_series=us10y_series,
    show_ui=False,
)
```

### 股票分类分析

```python
from services.ai_analysis import analyze_stock_classification

result = analyze_stock_classification(
    stock_list=stock_list,
    date_str="2026-03-29",
    show_ui=True,
)
```

### 自定义 prompt + 单次调用

```python
from tools.ai_analysis import build_macro_prompt, run_ai_analysis

prompt = build_macro_prompt(
    usdcny_series=usdcny_series,
    btc_series=btc_series,
    xau_series=xau_series,
    wti_series=wti_series,
    us10y_series=us10y_series,
)
result = run_ai_analysis(prompt, cache_key="macro_demo")
```

## 注意事项

- 页面层不要再从 `tools.ai_analysis` 导入 `analyze_*`
- UI 展示不要重新放回 `tools`
- Prompt 模板基础设施已经迁到 [infra/prompt_templates.py](/Users/chenh/Documents/Stocks/ch-stock/infra/prompt_templates.py)
