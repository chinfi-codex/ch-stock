# Agent Guidelines for ch-stock

Repository for Chinese stock market analysis with Streamlit dashboard.

## Build & Run Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run Streamlit app
streamlit run app.py

# Run scheduler (single execution)
python review_scheduler.py --run-once --date 2026-03-19

# Run scheduler (daily at 18:30)
python review_scheduler.py --time 18:30
```

## Code Style Guidelines

### Python Style
- **Shebang**: `#!/usr/bin/env python` with `# -*- coding: utf-8 -*-`
- **Line length**: Max 120 characters
- **Indent**: 4 spaces, no tabs
- **Encoding**: UTF-8 for all files

### Imports Order
```python
# 1. Standard library
import os
import sys
from datetime import datetime, timedelta

# 2. Third-party
import pandas as pd
import numpy as np
import streamlit as st
import akshare as ak
import tushare as ts

# 3. Local modules
from tools import get_market_data, get_all_stocks
from tools.stock_data import get_ak_price_df
```

### Naming Conventions
- **Functions**: `snake_case` (e.g., `get_market_data`, `fetch_stock_info`)
- **Variables**: `snake_case` (e.g., `trade_date`, `stock_list`)
- **Constants**: `UPPER_SNAKE` (e.g., `MAX_RETRIES`, `DEFAULT_TIMEOUT`)
- **Private**: `_prefix` for internal use (e.g., `_get_token`, `_format_data`)

### Type Hints
Use type hints for function signatures:
```python
def get_stock_data(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """获取股票数据"""
    pass
```

### Documentation
- Use docstrings for all public functions
- Include Args and Returns sections
- Write docstrings in Chinese for this codebase

```python
def fetch_zt_list(trade_date: str = "") -> list:
    """
    获取涨停股票列表

    Args:
        trade_date: 交易日期，格式 "YYYYMMDD"，空字符串表示当天

    Returns:
        list: 涨停股票列表，按封板时间排序
    """
```

### Error Handling
- Use specific exceptions when possible
- Log warnings for recoverable errors
- Provide fallback behavior for external API failures
- Use try-except blocks for network calls

```python
try:
    df = pro.daily(ts_code=ts_code)
except Exception as e:
    logging.warning("API call failed: %s", e)
    return pd.DataFrame()  # Return empty fallback
```

### Streamlit Specific
- Use `@st.cache_data()` for expensive operations
- Set appropriate TTL (e.g., `ttl="10m"` for 10 minutes)
- Handle session state carefully
- Use `unsafe_allow_html=True` sparingly

### Data Processing
- Check DataFrame emptiness: `if df is None or df.empty:`
- Handle NaN values explicitly with `pd.notna()` or `dropna()`
- Convert numeric columns: `pd.to_numeric(series, errors="coerce")`
- Format dates consistently: `pd.to_datetime(date_str, errors="coerce")`

### API Keys & Secrets
- Never commit API keys to git
- Use environment variables or `.streamlit/secrets.toml`
- Access via: `os.environ.get("TUSHARE_TOKEN")` or `st.secrets.get("tushare_token")`

### Git Workflow
- Commit frequently with clear messages
- Push after completing features
- Clean up temp/test files before final commit
- Remove __pycache__ and .pyc files (in .gitignore)

### Testing
- No formal test suite in this project
- Test manually by running the app: `streamlit run app.py`
- Verify data fetching functions work with sample dates
- Check error handling with invalid inputs

### File Organization
```
ch-stock/
├── app.py                 # Main Streamlit entry
├── review_scheduler.py    # Data collection scheduler
├── data_sources.py        # Data source interfaces
├── requirements.txt
├── tools/                 # Utility modules
│   ├── __init__.py
│   ├── market_data.py
│   ├── stock_data.py
│   └── utils.py
├── pages/                 # Streamlit multipage
└── datas/                 # Data storage (CSV/JSON)
```
