# daily_basic 存储与同步说明

更新时间：2026-03-29

## 当前分层

`daily_basic` 相关能力已经拆成三层：

- [infra/daily_basic_repository.py](/Users/chenh/Documents/Stocks/ch-stock/infra/daily_basic_repository.py)
  - 纯仓储
  - 负责保存、查询、存在性检查、最近同步日期等
- [services/daily_basic_service.py](/Users/chenh/Documents/Stocks/ch-stock/services/daily_basic_service.py)
  - 业务流程
  - 负责本地优先查询、缺失回源、缺失日期计算
- [services/daily_basic_sync.py](/Users/chenh/Documents/Stocks/ch-stock/services/daily_basic_sync.py)
  - 同步任务
  - 负责历史补录、最近 N 天同步、状态查看

## 已移除的旧入口

以下旧入口已经废弃并删除：

- `tools.daily_basic_storage`
- `tools.daily_basic_storage.get_daily_basic_smart`
- `tools.daily_basic_storage.query_daily_basic`
- `tools.daily_basic_storage.save_daily_basic_sync`
- `tools.daily_basic_storage.save_daily_basic_async`

不要再从 `tools` 导入 `daily_basic` 相关能力。

## 应该怎么用

### 业务调用

```python
from services.daily_basic_service import get_daily_basic_smart

df = get_daily_basic_smart(trade_date="20260327", use_cache=True)
```

这是默认推荐入口：

- 先查本地
- 本地缺失时回源 Tushare
- 成功后写回 repository

### 仓储调用

```python
from infra.daily_basic_repository import (
    query_daily_basic,
    save_daily_basic_sync,
    save_daily_basic_async,
    check_data_existence,
    get_last_sync_date,
)
```

这些函数只适合 service 或同步脚本使用，不建议页面直接调用。

### 同步脚本

```bash
python services/daily_basic_sync.py sync_all 20260101
python services/daily_basic_sync.py sync_recent 7
python services/daily_basic_sync.py status
```

## 当前职责边界

### infra/daily_basic_repository.py

只负责：

- `save_daily_basic_sync`
- `save_daily_basic_many`
- `save_daily_basic_async`
- `query_daily_basic`
- `check_data_existence`
- `get_last_sync_date`
- `get_database_path`

### services/daily_basic_service.py

负责：

- `get_daily_basic_smart`
- `get_missing_dates`

### services/daily_basic_sync.py

负责：

- 历史补录
- 最近 N 天同步
- 状态查询

## 设计约束

- `daily_basic` 的数据库存取不允许再回到 `tools`
- 页面层不要直接拼接“查本地 + 回源 + 写回”流程
- 配置读取统一通过 [infra/config.py](/Users/chenh/Documents/Stocks/ch-stock/infra/config.py)
