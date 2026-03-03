# ch-stock

A 股交易复盘与数据更新系统：以**交易日自动更新 + 复盘看板展示**为核心，服务日常盘后复盘和次日决策准备。

---

## 1. 项目目标

`ch-stock` 主要解决三件事：

1. **交易日数据自动拉取并落库**（指数、全市场、情绪、外盘、融资、创业板估值等）
2. **结构化复盘特征生成**（涨幅 Top100 分层、板块分布、K 线形态）
3. **可视化复盘展示**（Streamlit 页面）

适用场景：盘后快速判断市场状态（强弱、风格、情绪）并沉淀可追溯数据。

---

## 2. 系统架构

```text
数据源（Tushare / AkShare / AlphaVantage）
            ↓
  data_updater/trade_day_updater.py
            ↓
        MySQL（结构化落库）
            ↓
      Streamlit 可视化（app.py）
```

核心流程：
- 交易日判断 → 数据抓取 → 指标计算/特征识别 → 入库 → 前端读取展示

---

## 3. 核心能力

### 3.1 交易日更新（Data Pipeline）
- 自动识别是否交易日（Tushare `trade_cal`）
- 更新外盘资产（如 BTC、黄金、美债等）
- 更新三大指数与市场活跃度
- 更新全市场日指标并生成分组（如涨幅 Top100）
- 计算并保存复盘特征（市值分层、成交额分层、板块分布、K 线形态）
- 记录任务日志（成功/失败/原因）

### 3.2 复盘展示（Dashboard）
- 外盘概览与趋势
- 大盘指数与情绪指标
- 全市场涨跌分布
- 涨幅 Top100 结构化特征看板

### 3.3 调度与通知
- 支持 shell 脚本 + 定时任务运行
- 支持 Feishu 通知脚本（任务成功/失败提醒）

---

## 4. 目录结构（当前）

```text
ch-stock/
├── app.py                         # Streamlit 主应用
├── data_updater/
│   └── trade_day_updater.py       # 交易日数据更新主逻辑
├── database/
│   ├── schema.sql                 # MySQL 表结构
│   ├── db_manager.py              # DB 管理
│   └── init_db.py                 # 初始化脚本
├── tools/                         # 指标、图形、存储、形态识别等工具集
├── pages/                         # Streamlit 多页面
├── scripts/                       # 辅助脚本
├── logs/                          # 日志目录
├── run_ch_stock.sh                # 启动看板（streamlit）
├── run_daily_review.sh            # 单次复盘任务脚本
├── run_daily_review_notify.sh     # 复盘+结果通知脚本
├── review_scheduler.py            # 调度入口（支持 --run-once）
├── requirements.txt
└── README.md
```

> 注：历史文档 `README_REVIEW.md` / `README_WEB.md` 保留用于补充说明。

---

## 5. 环境准备

### 5.1 Python 依赖

```bash
pip install -r requirements.txt
```

### 5.2 环境变量（建议）

最少需要：
- `TUSHARE_TOKEN`（必需）
- `DB_HOST` `DB_PORT` `DB_USER` `DB_PASSWORD` `DB_NAME`（MySQL）

可选：
- `ALPHAVANTAGE_API_KEY`（部分外盘数据）
- `FEISHU_OPEN_ID`（通知脚本）

> 建议通过 `.env` 或系统环境变量注入，不要把密钥写入仓库。

---

## 6. 快速开始

### 6.1 初始化数据库

```bash
python database/init_db.py
```

### 6.2 执行一次交易日更新

```bash
python data_updater/trade_day_updater.py 20260303
```

或（Python 调用）：

```python
from data_updater.trade_day_updater import run_daily_update
result = run_daily_update("20260303")
print(result)
```

### 6.3 启动可视化看板

```bash
streamlit run app.py
```

默认访问：`http://127.0.0.1:8501`

---

## 7. 常用运行方式

### 7.1 启动看板（脚本）

```bash
bash run_ch_stock.sh
```

### 7.2 运行一次复盘任务

```bash
bash run_daily_review.sh
```

### 7.3 运行复盘 + Feishu 通知

```bash
bash run_daily_review_notify.sh
```

### 7.4 调度器单次运行

```bash
python review_scheduler.py --run-once --date 2026-03-03 --skip-weekend
```

---

## 8. 关键数据表（节选）

- `job_run_log`：任务执行日志
- `trade_calendar`：交易日历
- `external_asset_daily`：外盘资产日线
- `index_daily`：指数日线
- `market_activity_daily`：市场活跃度（日）
- `stock_daily_basic`：全市场日指标
- `stock_group_member`：分组结果（Top100）
- `gainer_feature_stock`：涨幅 Top100 个股特征
- `gainer_feature_summary`：涨幅 Top100 汇总特征
- `margin_trade_daily`：融资融券
- `gem_pe_daily`：创业板估值

完整定义见：`database/schema.sql`

---

## 9. 故障排查

1. **更新失败**
   - 先看 `logs/` 和 `job_run_log` 的错误信息
   - 检查 Tushare Token 是否失效/限频
   - 检查网络与第三方数据源可用性

2. **数据库异常**
   - 检查 MySQL 连接参数
   - 确认表结构已初始化（`database/init_db.py`）

3. **外盘数据不全**
   - 检查 `ALPHAVANTAGE_API_KEY`
   - 免费额度触发限频时可能出现缺失

---

## 10. 协作约定

项目协作流程参考：`WORKFLOW.md`

核心约定：
- 不在 `main` 直接开发
- 功能分支开发 + 测试确认后合并
- 生产部署仅使用 `main`

---

## License

当前仓库未单独声明开源许可证；默认按仓库所有者内部协作规则使用。