# 📈 ch-stock | A股交易复盘系统

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Tushare](https://img.shields.io/badge/Tushare-Data-green.svg)](https://tushare.pro/)
[![License](https://img.shields.io/badge/license-MIT-lightgrey.svg)]()

> 交易日自动更新 + 复盘看板展示，服务日常盘后复盘和次日决策准备

![Dashboard Preview](docs/preview.png)

---

## ✨ 核心功能

| 功能模块 | 说明 |
|---------|------|
| 📊 **市场概览** | 三大指数K线、成交额趋势、情绪指数、涨跌家数统计 |
| 🌍 **外围指标** | 人民币汇率、BTC、黄金、WTI原油、美债收益率 |
| 📈 **个股分析** | 成交额Top50、10:30前涨停股K线展示 |
| 🔄 **数据更新** | 交易日自动拉取指数、情绪、外盘、融资等数据 |
| 🏢 **投研工具** | 公司基本面分析、股票详情页 |

---

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置 API 密钥

创建 `.streamlit/secrets.toml`：

```toml
tushare_token = "your-tushare-token"
alpha_vantage_key = "your-alpha-vantage-key"  # 可选，用于外围指标
```

### 3. 启动应用

```bash
streamlit run app.py
```

访问 http://localhost:8501

---

## 📸 界面预览

```
┌─────────────────────────────────────────────────────────────┐
│  ch-stock  A股交易复盘系统                    [日期选择]     │
├─────────────────────────────────────────────────────────────┤
│  📊 上证指数    📊 创业板指    📊 科创板指                   │
│  [K线图]        [K线图]        [K线图]                      │
├─────────────────────────────────────────────────────────────┤
│  💹 成交额趋势(万亿)  |  😊 情绪指数  |  💰 融资净买入       │
├─────────────────────────────────────────────────────────────┤
│  📈 上涨vs下跌  |  📉 涨停vs跌停  |  🏆 风格指数            │
├─────────────────────────────────────────────────────────────┤
│  🌍 外围指标: USD/CNY | BTC | XAU | WTI | US10Y            │
├─────────────────────────────────────────────────────────────┤
│  📋 成交额Top50个股K线  |  📋 10:30前涨停股                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 📁 项目结构

```
ch-stock/
├── app.py                      # Streamlit 主应用
├── review_scheduler.py         # 市场数据更新脚本
├── data_sources.py             # 数据源接口
├── tools/
│   ├── market_data.py          # 市场数据获取
│   ├── stock_data.py           # 个股数据与K线绘制
│   ├── financial_data.py       # 外围指标(汇率/黄金/BTC等)
│   └── storage_utils.py        # 数据存储工具
├── pages/
│   └── 10-公司投研专家.py      # 投研分析页面
├── datas/
│   ├── market_data.csv         # 历史市场统计数据
│   └── reviews/                # 每日复盘JSON数据
├── requirements.txt
└── README.md
```

---

## 🛠️ 手动更新数据

```bash
# 更新当日数据
python review_scheduler.py --run-once

# 指定日期更新
python review_scheduler.py --run-once --date 2026-03-23
```

---

## ⚙️ 环境变量

| 变量名 | 必需 | 说明 |
|-------|------|------|
| `TUSHARE_TOKEN` | ✅ | Tushare API Token |
| `ALPHAVANTAGE_API_KEY` | ❌ | Alpha Vantage API Key(外围指标) |

---

## 📝 数据来源

- [Tushare](https://tushare.pro/) - A股基础数据、指数、融资融券
- [AKShare](https://www.akshare.xyz/) - 实时行情、涨停数据
- [Alpha Vantage](https://www.alphavantage.co/) - 外汇、加密货币、大宗商品

---

## 🤝 贡献指南

1. Fork 本仓库
2. 创建功能分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

---

## 📄 License

MIT License © 2026 ch-stock

---

<p align="center">
  Made with ❤️ for A股交易者
</p>
