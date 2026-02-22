# 股票每日复盘系统 - Web 版

## 架构

```
前端 (HTML/CSS/JS) <--REST API--> Flask 后端 <--MySQL--> 数据库
```

## 目录结构

```
web_app/
├── api.py              # Flask API 后端
├── app.py              # 应用入口
├── static/
│   ├── css/
│   │   └── style.css   # 英为财情风格样式
│   └── js/
│       └── app.js      # 前端逻辑 + ECharts 图表
└── templates/
    └── index.html      # 主页面
```

## 快速开始

### 1. 安装依赖

```bash
pip install flask flask-cors pymysql
```

### 2. 配置环境变量

确保设置了以下环境变量：

```bash
# Windows CMD
set DB_HOST=47.90.205.168
set DB_PORT=3306
set DB_USER=boss_remote
set DB_PASSWORD=0V1-fE4Aui_G8G@XY@_h
set DB_NAME=stock_review
set TUSHARE_TOKEN=your_token

# PowerShell
$env:DB_HOST="47.90.205.168"
$env:DB_PORT="3306"
$env:DB_USER="boss_remote"
$env:DB_PASSWORD="0V1-fE4Aui_G8G@XY@_h"
$env:DB_NAME="stock_review"
```

### 3. 启动服务

```bash
python start_web.py
```

访问: http://localhost:5000

## API 接口

| 接口 | 说明 |
|------|------|
| GET /api/check_date/<date> | 检查日期数据状态 |
| GET /api/external_assets/<date> | 外围资产数据 |
| GET /api/index_data/<date> | 三大指数数据 |
| GET /api/market_activity/<date> | 市场活跃度 |
| GET /api/margin_data/<date> | 融资融券 |
| GET /api/gem_pe/<date> | 创业板PE |
| GET /api/gainer_distribution/<date>/<type> | 涨幅Top100分布 |
| GET /api/all_stocks_distribution/<date> | 全市场分布 |
| POST /api/update_data | 手动更新数据 |

## 特性

- **专业金融风格**: 深色主题，参考英为财情配色
- **响应式设计**: 支持桌面端和移动端
- **实时图表**: 使用 ECharts 绘制专业金融图表
- **数据缓存**: API 响应缓存，提升加载速度
- **移动端适配**: 完整的移动端响应式布局

## 图表类型

- K线图（含MA5/20/60）
- 折线图（成交额、情绪指数、PE趋势）
- 双折线图（涨跌家数、涨跌停数量）
- 柱状图（融资净买入、涨跌幅分布）
- 饼图（市值/成交额/板块/形态分布）
- 迷你折线图（外盘资产趋势）
