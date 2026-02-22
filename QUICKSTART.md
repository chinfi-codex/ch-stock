# 股票复盘系统 - 快速启动指南

## 系统架构

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐
│   前端页面       │────▶│  Flask API   │────▶│   MySQL     │
│  (英为财情风格)  │     │              │     │  数据库     │
└─────────────────┘     └──────────────┘     └─────────────┘
```

## 已创建的文件

```
ch-stock/
├── database/                    # 数据库模块
│   ├── schema.sql              # 表结构
│   ├── db_manager.py           # 数据库连接
│   └── init_db.py              # 初始化脚本
├── data_updater/                # 数据更新
│   └── trade_day_updater.py    # 数据爬取+计算
├── tools/
│   └── kline_patterns.py       # K线形态识别
├── web_app/                     # Web 前端
│   ├── api.py                  # Flask API
│   ├── app.py                  # 应用入口
│   ├── static/
│   │   ├── css/style.css       # 英为财情风格样式
│   │   └── js/app.js           # 图表交互
│   └── templates/index.html    # 主页面
├── start_web.py                # 启动脚本
├── README_REVIEW.md            # 完整文档
└── README_WEB.md               # Web版文档
```

## 第一步：配置环境变量

在 Windows PowerShell 中执行：

```powershell
$env:DB_HOST="47.90.205.168"
$env:DB_PORT="3306"
$env:DB_USER="boss_remote"
$env:DB_PASSWORD="0V1-fE4Aui_G8G@XY@_h"
$env:DB_NAME="stock_review"
$env:TUSHARE_TOKEN="your_tushare_token"
```

## 第二步：启动 Web 服务

```bash
python start_web.py
```

访问: http://localhost:5000

## 第三步：更新数据（首次使用）

在页面上：
1. 选择日期（如 2025-02-20）
2. 点击「更新数据」按钮
3. 等待数据获取完成
4. 点击「加载数据」查看

## 页面功能

### 1. 外盘数据
- BTC/USD、黄金、汇率、美债
- 120日趋势迷你图

### 2. 大盘数据
- 三大指数实时卡片
- 上证指数K线图（含MA5/20/60）
- 成交额、情绪指数、融资净买入
- 涨跌家数、涨跌停数量、创业板PE

### 3. 市场全貌
- 全市场涨跌幅分布
- 涨幅Top100：市值分层、成交额分层
- 涨幅Top100：板块分布、K线形态分布

## 技术栈

- **后端**: Flask + PyMySQL
- **前端**: HTML5 + CSS3 + Vanilla JS
- **图表**: ECharts 5
- **数据**: Tushare + AkShare

## 设计风格

- **配色**: 深色主题（#0a0e1a 背景）
- **涨跌**: 绿色涨(#00d084) / 红色跌(#ff4d4d)
- **强调色**: 蓝色(#2962ff)
- **响应式**: 支持桌面/平板/手机
