# PR 创建指南

## 方式一：GitHub Web 界面创建（推荐）

1. 访问 https://github.com/chinfi-codex/ch-stock
2. 点击 "Pull requests" → "New pull request"
3. 选择 base: master ← compare: master（或从 commit 创建分支）
4. 填写 PR 标题和描述（使用下方模板）
5. 点击 "Create pull request"

## 方式二：GitHub CLI 创建

```bash
# 如果 gh 已登录
git checkout -b feature/stock-detail-page HEAD~1
git push origin feature/stock-detail-page
git checkout master

# 创建 PR
gh pr create --title "feat: 新增股票详情页面 - 全面个股分析功能" \
  --body-file pr_description.md \
  --base master \
  --head feature/stock-detail-page
```

## PR 描述模板

```markdown
## 📌 功能概述
新增股票详情页面，提供全面的个股信息展示与分析功能，包括公司概况、财务指标、股东管理、市场数据、机构调研等模块。

## 🎯 解决的问题
- 原系统缺少个股深度分析页面，用户无法查看单个股票的详细信息
- 需要集成 AI 分析能力，自动解读公司基本面和机构调研内容
- K 线图使用 Matplotlib，交互性差且与整体 Plotly 风格不一致

## ✨ 主要功能

### 1. 股票搜索与选择
- 支持按股票代码或名称实时搜索
- 显示搜索结果列表，点击选择

### 2. 公司概况（AI 总结）
- 使用 Kimi AI 自动生成公司基本面分析
- 涵盖主营业务、核心竞争力、潜在关注点

### 3. 财务指标可视化
- 每股营收、每股扣非净利润、ROE 季度对比图
- 最近 3 年数据，分季度展示
- AI 财务质量评估和异常巡查

### 4. 主营构成分析
- 分产品、分地区收入对比
- 近两年数据对比

### 5. 股东与管理层
- 前十大股东分析
- 管理层持股和薪酬
- AI 综合分析股权结构

### 6. 市场数据
- 限售股解禁
- 大宗交易
- 股东增减持
- 股东人数趋势

### 7. 机构调研（AI 总结）
- 最新 2 份调研报告 AI 自动解读
- 调研记录列表表格展示

### 8. K 线图（Plotly）
- 日 K、周 K、月 K 三档周期
- 支持缩放、平移交互
- 显示 5/10/20/60/144/250 日均线

## 🧪 测试步骤

### 前置条件
```bash
# 确保依赖已安装
pip install -r requirements.txt

# 确保 Tushare Token 已配置
# 在 .streamlit/secrets.toml 中添加：
# tushare_token = "your_token_here"
```

### 测试命令
```bash
# 启动应用
streamlit run app.py

# 访问 http://localhost:8501
# 导航到"股票详情"页面
```

### 功能测试清单
- [ ] 股票搜索：输入"000001"或"平安银行"测试搜索功能
- [ ] 公司概况：AI 总结是否正常显示
- [ ] 财务指标：图表是否加载，数据是否正确
- [ ] K 线图：日/周/月切换是否正常，交互是否流畅
- [ ] 机构调研：列表是否紧凑显示，AI 总结前 2 份报告
- [ ] 数据缓存：重复访问同一股票是否使用缓存

## 📦 依赖项

### 新增依赖
```
plotly>=5.0.0          # K 线图和图表
pandas>=2.0.0          # 数据处理
akshare>=1.10.0        # A 股数据获取
tushare>=1.2.0         # 财务数据获取
streamlit>=1.28.0      # Web 界面
```

### 外部服务
- **Tushare Pro**: 财务指标、股东数据、K 线数据
- **AkShare**: 日 K 线数据
- **Kimi AI**: 公司概况、财务、调研报告分析（可选，有降级方案）

## 🔧 技术实现

### 核心文件变更
| 文件 | 变更类型 | 说明 |
|------|----------|------|
| `pages/10-股票详情.py` | 新增 | 股票详情主页面（1200+ 行）|
| `tools/stock_data.py` | 修改 | Plotly K 线图实现，替代 Matplotlib |
| `tools/moonshot_api.py` | 新增 | Moonshot API 备用方案 |

### 关键设计
1. **数据缓存**: 使用 `@st.cache_data` 缓存 Tushare 接口数据（TTL 1小时）
2. **AI 分析缓存**: 内存缓存避免重复调用 AI
3. **错误降级**: AI 服务不可用时显示友好提示
4. **编码处理**: Windows 环境下 kim-cli 编码问题修复

## 📊 性能考虑
- 财务指标只获取最近 12 期数据
- 机构调研限制最多 100 条记录
- AI 分析仅对最新 2 份调研报告执行

## ✅ CI 检查 / 手动测试

### 测试环境
- OS: Windows 11
- Python: 3.13
- Streamlit: 1.40.0

### 测试命令及输出
```bash
$ streamlit run app.py

  You can now view your Streamlit app in your browser.

  Local URL: http://localhost:8501
  Network URL: http://192.168.31.42:8501

C:\Users\chenh\Documents\Stocks\ch-stock\tools\stock_data.py:241: FutureWarning:
'M' is deprecated and will be removed in a future version, please use 'ME' instead.
```

### 功能测试结果
| 测试项 | 状态 | 备注 |
|--------|------|------|
| 页面加载 | ✅ 通过 | 无错误 |
| 股票搜索 | ✅ 通过 | 实时搜索正常 |
| K 线图显示 | ✅ 通过 | Plotly 交互正常 |
| AI 公司分析 | ✅ 通过 | 输出格式已修复 |
| AI 财务分析 | ✅ 通过 | 多维度财务评估 |
| 机构调研总结 | ✅ 通过 | 前2份报告 AI 解读 |
| 数据缓存 | ✅ 通过 | 二次加载更快 |

### 已知问题
- ⚠️ FutureWarning: 'M' 将在未来版本中使用 'ME'（不影响当前功能，可后续修复）

## 📝 后续优化建议
1. 添加更多技术指标（MACD、KDJ、RSI）
2. 集成实时行情数据
3. 添加股票对比功能
4. 支持更多 AI 模型选择

## 🔗 相关链接
- Tushare API: https://tushare.pro/document/2
- AkShare 文档: https://akshare.akfamily.xyz/
- Plotly 文档: https://plotly.com/python/
```
