"""
公司投研专家页面
- 左侧2/3：信息面板（公司概况、财务指标、主营构成、股东管理层、市场数据、机构调研）
- 右侧1/3：K线图（日/周/月）
"""

import streamlit as st

# =============================================================================
# AI Prompt 配置（可在侧边栏自定义）
# =============================================================================
DEFAULT_PROMPTS = {
    "company_basic": """你是一位专业的投资分析师，请根据以下公司信息生成一段简洁的总结（80-120字）：

{text}

请涵盖：
1. 主营业务和行业地位
2. 核心竞争力或特色
3. 潜在关注点

要求：简洁专业，突出投资亮点和风险。""",
    "financial": """你是一位资深财务分析师，请对以下原始财务数据进行深度质量评估和异常巡查。

原始财务数据：
{text}

请从以下维度进行专业分析（150-200字）：

1. **盈利质量分析**
   - 扣非净利润与净利润的差异（是否存在非经常性损益粉饰）
   - 毛利率、净利率的稳定性和趋势
   - 利润含金量（经营现金流与净利润匹配度）

2. **资产负债健康度**
   - 资产负债率水平和变化趋势
   - 流动比率、速动比率反映的短期偿债能力
   - 有息负债比例和财务杠杆风险

3. **营运效率评估**
   - 应收账款周转、存货周转效率
   - 总资产周转率趋势
   - 是否存在资产周转放缓风险

4. **异常信号巡查**
   - 收入与利润增长是否匹配
   - 是否存在季节性或周期性异常波动
   - 关键财务指标的突变点和可能原因
   - 潜在的财务粉饰或风险信号

要求：
- 不要简单罗列数据，重在分析质量、趋势和异常
- 指出具体的财务风险点和需关注的指标
- 语言专业简洁，突出核心判断""",
    "shareholders": """请根据以下股东和管理层数据，生成一段简洁的股权结构分析总结（150-200字）：

{text}

请重点分析：
1. **主要股东**：前3大股东是谁？持股性质（国有/民营/机构）？
2. **大股东占比**：前5大、前10大股东合计持股比例？股权集中度如何？
3. **社保基金**：是否有社保基金持股？如有，持股比例多少？
4. **管理层持股**：管理层合计持股数量及占总股本比例？激励机制如何？

要求：
- 数据要准确，提及具体股东名称和持股比例
- 分析股权结构稳定性和治理风险
- 语言简洁专业，适合投资者快速了解股权结构""",
    "research": """你是一位专业的投资分析师，请根据以下机构调研信息进行简要分析（60-100字）：

调研日期：{date}
调研标题：{title}
PDF链接：{pdf_url}

请分析：
1. 调研的主要目的或背景（如新产品、业绩说明、战略规划等）
2. 可能涉及的核心关注点
3. 对投资者的参考价值

要求：简洁专业，突出投资要点，不要复述标题字面意思，重在解读背后的投资逻辑。""",
    "technical": """你是一位资深股票技术分析师，请根据以下K线数据进行专业技术分析。

【股票】{stock_name}

【最新价格数据】
当前价：{current_price:.2f}元
今日涨跌：{change_pct:+.2f}%
今日振幅：{amplitude:.2f}%

【均线系统】
MA5：{ma5:.2f}元 {ma5_signal}
MA10：{ma10:.2f}元 {ma10_signal}
MA20：{ma20:.2f}元 {ma20_signal}
{ma60_line}
均线排列：{ma_alignment}

【MACD指标】
DIF：{macd:.3f}
DEA：{signal:.3f}
MACD柱：{histogram:.3f}
信号：{macd_signal}

【RSI(14)】
当前值：{rsi:.2f}
状态：{rsi_status}

【KDJ(9,3,3)】
K：{k:.2f}
D：{d:.2f}
J：{j:.2f}
信号：{kdj_signal}

【近期K线数据（最近60个交易日）】
{klines}

请提供以下技术分析（简洁专业，200-300字）：

1. **趋势分析**：当前处于什么趋势？均线系统给出什么信号？

2. **技术指标研判**：
   - MACD、RSI、KDJ是否共振？
   - 是否有超买/超卖信号？
   - 是否有金叉/死叉信号？

3. **形态识别**：
   - 是否形成头肩顶/底、双顶/底、三角形等经典形态？
   - 关键支撑/阻力位在哪里？

4. **技术面综合判断**：
   - 多空力量对比
   - 短期操作策略建议（看多/看空/观望）
   - 关键价位（止损/止盈参考）

要求：
- 基于上述数据客观分析
- 给出明确的操作倾向
- 风险提示必不可少""",
}

# 初始化session_state中的prompt配置
for key, default_value in DEFAULT_PROMPTS.items():
    if f"prompt_{key}" not in st.session_state:
        st.session_state[f"prompt_{key}"] = default_value


def get_prompt(key: str) -> str:
    """获取当前配置的prompt"""
    return st.session_state.get(f"prompt_{key}", DEFAULT_PROMPTS.get(key, ""))


def reset_prompts():
    """重置所有prompt为默认值"""
    for key in DEFAULT_PROMPTS:
        st.session_state[f"prompt_{key}"] = DEFAULT_PROMPTS[key]


import streamlit as st
import pandas as pd
import tushare as ts
from datetime import datetime, timedelta
import plotly.graph_objects as go
import subprocess
import os
import akshare as ak
from tools.utils import get_tushare_token, convert_to_ts_code, convert_to_ak_code
from tools.stock_data import (
    get_ak_price_df,
    get_tushare_weekly_df,
    get_tushare_monthly_df,
    plotK,
)
from tools.llm_tools import call_kimi_print, clean_ai_output, ai_summarize_cached
from tools.crawlers import cninfo_announcement_spider, get_cninfo_orgid


# =============================================================================
# 初始化 Tushare Pro API
# =============================================================================
@st.cache_resource
def get_tushare_pro():
    """获取 Tushare Pro API 客户端"""
    token = get_tushare_token()
    if not token:
        return None
    return ts.pro_api(token)


# =============================================================================
# 股票检索相关函数
# =============================================================================
@st.cache_data(ttl="1d")
def get_all_stocks_list():
    """获取所有上市股票列表"""
    pro = get_tushare_pro()
    if pro is None:
        return pd.DataFrame()
    try:
        df = pro.stock_basic(
            list_status="L", fields="ts_code,name,area,industry,market,list_date"
        )
        if df is None or df.empty:
            return pd.DataFrame()
        df["code"] = df["ts_code"].str.split(".").str[0]

        def classify_board(code6):
            if code6.startswith("688") or code6.startswith("689"):
                return "科创板"
            if code6.startswith(("300", "301")):
                return "创业板"
            if code6.startswith("8") or code6.startswith("4"):
                return "北交所"
            if code6.startswith(("0", "3", "6")):
                return "主板"
            return "其他"

        df["board"] = df["code"].apply(classify_board)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl="1h")
def search_stocks(query, limit=10):
    """搜索股票（代码或名称）"""
    if not query:
        return pd.DataFrame()

    query = query.strip()
    all_stocks = get_all_stocks_list()

    if all_stocks.empty:
        return pd.DataFrame()

    if query.isdigit():
        if len(query) == 6:
            mask = all_stocks["code"] == query
        else:
            mask = all_stocks["code"].str.contains(query, na=False)
    else:
        mask = all_stocks["name"].str.contains(query, case=False, na=False)

    result = all_stocks[mask].copy()
    result = result.sort_values("code")

    if len(result) > limit:
        result = result.head(limit)

    return result


def get_stock_info_by_code(ts_code):
    """根据ts_code获取股票完整信息"""
    all_stocks = get_all_stocks_list()
    if all_stocks.empty:
        return None

    result = all_stocks[all_stocks["ts_code"] == ts_code]
    if result.empty:
        return None
    return result.iloc[0].to_dict()


def get_stock_name_by_code(ts_code):
    """根据代码获取股票名称"""
    stock_info = get_stock_info_by_code(ts_code)
    if stock_info:
        return stock_info.get("name", ts_code)
    return ts_code


# =============================================================================
# 数据获取函数
# =============================================================================
@st.cache_data(ttl="1h")
def get_stock_basic_info(ts_code):
    """获取上市公司基本信息"""
    pro = get_tushare_pro()
    if pro is None:
        return None
    try:
        df = pro.stock_company(ts_code=ts_code)
        if df is not None and not df.empty:
            return df.iloc[0].to_dict()
        return None
    except Exception:
        return None


@st.cache_data(ttl="1h")
def get_financial_indicators(ts_code, limit=12):
    """获取财务指标数据"""
    pro = get_tushare_pro()
    if pro is None:
        return None
    try:
        df = pro.fina_indicator(ts_code=ts_code, limit=limit)
        if df is not None and not df.empty:
            df = df.sort_values("end_date", ascending=False)
            return df
        return None
    except Exception:
        return None


@st.cache_data(ttl="1h")
def get_main_business(ts_code, bz_type="P", limit=20):
    """获取主营业务构成"""
    pro = get_tushare_pro()
    if pro is None:
        return None
    try:
        df = pro.fina_mainbz(ts_code=ts_code, type=bz_type)
        if df is not None and not df.empty:
            df = df.sort_values("end_date", ascending=False)
            if len(df) > limit * 5:
                df = df.head(limit * 5)
            return df
        return None
    except Exception:
        return None


@st.cache_data(ttl="1h")
def get_top10_holders(ts_code, limit=4):
    """获取前十大股东"""
    pro = get_tushare_pro()
    if pro is None:
        return None
    try:
        df = pro.top10_holders(ts_code=ts_code)
        if df is not None and not df.empty:
            df = df.sort_values(["end_date", "hold_ratio"], ascending=[False, False])
            latest_dates = df["end_date"].unique()[:limit]
            df = df[df["end_date"].isin(latest_dates)]
            return df
        return None
    except Exception:
        return None


@st.cache_data(ttl="1h")
def get_managers(ts_code):
    """获取管理层信息"""
    pro = get_tushare_pro()
    if pro is None:
        return None
    try:
        df = pro.stk_managers(ts_code=ts_code)
        if df is not None and not df.empty:
            latest_ann_date = df["ann_date"].max()
            df = df[df["ann_date"] == latest_ann_date]
            return df
        return None
    except Exception:
        return None


@st.cache_data(ttl="1h")
def get_manager_rewards(ts_code):
    """获取管理层薪酬和持股"""
    pro = get_tushare_pro()
    if pro is None:
        return None
    try:
        df = pro.stk_rewards(ts_code=ts_code)
        if df is not None and not df.empty:
            latest_end_date = df["end_date"].max()
            df = df[df["end_date"] == latest_end_date]
            return df
        return None
    except Exception:
        return None


@st.cache_data(ttl="1h")
def get_share_float(ts_code, limit=50):
    """获取限售股解禁数据"""
    pro = get_tushare_pro()
    if pro is None:
        return None
    try:
        start_date = datetime.now().strftime("%Y%m%d")
        end_date = (datetime.now() + timedelta(days=365 * 3)).strftime("%Y%m%d")
        df = pro.share_float(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            df = df.sort_values("float_date")
            if len(df) > limit:
                df = df.head(limit)
            return df
        return None
    except Exception:
        return None


@st.cache_data(ttl="1h")
def get_block_trade(ts_code, limit=100):
    """获取大宗交易数据"""
    pro = get_tushare_pro()
    if pro is None:
        return None
    try:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")
        df = pro.block_trade(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            df = df.sort_values("trade_date", ascending=False)
            if len(df) > limit:
                df = df.head(limit)
            return df
        return None
    except Exception:
        return None


@st.cache_data(ttl="1h")
def get_stk_holdertrade(ts_code, limit=100):
    """获取股东增减持数据"""
    pro = get_tushare_pro()
    if pro is None:
        return None
    try:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        df = pro.stk_holdertrade(
            ts_code=ts_code, start_date=start_date, end_date=end_date
        )
        if df is not None and not df.empty:
            df = df.sort_values("ann_date", ascending=False)
            if len(df) > limit:
                df = df.head(limit)
            return df
        return None
    except Exception:
        return None


@st.cache_data(ttl="1h")
def get_stk_holdernumber(ts_code, limit=50):
    """获取股东人数数据"""
    pro = get_tushare_pro()
    if pro is None:
        return None
    try:
        df = pro.stk_holdernumber(ts_code=ts_code)
        if df is not None and not df.empty:
            df = df.sort_values("end_date", ascending=False)
            if len(df) > limit:
                df = df.head(limit)
            return df
        return None
    except Exception:
        return None


@st.cache_data(ttl="1h")
def get_institute_research(code, start_date=None, end_date=None, limit=100):
    """获取机构调研数据（巨潮资讯网）- 使用 code,orgId 格式精确查询"""
    try:
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

        seDate = f"{start_date}~{end_date}"

        # 获取 orgId
        orgId = get_cninfo_orgid(code)
        if orgId is None:
            return None

        # 构造 code,orgId 格式
        stock_param = f"{code},{orgId}"

        all_records = []
        for pageNum in range(1, 20):
            try:
                df = cninfo_announcement_spider(
                    pageNum=pageNum,
                    tabType="relation",
                    stock=stock_param,
                    seDate=seDate,
                )
                if df is None or df.empty:
                    break
                all_records.append(df)
                if len(df) < 30:
                    break
            except Exception:
                break

        if not all_records:
            return None

        result = pd.concat(all_records, ignore_index=True)
        result = result.drop_duplicates(
            subset=["announcementTime", "announcementTitle"]
        )
        result = result.sort_values("announcementTime", ascending=False)

        if len(result) > limit:
            result = result.head(limit)

        return result
    except Exception:
        return None


# =============================================================================
# 缓存辅助函数
# =============================================================================
def get_cache_key(prefix: str, data: dict, prompt_template: str) -> str:
    """生成缓存 key"""
    import hashlib

    # 包含 ts_code 和 data_hash（如果有）来确保唯一性
    ts_code = data.get("ts_code", "")
    data_hash = data.get("data_hash", "")
    holders_hash = data.get("holders_hash", "")
    rewards_hash = data.get("rewards_hash", "")

    key_parts = [prefix, ts_code]
    if data_hash:
        key_parts.append(data_hash)
    if holders_hash:
        key_parts.append(holders_hash)
    if rewards_hash:
        key_parts.append(rewards_hash)

    key_data = (
        f"{'_'.join(key_parts)}_{hashlib.md5(prompt_template.encode()).hexdigest()[:8]}"
    )
    return key_data


def get_dataframe_hash(df: pd.DataFrame) -> str:
    """计算 DataFrame 的 hash 值，用于缓存 key"""
    import hashlib

    if df is None or df.empty:
        return "empty"
    # 使用 DataFrame 的前100行和关键列计算 hash
    df_sample = df.head(100)
    try:
        # 将 DataFrame 转换为 bytes 并计算 hash
        df_bytes = df_sample.to_csv(index=False).encode("utf-8")
        return hashlib.md5(df_bytes).hexdigest()[:16]
    except Exception:
        # 如果失败，使用行数和列名的组合
        return f"{len(df)}_{'_'.join(df.columns[:5])}"


def ai_summarize_research(title: str, date: str, pdf_url: str) -> str:
    """AI 总结机构调研报告"""
    # 使用配置的prompt模板
    prompt_template = get_prompt("research")
    prompt = prompt_template.format(date=date, title=title, pdf_url=pdf_url)

    cache_key = f"research_{date}_{hash(title) % 10000}"
    raw_result = call_kimi_print(prompt, cache_key)
    return clean_ai_output(raw_result)


# =============================================================================
# AI 分析函数
# =============================================================================
def ai_summarize_company_basic(company_info: dict) -> str:
    """AI 总结：公司基本信息"""
    prompt_template = get_prompt("company_basic")

    text = f"""公司名称：{company_info.get("name", "-")}
所属行业：{company_info.get("industry", "-")}
注册地：{company_info.get("area", "-")}
上市日期：{company_info.get("list_date", "-")}
公司介绍：{company_info.get("introduction", "-")[:500]}..."""

    cache_key = get_cache_key(
        "company", {"ts_code": company_info.get("ts_code", "")}, prompt_template
    )
    return ai_summarize_cached(text, prompt_template, cache_key)


def ai_analyze_financial_trend(fina_df: pd.DataFrame, ts_code: str = "") -> str:
    """AI 分析：财务质量评估和异常巡查"""
    if fina_df is None or fina_df.empty:
        return "暂无财务数据可供分析"

    prompt_template = get_prompt("financial")

    # 构建完整的原始财务数据文本
    fina_df = fina_df.sort_values("end_date", ascending=False).head(8)
    text_lines = []

    for _, row in fina_df.iterrows():
        period = row.get("end_date", "-")
        text_lines.append(f"\n【报告期：{period}】")

        # 收入利润指标
        revenue = row.get("total_revenue_ps", None)
        profit = row.get("profit_dedt", None)
        net_profit = row.get("netprofit_margin", None)
        text_lines.append(
            f"  营收：{revenue:.2f}元/股" if pd.notna(revenue) else "  营收：无"
        )
        text_lines.append(
            f"  扣非净利润：{profit:.2f}元" if pd.notna(profit) else "  扣非净利润：无"
        )
        text_lines.append(
            f"  销售净利率：{net_profit:.2f}%"
            if pd.notna(net_profit)
            else "  销售净利率：无"
        )

        # 盈利能力
        roe = row.get("roe", None)
        gross_margin = row.get("grossprofit_margin", None)
        text_lines.append(f"  ROE：{roe:.2f}%" if pd.notna(roe) else "  ROE：无")
        text_lines.append(
            f"  毛利率：{gross_margin:.2f}%"
            if pd.notna(gross_margin)
            else "  毛利率：无"
        )

        # 资产负债
        debt_ratio = row.get("debt_to_assets", None)
        current_ratio = row.get("current_ratio", None)
        quick_ratio = row.get("quick_ratio", None)
        text_lines.append(
            f"  资产负债率：{debt_ratio:.2f}%"
            if pd.notna(debt_ratio)
            else "  资产负债率：无"
        )
        text_lines.append(
            f"  流动比率：{current_ratio:.2f}"
            if pd.notna(current_ratio)
            else "  流动比率：无"
        )
        text_lines.append(
            f"  速动比率：{quick_ratio:.2f}"
            if pd.notna(quick_ratio)
            else "  速动比率：无"
        )

        # 现金流
        cfps = row.get("cfps", None)
        text_lines.append(
            f"  每股现金流：{cfps:.2f}" if pd.notna(cfps) else "  每股现金流：无"
        )

    text = "\n".join(text_lines)

    # 生成包含股票代码和数据 hash 的缓存 key，避免不同股票数据串台
    data_hash = get_dataframe_hash(fina_df)
    cache_key = get_cache_key(
        "fin_trend",
        {"ts_code": ts_code, "periods": len(fina_df), "data_hash": data_hash},
        prompt_template,
    )
    return ai_summarize_cached(text, prompt_template, cache_key)


def ai_analyze_shareholders_and_managers(
    holders_df: pd.DataFrame,
    managers_df: pd.DataFrame,
    rewards_df: pd.DataFrame,
    ts_code: str = "",
) -> str:
    """AI 综合分析：十大股东和管理层"""

    prompt_template = get_prompt("shareholders")

    # 构建分析文本
    text_lines = []

    # 股东数据
    if holders_df is not None and not holders_df.empty:
        latest_period = holders_df["end_date"].iloc[0]
        period_df = holders_df[holders_df["end_date"] == latest_period].copy()
        period_df = period_df.sort_values("hold_ratio", ascending=False)

        text_lines.append(f"【十大股东数据 - 报告期：{latest_period}】")
        text_lines.append(f"股东总数：{len(period_df)}家")

        top5_ratio = (
            period_df.head(5)["hold_ratio"].sum()
            if len(period_df) >= 5
            else period_df["hold_ratio"].sum()
        )
        top10_ratio = period_df["hold_ratio"].sum()
        text_lines.append(f"前5大股东合计持股：{top5_ratio:.2f}%")
        text_lines.append(f"前10大股东合计持股：{top10_ratio:.2f}%")

        text_lines.append("\n主要股东详情：")
        for idx, row in period_df.head(5).iterrows():
            holder_name = row.get("holder_name", "-")
            hold_ratio = row.get("hold_ratio", 0)
            text_lines.append(f"- {holder_name}: {hold_ratio:.2f}%")

        # 检查是否有社保基金
        social_security = period_df[
            period_df["holder_name"].str.contains(
                "社保|全国社会保障", na=False, case=False
            )
        ]
        if not social_security.empty:
            text_lines.append(f"\n社保基金持股：")
            for idx, row in social_security.iterrows():
                text_lines.append(f"- {row['holder_name']}: {row['hold_ratio']:.2f}%")
        else:
            text_lines.append("\n社保基金持股：无")

    # 管理层数据
    if rewards_df is not None and not rewards_df.empty:
        text_lines.append(f"\n【管理层持股数据】")
        total_hold = rewards_df["hold_vol"].sum()
        # 估算总股本
        total_shares = 0
        if holders_df is not None and not holders_df.empty:
            latest_period = holders_df["end_date"].iloc[0]
            period_df = holders_df[holders_df["end_date"] == latest_period]
            if (
                not period_df.empty
                and "hold_ratio" in period_df.columns
                and "hold_amount" in period_df.columns
            ):
                first_row = period_df.iloc[0]
                if first_row["hold_ratio"] > 0:
                    total_shares = first_row["hold_amount"] / (
                        first_row["hold_ratio"] / 100
                    )

        mgmt_ratio = (total_hold / total_shares * 100) if total_shares > 0 else 0
        text_lines.append(f"管理层合计持股：{total_hold:,.0f}股")
        text_lines.append(f"管理层持股比例：{mgmt_ratio:.4f}%")
        text_lines.append(f"管理层人数：{len(rewards_df)}人")

    text = "\n".join(text_lines) if text_lines else "暂无股东及管理层数据"

    # 生成包含股票代码和数据 hash 的缓存 key，避免不同股票数据串台
    holders_hash = get_dataframe_hash(holders_df)
    rewards_hash = get_dataframe_hash(rewards_df)
    cache_key = get_cache_key(
        "shareholders_managers",
        {
            "ts_code": ts_code,
            "holders_hash": holders_hash,
            "rewards_hash": rewards_hash,
        },
        prompt_template,
    )
    return ai_summarize_cached(text, prompt_template, cache_key)


# =============================================================================
# AI 技术分析函数
# =============================================================================
def ai_analyze_technical(df_daily, df_weekly, df_monthly, stock_name="") -> str:
    """AI分析技术形态和指标 - 使用kimi print模式"""
    if df_daily is None or df_daily.empty:
        return "暂无数据可供技术分析"

    # 准备K线数据（取最近60个交易日）
    df = df_daily.tail(60).copy()

    # 构建K线数据文本
    klines = []
    for idx, row in df.iterrows():
        date_str = idx.strftime("%m-%d") if hasattr(idx, "strftime") else str(idx)[:5]
        klines.append(
            f"{date_str}: 开{row['open']:.2f} 高{row['high']:.2f} 低{row['low']:.2f} 收{row['close']:.2f} 量{row['volume'] / 10000:.1f}万"
        )

    # 计算基础均线
    ma5 = df["close"].rolling(5).mean().iloc[-1]
    ma10 = df["close"].rolling(10).mean().iloc[-1]
    ma20 = df["close"].rolling(20).mean().iloc[-1]
    ma60 = df["close"].rolling(60).mean().iloc[-1] if len(df) >= 60 else None

    # 计算MACD
    exp1 = df["close"].ewm(span=12, adjust=False).mean()
    exp2 = df["close"].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False).mean()

    # 计算RSI
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))

    # 计算KDJ
    low_list = df["low"].rolling(window=9, min_periods=9).min()
    high_list = df["high"].rolling(window=9, min_periods=9).max()
    rsv = (df["close"] - low_list) / (high_list - low_list) * 100
    k = rsv.ewm(com=2, adjust=False).mean()
    d = k.ewm(com=2, adjust=False).mean()
    j = 3 * k - 2 * d

    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else latest

    # 准备技术指标数据
    technical_data = {
        "stock_name": stock_name,
        "current_price": latest["close"],
        "change_pct": (latest["close"] / prev["close"] - 1) * 100,
        "amplitude": (latest["high"] / latest["low"] - 1) * 100,
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma60": ma60 if ma60 else 0,
        "ma5_signal": "↑" if latest["close"] > ma5 else "↓",
        "ma10_signal": "↑" if latest["close"] > ma10 else "↓",
        "ma20_signal": "↑" if latest["close"] > ma20 else "↓",
        "ma60_line": f"MA60：{ma60:.2f}元 {'↑' if latest['close'] > ma60 else '↓'}"
        if ma60
        else "",
        "ma_alignment": "多头排列"
        if ma5 > ma10 > ma20
        else "空头排列"
        if ma5 < ma10 < ma20
        else "缠绕",
        "macd": macd.iloc[-1],
        "signal": signal.iloc[-1],
        "histogram": (macd.iloc[-1] - signal.iloc[-1]) * 2,
        "macd_signal": "金叉"
        if macd.iloc[-1] > signal.iloc[-1] and macd.iloc[-2] <= signal.iloc[-2]
        else "死叉"
        if macd.iloc[-1] < signal.iloc[-1] and macd.iloc[-2] >= signal.iloc[-2]
        else "DIF在DEA上方"
        if macd.iloc[-1] > signal.iloc[-1]
        else "DIF在DEA下方",
        "rsi": rsi.iloc[-1],
        "rsi_status": "超买(>70)"
        if rsi.iloc[-1] > 70
        else "超卖(<30)"
        if rsi.iloc[-1] < 30
        else "中性",
        "k": k.iloc[-1],
        "d": d.iloc[-1],
        "j": j.iloc[-1],
        "kdj_signal": "金叉"
        if k.iloc[-1] > d.iloc[-1] and k.iloc[-2] <= d.iloc[-2]
        else "死叉"
        if k.iloc[-1] < d.iloc[-1] and k.iloc[-2] >= d.iloc[-2]
        else "K在D上方"
        if k.iloc[-1] > d.iloc[-1]
        else "K在D下方",
        "klines": "\n".join(klines),
    }

    # 使用配置的prompt模板
    prompt_template = get_prompt("technical")
    prompt = prompt_template.format(**technical_data)

    # 使用kimi print模式直接获取分析结果
    cache_key = f"technical_{stock_name}_{hash(str(latest['close'])) % 100000}"
    return call_kimi_print(prompt, cache_key)


# =============================================================================
# 工具函数
# =============================================================================
def format_number(value, unit="", decimal=2):
    """格式化数字显示"""
    if pd.isna(value) or value is None:
        return "-"
    try:
        num = float(value)
        if abs(num) >= 1e8:
            return f"{num / 1e8:.{decimal}f}亿{unit}"
        elif abs(num) >= 1e4:
            return f"{num / 1e4:.{decimal}f}万{unit}"
        else:
            return f"{num:.{decimal}f}{unit}"
    except:
        return str(value)


def format_percent(value):
    """格式化百分比"""
    if pd.isna(value) or value is None:
        return "-"
    try:
        return f"{float(value):.2f}%"
    except:
        return str(value)


# =============================================================================
# 页面主体
# =============================================================================
def main():
    st.set_page_config(
        page_title="公司投研专家",
        page_icon="📊",
        layout="wide",
    )

    st.title("📊 公司投研专家")

    # 检查 Tushare Token
    pro = get_tushare_pro()
    if pro is None:
        st.error("⚠️ 未配置 Tushare Token")
        st.info("在 .streamlit/secrets.toml 中添加：`tushare_token = '你的token'`")
        return

    # =============================================================================
    # 侧边栏：AI Prompt 配置
    # =============================================================================
    with st.sidebar:
        st.markdown("## 🤖 AI分析配置")

        # 使用expander来组织不同的prompt配置
        with st.expander("📋 公司概况Prompt", expanded=False):
            st.session_state["prompt_company_basic"] = st.text_area(
                "公司概况分析Prompt",
                value=st.session_state.get(
                    "prompt_company_basic", DEFAULT_PROMPTS["company_basic"]
                ),
                height=200,
                key="config_company_basic",
            )

        with st.expander("💰 财务分析Prompt", expanded=False):
            st.session_state["prompt_financial"] = st.text_area(
                "财务分析Prompt",
                value=st.session_state.get(
                    "prompt_financial", DEFAULT_PROMPTS["financial"]
                ),
                height=300,
                key="config_financial",
            )

        with st.expander("👥 股东分析Prompt", expanded=False):
            st.session_state["prompt_shareholders"] = st.text_area(
                "股东分析Prompt",
                value=st.session_state.get(
                    "prompt_shareholders", DEFAULT_PROMPTS["shareholders"]
                ),
                height=250,
                key="config_shareholders",
            )

        with st.expander("🏢 调研分析Prompt", expanded=False):
            st.session_state["prompt_research"] = st.text_area(
                "调研分析Prompt",
                value=st.session_state.get(
                    "prompt_research", DEFAULT_PROMPTS["research"]
                ),
                height=200,
                key="config_research",
            )

        with st.expander("📈 技术分析Prompt", expanded=False):
            st.text(
                "可用变量: {stock_name}, {current_price}, {change_pct}, {amplitude}, {ma5}, {ma10}, {ma20}, {ma60}, {ma5_signal}, {ma10_signal}, {ma20_signal}, {ma60_line}, {ma_alignment}, {macd}, {signal}, {histogram}, {macd_signal}, {rsi}, {rsi_status}, {k}, {d}, {j}, {kdj_signal}, {klines}"
            )
            st.session_state["prompt_technical"] = st.text_area(
                "技术分析Prompt",
                value=st.session_state.get(
                    "prompt_technical", DEFAULT_PROMPTS["technical"]
                ),
                height=400,
                key="config_technical",
            )

        # 重置按钮
        if st.button("🔄 重置为默认Prompt", use_container_width=True):
            reset_prompts()
            st.rerun()

        st.divider()

    # 初始化 session state
    if "search_query" not in st.session_state:
        st.session_state["search_query"] = ""
    if "selected_ts_code" not in st.session_state:
        st.session_state["selected_ts_code"] = None

    # 搜索输入框 - 实时搜索
    search_input = st.text_input(
        "输入股票代码或名称",
        placeholder="例如：000001 或 平安银行（输入后自动搜索）",
        key="search_input",
    )

    # 搜索建议区域
    if search_input and len(search_input.strip()) >= 1:
        search_results = search_stocks(search_input.strip(), limit=15)

        if not search_results.empty:
            st.markdown("**🔍 搜索结果（点击选择）：**")

            cols_per_row = 5
            rows = (len(search_results) + cols_per_row - 1) // cols_per_row

            for row_idx in range(rows):
                cols = st.columns(cols_per_row)
                for col_idx in range(cols_per_row):
                    idx = row_idx * cols_per_row + col_idx
                    if idx < len(search_results):
                        row = search_results.iloc[idx]
                        with cols[col_idx]:
                            btn_label = f"{row['name']} ({row['code']})"
                            if st.button(
                                btn_label,
                                key=f"btn_{row['ts_code']}",
                                use_container_width=True,
                            ):
                                st.session_state["selected_ts_code"] = row["ts_code"]
                                st.session_state["search_query"] = row["name"]
                                st.rerun()

    # 获取当前选中的股票
    ts_code = st.session_state.get("selected_ts_code")

    # 如果用户输入的是完整代码格式，直接解析
    if not ts_code and search_input:
        input_str = search_input.strip()
        if input_str.isdigit() and len(input_str) == 6:
            ts_code = convert_to_ts_code(input_str)
        elif "." in input_str.upper():
            ts_code = convert_to_ts_code(input_str)
        elif input_str:
            results = search_stocks(input_str, limit=1)
            if not results.empty:
                ts_code = results.iloc[0]["ts_code"]

    # 显示选中股票的详情
    if ts_code:
        stock_info = get_stock_info_by_code(ts_code)
        stock_name = (
            stock_info.get("name", ts_code)
            if stock_info
            else get_stock_name_by_code(ts_code)
        )

        # 股票标题栏
        col1, col2, col3 = st.columns([3, 1, 1])
        with col1:
            st.markdown(f"## {stock_name} ({ts_code})")
        with col2:
            if stock_info:
                st.caption(
                    f"板块: {stock_info.get('board', '-')} | 行业: {stock_info.get('industry', '-')}"
                )
        with col3:
            if st.button("🔄 重新搜索", use_container_width=True):
                st.session_state["selected_ts_code"] = None
                st.session_state["search_input"] = ""
                st.rerun()

        st.divider()

        # ========== 左右布局：左侧2/3信息，右侧1/3 K线 ==========
        left_col, right_col = st.columns([2, 1])

        # ========== 右侧：K线图（从上到下直接展示日、周、月） ==========
        with right_col:
            code6 = ts_code.split(".")[0] if "." in ts_code else ts_code[:6]

            # 初始化数据变量
            daily_df = None
            weekly_df = None
            monthly_df = None

            # 日K
            st.markdown("**📈 日线（200日）**")
            daily_df = get_ak_price_df(convert_to_ak_code(ts_code), count=200)
            if daily_df is not None and not daily_df.empty:
                plotK(daily_df, k="d")
            else:
                st.warning("暂无日K数据")

            st.markdown("---")

            # 周K
            st.markdown("**📈 周线（56周）**")
            try:
                weekly_df = get_tushare_weekly_df(code6, count=56)
                if weekly_df is not None and not weekly_df.empty:
                    plotK(weekly_df, k="w")
                else:
                    st.warning("暂无周K数据")
            except Exception as e:
                st.warning(f"周K数据获取失败")

            st.markdown("---")

            # 月K
            st.markdown("**📈 月线（20月）**")
            try:
                monthly_df = get_tushare_monthly_df(code6, count=20)
                if monthly_df is not None and not monthly_df.empty:
                    plotK(monthly_df, k="m")
                else:
                    st.warning("暂无月K数据")
            except Exception as e:
                st.warning(f"月K数据获取失败")

            # AI技术分析
            st.markdown("---")
            st.markdown("**🤖 AI技术分析**")
            with st.spinner("正在进行技术分析..."):
                try:
                    technical_analysis = ai_analyze_technical(
                        daily_df, weekly_df, monthly_df, stock_name
                    )
                    st.info(technical_analysis)
                except Exception as e:
                    st.warning("技术分析暂时不可用")

        # ========== 左侧：信息面板 ==========
        with left_col:
            # 公司基本信息 - 仅展示AI总结
            with st.expander("📋 公司概况", expanded=True):
                company_info = get_stock_basic_info(ts_code)
                if company_info:
                    with st.spinner("正在生成公司分析..."):
                        ai_summary = ai_summarize_company_basic(company_info)
                    st.info(ai_summary)
                else:
                    st.warning("暂无公司信息数据")

            # 分组1：财务指标与主营构成
            with st.expander("💰 财务指标 & 🏭 主营构成", expanded=True):
                fina_df = get_financial_indicators(ts_code, limit=12)
                mainbz_product = get_main_business(ts_code, bz_type="P", limit=20)

                if fina_df is not None and not fina_df.empty:
                    # 准备财务数据 - 提取年份和季度
                    fina_df = fina_df.sort_values("end_date")
                    fina_df["year"] = fina_df["end_date"].str[:4].astype(int)
                    fina_df["quarter"] = (
                        fina_df["end_date"]
                        .str[4:6]
                        .astype(int)
                        .apply(lambda m: (m - 1) // 3 + 1)
                    )
                    fina_df["year_quarter"] = (
                        fina_df["year"].astype(str)
                        + "Q"
                        + fina_df["quarter"].astype(str)
                    )

                    # 获取最近3年的数据
                    years = sorted(fina_df["year"].unique())[-3:]
                    quarters = ["Q1", "Q2", "Q3", "Q4"]
                    colors = {
                        "Q1": "#5470c6",
                        "Q2": "#91cc75",
                        "Q3": "#fac858",
                        "Q4": "#ee6666",
                    }

                    # 图表1：每股营收
                    fig1 = go.Figure()
                    for q in quarters:
                        y_values = []
                        for y in years:
                            row = fina_df[
                                (fina_df["year"] == y)
                                & (fina_df["quarter"] == int(q[1]))
                            ]
                            if not row.empty:
                                y_values.append(row["total_revenue_ps"].values[0])
                            else:
                                y_values.append(0)
                        fig1.add_trace(
                            go.Bar(
                                name=q,
                                x=[f"{y}年" for y in years],
                                y=y_values,
                                marker_color=colors[q],
                            )
                        )

                    fig1.update_layout(
                        height=350,
                        barmode="group",
                        xaxis_title="年度",
                        yaxis_title="每股营收(元)",
                        legend_title="季度",
                        bargap=0.2,
                        bargroupgap=0.1,
                        xaxis={"type": "category"},
                    )
                    st.plotly_chart(fig1, use_container_width=True)

                    # 图表2：每股扣非净利润
                    fig2 = go.Figure()
                    for q in quarters:
                        y_values = []
                        for y in years:
                            row = fina_df[
                                (fina_df["year"] == y)
                                & (fina_df["quarter"] == int(q[1]))
                            ]
                            if not row.empty:
                                y_values.append(row["profit_dedt"].values[0])
                            else:
                                y_values.append(0)
                        y_title = "每股扣非净利润(元)"
                        fig2.add_trace(
                            go.Bar(
                                name=q,
                                x=[f"{y}年" for y in years],
                                y=y_values,
                                marker_color=colors[q],
                            )
                        )

                    fig2.update_layout(
                        height=350,
                        barmode="group",
                        xaxis_title="年度",
                        yaxis_title=y_title,
                        legend_title="季度",
                        bargap=0.2,
                        bargroupgap=0.1,
                        xaxis={"type": "category"},
                    )
                    st.plotly_chart(fig2, use_container_width=True)

                    # 图表3：ROE
                    fig3 = go.Figure()
                    for q in quarters:
                        y_values = []
                        for y in years:
                            row = fina_df[
                                (fina_df["year"] == y)
                                & (fina_df["quarter"] == int(q[1]))
                            ]
                            if not row.empty:
                                y_values.append(row["roe"].values[0])
                            else:
                                y_values.append(0)
                        fig3.add_trace(
                            go.Bar(
                                name=q,
                                x=[f"{y}年" for y in years],
                                y=y_values,
                                marker_color=colors[q],
                            )
                        )

                    fig3.update_layout(
                        height=350,
                        barmode="group",
                        xaxis_title="年度",
                        yaxis_title="ROE(%)",
                        legend_title="季度",
                        bargap=0.2,
                        bargroupgap=0.1,
                        xaxis={"type": "category"},
                    )
                    st.plotly_chart(fig3, use_container_width=True)

                    # AI分析财务趋势
                    with st.spinner("正在分析财务数据..."):
                        financial_summary = ai_analyze_financial_trend(fina_df, ts_code)
                    st.info(financial_summary)
                else:
                    st.warning("暂无财务指标数据")

                # 主营构成 - 分产品
                if mainbz_product is not None and not mainbz_product.empty:
                    st.markdown("##### 主营构成 - 分产品")

                    periods = mainbz_product["end_date"].unique()[:2]
                    if len(periods) >= 1:

                        def format_year(date_str):
                            return date_str[2:4] + "年"

                        latest_data = mainbz_product[
                            mainbz_product["end_date"] == periods[0]
                        ].head(8)
                        latest_year_label = format_year(periods[0])

                        fig4 = go.Figure()

                        # 先添加较旧的年份（绿色，图例在前）
                        if len(periods) >= 2:
                            prev_data = mainbz_product[
                                mainbz_product["end_date"] == periods[1]
                            ].head(8)
                            prev_year_label = format_year(periods[1])
                            prev_dict = dict(
                                zip(prev_data["bz_item"], prev_data["bz_sales"])
                            )
                            prev_sales = [
                                prev_dict.get(item, 0) / 1e8
                                for item in latest_data["bz_item"]
                            ]
                            fig4.add_trace(
                                go.Bar(
                                    x=latest_data["bz_item"],
                                    y=prev_sales,
                                    name=prev_year_label,
                                    marker_color="#91cc75",
                                )
                            )

                        # 后添加较新的年份（蓝色，图例在后）
                        fig4.add_trace(
                            go.Bar(
                                x=latest_data["bz_item"],
                                y=latest_data["bz_sales"] / 1e8,
                                name=latest_year_label,
                                marker_color="#5470c6",
                            )
                        )

                        fig4.update_layout(
                            height=350,
                            xaxis_title="产品",
                            yaxis_title="销售收入(亿元)",
                            barmode="group",
                            xaxis_tickangle=-45,
                            legend_traceorder="normal",
                        )
                        st.plotly_chart(fig4, use_container_width=True)

                # 主营构成 - 分地区
                mainbz_region = get_main_business(ts_code, bz_type="D", limit=30)
                if mainbz_region is not None and not mainbz_region.empty:
                    st.markdown("##### 主营构成 - 分地区")

                    periods = mainbz_region["end_date"].unique()[:2]
                    if len(periods) >= 1:

                        def format_year(date_str):
                            return date_str[2:4] + "年"

                        latest_data = mainbz_region[
                            mainbz_region["end_date"] == periods[0]
                        ].head(8)
                        latest_year_label = format_year(periods[0])

                        fig5 = go.Figure()

                        # 先添加较旧的年份（浅蓝色，图例在前）
                        if len(periods) >= 2:
                            prev_data = mainbz_region[
                                mainbz_region["end_date"] == periods[1]
                            ].head(8)
                            prev_year_label = format_year(periods[1])
                            prev_dict = dict(
                                zip(prev_data["bz_item"], prev_data["bz_sales"])
                            )
                            prev_sales = [
                                prev_dict.get(item, 0) / 1e8
                                for item in latest_data["bz_item"]
                            ]
                            fig5.add_trace(
                                go.Bar(
                                    x=latest_data["bz_item"],
                                    y=prev_sales,
                                    name=prev_year_label,
                                    marker_color="#73c0de",
                                )
                            )

                        # 后添加较新的年份（红色，图例在后）
                        fig5.add_trace(
                            go.Bar(
                                x=latest_data["bz_item"],
                                y=latest_data["bz_sales"] / 1e8,
                                name=latest_year_label,
                                marker_color="#ee6666",
                            )
                        )

                        fig5.update_layout(
                            height=350,
                            xaxis_title="地区",
                            yaxis_title="销售收入(亿元)",
                            barmode="group",
                            xaxis_tickangle=-45,
                            legend_traceorder="normal",
                        )
                        st.plotly_chart(fig5, use_container_width=True)
                else:
                    st.warning("暂无主营构成数据")

            # 分组2：十大股东和管理层（合并AI分析）
            with st.expander("👥 十大股东 & 👔 管理层", expanded=True):
                holders_df = get_top10_holders(ts_code, limit=4)
                managers_df = get_managers(ts_code)
                rewards_df = get_manager_rewards(ts_code)

                if (
                    (holders_df is not None and not holders_df.empty)
                    or (managers_df is not None and not managers_df.empty)
                    or (rewards_df is not None and not rewards_df.empty)
                ):
                    with st.spinner("正在分析股东与管理层..."):
                        shareholder_manager_summary = (
                            ai_analyze_shareholders_and_managers(
                                holders_df, managers_df, rewards_df, ts_code
                            )
                        )
                    st.info(shareholder_manager_summary)
                else:
                    st.warning("暂无股东及管理层数据")

            # 分组3：限售解禁、大宗交易、增减持、股东人数（无AI总结）
            with st.expander(
                "🔓 限售解禁 & 📦 大宗交易 & 📈 增减持 & 👨‍👩‍👧‍👦 股东人数",
                expanded=True,
            ):
                float_df = get_share_float(ts_code)
                block_df = get_block_trade(ts_code)
                trade_df = get_stk_holdertrade(ts_code)
                holder_num_df = get_stk_holdernumber(ts_code)

                sub_tabs = st.tabs(["限售解禁", "大宗交易", "增减持", "股东人数"])

                with sub_tabs[0]:
                    if float_df is not None and not float_df.empty:
                        total_float_share = float_df["float_share"].sum()
                        avg_float_ratio = float_df["float_ratio"].mean()

                        col1, col2, col3 = st.columns(3)
                        col1.metric("待解禁记录数", len(float_df))
                        col2.metric(
                            "合计解禁股份", format_number(total_float_share, "股", 0)
                        )
                        col3.metric("平均占比", f"{avg_float_ratio:.2f}%")

                        float_df["float_date"] = pd.to_datetime(float_df["float_date"])
                        display_df = float_df[
                            [
                                "float_date",
                                "holder_name",
                                "float_share",
                                "float_ratio",
                                "share_type",
                            ]
                        ].copy()
                        display_df.columns = [
                            "解禁日期",
                            "股东名称",
                            "流通股份",
                            "占总股本比例",
                            "股份类型",
                        ]
                        display_df["流通股份"] = display_df["流通股份"].apply(
                            lambda x: format_number(x, "股", 0)
                        )
                        display_df["占总股本比例"] = display_df["占总股本比例"].apply(
                            format_percent
                        )

                        st.dataframe(
                            display_df, use_container_width=True, hide_index=True
                        )
                    else:
                        st.info("📭 暂无待解禁数据")

                with sub_tabs[1]:
                    if block_df is not None and not block_df.empty:
                        total_amount = block_df["amount"].sum()
                        total_vol = block_df["vol"].sum()

                        col1, col2, col3 = st.columns(3)
                        col1.metric("交易笔数", len(block_df))
                        col2.metric("合计成交金额", format_number(total_amount, "元"))
                        col3.metric(
                            "合计成交股数", format_number(total_vol * 10000, "股", 0)
                        )

                        display_df = block_df[
                            ["trade_date", "price", "vol", "amount", "buyer", "seller"]
                        ].copy()
                        display_df.columns = [
                            "交易日期",
                            "成交价",
                            "成交量(万股)",
                            "成交金额(元)",
                            "买方营业部",
                            "卖方营业部",
                        ]
                        display_df["成交金额(元)"] = display_df["成交金额(元)"].apply(
                            lambda x: format_number(x, "元")
                        )

                        st.dataframe(
                            display_df, use_container_width=True, hide_index=True
                        )
                    else:
                        st.info("📭 近90天内暂无大宗交易数据")

                with sub_tabs[2]:
                    if trade_df is not None and not trade_df.empty:
                        increase_df = trade_df[trade_df["in_de"] == "IN"]
                        decrease_df = trade_df[trade_df["in_de"] == "DE"]

                        col1, col2, col3 = st.columns(3)
                        col1.metric("增减持记录", len(trade_df))
                        col2.metric("增持记录", len(increase_df))
                        col3.metric("减持记录", len(decrease_df))

                        display_df = trade_df[
                            [
                                "ann_date",
                                "holder_name",
                                "holder_type",
                                "in_de",
                                "change_vol",
                                "after_share",
                            ]
                        ].copy()
                        display_df.columns = [
                            "公告日期",
                            "股东名称",
                            "股东类型",
                            "增减持",
                            "变动数量(万股)",
                            "变动后持股(万股)",
                        ]
                        display_df["增减持"] = display_df["增减持"].map(
                            {"IN": "增持", "DE": "减持"}
                        )

                        st.dataframe(
                            display_df, use_container_width=True, hide_index=True
                        )
                    else:
                        st.info("📭 近一年内暂无增减持数据")

                with sub_tabs[3]:
                    if holder_num_df is not None and not holder_num_df.empty:
                        latest_num = holder_num_df.iloc[0]["holder_num"]
                        prev_num = (
                            holder_num_df.iloc[1]["holder_num"]
                            if len(holder_num_df) > 1
                            else latest_num
                        )
                        change_pct = (
                            (latest_num - prev_num) / prev_num * 100
                            if prev_num > 0
                            else 0
                        )

                        col1, col2 = st.columns(2)
                        col1.metric("最新股东人数", f"{latest_num:,.0f}人")
                        col2.metric("环比变化", f"{change_pct:+.2f}%")

                        chart_df = holder_num_df.sort_values("end_date")
                        fig = go.Figure()
                        fig.add_trace(
                            go.Scatter(
                                x=chart_df["end_date"],
                                y=chart_df["holder_num"],
                                mode="lines+markers",
                                line=dict(color="#5470c6", width=2),
                                marker=dict(size=6),
                                name="股东人数",
                            )
                        )
                        fig.add_trace(
                            go.Scatter(
                                x=chart_df["end_date"],
                                y=chart_df["holder_num"],
                                fill="tozeroy",
                                fillcolor="rgba(84, 112, 198, 0.1)",
                                line=dict(width=0),
                                showlegend=False,
                            )
                        )
                        fig.update_layout(
                            height=350,
                            xaxis_title="日期",
                            yaxis_title="股东人数",
                            showlegend=False,
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("📭 暂无股东人数数据")

            # 分组4：机构调研
            with st.expander("🏢 机构调研", expanded=True):
                code6 = ts_code.split(".")[0] if "." in ts_code else ts_code[:6]
                research_df = get_institute_research(code6, limit=100)

                if research_df is not None and not research_df.empty:
                    total_records = len(research_df)
                    latest_date = (
                        research_df["announcementTime"].iloc[0]
                        if "announcementTime" in research_df.columns
                        else "-"
                    )

                    col1, col2 = st.columns(2)
                    col1.metric("调研公告数", total_records)
                    col2.metric("最新公告日期", latest_date)

                    # AI 总结前2份调研报告
                    st.markdown("##### 🤖 AI 调研总结（最新2份）")
                    for idx in range(min(2, len(research_df))):
                        row = research_df.iloc[idx]
                        with st.spinner(f"正在分析第 {idx + 1} 份调研报告..."):
                            research_summary = ai_summarize_research(
                                title=row["announcementTitle"],
                                date=row["announcementTime"],
                                pdf_url=row["adjunctUrl"],
                            )
                        with st.container(border=True):
                            st.markdown(
                                f"**{row['announcementTime']} - {row['announcementTitle']}**"
                            )
                            st.info(research_summary)

                    # 紧凑的表格显示所有记录
                    st.markdown("##### 📋 全部调研记录")
                    display_df = (
                        research_df[
                            ["announcementTime", "announcementTitle", "adjunctUrl"]
                        ]
                        .head(10)
                        .copy()
                    )
                    display_df.columns = ["日期", "标题", "PDF链接"]

                    st.dataframe(
                        display_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "日期": st.column_config.TextColumn("日期", width="small"),
                            "标题": st.column_config.TextColumn("标题", width="large"),
                            "PDF链接": st.column_config.LinkColumn(
                                "PDF", width="small", display_text="📄"
                            ),
                        },
                    )
                else:
                    st.info("📭 暂无机构调研记录")


if __name__ == "__main__":
    main()
