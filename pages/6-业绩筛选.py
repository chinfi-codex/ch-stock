import os
import streamlit as st
import pandas as pd
import akshare as ak
import mplfinance as mpf
import tushare as ts
from tools.llm_tools import get_llm_response
from tools.stock_data import get_ak_price_df
import datetime


UNWANTED_COLUMNS = ['序号']
NET_PROFIT_THRESHOLD = 1e8
REVENUE_GROWTH_THRESHOLD = 30
NON_RECURRING_GROWTH_THRESHOLD = 30


@st.cache_data(ttl="6h")
def load_report(report_type: str, date_str: str) -> pd.DataFrame:
    try:
        if report_type == "业绩报告":
            df = ak.stock_yjbb_em(date=date_str)
        else:
            df = load_forecast_from_ts(date_str)
        if df is None or len(df) == 0:
            return pd.DataFrame()
        if '股票代码' in df.columns:
            df['股票代码'] = df['股票代码'].astype(str)
        # 预处理：将关键数值字段转为数值
        df = preprocess_numeric_fields(df)
        return df
    except Exception:
        return pd.DataFrame()


def preprocess_numeric_fields(df: pd.DataFrame) -> pd.DataFrame:
    dfc = df.copy()
    numeric_like_cols = [
        '业绩变动幅度',
        '业绩变动幅度下限',
        '预测数值',
        '上年同期值',
        '净利润-同比增长',
        '营业总收入-同比增长',
        '净利润-净利润',
        '营业总收入-营业总收入',
        '净利润-扣除非经常性损益后的净利润',
        '净利润-扣除非经常性损益后的净利润-同比增长',
        '扣非净利润-同比增长',
    ]
    for c in numeric_like_cols:
        if c in dfc.columns:
            # 去掉可能存在的百分号后转数值
            dfc[c] = pd.to_numeric(
                dfc[c].astype(str).str.replace('%', '', regex=False).str.replace(',', '', regex=False),
                errors='coerce'
            )
    return dfc


def drop_unwanted_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in UNWANTED_COLUMNS if c in df.columns]
    if not cols:
        return df
    return df.drop(columns=cols)


def filter_base(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame()
    filtered = df.copy()
    if '股票简称' in filtered.columns:
        filtered = filtered[~filtered['股票简称'].astype(str).str.contains('ST', na=False)]
    if '股票代码' in filtered.columns:
        filtered = filtered[~filtered['股票代码'].str.startswith(('9', '8'))]
    return filtered.reset_index(drop=True)


def _get_ts_client():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("缺少环境变量 TUSHARE_TOKEN，无法使用 tushare 接口")
    ts.set_token(token)
    return ts.pro_api(token)


def _format_symbol(ts_code: str) -> str:
    if not ts_code:
        return ""
    return ts_code.split(".")[0]


def load_forecast_from_ts(period: str) -> pd.DataFrame:
    """
    使用 tushare forecast 接口获取业绩预告数据，字段对齐现有展示。
    """
    try:
        pro = _get_ts_client()
    except Exception:
        return pd.DataFrame()

    try:
        df = pro.forecast(period=period)
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    dfc = df.copy()
    dfc["股票代码"] = dfc["ts_code"].apply(_format_symbol)
    dfc["股票简称"] = dfc["股票代码"]
    dfc["公告日期"] = dfc.get("ann_date")

    # 选用区间上限作为业绩变动幅度/预测数值，若为空则用下限
    pct = dfc.get("p_change_max")
    pct_fallback = dfc.get("p_change_min")
    if pct is not None:
        dfc["业绩变动幅度"] = pct
    elif pct_fallback is not None:
        dfc["业绩变动幅度"] = pct_fallback

    net_max = dfc.get("net_profit_max")
    net_min = dfc.get("net_profit_min")
    if net_max is not None:
        dfc["预测数值"] = net_max
        dfc["业绩变动幅度下限"] = pct_fallback
    if net_min is not None and "预测数值" not in dfc:
        dfc["预测数值"] = net_min

    if "last_parent_net" in dfc.columns:
        dfc["上年同期值"] = dfc["last_parent_net"]
    if "change_reason" in dfc.columns:
        dfc["业绩变动原因"] = dfc["change_reason"]
    dfc["预测指标"] = "净利润"

    keep_cols = [
        "股票代码",
        "股票简称",
        "公告日期",
        "业绩变动幅度",
        "业绩变动幅度下限",
        "预测数值",
        "上年同期值",
        "业绩变动原因",
        "预测指标",
    ]
    # 仅保留需要的列，避免后续展示冲突
    dfc = dfc[[c for c in keep_cols if c in dfc.columns]]
    return dfc


def _pick_first_available(df: pd.DataFrame, candidates: list[str]) -> str:
    for col in candidates:
        if col in df.columns:
            return col
    return ""


def apply_default_filters(df: pd.DataFrame, report_type: str) -> tuple[pd.DataFrame, list[str]]:
    """加载后默认应用的基础筛选条件。"""
    if df is None or df.empty:
        return pd.DataFrame(), []

    # 业绩预告：仅使用业绩变动幅度列，要求大于 30%
    if report_type == "业绩预告":
        if '业绩变动幅度' in df.columns:
            filtered = df[pd.to_numeric(df['业绩变动幅度'], errors='coerce') > 30].copy()
            notes = ["业绩变动幅度 > 30%（业绩预告）"]
            return filtered.reset_index(drop=True), notes
        return pd.DataFrame(), ["未找到业绩变动幅度列，无法按业绩预告默认筛选"]

    filtered = df.copy()
    notes: list[str] = []

    net_profit_col = _pick_first_available(filtered, ['净利润-净利润'])
    if net_profit_col:
        filtered = filtered[pd.to_numeric(filtered[net_profit_col], errors='coerce') > NET_PROFIT_THRESHOLD]
        notes.append("净利润 > 1 亿元")
    else:
        notes.append("未找到净利润列，跳过净利润过滤")

    revenue_growth_col = _pick_first_available(filtered, ['营业总收入-同比增长'])
    if revenue_growth_col:
        filtered = filtered[pd.to_numeric(filtered[revenue_growth_col], errors='coerce') > REVENUE_GROWTH_THRESHOLD]
        notes.append("营业收入增速 > 30%")
    else:
        notes.append("未找到营业收入增速列，跳过营业收入过滤")

    non_recurring_growth_col = _pick_first_available(
        filtered,
        ['净利润-扣除非经常性损益后的净利润-同比增长', '扣非净利润-同比增长'],
    )
    if non_recurring_growth_col:
        filtered = filtered[
            pd.to_numeric(filtered[non_recurring_growth_col], errors='coerce') > NON_RECURRING_GROWTH_THRESHOLD
        ]
        notes.append("扣非净利润增速 > 30%")
    else:
        notes.append("未找到扣非净利润增速列，跳过扣非净利润过滤")

    return filtered.reset_index(drop=True), notes


def _get_announcement_date(row: pd.Series) -> str | None:
    for col in ['最新公告日期', '公告日期', '公告日']:
        if col in row and pd.notna(row[col]):
            return str(row[col])
    return None


def _find_highlight_ts(df: pd.DataFrame, announce_date: str | None):
    if not announce_date:
        return None
    try:
        target = pd.to_datetime(announce_date)
    except Exception:
        return None
    if target in df.index:
        return target
    try:
        idx = df.index.get_indexer([target], method='nearest')
        if len(idx) and idx[0] != -1:
            return df.index[idx[0]]
    except Exception:
        return None
    return None


def plot_kline_grid(df: pd.DataFrame, max_charts: int = 12):
    if len(df) == 0 or '股票代码' not in df.columns:
        st.info("没有可绘制K线的数据")
        return

    to_plot = df.head(max_charts).copy()
    st.caption(f"展示前 {len(to_plot)} 只股票的日K线（每行四个，业绩公布日标注箭头）")

    rows = [to_plot.iloc[i:i + 4] for i in range(0, len(to_plot), 4)]
    for row_df in rows:
        cols = st.columns(4)
        for col, (_, row) in zip(cols, row_df.iterrows()):
            with col:
                stock_name = row.get('股票简称', '')
                code = str(row['股票代码'])
                announce_date = _get_announcement_date(row)
                st.markdown(f"**{stock_name} ({code})**")
                with st.spinner("加载K线..."):
                    try:
                        price_df = get_ak_price_df(code, count=180)
                    except Exception as e:
                        st.warning(f"获取K线失败: {e}")
                        continue

                    if price_df is None or price_df.empty:
                        st.warning("无K线数据")
                        continue

                    highlight_ts = _find_highlight_ts(price_df, announce_date)
                    fig, axes = mpf.plot(
                        price_df,
                        type='candle',
                        mav=(5, 10, 20),
                        volume=True,
                        style='yahoo',
                        returnfig=True,
                        figsize=(6, 4),
                    )
                    ax_price = axes[0] if isinstance(axes, (list, tuple)) else axes

                    if highlight_ts is not None:
                        high_price = float(price_df.loc[highlight_ts, 'high'])
                        ax_price.annotate(
                            '',
                            xy=(highlight_ts, high_price),
                            xytext=(highlight_ts, high_price * 1.06),
                            arrowprops=dict(facecolor='red', shrink=0.05, width=1.5, headwidth=10),
                        )
                        ax_price.text(
                            highlight_ts,
                            high_price * 1.08,
                            "业绩公布",
                            color='red',
                            ha='center',
                            va='bottom',
                            fontsize=8,
                        )

                    st.pyplot(fig)


def color_growth(val):
    try:
        v = float(val)
    except Exception:
        return ''
    if v < 0:
        return 'background-color: #e6f4ea; color: #0f8f2c'  # 负增长标绿
    if v > 10:
        return 'background-color: #ffe5e5; color: #b00000'  # >10% 高增长标红
    return ''


def style_overview(df: pd.DataFrame):
    growth_cols = [
        c
        for c in df.columns
        if c in ['业绩变动幅度', '净利润-同比增长', '营业总收入-同比增长']
    ]
    styler = df.style.applymap(color_growth, subset=growth_cols)
    if growth_cols:
        percent_fmt = {c: "{:.2f}%" for c in growth_cols}
        styler = styler.format(percent_fmt)

    # 金额列（显示为"xx亿"，最多两位小数）
    money_cols = [
        c
        for c in df.columns
        if c in ['预测数值', '上年同期值', '净利润-净利润', '营业总收入-营业总收入']
    ]

    def _format_to_yi(x):
        try:
            v = float(x) / 1e8
            s = f"{v:.2f}".rstrip('0').rstrip('.')
            return f"{s}亿"
        except Exception:
            return "N/A"

    if money_cols:
        styler = styler.format({c: _format_to_yi for c in money_cols})
    return styler


def get_report_period_options(count: int = 8) -> list[str]:
    today = datetime.date.today()
    quarter_ends = [(12, 31), (9, 30), (6, 30), (3, 31)]
    options: list[str] = []
    year = today.year
    while len(options) < count:
        for month, day in quarter_ends:
            dt = datetime.date(year, month, day)
            if dt <= today:
                options.append(dt.strftime("%Y%m%d"))
                if len(options) >= count:
                    break
        year -= 1
    return options


def format_period_label(date_str: str) -> str:
    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"


# 界面
# 配置区域
st.subheader("🔧 筛选配置")

# 报告类型和业绩期
report_type = st.selectbox("业绩类型", options=["业绩预告", "业绩报告"], index=0)
period_options = get_report_period_options()
selected_period = st.selectbox(
    "业绩期", options=period_options, index=0, format_func=format_period_label
)

if "load_requested" not in st.session_state:
    st.session_state.load_requested = False

if st.button("加载数据"):
    st.session_state.load_requested = True

report_df = pd.DataFrame()
base_df = pd.DataFrame()
filtered_df = pd.DataFrame()
filter_notes: list[str] = []
if st.session_state.load_requested:
    report_df = load_report(report_type, selected_period)
    base_df = filter_base(report_df)
    if report_type == "业绩预告":
        # 业绩预告默认展示当期全部数据（保留后续用户自选的筛选项）
        filtered_df = base_df.copy()
        filter_notes = []
    else:
        filtered_df, filter_notes = apply_default_filters(base_df, report_type)

change_min = None
change_max = None
selected_indicator = ""

if report_type == "业绩预告":
    # 筛选参数配置
    with st.expander("📊 业绩预告筛选参数", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            change_min = st.number_input(
                "业绩变动幅度下限 (%)",
                min_value=-200.0,
                max_value=500.0,
                value=-50.0,
                step=1.0,
                help="筛选业绩变动幅度大于等于此阈值的股票"
            )
        with col2:
            change_max = st.number_input(
                "业绩变动幅度上限 (%)",
                min_value=-200.0,
                max_value=500.0,
                value=200.0,
                step=1.0,
                help="筛选业绩变动幅度小于等于此阈值的股票"
            )

        indicator_options = []
        source_df = filtered_df if len(filtered_df) > 0 else base_df
        if '预测指标' in source_df.columns:
            indicator_options = sorted([t for t in source_df['预测指标'].dropna().unique()])
        if indicator_options:
            default_indicator = "净利润" if "净利润" in indicator_options else indicator_options[0]
            selected_indicator = st.selectbox(
                "预测指标",
                options=indicator_options,
                index=indicator_options.index(default_indicator),
            )
        else:
            st.caption("未发现可用的预测指标选项")

st.divider()


if len(report_df) == 0:
    st.warning("未获取到数据或数据为空")
elif len(filtered_df) == 0:
    st.warning("默认筛选条件下没有符合的股票")
    if filter_notes:
        st.info("默认筛选条件：" + "；".join(filter_notes))
else:
    working_df = filtered_df.copy()
    if filter_notes:
        st.info("默认筛选条件：" + "；".join(filter_notes))
    if selected_indicator and report_type == "业绩预告":
        working_df = working_df[working_df['预测指标'] == selected_indicator]
    if (
        report_type == "业绩预告"
        and '业绩变动幅度' in working_df.columns
        and change_min is not None
        and change_max is not None
    ):
        if change_min > change_max:
            st.error("业绩变动幅度下限不能大于上限")
        else:
            working_df = working_df[
                (working_df['业绩变动幅度'] >= change_min)
                & (working_df['业绩变动幅度'] <= change_max)
            ]
    working_df = working_df.reset_index(drop=True)

    base_view = drop_unwanted_columns(working_df)
    if report_type == "业绩预告":
        preferred_cols = [
            '股票简称',
            '业绩变动幅度',
            '预测数值',
            '上年同期值',
            '业绩变动原因',
            '公告日期',
        ]
    else:
        preferred_cols = [
            '股票简称',
            '净利润-净利润',
            '净利润-同比增长',
            '营业总收入-营业总收入',
            '营业总收入-同比增长',
            '净资产收益率',
            '最新公告日期',
        ]
    show_cols = [c for c in preferred_cols if c in base_view.columns]
    display_df = base_view[show_cols].rename(columns={'股票简称': '股票名称'})

    st.subheader(f"📋 {report_type}数据")
    st.caption(f"共筛选出 {len(display_df)} 只股票")
    if len(display_df) == 0:
        st.warning("筛选后无数据可显示")
    else:
        st.write(style_overview(display_df))
        st.divider()
        st.subheader("📈 K线图")
        plot_kline_grid(working_df)
