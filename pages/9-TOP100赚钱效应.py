import time
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from tools.top100_effect import (
    compute_top100_profit_effect,
    fetch_top100_layered_features_by_date,
    fetch_stock_kline_with_limit,
    validate_trade_day,
)


def _render_bar_chart(title: str, stats: list[dict], colors: list[str] | None = None, height: int = 340):
    if not stats:
        st.info(f"{title}: 暂无数据")
        return

    labels = [str(x.get("label", "")) for x in stats]
    counts = [int(x.get("count", 0)) for x in stats]
    ratios = [float(x.get("ratio", 0.0)) for x in stats]
    texts = [f"{c} ({r:.0%})" for c, r in zip(counts, ratios)]

    fig = go.Figure(
        go.Bar(
            x=labels,
            y=counts,
            customdata=ratios,
            text=texts,
            textposition="outside",
            marker_color=colors,
            hovertemplate="%{x}<br>数量: %{y}<br>占比: %{customdata:.1%}<extra></extra>",
        )
    )
    fig.update_layout(
        title=title,
        xaxis_title="分类",
        yaxis_title="数量",
        height=height,
        margin=dict(t=45, l=10, r=10, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_layered_trend_charts(history_df: pd.DataFrame):
    if history_df is None or history_df.empty:
        return

    view = history_df.copy()
    view["trade_date"] = pd.to_datetime(view["trade_date"], errors="coerce")
    view = view.dropna(subset=["trade_date"]).sort_values("trade_date")
    if view.empty:
        return

    # 剔除无有效分层数据日期：分层/板块字段全空或全0视为无数据
    data_cols = [
        "turnover_lt_5e8",
        "turnover_5e8_to_50",
        "turnover_50e8_to_90",
        "turnover_gt_90e8",
        "mktcap_lt_5e9",
        "mktcap_5e9_to_10",
        "mktcap_10e9_to_20",
        "mktcap_20e9_to_50",
        "mktcap_gt_50e9",
        "board_main",
        "board_gem",
        "board_star",
    ]
    for col in data_cols:
        if col in view.columns:
            view[col] = pd.to_numeric(view[col], errors="coerce")
    existing_cols = [c for c in data_cols if c in view.columns]
    if existing_cols:
        valid_mask = view[existing_cols].notna().any(axis=1) & (view[existing_cols].fillna(0).sum(axis=1) > 0)
        view = view[valid_mask].copy()
    if view.empty:
        st.info("趋势图无有效日期数据（已剔除空数据日期）")
        return

    # 关键趋势：高成交占比、高市值占比、创业/科创占比
    view["high_turnover_ratio"] = (
        pd.to_numeric(view.get("turnover_50e8_to_90"), errors="coerce").fillna(0)
        + pd.to_numeric(view.get("turnover_gt_90e8"), errors="coerce").fillna(0)
    ) / 100.0
    view["high_mktcap_ratio"] = (
        pd.to_numeric(view.get("mktcap_20e9_to_50"), errors="coerce").fillna(0)
        + pd.to_numeric(view.get("mktcap_gt_50e9"), errors="coerce").fillna(0)
    ) / 100.0
    view["growth_board_ratio"] = (
        pd.to_numeric(view.get("board_gem"), errors="coerce").fillna(0)
        + pd.to_numeric(view.get("board_star"), errors="coerce").fillna(0)
    ) / 100.0

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=view["trade_date"],
            y=view["high_turnover_ratio"],
            mode="lines+markers",
            name="高成交占比(>=50亿)",
            line=dict(color="#c0392b", width=2),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=view["trade_date"],
            y=view["high_mktcap_ratio"],
            mode="lines+markers",
            name="高市值占比(>=200亿)",
            line=dict(color="#2980b9", width=2),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=view["trade_date"],
            y=view["growth_board_ratio"],
            mode="lines+markers",
            name="成长板占比(创业+科创)",
            line=dict(color="#16a085", width=2),
        )
    )
    fig.update_layout(
        title="Top100结构变化趋势（占比）",
        yaxis=dict(title="占比", tickformat=".0%"),
        xaxis_title="日期",
        height=330,
        margin=dict(t=45, l=10, r=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    st.plotly_chart(fig, use_container_width=True)

    board_fig = go.Figure()
    board_fig.add_trace(
        go.Scatter(
            x=view["trade_date"],
            y=pd.to_numeric(view.get("board_main"), errors="coerce").fillna(0) / 100.0,
            stackgroup="one",
            mode="lines",
            name="主板",
            line=dict(width=0.8, color="#7f8c8d"),
        )
    )
    board_fig.add_trace(
        go.Scatter(
            x=view["trade_date"],
            y=pd.to_numeric(view.get("board_gem"), errors="coerce").fillna(0) / 100.0,
            stackgroup="one",
            mode="lines",
            name="创业板",
            line=dict(width=0.8, color="#f39c12"),
        )
    )
    board_fig.add_trace(
        go.Scatter(
            x=view["trade_date"],
            y=pd.to_numeric(view.get("board_star"), errors="coerce").fillna(0) / 100.0,
            stackgroup="one",
            mode="lines",
            name="科创板",
            line=dict(width=0.8, color="#8e44ad"),
        )
    )
    board_fig.update_layout(
        title="板块构成趋势（100只占比堆叠）",
        yaxis=dict(title="占比", tickformat=".0%", range=[0, 1]),
        xaxis_title="日期",
        height=320,
        margin=dict(t=45, l=10, r=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    st.plotly_chart(board_fig, use_container_width=True)


def _build_layered_change_table(history_df: pd.DataFrame) -> pd.DataFrame:
    if history_df is None or history_df.empty:
        return pd.DataFrame()

    view = history_df.copy()
    view["trade_date"] = pd.to_datetime(view["trade_date"], errors="coerce")
    view = view.dropna(subset=["trade_date"]).sort_values("trade_date", ascending=False).reset_index(drop=True)
    if view.empty:
        return pd.DataFrame()

    view["高成交占比(>=50亿)"] = (
        pd.to_numeric(view.get("turnover_50e8_to_90"), errors="coerce").fillna(0)
        + pd.to_numeric(view.get("turnover_gt_90e8"), errors="coerce").fillna(0)
    ) / 100.0
    view["高市值占比(>=200亿)"] = (
        pd.to_numeric(view.get("mktcap_20e9_to_50"), errors="coerce").fillna(0)
        + pd.to_numeric(view.get("mktcap_gt_50e9"), errors="coerce").fillna(0)
    ) / 100.0
    view["成长板占比(创业+科创)"] = (
        pd.to_numeric(view.get("board_gem"), errors="coerce").fillna(0)
        + pd.to_numeric(view.get("board_star"), errors="coerce").fillna(0)
    ) / 100.0

    view["Δ高成交占比"] = view["高成交占比(>=50亿)"] - view["高成交占比(>=50亿)"].shift(-1)
    view["Δ高市值占比"] = view["高市值占比(>=200亿)"] - view["高市值占比(>=200亿)"].shift(-1)
    view["Δ成长板占比"] = view["成长板占比(创业+科创)"] - view["成长板占比(创业+科创)"].shift(-1)

    view["日期"] = view["trade_date"].dt.strftime("%Y-%m-%d")
    cols = [
        "日期",
        "高成交占比(>=50亿)",
        "Δ高成交占比",
        "高市值占比(>=200亿)",
        "Δ高市值占比",
        "成长板占比(创业+科创)",
        "Δ成长板占比",
    ]
    return view[cols]


def _format_detail_df(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df is None or detail_df.empty:
        return pd.DataFrame()

    view = detail_df.copy()
    bool_cols = [
        "is_limit_up_today",
        "is_box_120",
        "pattern_low_breakout",
        "pattern_low_first_board",
        "pattern_box_breakout_120",
        "pattern_new_high_500",
        "pattern_box_bottom_start",
    ]
    for col in bool_cols:
        if col in view.columns:
            view[col] = view[col].map(lambda x: "是" if bool(x) else "否")

    for col in ["pct", "amount_yi", "mkt_cap_yi", "pos_120", "amount_ratio_20", "ma20_slope_10", "box_width_120", "hit_ratio_120"]:
        if col in view.columns:
            view[col] = pd.to_numeric(view[col], errors="coerce")

    rename_map = {
        "rank": "排名",
        "code": "代码",
        "name": "名称",
        "pct": "涨幅%",
        "amount_yi": "成交额(亿)",
        "mkt_cap_yi": "市值(亿)",
        "board_type": "板块",
        "pos_120": "pos_120",
        "amount_ratio_20": "amount_ratio_20",
        "limit_streak": "连板数",
        "is_limit_up_today": "当日涨停",
        "is_box_120": "is_box_120",
        "pattern_low_breakout": "低位突破",
        "pattern_low_first_board": "低位首板",
        "pattern_box_breakout_120": "120日新高箱体突破",
        "pattern_new_high_500": "历史新高(500D)",
        "pattern_box_bottom_start": "箱体底部区间启动",
        "ma20_slope_10": "slope(MA20,10)",
        "box_width_120": "box_width_120",
        "hit_ratio_120": "hit_ratio_120",
    }
    view = view.rename(columns=rename_map)

    display_cols = [
        "排名",
        "代码",
        "名称",
        "涨幅%",
        "成交额(亿)",
        "市值(亿)",
        "板块",
        "pos_120",
        "amount_ratio_20",
        "连板数",
        "当日涨停",
        "is_box_120",
        "box_width_120",
        "hit_ratio_120",
        "slope(MA20,10)",
        "低位突破",
        "低位首板",
        "120日新高箱体突破",
        "历史新高(500D)",
        "箱体底部区间启动",
    ]
    display_cols = [c for c in display_cols if c in view.columns]
    return view[display_cols]


def _build_kline_figure(kline_df: pd.DataFrame, title: str) -> go.Figure:
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=kline_df["trade_date"],
                open=kline_df["open"],
                high=kline_df["high"],
                low=kline_df["low"],
                close=kline_df["close"],
                increasing_line_color="#e74c3c",
                decreasing_line_color="#27ae60",
                showlegend=False,
            )
        ]
    )
    fig.update_layout(
        title=title,
        xaxis_rangeslider_visible=False,
        height=260,
        margin=dict(t=40, l=10, r=10, b=10),
    )
    return fig


def _render_pattern_kline_grid(details_df: pd.DataFrame, trade_date: str):
    if details_df is None or details_df.empty:
        st.info("暂无K线展示数据")
        return

    pattern_config = [
        ("pattern_low_breakout", "低位突破"),
        ("pattern_low_first_board", "低位首板"),
        ("pattern_box_breakout_120", "120日新高箱体突破"),
        ("pattern_new_high_500", "历史新高(500D)"),
        ("pattern_box_bottom_start", "箱体底部区间启动"),
    ]
    trade_date_str = pd.to_datetime(trade_date, errors="coerce").strftime("%Y%m%d")

    for pattern_col, pattern_label in pattern_config:
        if pattern_col not in details_df.columns:
            continue

        hit_df = details_df[details_df[pattern_col] == True].copy()
        if "rank" in hit_df.columns:
            hit_df = hit_df.sort_values("rank")
        hit_df = hit_df.head(8)

        st.markdown(f"#### {pattern_label}（最多展示8只）")
        if hit_df.empty:
            st.info("该分类当日无命中股票")
            continue

        for start in range(0, len(hit_df), 4):
            cols = st.columns(4)
            chunk = hit_df.iloc[start : start + 4]
            for i, (_, row) in enumerate(chunk.iterrows()):
                with cols[i]:
                    ts_code = str(row.get("ts_code", "")).strip()
                    code = str(row.get("code", "")).strip()
                    name = str(row.get("name", "")).strip()
                    rank = row.get("rank", "")
                    pct = pd.to_numeric(row.get("pct"), errors="coerce")

                    if not ts_code:
                        st.caption(f"#{rank} {code} {name}")
                        st.warning("缺少ts_code")
                        continue

                    kline = fetch_stock_kline_with_limit(
                        ts_code=ts_code,
                        trade_date=trade_date_str,
                        lookback_days=620,
                    )
                    if kline is None or kline.empty:
                        st.caption(f"#{rank} {code} {name}")
                        st.warning("K线数据为空")
                        continue

                    kline = kline.sort_values("trade_date").tail(90).copy()
                    sub_title = f"#{rank} {code} {name}"
                    if pd.notna(pct):
                        sub_title = f"{sub_title} ({pct:.2f}%)"
                    fig = _build_kline_figure(kline, sub_title)
                    st.plotly_chart(
                        fig,
                        use_container_width=True,
                        key=f"kline_{pattern_col}_{code}_{start}_{i}",
                    )


st.title("TOP100赚钱效应")
st.caption("实时计算：去除ST、北交所后，统计涨幅前100股票。")

default_date = datetime.now().date()
selected_date = st.date_input("选择日期", value=default_date)
require_newhigh_volume = st.checkbox("历史新高需量能(amount_ratio_20>=1.2)", value=True)
load_btn = st.button("加载")

if load_btn:
    progress = st.progress(0, text="开始校验交易日...")
    ok, msg = validate_trade_day(selected_date)
    if not ok:
        progress.empty()
        st.error(msg)
        st.stop()

    progress.progress(20, text="正在实时计算Top100赚钱效应...")
    start_ts = time.perf_counter()
    try:
        result = compute_top100_profit_effect(
            selected_date,
            require_newhigh_volume=require_newhigh_volume,
        )
    except Exception as e:
        progress.empty()
        st.error(f"计算失败: {e}")
        st.stop()

    elapsed = time.perf_counter() - start_ts
    progress.progress(100, text="计算完成")
    time.sleep(0.2)
    progress.empty()

    st.session_state["top100_effect_result"] = result
    st.session_state["top100_effect_elapsed"] = elapsed

result = st.session_state.get("top100_effect_result")
elapsed = st.session_state.get("top100_effect_elapsed")

if not result:
    st.info("请选择交易日后点击“加载”。")
    st.stop()

sample_size = int(result.get("sample_size", 0))
trade_date = result.get("trade_date", "")
elapsed_text = f"，耗时 {elapsed:.2f}s" if isinstance(elapsed, (int, float)) else ""
st.success(f"统计日期: {trade_date}，样本数: {sample_size}{elapsed_text}")
st.caption("K线形态统计为多标签并行，同一只股票可命中多个形态。")

row1 = st.columns(2)
with row1[0]:
    _render_bar_chart(
        "成交额分层（亿）",
        result.get("amount_stats", []),
        colors=["#9b59b6", "#3498db", "#f39c12", "#e74c3c"],
    )
with row1[1]:
    _render_bar_chart(
        "市值分层（亿）",
        result.get("mktcap_stats", []),
        colors=["#16a085", "#1abc9c", "#27ae60", "#2ecc71", "#58d68d"],
    )

row2 = st.columns(2)
with row2[0]:
    _render_bar_chart(
        "板块分类",
        result.get("board_stats", []),
        colors=["#2f80ed", "#f2994a", "#eb5757"],
    )
with row2[1]:
    _render_bar_chart(
        "K线形态命中（多标签）",
        result.get("pattern_stats", []),
        colors=["#34495e", "#8e44ad", "#2980b9", "#d35400", "#27ae60"],
    )

st.markdown("### Top100分层特征（按日期）")
history_df = fetch_top100_layered_features_by_date(limit_days=250)

if history_df.empty:
    st.info("暂无按日期分层特征数据（表 gainer_feature_summary 为空或未同步）")
else:
    _render_layered_trend_charts(history_df)

    st.markdown("#### 关键指标日环比变化（Δ）")
    change_df = _build_layered_change_table(history_df)
    if not change_df.empty:
        st.dataframe(
            change_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "高成交占比(>=50亿)": st.column_config.NumberColumn(format="%.1f%%"),
                "Δ高成交占比": st.column_config.NumberColumn(format="%+.1f%%"),
                "高市值占比(>=200亿)": st.column_config.NumberColumn(format="%.1f%%"),
                "Δ高市值占比": st.column_config.NumberColumn(format="%+.1f%%"),
                "成长板占比(创业+科创)": st.column_config.NumberColumn(format="%.1f%%"),
                "Δ成长板占比": st.column_config.NumberColumn(format="%+.1f%%"),
            },
        )

    st.markdown("#### 原始分层明细（按日期）")
    rename_map = {
        "trade_date": "日期",
        "turnover_lt_5e8": "成交额<5亿",
        "turnover_5e8_to_50": "成交额5-50亿",
        "turnover_50e8_to_90": "成交额50-90亿",
        "turnover_gt_90e8": "成交额>90亿",
        "mktcap_lt_5e9": "市值<50亿",
        "mktcap_5e9_to_10": "市值50-100亿",
        "mktcap_10e9_to_20": "市值100-200亿",
        "mktcap_20e9_to_50": "市值200-500亿",
        "mktcap_gt_50e9": "市值>500亿",
        "board_main": "主板",
        "board_gem": "创业板",
        "board_star": "科创板",
    }
    display_df = history_df.rename(columns=rename_map)
    display_cols = [
        "日期",
        "成交额<5亿",
        "成交额5-50亿",
        "成交额50-90亿",
        "成交额>90亿",
        "市值<50亿",
        "市值50-100亿",
        "市值100-200亿",
        "市值200-500亿",
        "市值>500亿",
        "主板",
        "创业板",
        "科创板",
    ]
    display_cols = [c for c in display_cols if c in display_df.columns]
    st.dataframe(display_df[display_cols], use_container_width=True, hide_index=True)

st.markdown("### 明细（100只）")
details = pd.DataFrame(result.get("details", []))
display_df = _format_detail_df(details)
if display_df.empty:
    st.info("暂无明细数据")
else:
    st.dataframe(display_df, use_container_width=True, hide_index=True)

st.markdown("### K线特征样本展示")
_render_pattern_kline_grid(details, trade_date)
