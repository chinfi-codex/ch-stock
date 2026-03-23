#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import pandas as pd
import streamlit as st
import akshare as ak
import datetime
import numpy as np
import plotly.graph_objects as go
from tools import (
    plotK,
    get_market_data,
    get_all_stocks,
    get_financing_net_buy_series,
    get_gem_pe_series,
)
from tools.stock_data import get_ak_price_df
from tools.financial_data import EconomicIndicators
from data_sources import (
    _normalize_top_stocks_df,
    _df_to_records,
    _records_to_df,
    _build_pct_distribution,
    get_benchmark_kline,
)
import requests
import json
import time


def _section_title(title):
    st.markdown(
        f"<div style='font-size:26px;font-weight:700;margin:8px 0 8px 0;'>{title}</div>",
        unsafe_allow_html=True,
    )


def _latest_metric_from_df(df, value_col, date_col="date"):
    if df is None or df.empty or value_col not in df.columns:
        return None
    view = df.copy()
    if date_col in view.columns:
        view[date_col] = pd.to_datetime(view[date_col], errors="coerce")
    view[value_col] = pd.to_numeric(view[value_col], errors="coerce")
    view = view.dropna(subset=[value_col])
    if date_col in view.columns:
        view = view.dropna(subset=[date_col]).sort_values(date_col, ascending=False)
    if view.empty:
        return None
    latest = view.iloc[0]
    prev_value = None
    if len(view) > 1:
        prev_value = view.iloc[1][value_col]
    return {
        "date": latest[date_col] if date_col in view.columns else None,
        "value": float(latest[value_col]),
        "prev_value": float(prev_value)
        if prev_value is not None and not pd.isna(prev_value)
        else None,
    }


def _calc_pct_change(current, previous):
    if current is None or previous is None or previous == 0:
        return None
    return (current / previous - 1) * 100


def _series_from_df(df, value_col, days):
    if df is None or df.empty or value_col not in df.columns:
        return []
    view = df.copy()
    if "date" in view.columns:
        view["date"] = pd.to_datetime(view["date"], errors="coerce")
    view[value_col] = pd.to_numeric(view[value_col], errors="coerce")
    view = view.dropna(subset=[value_col])
    if "date" in view.columns:
        view = view.dropna(subset=["date"]).sort_values("date")
    if view.empty:
        return []
    view = view.tail(int(days))
    if "date" in view.columns:
        view["date"] = view["date"].dt.strftime("%Y-%m-%d")
    view = view[["date", value_col]].rename(columns={value_col: "value"})
    return view.to_dict(orient="records")


@st.cache_data(ttl="1h")
def build_external_section(days=120):
    usdcny_metric = None
    btc_metric = None
    xau_metric = None
    wti_metric = None
    us10y_metric = None

    usdcny_series = []
    btc_series = []
    xau_series = []
    wti_series = []
    us10y_series = []

    fetch_len = max(int(days * 2), 60)

    try:
        usdcny_df = EconomicIndicators.get_exchangerates_daily(
            from_currency="USD", to_currency="CNY", curDate=fetch_len
        )
        if usdcny_df is not None and not usdcny_df.empty:
            # DataFrame already has 'date' and '4. close' columns
            usdcny_df = usdcny_df.rename(columns={"4. close": "value"})
        usdcny_metric = _latest_metric_from_df(usdcny_df, "value")
        usdcny_series = _series_from_df(usdcny_df, "value", days)
    except Exception:
        usdcny_metric = None

    try:
        btc_df = EconomicIndicators.get_crypto_daily(
            symbol="BTC", market="USD", curDate=fetch_len
        )
        btc_metric = _latest_metric_from_df(btc_df, "close")
        btc_series = _series_from_df(btc_df, "close", days)
    except Exception:
        btc_metric = None

    try:
        us10y_df = EconomicIndicators.get_treasury_yield(
            maturity="10year", interval="daily", curDate=fetch_len
        )
        us10y_metric = _latest_metric_from_df(us10y_df, "value")
        us10y_series = _series_from_df(us10y_df, "value", days)
    except Exception:
        us10y_metric = None

    try:
        xau_df = EconomicIndicators.get_gold_silver_history(
            symbol="XAU", interval="daily", curDate=fetch_len
        )
        xau_metric = _latest_metric_from_df(xau_df, "value")
        xau_series = _series_from_df(xau_df, "value", days)
    except Exception:
        xau_metric = None

    try:
        wti_df = EconomicIndicators.get_commodities(
            commodity="WTI", interval="daily", curDate=fetch_len
        )
        wti_metric = _latest_metric_from_df(wti_df, "value")
        wti_series = _series_from_df(wti_df, "value", days)
    except Exception:
        wti_metric = None

    def _attach_change(metric, change_kind, series):
        if not metric:
            return None
        current = metric.get("value")
        prev = metric.get("prev_value")
        change = None
        if change_kind == "pct":
            change = _calc_pct_change(current, prev)
        elif change_kind == "bp":
            if prev is not None:
                change = (current - prev) * 100
        return {
            "date": metric.get("date"),
            "value": current,
            "prev_value": prev,
            "change": change,
            "series": series,
        }

    return {
        "usdcny": _attach_change(usdcny_metric, "pct", usdcny_series),
        "btc": _attach_change(btc_metric, "pct", btc_series),
        "xau": _attach_change(xau_metric, "pct", xau_series),
        "wti": _attach_change(wti_metric, "pct", wti_series),
        "us10y": _attach_change(us10y_metric, "bp", us10y_series),
    }


def _to_iso_date(value):
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


@st.cache_data(ttl="1h")
def _load_realtime_indices(select_date):
    end_date = _to_iso_date(select_date) or datetime.datetime.now().strftime("%Y-%m-%d")
    start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=420)).strftime(
        "%Y-%m-%d"
    )
    sh_df = get_benchmark_kline(
        start_date=start_date, end_date=end_date, symbol="sh000001"
    )
    cyb_df = get_benchmark_kline(
        start_date=start_date, end_date=end_date, symbol="sz399006"
    )
    kcb_df = get_benchmark_kline(
        start_date=start_date, end_date=end_date, symbol="sh000688"
    )
    return {
        "sh_df": _df_to_records(sh_df),
        "cyb_df": _df_to_records(cyb_df),
        "kcb_df": _df_to_records(kcb_df),
    }


def _build_top100_range_from_gainers(gainers_records):
    if not gainers_records:
        return {"sh_stocks": [], "cyb_kcb_stocks": []}
    df = pd.DataFrame(gainers_records)
    if df.empty or not {"code", "name", "pct", "amount"}.issubset(df.columns):
        return {"sh_stocks": [], "cyb_kcb_stocks": []}
    df["pct"] = pd.to_numeric(df["pct"], errors="coerce")
    df = df.dropna(subset=["pct"]).sort_values("pct", ascending=False).head(100)
    code6 = df["code"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("")
    sh_df = df[
        (code6.str.startswith("6") | code6.str.startswith("0"))
        & ~code6.str.startswith("688")
    ]
    cyb_kcb_df = df[code6.str.startswith("3") | code6.str.startswith("688")]
    return {
        "sh_stocks": sh_df[["name", "code", "pct", "amount"]].to_dict(orient="records"),
        "cyb_kcb_stocks": cyb_kcb_df[["name", "code", "pct", "amount"]].to_dict(
            orient="records"
        ),
    }


def _find_market_value_by_keywords(market_data, keywords, default=None):
    if market_data is None or market_data.empty:
        return default
    if not {"item", "value"}.issubset(set(market_data.columns)):
        return default
    for _, row in market_data.iterrows():
        item = str(row.get("item", ""))
        if any(k in item for k in keywords):
            return row.get("value")
    return default


@st.cache_data(ttl="1h")
def build_market_section(select_date, all_stocks_df=None):
    sh_df, cyb_df, kcb_df, market_data = get_market_data()
    up_stocks = _find_market_value_by_keywords(market_data, ["上涨"])
    down_stocks = _find_market_value_by_keywords(market_data, ["下跌"])
    limit_up = _find_market_value_by_keywords(market_data, ["涨停"])
    limit_down = _find_market_value_by_keywords(market_data, ["跌停"])
    activity = _find_market_value_by_keywords(market_data, ["活跃", "情绪"])
    if isinstance(activity, str) and "%" in str(activity):
        activity = str(activity).replace("%", "")

    if all_stocks_df is None:
        try:
            all_stocks_df = get_all_stocks(select_date)
        except Exception:
            all_stocks_df = None

    market_overview = {
        "上涨": up_stocks,
        "下跌": down_stocks,
        "涨停": limit_up,
        "跌停": limit_down,
        "活跃度": activity,
        "range_distribution": _build_pct_distribution(all_stocks_df),
    }
    indices = {
        "sh_df": _df_to_records(sh_df),
        "cyb_df": _df_to_records(cyb_df),
        "kcb_df": _df_to_records(kcb_df),
    }
    return {"indices": indices, "market_overview": market_overview}, all_stocks_df


@st.cache_data(ttl="1h")
def build_top100_section(select_date, all_stocks_df=None):
    source_df = (
        all_stocks_df if all_stocks_df is not None else get_all_stocks(select_date)
    )
    today_top_stocks = _normalize_top_stocks_df(source_df)

    if not today_top_stocks.empty:
        cols = list(today_top_stocks.columns)
        if len(cols) >= 4:
            rename_pos = {
                cols[0]: "name",
                cols[1]: "code",
                cols[2]: "pct",
                cols[3]: "amount",
            }
            today_top_stocks = today_top_stocks.rename(columns=rename_pos)

    turnover_records = []
    if not today_top_stocks.empty and {"pct", "amount", "name", "code"}.issubset(
        today_top_stocks.columns
    ):
        top_100_by_turnover = today_top_stocks.sort_values(
            "amount", ascending=False
        ).head(100)
        top_100_by_turnover["pct"] = pd.to_numeric(
            top_100_by_turnover["pct"], errors="coerce"
        )
        turnover_records = top_100_by_turnover[
            ["name", "code", "pct", "amount"]
        ].to_dict(orient="records")

    range_data = {"sh_stocks": [], "cyb_kcb_stocks": []}
    if not today_top_stocks.empty and {"pct", "amount", "name", "code"}.issubset(
        today_top_stocks.columns
    ):
        top_100_by_range = today_top_stocks.sort_values("pct", ascending=False).head(
            100
        )
        code_series = top_100_by_range["code"].astype(str)
        sh_df = top_100_by_range[
            (
                code_series.str[2:].str.startswith("6")
                | code_series.str[2:].str.startswith("0")
            )
            & ~code_series.str[2:].str.startswith("688")
        ]
        cyb_kcb_df = top_100_by_range[
            code_series.str[2:].str.startswith("3")
            | code_series.str[2:].str.startswith("688")
        ]
        range_data = {
            "sh_stocks": sh_df[["name", "code", "pct", "amount"]].to_dict(
                orient="records"
            ),
            "cyb_kcb_stocks": cyb_kcb_df[["name", "code", "pct", "amount"]].to_dict(
                orient="records"
            ),
        }

    return {
        "top_100_turnover": turnover_records,
        "top_100_range": range_data,
    }, source_df


def is_review_data_complete(review_data):
    if not review_data:
        return False
    indices = review_data.get("indices") or {}
    if not indices:
        return False
    if (
        not indices.get("sh_df")
        or not indices.get("cyb_df")
        or not indices.get("kcb_df")
    ):
        return False
    if not review_data.get("market_overview"):
        return False
    if not review_data.get("top_100_turnover"):
        return False
    top_100_range = review_data.get("top_100_range") or {}
    if not top_100_range.get("sh_stocks") and not top_100_range.get("cyb_kcb_stocks"):
        return False
    return True


def build_review_data(select_date, show_modules=None):
    review_data = {"date": select_date.strftime("%Y-%m-%d")}

    def _should(key):
        return show_modules is None or show_modules.get(key, True)

    all_stocks_df = None

    if _should("external"):
        review_data["external"] = build_external_section()
    else:
        review_data["external"] = {}

    if _should("market"):
        market_section, all_stocks_df = build_market_section(select_date, all_stocks_df)
        review_data.update(market_section)
        review_data["financing_series"] = []
        review_data["gem_pe_series"] = []
    else:
        review_data["indices"] = {"sh_df": [], "cyb_df": [], "kcb_df": []}
        review_data["market_overview"] = {}
        review_data["financing_series"] = []
        review_data["gem_pe_series"] = []

    if _should("top100"):
        top_section, all_stocks_df = build_top100_section(select_date, all_stocks_df)
        review_data.update(top_section)

        gainers = []
        losers = []
        if (
            all_stocks_df is not None
            and not all_stocks_df.empty
            and "pct" in all_stocks_df.columns
        ):
            view = all_stocks_df.copy()
            view["pct"] = pd.to_numeric(view["pct"], errors="coerce")
            if "amount" in view.columns:
                view["amount"] = pd.to_numeric(view["amount"], errors="coerce")
            if "mkt_cap" in view.columns:
                view["mkt_cap"] = pd.to_numeric(view["mkt_cap"], errors="coerce")
            if "code" in view.columns:
                view["code"] = view["code"].astype(str)
            if "name" in view.columns:
                view["name"] = view["name"].astype(str)

            view = view.dropna(
                subset=[col for col in ["pct", "code", "name"] if col in view.columns]
            )

            # 去除 ST + 北交（4/8开头或 bj 前缀）
            if {"code", "name"}.issubset(view.columns):
                code_str = view["code"].astype(str).str.lower().str.strip()
                code6 = code_str.str.extract(r"(\d{6})", expand=False).fillna("")
                is_bj = code_str.str.startswith("bj") | code6.str.startswith(("4", "8"))
                is_st = (
                    view["name"].astype(str).str.upper().str.contains("ST", na=False)
                )
                view = view[~is_bj & ~is_st]

            gainers_df = view.sort_values("pct", ascending=False).head(100)
            losers_df = view.sort_values("pct", ascending=True).head(100)
            keep_cols = [
                col
                for col in ["code", "name", "pct", "amount", "mkt_cap"]
                if col in view.columns
            ]
            if keep_cols:
                gainers = gainers_df[keep_cols].to_dict(orient="records")
                losers = losers_df[keep_cols].to_dict(orient="records")
        review_data["top_100_gainers"] = gainers
        review_data["top_100_losers"] = losers
    else:
        review_data["top_100_turnover"] = []
        review_data["top_100_range"] = {"sh_stocks": [], "cyb_kcb_stocks": []}
        review_data["top_100_gainers"] = []
        review_data["top_100_losers"] = []

    return review_data


def display_review_data(review_data, show_modules=None):
    show_modules = show_modules or {}
    date_str = review_data.get("date")
    if isinstance(date_str, (datetime.date, datetime.datetime)):
        zt_date = date_str.strftime("%Y%m%d")
    elif isinstance(date_str, str) and date_str:
        zt_date = date_str.replace("-", "")
    else:
        zt_date = datetime.datetime.now().strftime("%Y%m%d")

    def _show(key):
        return show_modules.get(key, True)

    if _show("external"):
        _section_title("\u5916\u56f4\u6307\u6807")
        external = review_data.get("external") or {}

        def _format_date(value):
            if isinstance(value, (pd.Timestamp, datetime.date, datetime.datetime)):
                return value.strftime("%Y-%m-%d")
            if isinstance(value, str):
                return value
            return ""

        def _format_value(value, fmt, default="\u2014"):
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return default
            return fmt.format(value)

        def _format_delta(value, fmt):
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            return fmt.format(value)

        usdcny = external.get("usdcny") or {}
        btc = external.get("btc") or {}
        xau = external.get("xau") or {}
        wti = external.get("wti") or {}
        us10y = external.get("us10y") or {}

        def _render_sparkline(series, color):
            if not series:
                return
            df = pd.DataFrame(series)
            if df.empty or "date" not in df.columns or "value" not in df.columns:
                return
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna(subset=["date", "value"])
            if df.empty:
                return
            fig = go.Figure(
                go.Scatter(
                    x=df["date"],
                    y=df["value"],
                    mode="lines",
                    line=dict(color=color, width=2),
                )
            )
            fig.update_layout(
                height=120,
                width=220,
                margin=dict(l=6, r=6, t=6, b=6),
                xaxis=dict(visible=False),
                yaxis=dict(visible=False),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=False)

        cols = st.columns(5)
        with cols[0]:
            st.metric(
                "人民币汇率 (USD/CNY)",
                _format_value(usdcny.get("value"), "{:.4f}"),
                _format_delta(usdcny.get("change"), "{:+.2f}%"),
            )
            usdcny_date = _format_date(usdcny.get("date"))
            if usdcny_date:
                st.caption(f"日期: {usdcny_date}")
            _render_sparkline(usdcny.get("series") or [], "#27ae60")
        with cols[1]:
            st.metric(
                "\u6bd4\u7279\u5e01 (BTC/USD)",
                _format_value(btc.get("value"), "${:,.0f}"),
                _format_delta(btc.get("change"), "{:+.2f}%"),
            )
            btc_date = _format_date(btc.get("date"))
            if btc_date:
                st.caption(f"\u65e5\u671f: {btc_date}")
            _render_sparkline(btc.get("series") or [], "#f39c12")
        with cols[2]:
            st.metric(
                "XAU 金价 (USD/oz)",
                _format_value(xau.get("value"), "{:,.2f}"),
                _format_delta(xau.get("change"), "{:+.2f}%"),
            )
            xau_date = _format_date(xau.get("date"))
            if xau_date:
                st.caption(f"\u65e5\u671f: {xau_date}")
            _render_sparkline(xau.get("series") or [], "#d4af37")
        with cols[3]:
            st.metric(
                "WTI 油价 (USD/bbl)",
                _format_value(wti.get("value"), "{:,.2f}"),
                _format_delta(wti.get("change"), "{:+.2f}%"),
            )
            wti_date = _format_date(wti.get("date"))
            if wti_date:
                st.caption(f"日期: {wti_date}")
            _render_sparkline(wti.get("series") or [], "#e67e22")
        with cols[4]:
            st.metric(
                "\u7f8e\u56fd10Y\u56fd\u503a\u6536\u76ca\u7387",
                _format_value(us10y.get("value"), "{:.2f}%"),
                _format_delta(us10y.get("change"), "{:+.1f}bp"),
            )
            us10y_date = _format_date(us10y.get("date"))
            if us10y_date:
                st.caption(f"\u65e5\u671f: {us10y_date}")
            _render_sparkline(us10y.get("series") or [], "#2f80ed")
        
        # AI 宏观分析框架
        with st.container():
            st.markdown("#### 🤖 宏观市场分析")
            
            # 提取数据序列用于趋势分析
            def _get_trend_direction(series, threshold=0.02):
                """判断趋势方向: up/down/sideways"""
                if not series or len(series) < 5:
                    return "数据不足"
                recent = series[-5:]
                values = [s.get("value", 0) for s in recent if s.get("value")]
                if len(values) < 2:
                    return "数据不足"
                change = (values[-1] - values[0]) / values[0] if values[0] != 0 else 0
                if change > threshold:
                    return "上行"
                elif change < -threshold:
                    return "下行"
                return "震荡"
            
            usdcny_series = usdcny.get("series") or []
            btc_series = btc.get("series") or []
            xau_series = xau.get("series") or []
            wti_series = wti.get("series") or []
            us10y_series = us10y.get("series") or []
            
            # 趋势状态
            trends = {
                "人民币汇率": _get_trend_direction(usdcny_series),
                "比特币": _get_trend_direction(btc_series),
                "黄金": _get_trend_direction(xau_series),
                "WTI原油": _get_trend_direction(wti_series),
                "美债10Y": _get_trend_direction(us10y_series, 0.05),
            }
            
            # 分析资产间相关性逻辑
            risk_on_count = 0  # 风险资产偏强
            risk_off_count = 0  # 避险资产偏强
            
            # BTC作为风险资产指标
            if trends["比特币"] == "上行":
                risk_on_count += 1
            elif trends["比特币"] == "下行":
                risk_off_count += 1
                
            # 黄金作为避险资产指标
            if trends["黄金"] == "上行":
                risk_off_count += 1
            elif trends["黄金"] == "下行":
                risk_on_count += 1
                
            # 美债收益率上行=风险承压
            if trends["美债10Y"] == "上行":
                risk_off_count += 1
            elif trends["美债10Y"] == "下行":
                risk_on_count += 1
                
            # 人民币贬值=新兴市场承压
            if trends["人民币汇率"] == "上行":
                risk_off_count += 0.5
            elif trends["人民币汇率"] == "下行":
                risk_on_count += 0.5
            
            # 构建分析报告
            with st.expander("📊 五类风险资产联动分析", expanded=True):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**📈 各指标趋势状态**")
                    for asset, trend in trends.items():
                        emoji = {"上行": "📈", "下行": "📉", "震荡": "➡️", "数据不足": "❓"}.get(trend, "❓")
                        st.markdown(f"• {asset}: {emoji} {trend}")
                
                with col2:
                    st.markdown("**🔄 资产相关性逻辑**")
                    if risk_on_count > risk_off_count:
                        st.success("""**风险偏好升温**  
                        风险资产(BTC)偏强 + 避险资产(黄金)偏弱  
                        市场追逐收益，流动性充裕""")
                    elif risk_off_count > risk_on_count:
                        st.warning("""**避险情绪主导**  
                        避险资产(黄金、美债)偏强 + 风险资产承压  
                        资金寻求安全，不确定性上升""")
                    else:
                        st.info("""**信号混合**  
                        风险/避险资产同步震荡  
                        市场等待明确方向""")
                    
                    st.markdown("---")
                    st.markdown("**💡 通胀预期**")
                    if trends["WTI原油"] == "上行" and trends["黄金"] == "上行":
                        st.warning("商品双涨 → 通胀压力上升")
                    elif trends["WTI原油"] == "下行" and trends["黄金"] == "下行":
                        st.success("商品双跌 → 通胀压力缓解")
                    else:
                        st.info("商品分化 → 通胀信号不明")
            
            # 核心结论
            st.markdown("**🎯 市场正在定价什么**")
            conclusions = []
            
            # 结论1: 风险偏好状态
            if risk_on_count >= 2.5:
                conclusions.append("1. **全球风险偏好偏暖**：BTC等风险资产走强，资金追逐收益，对A股情绪形成正面传导")
            elif risk_off_count >= 2.5:
                conclusions.append("1. **全球避险情绪升温**：黄金+美债同步走强，地缘政治或衰退担忧主导，A股承压")
            else:
                conclusions.append("1. **风险情绪中性震荡**：多空因素交织，市场等待美联储或地缘局势的明确信号")
            
            # 结论2: 汇率与资金流向
            if trends["人民币汇率"] == "上行":
                conclusions.append("2. **人民币贬值压力**：USD/CNY上行，外资流出压力+进口成本上升，关注央行干预")
            elif trends["人民币汇率"] == "下行":
                conclusions.append("2. **人民币升值动能**：外资回流预期，利好人民币资产，北向资金或回流")
            else:
                conclusions.append("2. **汇率相对稳定**：短期双向波动，关注7.3关键心理位突破情况")
            
            # 结论3: 利率与估值环境
            if trends["美债10Y"] == "上行":
                conclusions.append("3. **全球利率上行**：美债收益率走高压制成长股估值，高股息防御板块占优")
            elif trends["美债10Y"] == "下行":
                conclusions.append("3. **降息预期升温**：美债收益率回落利好估值扩张，关注科技/新能源等成长板块")
            else:
                conclusions.append("3. **利率环境平稳**：美联储政策预期稳定，个股业绩驱动为主")
            
            for conclusion in conclusions:
                st.markdown(f"{conclusion}")
            
            st.caption("*提示：以上分析基于价格趋势的技术推演，不构成投资建议。请结合基本面综合判断。*")
        
        st.markdown("---")

    if _show("market"):
        _section_title("今日大盘")
        indices = review_data.get("indices", {})
        sh_df = _records_to_df(indices.get("sh_df", []))
        cyb_df = _records_to_df(indices.get("cyb_df", []))
        kcb_df = _records_to_df(indices.get("kcb_df", []))

        market_overview = review_data.get("market_overview", {})
        up_stocks = market_overview.get("上涨")
        down_stocks = market_overview.get("下跌")
        limit_up = market_overview.get("涨停")
        limit_down = market_overview.get("跌停")
        activity = market_overview.get("活跃度")

        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            st.markdown("上证指数")
            if not sh_df.empty:
                plotK(sh_df, show_macd=False)
        with col2:
            st.markdown("创业板指数")
            if not cyb_df.empty:
                plotK(cyb_df, show_macd=False)
        with col3:
            st.markdown("科创板指数")
            if not kcb_df.empty:
                plotK(kcb_df, show_macd=False)

        import os

        csv_file = os.path.join("datas", "market_data.csv")
        if os.path.exists(csv_file):
            try:
                df_history = pd.read_csv(csv_file)
                df_history = df_history.loc[
                    :, ~df_history.columns.str.contains("^Unnamed")
                ]
                if "日期" in df_history.columns:
                    df_history["日期"] = pd.to_datetime(
                        df_history["日期"], errors="coerce"
                    )
                    df_history = df_history.dropna(subset=["日期"])
                    df_history = df_history.sort_values("日期")
                    df_history = df_history.tail(100)
                    numeric_cols = ["上涨", "下跌", "涨停", "跌停", "活跃度", "成交额"]
                    for col in numeric_cols:
                        if col in df_history.columns:
                            if col == "活跃度":
                                df_history[col] = (
                                    df_history[col]
                                    .astype(str)
                                    .str.replace("%", "")
                                    .astype(float)
                                )
                            else:
                                df_history[col] = pd.to_numeric(
                                    df_history[col], errors="coerce"
                                )

                    latest_row = df_history.iloc[-1] if not df_history.empty else None
                    if latest_row is not None:
                        up_stocks = latest_row.get("上涨", up_stocks)
                        down_stocks = latest_row.get("下跌", down_stocks)
                        limit_up = latest_row.get("涨停", limit_up)
                        limit_down = latest_row.get("跌停", limit_down)
                        activity = latest_row.get("活跃度", activity)
                    fin_series = get_financing_net_buy_series(60)
                    gem_pe_series = get_gem_pe_series(500)

                    first_row = st.columns(3)
                    with first_row[0]:
                        if "成交额" in df_history.columns:
                            amount_df = df_history.dropna(subset=["成交额"]).tail(100).copy()
                            if amount_df.empty:
                                st.info("暂无成交额数据")
                            else:
                                # 转换为万亿单位（原始数据是千元，1万亿 = 10^9 千元）
                                amount_df["成交额_万亿"] = amount_df["成交额"] / 1e9
                                fig_amount = go.Figure()
                                fig_amount.add_trace(
                                    go.Scatter(
                                        x=amount_df["日期"],
                                        y=amount_df["成交额_万亿"],
                                        mode="lines+markers",
                                        name="成交额",
                                        line=dict(color="#4c6ef5", width=2),
                                        marker=dict(size=4),
                                        hovertemplate="%{x|%Y-%m-%d}<br>成交额: %{y:.2f} 万亿<extra></extra>",
                                    )
                                )
                                fig_amount.update_layout(
                                    title="成交额趋势（万亿）",
                                    xaxis_title="日期",
                                    yaxis_title="成交额（万亿）",
                                    height=300,
                                    hovermode="x unified",
                                )
                                fig_amount.update_yaxes(
                                    tickformat=".2f", exponentformat="none"
                                )
                                st.plotly_chart(fig_amount, use_container_width=True)

                    with first_row[1]:
                        if "活跃度" in df_history.columns:
                            activity_df = df_history.dropna(subset=["活跃度"]).tail(100).copy()
                            if activity_df.empty:
                                st.info("暂无活跃度数据")
                            else:
                                fig_activity = go.Figure()
                                fig_activity.add_trace(
                                    go.Scatter(
                                        x=activity_df["日期"],
                                        y=activity_df["活跃度"],
                                        mode="lines+markers",
                                        name="情绪指数",
                                        line=dict(color="#f39c12", width=2),
                                        marker=dict(size=4),
                                    )
                                )
                                fig_activity.update_layout(
                                    title="情绪指数（活跃度）",
                                    xaxis_title="日期",
                                    yaxis_title="活跃度 (%)",
                                    height=300,
                                    hovermode="x unified",
                                )
                                st.plotly_chart(fig_activity, use_container_width=True)

                    with first_row[2]:
                        if fin_series is not None and not fin_series.empty:
                            fin_series = fin_series.sort_values("date").copy()
                            fin_series["date"] = pd.to_datetime(
                                fin_series["date"], errors="coerce"
                            )
                            fin_series["融资净买入"] = pd.to_numeric(
                                fin_series["融资净买入"], errors="coerce"
                            )
                            fin_plot_df = fin_series.dropna(
                                subset=["date", "融资净买入"]
                            )
                            if fin_plot_df.empty:
                                st.info("暂无融资净买入数据")
                            else:
                                colors = fin_plot_df["融资净买入"].apply(
                                    lambda x: "#e74c3c" if x >= 0 else "#2ecc71"
                                )
                                fig_financing = go.Figure(
                                    go.Bar(
                                        x=fin_plot_df["date"],
                                        y=fin_plot_df["融资净买入"],
                                        marker_color=colors,
                                        name="融资净买入",
                                        hovertemplate="%{x|%Y-%m-%d}<br>净买入: %{y:.0f}<extra></extra>",
                                    )
                                )
                                fig_financing.update_layout(
                                    title="融资净买入（近60交易日）",
                                    xaxis_title="日期",
                                    yaxis_title="金额",
                                    height=300,
                                    hovermode="x unified",
                                    bargap=0.2,
                                )
                                st.plotly_chart(fig_financing, use_container_width=True)

                    second_row = st.columns(3)
                    with second_row[0]:
                        if (
                            "上涨" in df_history.columns
                            and "下跌" in df_history.columns
                        ):
                            up_down_df = df_history[
                                df_history[["上涨", "下跌"]].notna().any(axis=1)
                            ].tail(100).copy()
                            if up_down_df.empty:
                                st.info("暂无涨跌家数数据")
                            else:
                                up_trace_df = up_down_df.dropna(subset=["上涨"])
                                down_trace_df = up_down_df.dropna(subset=["下跌"])
                                fig_up_down = go.Figure()
                                if not up_trace_df.empty:
                                    fig_up_down.add_trace(
                                        go.Scatter(
                                            x=up_trace_df["日期"],
                                            y=up_trace_df["上涨"],
                                            mode="lines+markers",
                                            name="上涨数",
                                            line=dict(color="#e74c3c", width=2),
                                            marker=dict(size=4),
                                        )
                                    )
                                if not down_trace_df.empty:
                                    fig_up_down.add_trace(
                                        go.Scatter(
                                            x=down_trace_df["日期"],
                                            y=down_trace_df["下跌"],
                                            mode="lines+markers",
                                            name="下跌数",
                                            line=dict(color="#2ecc71", width=2),
                                            marker=dict(size=4),
                                        )
                                    )
                                fig_up_down.update_layout(
                                    title="上涨数 vs 下跌数",
                                    xaxis_title="日期",
                                    yaxis_title="数量",
                                    height=300,
                                    hovermode="x unified",
                                    legend=dict(
                                        orientation="h",
                                        yanchor="bottom",
                                        y=1.02,
                                        xanchor="right",
                                        x=1,
                                    ),
                                )
                                st.plotly_chart(fig_up_down, use_container_width=True)

                    with second_row[1]:
                        if (
                            "涨停" in df_history.columns
                            and "跌停" in df_history.columns
                        ):
                            limit_df = df_history[
                                df_history[["涨停", "跌停"]].notna().any(axis=1)
                            ].tail(100).copy()
                            if limit_df.empty:
                                st.info("暂无涨停/跌停数据")
                            else:
                                zt_trace_df = limit_df.dropna(subset=["涨停"])
                                dt_trace_df = limit_df.dropna(subset=["跌停"])
                                fig_limit = go.Figure()
                                if not zt_trace_df.empty:
                                    fig_limit.add_trace(
                                        go.Scatter(
                                            x=zt_trace_df["日期"],
                                            y=zt_trace_df["涨停"],
                                            mode="lines+markers",
                                            name="涨停数",
                                            line=dict(color="#c0392b", width=2),
                                            marker=dict(size=4),
                                        )
                                    )
                                if not dt_trace_df.empty:
                                    fig_limit.add_trace(
                                        go.Scatter(
                                            x=dt_trace_df["日期"],
                                            y=dt_trace_df["跌停"],
                                            mode="lines+markers",
                                            name="跌停数",
                                            line=dict(color="#27ae60", width=2),
                                            marker=dict(size=4),
                                        )
                                    )
                                fig_limit.update_layout(
                                    title="涨停数 vs 跌停数",
                                    xaxis_title="日期",
                                    yaxis_title="数量",
                                    height=300,
                                    hovermode="x unified",
                                    legend=dict(
                                        orientation="h",
                                        yanchor="bottom",
                                        y=1.02,
                                        xanchor="right",
                                        x=1,
                                    ),
                                )
                                st.plotly_chart(fig_limit, use_container_width=True)

                    with second_row[2]:
                        if gem_pe_series is not None and not gem_pe_series.empty:
                            gem_pe_series = gem_pe_series.sort_values("date").copy()
                            gem_pe_series["date"] = pd.to_datetime(
                                gem_pe_series["date"], errors="coerce"
                            )
                            gem_pe_series["市盈率"] = pd.to_numeric(
                                gem_pe_series["市盈率"], errors="coerce"
                            )
                            gem_plot_df = gem_pe_series.dropna(
                                subset=["date", "市盈率"]
                            )
                            if gem_plot_df.empty:
                                st.info("暂无创业板市盈率数据")
                            else:
                                fig_gem_pe = go.Figure()
                                fig_gem_pe.add_trace(
                                    go.Scatter(
                                        x=gem_plot_df["date"],
                                        y=gem_plot_df["市盈率"],
                                        mode="lines+markers",
                                        name="创业板市盈率",
                                        line=dict(color="#1f77b4", width=2),
                                        marker=dict(size=4),
                                        hovertemplate="%{x|%Y-%m-%d}<br>PE: %{y:.2f}<extra></extra>",
                                    )
                                )
                                fig_gem_pe.update_layout(
                                    title="创业板市盈率（近500交易日）",
                                    xaxis_title="日期",
                                    yaxis_title="PE",
                                    height=300,
                                    hovermode="x unified",
                                )
                                st.plotly_chart(fig_gem_pe, use_container_width=True)
                        else:
                            st.info("暂无创业板市盈率数据")
            except Exception as e:
                st.warning(f"读取历史市场数据失败: {e}")

        # ========== 风格指数 K 线图（120日） ==========
        st.markdown("---")
        _section_title("风格指数（120日K线）")

        # 指数配置：(代码, 名称, 描述)
        STYLE_INDICES = [
            ("sh000016", "上证50", "超大盘"),
            ("sh000300", "沪深300", "大盘"),
            ("sh000905", "中证500", "中盘"),
            ("sh000852", "中证1000", "小盘"),
            ("sz399376", "小盘成长", "成长风格"),
            ("sh000015", "红利指数", "红利策略"),
        ]

        @st.cache_data(ttl="1h")
        def _get_index_kline(symbol, days=120):
            """获取指数 K 线数据"""
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=days * 2)
            end_str = end_date.strftime("%Y-%m-%d")
            start_str = start_date.strftime("%Y-%m-%d")
            df = get_benchmark_kline(
                start_date=start_str, end_date=end_str, symbol=symbol
            )
            if df is not None and not df.empty:
                df = df.tail(days)
            return df

        # 6个指数，一行3列，共2行
        for row_idx in range(2):
            cols = st.columns(3)
            for col_idx in range(3):
                idx = row_idx * 3 + col_idx
                if idx < len(STYLE_INDICES):
                    symbol, name, desc = STYLE_INDICES[idx]
                    with cols[col_idx]:
                        st.markdown(f"**{name}** *{desc}*")
                        df = _get_index_kline(symbol, days=120)
                        if df is not None and not df.empty:
                            required_cols = ["open", "high", "low", "close", "volume"]
                            if all(c in df.columns for c in required_cols):
                                plotK(
                                    df,
                                    k="d",
                                    plot_type="candle",
                                    ma_line=(5, 20, 60),
                                    container=st,
                                    show_macd=False,
                                )
                            else:
                                st.warning(f"{name} 数据不完整")
                        else:
                            st.warning(f"{name} 暂无数据")

        st.markdown("---")

    if _show("top100"):
        _section_title("\u5e02\u573a\u5168\u8c8c\u5206\u6790")

        # 保留原有2张图：当日涨跌幅分布 + 成交额Top100涨跌分布
        top_100_records = review_data.get("top_100_turnover", [])
        market_overview = review_data.get("market_overview", {})
        range_distribution = market_overview.get("range_distribution", [])

        top_100_by_turnover = (
            pd.DataFrame(top_100_records) if top_100_records else pd.DataFrame()
        )
        if not top_100_by_turnover.empty and "pct" in top_100_by_turnover.columns:
            top_100_by_turnover["pct"] = pd.to_numeric(
                top_100_by_turnover["pct"], errors="coerce"
            )
            up_stocks = top_100_by_turnover[top_100_by_turnover["pct"] > 2]
            down_stocks = top_100_by_turnover[top_100_by_turnover["pct"] < -2]
            shake_stocks = top_100_by_turnover[
                (top_100_by_turnover["pct"] >= -2) & (top_100_by_turnover["pct"] <= 2)
            ]
            shake_count = len(shake_stocks)
        else:
            up_stocks = pd.DataFrame()
            down_stocks = pd.DataFrame()
            shake_count = 0

        overview_cols = st.columns(2)
        with overview_cols[0]:
            if range_distribution:
                dist_df = pd.DataFrame(range_distribution)
                if not dist_df.empty:
                    order_labels = [
                        ">20%",
                        "10%~20%",
                        "5%~10%",
                        "3%~5%",
                        "0%~3%",
                        "-3%~0%",
                        "-5%~-3%",
                        "-10%~-5%",
                        "<-10%",
                    ]
                    order_map = {label: idx for idx, label in enumerate(order_labels)}
                    dist_df["order"] = dist_df["label"].map(order_map)
                    dist_df = dist_df.sort_values("order")
                    colors = [
                        "#e74c3c" if row["bucket_start"] >= 0 else "#2ecc71"
                        for _, row in dist_df.iterrows()
                    ]
                    fig_range = go.Figure(
                        go.Bar(
                            x=dist_df["label"],
                            y=dist_df["count"],
                            text=dist_df["count"],
                            textposition="outside",
                            marker_color=colors,
                            hovertemplate="%{x}<br>\u5bb6\u6570: %{y}<extra></extra>",
                        )
                    )
                    fig_range.update_layout(
                        title="\u5f53\u65e5\u6da8\u8dcc\u5e45\u5206\u5e03",
                        xaxis_title="\u6da8\u8dcc\u5e45\u533a\u95f4(%)",
                        yaxis_title="\u5bb6\u6570",
                        height=400,
                        margin=dict(t=30, l=10, r=10, b=40),
                    )
                    st.plotly_chart(fig_range, use_container_width=True)
                else:
                    st.info("\u6682\u65e0\u6da8\u8dcc\u5e45\u5206\u5e03\u6570\u636e")
            else:
                st.info("\u6682\u65e0\u6da8\u8dcc\u5e45\u5206\u5e03\u6570\u636e")

        with overview_cols[1]:
            if top_100_by_turnover.empty:
                st.info("\u6682\u65e0TOP100\u6570\u636e")
            else:
                categories = [
                    "\u5c0f\u6da8(2-5%)",
                    "\u4e2d\u6da8(5-9%)",
                    "\u5927\u6da8(>9%)",
                    "\u5c0f\u8dcc(-2~-5%)",
                    "\u4e2d\u8dcc(-5~-9%)",
                    "\u5927\u8dcc(<-9%)",
                    "\u9707\u8361(-2%~2%)",
                ]
                small_up = up_stocks[(up_stocks["pct"] >= 2) & (up_stocks["pct"] < 5)]
                medium_up = up_stocks[(up_stocks["pct"] >= 5) & (up_stocks["pct"] < 9)]
                large_up = up_stocks[up_stocks["pct"] >= 9]
                small_down = down_stocks[
                    (down_stocks["pct"] <= -2) & (down_stocks["pct"] > -5)
                ]
                medium_down = down_stocks[
                    (down_stocks["pct"] <= -5) & (down_stocks["pct"] > -9)
                ]
                large_down = down_stocks[down_stocks["pct"] <= -9]
                values = [
                    len(small_up),
                    len(medium_up),
                    len(large_up),
                    len(small_down),
                    len(medium_down),
                    len(large_down),
                    shake_count,
                ]
                colors = [
                    "#c0392b",
                    "#a93226",
                    "#922b21",
                    "#27ae60",
                    "#229954",
                    "#1e8449",
                    "#f39c12",
                ]
                fig_bar = go.Figure(
                    go.Bar(
                        x=categories,
                        y=values,
                        marker_color=colors,
                        text=values,
                        textposition="outside",
                        hovertemplate="<b>%{x}</b><br>\u6570\u91cf: %{y}<extra></extra>",
                    )
                )
                fig_bar.update_layout(
                    title="\u6210\u4ea4\u989dTop100\u6da8\u8dcc\u5206\u5e03",
                    xaxis_title="\u5206\u7c7b",
                    yaxis_title="\u80a1\u7968\u6570\u91cf",
                    height=400,
                    xaxis=dict(tickangle=-45),
                    yaxis=dict(range=[0, max(values) * 1.2] if values else [0, 10]),
                )
                st.plotly_chart(fig_bar, use_container_width=True)

        top_100_gainers_records = review_data.get("top_100_gainers", [])
        gainers_df = (
            pd.DataFrame(top_100_gainers_records)
            if top_100_gainers_records
            else pd.DataFrame()
        )

        if gainers_df.empty:
            st.info("\u6682\u65e0\u6da8\u5e45Top100\u6570\u636e")
        else:
            for col in ["pct", "amount", "mkt_cap"]:
                if col in gainers_df.columns:
                    gainers_df[col] = pd.to_numeric(gainers_df[col], errors="coerce")
            gainers_df = gainers_df.dropna(
                subset=[c for c in ["code", "name", "pct"] if c in gainers_df.columns]
            )
            gainers_df = gainers_df.sort_values("pct", ascending=False).head(100)

            if gainers_df.empty:
                st.info(
                    "\u6682\u65e0\u6ee1\u8db3\u6761\u4ef6\u7684\u6da8\u5e45Top100\u6570\u636e"
                )
            else:
                amount_yi = (
                    (gainers_df["amount"] / 1e8)
                    if "amount" in gainers_df.columns
                    else pd.Series(dtype=float)
                )
                mkt_cap_yi = (
                    (gainers_df["mkt_cap"] / 1e8)
                    if "mkt_cap" in gainers_df.columns
                    else pd.Series(dtype=float)
                )

                amount_labels = ["<5\u4ebf", "5-50\u4ebf", "50-90\u4ebf", ">90\u4ebf"]
                amount_values = [
                    int((amount_yi < 5).sum()) if not amount_yi.empty else 0,
                    int(((amount_yi >= 5) & (amount_yi < 50)).sum())
                    if not amount_yi.empty
                    else 0,
                    int(((amount_yi >= 50) & (amount_yi < 90)).sum())
                    if not amount_yi.empty
                    else 0,
                    int((amount_yi >= 90).sum()) if not amount_yi.empty else 0,
                ]

                mkt_labels = [
                    "<50\u4ebf",
                    "50-100\u4ebf",
                    "100-200\u4ebf",
                    "200-500\u4ebf",
                    ">500\u4ebf",
                ]
                mkt_values = [
                    int((mkt_cap_yi < 50).sum()) if not mkt_cap_yi.empty else 0,
                    int(((mkt_cap_yi >= 50) & (mkt_cap_yi < 100)).sum())
                    if not mkt_cap_yi.empty
                    else 0,
                    int(((mkt_cap_yi >= 100) & (mkt_cap_yi < 200)).sum())
                    if not mkt_cap_yi.empty
                    else 0,
                    int(((mkt_cap_yi >= 200) & (mkt_cap_yi < 500)).sum())
                    if not mkt_cap_yi.empty
                    else 0,
                    int((mkt_cap_yi >= 500).sum()) if not mkt_cap_yi.empty else 0,
                ]

                code_series = gainers_df["code"].astype(str).str.lower().str.strip()
                code6 = code_series.str.extract(r"(\d{6})", expand=False).fillna("")
                kcb_mask = code6.str.startswith("688")
                cyb_mask = code6.str.startswith(("300", "301"))

                board_labels = [
                    "\u4e3b\u677f\uff08\u4e0a\u8bc1+\u6df1\u5733\uff09",
                    "\u521b\u4e1a\u677f",
                    "\u79d1\u521b\u677f",
                ]
                board_values = [
                    int((~kcb_mask & ~cyb_mask).sum()),
                    int(cyb_mask.sum()),
                    int(kcb_mask.sum()),
                ]

                row_cols = st.columns(3)

                with row_cols[0]:
                    fig_amount = go.Figure(
                        go.Bar(
                            x=amount_labels,
                            y=amount_values,
                            marker_color=["#9b59b6", "#3498db", "#f39c12", "#e74c3c"],
                            text=amount_values,
                            textposition="outside",
                            hovertemplate="%{x}<br>\u6570\u91cf: %{y}<extra></extra>",
                        )
                    )
                    fig_amount.update_layout(
                        title="\u6210\u4ea4\u989d\u5206\u5c42\uff08\u6da8\u5e45Top100\uff09",
                        xaxis_title="\u6210\u4ea4\u989d\u533a\u95f4\uff08\u4ebf\u5143\uff09",
                        yaxis_title="\u80a1\u7968\u6570\u91cf",
                        height=350,
                    )
                    st.plotly_chart(fig_amount, use_container_width=True)

                with row_cols[1]:
                    fig_mkt = go.Figure(
                        go.Bar(
                            x=mkt_labels,
                            y=mkt_values,
                            marker_color=[
                                "#16a085",
                                "#1abc9c",
                                "#27ae60",
                                "#2ecc71",
                                "#58d68d",
                            ],
                            text=mkt_values,
                            textposition="outside",
                            hovertemplate="%{x}<br>\u6570\u91cf: %{y}<extra></extra>",
                        )
                    )
                    fig_mkt.update_layout(
                        title="\u5e02\u503c\u5206\u5c42\uff08\u6da8\u5e45Top100\uff09",
                        xaxis_title="\u5e02\u503c\u533a\u95f4\uff08\u4ebf\u5143\uff09",
                        yaxis_title="\u80a1\u7968\u6570\u91cf",
                        height=350,
                    )
                    st.plotly_chart(fig_mkt, use_container_width=True)

                with row_cols[2]:
                    fig_board = go.Figure(
                        go.Bar(
                            x=board_labels,
                            y=board_values,
                            marker_color=["#2f80ed", "#f2994a", "#eb5757"],
                            text=board_values,
                            textposition="outside",
                            hovertemplate="%{x}<br>\u6570\u91cf: %{y}<extra></extra>",
                        )
                    )
                    fig_board.update_layout(
                        title="\u677f\u5757\u5206\u7c7b\uff08\u6da8\u5e45Top100\uff09",
                        xaxis_title="\u677f\u5757",
                        yaxis_title="\u80a1\u7968\u6570\u91cf",
                        height=350,
                    )
                    st.plotly_chart(fig_board, use_container_width=True)

        st.markdown("---")


st.set_page_config(page_title="复盘", page_icon="🚀", layout="wide")

today = datetime.datetime.now()
select_date = st.date_input("选择日期", today)
st.markdown("#### 复盘模块显示")
show_external = st.checkbox("\u5916\u56f4\u6307\u6807", value=True, key="show_external")
show_market = st.checkbox("今日大盘", value=True, key="show_market")
show_top100 = st.checkbox("市场全貌分析", value=True, key="show_top100")
show_modules = {
    "external": show_external,
    "market": show_market,
    "top100": show_top100,
}
realtime_load_btn = st.button("实时Load")

if realtime_load_btn:
    if select_date.weekday() >= 5:
        st.warning("非交易日")
        st.stop()

    review_data = build_review_data(select_date, show_modules)
    display_review_data(review_data, show_modules)
