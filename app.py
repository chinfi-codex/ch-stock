#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import streamlit as st
import akshare as ak
import datetime
import numpy as np
import plotly.graph_objects as go
import json
import re
from html import escape
from tools import (
    plotK,
    get_market_data,
    get_all_stocks,
    get_llm_response,
    get_longhu_data,
    get_financing_net_buy_series,
)
from tools.financial_data import EconomicIndicators
from tools.storage_utils import save_review_data, load_review_data
from data_sources import (
    _normalize_top_stocks_df,
    _safe_market_value,
    get_zt_pool,
    get_dt_pool,
    _df_to_records,
    _records_to_df,
    _build_pct_distribution,
)
from indicators import (
    calculate_ma,
    calculate_ma_slope,
)



def _build_index_snapshot(df):
    if df is None or df.empty or 'close' not in df.columns:
        return {}
    close = pd.to_numeric(df['close'], errors='coerce')
    ma5 = calculate_ma(close, 5)
    ma20 = calculate_ma(close, 20)
    ma60 = calculate_ma(close, 60)
    recent_60d = _df_to_records(df.tail(60))
    snapshot = {
        'close': float(close.iloc[-1]) if not pd.isna(close.iloc[-1]) else None,
        'ma5': float(ma5.iloc[-1]) if not pd.isna(ma5.iloc[-1]) else None,
        'ma20': float(ma20.iloc[-1]) if not pd.isna(ma20.iloc[-1]) else None,
        'ma60': float(ma60.iloc[-1]) if not pd.isna(ma60.iloc[-1]) else None,
        'ret_5d_pct': float(close.pct_change(5).iloc[-1] * 100) if len(close) > 5 else None,
        'ret_20d_pct': float(close.pct_change(20).iloc[-1] * 100) if len(close) > 20 else None,
        'high_20d': float(close.tail(20).max()) if len(close) >= 20 else None,
        'low_20d': float(close.tail(20).min()) if len(close) >= 20 else None,
        'ma20_slope': float(calculate_ma_slope(ma20.dropna(), period=5)) if ma20 is not None else None,
        'recent_60d': recent_60d
    }
    return snapshot


def _build_zt_tiers(zt_pool):
    if zt_pool is None or zt_pool.empty:
        return pd.DataFrame(), pd.DataFrame()
    df = zt_pool.copy()
    if '连板数' not in df.columns:
        return pd.DataFrame(), pd.DataFrame()
    def _parse_streak_text(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        text = str(value).strip()
        if text == '首板':
            return 1
        match = re.search(r"(\d+)", text)
        return int(match.group(1)) if match else None

    df['连板'] = df['连板数'].apply(_parse_streak_text)
    df = df.dropna(subset=['连板'])
    df['连板'] = df['连板'].astype(int)
    tier_counts = (
        df.groupby('连板')
        .size()
        .reset_index(name='数量')
        .sort_values('连板', ascending=False)
    )
    return df, tier_counts


def _build_dt_tiers(dt_pool):
    if dt_pool is None or dt_pool.empty:
        return pd.DataFrame(), pd.DataFrame()
    df = dt_pool.copy()
    streak_col = None
    for candidate in [
        '连续跌停天数', '连跌天数', '跌停天数', '连续跌停', '连跌', '跌停板数', '跌停数'
    ]:
        if candidate in df.columns:
            streak_col = candidate
            break
    if streak_col is None:
        return pd.DataFrame(), pd.DataFrame()

    def _parse_streak_text(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        text = str(value).strip()
        if text in {'首跌', '首板', '首日'}:
            return 1
        match = re.search(r"(\d+)", text)
        return int(match.group(1)) if match else None

    df['连续跌停'] = df[streak_col].apply(_parse_streak_text)
    df = df.dropna(subset=['连续跌停'])
    df['连续跌停'] = df['连续跌停'].astype(int)
    tier_counts = (
        df.groupby('连续跌停')
        .size()
        .reset_index(name='数量')
        .sort_values('连续跌停', ascending=False)
    )
    return df, tier_counts


def _section_title(title):
    st.markdown(
        f"<div style='font-size:26px;font-weight:700;margin:8px 0 8px 0;'>{title}</div>",
        unsafe_allow_html=True,
    )


def _split_summary_parts(summary, expected=3):
    if not summary:
        return [""] * expected
    text = summary.replace("\r", "")
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"^#+\s*", "", text, flags=re.MULTILINE)
    text = text.replace("•", "\n•")
    lines = [line.strip() for line in re.split(r"\n+", text) if line.strip()]

    def _clean(line):
        return re.sub(r"^[\-\*\d\.\)\(、\s•]+", "", line).strip()

    lines = [_clean(line) for line in lines if _clean(line)]

    if len(lines) < expected:
        parts = re.split(r"[；;。]\s*", text)
        parts = [_clean(part) for part in parts if _clean(part)]
        lines = parts

    if not lines:
        return [""] * expected
    if len(lines) >= expected:
        return lines[: expected - 1] + [" ".join(lines[expected - 1 :])]
    return lines + [""] * (expected - len(lines))


def _render_three_blocks(title, labels, contents):
    st.markdown(f"#### {title}")
    cols = st.columns(3)
    for idx, col in enumerate(cols):
        label = labels[idx] if idx < len(labels) else f"要点{idx + 1}"
        content = contents[idx] if idx < len(contents) else ""
        if not content:
            content = "—"
        safe_label = escape(label)
        safe_content = escape(content).replace("\n", "<br>")
        with col:
            st.markdown(
                f"""
                <div style="
                    background:#f7f8fb;
                    border:1px solid #e6e9f2;
                    border-radius:10px;
                    padding:12px 14px;
                    margin:6px 0 12px 0;
                    min-height:90px;
                ">
                    <div style="font-weight:700;color:#243b53;margin-bottom:6px;">{safe_label}</div>
                    <div style="color:#4a5568;font-size:0.92rem;line-height:1.5;">{safe_content}</div>
                </div>
                """,
                unsafe_allow_html=True
            )


def _build_top100_summary(top_turnover_records, top_range_records):
    turnover_df = pd.DataFrame(top_turnover_records) if top_turnover_records else pd.DataFrame()
    range_df = pd.DataFrame(top_range_records) if top_range_records else pd.DataFrame()

    def _normalize(df):
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        if "代码" in df.columns:
            df["代码"] = df["代码"].astype(str)
        if "涨跌幅" in df.columns:
            df["涨跌幅"] = pd.to_numeric(df["涨跌幅"], errors="coerce")
        if "成交额" in df.columns:
            df["成交额"] = pd.to_numeric(df["成交额"], errors="coerce")
        return df

    turnover_df = _normalize(turnover_df)
    range_df = _normalize(range_df)

    if turnover_df.empty and range_df.empty:
        return ""

    lines = []
    overlap_ratio = None
    strong_up_ratio = None
    concentration = None

    if (
        not turnover_df.empty
        and not range_df.empty
        and "代码" in turnover_df.columns
        and "代码" in range_df.columns
    ):
        overlap = set(turnover_df["代码"].dropna()) & set(range_df["代码"].dropna())
        overlap_count = len(overlap)
        overlap_ratio = overlap_count / 100
        lines.append(f"交集: {overlap_count} / 100 ({overlap_ratio * 100:.1f}%)")

    if not turnover_df.empty and "成交额" in turnover_df.columns:
        amount_sum = turnover_df["成交额"].sum()
        top10_sum = (
            turnover_df.sort_values("成交额", ascending=False).head(10)["成交额"].sum()
        )
        if amount_sum:
            concentration = top10_sum / amount_sum
            lines.append(f"成交额集中度: Top10占比 {concentration * 100:.1f}%")

    if not range_df.empty and "涨跌幅" in range_df.columns:
        pct = range_df["涨跌幅"]
        strong_up = (pct >= 7).sum()
        strong_down = (pct <= -7).sum()
        strong_up_ratio = strong_up / len(range_df) if len(range_df) else 0
        median_pct = pct.median()
        lines.append(f"涨幅强势股(>=7%): {strong_up}，弱势股(<=-7%): {strong_down}")
        if pd.notna(median_pct):
            lines.append(f"涨幅中位数: {median_pct:.2f}%")

    tone = "数据不足，难以判断主线集中度"
    if overlap_ratio is not None and concentration is not None and strong_up_ratio is not None:
        if overlap_ratio >= 0.35 and concentration >= 0.45 and strong_up_ratio >= 0.25:
            tone = "主线较清晰，资金与涨幅形成共振"
        elif overlap_ratio <= 0.2 and strong_up_ratio < 0.2:
            tone = "轮动与分歧偏多，主线集中度不足"
        else:
            tone = "情绪有分歧，资金与涨幅存在错位"

    summary_lines = "<br>".join([f"• {line}" for line in lines])
    html = (
        "<div style='background:#f7f7f9;border-left:4px solid #4f7cff;"
        "border-radius:8px;padding:12px 16px;margin:10px 0 8px 0;"
        "color:#1f2d3d;font-size:0.95rem;line-height:1.6;'>"
        f"<b>盘面摘要</b><br>{tone}"
    )
    if summary_lines:
        html += f"<br>{summary_lines}"
    html += "</div>"
    return html


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
        "prev_value": float(prev_value) if prev_value is not None and not pd.isna(prev_value) else None,
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


def build_external_section(days=30):
    btc_metric = None
    us10y_metric = None
    xau_metric = None

    btc_series = []
    us10y_series = []
    xau_series = []

    fetch_len = max(int(days * 2), 60)

    try:
        btc_df = EconomicIndicators.get_crypto_daily(symbol="BTC", market="USD", curDate=fetch_len)
        btc_metric = _latest_metric_from_df(btc_df, "close")
        btc_series = _series_from_df(btc_df, "close", days)
    except Exception:
        btc_metric = None

    try:
        us10y_df = EconomicIndicators.get_treasury_yield(maturity="10year", interval="daily", curDate=fetch_len)
        us10y_metric = _latest_metric_from_df(us10y_df, "value")
        us10y_series = _series_from_df(us10y_df, "value", days)
    except Exception:
        us10y_metric = None

    try:
        xau_df = EconomicIndicators.get_gold_silver_history(symbol="XAU", interval="daily", curDate=fetch_len)
        xau_metric = _latest_metric_from_df(xau_df, "value")
        xau_series = _series_from_df(xau_df, "value", days)
    except Exception:
        xau_metric = None

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
        "btc": _attach_change(btc_metric, "pct", btc_series),
        "us10y": _attach_change(us10y_metric, "bp", us10y_series),
        "xau": _attach_change(xau_metric, "pct", xau_series),
    }



LLM_TEMPLATES = {
    "external_risk": {
        "system": (
            "You are a market risk assessment assistant. Use the provided external indicators to assess near-term risk. "
            "Be objective and avoid predictions. Reply in Chinese."
        ),
        "prompt": (
            "Below are the external indicators with latest values and last 30-day series (JSON):\n"
            "{payload}\n\n"
            "Output:\n"
            "1) First line: \u98ce\u9669\u7b49\u7ea7\uff1a\u4f4e / \u4e2d / \u9ad8 (choose one)\n"
            "2) Next lines: 1-2 sentences explanation, <=80 Chinese characters, Markdown format."
        ),
    },
    "market_overview": {
        "system": (
            "你是专业的A股市场分析师，擅长用技术指标与市场结构数据给出简洁判断。"
            "回答需客观、可解释、避免过度预测。"
        ),
        "prompt": (
            "以下是三大指数与大盘整体数据的摘要（JSON）：\\n"
            "{payload}\\n\\n"
            "请完成技术分析总结：大盘当前的形态：从量价关系，macd等技术指标判断大盘日内走势\\n"
            "重点关注上证指数趋势，以4050为支撑，在上方箱体内的运作趋势，关注创业板是否有效放量突破350，科创板趋势性\\n"
            "输出中文，简洁专业，注意合理分段，总计不超过200字，使用markdown格式输出。"
        ),
    },
    "market_style": {
        "system": (
            "你是A股市场情绪与短线风格分析师，"
            "需要根据连板数据和涨停类指标评估市场结构，"
            "从情绪和磁吸效应角度简洁归纳市场风格。"
        ),
        "prompt": (
            "以下是今日连板与涨停数据摘要（JSON）：\\n"
            "{payload}\\n\\n"
            "请基于连板数据、涨停炸板率、连板高度、连板股家数"
            "总结市场风格，分段输出，简洁专业，使用markdown格式输出。"
        ),
    },
    "profit_effect": {
        "system": (
            "你是A股市场复盘分析师，擅长从涨幅Top100数据归纳赚钱效应。"
            "回答需客观、可解释、避免过度预测。"
        ),
        "prompt": (
            "以下是全市场涨幅Top100股票数据摘要（JSON）：\n"
            "{payload}\n\n"
            "请总结赚钱效应：包括强势风格/方向、涨幅集中度、情绪温度与持续性线索。"
            "输出中文，简洁专业，分段，不超过150字，使用markdown格式。"
        ),
    },
    "loss_effect": {
        "system": (
            "你是A股市场复盘分析师，擅长从跌幅Top100数据归纳亏钱效应。"
            "回答需客观、可解释、避免过度预测。"
        ),
        "prompt": (
            "以下是全市场跌幅Top100股票数据摘要（JSON）：\n"
            "{payload}\n\n"
            "请总结亏钱效应：包括弱势风格/方向、跌幅集中度、风险偏好与恐慌程度线索。"
            "输出中文，简洁专业，分段，不超过150字，使用markdown格式。"
        ),
    },
}


@st.cache_data(ttl="1h")
def llm_summarize(template_id, payload_str):
    template = LLM_TEMPLATES.get(template_id)
    if not template:
        return ""
    prompt = template["prompt"].format(payload=payload_str)
    try:
        return get_llm_response(prompt, system_message=template.get("system"))
    except Exception as exc:
        return f"AIæ‘˜è¦ç”Ÿæˆå¤±è´¥ï¼š{exc}"


def build_market_section(select_date, all_stocks_df=None):
    sh_df, cyb_df, kcb_df, market_data = get_market_data()
    up_stocks = _safe_market_value(market_data, '涓婃定')
    down_stocks = _safe_market_value(market_data, '涓嬭穼')
    limit_up = _safe_market_value(market_data, '娑ㄥ仠')
    limit_down = _safe_market_value(market_data, '璺屽仠')
    activity = _safe_market_value(market_data, '娲昏穬搴?')
    if isinstance(activity, str) and '%' in str(activity):
        activity = str(activity).replace('%', '')

    if all_stocks_df is None:
        try:
            all_stocks_df = get_all_stocks(select_date)
        except Exception:
            all_stocks_df = None

    market_overview = {
        '涓婃定': up_stocks,
        '涓嬭穼': down_stocks,
        '娑ㄥ仠': limit_up,
        '璺屽仠': limit_down,
        '娲昏穬搴?': activity,
        'range_distribution': _build_pct_distribution(all_stocks_df)
    }
    indices = {
        'sh_df': _df_to_records(sh_df),
        'cyb_df': _df_to_records(cyb_df),
        'kcb_df': _df_to_records(kcb_df)
    }
    return {'indices': indices, 'market_overview': market_overview}, all_stocks_df


def build_top100_section(select_date, all_stocks_df=None):
    source_df = all_stocks_df if all_stocks_df is not None else get_all_stocks(select_date)
    today_top_stocks = _normalize_top_stocks_df(source_df)

    if not today_top_stocks.empty:
        cols = list(today_top_stocks.columns)
        if len(cols) >= 4:
            rename_pos = {cols[0]: 'name', cols[1]: 'code', cols[2]: 'pct', cols[3]: 'amount'}
            today_top_stocks = today_top_stocks.rename(columns=rename_pos)

    turnover_records = []
    if not today_top_stocks.empty and {'pct', 'amount', 'name', 'code'}.issubset(today_top_stocks.columns):
        top_100_by_turnover = today_top_stocks.sort_values('amount', ascending=False).head(100)
        top_100_by_turnover['pct'] = pd.to_numeric(top_100_by_turnover['pct'], errors='coerce')
        turnover_records = top_100_by_turnover[['name', 'code', 'pct', 'amount']].to_dict(orient='records')

    range_data = {'sh_stocks': [], 'cyb_kcb_stocks': []}
    if not today_top_stocks.empty and {'pct', 'amount', 'name', 'code'}.issubset(today_top_stocks.columns):
        top_100_by_range = today_top_stocks.sort_values('pct', ascending=False).head(100)
        code_series = top_100_by_range['code'].astype(str)
        sh_df = top_100_by_range[
            (code_series.str[2:].str.startswith('6') | code_series.str[2:].str.startswith('0')) & ~code_series.str[2:].str.startswith('688')
        ]
        cyb_kcb_df = top_100_by_range[
            code_series.str[2:].str.startswith('3') |
            code_series.str[2:].str.startswith('688')
        ]
        range_data = {
            'sh_stocks': sh_df[['name', 'code', 'pct', 'amount']].to_dict(orient='records'),
            'cyb_kcb_stocks': cyb_kcb_df[['name', 'code', 'pct', 'amount']].to_dict(orient='records')
        }

    return {'top_100_turnover': turnover_records, 'top_100_range': range_data}, source_df

def is_review_data_complete(review_data):
    if not review_data:
        return False
    indices = review_data.get('indices') or {}
    if not indices:
        return False
    if not indices.get('sh_df') or not indices.get('cyb_df') or not indices.get('kcb_df'):
        return False
    if not review_data.get('market_overview'):
        return False
    if not review_data.get('top_100_turnover'):
        return False
    top_100_range = review_data.get('top_100_range') or {}
    if not top_100_range.get('sh_stocks') and not top_100_range.get('cyb_kcb_stocks'):
        return False
    return True


def build_review_data(select_date, show_modules=None):
    review_data = {'date': select_date.strftime('%Y-%m-%d')}

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
    else:
        review_data['indices'] = {'sh_df': [], 'cyb_df': [], 'kcb_df': []}
        review_data['market_overview'] = {}

    if _should("top100"):
        top_section, all_stocks_df = build_top100_section(select_date, all_stocks_df)
        review_data.update(top_section)

        gainers = []
        losers = []
        if all_stocks_df is not None and not all_stocks_df.empty and "pct" in all_stocks_df.columns:
            view = all_stocks_df.copy()
            view["pct"] = pd.to_numeric(view["pct"], errors="coerce")
            view = view.dropna(subset=["pct"])
            gainers_df = view.sort_values("pct", ascending=False).head(100)
            losers_df = view.sort_values("pct", ascending=True).head(100)
            keep_cols = [col for col in ["code", "name", "pct", "amount", "mkt_cap"] if col in view.columns]
            if keep_cols:
                gainers = gainers_df[keep_cols].to_dict(orient="records")
                losers = losers_df[keep_cols].to_dict(orient="records")
        review_data["top_100_gainers"] = gainers
        review_data["top_100_losers"] = losers
    else:
        review_data['top_100_turnover'] = [] 
        review_data['top_100_range'] = {'sh_stocks': [], 'cyb_kcb_stocks': []}
        review_data["top_100_gainers"] = []
        review_data["top_100_losers"] = []

    return review_data

def display_review_data(review_data, show_modules=None):
    show_modules = show_modules or {}
    date_str = review_data.get('date')
    if isinstance(date_str, (datetime.date, datetime.datetime)):
        zt_date = date_str.strftime('%Y%m%d')
    elif isinstance(date_str, str) and date_str:
        zt_date = date_str.replace('-', '')
    else:
        zt_date = datetime.datetime.now().strftime('%Y%m%d')
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

        btc = external.get("btc") or {}
        us10y = external.get("us10y") or {}
        xau = external.get("xau") or {}

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
            fig = go.Figure(go.Scatter(
                x=df["date"],
                y=df["value"],
                mode="lines",
                line=dict(color=color, width=2),
            ))
            fig.update_layout(
                height=120,
                width=220,
                margin=dict(l=6, r=6, t=6, b=6),
                xaxis=dict(visible=False),
                yaxis=dict(visible=False),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=False)

        cols = st.columns(3)
        with cols[0]:
            st.metric(
                "\u6bd4\u7279\u5e01 (BTC/USD)",
                _format_value(btc.get("value"), "${:,.0f}"),
                _format_delta(btc.get("change"), "{:+.2f}%"),
            )
            btc_date = _format_date(btc.get("date"))
            if btc_date:
                st.caption(f"\u65e5\u671f: {btc_date}")
            _render_sparkline(btc.get("series") or [], "#f39c12")
        with cols[1]:
            st.metric(
                "\u7f8e\u56fd10Y\u56fd\u503a\u6536\u76ca\u7387",
                _format_value(us10y.get("value"), "{:.2f}%"),
                _format_delta(us10y.get("change"), "{:+.1f}bp"),
            )
            us10y_date = _format_date(us10y.get("date"))
            if us10y_date:
                st.caption(f"\u65e5\u671f: {us10y_date}")
            _render_sparkline(us10y.get("series") or [], "#2f80ed")
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

        risk_payload = {
            "btc_usd": {
                "date": _format_date(btc.get("date")),
                "price": btc.get("value"),
                "change_pct": btc.get("change"),
                "series": btc.get("series") or [],
            },
            "us10y_yield": {
                "date": _format_date(us10y.get("date")),
                "yield_pct": us10y.get("value"),
                "change_bp": us10y.get("change"),
                "series": us10y.get("series") or [],
            },
            "xau_gold": {
                "date": _format_date(xau.get("date")),
                "price": xau.get("value"),
                "change_pct": xau.get("change"),
                "series": xau.get("series") or [],
            },
        }
        has_data = any(metric.get("value") is not None for metric in (btc, us10y, xau))
        if has_data:
            payload_str = json.dumps(risk_payload, ensure_ascii=False)
            risk_summary = llm_summarize("external_risk", payload_str)
            if risk_summary:
                st.markdown(
                    f"""
                    <div style="
                        background:#eef4ff;
                        border-left:4px solid #2f80ed;
                        border-radius:8px;
                        padding:12px 16px;
                        margin:8px 0 16px 0;
                        color:#1f2d3d;
                        font-size:0.95rem;
                        line-height:1.6;
                    ">
                        {risk_summary.replace("\n", "<br>")}
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        st.markdown("---")

    if _show("market"):
        _section_title("今日大盘")
        indices = review_data.get('indices', {})
        sh_df = _records_to_df(indices.get('sh_df', []))
        cyb_df = _records_to_df(indices.get('cyb_df', []))
        kcb_df = _records_to_df(indices.get('kcb_df', []))

        market_overview = review_data.get('market_overview', {})
        up_stocks = market_overview.get('上涨')
        down_stocks = market_overview.get('下跌')
        limit_up = market_overview.get('涨停')
        limit_down = market_overview.get('跌停')
        activity = market_overview.get('活跃度')

        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            st.markdown("上证指数")
            if not sh_df.empty:
                plotK(sh_df)
        with col2:
            st.markdown("创业板指数")
            if not cyb_df.empty:
                plotK(cyb_df)
        with col3:
            st.markdown("科创板指数")
            if not kcb_df.empty:
                plotK(kcb_df)

        import os
        csv_file = os.path.join('datas', 'market_data.csv')
        if os.path.exists(csv_file):
            try:
                df_history = pd.read_csv(csv_file)
                df_history = df_history.loc[:, ~df_history.columns.str.contains('^Unnamed')]
                if '日期' in df_history.columns:
                    df_history['日期'] = pd.to_datetime(df_history['日期'], errors='coerce')
                    df_history = df_history.dropna(subset=['日期'])
                    df_history = df_history.sort_values('日期')
                    df_history = df_history.tail(30)
                    numeric_cols = ['上涨', '下跌', '涨停', '跌停', '活跃度', '成交额']
                    for col in numeric_cols:
                        if col in df_history.columns:
                            if col == '活跃度':
                                df_history[col] = df_history[col].astype(str).str.replace('%', '').astype(float)
                            else:
                                df_history[col] = pd.to_numeric(df_history[col], errors='coerce')

                    latest_row = df_history.iloc[-1] if not df_history.empty else None
                    if latest_row is not None:
                        up_stocks = latest_row.get('上涨', up_stocks)
                        down_stocks = latest_row.get('下跌', down_stocks)
                        limit_up = latest_row.get('涨停', limit_up)
                        limit_down = latest_row.get('跌停', limit_down)
                        activity = latest_row.get('活跃度', activity)

                    summary_payload = {
                        'indices': {
                            '上证指数': _build_index_snapshot(sh_df),
                            '创业板指数': _build_index_snapshot(cyb_df),
                            '科创板指数': _build_index_snapshot(kcb_df)
                        },
                    }
                    payload_str = json.dumps(summary_payload, ensure_ascii=False)
                    llm_summary = llm_summarize('market_overview', payload_str)
                    if llm_summary:
                        st.markdown(
                            f"""
                            <div style="
                                background: #f2f6ff;
                                border-left: 4px solid #4c6ef5;
                                border-radius: 8px;
                                padding: 12px 16px;
                                margin: 8px 0 16px 0;
                                color: #1a2b49;
                                font-size: 0.95rem;
                                line-height: 1.6;
                            ">
                                {llm_summary.replace('\n', '<br>')}
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                    fin_series = get_financing_net_buy_series(60)

                    first_row = st.columns(3)
                    with first_row[0]:
                        if '成交额' in df_history.columns:
                            fig_amount = go.Figure()
                            fig_amount.add_trace(go.Scatter(
                                x=df_history['日期'],
                                y=df_history['成交额'],
                                mode='lines+markers',
                                name='成交额',
                                line=dict(color='#4c6ef5', width=2),
                                marker=dict(size=4)
                            ))
                            fig_amount.update_layout(
                                title='成交额',
                                xaxis_title='日期',
                                yaxis_title='成交额',
                                height=300,
                                hovermode='x unified'
                            )
                            st.plotly_chart(fig_amount, use_container_width=True)

                    with first_row[1]:
                        if '活跃度' in df_history.columns:
                            fig_activity = go.Figure()
                            fig_activity.add_trace(go.Scatter(
                                x=df_history['日期'],
                                y=df_history['活跃度'],
                                mode='lines+markers',
                                name='情绪指数',
                                line=dict(color='#f39c12', width=2),
                                marker=dict(size=4)
                            ))
                            fig_activity.update_layout(
                                title='情绪指数（活跃度）',
                                xaxis_title='日期',
                                yaxis_title='活跃度 (%)',
                                height=300,
                                hovermode='x unified'
                            )
                            st.plotly_chart(fig_activity, use_container_width=True)

                    with first_row[2]:
                        if fin_series is not None and not fin_series.empty:
                            fin_series = fin_series.sort_values('date')
                            colors = fin_series['融资净买入'].apply(lambda x: '#e74c3c' if x >= 0 else '#2ecc71')
                            fig_financing = go.Figure(go.Bar(
                                x=fin_series['date'],
                                y=fin_series['融资净买入'],
                                marker_color=colors,
                                name='融资净买入',
                                hovertemplate='%{x|%Y-%m-%d}<br>净买入: %{y:.0f}<extra></extra>'
                            ))
                            fig_financing.update_layout(
                                title='融资净买入（近60交易日）',
                                xaxis_title='日期',
                                yaxis_title='金额',
                                height=300,
                                hovermode='x unified',
                                bargap=0.2
                            )
                            st.plotly_chart(fig_financing, use_container_width=True)

                    second_row = st.columns(3)
                    with second_row[0]:
                        if '上涨' in df_history.columns and '下跌' in df_history.columns:
                            fig_up_down = go.Figure()
                            fig_up_down.add_trace(go.Scatter(
                                x=df_history['日期'],
                                y=df_history['上涨'],
                                mode='lines+markers',
                                name='上涨数',
                                line=dict(color='#e74c3c', width=2),
                                marker=dict(size=4)
                            ))
                            fig_up_down.add_trace(go.Scatter(
                                x=df_history['日期'],
                                y=df_history['下跌'],
                                mode='lines+markers',
                                name='下跌数',
                                line=dict(color='#2ecc71', width=2),
                                marker=dict(size=4)
                            ))
                            fig_up_down.update_layout(
                                title='上涨数 vs 下跌数',
                                xaxis_title='日期',
                                yaxis_title='数量',
                                height=300,
                                hovermode='x unified',
                                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                            )
                            st.plotly_chart(fig_up_down, use_container_width=True)
                    with second_row[1]:
                        if '涨停' in df_history.columns and '跌停' in df_history.columns:
                            fig_limit = go.Figure()
                            fig_limit.add_trace(go.Scatter(
                                x=df_history['日期'],
                                y=df_history['涨停'],
                                mode='lines+markers',
                                name='涨停数',
                                line=dict(color='#c0392b', width=2),
                                marker=dict(size=4)
                            ))
                            fig_limit.add_trace(go.Scatter(
                                x=df_history['日期'],
                                y=df_history['跌停'],
                                mode='lines+markers',
                                name='跌停数',
                                line=dict(color='#27ae60', width=2),
                                marker=dict(size=4)
                            ))
                            fig_limit.update_layout(
                                title='涨停数 vs 跌停数',
                                xaxis_title='日期',
                                yaxis_title='数量',
                                height=300,
                                hovermode='x unified',
                                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                            )
                            st.plotly_chart(fig_limit, use_container_width=True)
                    with second_row[2]:
                        st.empty()
            except Exception as e:
                st.warning(f"读取历史市场数据失败: {e}")

        st.markdown("---")

    if _show("top100"):
        _section_title("市场全貌分析")
        top_100_records = review_data.get('top_100_turnover', [])
        market_overview = review_data.get('market_overview', {})
        range_distribution = market_overview.get('range_distribution', [])

        top_100_by_turnover = pd.DataFrame(top_100_records) if top_100_records else pd.DataFrame()
        if not top_100_by_turnover.empty:
            top_100_by_turnover['pct'] = pd.to_numeric(top_100_by_turnover['pct'], errors='coerce')
            up_stocks = top_100_by_turnover[top_100_by_turnover['pct'] > 2]
            down_stocks = top_100_by_turnover[top_100_by_turnover['pct'] < -2]
            shake_stocks = top_100_by_turnover[(top_100_by_turnover['pct'] >= -2) & (top_100_by_turnover['pct'] <= 2)]
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
                    order_labels = [">20%", "10%~20%", "5%~10%", "3%~5%", "0%~3%", "-3%~0%", "-5%~-3%", "-10%~-5%", "<-10%"]
                    order_map = {label: idx for idx, label in enumerate(order_labels)}
                    dist_df['order'] = dist_df['label'].map(order_map)
                    dist_df = dist_df.sort_values('order')
                    colors = ['#e74c3c' if row['bucket_start'] >= 0 else '#2ecc71' for _, row in dist_df.iterrows()]
                    fig_range = go.Figure(go.Bar(
                        x=dist_df['label'],
                        y=dist_df['count'],
                        text=dist_df['count'],
                        textposition='outside',
                        marker_color=colors,
                        hovertemplate='%{x}<br>家数: %{y}<extra></extra>'
                    ))
                    fig_range.update_layout(
                        title='当日涨跌幅分布',
                        xaxis_title='涨跌幅区间(%)',
                        yaxis_title='家数',
                        height=400,
                        margin=dict(t=30, l=10, r=10, b=40)
                    )
                    st.plotly_chart(fig_range, use_container_width=True)
                else:
                    st.info("暂无涨跌幅分布数据")
            else:
                st.info("暂无涨跌幅分布数据")
        with overview_cols[1]:
            if top_100_by_turnover.empty:
                st.info("暂无TOP100数据")
            else:
                categories = ['小涨(2-5%)', '中涨(5-9%)', '大涨(>9%)',
                            '小跌(-2~-5%)', '中跌(-5~-9%)', '大跌(<-9%)',
                            '震荡(-2%~2%)']
                small_up = up_stocks[(up_stocks['pct'] >= 2) & (up_stocks['pct'] < 5)]
                medium_up = up_stocks[(up_stocks['pct'] >= 5) & (up_stocks['pct'] < 9)]
                large_up = up_stocks[up_stocks['pct'] >= 9]
                small_down = down_stocks[(down_stocks['pct'] <= -2) & (down_stocks['pct'] > -5)]
                medium_down = down_stocks[(down_stocks['pct'] <= -5) & (down_stocks['pct'] > -9)]
                large_down = down_stocks[down_stocks['pct'] <= -9]
                values = [
                    len(small_up),
                    len(medium_up),
                    len(large_up),
                    len(small_down),
                    len(medium_down),
                    len(large_down),
                    shake_count,
                ]
                colors = ['#c0392b', '#a93226', '#922b21',
                        '#27ae60', '#229954', '#1e8449',
                        '#f39c12']
                fig_bar = go.Figure()
                fig_bar.add_trace(go.Bar(
                    x=categories,
                    y=values,
                    marker_color=colors,
                    text=values,
                    textposition='outside',
                    hovertemplate='<b>%{x}</b><br>数量: %{y}只<extra></extra>'
                ))
                fig_bar.update_layout(
                    title='成交额100涨幅分布',
                    xaxis_title='分类',
                    yaxis_title='股票数量',
                    height=400,
                    xaxis=dict(tickangle=-45),
                    yaxis=dict(range=[0, max(values) * 1.2] if values else [0, 10])
                )
                st.plotly_chart(fig_bar, use_container_width=True)

        top_100_gainers = review_data.get('top_100_gainers', [])
        top_100_losers = review_data.get('top_100_losers', [])

        if top_100_gainers:
            payload_str = json.dumps({'top_100_gainers': top_100_gainers}, ensure_ascii=False)
            profit_summary = llm_summarize('profit_effect', payload_str)
            if profit_summary:
                profit_parts = _split_summary_parts(profit_summary, expected=3)
                _render_three_blocks(
                    "赚钱效应",
                    ["强势风格/方向", "涨幅集中度", "情绪温度与持续性"],
                    profit_parts
                )
        else:
            st.info("暂无涨幅Top100数据")

        if top_100_losers:
            payload_str = json.dumps({'top_100_losers': top_100_losers}, ensure_ascii=False)
            loss_summary = llm_summarize('loss_effect', payload_str)
            if loss_summary:
                loss_parts = _split_summary_parts(loss_summary, expected=3)
                _render_three_blocks(
                    "亏钱效应",
                    ["弱势风格/方向", "跌幅集中度", "风险偏好与恐慌程度"],
                    loss_parts
                )
        else:
            st.info("暂无跌幅Top100数据")

        st.markdown("---")

    if _show("short"):
        _section_title("短线效应")
        st.markdown("#### 游资龙虎榜")
        try:
            lh_yz_df = get_longhu_data(zt_date)
        except Exception as exc:
            st.warning(f"获取游资龙虎榜失败: {exc}")
            lh_yz_df = pd.DataFrame()

        if lh_yz_df is None or lh_yz_df.empty:
            st.info("暂无游资龙虎榜数据")
        else:
            net_col = None
            for candidate in ['净买入', '净买入额', '净买入金额']:
                if candidate in lh_yz_df.columns:
                    net_col = candidate
                    break
            if net_col is None and {'买入金额', '卖出金额'}.issubset(lh_yz_df.columns):
                lh_yz_df['净买入'] = lh_yz_df['买入金额'] - lh_yz_df['卖出金额']
                net_col = '净买入'

            if net_col is None:
                st.info("游资龙虎榜缺少净买入字段，无法排序")
            else:
                def _format_amount(value):
                    if value is None or (isinstance(value, float) and pd.isna(value)):
                        return ""
                    try:
                        amount = float(value)
                    except Exception:
                        return ""
                    if abs(amount) >= 1e8:
                        return f"{amount / 1e8:.2f}亿元"
                    return f"{amount / 1e4:.0f}万"

                view_df = lh_yz_df.copy()
                view_df['净买入金额'] = view_df[net_col].clip(lower=0)
                view_df['净卖出金额'] = (-view_df[net_col]).clip(lower=0)

                buy_df = view_df.sort_values('净买入金额', ascending=False)
                sell_df = view_df.sort_values('净卖出金额', ascending=False)

                for col in ['净买入金额', '净卖出金额']:
                    view_df[col] = view_df[col].apply(_format_amount)
                    buy_df[col] = buy_df[col].apply(_format_amount)
                    sell_df[col] = sell_df[col].apply(_format_amount)

                buy_cols = [col for col in ['游资', '买入股票', '净买入金额'] if col in view_df.columns]
                sell_cols = [col for col in ['游资', '买入股票', '净卖出金额'] if col in view_df.columns]
                if not buy_cols:
                    buy_cols = list(view_df.columns)
                if not sell_cols:
                    sell_cols = list(view_df.columns)
                col_buy, col_sell = st.columns(2)
                with col_buy:
                    st.markdown("**净买入排序**")
                    st.dataframe(
                        buy_df[buy_cols].reset_index(drop=True),
                        hide_index=True,
                        use_container_width=True
                    )
                with col_sell:
                    st.markdown("**净卖出排序**")
                    st.dataframe(
                        sell_df[sell_cols].reset_index(drop=True),
                        hide_index=True,
                        use_container_width=True
                    )

        st.markdown("#### 短线连板梯队")
        zt_pool = get_zt_pool(zt_date)
        dt_pool_error = False
        try:
            dt_pool = get_dt_pool(zt_date)
        except Exception as exc:
            st.warning(f"获取跌停股池失败: {exc}")
            dt_pool_error = True
            dt_pool = pd.DataFrame()
        dt_tier_df, dt_tier_counts = _build_dt_tiers(dt_pool)

        tier_df = pd.DataFrame()
        tier_counts = pd.DataFrame()
        total_zt = 0
        broken_count = None
        break_rate = None
        max_streak = None
        streak_stock_count = None

        if zt_pool is None or zt_pool.empty:
            st.info("暂无涨停池数据")
        else:
            keep_cols = ['代码', '名称', '涨跌幅', '成交额', '总市值', '首次封板时间', '炸板次数', '连板数']
            existing_cols = [col for col in keep_cols if col in zt_pool.columns]
            if existing_cols:
                zt_pool = zt_pool[existing_cols].copy()
            if '首次封板时间' in zt_pool.columns:
                zt_pool = zt_pool.sort_values(by='首次封板时间')

            total_zt = len(zt_pool)
            if '炸板次数' in zt_pool.columns:
                broken_count = (pd.to_numeric(zt_pool['炸板次数'], errors='coerce').fillna(0) > 0).sum()
                break_rate = broken_count / total_zt if total_zt > 0 else None

            tier_df, tier_counts = _build_zt_tiers(zt_pool)
            if not tier_df.empty and '连板' in tier_df.columns:
                max_streak = int(tier_df['连板'].max())
                streak_stock_count = int((tier_df['连板'] >= 2).sum())

            metric_cols = st.columns(4)
            with metric_cols[0]:
                st.metric("连板股家数", streak_stock_count if streak_stock_count is not None else 0)
            with metric_cols[1]:
                st.metric("连板高度", max_streak if max_streak is not None else 0)
            with metric_cols[2]:
                rate_text = f"{break_rate * 100:.1f}%" if break_rate is not None else "—"
                st.metric("涨停炸板率", rate_text)
            with metric_cols[3]:
                st.metric("炸板家数", broken_count if broken_count is not None else 0)

            chart_cols = st.columns(2)
            with chart_cols[0]:
                if tier_counts.empty or '连板' not in tier_counts.columns:
                    st.info("暂无连板统计数据")
                else:
                    chart_df = tier_counts[['连板', '数量']].copy()
                    chart_df['连板'] = pd.to_numeric(chart_df['连板'], errors='coerce')
                    chart_df['数量'] = pd.to_numeric(chart_df['数量'], errors='coerce')
                    chart_df = chart_df.dropna(subset=['连板', '数量']).sort_values('连板')
                    if chart_df.empty:
                        st.info("暂无连板统计数据")
                    else:
                        fig_tier = go.Figure(go.Bar(
                            x=chart_df['连板'].astype(int).astype(str) + "板",
                            y=chart_df['数量'],
                            text=chart_df['数量'],
                            textposition='outside',
                            marker_color='#4f7cff',
                            hovertemplate='连板: %{x}<br>数量: %{y}<extra></extra>'
                        ))
                        fig_tier.update_layout(
                            title='连板高度-家数分布',
                            xaxis_title='连板高度',
                            yaxis_title='家数',
                            height=320,
                            margin=dict(t=40, l=10, r=10, b=40)
                        )
                        st.plotly_chart(fig_tier, use_container_width=True)
            with chart_cols[1]:
                if dt_pool_error:
                    st.info("暂无跌停股池数据")
                elif dt_tier_counts.empty or '连续跌停' not in dt_tier_counts.columns:
                    st.info("暂无跌停统计数据")
                else:
                    dt_chart = dt_tier_counts[['连续跌停', '数量']].copy()
                    dt_chart['连续跌停'] = pd.to_numeric(dt_chart['连续跌停'], errors='coerce')
                    dt_chart['数量'] = pd.to_numeric(dt_chart['数量'], errors='coerce')
                    dt_chart = dt_chart.dropna(subset=['连续跌停', '数量']).sort_values('连续跌停')
                    if dt_chart.empty:
                        st.info("暂无跌停统计数据")
                    else:
                        fig_dt = go.Figure(go.Bar(
                            x=dt_chart['连续跌停'].astype(int).astype(str) + "板",
                            y=dt_chart['数量'],
                            text=dt_chart['数量'],
                            textposition='outside',
                            marker_color='#2ecc71',
                            hovertemplate='连续跌停: %{x}<br>数量: %{y}<extra></extra>'
                        ))
                        fig_dt.update_layout(
                            title='连续跌停高度-家数分布',
                            xaxis_title='连续跌停高度',
                            yaxis_title='家数',
                            height=320,
                            margin=dict(t=40, l=10, r=10, b=40)
                        )
                        st.plotly_chart(fig_dt, use_container_width=True)

            detail_cols = st.columns(2)
            with detail_cols[0]:
                if tier_counts.empty:
                    st.info("暂无连板明细数据")
                else:
                    grouped = []
                    for streak, group in tier_df.groupby('连板'):
                        if '名称' in group.columns and '代码' in group.columns:
                            names = '、'.join(
                                [f"{n}({c})" for n, c in zip(group['名称'], group['代码'])]
                            )
                        elif '名称' in group.columns:
                            names = '、'.join(group['名称'].astype(str))
                        elif '代码' in group.columns:
                            names = '、'.join(group['代码'].astype(str))
                        else:
                            names = ''
                        grouped.append({'连板': streak, '数量': len(group), '个股': names})
                    tier_detail = pd.DataFrame(grouped).sort_values('连板', ascending=False)
                    st.dataframe(tier_detail, use_container_width=True, hide_index=True)
            with detail_cols[1]:
                if dt_pool_error:
                    st.info("暂无跌停股池数据")
                elif dt_tier_counts.empty:
                    st.info("暂无跌停明细数据")
                else:
                    dt_grouped = []
                    for streak, group in dt_tier_df.groupby('连续跌停'):
                        if '名称' in group.columns and '代码' in group.columns:
                            names = '、'.join(
                                [f"{n}({c})" for n, c in zip(group['名称'], group['代码'])]
                            )
                        elif '名称' in group.columns:
                            names = '、'.join(group['名称'].astype(str))
                        elif '代码' in group.columns:
                            names = '、'.join(group['代码'].astype(str))
                        else:
                            names = ''
                        dt_grouped.append({'连续跌停': streak, '数量': len(group), '个股': names})
                    dt_detail = pd.DataFrame(dt_grouped).sort_values('连续跌停', ascending=False)
                    st.dataframe(dt_detail, use_container_width=True, hide_index=True)

        # AI 总结已移除



st.set_page_config(
    page_title="复盘",
    page_icon="🚀",
    layout="wide"
)

today = datetime.datetime.now()
select_date = st.date_input("选择日期",today)
st.markdown("#### 复盘模块显示")
show_external = st.checkbox("\u5916\u56f4\u6307\u6807", value=True, key="show_external")
show_market = st.checkbox("今日大盘", value=True, key="show_market")
show_top100 = st.checkbox("市场全貌分析", value=True, key="show_top100")
show_short = st.checkbox("短线效应", value=True, key="show_short")
show_modules = {
    "external": show_external,
    "market": show_market,
    "top100": show_top100,
    "short": show_short,
}
load_btn = st.button('Load')

if load_btn:
    if select_date.weekday() >= 5: 
        st.warning("非交易日")
        st.stop()
    
    # date_str = select_date.strftime('%Y-%m-%d')
    # cached_data = load_review_data(date_str)
    # if cached_data:
    #     st.info(f"使用已缓存复盘数据: {date_str}")
    #     display_review_data(cached_data)
    # else:
    review_data = build_review_data(select_date, show_modules)
    #     if is_review_data_complete(review_data):
    #         save_review_data(date_str, review_data)
    #         st.success(f"复盘数据已保存: {date_str}")
    #     else:
    #         st.warning("复盘数据不完整，未写入缓存")
    display_review_data(review_data, show_modules)
