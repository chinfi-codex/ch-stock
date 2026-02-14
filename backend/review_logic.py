# -*- coding: utf-8 -*-
"""ch-stock review logic (extracted from app.py).

Important constraint:
- Keep data/compute logic identical to app.py.
- Do NOT include Streamlit UI rendering; this module is API-friendly.

NOTE: Streamlit caching decorators are removed; outputs should remain identical.
"""

import datetime
import json

import pandas as pd

from tools import get_llm_response
from tools.market_data import get_market_data, get_all_stocks
from tools.financial_data import EconomicIndicators
from tools.storage_utils import save_review_data, load_review_data

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


def build_external_section(days=120):
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
            "你是外盘与风险资产复盘分析师，擅长基于时间序列做趋势判断。"
            "回答需客观、可解释、避免过度预测。"
        ),
        "prompt": (
            "以下是外围指标数据（JSON）：\n"
            "{payload}\n\n"
            "关注比特币和金价的中期（120天）、短期（两天内）走势趋势，并分别进行分析。\n"
            "输出格式：\n"
            "比特币：中期...；短期...\n"
            "金价：中期...；短期...\n"
            "要求：中文，简洁专业，不超过120字，不要输出额外说明。"
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
            "请结构化总结赚钱效应，严格输出3行，按顺序对应："
            "1) 强势风格/方向 2) 涨幅集中度 3) 情绪温度与持续性。\n"
            "格式要求：\n"
            "1. <一句话>\n"
            "2. <一句话>\n"
            "3. <一句话>\n"
            "每行不超过100字；不要输出标题、前后说明、代码块。"
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
            "请结构化总结亏钱效应，严格输出3行，按顺序对应："
            "1) 弱势风格/方向 2) 跌幅集中度 3) 风险偏好与恐慌程度。\n"
            "格式要求：\n"
            "1. <一句话>\n"
            "2. <一句话>\n"
            "3. <一句话>\n"
            "每行不超过100字；不要输出标题、前后说明、代码块。"
        ),
    },
}


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

