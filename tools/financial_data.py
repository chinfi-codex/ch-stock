#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
金融数据原子能力。
"""

import logging
import time

import pandas as pd
import requests
import streamlit as st
import tushare as ts

from infra.config import get_alpha_vantage_key, get_tushare_token


logger = logging.getLogger(__name__)


def _get_av_key():
    """获取 Alpha Vantage API Key。"""
    return get_alpha_vantage_key()


class EconomicIndicators:
    """经济指标获取。"""

    _last_request_time = 0

    @classmethod
    def _rate_limited_request(cls, url, max_retries=3):
        for attempt in range(max_retries):
            try:
                elapsed = time.time() - cls._last_request_time
                if elapsed < 12:
                    time.sleep(12 - elapsed)

                response = requests.get(url, timeout=30)
                cls._last_request_time = time.time()
                if response.status_code != 200:
                    logger.error(f"HTTP {response.status_code}: {response.text}")
                    return {}

                data = response.json()
                if "Note" in data or "Information" in data:
                    logger.warning(f"Alpha Vantage API limit: {data}")
                    if attempt < max_retries - 1:
                        time.sleep(15)
                        continue
                return data
            except Exception as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
        return {}

    @staticmethod
    @st.cache_data(ttl="5m", show_spinner=True)
    def get_exchangerates_realtime(from_currency="usd", to_currency="cny"):
        av_key = _get_av_key()
        if not av_key:
            return None
        url = (
            "https://www.alphavantage.co/query?"
            f"function=CURRENCY_EXCHANGE_RATE&from_currency={from_currency}"
            f"&to_currency={to_currency}&apikey={av_key}"
        )
        response = EconomicIndicators._rate_limited_request(url)
        return response.get("Realtime Currency Exchange Rate")

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_exchangerates_daily(from_currency="usd", to_currency="cny", curDate=60):
        av_key = _get_av_key()
        if not av_key:
            return pd.DataFrame()
        url = (
            "https://www.alphavantage.co/query?"
            f"function=FX_DAILY&from_symbol={from_currency}&to_symbol={to_currency}"
            f"&apikey={av_key}"
        )
        response = EconomicIndicators._rate_limited_request(url)
        try:
            time_series_data = response.get("Time Series FX (Daily)")
            if not time_series_data:
                return pd.DataFrame()
            df = pd.DataFrame(time_series_data).transpose()
            df.index.name = "date"
            df = df.reset_index()
            df["4. close"] = pd.to_numeric(df["4. close"], errors="coerce")
            return df.head(curDate)
        except Exception as e:
            logger.error(f"Failed to parse FX data: {e}")
            return pd.DataFrame()

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_treasury_yield(maturity="10year", interval="daily", curDate=3):
        av_key = _get_av_key()
        if not av_key:
            return pd.DataFrame()
        url = (
            "https://www.alphavantage.co/query?"
            f"function=TREASURY_YIELD&interval={interval}&maturity={maturity}"
            f"&apikey={av_key}"
        )
        response = EconomicIndicators._rate_limited_request(url)
        try:
            data = response.get("data")
            if not data:
                return pd.DataFrame()
            df = pd.DataFrame(data)
            df["value"] = pd.to_numeric(df.get("value"), errors="coerce")
            return df.head(curDate)
        except Exception as e:
            logger.error(f"Failed to parse treasury data: {e}")
            return pd.DataFrame()

    @staticmethod
    @st.cache_data(ttl="15m", show_spinner=True)
    def get_equity_daily(symbol, outputsize="compact", curDate=60):
        av_key = _get_av_key()
        if not av_key:
            return pd.DataFrame()
        url = (
            "https://www.alphavantage.co/query?"
            f"function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}"
            f"&outputsize={outputsize}&apikey={av_key}"
        )
        response = EconomicIndicators._rate_limited_request(url)
        try:
            ts_key = next((k for k in response.keys() if "Time Series" in k), None)
            if not ts_key:
                return pd.DataFrame()
            df = pd.DataFrame(response[ts_key]).T
            close_col = next(
                (col for col in df.columns if col.strip().startswith("4. close")), None
            )
            if close_col is None:
                close_col = next(
                    (col for col in df.columns if "close" in col.lower()), None
                )
            if close_col is None:
                return pd.DataFrame()
            df = df[[close_col]].rename(columns={close_col: "close"})
            df.index.name = "date"
            df = df.reset_index()
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            return df.head(curDate)
        except Exception as e:
            logger.error(f"Failed to parse equity data: {e}")
            return pd.DataFrame()

    @staticmethod
    @st.cache_data(ttl="15m", show_spinner=True)
    def get_crypto_daily(symbol="BTC", market="USD", curDate=60):
        av_key = _get_av_key()
        if not av_key:
            return pd.DataFrame()
        url = (
            "https://www.alphavantage.co/query?"
            f"function=DIGITAL_CURRENCY_DAILY&symbol={symbol}&market={market}"
            f"&apikey={av_key}"
        )
        response = EconomicIndicators._rate_limited_request(url)
        try:
            ts_key = next((k for k in response.keys() if "Time Series" in k), None)
            if not ts_key:
                return pd.DataFrame()
            df = pd.DataFrame(response[ts_key]).T
            market_upper = str(market).upper()
            close_col = next(
                (
                    col
                    for col in df.columns
                    if "close" in col.lower() and f"({market_upper})" in col
                ),
                None,
            )
            if close_col is None:
                close_col = next(
                    (col for col in df.columns if "close" in col.lower()), None
                )
            if close_col is None:
                return pd.DataFrame()
            df = df[[close_col]].rename(columns={close_col: "close"})
            df.index.name = "date"
            df = df.reset_index()
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            return df.head(curDate)
        except Exception as e:
            logger.error(f"Failed to parse crypto data: {e}")
            return pd.DataFrame()

    @staticmethod
    @st.cache_data(ttl="15day", show_spinner=True)
    def get_federal_rate(interval="monthly"):
        av_key = _get_av_key()
        if not av_key:
            return pd.DataFrame()
        url = (
            "https://www.alphavantage.co/query?"
            f"function=FEDERAL_FUNDS_RATE&interval={interval}&apikey={av_key}"
        )
        response = EconomicIndicators._rate_limited_request(url)
        try:
            data = response.get("data")
            if not data:
                return pd.DataFrame()
            df = pd.DataFrame(data)
            df["value"] = pd.to_numeric(df.get("value"), errors="coerce")
            return df.head(60)
        except Exception as e:
            logger.error(f"Failed to parse federal rate data: {e}")
            return pd.DataFrame()

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_commodities(commodity, interval="daily", curDate=60):
        av_key = _get_av_key()
        if not av_key:
            return pd.DataFrame()
        url = (
            "https://www.alphavantage.co/query?"
            f"function={commodity.upper()}&interval={interval}&apikey={av_key}"
        )
        response = EconomicIndicators._rate_limited_request(url)
        try:
            data = response.get("data")
            if not data:
                return pd.DataFrame()
            df = pd.DataFrame(data)
            df["value"] = pd.to_numeric(df.get("value"), errors="coerce")
            return df.head(curDate)
        except Exception as e:
            logger.error(f"Failed to parse commodities data: {e}")
            return pd.DataFrame()

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_gold_silver_history(symbol="XAU", interval="daily", curDate=60):
        av_key = _get_av_key()
        if not av_key:
            return pd.DataFrame()
        url = (
            "https://www.alphavantage.co/query?"
            f"function=GOLD_SILVER_HISTORY&symbol={symbol}&interval={interval}"
            f"&apikey={av_key}"
        )
        response = EconomicIndicators._rate_limited_request(url)
        try:
            data = response.get("data") or []
            if isinstance(data, list) and data:
                df = pd.DataFrame(data)
            else:
                ts_key = next((k for k in response.keys() if "Time Series" in k), None)
                if not ts_key:
                    return pd.DataFrame()
                df = pd.DataFrame(response[ts_key]).T
                df.index.name = "date"
                df = df.reset_index()
            if df.empty:
                return df
            if "value" not in df.columns:
                for col in ["price", "close", "4. close"]:
                    if col in df.columns:
                        df = df.rename(columns={col: "value"})
                        break
            df["value"] = pd.to_numeric(df.get("value"), errors="coerce")
            return df.head(curDate)
        except Exception as e:
            logger.error(f"Failed to parse gold/silver data: {e}")
            return pd.DataFrame()

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_cn_cpi(limit=120):
        pro = ts.pro_api(get_tushare_token())
        return pro.cn_cpi(limit=limit)

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_cn_ppi(limit=120):
        pro = ts.pro_api(get_tushare_token())
        return pro.cn_ppi(limit=limit)

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_cn_money_supply(limit=120):
        pro = ts.pro_api(get_tushare_token())
        return pro.cn_m(limit=limit)

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_cn_soci(limit=120):
        pro = ts.pro_api(get_tushare_token())
        try:
            return pro.cn_soci(limit=limit)
        except Exception:
            return pro.query("cn_soci", limit=limit)

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_cn_pmi(limit=120):
        pro = ts.pro_api(get_tushare_token())
        return pro.cn_pmi(limit=limit)
