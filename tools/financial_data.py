"""
金融数据模块
包含股票数据、经济指标、汇率、债券、商品等金融数据获取功能
"""

import os
import time
import logging
import requests
import streamlit as st
import pandas as pd
import tushare as ts


logger = logging.getLogger(__name__)


def _get_av_key():
    """获取 Alpha Vantage API Key，支持 secrets 和环境变量"""
    try:
        return st.secrets.get("alpha_vantage_key") or os.environ.get(
            "ALPHAVANTAGE_API_KEY"
        )
    except Exception:
        return os.environ.get("ALPHAVANTAGE_API_KEY")


class EconomicIndicators:
    """经济指标类"""

    _last_request_time = 0  # 用于限流

    @classmethod
    def _rate_limited_request(cls, url, max_retries=3):
        """带限流的请求，Alpha Vantage 免费版限制每分钟5次"""
        for attempt in range(max_retries):
            try:
                # 确保两次请求间隔至少 12 秒（每分钟5次 = 每12秒1次）
                elapsed = time.time() - cls._last_request_time
                if elapsed < 12:
                    time.sleep(12 - elapsed)

                response = requests.get(url, timeout=30)
                cls._last_request_time = time.time()

                if response.status_code == 200:
                    data = response.json()
                    # 检查 API 限流信息
                    if "Note" in data or "Information" in data:
                        logger.warning(f"Alpha Vantage API limit: {data}")
                        if attempt < max_retries - 1:
                            time.sleep(15)
                            continue
                    return data
                else:
                    logger.error(f"HTTP {response.status_code}: {response.text}")
                    return {}
            except Exception as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
        return {}

    @staticmethod
    @st.cache_data(ttl="5m", show_spinner=True)
    def get_exchangerates_realtime(from_currency="usd", to_currency="cny"):
        """获取实时汇率"""
        av_key = _get_av_key()
        if not av_key:
            logger.error("Alpha Vantage API key not found")
            return None
        url = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={from_currency}&to_currency={to_currency}&apikey={av_key}"
        response = EconomicIndicators._rate_limited_request(url)
        try:
            return response.get("Realtime Currency Exchange Rate")
        except Exception as e:
            logger.error(f"Failed to parse exchange rate: {e}")
            return None

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_exchangerates_daily(from_currency="usd", to_currency="cny", curDate=60):
        """获取汇率日线数据"""
        av_key = _get_av_key()
        if not av_key:
            logger.error("Alpha Vantage API key not found")
            return pd.DataFrame()
        url = f"https://www.alphavantage.co/query?function=FX_DAILY&from_symbol={from_currency}&to_symbol={to_currency}&apikey={av_key}"
        response = EconomicIndicators._rate_limited_request(url)
        try:
            time_series_data = response.get("Time Series FX (Daily)")
            if not time_series_data:
                logger.warning(f"No FX data in response: {response.keys()}")
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
        """获取美国国债收益率"""
        av_key = _get_av_key()
        if not av_key:
            logger.error("Alpha Vantage API key not found")
            return pd.DataFrame()
        url = f"https://www.alphavantage.co/query?function=TREASURY_YIELD&interval={interval}&maturity={maturity}&apikey={av_key}"
        response = EconomicIndicators._rate_limited_request(url)
        try:
            data = response.get("data")
            if not data:
                logger.warning(f"No treasury data in response")
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
        """Get US equity/ETF daily series."""
        av_key = _get_av_key()
        if not av_key:
            logger.error("Alpha Vantage API key not found")
            return pd.DataFrame()
        url = (
            "https://www.alphavantage.co/query?"
            f"function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize={outputsize}&apikey={av_key}"
        )
        response = EconomicIndicators._rate_limited_request(url)
        try:
            ts_key = next((k for k in response.keys() if "Time Series" in k), None)
            if not ts_key:
                logger.warning(f"No time series data for {symbol}")
                return pd.DataFrame()
            df = pd.DataFrame(response[ts_key]).T
            close_col = None
            for col in df.columns:
                if col.strip().startswith("4. close"):
                    close_col = col
                    break
            if close_col is None:
                for col in df.columns:
                    if "close" in col.lower():
                        close_col = col
                        break
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
        """Get digital currency daily series."""
        av_key = _get_av_key()
        if not av_key:
            logger.error("Alpha Vantage API key not found")
            return pd.DataFrame()
        url = (
            "https://www.alphavantage.co/query?"
            f"function=DIGITAL_CURRENCY_DAILY&symbol={symbol}&market={market}&apikey={av_key}"
        )
        response = EconomicIndicators._rate_limited_request(url)
        try:
            ts_key = next((k for k in response.keys() if "Time Series" in k), None)
            if not ts_key:
                logger.warning(f"No crypto time series for {symbol}")
                return pd.DataFrame()
            df = pd.DataFrame(response[ts_key]).T
            market_upper = str(market).upper()
            close_col = None
            for col in df.columns:
                if "close" in col.lower() and f"({market_upper})" in col:
                    close_col = col
                    break
            if close_col is None:
                for col in df.columns:
                    if "close" in col.lower():
                        close_col = col
                        break
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
        """获取美联储利率"""
        av_key = _get_av_key()
        if not av_key:
            logger.error("Alpha Vantage API key not found")
            return pd.DataFrame()
        url = f"https://www.alphavantage.co/query?function=FEDERAL_FUNDS_RATE&interval={interval}&apikey={av_key}"
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
        """获取商品数据（原油WTI, 天然气NATURAL_GAS等）"""
        av_key = _get_av_key()
        if not av_key:
            logger.error("Alpha Vantage API key not found")
            return pd.DataFrame()
        url = f"https://www.alphavantage.co/query?function={commodity.upper()}&interval={interval}&apikey={av_key}"
        response = EconomicIndicators._rate_limited_request(url)
        try:
            data = response.get("data")
            if not data:
                logger.warning(f"No commodities data for {commodity}")
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
        """Get gold/silver history from Alpha Vantage."""
        av_key = _get_av_key()
        if not av_key:
            logger.error("Alpha Vantage API key not found")
            return pd.DataFrame()
        url = (
            "https://www.alphavantage.co/query?"
            f"function=GOLD_SILVER_HISTORY&symbol={symbol}&interval={interval}&apikey={av_key}"
        )
        response = EconomicIndicators._rate_limited_request(url)
        try:
            data = response.get("data") or []
            if isinstance(data, list) and data:
                df = pd.DataFrame(data)
            else:
                ts_key = next((k for k in response.keys() if "Time Series" in k), None)
                if not ts_key:
                    logger.warning(f"No gold/silver data for {symbol}")
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
        """获取中国CPI数据"""
        pro = ts.pro_api(st.secrets.get("tushare_token", ""))
        return pro.cn_cpi(limit=limit)

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_cn_ppi(limit=120):
        """获取中国PPI数据"""
        pro = ts.pro_api(st.secrets.get("tushare_token", ""))
        return pro.cn_ppi(limit=limit)

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_cn_money_supply(limit=120):
        """获取中国M0/M1/M2月度数据"""
        pro = ts.pro_api(st.secrets.get("tushare_token", ""))
        return pro.cn_m(limit=limit)

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_cn_soci(limit=120):
        """获取社融月度增量数据"""
        pro = ts.pro_api(st.secrets.get("tushare_token", ""))
        try:
            return pro.cn_soci(limit=limit)
        except Exception:
            return pro.query("cn_soci", limit=limit)

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_cn_pmi(limit=120):
        """获取中国 PMI 数据"""
        pro = ts.pro_api(st.secrets.get("tushare_token", ""))
        return pro.cn_pmi(limit=limit)
