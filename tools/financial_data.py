"""
金融数据模块
包含股票数据、经济指标、汇率、债券、商品等金融数据获取功能
"""

import requests
import streamlit as st
import pandas as pd
import tushare as ts


class EconomicIndicators:
    """经济指标类"""
    
    @staticmethod
    @st.cache_data(ttl="5m", show_spinner=True)
    def get_exchangerates_realtime(from_currency='usd', to_currency='cny'):
        """获取实时汇率"""
        av_key = st.secrets['alpha_vantage_key']
        url = f'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={from_currency}&to_currency={to_currency}&apikey={av_key}'
        response = requests.get(url).json()
        rate = response['Realtime Currency Exchange Rate']
        return rate
    
    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_exchangerates_daily(from_currency='usd', to_currency='cny', curDate=60):
        """获取汇率日线数据"""
        av_key = st.secrets['alpha_vantage_key']
        url = f'https://www.alphavantage.co/query?function=FX_DAILY&from_symbol={from_currency}&to_symbol={to_currency}&apikey={av_key}'
        response = requests.get(url).json()
        time_series_data = response['Time Series FX (Daily)']
        df = pd.DataFrame(time_series_data).transpose()[['4. close']]
        return df.head(curDate)

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_treasury_yield(maturity='10year', interval='daily', curDate=3):
        """获取美国国债收益率"""
        av_key = st.secrets['alpha_vantage_key']
        url = f'https://www.alphavantage.co/query?function=TREASURY_YIELD&interval={interval}&maturity={maturity}&apikey={av_key}'
        response = requests.get(url).json()
        df = pd.DataFrame(response['data'])
        return df.head(curDate)

    @staticmethod
    @st.cache_data(ttl="15m", show_spinner=True)
    def get_equity_daily(symbol, outputsize="compact", curDate=60):
        """Get US equity/ETF daily series."""
        av_key = st.secrets['alpha_vantage_key']
        url = (
            "https://www.alphavantage.co/query?"
            f"function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize={outputsize}&apikey={av_key}"
        )
        response = requests.get(url).json()
        ts_key = next((k for k in response.keys() if "Time Series" in k), None)
        if not ts_key:
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
        return df.head(curDate)

    @staticmethod
    @st.cache_data(ttl="15m", show_spinner=True)
    def get_crypto_daily(symbol="BTC", market="USD", curDate=60):
        """Get digital currency daily series."""
        av_key = st.secrets['alpha_vantage_key']
        url = (
            "https://www.alphavantage.co/query?"
            f"function=DIGITAL_CURRENCY_DAILY&symbol={symbol}&market={market}&apikey={av_key}"
        )
        response = requests.get(url).json()
        ts_key = next((k for k in response.keys() if "Time Series" in k), None)
        if not ts_key:
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
        return df.head(curDate)

    @staticmethod
    @st.cache_data(ttl="15day", show_spinner=True)
    def get_federal_rate(interval='monthly'):
        """获取美联储利率"""
        av_key = st.secrets['alpha_vantage_key']
        url = f'https://www.alphavantage.co/query?function=FEDERAL_FUNDS_RATE&interval={interval}&apikey={av_key}'
        response = requests.get(url).json()
        df = pd.DataFrame(response['data'])
        return df.head(60)

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_commodities(commodity, interval="daily", curDate=60):
        """获取商品数据（原油WTI, 天然气NATURAL_GAS等）"""
        av_key = st.secrets['alpha_vantage_key']
        url = f"https://www.alphavantage.co/query?function={commodity.upper()}&interval={interval}&apikey={av_key}"
        response = requests.get(url).json()
        df = pd.DataFrame(response['data'])
        return df.head(curDate)
    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_gold_silver_history(symbol="XAU", interval="daily", curDate=60):
        """Get gold/silver history from Alpha Vantage."""
        av_key = st.secrets['alpha_vantage_key']
        url = (
            "https://www.alphavantage.co/query?"
            f"function=GOLD_SILVER_HISTORY&symbol={symbol}&interval={interval}&apikey={av_key}"
        )
        response = requests.get(url).json()
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
        return df.head(curDate)


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
            return pro.query('cn_soci', limit=limit)

    @staticmethod
    @st.cache_data(ttl="0.5day", show_spinner=True)
    def get_cn_pmi(limit=120):
        """获取中国 PMI 数据"""
        pro = ts.pro_api(st.secrets.get("tushare_token", ""))
        return pro.cn_pmi(limit=limit)


@st.cache_data(ttl="1h", show_spinner=True)
def get_fed_rate_cut_probability():
    """
    获取 Polymarket 上美联储下一次降息概率（独立方法）
    逻辑参考 py-clob-client 的 CLOB 示例，直接使用 HTTP 请求实现。
    """
    try:
        url = "https://clob.polymarket.com/markets"
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        }

        resp = requests.get(
            url,
            headers=headers,
            params={"active": "true", "limit": 200},
            timeout=10,
        )
        if resp.status_code != 200:
            return None

        markets = resp.json()

        fed_markets = []
        for m in markets:
            q = (m.get("question") or "").lower()
            title = (m.get("title") or "").lower()
            text = f"{q} {title}"
            if ("fed" in text or "federal reserve" in text) and (
                "rate cut" in text
                or "cut rates" in text
                or "lower rates" in text
                or "reduce rates" in text
            ):
                fed_markets.append(m)

        if not fed_markets:
            return None

        market = fed_markets[0]

        # 优先使用 outcomes 中 yes 的价格
        outcomes = market.get("outcomes") or []
        for outcome in outcomes:
            name = (outcome.get("name") or outcome.get("outcome") or "").lower()
            if "yes" in name:
                price = float(outcome.get("price") or 0)
                if price > 0:
                    return {
                        "probability": price * 100,
                        "question": market.get("question") or market.get("title", ""),
                        "market_url": f"https://polymarket.com/event/{market.get('slug', market.get('id', ''))}",
                    }

        # 兼容：若有 price 字段，则直接使用
        if "price" in market:
            price = float(market.get("price") or 0)
            if price > 0:
                return {
                    "probability": price * 100,
                    "question": market.get("question") or market.get("title", ""),
                    "market_url": f"https://polymarket.com/event/{market.get('slug', market.get('id', ''))}",
                }

        # 备用：根据 conditionId 读 orderbook
        condition_id = market.get("conditionId") or market.get("id")
        if condition_id:
            try:
                ob_url = f"https://clob.polymarket.com/orderbook/{condition_id}"
                ob_resp = requests.get(ob_url, headers=headers, timeout=10)
                if ob_resp.status_code == 200:
                    ob = ob_resp.json()
                    yes = ob.get("yes")
                    if isinstance(yes, dict):
                        bids = yes.get("bids") or []
                        if bids:
                            top = bids[0]
                            price = (
                                float(top.get("price"))
                                if isinstance(top, dict)
                                else float(top)
                            )
                            if price > 0:
                                return {
                                    "probability": price * 100,
                                    "question": market.get("question")
                                    or market.get("title", ""),
                                    "market_url": f"https://polymarket.com/event/{market.get('slug', market.get('id', ''))}",
                                }
            except Exception:
                pass

        return None
    except Exception:
        return None
