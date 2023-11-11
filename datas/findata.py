import requests
import json
import streamlit as st
import pandas as pd


av_key = st.secrets['alpha_vantage_key']

class StockRetriever:
    @staticmethod
    def get_stocks_followed(xueqiu_uid):
        headers = {
            'authority': 'stock.xueqiu.com',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
            'cookie': 'xq_is_login=1; u=4873962436; device_id=bed3fa501783969d3e6b52bafa603ddf; s=be1vwesb9e; bid=dae282ad1417f3f26c00a2c349cca9dd_lbt7ys4t; ph_phc_bxhp1pdfgYElECvQ1fjNxOtpl8RZiGCtTMCOwDTWBaL_posthog=%7B%22distinct_id%22%3A%223713fb30a9838dd19e32a9b3f8edd5c16aa4749bdfb08a98b5dd87b7e961%22%2C%22%24device_id%22%3A%22186da47188fbd-0e5685b1dabb57-1c3a645d-1fa400-186da4718901606%22%2C%22%24user_id%22%3A%223713fb30a9838dd19e32a9b3f8edd5c16aa4749bdfb08a98b5dd87b7e961%22%2C%22%24referrer%22%3A%22https%3A%2F%2Fxueqiu.com%2FS%2FSZ002236%22%2C%22%24referring_domain%22%3A%22xueqiu.com%22%2C%22%24sesid%22%3A%5B1678699338184%2C%22186da4719c87dd-0fc53581fa73a3-1c3a645d-1fa400-186da4719c91893%22%2C1678699338184%5D%2C%22%24session_recording_enabled_server_side%22%3Afalse%2C%22%24active_feature_flags%22%3A%5B%5D%2C%22%24enabled_feature_flags%22%3A%7B%7D%7D; snbim_minify=true; Hm_lvt_1db88642e346389874251b5a1eded6e3=1685668457; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1686286093; xq_a_token=7cf1b8a4fb0ce4d7accd33c4c328ba9e64f31eb3; xqat=7cf1b8a4fb0ce4d7accd33c4c328ba9e64f31eb3; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjQ4NzM5NjI0MzYsImlzcyI6InVjIiwiZXhwIjoxNjg5NjQ2MjAwLCJjdG0iOjE2ODcwNTQyMDAwNDAsImNpZCI6ImQ5ZDBuNEFadXAifQ.ni2EH0Zk6cfjmIrkkiAo7zvKYlfLEuV4p_pdICkHxjx3NTsgs980Pyb5xpzfnm5If77aorZmu9X45F1WAsEtbyJY9FO5ArEEPb-C3XMlKs2u2unP_ynL4eL9Rhcz6XHtdlD1sNRMezH8hRvfOu9Tdhn-lf9xWlexW0XmiW801tpRuDSawcAF8UK9TS5QgC-894Ps9qye1zb3WJBOKkKjE_8Qfz3qvLkFFa1_WML2T70f9G8sTu2hC4PUBdtqO1k_5JCFgHNDbgZpfPw2ZSAEmvIFkKTVEgXX9ioOF94Bv4UoGqdX__FNJsakJg61KI_cjn2r5xsgCld_Rh-t1ujgeg; xq_r_token=ea75a7348e90752c776a04848788c2acb1af0840',
            'origin': 'https://xueqiu.com',
            'referer': 'https://xueqiu.com/u/4873962436',
            'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        }
        url = f'https://stock.xueqiu.com/v5/stock/portfolio/stock/list.json?pid=-6&category=1&size=1000&uid={xueqiu_uid}'
        response = requests.get(url, headers=headers)
        stocks = response.json()["data"]["stocks"]
        return stocks

    @staticmethod
    @st.cache_data(ttl='0.5day',show_spinner=True)
    def get_stock_endpoint(symbol):
        '''
        symbol：美股代码 上证代码.SHH 深圳.SHZ
        '''
        if symbol.startswith('60'): symbol = symbol + '.SHH'
        if symbol.startswith('00'): symbol = symbol + '.SHZ'
        url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={av_key}'
        response = requests.get(url).json()

        time_series_data = response['Global Quote']
        df = pd.DataFrame.from_dict(time_series_data, orient='index')
        return df

    @staticmethod
    #@st.cache_data(ttl='0.5day')
    def get_stock_daily(symbol):
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&apikey={av_key}'
        response = requests.get(url).json()

        time_series_data = response['Time Series (Daily)']
        df = pd.DataFrame(time_series_data).transpose()[['4. close']]
        df.columns = [symbol]
        return df


class EconomicIndicators:
    # 实时汇率
    @staticmethod
    @st.cache_data(ttl="5m",show_spinner=True)
    def get_exchangerates_realtime(from_currency='usd',to_currency='cny')->dict:
        url = f'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={from_currency}&to_currency={to_currency}&apikey={av_key}'
        response = requests.get(url).json()
        rate = response['Realtime Currency Exchange Rate']
        return rate
    
    # 汇率日线
    @staticmethod
    @st.cache_data(ttl="0.5day",show_spinner=True)
    def get_exchangerates_daily(from_currency='usd',to_currency='cny',curDate=60)->pd.DataFrame:
        url = f'https://www.alphavantage.co/query?function=FX_DAILY&from_symbol={from_currency}&to_symbol={to_currency}&apikey={av_key}'
        response = requests.get(url).json()
        time_series_data = response['Time Series FX (Daily)']
        df = pd.DataFrame(time_series_data).transpose()[['4. close']]
        return df.head(curDate)

    # 美国债
    @staticmethod
    @st.cache_data(ttl="0.5day",show_spinner=True)
    def get_treasury_yield(maturity='10year', interval='daily',curDate=3)->pd.DataFrame:
        url = f'https://www.alphavantage.co/query?function=TREASURY_YIELD&interval={interval}&maturity={maturity}&apikey={av_key}'
        response = requests.get(url).json()
        df = pd.DataFrame(response['data'])
        return df.head(curDate)

    # 美利率
    @staticmethod
    @st.cache_data(ttl="15day",show_spinner=True)
    def get_federal_rate(interval='monthly')->pd.DataFrame:
        url = f'https://www.alphavantage.co/query?function=FEDERAL_FUNDS_RATE&interval={interval}&apikey={av_key}'
        response = requests.get(url).json()
        df = pd.DataFrame(response['data'])
        return df.head(60)


    # 原油WTI, 天然气NATURAL_GAS
    @staticmethod
    @st.cache_data(ttl="0.5day",show_spinner=True)
    def get_commodities(commodity,interval="daily", curDate=60)->pd.DataFrame:
        url = f"https://www.alphavantage.co/query?function={commodity.upper()}&interval={interval}&apikey={av_key}"
        response = requests.get(url).json()
        df = pd.DataFrame(response['data'])
        return df.head(curDate)




