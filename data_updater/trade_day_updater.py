#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
交易日数据更新器
- 交易日判断
- 外围数据获取
- 大盘数据获取
- 全市场股票指标获取
- 数据特征计算
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

import pandas as pd
import numpy as np
import tushare as ts
import akshare as ak
import requests


def safe_json_dumps(obj: Any) -> str:
    """安全地将对象转为JSON字符串，处理NaN/Infinity/numpy类型等"""
    def json_encoder(o):
        if isinstance(o, (datetime, pd.Timestamp)):
            return o.isoformat()
        if isinstance(o, np.bool_):
            return bool(o)
        if isinstance(o, (np.integer, np.floating)):
            if np.isnan(o) or np.isinf(o):
                return None
            return float(o) if isinstance(o, np.floating) else int(o)
        if isinstance(o, np.ndarray):
            return o.tolist()
        if pd.isna(o):
            return None
        return str(o)
    
    # 清理对象中的特殊值
    def clean_value(v):
        if isinstance(v, dict):
            return {k: clean_value(vv) for k, vv in v.items()}
        elif isinstance(v, (list, tuple)):
            return [clean_value(vv) for vv in v]
        elif isinstance(v, np.bool_):
            return bool(v)
        elif isinstance(v, (np.integer, np.floating)):
            if np.isnan(v) or np.isinf(v):
                return None
            return float(v) if isinstance(v, np.floating) else int(v)
        elif pd.isna(v):
            return None
        return v
    
    cleaned_obj = clean_value(obj)
    return json.dumps(cleaned_obj, default=json_encoder, ensure_ascii=False)

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_manager import DBManager, get_db
from tools.kline_patterns import KLinePatternRecognizer, PatternResult

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TradeDayUpdater:
    """交易日数据更新器"""
    
    def __init__(self):
        self.db = DBManager()
        self.ts_pro = None
        self._init_tushare()
        self.pattern_recognizer = KLinePatternRecognizer()
        
    def _init_tushare(self):
        """初始化Tushare API"""
        token = os.getenv('TUSHARE_TOKEN')
        if not token:
            # 尝试从streamlit secrets读取
            try:
                import streamlit as st
                token = st.secrets.get("tushare_token")
            except:
                pass
        
        if token:
            ts.set_token(token)
            self.ts_pro = ts.pro_api()
            logger.info("✓ Tushare API 初始化成功")
        else:
            logger.warning("⚠ Tushare Token 未配置")
    

    def _normalize_trade_date(self, date: str = None) -> str:
        """Normalize date to YYYYMMDD."""
        if not date:
            return datetime.now().strftime('%Y%m%d')
        raw = str(date).strip().replace('-', '')
        if len(raw) != 8 or not raw.isdigit():
            raise ValueError(f"invalid trade date: {date}")
        datetime.strptime(raw, '%Y%m%d')
        return raw

    def _date_to_iso(self, date: str) -> str:
        date = self._normalize_trade_date(date)
        return f"{date[:4]}-{date[4:6]}-{date[6:]}"

    def _alphavantage_get_json(self, params: Dict[str, str]) -> Optional[Dict]:
        api_key = os.getenv('ALPHAVANTAGE_API_KEY')
        if not api_key:
            try:
                import streamlit as st
                api_key = (
                    st.secrets.get('ALPHAVANTAGE_API_KEY')
                    or st.secrets.get('alphavantage_api_key')
                    or st.secrets.get('alpha_vantage_api_key')
                    or st.secrets.get('alpha_vantage_key')
                )
            except Exception:
                api_key = None
        if not api_key:
            logger.warning('ALPHAVANTAGE_API_KEY is not configured')
            return None

        merged = dict(params)
        merged['apikey'] = api_key
        try:
            response = requests.get('https://www.alphavantage.co/query', params=merged, timeout=30)
            response.raise_for_status()
            data = response.json()
            if 'Error Message' in data:
                logger.error(f"AlphaVantage error: {data['Error Message']}")
                return None
            if 'Note' in data:
                logger.warning(f"AlphaVantage note: {data['Note']}")
            return data
        except Exception as e:
            logger.error(f'AlphaVantage request failed: {e}')
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None

    def _fetch_fx_daily(self, from_symbol: str, to_symbol: str, date: str) -> Optional[Dict]:
        payload = self._alphavantage_get_json({
            'function': 'FX_DAILY',
            'from_symbol': from_symbol,
            'to_symbol': to_symbol,
            'outputsize': 'full',
        })
        if not payload:
            return None

        series = payload.get('Time Series FX (Daily)')
        if not isinstance(series, dict):
            return None

        date_iso = self._date_to_iso(date)
        target_date = None
        if date_iso in series:
            target_date = date_iso
        else:
            for d in sorted(series.keys(), reverse=True):
                if d <= date_iso:
                    target_date = d
                    break
        if not target_date and series:
            # AlphaVantage free tier may only return compact history.
            # Fall back to the oldest available row.
            target_date = sorted(series.keys())[0]
        if not target_date:
            return None

        today = series.get(target_date, {})
        close_price = self._safe_float(today.get('4. close'))
        open_price = self._safe_float(today.get('1. open'))
        high_price = self._safe_float(today.get('2. high'))
        low_price = self._safe_float(today.get('3. low'))

        prev_close = None
        for d in sorted(series.keys(), reverse=True):
            if d < target_date:
                prev_close = self._safe_float(series[d].get('4. close'))
                break

        pct_change = None
        if close_price is not None and prev_close and prev_close != 0:
            pct_change = (close_price - prev_close) / prev_close * 100

        return {
            'open_price': open_price,
            'high_price': high_price,
            'low_price': low_price,
            'close_price': close_price,
            'pct_change': pct_change,
            'raw_payload': safe_json_dumps(today),
        }

    def _fetch_currency_exchange_rate(self, from_currency: str, to_currency: str) -> Optional[Dict]:
        payload = self._alphavantage_get_json({
            'function': 'CURRENCY_EXCHANGE_RATE',
            'from_currency': from_currency,
            'to_currency': to_currency,
        })
        if not payload:
            return None

        data = payload.get('Realtime Currency Exchange Rate')
        if not isinstance(data, dict):
            return None

        rate = self._safe_float(data.get('5. Exchange Rate'))
        return {
            'close_price': rate,
            'pct_change': None,
            'raw_payload': safe_json_dumps(data),
        }

    def _fetch_treasury_yield(self, date: str, maturity: str = '10year') -> Optional[Dict]:
        payload = self._alphavantage_get_json({
            'function': 'TREASURY_YIELD',
            'interval': 'daily',
            'maturity': maturity,
        })
        if not payload:
            return None

        data = payload.get('data')
        if not isinstance(data, list) or not data:
            return None

        date_iso = self._date_to_iso(date)
        today_row = None
        prev_row = None

        for row in data:
            d = row.get('date')
            if d == date_iso:
                today_row = row
                break
        if not today_row:
            for row in data:
                d = row.get('date')
                if d and d <= date_iso:
                    today_row = row
                    break
        if not today_row and data:
            today_row = data[-1]

        for row in data:
            d = row.get('date')
            if d and d < date_iso:
                prev_row = row
                break

        close_price = self._safe_float(today_row.get('value'))
        prev_close = self._safe_float(prev_row.get('value')) if prev_row else None
        pct_change = None
        if close_price is not None and prev_close and prev_close != 0:
            pct_change = (close_price - prev_close) / prev_close * 100

        return {
            'close_price': close_price,
            'pct_change': pct_change,
            'raw_payload': safe_json_dumps(today_row),
        }

    def _fetch_gold_silver_history(self, symbol: str, date: str) -> Optional[Dict]:
        payload = self._alphavantage_get_json({
            'function': 'GOLD_SILVER_HISTORY',
            'symbol': symbol,
            'interval': 'daily',
        })
        if not payload:
            return None

        data = payload.get('data')
        if not isinstance(data, list) or not data:
            return None

        date_iso = self._date_to_iso(date)
        today_row = None
        for row in data:
            d = row.get('date')
            if d == date_iso:
                today_row = row
                break
        if not today_row:
            for row in data:
                d = row.get('date')
                if d and d <= date_iso:
                    today_row = row
                    break
        if not today_row:
            today_row = data[-1]

        value = self._safe_float(today_row.get('value') or today_row.get('price') or today_row.get('close'))
        return {
            'close_price': value,
            'pct_change': None,
            'raw_payload': safe_json_dumps(today_row),
        }

    def _fetch_equity_daily(self, symbol: str, date: str) -> Optional[Dict]:
        payload = self._alphavantage_get_json({
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'outputsize': 'full',
        })
        if not payload:
            return None

        series = payload.get('Time Series (Daily)')
        if not isinstance(series, dict):
            return None

        date_iso = self._date_to_iso(date)
        target_date = None
        if date_iso in series:
            target_date = date_iso
        else:
            for d in sorted(series.keys(), reverse=True):
                if d <= date_iso:
                    target_date = d
                    break
        if not target_date:
            return None

        today = series.get(target_date, {})
        close_price = self._safe_float(today.get('4. close'))
        prev_close = None
        for d in sorted(series.keys(), reverse=True):
            if d < target_date:
                prev_close = self._safe_float(series[d].get('4. close'))
                break
        pct_change = None
        if close_price is not None and prev_close and prev_close != 0:
            pct_change = (close_price - prev_close) / prev_close * 100

        return {
            'open_price': self._safe_float(today.get('1. open')),
            'high_price': self._safe_float(today.get('2. high')),
            'low_price': self._safe_float(today.get('3. low')),
            'close_price': close_price,
            'pct_change': pct_change,
            'volume': int(float(today.get('5. volume', 0))) if today.get('5. volume') is not None else None,
            'raw_payload': safe_json_dumps(today),
        }

    def update_stock_master(self) -> bool:
        """Refresh stock master using Tushare stock_basic."""
        if not self.ts_pro:
            return False

        try:
            all_rows = []
            for status in ('L', 'D', 'P'):
                df = self.ts_pro.stock_basic(
                    exchange='',
                    list_status=status,
                    fields='ts_code,symbol,name,market,exchange,list_date,industry,list_status'
                )
                if df is not None and not df.empty:
                    all_rows.append(df)

            if not all_rows:
                logger.warning('stock_basic returned empty')
                return False

            full = pd.concat(all_rows, ignore_index=True).drop_duplicates(subset=['ts_code'])

            records = []
            for _, row in full.iterrows():
                name = str(row.get('name') or '')
                symbol = str(row.get('symbol') or '')
                exchange = str(row.get('exchange') or '')
                list_status = str(row.get('list_status') or 'L')
                market = str(row.get('market') or '')

                records.append({
                    'ts_code': row['ts_code'],
                    'symbol': symbol,
                    'name': name,
                    'market': market,
                    'exchange': exchange,
                    'is_st': 1 if 'ST' in name.upper() else 0,
                    'is_delist': 1 if list_status == 'D' or '?' in name else 0,
                    'is_bse': 1 if exchange == 'BSE' or symbol.startswith('8') or symbol.startswith('4') else 0,
                    'list_date': row.get('list_date') if pd.notna(row.get('list_date')) else None,
                    'industry': row.get('industry') if pd.notna(row.get('industry')) else None,
                })

            batch_size = 1000
            for i in range(0, len(records), batch_size):
                self.db.upsert_many('stock_master', records[i:i+batch_size], ['ts_code'])

            logger.info(f'stock_master updated: {len(records)}')
            return True
        except Exception as e:
            logger.error(f'update_stock_master failed: {e}')
            return False

    def _get_stock_meta_map(self) -> Dict[str, Dict[str, Any]]:
        """Get stock metadata map from stock_master."""
        meta = {}
        try:
            rows = self.db.query(
                "SELECT ts_code, name, is_st, is_delist, is_bse FROM stock_master"
            )
            for row in rows:
                meta[row['ts_code']] = row
        except Exception as e:
            logger.error(f'_get_stock_meta_map failed: {e}')
        return meta
    # ==================== 1. 交易日判断 ====================
    

    def check_trade_day(self, date: str = None) -> Tuple[bool, str]:
        """Check whether date is a trade day."""
        date = self._normalize_trade_date(date)

        result = self.db.query_one(
            "SELECT is_open FROM trade_calendar WHERE cal_date = %s",
            (date,)
        )
        if result:
            is_open = int(result['is_open']) == 1
            return is_open, 'trade day (local cache)' if is_open else 'non-trade day (local cache)'

        if self.ts_pro:
            try:
                df = self.ts_pro.trade_cal(exchange='SSE', start_date=date, end_date=date)
                if df is not None and not df.empty:
                    self._save_trade_calendar(df)
                    is_open = int(df.iloc[0]['is_open']) == 1
                    return is_open, 'trade day (tushare)' if is_open else 'non-trade day (tushare)'
            except Exception as e:
                logger.error(f'fetch trade calendar failed: {e}')

        dt = datetime.strptime(date, '%Y%m%d')
        if dt.weekday() >= 5:
            return False, 'weekend fallback'
        return True, 'weekday fallback'


    def _save_trade_calendar(self, df: pd.DataFrame):
        """Save trade calendar to DB."""
        records = []
        for _, row in df.iterrows():
            cal_date = str(row.get('cal_date', '')).replace('-', '')
            pretrade_date = row.get('pretrade_date')
            pretrade_date = str(pretrade_date).replace('-', '') if pd.notna(pretrade_date) else None
            records.append({
                'cal_date': cal_date,
                'is_open': 1 if int(row.get('is_open', 0)) == 1 else 0,
                'pretrade_date': pretrade_date,
            })

        for record in records:
            self.db.upsert('trade_calendar', record, ['cal_date'])


    def update_external_assets(self, date: str) -> bool:
        """Update external assets using AlphaVantage."""
        date = self._normalize_trade_date(date)
        logger.info(f'updating external assets: {date}')

        assets = [
            {'code': 'BTCUSD', 'name': 'BTC/USD', 'func': self._fetch_btc_usd},
            {'code': 'XAUUSD', 'name': 'XAU/USD', 'func': self._fetch_xau_usd},
            {'code': 'USDCNY', 'name': 'USD/CNY', 'func': self._fetch_usdcny},
            {'code': 'US10Y', 'name': 'US 10Y Treasury Yield', 'func': self._fetch_us10y},
        ]

        success_count = 0
        for asset in assets:
            try:
                data = asset['func'](date)
                if not data:
                    # Fallback: carry forward the latest available row for this asset.
                    cached = self.db.query_one(
                        """
                        SELECT open_price, high_price, low_price, close_price, pct_change, volume, raw_payload
                        FROM external_asset_daily
                        WHERE asset_code = %s AND trade_date <= %s
                        ORDER BY trade_date DESC
                        LIMIT 1
                        """,
                        (asset['code'], date)
                    )
                    if cached:
                        data = {
                            'open_price': cached.get('open_price'),
                            'high_price': cached.get('high_price'),
                            'low_price': cached.get('low_price'),
                            'close_price': cached.get('close_price'),
                            'pct_change': cached.get('pct_change'),
                            'volume': cached.get('volume'),
                            'raw_payload': safe_json_dumps({
                                'source': 'carry_forward_cache',
                                'origin_payload': cached.get('raw_payload')
                            }),
                        }
                        logger.warning(f"external asset fallback used: {asset['code']}")
                    else:
                        logger.warning(f"external asset missing: {asset['code']}")
                        continue
                data['trade_date'] = date
                data['asset_code'] = asset['code']
                data['asset_name'] = asset['name']
                self.db.upsert('external_asset_daily', data, ['trade_date', 'asset_code'])
                success_count += 1
            except Exception as e:
                logger.error(f"external asset update failed {asset['code']}: {e}")

        logger.info(f'external assets updated: {success_count}/{len(assets)}')
        return success_count == len(assets)


    def _fetch_btc_usd(self, date: str) -> Optional[Dict]:
        payload = self._alphavantage_get_json({
            'function': 'DIGITAL_CURRENCY_DAILY',
            'symbol': 'BTC',
            'market': 'USD',
        })
        if not payload:
            return None

        series = payload.get('Time Series (Digital Currency Daily)')
        if not isinstance(series, dict):
            return None

        date_iso = self._date_to_iso(date)
        if date_iso not in series:
            return None

        today = series[date_iso]
        close_price = self._safe_float(today.get('4a. close (USD)') or today.get('4. close'))
        prev_close = None
        for d in sorted(series.keys(), reverse=True):
            if d < date_iso:
                prev_close = self._safe_float(series[d].get('4a. close (USD)') or series[d].get('4. close'))
                break

        pct_change = None
        if close_price is not None and prev_close and prev_close != 0:
            pct_change = (close_price - prev_close) / prev_close * 100

        return {
            'open_price': self._safe_float(today.get('1a. open (USD)') or today.get('1. open')),
            'high_price': self._safe_float(today.get('2a. high (USD)') or today.get('2. high')),
            'low_price': self._safe_float(today.get('3a. low (USD)') or today.get('3. low')),
            'close_price': close_price,
            'pct_change': pct_change,
            'volume': int(float(today.get('5. volume', 0))) if today.get('5. volume') is not None else None,
            'raw_payload': safe_json_dumps(today),
        }


    def _fetch_xau_usd(self, date: str) -> Optional[Dict]:
        # AlphaVantage XAU/USD should use GOLD_SILVER_HISTORY.
        # FX_DAILY and CURRENCY_EXCHANGE_RATE do not support XAU in this account tier.
        result = self._fetch_gold_silver_history('XAU', date)
        if result:
            return result
        return self._fetch_equity_daily('GLD', date)


    def _fetch_usdcny(self, date: str) -> Optional[Dict]:
        result = self._fetch_fx_daily('USD', 'CNY', date)
        if result:
            return result
        return self._fetch_currency_exchange_rate('USD', 'CNY')


    def _fetch_us10y(self, date: str) -> Optional[Dict]:
        return self._fetch_treasury_yield(date, maturity='10year')

    def update_index_data(self, date: str) -> bool:
        """
        更新三大指数日线数据
        - 上证指数 000001.SH
        - 创业板指 399006.SZ
        - 科创板指 000688.SH
        """
        logger.info(f"开始更新三大指数数据: {date}")
        
        indices = [
            {'code': '000001.SH', 'name': '上证指数'},
            {'code': '399006.SZ', 'name': '创业板指'},
            {'code': '000688.SH', 'name': '科创板指'},
        ]
        
        success_count = 0
        for idx in indices:
            try:
                if self.ts_pro:
                    df = self.ts_pro.index_daily(ts_code=idx['code'], trade_date=date)
                    if not df.empty:
                        row = df.iloc[0]
                        data = {
                            'trade_date': date,
                            'ts_code': idx['code'],
                            'name': idx['name'],
                            'open_price': float(row.get('open', 0)),
                            'high_price': float(row.get('high', 0)),
                            'low_price': float(row.get('low', 0)),
                            'close_price': float(row.get('close', 0)),
                            'pre_close': float(row.get('pre_close', 0)),
                            'pct_change': float(row.get('pct_chg', 0)),
                            'volume': int(row.get('vol', 0)),
                            'amount': float(row.get('amount', 0)),
                            'raw_payload': safe_json_dumps(row.to_dict()),
                        }
                        self.db.upsert('index_daily', data, ['trade_date', 'ts_code'])
                        logger.info(f"  ✓ {idx['name']} 更新成功")
                        success_count += 1
            except Exception as e:
                logger.error(f"  ✗ {idx['name']} 更新失败: {e}")
        
        logger.info(f"三大指数更新完成: {success_count}/{len(indices)}")
        return success_count == len(indices)
    
    def update_market_activity(self, date: str) -> bool:
        """
        更新市场活跃度数据
        使用 Tushare daily 接口获取全市场成交额（沪深创合并口径）
        """
        logger.info(f"开始更新市场活跃度: {date}")

        try:
            # 1. 从 akshare 获取涨跌家数、涨停跌停数（这些字段没问题）
            up_count, down_count, zt_count, dt_count = 0, 0, 0, 0
            try:
                df_legu = ak.stock_market_activity_legu()
                if df_legu is not None and not df_legu.empty:
                    up_count = int(df_legu[df_legu['item'] == '上涨家数']['value'].values[0]) if '上涨家数' in df_legu['item'].values else 0
                    down_count = int(df_legu[df_legu['item'] == '下跌家数']['value'].values[0]) if '下跌家数' in df_legu['item'].values else 0
            except Exception as e:
                logger.warning(f"akshare 市场活跃度获取失败（将尝试 Tushare 兜底）: {e}")

            # 2. 从 akshare 获取涨停跌停数
            try:
                zt_df = ak.stock_zt_pool_em(date=date)
                zt_count = len(zt_df) if zt_df is not None else 0
            except:
                zt_count = 0

            try:
                dt_df = ak.stock_zt_pool_dtgc_em(date=date)
                dt_count = len(dt_df) if dt_df is not None else 0
            except:
                dt_count = 0

            # 3. 使用 Tushare daily 接口获取全市场成交额（沪深创合并）
            # 口径：沪市 + 深市 + 创业板，单位：千元 -> 转为元后 / 1e8 = 亿元
            total_amount = 0.0
            if self.ts_pro:
                try:
                    daily_df = self.ts_pro.daily(trade_date=date, fields="ts_code,trade_date,amount")
                    if daily_df is not None and not daily_df.empty and "amount" in daily_df.columns:
                        # amount 单位是千元，求和后转为亿元
                        total_amount_k = pd.to_numeric(daily_df["amount"], errors="coerce").sum()
                        total_amount = float(total_amount_k) * 1000.0 / 1e8  # 千元 -> 元 -> 亿元
                        logger.info(f"全市场成交额（沪深创合并）: {total_amount:.2f} 亿元")
                except Exception as e:
                    logger.error(f"Tushare daily 接口获取成交额失败: {e}")

            # 4. 如果 Tushare 获取失败，尝试从指数数据反推（兜底）
            if total_amount <= 0:
                try:
                    index_amounts = []
                    for idx_code in ["000001.SH", "399006.SZ", "399001.SZ"]:
                        idx_df = self.ts_pro.index_daily(ts_code=idx_code, trade_date=date, fields="ts_code,amount")
                        if idx_df is not None and not idx_df.empty and "amount" in idx_df.columns:
                            idx_amount = pd.to_numeric(idx_df["amount"], errors="coerce").sum()
                            index_amounts.append(float(idx_amount))
                    if index_amounts:
                        # 指数 amount 单位也是千元
                        total_amount = sum(index_amounts) * 1000.0 / 1e8
                        logger.info(f"全市场成交额（指数口径兜底）: {total_amount:.2f} 亿元")
                except Exception as e:
                    logger.error(f"指数成交额兜底获取失败: {e}")

            # 5. 计算活跃度指数 (0-100)
            total = up_count + down_count
            activity_index = (up_count / total * 100) if total > 0 else 50

            data = {
                'trade_date': date,
                'up_count': up_count,
                'down_count': down_count,
                'zt_count': zt_count,
                'dt_count': dt_count,
                'activity_index': activity_index,
                'total_amount': total_amount,  # 单位：亿元
                'raw_payload': safe_json_dumps({
                    'up_count': up_count,
                    'down_count': down_count,
                    'zt_count': zt_count,
                    'dt_count': dt_count,
                    'total_amount_yi': total_amount,
                    'source': 'tushare_daily' if total_amount > 0 else 'fallback',
                }),
            }

            self.db.upsert('market_activity_daily', data, ['trade_date'])
            logger.info(f"✓ 市场活跃度更新成功: 涨{up_count}/跌{down_count}/涨停{zt_count}/跌停{dt_count}/成交额{total_amount:.2f}亿")
            return True

        except Exception as e:
            logger.error(f"市场活跃度更新失败: {e}")
            import traceback
            traceback.print_exc()

        return False
    
    # ==================== 4. 全市场股票指标获取 ====================
    

    def update_stock_daily_basic(self, date: str) -> bool:
        """Update all-stock daily basic snapshot."""
        date = self._normalize_trade_date(date)
        logger.info(f'updating stock_daily_basic: {date}')

        if not self.ts_pro:
            logger.error('Tushare not initialized')
            return False

        try:
            self.update_stock_master()

            df_basic = self.ts_pro.daily_basic(trade_date=date)
            if df_basic is None or df_basic.empty:
                logger.warning('daily_basic empty')
                return False

            df_daily = self.ts_pro.daily(trade_date=date)
            if df_daily is not None and not df_daily.empty:
                df_daily = df_daily[['ts_code', 'pct_chg', 'amount', 'vol', 'close']]
                df = df_basic.merge(df_daily, on='ts_code', how='left')
            else:
                df = df_basic

            stock_meta = self._get_stock_meta_map()

            records = []
            for _, row in df.iterrows():
                ts_code = row['ts_code']
                symbol = ts_code.split('.')[0]
                meta = stock_meta.get(ts_code, {})
                name = meta.get('name') or ''

                records.append({
                    'trade_date': date,
                    'ts_code': ts_code,
                    'symbol': symbol,
                    'name': name,
                    'close_price': float(row.get('close', 0)) if pd.notna(row.get('close')) else None,
                    'pct_change': float(row.get('pct_chg', 0)) if pd.notna(row.get('pct_chg')) else None,
                    'turnover_rate': float(row.get('turnover_rate', 0)) if pd.notna(row.get('turnover_rate')) else None,
                    'turnover_rate_f': float(row.get('turnover_rate_f', 0)) if pd.notna(row.get('turnover_rate_f')) else None,
                    'volume_ratio': float(row.get('volume_ratio', 0)) if pd.notna(row.get('volume_ratio')) else None,
                    'pe_ttm': float(row.get('pe_ttm', 0)) if pd.notna(row.get('pe_ttm')) else None,
                    'pe_lyr': float(row.get('pe', 0)) if pd.notna(row.get('pe')) else None,
                    'pb': float(row.get('pb', 0)) if pd.notna(row.get('pb')) else None,
                    'ps_ttm': float(row.get('ps_ttm', 0)) if pd.notna(row.get('ps_ttm')) else None,
                    'dv_ttm': float(row.get('dv_ttm', 0)) if pd.notna(row.get('dv_ttm')) else None,
                    'total_share': int(row.get('total_share', 0)) if pd.notna(row.get('total_share')) else None,
                    'float_share': int(row.get('float_share', 0)) if pd.notna(row.get('float_share')) else None,
                    'free_share': int(row.get('free_share', 0)) if pd.notna(row.get('free_share')) else None,
                    'total_mv': float(row.get('total_mv', 0)) if pd.notna(row.get('total_mv')) else None,
                    'circ_mv': float(row.get('circ_mv', 0)) if pd.notna(row.get('circ_mv')) else None,
                    'amount': float(row.get('amount', 0)) if pd.notna(row.get('amount')) else None,
                    'raw_payload': safe_json_dumps(row.to_dict()),
                })

            if records:
                batch_size = 500
                for i in range(0, len(records), batch_size):
                    self.db.upsert_many('stock_daily_basic', records[i:i + batch_size], ['trade_date', 'ts_code'])
                logger.info(f'stock_daily_basic upserted: {len(records)}')
                return True

        except Exception as e:
            logger.error(f'update_stock_daily_basic failed: {e}')
            import traceback
            traceback.print_exc()

        return False


    def _get_stock_names(self) -> Dict[str, str]:
        """Backward compatible wrapper, source is stock_master."""
        meta = self._get_stock_meta_map()
        return {k: (v.get('name') or '') for k, v in meta.items()}


    def generate_stock_groups(self, date: str) -> bool:
        """Generate top100 groups based on filtered universe."""
        date = self._normalize_trade_date(date)
        logger.info(f'generating stock groups: {date}')

        try:
            rows = self.db.query(
                """
                SELECT sdb.*, COALESCE(sm.is_st, 0) AS is_st,
                       COALESCE(sm.is_delist, 0) AS is_delist,
                       COALESCE(sm.is_bse, 0) AS is_bse
                FROM stock_daily_basic sdb
                LEFT JOIN stock_master sm ON sdb.ts_code = sm.ts_code
                WHERE sdb.trade_date = %s
                """,
                (date,)
            )

            if not rows:
                logger.warning('stock_daily_basic is empty for group generation')
                return False

            df = pd.DataFrame(rows)
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
            df['pct_change'] = pd.to_numeric(df['pct_change'], errors='coerce')
            df['total_mv'] = pd.to_numeric(df['total_mv'], errors='coerce')
            df = df.drop_duplicates(subset=['ts_code'], keep='first')

            df = df[df['is_st'] != 1]
            df = df[df['is_delist'] != 1]
            df = df[df['is_bse'] != 1]
            df = df[~df['symbol'].astype(str).str.startswith(('8', '4'), na=False)]
            df = df[df['amount'].notna()]
            df = df[df['amount'] > 0]

            if df.empty:
                logger.warning('filtered universe is empty')
                return False

            groups = []

            turnover_top = df.nlargest(100, 'amount')
            for i, (_, row) in enumerate(turnover_top.iterrows(), 1):
                groups.append({
                    'trade_date': date,
                    'group_type': 'top_100_turnover',
                    'rank_no': i,
                    'ts_code': row['ts_code'],
                    'symbol': row['symbol'],
                    'name': row['name'],
                    'close_price': row['close_price'],
                    'pct_change': row['pct_change'],
                    'amount': row['amount'],
                    'total_mv': row['total_mv'],
                })

            gainers = df[df['pct_change'].notna()].nlargest(100, 'pct_change')
            for i, (_, row) in enumerate(gainers.iterrows(), 1):
                groups.append({
                    'trade_date': date,
                    'group_type': 'top_100_gainers',
                    'rank_no': i,
                    'ts_code': row['ts_code'],
                    'symbol': row['symbol'],
                    'name': row['name'],
                    'close_price': row['close_price'],
                    'pct_change': row['pct_change'],
                    'amount': row['amount'],
                    'total_mv': row['total_mv'],
                })

            losers = df[df['pct_change'].notna()].nsmallest(100, 'pct_change')
            for i, (_, row) in enumerate(losers.iterrows(), 1):
                groups.append({
                    'trade_date': date,
                    'group_type': 'top_100_losers',
                    'rank_no': i,
                    'ts_code': row['ts_code'],
                    'symbol': row['symbol'],
                    'name': row['name'],
                    'close_price': row['close_price'],
                    'pct_change': row['pct_change'],
                    'amount': row['amount'],
                    'total_mv': row['total_mv'],
                })

            if groups:
                self.db.execute("DELETE FROM stock_group_member WHERE trade_date = %s", (date,))
                self.db.upsert_many('stock_group_member', groups, ['trade_date', 'group_type', 'rank_no'])
                logger.info(
                    f"groups done turnover={len(turnover_top)} gainers={len(gainers)} losers={len(losers)}"
                )
                return True

        except Exception as e:
            logger.error(f'generate_stock_groups failed: {e}')
            import traceback
            traceback.print_exc()

        return False

    def calculate_gainer_features(self, date: str) -> bool:
        """
        计算涨幅Top100个股的特征
        - 成交额分层
        - 市值分层
        - 板块分类
        - K线形态识别
        """
        logger.info(f"开始计算涨幅Top100特征: {date}")
        
        try:
            # 获取涨幅Top100
            gainers = self.db.query(
                "SELECT * FROM stock_group_member WHERE trade_date = %s AND group_type = 'top_100_gainers' ORDER BY rank_no",
                (date,)
            )
            
            if not gainers:
                logger.warning("无涨幅Top100数据")
                return False
            
            feature_records = []
            pattern_dist = {}  # 形态分布统计
            turnover_stats = {'lt_5e8': 0, 'e8_5_to_50': 0, 'e8_50_to_90': 0, 'gt_9e9': 0}
            mktcap_stats = {'lt_5e9': 0, 'e9_5_to_10': 0, 'e9_10_to_20': 0, 'e9_20_to_50': 0, 'gt_5e10': 0}
            board_stats = {'main': 0, 'gem': 0, 'star': 0}
            unclassified = 0
            
            # 使用线程池并行处理
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_stock = {
                    executor.submit(self._analyze_single_stock, stock, date): stock 
                    for stock in gainers
                }
                
                for future in as_completed(future_to_stock):
                    stock = future_to_stock[future]
                    try:
                        result = future.result()
                        if result:
                            feature_records.append(result)
                            
                            # 统计分层
                            tb = result.get('turnover_bucket')
                            if tb in turnover_stats:
                                turnover_stats[tb] += 1
                            
                            mb = result.get('mktcap_bucket')
                            if mb in mktcap_stats:
                                mktcap_stats[mb] += 1
                            
                            bb = result.get('board_type')
                            if bb in board_stats:
                                board_stats[bb] += 1
                            
                            # 统计形态
                            pc = result.get('pattern_code')
                            if pc:
                                pattern_dist[pc] = pattern_dist.get(pc, 0) + 1
                            else:
                                unclassified += 1
                                
                    except Exception as e:
                        logger.error(f"分析股票 {stock.get('ts_code')} 失败: {e}")
            
            # 保存个股特征
            if feature_records:
                self.db.upsert_many('gainer_feature_stock', feature_records, ['trade_date', 'ts_code'])
                logger.info(f"✓ 涨幅Top100个股特征保存成功: {len(feature_records)} 条")
            
            # 保存汇总统计
            summary = {
                'trade_date': date,
                'turnover_lt_5e8': turnover_stats['lt_5e8'],
                'turnover_5e8_to_50': turnover_stats['e8_5_to_50'],
                'turnover_50e8_to_90': turnover_stats['e8_50_to_90'],
                'turnover_gt_90e8': turnover_stats['gt_9e9'],
                'mktcap_lt_5e9': mktcap_stats['lt_5e9'],
                'mktcap_5e9_to_10': mktcap_stats['e9_5_to_10'],
                'mktcap_10e9_to_20': mktcap_stats['e9_10_to_20'],
                'mktcap_20e9_to_50': mktcap_stats['e9_20_to_50'],
                'mktcap_gt_50e9': mktcap_stats['gt_5e10'],
                'board_main': board_stats['main'],
                'board_gem': board_stats['gem'],
                'board_star': board_stats['star'],
                'pattern_distribution': safe_json_dumps(pattern_dist),
                'pattern_unclassified': unclassified,
            }
            self.db.upsert('gainer_feature_summary', summary, ['trade_date'])
            logger.info(f"✓ 涨幅Top100汇总统计保存成功")
            
            return True
            
        except Exception as e:
            logger.error(f"计算特征失败: {e}")
            import traceback
            traceback.print_exc()
        
        return False
    
    def _analyze_single_stock(self, stock: Dict, date: str) -> Optional[Dict]:
        """分析单只股票的特征"""
        ts_code = stock['ts_code']
        symbol = stock['symbol']
        
        try:
            # 1. 成交额分层 (元转亿元)
            amount = stock.get('amount', 0) or 0  # 千元单位
            amount_yuan = amount * 1000  # 转为元
            
            if amount_yuan < 5e8:  # < 5亿
                turnover_bucket = 'lt_5e8'
            elif amount_yuan < 5e9:  # 5-50亿
                turnover_bucket = 'e8_5_to_50'
            elif amount_yuan < 9e9:  # 50-90亿
                turnover_bucket = 'e8_50_to_90'
            else:  # > 90亿
                turnover_bucket = 'gt_9e9'
            
            # 2. 市值分层 (万元转亿元)
            total_mv = stock.get('total_mv', 0) or 0  # 万元
            total_mv_yuan = total_mv * 10000  # 转为元
            
            if total_mv_yuan < 5e9:  # < 50亿
                mktcap_bucket = 'lt_5e9'
            elif total_mv_yuan < 1e10:  # 50-100亿
                mktcap_bucket = 'e9_5_to_10'
            elif total_mv_yuan < 2e10:  # 100-200亿
                mktcap_bucket = 'e9_10_to_20'
            elif total_mv_yuan < 5e10:  # 200-500亿
                mktcap_bucket = 'e9_20_to_50'
            else:  # > 500亿
                mktcap_bucket = 'gt_5e10'
            
            # 3. 板块分类
            if symbol.startswith('688'):
                board_type = 'star'  # 科创板
            elif symbol.startswith('300') or symbol.startswith('301'):
                board_type = 'gem'  # 创业板
            else:
                board_type = 'main'  # 主板
            
            # 4. K线形态识别
            kline_df = self._fetch_kline(ts_code, date, days=20)
            pattern_code = None
            pattern_name = None
            pattern_confidence = None
            kline_snapshot = None
            
            if kline_df is not None and len(kline_df) >= 3:
                pattern = self.pattern_recognizer.recognize(kline_df)
                if pattern:
                    pattern_code = pattern.code
                    pattern_name = pattern.name
                    pattern_confidence = pattern.confidence
                
                # 保存K线快照（最近5天）
                kline_snapshot = safe_json_dumps(kline_df.tail(5).to_dict(orient='records'))
            
            return {
                'trade_date': date,
                'ts_code': ts_code,
                'symbol': symbol,
                'name': stock.get('name', ''),
                'rank_no': stock.get('rank_no', 0),
                'close_price': stock.get('close_price'),
                'pct_change': stock.get('pct_change'),
                'amount': amount,
                'total_mv': total_mv,
                'turnover_bucket': turnover_bucket,
                'mktcap_bucket': mktcap_bucket,
                'board_type': board_type,
                'pattern_code': pattern_code,
                'pattern_name': pattern_name,
                'pattern_version': '1.0',
                'pattern_confidence': pattern_confidence,
                'kline_snapshot': kline_snapshot,
            }
            
        except Exception as e:
            logger.error(f"分析股票 {ts_code} 失败: {e}")
        
        return None
    
    def _fetch_kline(self, ts_code: str, end_date: str, days: int = 20) -> Optional[pd.DataFrame]:
        """获取股票K线数据"""
        try:
            # 计算开始日期
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            start_dt = end_dt - timedelta(days=days * 2)  # 多取一些以防节假日
            start_date = start_dt.strftime('%Y%m%d')
            
            if self.ts_pro:
                df = self.ts_pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                if not df.empty:
                    df = df.sort_values('trade_date')
                    df = df.tail(days)
                    df = df.rename(columns={
                        'open': 'open',
                        'high': 'high',
                        'low': 'low',
                        'close': 'close',
                        'vol': 'volume',
                        'amount': 'amount'
                    })
                    return df
        except Exception as e:
            logger.error(f"获取K线失败 {ts_code}: {e}")
        
        return None
    
    # ==================== 6. 主运行方法 ====================
    

    def run_full_update(self, date: str = None) -> Dict:
        """Run the full update pipeline for one trade date."""
        date = self._normalize_trade_date(date)

        logger.info('=' * 60)
        logger.info(f'start full update: {date}')
        logger.info('=' * 60)

        job_id = self._log_job_start(date, is_trade_day=0)

        result = {
            'date': date,
            'is_trade_day': False,
            'steps': {},
            'success': False,
            'message': ''
        }

        try:
            is_trade_day, msg = self.check_trade_day(date)
            result['is_trade_day'] = is_trade_day
            result['steps']['check_trade_day'] = msg

            if not is_trade_day:
                result['message'] = f'non-trade day: {msg}'
                self._log_job_end(job_id, 'skipped', result['message'], is_trade_day=0)
                return result

            result['steps']['external_assets'] = self.update_external_assets(date)
            time.sleep(1)

            result['steps']['index_data'] = self.update_index_data(date)
            time.sleep(1)

            result['steps']['market_activity'] = self.update_market_activity(date)
            time.sleep(1)

            result['steps']['stock_daily_basic'] = self.update_stock_daily_basic(date)
            time.sleep(2)

            result['steps']['stock_groups'] = self.generate_stock_groups(date)
            result['steps']['gainer_features'] = self.calculate_gainer_features(date)

            # 融资数据、创业板PE改为前端实时获取，不再做每日落库

            success_steps = sum(1 for v in result['steps'].values() if v)
            total_steps = len(result['steps'])
            result['success'] = success_steps >= total_steps * 0.7
            result['message'] = f'update done: {success_steps}/{total_steps} steps succeeded'

            self._log_job_end(job_id, 'success' if result['success'] else 'partial', result['message'], is_trade_day=1)

        except Exception as e:
            logger.error(f'run_full_update exception: {e}')
            import traceback
            traceback.print_exc()
            result['success'] = False
            result['message'] = f'update failed: {str(e)}'
            self._log_job_end(job_id, 'failed', result['message'], is_trade_day=1 if result['is_trade_day'] else 0)

        logger.info('=' * 60)
        logger.info(f'full update end: {result["message"]}')
        logger.info('=' * 60)

        return result


    def _log_job_start(self, date: str, is_trade_day: int = 0) -> int:
        """Insert a running job log row."""
        try:
            sql = """
                INSERT INTO job_run_log (trade_date, is_trade_day, status, start_time, message)
                VALUES (%s, %s, 'running', NOW(), 'job started')
            """
            self.db.execute(sql, (date, int(is_trade_day)))
            result = self.db.query_one("SELECT LAST_INSERT_ID() as id")
            return result['id'] if result else 0
        except Exception as e:
            logger.error(f'_log_job_start failed: {e}')
            return 0


    def _log_job_end(self, job_id: int, status: str, message: str, is_trade_day: Optional[int] = None):
        """Finalize job log row."""
        try:
            if is_trade_day is None:
                sql = """
                    UPDATE job_run_log
                    SET status = %s, end_time = NOW(), message = %s
                    WHERE id = %s
                """
                self.db.execute(sql, (status, message, job_id))
            else:
                sql = """
                    UPDATE job_run_log
                    SET status = %s, end_time = NOW(), message = %s, is_trade_day = %s
                    WHERE id = %s
                """
                self.db.execute(sql, (status, message, int(is_trade_day), job_id))
        except Exception as e:
            logger.error(f'_log_job_end failed: {e}')

    def close(self):
        """关闭资源"""
        if self.db:
            self.db.close()


# ==================== 便捷函数 ====================

def run_daily_update(date: str = None) -> Dict:
    """运行每日数据更新（便捷函数）"""
    updater = TradeDayUpdater()
    try:
        result = updater.run_full_update(date)
        return result
    finally:
        updater.close()


if __name__ == '__main__':
    # 命令行运行
    import sys
    
    date = None
    if len(sys.argv) > 1:
        date = sys.argv[1]
    
    result = run_daily_update(date)
    print(safe_json_dumps(result))
