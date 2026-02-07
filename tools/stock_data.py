"""
股票数据模块
包含K线数据获取、技术分析、图表绘制等功能
"""

import streamlit as st
import pandas as pd
import numpy as np
import datetime
import akshare as ak
import mplfinance as mpf
import matplotlib.dates as mdates
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import os
import tushare as ts

# 配置日志
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# tushare配置
TS_TOKEN = os.getenv(
    'TUSHARE_TOKEN',
    '943ca25f1428e5ed6d7d752b6b1496e6afdcac48ace4cf54e0d82a6e'
)
_ts_pro_client = None


def get_next_tradeday(date):
    """获取下一个交易日"""
    if date == datetime.datetime.today():
        return date
    day_of_week = date.weekday()

    if day_of_week >= 0 and day_of_week <= 3:  # 周一到周四
        next_date = date + datetime.timedelta(days=1)
    elif day_of_week == 4:  # 周五
        next_date = date + datetime.timedelta(days=3)  # 加3天以跳过周末
    else:  # 周六或周日
        return None
    return next_date.strftime('%Y-%m-%d')


def get_stock_short_name(code):
    """获取股票简称"""
    try:
        stock_info_df = ak.stock_individual_info_em(symbol=code)
        
        if '股票简称' in stock_info_df['item'].tolist():
            short_name_index = stock_info_df['item'].tolist().index('股票简称')
            short_name = stock_info_df['value'][short_name_index]
            return short_name
        else:
            return "股票简称列不存在或数据有误。"
    except Exception as e:
        logger.error(f"获取股票简称失败: {str(e)}")
        return f"获取简称失败: {code}"


def _get_ts_client():
    """惰性初始化tushare客户端"""
    global _ts_pro_client
    if _ts_pro_client is None:
        if not TS_TOKEN:
            raise ValueError("未配置 Tushare Token，无法使用兜底数据源")
        ts.set_token(TS_TOKEN)
        _ts_pro_client = ts.pro_api()
    return _ts_pro_client


def _convert_to_ts_code(code):
    """将多种代码格式转换为tushare ts_code"""
    if code is None:
        raise ValueError("股票代码不能为空")

    code = str(code).strip()
    upper_code = code.upper()

    if '.' in upper_code:
        prefix, suffix = upper_code.split('.', 1)
        suffix = suffix.replace('SS', 'SH')
        if suffix in {'SH', 'SZ', 'BJ'}:
            return f"{prefix}.{suffix}"

    if upper_code.startswith('SZ') or upper_code.startswith('SH') or upper_code.startswith('BJ'):
        body = upper_code[2:]
        suffix = upper_code[:2]
        return f"{body}.{suffix}"

    if len(code) == 6 and code.isdigit():
        if code.startswith(('0', '3')):
            suffix = 'SZ'
        elif code.startswith(('6', '9')):
            suffix = 'SH'
        elif code.startswith('8'):
            suffix = 'BJ'
        else:
            suffix = 'SZ'
        return f"{code}.{suffix}"

    return upper_code


def get_tushare_price_df(code, end_date=None, count=60):
    """使用tushare获取股票日K线数据"""
    if end_date is None:
        end_date = datetime.datetime.now().strftime('%Y%m%d')

    ts_code = _convert_to_ts_code(code)
    pro = _get_ts_client()

    end_dt = datetime.datetime.strptime(end_date, '%Y%m%d')
    start_dt = end_dt - datetime.timedelta(days=max(count * 3, 120))
    start_date = start_dt.strftime('%Y%m%d')

    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        raise ValueError(f"Tushare未返回 {ts_code} 的日线数据")

    df = df.sort_values('trade_date')
    if len(df) > count:
        df = df.tail(count)

    df = df[['trade_date', 'open', 'close', 'high', 'low', 'vol']].copy()
    df.rename(columns={'trade_date': 'date', 'vol': 'volume'}, inplace=True)
    df['volume'] = df['volume'] * 100  # tushare单位为手，需要换算为股以匹配akshare
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    return df


@st.cache_data(ttl='0.5d')
def get_ak_price_df(code, end_date=None, count=60):
    """获取股票日K线数据，统一使用 TuShare 数据源"""
    if end_date is None:
        end_date = datetime.datetime.now().strftime('%Y%m%d')

    try:
        return get_tushare_price_df(code, end_date, count)
    except Exception as ts_error:
        logger.error(f"tushare获取 {code} 日线失败: {ts_error}")
        raise


@st.cache_data(ttl='0.5d')
def get_ak_interval_price_df(code, end_date=None, count=241):
    """获取股票分时数据（带重试机制）"""
    if end_date is None:
        end_date = datetime.datetime.now().strftime('%Y%m%d')
    
    df = ak.stock_zh_a_hist_min_em(
        symbol=code,
        end_date=end_date,
        period='1'
    ).tail(count)
    
    df.columns = ['date', 'open', 'close', 'high', 'low', 'volume_', 'volume', 'lastprice']
    df = df[['date', 'open', 'close', 'high', 'low', 'volume']]
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    return df


def plotK(df, k='d', plot_type='candle', ma_line=None, fail_zt=False, container=st, highlight_date=None):
    """绘制K线图"""
    if k == 'w':
        df = df.resample('W').agg({
            'open': 'first', 
            'high': 'max', 
            'low': 'min', 
            'close': 'last',
            'volume': 'sum'
        })
    if k == 'm':
        df = df.resample('M').agg({
            'open': 'first', 
            'high': 'max', 
            'low': 'min', 
            'close': 'last',
            'volume': 'sum'
        })

    if fail_zt:
        mc = mpf.make_marketcolors(up='black', down='darkgray', inherit=True)
    else:
        mc = mpf.make_marketcolors(up='r', down='g', inherit=True)
    s = mpf.make_mpf_style(marketcolors=mc, gridaxis='horizontal', gridstyle='dashed')
    plot_args = {
        'type': plot_type,
        'style': s,
        'volume': True,
        'returnfig': True
    }
    if ma_line is not None: 
        # 如果提供了自定义均线参数，使用提供的参数
        plot_args['mav'] = ma_line
    else:
        # 默认显示5、10、20日均线
        plot_args['mav'] = (5, 10, 20, 60, 144, 250)

    # 处理标注日期（仅用于箭头，不再画竖线）
    if highlight_date is not None:
        if isinstance(highlight_date, str):
            highlight_date = pd.to_datetime(highlight_date)
        elif isinstance(highlight_date, datetime.datetime):
            highlight_date = pd.to_datetime(highlight_date)

    fig, axe = mpf.plot(df, **plot_args)

    # 如果有标注日期，添加红色上箭头，标在当日 K 线下方
    if highlight_date is not None and highlight_date in df.index:
        ax = axe[0]  # 主图
        candle_low = float(df.loc[highlight_date, 'low'])
        x_val = mdates.date2num(pd.to_datetime(highlight_date))
        # 用实心短箭头从下指向当日最低价，避免错位
        ax.annotate(
            '',
            xy=(x_val, candle_low),
            xytext=(x_val, candle_low * 0.93),
            xycoords=('data', 'data'),
            arrowprops=dict(
                arrowstyle='simple',
                color='red',
                lw=0,
                alpha=0.9,
                shrinkA=0,
                shrinkB=0,
            ),
            annotation_clip=True,
            zorder=6,
        )

    container.pyplot(fig)


class PriceData:
    """股票价格数据类"""
    
    def __init__(self, code):
        self.code = code

    def buy_date_price(self, buy_date):
        """获取买入日期价格"""
        df = get_ak_price_df(self.code, buy_date.strftime('%Y%m%d'), count=2)
        return df.to_dict('records')

    def next_tradeday_price(self, buy_date):
        """获取下一个交易日价格"""
        next_tradeday = get_next_tradeday(buy_date).replace('-', '')
        day_df = get_ak_price_df(self.code, next_tradeday, count=1)
        hour_df = ak.stock_zh_a_hist_min_em(self.code, start_date=next_tradeday, end_date=next_tradeday, period='30')
        price_dict = {
            'code': self.code,
            'day': day_df.to_dict('records'),
            'hours': hour_df.to_dict('records') 
        }
        return price_dict

    def plotDayK(self, buy_date, container=st):
        """绘制日K线图"""
        df = get_ak_price_df(self.code, buy_date.strftime('%Y%m%d'))
        plotK(df, container=container)

    def plotIntervalK(self, buy_date, container=st):
        """绘制分时K线图"""
        df = get_ak_interval_price_df(self.code, buy_date.strftime('%Y%m%d'))
        plotK(df, plot_type='line', container=container)


class StockTechnical:
    """
    股票技术分析类
    
    对股票K线数据进行技术分析，包括：
    - 多周期数据预处理（周线、月线）
    - 新高后涨势指标（New High）
    - 高频换手短线情绪指标（High Turnover Sentiment）
    - 周/月线箱体突破形态（Box Breakout）
    """
    
    def __init__(self, df, date_col='date', stock_id_col=None):
        """
        初始化技术分析类
        
        参数:
            df: DataFrame，包含K线数据
                必需字段: open, high, low, close, volume, turnover
                可选字段: float_shares, amount
                索引: [date, stock_id] 或按 stock_id 分组后按 date 排序
            date_col: 日期列名（如果不在索引中）
            stock_id_col: 股票ID列名（如果不在索引中，且有多只股票）
        """
        self.df = df.copy()
        
        # 处理索引和列
        if isinstance(self.df.index, pd.MultiIndex):
            # MultiIndex: [date, stock_id]
            self.df = self.df.sort_index()
            self.multi_stock = True
        elif stock_id_col and stock_id_col in self.df.columns:
            # 单索引，但有stock_id列
            if date_col in self.df.columns:
                self.df[date_col] = pd.to_datetime(self.df[date_col])
                self.df = self.df.set_index([date_col, stock_id_col]).sort_index()
                self.multi_stock = True
            else:
                self.df.index = pd.to_datetime(self.df.index)
                self.multi_stock = True
        else:
            # 单只股票
            if date_col in self.df.columns:
                self.df[date_col] = pd.to_datetime(self.df[date_col])
                self.df = self.df.set_index(date_col).sort_index()
            else:
                self.df.index = pd.to_datetime(self.df.index)
            self.multi_stock = False
        
        # 验证必需字段
        required_cols = ['open', 'high', 'low', 'close', 'volume', 'turnover']
        missing_cols = [col for col in required_cols if col not in self.df.columns]
        if missing_cols:
            raise ValueError(f"缺少必需字段: {missing_cols}")
        
        # 计算换手率（如果没有）
        if 'turnover' not in self.df.columns or self.df['turnover'].isna().all():
            if 'float_shares' in self.df.columns:
                self.df['turnover'] = self.df['volume'] / self.df['float_shares']
            else:
                raise ValueError("缺少 turnover 字段，且无法从 float_shares 计算")
        
        # 初始化多周期数据
        self.weekly_df = None
        self.monthly_df = None
        
    def _groupby_stock(self, func, *args, **kwargs):
        """对多只股票应用函数"""
        if self.multi_stock:
            return self.df.groupby(level=1 if isinstance(self.df.index, pd.MultiIndex) else 'stock_id').apply(
                lambda x: func(x, *args, **kwargs)
            )
        else:
            return func(self.df, *args, **kwargs)
    
    def aggregate_weekly(self):
        """聚合周线数据"""
        def _agg_weekly(group_df):
            group_df = group_df.copy()
            is_multi = isinstance(group_df.index, pd.MultiIndex)
            
            if is_multi:
                # 保存股票ID
                stock_id = group_df.index.get_level_values(1)[0]
                dates = pd.to_datetime(group_df.index.get_level_values(0))
                group_df.index = dates
            else:
                group_df.index = pd.to_datetime(group_df.index)
            
            # 构建聚合字典
            agg_dict = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'turnover': 'sum'
            }
            if 'float_shares' in group_df.columns:
                agg_dict['float_shares'] = 'last'
            if 'amount' in group_df.columns:
                agg_dict['amount'] = 'sum'
            
            weekly = group_df.resample('W').agg(agg_dict)
            # 重命名列（float_shares 不重命名）
            rename_dict = {col: f'{col}_w' for col in weekly.columns if col != 'float_shares'}
            weekly = weekly.rename(columns=rename_dict)
            
            # 如果是多股票，恢复MultiIndex
            if is_multi:
                weekly.index = pd.MultiIndex.from_arrays([weekly.index, [stock_id] * len(weekly)], names=['date', 'stock_id'])
            
            return weekly
        
        if self.multi_stock:
            self.weekly_df = self._groupby_stock(_agg_weekly)
        else:
            self.weekly_df = _agg_weekly(self.df)
        return self.weekly_df
    
    def aggregate_monthly(self):
        """聚合月线数据"""
        def _agg_monthly(group_df):
            group_df = group_df.copy()
            is_multi = isinstance(group_df.index, pd.MultiIndex)
            
            if is_multi:
                # 保存股票ID
                stock_id = group_df.index.get_level_values(1)[0]
                dates = pd.to_datetime(group_df.index.get_level_values(0))
                group_df.index = dates
            else:
                group_df.index = pd.to_datetime(group_df.index)
            
            # 构建聚合字典
            agg_dict = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'turnover': 'sum'
            }
            if 'float_shares' in group_df.columns:
                agg_dict['float_shares'] = 'last'
            if 'amount' in group_df.columns:
                agg_dict['amount'] = 'sum'
            
            monthly = group_df.resample('M').agg(agg_dict)
            # 重命名列（float_shares 不重命名）
            rename_dict = {col: f'{col}_m' for col in monthly.columns if col != 'float_shares'}
            monthly = monthly.rename(columns=rename_dict)
            
            # 如果是多股票，恢复MultiIndex
            if is_multi:
                monthly.index = pd.MultiIndex.from_arrays([monthly.index, [stock_id] * len(monthly)], names=['date', 'stock_id'])
            
            return monthly
        
        if self.multi_stock:
            self.monthly_df = self._groupby_stock(_agg_monthly)
        else:
            self.monthly_df = _agg_monthly(self.df)
        return self.monthly_df
    
    def new_high_analysis(self, N_high=60, min_ret0=0.02, k_vol=10):
        """
        新高后涨势指标分析
        
        参数:
            N_high: 窗口天数，默认60
            min_ret0: 当日最小涨幅，默认0.02 (2%)
            k_vol: 量能均值窗口，默认10
        
        返回:
            DataFrame，包含NH_flag和各项特征
        """
        def _calc_new_high(group_df):
            df = group_df.copy()
            df = df.sort_index()
            
            # 计算滚动最高价
            df['rolling_max_close'] = df['close'].shift(1).rolling(window=N_high, min_periods=1).max()
            
            # 计算当日涨幅
            df['ret0'] = df['close'] / df['close'].shift(1) - 1
            
            # 新高标志
            df['NH_flag'] = (
                (df['close'] > df['rolling_max_close']) & 
                (df['ret0'] >= min_ret0)
            )
            
            # 新高当日特征（仅在NH_flag为True时计算）
            df['NH_strength'] = np.where(
                df['NH_flag'],
                (df['close'] - df['rolling_max_close']) / df['rolling_max_close'],
                np.nan
            )
            
            # 量能比率
            df['vol_mean'] = df['volume'].shift(1).rolling(window=k_vol, min_periods=1).mean()
            df['Vol_ratio_NH'] = np.where(
                df['NH_flag'],
                df['volume'] / df['vol_mean'],
                np.nan
            )
            
            # 实体比率
            df['Body_ratio_NH'] = np.where(
                df['NH_flag'],
                abs(df['close'] - df['open']) / (df['high'] - df['low'] + 1e-8),
                np.nan
            )
            
            # 上影线比率
            df['Upper_shadow_NH'] = np.where(
                df['NH_flag'],
                (df['high'] - df[['close', 'open']].max(axis=1)) / (df['high'] - df['low'] + 1e-8),
                np.nan
            )
            
            # MA20
            df['MA20'] = df['close'].rolling(window=20, min_periods=1).mean()
            df['MA20_gap_NH'] = np.where(
                df['NH_flag'],
                df['close'] / df['MA20'] - 1,
                np.nan
            )
            
            return df
        
        result = self._groupby_stock(_calc_new_high)
        return result
    
    def turnover_sentiment_analysis(self, turnover_thr=0.10, vol_ratio_thr=2.0, 
                                     range_thr=0.05, k_vol=10, 
                                     calc_score=False, score_weights=None):
        """
        高频换手短线情绪指标分析
        
        参数:
            turnover_thr: 高换手阈值，默认0.10 (10%)
            vol_ratio_thr: 量能放大阈值，默认2.0
            range_thr: 波动幅度阈值，默认0.05 (5%)
            k_vol: 量能均值窗口，默认10
            calc_score: 是否计算情绪打分，默认False
            score_weights: 打分权重 [w1, w2, w3, w4]，默认None
        
        返回:
            DataFrame，包含各项情绪指标
        """
        def _calc_sentiment(group_df):
            df = group_df.copy()
            df = df.sort_index()
            
            # 基础指标
            df['Vol_ratio'] = df['volume'] / df['volume'].shift(1).rolling(window=k_vol, min_periods=1).mean()
            df['Range'] = (df['high'] - df['low']) / (df['close'].shift(1) + 1e-8)
            df['Body_ratio'] = abs(df['close'] - df['open']) / (df['high'] - df['low'] + 1e-8)
            df['Upper_shadow'] = (df['high'] - df[['close', 'open']].max(axis=1)) / (df['high'] - df['low'] + 1e-8)
            
            # 情绪事件
            df['is_emotion'] = (
                (df['turnover'] > turnover_thr) &
                (df['Vol_ratio'] > vol_ratio_thr) &
                (df['Range'] > range_thr)
            )
            
            # 情绪打分（可选）
            if calc_score:
                if score_weights is None:
                    score_weights = [0.3, 0.3, 0.3, -0.1]  # 默认权重
                
                # Z-score标准化
                for col in ['turnover', 'Vol_ratio', 'Range', 'Upper_shadow']:
                    mean_val = df[col].mean()
                    std_val = df[col].std()
                    df[f'z_{col}'] = (df[col] - mean_val) / (std_val + 1e-8)
                
                df['Sentiment_score'] = (
                    score_weights[0] * df['z_turnover'] +
                    score_weights[1] * df['z_Vol_ratio'] +
                    score_weights[2] * df['z_Range'] +
                    score_weights[3] * df['z_Upper_shadow']
                )
            
            return df
        
        result = self._groupby_stock(_calc_sentiment)
        return result
    
    def box_breakout_analysis(self, period='W', Lw=12, box_width_max=0.2, 
                              eps=0.01, k_vol_w=6, vol_ratio_w_thr=1.5):
        """
        周/月线箱体突破形态分析
        
        参数:
            period: 周期类型，'W'（周线）或'M'（月线），默认'W'
            Lw: 箱体回看周/月数，默认12
            box_width_max: 箱体最大宽度，默认0.2 (20%)
            eps: 突破高点的最小超出比例，默认0.01 (1%)
            k_vol_w: 周/月量能均值窗口，默认6
            vol_ratio_w_thr: 突破周/月量能放大阈值，默认1.5
        
        返回:
            DataFrame，包含箱体和突破指标
        """
        # 确保已聚合周/月线数据
        if period == 'W':
            if self.weekly_df is None:
                self.aggregate_weekly()
            period_df = self.weekly_df.copy()
            high_col = 'high_w'
            low_col = 'low_w'
            close_col = 'close_w'
            volume_col = 'volume_w'
        elif period == 'M':
            if self.monthly_df is None:
                self.aggregate_monthly()
            period_df = self.monthly_df.copy()
            high_col = 'high_m'
            low_col = 'low_m'
            close_col = 'close_m'
            volume_col = 'volume_m'
        else:
            raise ValueError("period 必须是 'W' 或 'M'")
        
        def _calc_box_breakout(group_df):
            df = group_df.copy()
            df = df.sort_index()
            
            # 箱体识别
            df['Box_high'] = df[high_col].rolling(window=Lw, min_periods=1).max()
            df['Box_low'] = df[low_col].rolling(window=Lw, min_periods=1).min()
            df['Box_width'] = (df['Box_high'] / df['Box_low'] - 1)
            df['is_box'] = (df['Box_width'] < box_width_max)
            
            # 箱体突破事件
            df['Box_high_prev'] = df['Box_high'].shift(1)
            df['avg_vol_box'] = df[volume_col].shift(1).rolling(window=Lw, min_periods=1).mean()
            df['Vol_ratio_w'] = df[volume_col] / (df['avg_vol_box'] + 1e-8)
            
            df['Breakout_up'] = (
                df['is_box'].shift(1) &
                (df[close_col] > df['Box_high_prev'] * (1 + eps)) &
                (df['Vol_ratio_w'] > vol_ratio_w_thr)
            )
            
            df['Breakout_strength'] = np.where(
                df['Breakout_up'],
                (df[close_col] - df['Box_high_prev']) / df['Box_high_prev'],
                np.nan
            )
            
            return df
        
        if self.multi_stock:
            result = period_df.groupby(level=1 if isinstance(period_df.index, pd.MultiIndex) else 'stock_id').apply(_calc_box_breakout)
        else:
            result = _calc_box_breakout(period_df)
        
        return result
    
    def get_features(self, stock_id=None, include_new_high=True, include_sentiment=True, 
                     include_box_breakout=True, **kwargs):
        """
        获取指定股票的所有技术特征
        
        参数:
            stock_id: 股票ID（多只股票时使用）
            include_new_high: 是否包含新高指标，默认True
            include_sentiment: 是否包含情绪指标，默认True
            include_box_breakout: 是否包含箱体突破指标，默认True
            **kwargs: 传递给各分析方法的参数
        
        返回:
            DataFrame，包含所有特征
        """
        result_df = self.df.copy()
        
        if include_new_high:
            nh_params = {
                'N_high': kwargs.get('N_high', 60),
                'min_ret0': kwargs.get('min_ret0', 0.02),
                'k_vol': kwargs.get('k_vol', 10)
            }
            nh_result = self.new_high_analysis(**nh_params)
            if self.multi_stock and stock_id:
                nh_result = nh_result.xs(stock_id, level=1)
            result_df = result_df.join(nh_result[['NH_flag', 'NH_strength', 'Vol_ratio_NH', 
                                                   'Body_ratio_NH', 'Upper_shadow_NH', 'MA20_gap_NH']], 
                                       how='left')
        
        if include_sentiment:
            sentiment_params = {
                'turnover_thr': kwargs.get('turnover_thr', 0.10),
                'vol_ratio_thr': kwargs.get('vol_ratio_thr', 2.0),
                'range_thr': kwargs.get('range_thr', 0.05),
                'k_vol': kwargs.get('k_vol', 10),
                'calc_score': kwargs.get('calc_score', False),
                'score_weights': kwargs.get('score_weights', None)
            }
            sentiment_result = self.turnover_sentiment_analysis(**sentiment_params)
            if self.multi_stock and stock_id:
                sentiment_result = sentiment_result.xs(stock_id, level=1)
            result_df = result_df.join(sentiment_result[['Vol_ratio', 'Range', 'Body_ratio', 
                                                         'Upper_shadow', 'is_emotion', 'Sentiment_score']], 
                                       how='left')
        
        if include_box_breakout:
            box_params = {
                'period': kwargs.get('period', 'W'),
                'Lw': kwargs.get('Lw', 12),
                'box_width_max': kwargs.get('box_width_max', 0.2),
                'eps': kwargs.get('eps', 0.01),
                'k_vol_w': kwargs.get('k_vol_w', 6),
                'vol_ratio_w_thr': kwargs.get('vol_ratio_w_thr', 1.5)
            }
            box_result = self.box_breakout_analysis(**box_params)
            if self.multi_stock and stock_id:
                box_result = box_result.xs(stock_id, level=1)
            
            # 箱体突破是周/月线数据，需要重采样到日线并前向填充
            box_cols = ['Box_high', 'Box_low', 'Box_width', 'is_box', 'Breakout_up', 'Breakout_strength']
            box_cols = [col for col in box_cols if col in box_result.columns]
            
            if isinstance(box_result.index, pd.MultiIndex):
                # 如果是MultiIndex，先按股票分组处理
                box_result_daily = box_result[box_cols].groupby(level=1).apply(
                    lambda x: x.droplevel(1).resample('D').ffill()
                )
            else:
                box_result_daily = box_result[box_cols].resample('D').ffill()
            
            # 获取日线索引并对齐
            if isinstance(result_df.index, pd.MultiIndex):
                # 对于MultiIndex，需要按股票对齐
                if isinstance(box_result_daily.index, pd.MultiIndex):
                    box_result_aligned = box_result_daily.reindex(result_df.index, method='ffill')
                else:
                    # 单股票情况，需要构建MultiIndex
                    daily_dates = result_df.index.get_level_values(0)
                    stock_ids = result_df.index.get_level_values(1)
                    box_result_aligned = box_result_daily.reindex(daily_dates, method='ffill')
                    box_result_aligned.index = pd.MultiIndex.from_arrays([daily_dates, stock_ids], names=result_df.index.names)
            else:
                # 单索引情况
                box_result_aligned = box_result_daily.reindex(result_df.index, method='ffill')
            
            result_df = result_df.join(box_result_aligned, how='left')
        
        if self.multi_stock and stock_id:
            return result_df.xs(stock_id, level=1)
        return result_df
    
