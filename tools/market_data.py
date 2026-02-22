"""
市场数据模块
包含大盘数据、赚钱效应分析、龙虎榜数据、板块数据等功能
"""

import streamlit as st
import pandas as pd
import akshare as ak
import os
import json
from datetime import datetime, date
import tushare as ts
from .utils import get_stock_list


def _get_index_amount(index_df, stat_date: str) -> float:
    if index_df is None or index_df.empty:
        return 0.0
    target_date = pd.to_datetime(stat_date, errors="coerce")
    if target_date is not pd.NaT and target_date in index_df.index:
        row = index_df.loc[target_date]
    else:
        row = index_df.iloc[-1]
    if "volume" in index_df.columns:
        try:
            return float(row["volume"])
        except Exception:
            return 0.0
    return 0.0


def _to_number(series):
    if series is None:
        return None
    s = series.astype(str).str.replace("%", "", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _get_prev_trade_date(trade_date: str, pro):
    try:
        start = (pd.to_datetime(trade_date) - pd.Timedelta(days=15)).strftime("%Y%m%d")
        cal = pro.trade_cal(exchange="", start_date=start, end_date=trade_date, is_open=1)
        if cal is None or cal.empty or "cal_date" not in cal.columns:
            return None
        open_dates = cal[cal["is_open"] == 1]["cal_date"].astype(str).tolist()
        if not open_dates:
            return None
        if trade_date in open_dates:
            idx = open_dates.index(trade_date)
            if idx == 0:
                return None
            return open_dates[idx - 1]
        return open_dates[-1]
    except Exception:
        return None


def _to_int(value, default=0):
    if value is None:
        return default
    try:
        s = str(value).replace("%", "").replace(",", "").strip()
        num = pd.to_numeric(s, errors="coerce")
        if pd.isna(num):
            return default
        return int(float(num))
    except Exception:
        return default


def _to_float(value):
    if value is None:
        return None
    try:
        s = str(value).replace("%", "").replace(",", "").strip()
        num = pd.to_numeric(s, errors="coerce")
        if pd.isna(num):
            return None
        return float(num)
    except Exception:
        return None


def _find_market_item_value(market_data: pd.DataFrame, keywords):
    if market_data is None or market_data.empty:
        return None
    if not {"item", "value"}.issubset(set(market_data.columns)):
        return None
    for _, r in market_data.iterrows():
        item = str(r.get("item", ""))
        if any(k in item for k in keywords):
            return r.get("value")
    return None


def _sync_market_activity_to_mysql(stat_date, market_data, total_amount):
    """
    同步市场数据到 MySQL market_activity_daily。
    失败时只打印日志，不影响主流程。
    """
    try:
        from database.db_manager import get_db
    except Exception as e:
        print(f"MySQL模块加载失败: {e}")
        return

    trade_date = pd.to_datetime(stat_date, errors="coerce")
    if pd.isna(trade_date):
        print(f"MySQL同步跳过，无法解析日期: {stat_date}")
        return
    trade_date = trade_date.strftime("%Y-%m-%d")

    up_raw = _find_market_item_value(market_data, ["上涨"])
    down_raw = _find_market_item_value(market_data, ["下跌"])
    zt_raw = _find_market_item_value(market_data, ["涨停"])
    dt_raw = _find_market_item_value(market_data, ["跌停"])
    activity_raw = _find_market_item_value(market_data, ["活跃", "情绪"])

    payload = {}
    if market_data is not None and not market_data.empty and {"item", "value"}.issubset(set(market_data.columns)):
        for _, r in market_data.iterrows():
            k = str(r.get("item", ""))
            v = r.get("value")
            if pd.isna(v):
                payload[k] = None
            else:
                payload[k] = str(v)

    # total_amount 来自 tushare daily.amount 汇总，保持与既有导入脚本一致：/1e8
    total_amount_yi = None
    try:
        total_amount_yi = float(total_amount) / 1e8 if total_amount is not None else None
    except Exception:
        total_amount_yi = None

    data = {
        "trade_date": trade_date,
        "up_count": _to_int(up_raw, 0),
        "down_count": _to_int(down_raw, 0),
        "zt_count": _to_int(zt_raw, 0),
        "dt_count": _to_int(dt_raw, 0),
        "activity_index": _to_float(activity_raw),
        "total_amount": total_amount_yi,
        "raw_payload": json.dumps(payload, ensure_ascii=False),
    }

    try:
        with get_db() as db:
            db.upsert("market_activity_daily", data, unique_keys=["trade_date"])
    except Exception as e:
        print(f"MySQL同步market_activity_daily失败: {e}")


def _get_financing_net_buy(trade_date: str):
    token = st.secrets.get("tushare_token") or os.environ.get("TUSHARE_TOKEN")
    if not token:
        return None
    pro = ts.pro_api(token)
    try:
        df = pro.margin(trade_date=trade_date)
    except Exception:
        return None
    if df is None or df.empty:
        return None

    for col in ["rzmre", "rzche", "rzye"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    net_buy = None
    if {"rzmre", "rzche"}.issubset(df.columns):
        net_buy = df["rzmre"].sum() - df["rzche"].sum()
    elif "rzmre" in df.columns:
        net_buy = df["rzmre"].sum()

    if net_buy is None and "rzye" in df.columns:
        prev_trade_date = _get_prev_trade_date(trade_date, pro)
        if prev_trade_date:
            try:
                prev_df = pro.margin(trade_date=prev_trade_date)
            except Exception:
                prev_df = None
            if prev_df is not None and not prev_df.empty and "rzye" in prev_df.columns:
                prev_df["rzye"] = pd.to_numeric(prev_df["rzye"], errors="coerce")
                curr_total = pd.to_numeric(df["rzye"], errors="coerce").sum()
                prev_total = pd.to_numeric(prev_df["rzye"], errors="coerce").sum()
                if pd.notna(curr_total) and pd.notna(prev_total):
                    net_buy = curr_total - prev_total
    return net_buy


@st.cache_data(ttl='1h')
def get_financing_net_buy_series(days: int = 60) -> pd.DataFrame:
    """
    按日期汇总近 N 个交易日的融资净买入（不落地 CSV）
    """
    token = st.secrets.get("tushare_token") or os.environ.get("TUSHARE_TOKEN")
    if not token:
        return pd.DataFrame()
    pro = ts.pro_api(token)
    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=days * 2)
    try:
        df = pro.margin(start_date=start.strftime("%Y%m%d"), end_date=end.strftime("%Y%m%d"))
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty or "trade_date" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    for col in ["rzmre", "rzche"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if {"rzmre", "rzche"}.issubset(df.columns):
        grouped = df.groupby("trade_date")[["rzmre", "rzche"]].sum()
        grouped["net_buy"] = grouped["rzmre"] - grouped["rzche"]
    elif "rzmre" in df.columns:
        grouped = df.groupby("trade_date")[["rzmre"]].sum()
        grouped["net_buy"] = grouped["rzmre"]
    else:
        return pd.DataFrame()

    grouped = grouped.reset_index()
    grouped = grouped.dropna(subset=["trade_date"])
    grouped = grouped.sort_values("trade_date")
    grouped = grouped.tail(days)
    grouped = grouped.rename(columns={"trade_date": "date", "net_buy": "融资净买入"})
    return grouped[["date", "融资净买入"]]


@st.cache_data(ttl='12h')
def get_gem_pe_series(days: int = 500) -> pd.DataFrame:
    """
    获取创业板市盈率（SZ_GEM）近 N 个交易日数据
    """
    token = st.secrets.get("tushare_token") or os.environ.get("TUSHARE_TOKEN")
    if not token:
        return pd.DataFrame()

    pro = ts.pro_api(token)
    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=days * 4)
    try:
        df = pro.daily_info(
            ts_code="SZ_GEM",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty or "trade_date" not in df.columns or "pe" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["pe"] = pd.to_numeric(df["pe"], errors="coerce")
    df = df.dropna(subset=["trade_date", "pe"])
    if df.empty:
        return pd.DataFrame()

    df = df.sort_values("trade_date").tail(days)
    df = df.rename(columns={"trade_date": "date", "pe": "市盈率"})
    return df[["date", "市盈率"]]


@st.cache_data(ttl='1d')
def get_market_data():
    """获取大盘数据：上证K，上涨家数、下跌家数、情绪指数"""
    def _fetch_index_kline(symbol: str, ts_code: str) -> pd.DataFrame:
        token = st.secrets.get("tushare_token") or os.environ.get("TUSHARE_TOKEN")
        # 优先走 Tushare
        if token:
            try:
                pro = ts.pro_api(token)
                end = pd.Timestamp.now().strftime("%Y%m%d")
                start = (pd.Timestamp.now() - pd.Timedelta(days=200)).strftime("%Y%m%d")
                df = pro.index_daily(ts_code=ts_code, start_date=start, end_date=end)
                if df is not None and not df.empty:
                    df = df.rename(columns={"trade_date": "date", "vol": "volume"})
                    df["date"] = pd.to_datetime(df["date"], errors="coerce")
                    df = df.dropna(subset=["date"]).sort_values("date").tail(100)
                    df.set_index("date", inplace=True)
                    return df
            except Exception:
                pass
        # fallback：AkShare
        try:
            df = ak.stock_zh_index_daily(symbol=symbol).tail(100)
        except Exception:
            return pd.DataFrame()
        if df is None or df.empty:
            return pd.DataFrame()
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        return df

    # 获取上证指数数据
    sh_df = _fetch_index_kline("sh000001", "000001.SH")
    
    # 获取创业板指数数据
    cyb_df = _fetch_index_kline("sz399006", "399006.SZ")

    # 获取科创板指数数据
    kcb_df = _fetch_index_kline("sh000688", "000688.SH")

    market_data = ak.stock_market_activity_legu()

    # 确保datas文件夹存在
    datas_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'datas')
    os.makedirs(datas_dir, exist_ok=True)
    csv_file = os.path.join(datas_dir, 'market_data.csv')

    # 提取market_data中的相关数据
    stat_date = None
    if '统计日期' in market_data['item'].values:
        stat_date = market_data.loc[market_data['item'] == '统计日期', 'value'].values[0]
        # 统一转换为 YYYY/MM/DD 格式
        try:
            stat_date = pd.to_datetime(stat_date).strftime('%Y/%m/%d')
        except:
            stat_date = pd.Timestamp.now().strftime('%Y/%m/%d')
    else:
        stat_date = pd.Timestamp.now().strftime('%Y/%m/%d')

    # 构造一行字典：表头为日期和所有item名，值为对应value
    row = {'日期': stat_date}
    for idx in range(0, 11):
        item = str(market_data.iloc[idx]['item'])
        value = market_data.iloc[idx]['value']
        row[item] = value

    # 使用 Tushare 全市场成交额作为量能口径（单位：千元）
    total_amount = 0.0
    try:
        token = st.secrets.get("tushare_token") or os.environ.get("TUSHARE_TOKEN")
        if token:
            pro = ts.pro_api(token)
            trade_date = pd.to_datetime(stat_date).strftime("%Y%m%d")
            daily = pro.daily(trade_date=trade_date, fields="ts_code,trade_date,amount")
            if daily is not None and not daily.empty and "amount" in daily.columns:
                total_amount = pd.to_numeric(daily["amount"], errors="coerce").sum()
    except Exception:
        total_amount = 0.0

    row['成交额'] = total_amount

    try:
        # 统一表头定义：日期 + 11 个指标
        columns = ['日期'] + [str(market_data.iloc[i]['item']) for i in range(0, 11)]
        if '成交额' not in columns:
            columns.append('成交额')

        # 检查CSV是否存在
        if os.path.exists(csv_file):
            df = pd.read_csv(csv_file)

            # 兼容历史文件：如果没有“日期”列，则根据当前 schema 修正表头
            if '日期' not in df.columns:
                if len(df.columns) == len(columns):
                    df.columns = columns
                else:
                    # 至少确保第一列为日期，避免 KeyError
                    first_cols = list(df.columns)
                    first_cols[0] = '日期'
                    df.columns = first_cols
            if '成交额' not in df.columns:
                df['成交额'] = ""

            # 检查是否已存在该日期，避免重复写入
            if not df[df['日期'] == stat_date].empty:
                idx = df.index[df['日期'] == stat_date][0]
                if (
                    '成交额' in df.columns
                    and (pd.isna(df.at[idx, '成交额']) or str(df.at[idx, '成交额']).strip() == "")
                ):
                    df.at[idx, '成交额'] = row.get('成交额', "")
                df.to_csv(csv_file, index=False)
            else:
                # 新数据插入首行，保持最近日期在上
                df = pd.concat([pd.DataFrame([row], columns=columns), df], ignore_index=True)
                df.to_csv(csv_file, index=False)
        else:
            # 新建数据，表头：日期及item
            df = pd.DataFrame([row], columns=columns)
            df.to_csv(csv_file, index=False)
    except Exception as e:
        print("写入market_data.csv失败:", e)

    _sync_market_activity_to_mysql(stat_date, market_data, total_amount)
    return sh_df, cyb_df, kcb_df, market_data


@st.cache_data(ttl='1d')
def get_all_stocks(base_date=None):
    if base_date is None:
        base_date = datetime.now().date()
    if isinstance(base_date, datetime):
        base_date = base_date.date()
    elif isinstance(base_date, str):
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                base_date = datetime.strptime(base_date, fmt).date()
                break
            except ValueError:
                continue
    if not isinstance(base_date, date):
        return pd.DataFrame()
    trade_date = base_date.strftime("%Y%m%d")
    if not trade_date:
        return pd.DataFrame()

    token = st.secrets.get("tushare_token") or os.environ.get("TUSHARE_TOKEN")
    if not token:
        return pd.DataFrame()

    pro = ts.pro_api(token)
    daily_basic = pro.daily_basic(trade_date=trade_date, fields="ts_code,trade_date,total_mv")
    daily = pro.daily(trade_date=trade_date, fields="ts_code,trade_date,pct_chg,amount")
    if daily_basic is None or daily_basic.empty or daily is None or daily.empty:
        return pd.DataFrame()

    merged = daily_basic.merge(daily, on=["ts_code", "trade_date"], how="left")
    stock_basic = pro.stock_basic(list_status="L", fields="ts_code,name")
    if stock_basic is not None and not stock_basic.empty:
        merged = merged.merge(stock_basic, on="ts_code", how="left")
    merged["code"] = merged["ts_code"].str.split(".").str[0]
    merged = merged.rename(
        columns={
            "pct_chg": "pct",
            "amount": "amount",
            "total_mv": "mkt_cap",
        }
    )
    merged["pct"] = _to_number(merged["pct"])
    merged["amount"] = _to_number(merged["amount"])
    merged["mkt_cap"] = _to_number(merged["mkt_cap"])
    merged["amount"] = merged["amount"] * 1000
    merged["mkt_cap"] = merged["mkt_cap"] * 10000
    merged["name"] = merged.get("name", "").fillna("")
    merged = merged.dropna(subset=["code", "pct", "amount", "mkt_cap"])
    return merged[["code", "name", "pct", "amount", "mkt_cap"]]


# 兼容旧调用
get_top_stocks = get_all_stocks


@st.cache_data(ttl='1d')
def get_longhu_data(date):
    """龙虎榜-游资数据"""
    chairs_set = {
        "毛老板": ['上海东方路','深圳金田路','成都通盈街','北京光华路'],
        "章盟主": ['中信证券杭州延安路','上海江苏路','宁波彩虹北路','上海建国西路','杭州四季路'],
        "赵老哥": ["浙商证券绍兴分","北京阜成路","上海嘉善路"],
        "方新侠": ["朱雀大街","兴业证券陕西分公司"],
        "小鳄鱼": ["南京大钟亭","中投证券南京太平南路","长江证券股份有限公司上海世纪大道","上海兰花路","源深路"],
        "作手新一": ["国泰君安证券股份有限公司南京太平南"],
        "炒股养家": ["上海宛平南路","上海茅台路","西安西大街","海口海德路","上海红宝石路"],
        "陈小群": ["金马路","黄河路"],
        "思明南路": ["东亚前海证券有限责任公司上海","东莞证券股份有限公司湖北分公司"],
        "湖里大道": ["湖里大道"],
        "呼家楼": ["呼家楼"],
        "小棉袄": ["上海证券有限责任公司上海分"],
        "小余余": ["申港证券浙江分公司","甬兴证券青岛同安路"],
        "西湖国贸": ["西湖国贸"],
        "桑田路": ["桑田路"],
        "上塘路": ["上塘路"],
        "章盟主": ["杭州延安路","上海江苏路","宁波彩虹北路","上海建国西路","杭州四季路"],
        "金开大道": ["金开大道"],
        "金田路": ["金田路"],
        "小棉袄": ["上海证券上海分"],
        "珍珠路": ["珍珠路"],
        "上海超短帮": ['上海新闸路','上海银城中路','泰闸路','浦东新区银城中路','东川路'],
        "徐晓": ['上海虹桥路'],
        "劳动路": ['中信证券股份有限公司北京总部'],
    }
    all_business_names = [name for names in chairs_set.values() for name in names]

    lh_yz_df = ak.stock_lhb_hyyyb_em(start_date=date, end_date=date)
    lh_yz_df = lh_yz_df[lh_yz_df['营业部名称'].str.contains('|'.join(all_business_names))]
    
    if not lh_yz_df.empty:
        def get_chair_name(business_name):
            for chair, names in chairs_set.items():
                if any(b in business_name for b in names):
                    return chair
            return None
            
        lh_yz_df['游资'] = lh_yz_df['营业部名称'].apply(get_chair_name)
        
        s = lh_yz_df['买入股票'].str.split(' ').apply(pd.Series, 1).stack().reset_index(level=1, drop=True)
        s.name = '买入股票_new'
        lh_yz_df = lh_yz_df.drop('买入股票', axis=1).join(s)
        lh_yz_df = lh_yz_df.rename(columns={'买入股票_new': '买入股票'}).reset_index()
    
        stocks_df = get_stock_list()
        for i, row in lh_yz_df.iterrows():
            try:
                symbol = stocks_df.loc[stocks_df['zwjc'] == row['买入股票'], 'code'].iloc[0]
                detail_buy_df = ak.stock_lhb_stock_detail_em(symbol=symbol, date=date, flag="买入")
                detail_buy_df = detail_buy_df[(detail_buy_df['交易营业部名称'] == row['营业部名称']) & ~(detail_buy_df['类型'].str.contains('三个交易日'))]

                detail_sell_df = ak.stock_lhb_stock_detail_em(symbol=symbol, date=date, flag="卖出")
                detail_sell_df = detail_sell_df[(detail_sell_df['交易营业部名称'] == row['营业部名称']) & ~(detail_sell_df['类型'].str.contains('三个交易日'))]
                detail_df_yz = pd.concat([detail_buy_df, detail_sell_df], axis=0)
                detail_df_yz.fillna(0, inplace=True)

                if len(detail_df_yz) > 1 and detail_df_yz['类型'].nunique() == 1:
                    index = 1
                else:
                    index = 0
                lh_yz_df.loc[i, '买入金额'] = detail_df_yz['买入金额'].iloc[0]
                lh_yz_df.loc[i, '卖出金额'] = detail_df_yz['卖出金额'].iloc[index]
            except Exception as e:
                pass

        lh_yz_df['净买入'] = lh_yz_df['买入金额'] - lh_yz_df['卖出金额']
        lh_yz_df = lh_yz_df[(lh_yz_df['买入金额'] > 10000000) | (lh_yz_df['卖出金额'] > 10000000)].sort_values('净买入', ascending=False)
    
    return lh_yz_df


@st.cache_data(ttl='1d')
def get_dfcf_concept_boards():
    """获取东方财富概念板块数据"""
    concept_df = ak.stock_board_concept_name_em()
    return concept_df


@st.cache_data(ttl='0.5d')
def get_concept_board_index(concept_name, count=181):
    """获取概念板块指数数据"""
    df = ak.stock_board_concept_hist_em(symbol=concept_name)
    if len(df) > count:
        df = df.tail(count)
    else:
        df = df.tail(len(df))
    df.columns = ['date', 'open', 'close', 'high', 'low', 'rate_pct', 'rate', 'volume_', 'volume', 'wide', 'change']
    df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    return df


