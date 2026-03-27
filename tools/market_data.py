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
from infra.config import get_tushare_token
from infra.data_utils import to_number
from .daily_basic_storage import get_daily_basic_smart, save_daily_basic_async


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


@st.cache_data(ttl="1h")
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
        df = pro.margin(
            start_date=start.strftime("%Y%m%d"), end_date=end.strftime("%Y%m%d")
        )
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


@st.cache_data(ttl="12h")
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

    if (
        df is None
        or df.empty
        or "trade_date" not in df.columns
        or "pe" not in df.columns
    ):
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


# 使用 tools/utils.py 中的 get_tushare_token 函数


@st.cache_data(ttl="1h")
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
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        return df

    # 获取上证指数数据
    sh_df = _fetch_index_kline("sh000001", "000001.SH")

    # 获取创业板指数数据
    cyb_df = _fetch_index_kline("sz399006", "399006.SZ")

    # 获取科创板指数数据
    kcb_df = _fetch_index_kline("sh000688", "000688.SH")

    market_data = ak.stock_market_activity_legu()

    # 确保datas文件夹存在
    datas_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "datas")
    os.makedirs(datas_dir, exist_ok=True)
    csv_file = os.path.join(datas_dir, "market_data.csv")

    # 提取market_data中的相关数据
    stat_date = None
    if "统计日期" in market_data["item"].values:
        stat_date = market_data.loc[market_data["item"] == "统计日期", "value"].values[
            0
        ]
        # 统一转换为 YYYY/MM/DD 格式
        try:
            stat_date = pd.to_datetime(stat_date).strftime("%Y/%m/%d")
        except:
            stat_date = pd.Timestamp.now().strftime("%Y/%m/%d")
    else:
        stat_date = pd.Timestamp.now().strftime("%Y/%m/%d")

    # 构造一行字典：表头为日期和所有item名，值为对应value
    row = {"日期": stat_date}
    for idx in range(0, 11):
        item = str(market_data.iloc[idx]["item"])
        value = market_data.iloc[idx]["value"]
        row[item] = value

    # 使用 Tushare 计算市场数据（成交额、上涨家数、下跌家数）
    total_amount = 0.0
    up_count = None
    down_count = None

    try:
        token = get_tushare_token()
        if token:
            pro = ts.pro_api(token)
            trade_date = pd.to_datetime(stat_date).strftime("%Y%m%d")

            # 获取当日所有股票的日线数据（包含成交额和涨跌幅）
            daily = pro.daily(
                trade_date=trade_date, fields="ts_code,trade_date,amount,pct_chg"
            )

            if daily is not None and not daily.empty:
                # 计算成交额（千元）
                if "amount" in daily.columns:
                    total_amount = pd.to_numeric(daily["amount"], errors="coerce").sum()

                # 计算上涨和下跌家数
                if "pct_chg" in daily.columns:
                    daily["pct_chg"] = pd.to_numeric(daily["pct_chg"], errors="coerce")
                    up_count = int((daily["pct_chg"] > 0).sum())
                    down_count = int((daily["pct_chg"] < 0).sum())
            else:
                print(f"Tushare daily 返回空数据: trade_date={trade_date}")
        else:
            print("未找到 TUSHARE_TOKEN，数据将置为0")
    except Exception as e:
        print(f"获取Tushare市场数据失败: {e}")
        total_amount = 0.0

    row["成交额"] = total_amount

    # 如果akshare的market_data中没有上涨/下跌数据，使用Tushare计算的数据
    if (
        "上涨" not in row
        or pd.isna(row.get("上涨"))
        or str(row.get("上涨")).strip() == ""
    ) and up_count is not None:
        row["上涨"] = up_count
    if (
        "下跌" not in row
        or pd.isna(row.get("下跌"))
        or str(row.get("下跌")).strip() == ""
    ) and down_count is not None:
        row["下跌"] = down_count

    try:
        # 统一表头定义：日期 + 11 个指标 + 成交额 + 上涨 + 下跌
        columns = ["日期"] + [str(market_data.iloc[i]["item"]) for i in range(0, 11)]
        if "成交额" not in columns:
            columns.append("成交额")
        if "上涨" not in columns:
            columns.append("上涨")
        if "下跌" not in columns:
            columns.append("下跌")

        # 检查CSV是否存在
        if os.path.exists(csv_file):
            df = pd.read_csv(csv_file)

            # 兼容历史文件：如果没有"日期"列，则根据当前 schema 修正表头
            if "日期" not in df.columns:
                if len(df.columns) == len(columns):
                    df.columns = columns
                else:
                    # 至少确保第一列为日期，避免 KeyError
                    first_cols = list(df.columns)
                    first_cols[0] = "日期"
                    df.columns = first_cols

            # 确保必要列存在
            for col in ["成交额", "上涨", "下跌"]:
                if col not in df.columns:
                    df[col] = ""

            # 检查是否已存在该日期，避免重复写入
            if not df[df["日期"] == stat_date].empty:
                idx = df.index[df["日期"] == stat_date][0]

                # 更新成交额（如果缺失）
                if "成交额" in df.columns and (
                    pd.isna(df.at[idx, "成交额"])
                    or str(df.at[idx, "成交额"]).strip() == ""
                ):
                    df.at[idx, "成交额"] = row.get("成交额", "")

                # 更新上涨家数（如果缺失）
                if "上涨" in df.columns and (
                    pd.isna(df.at[idx, "上涨"]) or str(df.at[idx, "上涨"]).strip() == ""
                ):
                    df.at[idx, "上涨"] = row.get("上涨", "")

                # 更新下跌家数（如果缺失）
                if "下跌" in df.columns and (
                    pd.isna(df.at[idx, "下跌"]) or str(df.at[idx, "下跌"]).strip() == ""
                ):
                    df.at[idx, "下跌"] = row.get("下跌", "")

                df.to_csv(csv_file, index=False)
            else:
                # 新数据插入首行，保持最近日期在上
                df = pd.concat(
                    [pd.DataFrame([row], columns=columns), df], ignore_index=True
                )
                df.to_csv(csv_file, index=False)
        else:
            # 新建数据，表头：日期及item
            df = pd.DataFrame([row], columns=columns)
            df.to_csv(csv_file, index=False)
    except Exception as e:
        print("写入market_data.csv失败:", e)

    return sh_df, cyb_df, kcb_df, market_data


@st.cache_data(ttl="30m")
def get_market_history(days: int = 30) -> pd.DataFrame:
    """
    获取市场历史数据。
    从本地 datas/market_data.csv 读取。
    返回列：日期、上涨、下跌、涨停、跌停、活跃度、成交额
    """
    safe_days = max(1, int(days))

    # 从 CSV 读取
    try:
        csv_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "datas", "market_data.csv"
        )
        if not os.path.exists(csv_file):
            return pd.DataFrame()

        df = pd.read_csv(csv_file)
        if df is None or df.empty:
            return pd.DataFrame()

        # 兼容列名乱码或不同版本：按列序做兜底映射
        cols = list(df.columns)
        if len(cols) < 13:
            return pd.DataFrame()

        out = pd.DataFrame()
        out["日期"] = df[cols[0]]
        out["上涨"] = df[cols[1]]
        out["涨停"] = df[cols[2]]
        out["下跌"] = df[cols[5]]
        out["跌停"] = df[cols[6]]
        out["活跃度"] = df[cols[11]]
        out["成交额"] = df[cols[12]]

        out["日期"] = pd.to_datetime(out["日期"], errors="coerce")
        out = out.dropna(subset=["日期"]).sort_values("日期").tail(safe_days)
        for col in ["上涨", "下跌", "涨停", "跌停", "成交额"]:
            out[col] = pd.to_numeric(out[col], errors="coerce")
        out["活跃度"] = pd.to_numeric(
            out["活跃度"].astype(str).str.replace("%", "", regex=False), errors="coerce"
        )
        return out
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl="1d")
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
    
    daily_basic = get_daily_basic_smart(
        trade_date=trade_date,
        fields=["ts_code", "trade_date", "total_mv"],
        use_cache=True
    )
    
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
    merged["pct"] = to_number(merged["pct"])
    merged["amount"] = to_number(merged["amount"])
    merged["mkt_cap"] = to_number(merged["mkt_cap"])
    merged["amount"] = merged["amount"] / 100000
    merged["mkt_cap"] = merged["mkt_cap"] / 10000
    merged["name"] = merged.get("name", "").fillna("")
    merged = merged.dropna(subset=["code", "pct", "amount", "mkt_cap"])
    return merged[["code", "name", "pct", "amount", "mkt_cap"]]


@st.cache_data(ttl="1d")
def get_longhu_data(date):
    """龙虎榜-游资数据"""
    chairs_set = {
        "毛老板": ["上海东方路", "深圳金田路", "成都通盈街", "北京光华路"],
        "章盟主": [
            "中信证券杭州延安路",
            "上海江苏路",
            "宁波彩虹北路",
            "上海建国西路",
            "杭州四季路",
        ],
        "赵老哥": ["浙商证券绍兴分", "北京阜成路", "上海嘉善路"],
        "方新侠": ["朱雀大街", "兴业证券陕西分公司"],
        "小鳄鱼": [
            "南京大钟亭",
            "中投证券南京太平南路",
            "长江证券股份有限公司上海世纪大道",
            "上海兰花路",
            "源深路",
        ],
        "作手新一": ["国泰君安证券股份有限公司南京太平南"],
        "炒股养家": [
            "上海宛平南路",
            "上海茅台路",
            "西安西大街",
            "海口海德路",
            "上海红宝石路",
        ],
        "陈小群": ["金马路", "黄河路"],
        "思明南路": ["东亚前海证券有限责任公司上海", "东莞证券股份有限公司湖北分公司"],
        "湖里大道": ["湖里大道"],
        "呼家楼": ["呼家楼"],
        "小棉袄": ["上海证券有限责任公司上海分"],
        "小余余": ["申港证券浙江分公司", "甬兴证券青岛同安路"],
        "西湖国贸": ["西湖国贸"],
        "桑田路": ["桑田路"],
        "上塘路": ["上塘路"],
        "章盟主": [
            "杭州延安路",
            "上海江苏路",
            "宁波彩虹北路",
            "上海建国西路",
            "杭州四季路",
        ],
        "金开大道": ["金开大道"],
        "金田路": ["金田路"],
        "小棉袄": ["上海证券上海分"],
        "珍珠路": ["珍珠路"],
        "上海超短帮": [
            "上海新闸路",
            "上海银城中路",
            "泰闸路",
            "浦东新区银城中路",
            "东川路",
        ],
        "徐晓": ["上海虹桥路"],
        "劳动路": ["中信证券股份有限公司北京总部"],
    }
    all_business_names = [name for names in chairs_set.values() for name in names]

    lh_yz_df = ak.stock_lhb_hyyyb_em(start_date=date, end_date=date)
    lh_yz_df = lh_yz_df[
        lh_yz_df["营业部名称"].str.contains("|".join(all_business_names))
    ]

    if not lh_yz_df.empty:

        def get_chair_name(business_name):
            for chair, names in chairs_set.items():
                if any(b in business_name for b in names):
                    return chair
            return None

        lh_yz_df["游资"] = lh_yz_df["营业部名称"].apply(get_chair_name)

        s = (
            lh_yz_df["买入股票"]
            .str.split(" ")
            .apply(pd.Series, 1)
            .stack()
            .reset_index(level=1, drop=True)
        )
        s.name = "买入股票_new"
        lh_yz_df = lh_yz_df.drop("买入股票", axis=1).join(s)
        lh_yz_df = lh_yz_df.rename(columns={"买入股票_new": "买入股票"}).reset_index()

        stocks_df = get_stock_list()
        for i, row in lh_yz_df.iterrows():
            try:
                symbol = stocks_df.loc[
                    stocks_df["zwjc"] == row["买入股票"], "code"
                ].iloc[0]
                detail_buy_df = ak.stock_lhb_stock_detail_em(
                    symbol=symbol, date=date, flag="买入"
                )
                detail_buy_df = detail_buy_df[
                    (detail_buy_df["交易营业部名称"] == row["营业部名称"])
                    & ~(detail_buy_df["类型"].str.contains("三个交易日"))
                ]

                detail_sell_df = ak.stock_lhb_stock_detail_em(
                    symbol=symbol, date=date, flag="卖出"
                )
                detail_sell_df = detail_sell_df[
                    (detail_sell_df["交易营业部名称"] == row["营业部名称"])
                    & ~(detail_sell_df["类型"].str.contains("三个交易日"))
                ]
                detail_df_yz = pd.concat([detail_buy_df, detail_sell_df], axis=0)
                detail_df_yz.fillna(0, inplace=True)

                if len(detail_df_yz) > 1 and detail_df_yz["类型"].nunique() == 1:
                    index = 1
                else:
                    index = 0
                lh_yz_df.loc[i, "买入金额"] = detail_df_yz["买入金额"].iloc[0]
                lh_yz_df.loc[i, "卖出金额"] = detail_df_yz["卖出金额"].iloc[index]
            except Exception as e:
                pass

        lh_yz_df["净买入"] = lh_yz_df["买入金额"] - lh_yz_df["卖出金额"]
        lh_yz_df = lh_yz_df[
            (lh_yz_df["买入金额"] > 10000000) | (lh_yz_df["卖出金额"] > 10000000)
        ].sort_values("净买入", ascending=False)

    return lh_yz_df


@st.cache_data(ttl="1d")
def get_dfcf_concept_boards():
    """获取东方财富概念板块数据"""
    concept_df = ak.stock_board_concept_name_em()
    return concept_df


@st.cache_data(ttl="0.5d")
def get_concept_board_index(concept_name, count=181):
    """获取概念板块指数数据"""
    df = ak.stock_board_concept_hist_em(symbol=concept_name)
    if len(df) > count:
        df = df.tail(count)
    else:
        df = df.tail(len(df))
    df.columns = [
        "date",
        "open",
        "close",
        "high",
        "low",
        "rate_pct",
        "rate",
        "volume_",
        "volume",
        "wide",
        "change",
    ]
    df = df[["date", "open", "high", "low", "close", "volume"]]
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    return df


@st.cache_data(ttl="1h")
def get_market_daily_stats(days: int = 30) -> pd.DataFrame:
    """
    从 Tushare 获取市场每日统计数据（成交额、涨跌家数、涨停跌停数）
    返回列：日期、成交额、上涨、下跌、涨停、跌停
    """
    token = st.secrets.get("tushare_token") or os.environ.get("TUSHARE_TOKEN")
    if not token:
        return pd.DataFrame()

    pro = ts.pro_api(token)
    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=days * 2)

    try:
        # 获取每日指标数据（包含成交额、涨跌家数等）
        df = pro.daily_info(
            start_date=start.strftime("%Y%m%d"), end_date=end.strftime("%Y%m%d")
        )
    except Exception as e:
        print(f"获取市场每日统计失败: {e}")
        return pd.DataFrame()

    if df is None or df.empty or "trade_date" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df.dropna(subset=["trade_date"])

    # 选择并重命名列
    result = pd.DataFrame()
    result["日期"] = df["trade_date"]

    # 成交额（如果存在）
    if "total_mv" in df.columns:
        result["成交额"] = pd.to_numeric(df["total_mv"], errors="coerce")
    elif "turnover" in df.columns:
        result["成交额"] = pd.to_numeric(df["turnover"], errors="coerce")
    else:
        result["成交额"] = None

    # 涨跌家数
    if "up_num" in df.columns:
        result["上涨"] = pd.to_numeric(df["up_num"], errors="coerce")
    else:
        result["上涨"] = None

    if "down_num" in df.columns:
        result["下跌"] = pd.to_numeric(df["down_num"], errors="coerce")
    else:
        result["下跌"] = None

    # 涨停跌停数（daily_info 可能没有，需要尝试其他接口）
    result["涨停"] = None
    result["跌停"] = None

    # 尝试从 limit_list 获取涨停跌停数据
    try:
        limit_df = pro.limit_list(
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            limit_type="U",
        )
        if limit_df is not None and not limit_df.empty:
            limit_df["trade_date"] = pd.to_datetime(
                limit_df["trade_date"], errors="coerce"
            )
            zt_counts = limit_df.groupby("trade_date").size().reset_index(name="涨停")
            result = result.merge(
                zt_counts, left_on="日期", right_on="trade_date", how="left"
            )
            result = result.drop(columns=["trade_date"], errors="ignore")
    except Exception:
        pass

    try:
        limit_df = pro.limit_list(
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            limit_type="D",
        )
        if limit_df is not None and not limit_df.empty:
            limit_df["trade_date"] = pd.to_datetime(
                limit_df["trade_date"], errors="coerce"
            )
            dt_counts = limit_df.groupby("trade_date").size().reset_index(name="跌停")
            result = result.merge(
                dt_counts, left_on="日期", right_on="trade_date", how="left"
            )
            result = result.drop(columns=["trade_date"], errors="ignore")
    except Exception:
        pass

    # 计算活跃度（上涨家数占比）
    result["活跃度"] = None
    mask = (result["上涨"].notna()) & (result["下跌"].notna())
    if mask.any():
        total = result.loc[mask, "上涨"] + result.loc[mask, "下跌"]
        result.loc[mask, "活跃度"] = (result.loc[mask, "上涨"] / total * 100).round(2)

    result = result.sort_values("日期").tail(days)
    return result.reset_index(drop=True)


@st.cache_data(ttl="1h")
def get_market_amount_series(days: int = 30) -> pd.DataFrame:
    """
    获取市场成交额序列（从 Tushare daily 接口汇总）
    返回列：日期、成交额（千元）
    """
    token = st.secrets.get("tushare_token") or os.environ.get("TUSHARE_TOKEN")
    if not token:
        return pd.DataFrame()

    pro = ts.pro_api(token)
    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=days * 2)

    try:
        df = pro.daily(
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            fields="trade_date,amount",
        )
    except Exception as e:
        print(f"获取成交额数据失败: {e}")
        return pd.DataFrame()

    if df is None or df.empty or "trade_date" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["trade_date", "amount"])

    # 按日期汇总
    daily_amount = df.groupby("trade_date")["amount"].sum().reset_index()
    daily_amount.columns = ["日期", "成交额"]
    daily_amount = daily_amount.sort_values("日期").tail(days)

    return daily_amount.reset_index(drop=True)
