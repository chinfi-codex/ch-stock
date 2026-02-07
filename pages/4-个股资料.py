# -*- coding: utf-8 -*-
import streamlit as st
import os
import pandas as pd
import datetime
import plotly.express as px
import tushare as ts
import json
import requests
from urllib.parse import urlparse

from tools import (
    get_llm_response,
    get_ak_price_df,
    plotK,
    cninfo_announcement_spider,
    get_stock_list,
    clean_filename,
)

st.set_page_config(layout="wide")

_TS_PRO = None


def _get_ts_pro():
    global _TS_PRO
    if _TS_PRO is None:
        token = st.secrets.get("tushare_token") or os.environ.get("TUSHARE_TOKEN")
        if not token:
            st.error("Missing TUSHARE_TOKEN; cannot fetch TuShare data")
            return None
        ts.set_token(token)
        _TS_PRO = ts.pro_api()
    return _TS_PRO


def _to_ts_code(code):
    code = str(code).strip().upper()
    if "." in code:
        return code.replace(".SS", ".SH")
    if code.startswith(("SH", "SZ", "BJ")) and len(code) == 8:
        return f"{code[2:]}.{code[:2]}"
    if len(code) == 6 and code.isdigit():
        if code.startswith(("0", "3")):
            suffix = "SZ"
        elif code.startswith(("6", "9")):
            suffix = "SH"
        elif code.startswith("8"):
            suffix = "BJ"
        else:
            suffix = "SZ"
        return f"{code}.{suffix}"
    return code


def _build_company_info(basic_df, company_df):
    rows = []

    def _append(label, value):
        if value is None or (isinstance(value, float) and pd.isna(value)) or value == "":
            return
        rows.append({"item": label, "value": value})

    industry = None
    if basic_df is not None and not basic_df.empty:
        row = basic_df.iloc[0]
        industry = row.get("industry")

    if company_df is not None and not company_df.empty:
        row = company_df.iloc[0]
        _append("公司介绍", row.get("introduction"))
        _append("主营业务", row.get("main_business"))
        _append("行业信息", industry)
        _append("办公地址", row.get("office"))
        _append("网站", row.get("website"))

    return pd.DataFrame(rows)


def _extract_business_scope(company_df):
    if company_df is None or company_df.empty:
        return ""
    row = company_df.iloc[0]
    for col in ["business_scope", "main_business"]:
        if col in company_df.columns and pd.notna(row.get(col)):
            return str(row.get(col))
    return ""


def _normalize_mainbz(df):
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    col_map = {}
    if "end_date" in df.columns:
        col_map["end_date"] = "报告日期"
    if "type" in df.columns:
        col_map["type"] = "分类类型"
    if "bz_item" in df.columns:
        col_map["bz_item"] = "主营构成"
    if "bz_sales" in df.columns:
        col_map["bz_sales"] = "主营收入"
    if "bz_profit" in df.columns:
        col_map["bz_profit"] = "主营利润"
    if "bz_cost" in df.columns:
        col_map["bz_cost"] = "主营成本"
    df = df.rename(columns=col_map)
    if "分类类型" in df.columns:
        df["分类类型"] = df["分类类型"].replace({"P": "按产品分类", "D": "按地区分类"})
    return df


def _pick_first_column(df, candidates):
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _format_percent(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    try:
        num = float(val)
    except Exception:
        return str(val)
    if abs(num) <= 1:
        num *= 100
    return f"{num:.2f}%"


def _normalize_top10_holders(df):
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    name_col = _pick_first_column(df, ["holder_name", "holder", "name"])
    ratio_col = _pick_first_column(df, ["hold_ratio", "ratio", "holdratio"])
    if not name_col or not ratio_col:
        return pd.DataFrame()
    view = df[[name_col, ratio_col]].copy()
    view.columns = ["股东名称", "持股比例"]
    view["持股比例"] = view["持股比例"].apply(_format_percent)
    return view


def _safe_pro_call(pro, api_name, **kwargs):
    if pro is None:
        return pd.DataFrame()
    try:
        api = getattr(pro, api_name, None)
        if callable(api):
            return api(**kwargs)
        return pro.query(api_name, **kwargs)
    except Exception:
        return pd.DataFrame()


def _df_to_records(df):
    if df is None or df.empty:
        return []
    try:
        json_str = df.to_json(orient="records", date_format="iso", force_ascii=False)
        return json.loads(json_str)
    except Exception:
        return df.astype(str).to_dict(orient="records")


def _pick_first_value(row, candidates):
    for name in candidates:
        if name in row and pd.notna(row[name]):
            return row[name]
    return ""


def _resolve_stock_name(stock_code, stock_list_df):
    if stock_list_df is None or stock_list_df.empty:
        return str(stock_code)
    code = str(stock_code).strip()
    match = stock_list_df[stock_list_df["code"] == code]
    if match.empty and len(code) >= 6:
        match = stock_list_df[stock_list_df["code"] == code[:6]]
    if match.empty:
        return code
    row = match.iloc[0].to_dict()
    return _pick_first_value(row, ["zwjc", "name", "secName", "code"]) or code


def _build_output_dir(stock_name, stock_code):
    safe_name = clean_filename(f"{stock_name}-{stock_code}")
    base_dir = os.path.abspath(os.path.join(os.getcwd(), "datas", "stocks", safe_name))
    os.makedirs(base_dir, exist_ok=True)
    return base_dir


def _guess_extension(url):
    try:
        path = urlparse(url).path
        ext = os.path.splitext(path)[1].lower()
        if ext and len(ext) <= 5:
            return ext
    except Exception:
        pass
    return ".pdf"


def _download_file(url, target_path, timeout=20):
    if not url:
        return {"status": "missing", "path": target_path}
    if os.path.exists(target_path):
        return {"status": "exists", "path": target_path}
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        response = requests.get(url, headers=headers, stream=True, timeout=timeout)
        response.raise_for_status()
        with open(target_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 128):
                if chunk:
                    f.write(chunk)
        return {"status": "downloaded", "path": target_path}
    except Exception:
        return {"status": "failed", "path": target_path}


def _collect_report_items(fundamental_data):
    items = []
    if not fundamental_data:
        return items

    relation_info = fundamental_data.get("relation_info")
    if relation_info is not None and not relation_info.empty:
        for _, row in relation_info.iterrows():
            items.append(
                {
                    "type": "调研",
                    "date": row.get("announcementTime", ""),
                    "title": row.get("announcementTitle", ""),
                    "url": row.get("adjunctUrl", ""),
                }
            )

    research_reports = fundamental_data.get("research_reports")
    if research_reports is not None and not research_reports.empty:
        url_col = _pick_first_column(research_reports, ["report_url", "url", "pdf_url"])
        title_col = _pick_first_column(research_reports, ["title", "report_title", "report_name"])
        date_col = _pick_first_column(research_reports, ["report_date", "pub_date", "ann_date", "end_date"])
        if url_col:
            for _, row in research_reports.iterrows():
                items.append(
                    {
                        "type": "研报",
                        "date": row.get(date_col, "") if date_col else "",
                        "title": row.get(title_col, "") if title_col else "",
                        "url": row.get(url_col, ""),
                    }
                )

    financial_reports = fundamental_data.get("financial_reports")
    if financial_reports is not None and not financial_reports.empty:
        for _, row in financial_reports.iterrows():
            title = str(row.get("announcementTitle", ""))
            if not title:
                continue
            if "摘要" in title:
                continue
            report_type = None
            if "半年度报告" in title or "中期报告" in title or "中报" in title:
                report_type = "中报"
            elif "年度报告" in title or "年报" in title:
                report_type = "年报"
            if not report_type:
                continue
            items.append(
                {
                    "type": report_type,
                    "date": row.get("announcementTime", ""),
                    "title": title,
                    "url": row.get("adjunctUrl", ""),
                }
            )
    return items


def _download_reports(report_items, output_dir):
    results = []
    if not report_items:
        return results

    seen_urls = set()
    for idx, item in enumerate(report_items, 1):
        url = str(item.get("url", "")).strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)

        title = str(item.get("title", "")).strip()
        date_val = item.get("date", "")
        date_str = ""
        if pd.notna(date_val) and date_val != "":
            try:
                date_str = pd.to_datetime(date_val).strftime("%Y%m%d")
            except Exception:
                date_str = str(date_val)

        if item.get("type") == "研报":
            prefix = "research"
        elif item.get("type") == "调研":
            prefix = "survey"
        elif item.get("type") == "年报":
            prefix = "annual"
        elif item.get("type") == "中报":
            prefix = "interim"
        else:
            prefix = "report"
        file_base = clean_filename(f"{prefix}_{date_str}_{title}") if title else f"{prefix}_{date_str}_{idx}"
        file_base = file_base.strip("_") or f"{prefix}_{idx}"
        file_name = f"{file_base}{_guess_extension(url)}"
        target_path = os.path.join(output_dir, file_name)
        result = _download_file(url, target_path)
        results.append(
            {
                "type": item.get("type", ""),
                "title": title,
                "url": url,
                "status": result["status"],
                "path": result["path"],
            }
        )
    return results


def _fetch_tushare_download_data(stock_code):
    pro = _get_ts_pro()
    if pro is None:
        return None

    ts_code = _to_ts_code(stock_code)

    basic_df = _safe_pro_call(
        pro,
        "stock_basic",
        ts_code=ts_code,
        fields="ts_code,symbol,name,area,industry,market,list_date",
    )
    company_df = _safe_pro_call(pro, "stock_company", ts_code=ts_code)

    managers_df = _safe_pro_call(pro, "stk_managers", ts_code=ts_code)
    salary_df = _safe_pro_call(pro, "tmt_salary", ts_code=ts_code)
    rewards_df = _safe_pro_call(pro, "stk_rewards", ts_code=ts_code)

    list_date = None
    if basic_df is not None and not basic_df.empty:
        list_date = basic_df.iloc[0].get("list_date")
    if not list_date or pd.isna(list_date):
        list_date = "19900101"

    daily_df = _safe_pro_call(pro, "daily", ts_code=ts_code, start_date=str(list_date))
    if daily_df is not None and not daily_df.empty and "trade_date" in daily_df.columns:
        daily_df = daily_df.sort_values("trade_date")

    eight_years_ago = (pd.Timestamp.now() - pd.DateOffset(years=8)).strftime("%Y%m%d")
    fina_indicator_df = _safe_pro_call(pro, "fina_indicator", ts_code=ts_code, start_date=eight_years_ago)

    mainbz_product_df = _safe_pro_call(pro, "fina_mainbz", ts_code=ts_code, type="P")
    mainbz_area_df = _safe_pro_call(pro, "fina_mainbz", ts_code=ts_code, type="D")

    top10_holders_df = _safe_pro_call(pro, "top10_holders", ts_code=ts_code)
    top10_floatholders_df = _safe_pro_call(pro, "top10_floatholders", ts_code=ts_code)
    holder_number_df = _safe_pro_call(pro, "stk_holdernumber", ts_code=ts_code)
    holder_trade_df = _safe_pro_call(pro, "stk_holdertrade", ts_code=ts_code)
    share_float_df = _safe_pro_call(pro, "share_float", ts_code=ts_code)

    return {
        "ts_code": ts_code,
        "basic_info": {
            "stock_basic": _df_to_records(basic_df),
            "stock_company": _df_to_records(company_df),
        },
        "management": {
            "managers": _df_to_records(managers_df),
            "tmt_salary": _df_to_records(salary_df),
            "stk_rewards": _df_to_records(rewards_df),
        },
        "daily_price": _df_to_records(daily_df),
        "financial_indicators": _df_to_records(fina_indicator_df),
        "main_business": {
            "product": _df_to_records(mainbz_product_df),
            "area": _df_to_records(mainbz_area_df),
        },
        "shareholders": {
            "top10_holders": _df_to_records(top10_holders_df),
            "top10_floatholders": _df_to_records(top10_floatholders_df),
            "holder_number": _df_to_records(holder_number_df),
            "holder_trade": _df_to_records(holder_trade_df),
            "share_float": _df_to_records(share_float_df),
        },
    }


def download_stock_package(stock_code, stock_name, fundamental_data):
    output_dir = _build_output_dir(stock_name, stock_code)

    tushare_payload = _fetch_tushare_download_data(stock_code)
    if tushare_payload is None:
        return {"ok": False, "message": "TuShare 接口不可用，无法下载"}

    data_payload = {
        "meta": {
            "stock_code": str(stock_code),
            "stock_name": str(stock_name),
            "ts_code": tushare_payload.get("ts_code"),
            "generated_at": datetime.datetime.now().isoformat(),
        },
        "basic_info": tushare_payload.get("basic_info"),
        "management": tushare_payload.get("management"),
        "daily_price": tushare_payload.get("daily_price"),
        "financial_indicators": tushare_payload.get("financial_indicators"),
        "main_business": tushare_payload.get("main_business"),
        "shareholders": tushare_payload.get("shareholders"),
    }

    json_path = os.path.join(output_dir, "tushare_data.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data_payload, f, ensure_ascii=False, indent=2)

    report_items = _collect_report_items(fundamental_data)
    download_results = _download_reports(report_items, output_dir)

    return {
        "ok": True,
        "output_dir": output_dir,
        "json_path": json_path,
        "reports": download_results,
    }


def fundamental_agent(stock_code):
    """
    基本面分析Agent - 负责获取公司基本信息、主营构成、股东信息等
    """
    try:
        pro = _get_ts_pro()
        if pro is None:
            return None

        ts_code = _to_ts_code(stock_code)

        basic_df = _safe_pro_call(
            pro,
            "stock_basic",
            ts_code=ts_code,
            fields="ts_code,symbol,name,area,industry,market,list_date",
        )
        company_df = _safe_pro_call(pro, "stock_company", ts_code=ts_code)
        company_info = _build_company_info(basic_df, company_df)
        business_scope = _extract_business_scope(company_df)

        business_df = _safe_pro_call(pro, "fina_mainbz", ts_code=ts_code, type="P")
        business_df = _normalize_mainbz(business_df)
        if business_df is not None and not business_df.empty and "报告日期" in business_df.columns:
            business_df = business_df.sort_values("报告日期", ascending=False)

        holders_df = _safe_pro_call(pro, "top10_holders", ts_code=ts_code)
        if holders_df is not None and not holders_df.empty and "end_date" in holders_df.columns:
            latest_end = holders_df["end_date"].max()
            holders_df = holders_df[holders_df["end_date"] == latest_end].copy()
        holders_df = _normalize_top10_holders(holders_df)

        today = datetime.date.today()
        thirty_days_ago = (today - datetime.timedelta(days=300)).strftime("%Y-%m-%d")
        today_str = today.strftime("%Y-%m-%d")
        query_date = f"{thirty_days_ago}~{today_str}"

        cninfo_info = stock_list_df[stock_list_df["code"] == stock_code]
        if not cninfo_info.empty:
            stock_query = f"{cninfo_info.iloc[0]['code']},{cninfo_info.iloc[0]['orgId']}"
            announcements_df = pd.DataFrame()
            for page in range(1, 4):
                temp_df = cninfo_announcement_spider(
                    pageNum=page,
                    tabType="relation",
                    stock=stock_query,
                    seDate=query_date,
                )
                announcements_df = pd.concat([announcements_df, temp_df], ignore_index=True)

            financial_reports_df = pd.DataFrame()
            for page in range(1, 4):
                temp_df = cninfo_announcement_spider(
                    pageNum=page,
                    tabType="fulltext",
                    category="category_yjygjxz_szsh;category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh",
                    stock=stock_query,
                    seDate=query_date,
                )
                financial_reports_df = pd.concat([financial_reports_df, temp_df], ignore_index=True)
        else:
            announcements_df = pd.DataFrame()
            financial_reports_df = pd.DataFrame()

        research_reports_df = _safe_pro_call(pro, "report", ts_code=ts_code)
        if research_reports_df is not None and not research_reports_df.empty:
            date_col = _pick_first_column(research_reports_df, ["report_date", "pub_date", "ann_date", "end_date"])
            if date_col:
                research_reports_df[date_col] = pd.to_datetime(research_reports_df[date_col], errors="coerce")
                research_reports_df = research_reports_df.sort_values(by=date_col, ascending=False).head(10)
        else:
            research_reports_df = pd.DataFrame()

        return {
            "company_info": company_info,
            "business_scope": business_scope,
            "business_composition": business_df,
            "major_holders": holders_df,
            "relation_info": announcements_df,
            "financial_reports": financial_reports_df,
            "research_reports": research_reports_df,
        }
    except Exception as e:
        st.error(f"基本面Agent获取数据失败: {str(e)}")
        return {}


@st.cache_data(ttl="1d")
def financial_agent(stock_code):
    """
    财务分析Agent - 负责获取和分析利润表数据
    """
    try:
        pro = _get_ts_pro()
        if pro is None:
            return {"profit_data": pd.DataFrame()}
        ts_code = _to_ts_code(stock_code)

        profit_df = _safe_pro_call(pro, "income", ts_code=ts_code)
        if profit_df is None or profit_df.empty:
            return {"profit_data": pd.DataFrame()}

        profit_df = profit_df.copy()
        date_col = "end_date" if "end_date" in profit_df.columns else None
        if date_col is None:
            for col in ["report_date", "ann_date", "f_ann_date"]:
                if col in profit_df.columns:
                    date_col = col
                    break
        if date_col is None:
            return {"profit_data": profit_df}

        profit_df[date_col] = pd.to_datetime(profit_df[date_col], errors="coerce")
        profit_df = profit_df.dropna(subset=[date_col]).sort_values(date_col)

        five_years_ago = pd.Timestamp.now() - pd.DateOffset(years=5)
        profit_df = profit_df[profit_df[date_col] >= five_years_ago]

        key_metrics = ["total_revenue", "operate_profit", "total_profit", "n_income"]
        metrics_names = {
            "total_revenue": "营业总收入",
            "operate_profit": "营业利润",
            "total_profit": "利润总额",
            "n_income": "净利润",
        }

        for metric in key_metrics:
            if metric in profit_df.columns:
                profit_df[f"{metric}_YOY"] = profit_df[metric].pct_change(4) * 100
                profit_df[f"{metric}_QOQ"] = profit_df[metric].pct_change() * 100

        return {
            "profit_data": profit_df,
            "metrics_names": metrics_names,
        }
    except Exception as e:
        st.error(f"财务Agent获取数据失败: {str(e)}")
        return {"profit_data": pd.DataFrame()}


def technical_agent(stock_code):
    """
    技术分析Agent - 负责K线图分析和技术指标计算
    """
    try:
        df = get_ak_price_df(stock_code, count=721)
        return {
            "price_data": df,
        }
    except Exception as e:
        st.error(f"技术面Agent获取数据失败: {str(e)}")
        return None


def summary_output(stock_code, fundamental_data, technical_data, financial_data=None):
    if not fundamental_data:
        st.error("基本面数据为空，请检查 TuShare 接口与 Token 配置")
        return
    col1, col, col2 = st.columns([6, 1, 3])
    with col1:
        st.subheader("📳 基本面")
        company_info = fundamental_data.get("company_info")
        business_scope = fundamental_data.get("business_scope")
        business_info = fundamental_data.get("business_composition")
        holders_info = fundamental_data.get("major_holders")
        relation_info = fundamental_data.get("relation_info")
        research_reports = fundamental_data.get("research_reports")
        financial_reports = fundamental_data.get("financial_reports")

        if company_info is not None and not company_info.empty:
            st.markdown("##### 公司信息")
            st.dataframe(company_info, hide_index=True)

        if business_scope:
            st.markdown("##### 经营范围")
            st.write(business_scope)

        if holders_info is not None and not holders_info.empty:
            st.markdown("##### 十大股东")
            st.dataframe(holders_info, hide_index=True)

        if business_info is not None and not business_info.empty:
            st.markdown("##### 主营构成（按产品）")
            st.dataframe(business_info, hide_index=True)

        # 主营构成可视化
        try:
            if (
                business_info is not None
                and not business_info.empty
                and {"报告日期", "分类类型", "主营构成", "主营收入"}.issubset(business_info.columns)
            ):
                product_df = business_info[business_info["分类类型"] == "按产品分类"].copy()
                if not product_df.empty:
                    date_str = product_df["报告日期"].astype(str)
                    product_df["年份"] = date_str.str[:4]
                    is_year_end = date_str.str.endswith("-12-31") | date_str.str.endswith("1231")
                    year_end_df = product_df[is_year_end].copy()
                    plot_base = year_end_df if not year_end_df.empty else product_df
                    plot_base["主营收入"] = pd.to_numeric(plot_base["主营收入"], errors="coerce")
                    plot_base = plot_base.dropna(subset=["主营收入", "年份", "主营构成"])
                    if not plot_base.empty:
                        plot_base = (
                            plot_base.groupby(["年份", "主营构成"], as_index=False)["主营收入"].sum()
                        )
                        plot_base = plot_base.sort_values(["年份", "主营构成"])
                        latest_year = plot_base["年份"].max()
                        latest_df = plot_base[plot_base["年份"] == latest_year]
                        top_items = (
                            latest_df.sort_values("主营收入", ascending=False)["主营构成"]
                            .dropna()
                            .unique()
                            .tolist()
                        )
                        default_items = top_items[:5] if top_items else []
                        selected_items = st.multiselect(
                            "选择产品分类（按年度趋势）",
                            options=sorted(plot_base["主营构成"].dropna().unique().tolist()),
                            default=default_items,
                        )
                        plot_df = plot_base
                        if selected_items:
                            plot_df = plot_df[plot_df["主营构成"].isin(selected_items)]
                        fig = px.bar(
                            plot_df,
                            x="年份",
                            y="主营收入",
                            color="主营构成",
                            barmode="group",
                            title="各年度各产品分类主营收入趋势",
                            labels={"年份": "年份", "主营收入": "主营收入", "主营构成": "产品分类"},
                        )
                        st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.info(f"主营构成可视化失败: {e}")

        st.markdown("##### 📢 年报")
        if financial_reports is not None and not financial_reports.empty:
            for _, row in financial_reports.iterrows():
                st.markdown(
                    f"{row['announcementTime']} - {row['announcementTitle']}:{row['adjunctUrl']}"
                )
        else:
            st.write("无数据")
        st.divider()

        st.markdown("##### 📢 机构调研")
        if relation_info is not None and not relation_info.empty:
            for _, row in relation_info.iterrows():
                st.markdown(
                    f"{row['announcementTime']} - {row['announcementTitle']}:{row['adjunctUrl']}"
                )
        else:
            st.write("无调研")
        st.divider()

        if research_reports is not None and not research_reports.empty:
            date_col = _pick_first_column(research_reports, ["report_date", "pub_date", "ann_date", "end_date"])
            org_col = _pick_first_column(research_reports, ["org_name", "org", "institution", "org_name"])
            title_col = _pick_first_column(research_reports, ["title", "report_title", "report_name"])
            url_col = _pick_first_column(research_reports, ["report_url", "url", "pdf_url"])
            if date_col and title_col:
                for _, row in research_reports.iterrows():
                    date_val = row.get(date_col, "")
                    org_val = row.get(org_col, "") if org_col else ""
                    title_val = row.get(title_col, "")
                    url_val = row.get(url_col, "") if url_col else ""
                    suffix = f" {org_val}" if org_val else ""
                    st.markdown(f"{date_val} - {title_val}{suffix}:{url_val}")
            else:
                st.dataframe(research_reports, hide_index=True)
        else:
            st.write("无研报")

    with col2:
        st.subheader("📈 技术面")
        st.write("日K线")
        plotK(
            technical_data["price_data"].tail(241),
            k="d",
            plot_type="candle",
            ma_line=None,
            fail_zt=False,
            container=st,
        )
        st.write("周K线")
        plotK(
            technical_data["price_data"].tail(361),
            k="w",
            plot_type="candle",
            ma_line=None,
            fail_zt=False,
            container=st,
        )
        st.write("月K线")
        plotK(
            technical_data["price_data"],
            k="m",
            plot_type="candle",
            ma_line=None,
            fail_zt=False,
            container=st,
        )


stock_list_df = get_stock_list()
stock_query = st.text_input("输入股票")
if stock_query:
    if not stock_query.isdigit() or (
        len(stock_query) == 6 and stock_query[:1].isalpha() and stock_query[1:].isdigit()
    ):
        stock_info = stock_list_df[stock_list_df["zwjc"] == stock_query]
        if not stock_info.empty:
            stock_code = stock_info.iloc[0]["code"]
        else:
            stock_code = stock_query
    else:
        stock_code = stock_query
    fundamental_data = fundamental_agent(stock_code)
    technical_data = technical_agent(stock_code)
    summary_output(stock_code, fundamental_data, technical_data)

    stock_name = _resolve_stock_name(stock_code, stock_list_df)
    if st.button(f"下载 {stock_name} ({stock_code}) 资料", type="primary", use_container_width=True):
        with st.spinner("正在下载该股票的资料，请稍候..."):
            result = download_stock_package(stock_code, stock_name, fundamental_data)
        if result.get("ok"):
            st.success(f"下载完成，已保存到：{result.get('output_dir')}")
        else:
            st.error(result.get("message", "下载失败"))
