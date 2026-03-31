#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""年报/半年报解析原子能力。"""

from __future__ import annotations

import datetime as dt
import io
import logging
import re
from functools import lru_cache
from typing import Any

import pandas as pd
import requests
import tushare as ts
from PyPDF2 import PdfReader

from infra.config import get_tushare_token
from infra.data_utils import calc_pct_change, convert_to_ts_code
from tools.crawlers import cninfo_announcement_spider, get_cninfo_orgid


logger = logging.getLogger(__name__)

REPORT_TYPE_ANNUAL = "annual"
REPORT_TYPE_HALF_YEAR = "half_year"
MAX_REPORT_LIMIT = 3
CNINFO_PAGE_LIMIT = 3
HTTP_TIMEOUT = 30
TEXT_MIN_LENGTH = 500
TEXT_MIN_CHINESE_CHARS = 200

CNINFO_REPORT_CATEGORIES = {
    REPORT_TYPE_ANNUAL: "category_ndbg_szsh",
    REPORT_TYPE_HALF_YEAR: "category_bndbg_szsh",
}

SECTION_TITLE_ALIASES = {
    "董事会报告": "董事会报告",
    "管理层讨论与分析": "管理层讨论与分析",
    "经营情况讨论与分析": "经营情况讨论与分析",
    "主要业务分析": "主要业务分析",
    "公司业务概要": "公司业务概要",
    "经营分析": "经营分析",
    "经营情况分析": "经营情况分析",
    "公司经营情况": "公司经营情况",
    "业务回顾与分析": "业务回顾与分析",
    "经营情况的讨论与分析": "经营情况的讨论与分析",
}

FINANCIAL_METRICS = [
    {
        "metric_name": "营业收入",
        "unit": "元",
        "sources": [("income", ["total_revenue", "revenue"])],
    },
    {
        "metric_name": "归母净利润",
        "unit": "元",
        "sources": [("income", ["n_income_attr_p"])],
    },
    {
        "metric_name": "扣非归母净利润",
        "unit": "元",
        "sources": [("fina_indicator", ["profit_dedt"])],
    },
    {
        "metric_name": "经营活动产生的现金流量净额",
        "unit": "元",
        "sources": [("cashflow", ["n_cashflow_act"])],
    },
    {
        "metric_name": "毛利率",
        "unit": "%",
        "sources": [("fina_indicator", ["grossprofit_margin"])],
    },
    {
        "metric_name": "净利率",
        "unit": "%",
        "sources": [("fina_indicator", ["netprofit_margin"])],
        "formula": "net_profit_margin",
    },
    {
        "metric_name": "ROE",
        "unit": "%",
        "sources": [("fina_indicator", ["roe_dt"])],
    },
    {
        "metric_name": "资产负债率",
        "unit": "%",
        "sources": [("fina_indicator", ["debt_to_assets"])],
        "formula": "debt_to_assets",
    },
]

PDF_NOISE_PATTERNS = [
    re.compile(r"^\d+$"),
    re.compile(r"^[\d\s/]+$"),
    re.compile(r"^第\d+页共\d+页$"),
]


@lru_cache(maxsize=1)
def _get_tushare_pro() -> Any | None:
    token = get_tushare_token()
    if not token:
        return None
    return ts.pro_api(token)


def _compact_text(value: str) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip()


def _normalize_date(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", str(value or ""))
    if len(text) >= 8:
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return ""


def _normalize_period(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", str(value or ""))
    return text[:8] if len(text) >= 8 else ""


def _report_type_from_title(title: str) -> str:
    compact_title = _compact_text(title)
    if "半年度报告" in compact_title or "半年报" in compact_title:
        return REPORT_TYPE_HALF_YEAR
    if "年度报告" in compact_title or "年报" in compact_title:
        return REPORT_TYPE_ANNUAL
    return ""


def _report_period_from_title(title: str) -> str:
    compact_title = _compact_text(title)
    match = re.search(r"(20\d{2})年", compact_title)
    if not match:
        return ""

    year = match.group(1)
    report_type = _report_type_from_title(compact_title)
    if report_type == REPORT_TYPE_HALF_YEAR:
        return f"{year}0630"
    if report_type == REPORT_TYPE_ANNUAL:
        return f"{year}1231"
    return ""


def _report_type_from_period(report_period: str) -> str:
    if report_period.endswith("1231"):
        return REPORT_TYPE_ANNUAL
    if report_period.endswith("0630"):
        return REPORT_TYPE_HALF_YEAR
    return ""


def _report_priority(title: str) -> int:
    compact_title = _compact_text(title)
    if not compact_title:
        return 999
    if "取消" in compact_title or "英文" in compact_title:
        return 999
    if "摘要" in compact_title:
        return 90
    if "修订" in compact_title or "更正" in compact_title:
        return 1
    if "年度报告" in compact_title or "半年度报告" in compact_title:
        return 0
    if "年报" in compact_title or "半年报" in compact_title:
        return 2
    return 50


def _is_target_report_title(title: str) -> bool:
    report_type = _report_type_from_title(title)
    if not report_type:
        return False
    return _report_priority(title) < 90


def _sort_report_candidates(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            -int(item.get("report_period") or 0),
            item.get("priority", 999),
            -int(re.sub(r"[^0-9]", "", item.get("announcement_date", "")) or 0),
            item.get("report_title", ""),
        ),
    )


def select_target_reports(
    candidates: list[dict[str, Any]],
    report_limit: int = 1,
) -> list[dict[str, Any]]:
    """筛选最新正式年报/半年报。"""
    normalized_limit = max(1, min(int(report_limit or 1), MAX_REPORT_LIMIT))
    filtered_items: list[dict[str, Any]] = []

    for item in candidates or []:
        report_title = str(item.get("report_title", "")).strip()
        if not _is_target_report_title(report_title):
            continue

        normalized_item = dict(item)
        normalized_item["priority"] = int(item.get("priority", _report_priority(report_title)))
        normalized_item["report_type"] = item.get("report_type") or _report_type_from_title(
            report_title
        )
        normalized_item["report_period"] = item.get("report_period") or _report_period_from_title(
            report_title
        )
        if not normalized_item["report_type"] or not normalized_item["report_period"]:
            continue
        filtered_items.append(normalized_item)

    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for item in _sort_report_candidates(filtered_items):
        key = (item["report_type"], item["report_period"])
        if key not in deduped:
            deduped[key] = item

    selected_items = _sort_report_candidates(list(deduped.values()))
    return selected_items[:normalized_limit]


def _cninfo_search_range() -> str:
    today = dt.date.today()
    start_date = today - dt.timedelta(days=365 * 5)
    return f"{start_date:%Y-%m-%d}~{today:%Y-%m-%d}"


def _normalize_cninfo_report_record(record: dict[str, Any], report_type: str) -> dict[str, Any] | None:
    title = str(record.get("announcementTitle", "")).strip()
    if not _is_target_report_title(title):
        return None

    report_period = _report_period_from_title(title)
    normalized_report_type = report_type or _report_type_from_title(title)
    if not report_period or not normalized_report_type:
        return None

    return {
        "report_title": title,
        "report_type": normalized_report_type,
        "report_period": report_period,
        "announcement_date": str(record.get("announcementTime", "")).strip(),
        "pdf_url": str(record.get("adjunctUrl", "")).strip(),
        "priority": _report_priority(title),
        "report_source": "cninfo",
    }


def _fetch_cninfo_report_candidates(ts_code: str) -> tuple[list[dict[str, Any]], list[str]]:
    code = str(ts_code).split(".")[0]
    org_id = get_cninfo_orgid(code)
    stock_arg = f"{code},{org_id}" if org_id else code
    search_range = _cninfo_search_range()
    candidates: list[dict[str, Any]] = []
    errors: list[str] = []

    for report_type, category in CNINFO_REPORT_CATEGORIES.items():
        for page_num in range(1, CNINFO_PAGE_LIMIT + 1):
            try:
                df = cninfo_announcement_spider(
                    pageNum=page_num,
                    tabType="fulltext",
                    stock=stock_arg,
                    category=category,
                    seDate=search_range,
                    use_rules=False,
                )
            except Exception as exc:
                errors.append(f"cninfo:{report_type}:{exc}")
                break

            if df is None or df.empty:
                break

            for record in df.to_dict(orient="records"):
                normalized_record = _normalize_cninfo_report_record(record, report_type)
                if normalized_record:
                    candidates.append(normalized_record)

            if len(df) < 30:
                break

    return candidates, errors


def _normalize_tushare_report_record(record: dict[str, Any]) -> dict[str, Any] | None:
    title = str(
        record.get("title")
        or record.get("ann_title")
        or record.get("name")
        or record.get("file_name")
        or ""
    ).strip()
    report_period = _normalize_period(record.get("end_date")) or _report_period_from_title(title)
    report_type = _report_type_from_period(report_period) or _report_type_from_title(title)
    if not report_type and not _is_target_report_title(title):
        return None

    if not report_type:
        return None

    return {
        "report_title": title or report_period,
        "report_type": report_type,
        "report_period": report_period,
        "announcement_date": _normalize_date(
            record.get("ann_date") or record.get("date") or record.get("pub_time")
        ),
        "pdf_url": str(
            record.get("url") or record.get("pdf_url") or record.get("ann_url") or ""
        ).strip(),
        "priority": _report_priority(title),
        "report_source": "tushare",
    }


def _fetch_tushare_report_candidates(ts_code: str) -> tuple[list[dict[str, Any]], list[str]]:
    pro = _get_tushare_pro()
    if pro is None:
        return [], ["tushare_token_missing"]

    start_date = (dt.date.today() - dt.timedelta(days=365 * 5)).strftime("%Y%m%d")
    end_date = dt.date.today().strftime("%Y%m%d")

    try:
        df = pro.anns_d(ts_code=ts_code, start_date=start_date, end_date=end_date)
    except Exception as exc:
        return [], [f"tushare_anns_d:{exc}"]

    if df is None or df.empty:
        return [], []

    candidates: list[dict[str, Any]] = []
    for record in df.to_dict(orient="records"):
        normalized_record = _normalize_tushare_report_record(record)
        if normalized_record:
            candidates.append(normalized_record)

    return candidates, []


def locate_periodic_reports(ts_code: str, report_limit: int = 1) -> dict[str, Any]:
    """定位最新年报/半年报。"""
    normalized_ts_code = convert_to_ts_code(ts_code)
    tushare_candidates, tushare_errors = _fetch_tushare_report_candidates(normalized_ts_code)
    selected_reports = select_target_reports(tushare_candidates, report_limit=report_limit)

    errors = list(tushare_errors)
    source = "tushare"
    fallback_used = False

    if not selected_reports:
        cninfo_candidates, cninfo_errors = _fetch_cninfo_report_candidates(normalized_ts_code)
        selected_reports = select_target_reports(cninfo_candidates, report_limit=report_limit)
        errors.extend(cninfo_errors)
        source = "cninfo"
        fallback_used = bool(tushare_errors)

    status = "report_located" if selected_reports else "report_not_found"
    if not selected_reports and errors:
        status = "report_locate_failed"

    return {
        "status": status,
        "reports": selected_reports,
        "source": source,
        "fallback_used": fallback_used,
        "errors": errors,
    }


def _dedupe_financial_df(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    view = df.copy()
    sort_columns = []
    ascending = []
    if "end_date" in view.columns:
        sort_columns.append("end_date")
        ascending.append(False)
    if "ann_date" in view.columns:
        sort_columns.append("ann_date")
        ascending.append(False)
    if sort_columns:
        view = view.sort_values(sort_columns, ascending=ascending)
    if "end_date" in view.columns:
        view = view.drop_duplicates(subset=["end_date"], keep="first")
    return view


def fetch_financial_statement_frames(ts_code: str) -> tuple[dict[str, pd.DataFrame], list[str]]:
    """抓取财务指标相关结构化数据。"""
    pro = _get_tushare_pro()
    if pro is None:
        return {}, ["tushare_token_missing"]

    normalized_ts_code = convert_to_ts_code(ts_code)
    dataset_specs = {
        "fina_indicator": "ts_code,ann_date,end_date,profit_dedt,roe_dt,grossprofit_margin,netprofit_margin,debt_to_assets",
        "income": "ts_code,ann_date,end_date,total_revenue,revenue,n_income_attr_p",
        "cashflow": "ts_code,ann_date,end_date,n_cashflow_act",
        "balancesheet": "ts_code,ann_date,end_date,total_assets,total_liab",
    }

    frames: dict[str, pd.DataFrame] = {}
    errors: list[str] = []

    for dataset_name, fields in dataset_specs.items():
        try:
            df = getattr(pro, dataset_name)(ts_code=normalized_ts_code, fields=fields)
            frames[dataset_name] = _dedupe_financial_df(df)
        except Exception as exc:
            frames[dataset_name] = pd.DataFrame()
            errors.append(f"{dataset_name}:{exc}")

    return frames, errors


def _value_from_df(
    df: pd.DataFrame | None,
    report_period: str,
    columns: list[str],
) -> tuple[float | None, str]:
    if df is None or df.empty or "end_date" not in df.columns:
        return None, ""

    matched_df = df[df["end_date"].astype(str) == str(report_period)]
    if matched_df.empty:
        return None, ""

    row = matched_df.iloc[0]
    for column in columns:
        if column not in matched_df.columns:
            continue
        value = pd.to_numeric(row.get(column), errors="coerce")
        if pd.notna(value):
            return float(value), column
    return None, ""


def _derived_metric_value(
    metric_name: str,
    report_period: str,
    frames: dict[str, pd.DataFrame],
) -> tuple[float | None, str]:
    if metric_name == "净利率":
        revenue_value, _ = _value_from_df(frames.get("income"), report_period, ["total_revenue", "revenue"])
        profit_value, _ = _value_from_df(frames.get("income"), report_period, ["n_income_attr_p"])
        if revenue_value and profit_value is not None:
            return float(profit_value / revenue_value * 100), "income.n_income_attr_p/total_revenue"
        return None, ""

    if metric_name == "资产负债率":
        total_assets, _ = _value_from_df(frames.get("balancesheet"), report_period, ["total_assets"])
        total_liab, _ = _value_from_df(frames.get("balancesheet"), report_period, ["total_liab"])
        if total_assets and total_liab is not None:
            return float(total_liab / total_assets * 100), "balancesheet.total_liab/total_assets"
        return None, ""

    return None, ""


def _metric_value(
    metric_spec: dict[str, Any],
    report_period: str,
    frames: dict[str, pd.DataFrame],
) -> tuple[float | None, str]:
    for dataset_name, columns in metric_spec.get("sources", []):
        value, source_column = _value_from_df(frames.get(dataset_name), report_period, columns)
        if value is not None:
            return value, f"{dataset_name}.{source_column}"

    formula_name = metric_spec.get("formula", "")
    if formula_name:
        return _derived_metric_value(metric_spec["metric_name"], report_period, frames)

    return None, ""


def _previous_report_period(report_period: str, report_type: str) -> str:
    if len(report_period) != 8 or not report_period[:4].isdigit():
        return ""
    previous_year = int(report_period[:4]) - 1
    if report_type == REPORT_TYPE_ANNUAL:
        return f"{previous_year}1231"
    if report_type == REPORT_TYPE_HALF_YEAR:
        return f"{previous_year}0630"
    return ""


def calculate_financial_changes(
    report_period: str,
    report_type: str,
    frames: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    """根据报告期计算核心财务指标变化。"""
    previous_period = _previous_report_period(report_period, report_type)
    results: list[dict[str, Any]] = []
    success_count = 0

    for metric_spec in FINANCIAL_METRICS:
        current_value, current_source = _metric_value(metric_spec, report_period, frames)
        previous_value, previous_source = _metric_value(metric_spec, previous_period, frames)

        if current_value is None and previous_value is None:
            status = "missing"
        elif current_value is None:
            status = "missing_current"
        elif previous_value is None:
            status = "missing_previous"
        else:
            status = "ok"
            success_count += 1

        change_value = (
            round(current_value - previous_value, 4)
            if current_value is not None and previous_value is not None
            else None
        )
        change_rate = (
            round(calc_pct_change(current_value, previous_value), 4)
            if current_value is not None and previous_value is not None
            else None
        )

        results.append(
            {
                "metric_name": metric_spec["metric_name"],
                "current_value": current_value,
                "previous_value": previous_value,
                "change_value": change_value,
                "change_rate": change_rate,
                "unit": metric_spec["unit"],
                "source": current_source or previous_source,
                "status": status,
            }
        )

    if success_count == len(FINANCIAL_METRICS):
        status = "financial_loaded"
    elif success_count > 0:
        status = "financial_partially_loaded"
    else:
        status = "financial_failed"

    return {
        "status": status,
        "compare_period": previous_period,
        "items": results,
    }


def get_financial_changes(ts_code: str, report_period: str, report_type: str) -> dict[str, Any]:
    """抓取并计算目标报告的财务指标变化。"""
    frames, errors = fetch_financial_statement_frames(ts_code)
    result = calculate_financial_changes(report_period, report_type, frames)
    result["errors"] = errors
    return result


def _is_pdf_noise_line(compact_line: str) -> bool:
    if not compact_line:
        return True
    for pattern in PDF_NOISE_PATTERNS:
        if pattern.match(compact_line):
            return True
    if "股份有限公司" in compact_line and len(compact_line) <= 30:
        return True
    if (
        ("年度报告" in compact_line or "半年度报告" in compact_line)
        and len(compact_line) <= 24
        and not any(alias in compact_line for alias in SECTION_TITLE_ALIASES)
    ):
        return True
    return False


def _normalize_line_items(text: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for index, raw_line in enumerate(str(text or "").splitlines()):
        normalized_line = re.sub(r"\s+", " ", raw_line).strip()
        compact_line = _compact_text(normalized_line)
        items.append(
            {
                "index": index,
                "raw": raw_line.rstrip(),
                "clean": normalized_line,
                "compact": compact_line,
                "is_noise": _is_pdf_noise_line(compact_line),
            }
        )
    return items


def _extract_heading_core(text: str) -> str:
    value = str(text or "").strip()
    patterns = [
        r"^第[一二三四五六七八九十百零]+章",
        r"^\d+(?:\.\d+){0,3}",
        r"^[（(][一二三四五六七八九十]+[)）]",
        r"^[一二三四五六七八九十]+、",
        r"^[（(]\d+[)）]",
    ]
    for pattern in patterns:
        value = re.sub(pattern, "", value, count=1).strip()
    return value


def _match_section_alias(clean_line: str) -> str:
    heading_core = _compact_text(_extract_heading_core(clean_line))
    if not heading_core or len(heading_core) > 30:
        return ""
    for alias in SECTION_TITLE_ALIASES:
        if heading_core == alias:
            return alias
    return ""


def _infer_heading_level(clean_line: str) -> int:
    stripped_line = str(clean_line or "").strip()
    if re.match(r"^第[一二三四五六七八九十百零]+章", stripped_line):
        return 1
    match = re.match(r"^(\d+(?:\.\d+){0,3})", stripped_line)
    if match:
        return match.group(1).count(".") + 1
    if re.match(r"^[（(][一二三四五六七八九十]+[)）]", stripped_line):
        return 2
    if re.match(r"^[一二三四五六七八九十]+、", stripped_line):
        return 2
    if re.match(r"^[（(]\d+[)）]", stripped_line):
        return 2
    return 99


def _is_general_heading(clean_line: str, compact_line: str) -> bool:
    if not compact_line or len(compact_line) > 40:
        return False
    if any(punctuation in clean_line for punctuation in ("。", "；", "？", "！")):
        return False
    if re.match(r"^第[一二三四五六七八九十百零]+章", clean_line):
        return True
    if re.match(r"^\d+(?:\.\d+){0,3}", clean_line):
        return True
    if re.match(r"^[（(][一二三四五六七八九十]+[)）]", clean_line):
        return True
    if re.match(r"^[一二三四五六七八九十]+、", clean_line):
        return True
    if re.match(r"^[（(]\d+[)）]", clean_line):
        return True
    heading_core = _compact_text(_extract_heading_core(clean_line))
    return heading_core in SECTION_TITLE_ALIASES


def classify_text_quality(text: str) -> str:
    compact_text = _compact_text(text)
    chinese_chars = re.findall(r"[\u4e00-\u9fff]", compact_text)
    if len(compact_text) < TEXT_MIN_LENGTH or len(chinese_chars) < TEXT_MIN_CHINESE_CHARS:
        return "low_quality"
    return "normal"


def extract_management_sections_from_text(text: str) -> dict[str, Any]:
    """从全文中识别经营分析相关章节并提取全文。"""
    line_items = _normalize_line_items(text)
    general_headings: list[dict[str, Any]] = []
    seen_headings: set[tuple[int, str]] = set()

    for item in line_items:
        if item["is_noise"] or not _is_general_heading(item["clean"], item["compact"]):
            continue
        compact_title = _compact_text(_extract_heading_core(item["clean"]))
        level = _infer_heading_level(item["clean"])
        heading_key = (level, compact_title)
        if heading_key in seen_headings:
            continue
        seen_headings.add(heading_key)

        general_headings.append(
            {
                "line_index": item["index"],
                "display_title": item["clean"],
                "compact_title": compact_title,
                "matched_alias": _match_section_alias(item["clean"]),
                "level": level,
            }
        )

    heading_candidates = [
        heading for heading in general_headings if heading.get("matched_alias")
    ]

    if not heading_candidates:
        return {
            "status": "sections_not_found",
            "sections": [],
        }

    quality_flag = classify_text_quality(text)
    sections: list[dict[str, Any]] = []

    for index, heading in enumerate(heading_candidates):
        end_line_index = len(line_items) - 1
        boundary_status = "boundary_unstable"

        current_heading_position = general_headings.index(heading)
        for next_heading in general_headings[current_heading_position + 1 :]:
            if next_heading["level"] <= heading["level"]:
                end_line_index = next_heading["line_index"] - 1
                boundary_status = "boundary_stable"
                break

        segment_items = line_items[heading["line_index"] : end_line_index + 1]
        raw_lines = [item["raw"] for item in segment_items if str(item["raw"]).strip()]
        cleaned_lines = [
            item["clean"]
            for item in segment_items
            if not item["is_noise"] and item["clean"]
        ]

        full_text_raw = "\n".join(raw_lines).strip()
        full_text_cleaned = "\n".join(cleaned_lines).strip()
        section_status = "extracted" if full_text_cleaned else "empty"

        sections.append(
            {
                "section_title": heading["display_title"],
                "section_order": len(sections) + 1,
                "full_text_raw": full_text_raw,
                "full_text_cleaned": full_text_cleaned,
                "quality_flag": quality_flag,
                "boundary_status": boundary_status,
                "status": section_status,
            }
        )

    if all(section["status"] == "extracted" for section in sections):
        status = "sections_extracted"
    else:
        status = "sections_partially_extracted"

    return {
        "status": status,
        "sections": sections,
    }


def extract_pdf_text(pdf_url: str) -> dict[str, Any]:
    """下载并提取 PDF 全文。"""
    if not str(pdf_url or "").strip():
        return {
            "status": "text_extraction_failed",
            "text": "",
            "quality_flag": "low_quality",
            "error": "missing_pdf_url",
        }

    try:
        response = requests.get(pdf_url, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
    except Exception as exc:
        return {
            "status": "text_extraction_failed",
            "text": "",
            "quality_flag": "low_quality",
            "error": f"pdf_download_failed:{exc}",
        }

    try:
        reader = PdfReader(io.BytesIO(response.content))
        pages = []
        for page in reader.pages:
            pages.append(page.extract_text() or "")
        text = "\n".join(pages).strip()
    except Exception as exc:
        return {
            "status": "text_extraction_failed",
            "text": "",
            "quality_flag": "low_quality",
            "error": f"pdf_parse_failed:{exc}",
        }

    if not text:
        return {
            "status": "text_extraction_failed",
            "text": "",
            "quality_flag": "low_quality",
            "error": "empty_pdf_text",
        }

    quality_flag = classify_text_quality(text)
    status = "text_extracted" if quality_flag == "normal" else "text_low_quality"
    return {
        "status": status,
        "text": text,
        "quality_flag": quality_flag,
        "error": "",
    }


def parse_management_sections_from_pdf(pdf_url: str) -> dict[str, Any]:
    """解析 PDF 并提取经营分析相关章节。"""
    text_result = extract_pdf_text(pdf_url)
    if not text_result.get("text"):
        return {
            "text_status": text_result["status"],
            "sections_status": "sections_not_found",
            "full_text": "",
            "quality_flag": text_result.get("quality_flag", "low_quality"),
            "management_sections": [],
            "errors": [text_result.get("error", "")] if text_result.get("error") else [],
        }

    section_result = extract_management_sections_from_text(text_result["text"])
    return {
        "text_status": text_result["status"],
        "sections_status": section_result["status"],
        "full_text": text_result["text"],
        "quality_flag": text_result["quality_flag"],
        "management_sections": section_result["sections"],
        "errors": [text_result.get("error", "")] if text_result.get("error") else [],
    }


def get_stock_identity(ts_code: str) -> dict[str, str]:
    """获取股票基础身份信息。"""
    normalized_ts_code = convert_to_ts_code(ts_code)
    default_payload = {
        "ts_code": normalized_ts_code,
        "code": normalized_ts_code.split(".")[0],
        "name": normalized_ts_code,
    }
    pro = _get_tushare_pro()
    if pro is None:
        return default_payload

    try:
        df = pro.stock_basic(ts_code=normalized_ts_code, fields="ts_code,symbol,name")
    except Exception:
        return default_payload

    if df is None or df.empty:
        return default_payload

    row = df.iloc[0]
    return {
        "ts_code": str(row.get("ts_code", normalized_ts_code)),
        "code": str(row.get("symbol", default_payload["code"])),
        "name": str(row.get("name", normalized_ts_code)),
    }
