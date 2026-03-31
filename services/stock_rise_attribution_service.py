#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""上涨归因验证页业务流程编排。"""

from __future__ import annotations

import datetime as dt
import hashlib
from typing import Any, Dict, Iterable, List, Optional

import streamlit as st

from infra.config import get_zsxq_cookie, get_zsxq_group_ids
from infra.web_scraper import scrape_with_jina_reader
from services.annual_report_service import get_annual_report_parser_result
from tools.ai_analysis import (
    build_evidence_brief_prompt,
    build_stock_rise_attribution_prompt,
    run_ai_analysis,
)
from tools.crawlers import (
    cninfo_announcement_spider,
    collect_p5w_interaction,
    fetch_topics_by_date,
    get_cninfo_orgid,
)
from tools.search_aggregation import search_web_content
from tools.utils import get_stock_list


SOURCE_CNINFO = "cninfo_announcement"
SOURCE_RESEARCH = "cninfo_research"
SOURCE_P5W = "p5w_interaction"
SOURCE_SEARCH = "search_aggregation"
SOURCE_ZSXQ = "zsxq"
SOURCE_REPORT_EARNINGS = "report_earnings"

SOURCE_LABELS = {
    SOURCE_CNINFO: "巨潮公告",
    SOURCE_RESEARCH: "机构调研",
    SOURCE_P5W: "全景网互动问答",
    SOURCE_SEARCH: "内容搜索聚合",
    SOURCE_ZSXQ: "知识星球",
    SOURCE_REPORT_EARNINGS: "报告业绩",
}

DEFAULT_SELECTED_SOURCES = (
    SOURCE_CNINFO,
    SOURCE_RESEARCH,
    SOURCE_P5W,
    SOURCE_REPORT_EARNINGS,
)
SOURCE_ORDER = (
    SOURCE_CNINFO,
    SOURCE_RESEARCH,
    SOURCE_P5W,
    SOURCE_SEARCH,
    SOURCE_ZSXQ,
    SOURCE_REPORT_EARNINGS,
)
DEFAULT_WINDOW_DAYS = 5
CNINFO_WINDOW_DAYS = 30
RESEARCH_WINDOW_DAYS = 90
SUMMARY_CHAR_LIMIT = 300
READER_CONTENT_LIMIT = 6000
REPORT_EARNINGS_LIMIT = 1
EXCLUDED_CNINFO_EARNINGS_CATEGORIES = {
    "category_yjygjxz_szsh",
    "category_ndbg_szsh",
    "category_bndbg_szsh",
    "category_yjdbg_szsh",
    "category_sjdbg_szsh",
}
PREFERRED_REPORT_METRICS = (
    "营业收入",
    "归母净利润",
    "扣非归母净利润",
    "经营活动产生的现金流量净额",
)


def _normalize_source_status(
    source: str,
    status: str,
    count: int = 0,
    error: str = "",
) -> Dict[str, Any]:
    return {
        "source": source,
        "source_label": SOURCE_LABELS.get(source, source),
        "status": status,
        "count": int(count or 0),
        "error": error.strip(),
    }


def _build_debug_entry(
    stage: str,
    *,
    message: str = "",
    data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "stage": str(stage or "").strip(),
        "message": str(message or "").strip(),
        "data": data or {},
    }


def _build_source_debug(
    source: str,
    *,
    window_dates: Optional[List[str]] = None,
    steps: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return {
        "source": source,
        "source_label": SOURCE_LABELS.get(source, source),
        "window_dates": list(window_dates or []),
        "steps": list(steps or []),
    }


def _date_range_strings(days: int = 5) -> List[str]:
    today = dt.date.today()
    start_date = today - dt.timedelta(days=max(days - 1, 0))
    return [
        (start_date + dt.timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(days)
    ]


def _window_days_for_source(source: str) -> int:
    if source == SOURCE_CNINFO:
        return CNINFO_WINDOW_DAYS
    if source == SOURCE_RESEARCH:
        return RESEARCH_WINDOW_DAYS
    return DEFAULT_WINDOW_DAYS


def _window_dates_for_source(source: str) -> List[str]:
    if source == SOURCE_REPORT_EARNINGS:
        return []
    return _date_range_strings(days=_window_days_for_source(source))


def _window_description() -> str:
    return (
        f"巨潮公告最近{CNINFO_WINDOW_DAYS}天；"
        f"机构调研最近{RESEARCH_WINDOW_DAYS}天；"
        f"互动问答、搜索聚合、知识星球最近{DEFAULT_WINDOW_DAYS}天；"
        f"报告业绩按默认最新{REPORT_EARNINGS_LIMIT}份"
    )


def _date_in_window(date_str: str, window_dates: List[str]) -> bool:
    return bool(date_str) and date_str in set(window_dates)


def _find_stock_matches(query: str, limit: int = 10) -> List[Dict[str, str]]:
    query = (query or "").strip()
    if not query:
        return []

    stock_df = get_stock_list()
    if stock_df is None or stock_df.empty:
        return []

    view = stock_df.copy()
    view["code"] = view["code"].astype(str).str.strip()
    view["name"] = view["zwjc"].astype(str).str.strip()
    view["pinyin"] = view["pinyin"].astype(str).str.strip().str.lower()
    view = view[view["category"].astype(str).str.contains("A股", na=False)]

    query_lower = query.lower()
    if query.isdigit():
        mask = view["code"].str.contains(query, na=False)
    else:
        mask = (
            view["name"].str.contains(query, case=False, na=False, regex=False)
            | view["pinyin"].str.contains(query_lower, na=False, regex=False)
        )

    result = view.loc[mask, ["code", "name", "orgId", "pinyin"]].sort_values("code")
    if limit > 0:
        result = result.head(limit)
    return result.to_dict(orient="records")


@st.cache_data(ttl="1d")
def search_stock_candidates(query: str, limit: int = 10) -> List[Dict[str, str]]:
    """搜索股票候选。"""
    return _find_stock_matches(query, limit=limit)


def _sort_evidence_items(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    level_order = {"strong": 0, "auxiliary": 1}

    def sort_key(item: Dict[str, Any]) -> Any:
        date_value = item.get("date") or ""
        has_date = 0 if date_value else 1
        return (
            has_date,
            -(int(date_value.replace("-", "")) if date_value else 0),
            level_order.get(item.get("evidence_level"), 9),
        )

    return sorted(items, key=sort_key)


def _dedupe_evidence_items(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    deduped: List[Dict[str, Any]] = []

    for item in items:
        key = (
            item.get("source", ""),
            item.get("url", ""),
            item.get("title", ""),
            item.get("date", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _truncate_summary(text: str, limit: int = SUMMARY_CHAR_LIMIT) -> str:
    cleaned = " ".join(str(text or "").split()).strip()
    if not cleaned:
        return ""
    return cleaned[:limit]


def _format_change_rate(change_rate: Any) -> str:
    if change_rate is None:
        return ""
    try:
        value = float(change_rate)
    except (TypeError, ValueError):
        return ""
    return f"{value:+.1f}%"


def _build_report_earnings_summary(report: Dict[str, Any]) -> str:
    metrics = report.get("financial_changes") or []
    by_name = {item.get("metric_name"): item for item in metrics}

    summary_parts: List[str] = []
    for metric_name in PREFERRED_REPORT_METRICS:
        metric = by_name.get(metric_name)
        if not metric or metric.get("status") != "ok":
            continue
        change_text = _format_change_rate(metric.get("change_rate"))
        if not change_text:
            continue
        summary_parts.append(f"{metric_name}{change_text}")

    if not summary_parts:
        for metric in metrics:
            if metric.get("status") != "ok":
                continue
            change_text = _format_change_rate(metric.get("change_rate"))
            if not change_text:
                continue
            summary_parts.append(f"{metric.get('metric_name', '指标')}{change_text}")
            if len(summary_parts) >= 4:
                break

    prefix = str(report.get("report_title") or "定期报告").strip()
    if not summary_parts:
        return _truncate_summary(f"{prefix}已披露，但核心财务指标变化数据暂不完整。")
    return _truncate_summary(f"{prefix}：{'；'.join(summary_parts)}。")


def _filter_cninfo_earnings_categories(
    records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return [
        record
        for record in records
        if str(record.get("category", "")).strip() not in EXCLUDED_CNINFO_EARNINGS_CATEGORIES
    ]


def _summarize_with_ai(
    *,
    source_label: str,
    title: str,
    content: str,
    cache_key: str,
    limit: int = SUMMARY_CHAR_LIMIT,
) -> str:
    prompt = build_evidence_brief_prompt(
        source_label=source_label,
        title=title,
        content=content[:READER_CONTENT_LIMIT],
        max_chars=limit,
    )
    try:
        result = run_ai_analysis(prompt, cache_key=cache_key, timeout=120)
    except Exception:
        return ""
    if not result or "AI分析暂时不可用" in result:
        return ""
    return _truncate_summary(result, limit=limit)


def _build_strong_evidence_summary(
    *,
    source_label: str,
    title: str,
    url: str,
    fallback_summary: str,
) -> tuple[str, List[str]]:
    base_summary = _truncate_summary(fallback_summary or title)
    if not url:
        return base_summary, ["标题级证据", "原文缺失"]

    reader_result = scrape_with_jina_reader(
        url,
        title=title,
        output_dir="",
        save_to_file=False,
    )
    if not reader_result.get("success"):
        return base_summary, ["标题级证据", "原文读取失败"]

    reader_content = _truncate_summary(reader_result.get("content", ""), limit=READER_CONTENT_LIMIT)
    if not reader_content:
        return base_summary, ["标题级证据", "原文读取失败"]

    cache_key = hashlib.md5(f"{source_label}|{title}|{url}".encode("utf-8")).hexdigest()
    summarized = _summarize_with_ai(
        source_label=source_label,
        title=title,
        content=reader_content,
        cache_key=f"evidence_brief_{cache_key}",
    )
    if not summarized:
        return base_summary, ["标题级证据", "原文读取失败"]
    return summarized, ["原文摘要"]


def _fetch_cninfo_records(
    *,
    code: str,
    org_id: str,
    tab_type: str,
    window_dates: List[str],
    max_pages: int = 5,
) -> List[Dict[str, Any]]:
    stock_arg = f"{code},{org_id}" if org_id else code
    se_date = f"{window_dates[0]}~{window_dates[-1]}"
    all_records: List[Dict[str, Any]] = []

    for page_num in range(1, max_pages + 1):
        df = cninfo_announcement_spider(
            pageNum=page_num,
            tabType=tab_type,
            stock=stock_arg,
            seDate=se_date,
        )
        if df is None or df.empty:
            break
        records = df.to_dict(orient="records")
        all_records.extend(records)
        if len(records) < 30:
            break

    return all_records


def _normalize_cninfo_items(
    records: List[Dict[str, Any]],
    *,
    source: str,
    base_tags: List[str],
) -> List[Dict[str, Any]]:
    items = []
    source_label = SOURCE_LABELS[source]
    for record in records:
        title = str(record.get("announcementTitle", "")).strip()
        event_date = str(record.get("announcementTime", "")).strip()
        url = str(record.get("adjunctUrl", "")).strip()
        summary, extra_tags = _build_strong_evidence_summary(
            source_label=source_label,
            title=title or source_label,
            url=url,
            fallback_summary=title or source_label,
        )
        items.append(
            {
                "source": source,
                "source_label": source_label,
                "date": event_date,
                "title": title or source_label,
                "summary": summary,
                "url": url,
                "evidence_level": "strong",
                "tags": base_tags + extra_tags,
                "raw": record,
            }
        )
    return items


def _normalize_report_earnings_item(report: Dict[str, Any], status: str) -> Dict[str, Any]:
    announcement_date = str(report.get("announcement_date", "")).strip()
    title = str(report.get("report_title", "")).strip() or SOURCE_LABELS[SOURCE_REPORT_EARNINGS]
    summary = _build_report_earnings_summary(report)
    return {
        "source": SOURCE_REPORT_EARNINGS,
        "source_label": SOURCE_LABELS[SOURCE_REPORT_EARNINGS],
        "date": announcement_date,
        "title": title,
        "summary": summary,
        "url": str(report.get("pdf_url", "")).strip(),
        "evidence_level": "strong",
        "tags": ["报告业绩", report.get("report_type", ""), status],
        "raw": {
            "report_period": report.get("report_period", ""),
            "announcement_date": announcement_date,
            "financial_changes": report.get("financial_changes", []),
        },
    }


def _normalize_p5w_items(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for record in records:
        title = str(record.get("title", "")).strip() or "互动问答"
        summary = _truncate_summary(record.get("summary", "") or "仅有提问或回复不足")
        items.append(
            {
                "source": SOURCE_P5W,
                "source_label": SOURCE_LABELS[SOURCE_P5W],
                "date": str(record.get("date", "")).strip(),
                "title": title,
                "summary": summary,
                "url": str(record.get("url", "")).strip(),
                "evidence_level": "strong",
                "tags": ["互动问答"],
                "raw": record,
            }
        )
    return items


def _topic_matches_stock(topic: Dict[str, Any], stock_code: str, stock_name: str) -> bool:
    text = " ".join(
        [
            str(topic.get("title", "")),
            str(topic.get("content", "")),
        ]
    )
    return stock_code in text or stock_name in text


def _normalize_zsxq_items(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for record in records:
        summary = _truncate_summary(record.get("content", ""))
        items.append(
            {
                "source": SOURCE_ZSXQ,
                "source_label": SOURCE_LABELS[SOURCE_ZSXQ],
                "date": str(record.get("created_at", "")).strip()[:10],
                "title": str(record.get("title", "")).strip() or "知识星球主题",
                "summary": summary or "知识星球提及",
                "url": str(record.get("url", "")).strip(),
                "evidence_level": "auxiliary",
                "tags": ["知识星球"],
                "raw": record,
            }
        )
    return items


def _search_queries(stock_name: str, stock_code: str) -> List[str]:
    del stock_code
    return [
        f"{stock_name} 上涨原因溯源，根据一周内高置信度证据/新闻，直接给出结论，100字以内。"
    ]


def _run_cninfo(stock_identity: Dict[str, str], window_dates: List[str]) -> Dict[str, Any]:
    debug_steps: List[Dict[str, Any]] = []
    org_id = stock_identity.get("org_id") or get_cninfo_orgid(stock_identity["code"])
    debug_steps.append(
        _build_debug_entry(
            "resolve_org_id",
            data={
                "requested_code": stock_identity["code"],
                "input_org_id": stock_identity.get("org_id", ""),
                "resolved_org_id": org_id or "",
            },
        )
    )
    records = _fetch_cninfo_records(
        code=stock_identity["code"],
        org_id=org_id or "",
        tab_type="fulltext",
        window_dates=window_dates,
    )
    debug_steps.append(
        _build_debug_entry(
            "fetch_records",
            data={
                "tab_type": "fulltext",
                "record_count": len(records),
                "titles": [
                    str(record.get("announcementTitle", "")).strip()
                    for record in records[:10]
                ],
            },
        )
    )
    original_count = len(records)
    records = _filter_cninfo_earnings_categories(records)
    debug_steps.append(
        _build_debug_entry(
            "filter_earnings_categories",
            data={
                "before_count": original_count,
                "after_count": len(records),
                "filtered_count": original_count - len(records),
            },
        )
    )
    items = _normalize_cninfo_items(records, source=SOURCE_CNINFO, base_tags=["公告"])
    debug_steps.append(
        _build_debug_entry(
            "normalize_items",
            data={
                "item_count": len(items),
                "titles": [item.get("title", "") for item in items[:10]],
            },
        )
    )
    if not items:
        return {
            "status": _normalize_source_status(SOURCE_CNINFO, "empty", count=0),
            "items": [],
            "debug": _build_source_debug(
                SOURCE_CNINFO,
                window_dates=window_dates,
                steps=debug_steps,
            ),
        }
    return {
        "status": _normalize_source_status(SOURCE_CNINFO, "available", count=len(items)),
        "items": items,
        "debug": _build_source_debug(
            SOURCE_CNINFO,
            window_dates=window_dates,
            steps=debug_steps,
        ),
    }


def _run_research(stock_identity: Dict[str, str], window_dates: List[str]) -> Dict[str, Any]:
    debug_steps: List[Dict[str, Any]] = []
    org_id = stock_identity.get("org_id") or get_cninfo_orgid(stock_identity["code"])
    debug_steps.append(
        _build_debug_entry(
            "resolve_org_id",
            data={
                "requested_code": stock_identity["code"],
                "input_org_id": stock_identity.get("org_id", ""),
                "resolved_org_id": org_id or "",
            },
        )
    )
    records = _fetch_cninfo_records(
        code=stock_identity["code"],
        org_id=org_id or "",
        tab_type="relation",
        window_dates=window_dates,
    )
    debug_steps.append(
        _build_debug_entry(
            "fetch_records",
            data={
                "tab_type": "relation",
                "record_count": len(records),
                "titles": [
                    str(record.get("announcementTitle", "")).strip()
                    for record in records[:10]
                ],
            },
        )
    )
    items = _normalize_cninfo_items(records, source=SOURCE_RESEARCH, base_tags=["机构调研"])
    debug_steps.append(
        _build_debug_entry(
            "normalize_items",
            data={
                "item_count": len(items),
                "titles": [item.get("title", "") for item in items[:10]],
            },
        )
    )
    if not items:
        return {
            "status": _normalize_source_status(SOURCE_RESEARCH, "empty", count=0),
            "items": [],
            "debug": _build_source_debug(
                SOURCE_RESEARCH,
                window_dates=window_dates,
                steps=debug_steps,
            ),
        }
    return {
        "status": _normalize_source_status(
            SOURCE_RESEARCH, "available", count=len(items)
        ),
        "items": items,
        "debug": _build_source_debug(
            SOURCE_RESEARCH,
            window_dates=window_dates,
            steps=debug_steps,
        ),
    }


def _run_p5w(stock_identity: Dict[str, str], window_dates: List[str]) -> Dict[str, Any]:
    raw_items: List[Dict[str, Any]] = []
    errors: List[str] = []
    debug_steps: List[Dict[str, Any]] = []
    for date_str in window_dates:
        result = collect_p5w_interaction(date=date_str, company_code=stock_identity["code"])
        fetched_items = result.get("items") or []
        debug_steps.append(
            _build_debug_entry(
                "fetch_daily_interaction",
                data={
                    "date": date_str,
                    "error": result.get("error", ""),
                    "raw_count": len(fetched_items),
                    "titles": [
                        str(item.get("title", "")).strip()
                        for item in fetched_items[:10]
                    ],
                },
            )
        )
        if result.get("error"):
            errors.append(f"{date_str}: {result['error']}")
            continue
        raw_items.extend(fetched_items)

    items = _normalize_p5w_items(raw_items)
    debug_steps.append(
        _build_debug_entry(
            "normalize_items",
            data={
                "raw_count": len(raw_items),
                "item_count": len(items),
                "titles": [item.get("title", "") for item in items[:10]],
            },
        )
    )
    if items:
        return {
            "status": _normalize_source_status(SOURCE_P5W, "available", count=len(items)),
            "items": items,
            "debug": _build_source_debug(
                SOURCE_P5W,
                window_dates=window_dates,
                steps=debug_steps,
            ),
        }

    status = "failed" if errors and len(errors) == len(window_dates) else "empty"
    error_text = " | ".join(errors) if status == "failed" else ""
    return {
        "status": _normalize_source_status(SOURCE_P5W, status, count=0, error=error_text),
        "items": [],
        "debug": _build_source_debug(
            SOURCE_P5W,
            window_dates=window_dates,
            steps=debug_steps,
        ),
    }


def _run_search(stock_identity: Dict[str, str], window_dates: List[str]) -> Dict[str, Any]:
    debug_steps: List[Dict[str, Any]] = []
    query = _search_queries(stock_identity["name"], stock_identity["code"])[0]
    result = search_web_content(
        query,
        limit=1,
        fetch_reader_summary=True,
    )
    result_status = result.get("status", "failed")
    result_content = str(result.get("content", "")).strip()
    debug_steps.append(
        _build_debug_entry(
            "run_query",
            data={
                "query": query,
                "status": result_status,
                "error": result.get("error", ""),
                "content_length": len(result_content),
                "content_preview": result_content[:120],
            },
        )
    )
    if result_status == "unconfigured":
        return {
            "status": _normalize_source_status(
                SOURCE_SEARCH,
                "unconfigured",
                count=0,
                error=result.get("error", ""),
            ),
            "items": [],
            "debug": _build_source_debug(
                SOURCE_SEARCH,
                window_dates=window_dates,
                steps=debug_steps,
            ),
        }
    if result_status == "failed":
        return {
            "status": _normalize_source_status(
                SOURCE_SEARCH,
                "failed",
                count=0,
                error=result.get("error", ""),
            ),
            "items": [],
            "debug": _build_source_debug(
                SOURCE_SEARCH,
                window_dates=window_dates,
                steps=debug_steps,
            ),
        }
    if result_status != "available" or not result_content:
        return {
            "status": _normalize_source_status(SOURCE_SEARCH, "empty", count=0),
            "items": [],
            "debug": _build_source_debug(
                SOURCE_SEARCH,
                window_dates=window_dates,
                steps=debug_steps,
            ),
        }

    items = [
        {
            "source": SOURCE_SEARCH,
            "source_label": SOURCE_LABELS[SOURCE_SEARCH],
            "date": "",
            "title": f"{stock_identity['name']} 上涨原因溯源",
            "summary": _truncate_summary(result_content),
            "url": "",
            "evidence_level": "auxiliary",
            "tags": ["搜索聚合", "时间未知"],
            "raw": {
                "provider": "kimi_cli",
                "query": query,
                "content": result_content,
            },
        }
    ]
    debug_steps.append(
        _build_debug_entry(
            "build_search_evidence",
            data={
                "item_count": len(items),
                "title": items[0]["title"],
                "summary_length": len(items[0]["summary"]),
            },
        )
    )
    return {
        "status": _normalize_source_status(
            SOURCE_SEARCH,
            "available",
            count=len(items),
        ),
        "items": items,
        "debug": _build_source_debug(
            SOURCE_SEARCH,
            window_dates=window_dates,
            steps=debug_steps,
        ),
    }


def _run_zsxq(stock_identity: Dict[str, str], window_dates: List[str]) -> Dict[str, Any]:
    debug_steps: List[Dict[str, Any]] = []
    if not get_zsxq_cookie() or not get_zsxq_group_ids():
        return {
            "status": _normalize_source_status(
                SOURCE_ZSXQ,
                "unconfigured",
                count=0,
                error="未配置 ZSXQ_COOKIE 或 ZSXQ_GROUP_IDS",
            ),
            "items": [],
            "debug": _build_source_debug(
                SOURCE_ZSXQ,
                window_dates=window_dates,
                steps=[
                    _build_debug_entry(
                        "config_check",
                        data={
                            "cookie_configured": bool(get_zsxq_cookie()),
                            "group_ids_configured": bool(get_zsxq_group_ids()),
                        },
                    )
                ],
            ),
        }

    raw_items: List[Dict[str, Any]] = []
    errors: List[str] = []
    for date_str in window_dates:
        result = fetch_topics_by_date(date_str, limit=30)
        fetched_items = result.get("items") or []
        matched_items = []
        unmatched_titles = []
        if result.get("error"):
            errors.append(result["error"])
        for item in fetched_items:
            matched = _topic_matches_stock(item, stock_identity["code"], stock_identity["name"])
            if matched:
                matched_items.append(item)
                raw_items.append(item)
            elif len(unmatched_titles) < 10:
                unmatched_titles.append(str(item.get("title", "")).strip())
        debug_steps.append(
            _build_debug_entry(
                "fetch_topics_by_date",
                data={
                    "date": date_str,
                    "error": result.get("error", ""),
                    "raw_count": len(fetched_items),
                    "matched_count": len(matched_items),
                    "matched_titles": [
                        str(item.get("title", "")).strip() for item in matched_items[:10]
                    ],
                    "unmatched_titles_sample": unmatched_titles,
                },
            )
        )

    items = _normalize_zsxq_items(raw_items)
    items = _dedupe_evidence_items(items)
    debug_steps.append(
        _build_debug_entry(
            "normalize_and_dedupe",
            data={
                "matched_raw_count": len(raw_items),
                "final_count": len(items),
                "titles": [item.get("title", "") for item in items[:10]],
            },
        )
    )
    if items:
        return {
            "status": _normalize_source_status(SOURCE_ZSXQ, "available", count=len(items)),
            "items": items,
            "debug": _build_source_debug(
                SOURCE_ZSXQ,
                window_dates=window_dates,
                steps=debug_steps,
            ),
        }

    if errors and not raw_items:
        return {
            "status": _normalize_source_status(
                SOURCE_ZSXQ,
                "failed",
                count=0,
                error=" | ".join(errors),
            ),
            "items": [],
            "debug": _build_source_debug(
                SOURCE_ZSXQ,
                window_dates=window_dates,
                steps=debug_steps,
            ),
        }

    return {
        "status": _normalize_source_status(SOURCE_ZSXQ, "empty", count=0),
        "items": [],
        "debug": _build_source_debug(
            SOURCE_ZSXQ,
            window_dates=window_dates,
            steps=debug_steps,
        ),
    }


def _run_report_earnings(
    stock_identity: Dict[str, str],
    window_dates: List[str],
) -> Dict[str, Any]:
    debug_steps: List[Dict[str, Any]] = []
    try:
        result = get_annual_report_parser_result(
            stock_identity["code"],
            report_limit=REPORT_EARNINGS_LIMIT,
        )
    except Exception as exc:
        return {
            "status": _normalize_source_status(
                SOURCE_REPORT_EARNINGS,
                "report_earnings_failed",
                count=0,
                error=str(exc),
            ),
            "items": [],
            "debug": _build_source_debug(
                SOURCE_REPORT_EARNINGS,
                window_dates=window_dates,
                steps=[
                    _build_debug_entry(
                        "load_report",
                        data={
                            "report_limit": REPORT_EARNINGS_LIMIT,
                            "exception": str(exc),
                        },
                    )
                ],
            ),
        }

    reports = result.get("reports") or []
    debug_steps.append(
        _build_debug_entry(
            "load_report",
            data={
                "report_limit": REPORT_EARNINGS_LIMIT,
                "overall_status": result.get("overall_status", ""),
                "report_count": len(reports),
                "errors": result.get("errors") or [],
            },
        )
    )
    if not reports:
        overall_status = result.get("overall_status", "")
        status = (
            "report_earnings_failed"
            if overall_status in {"failed", "report_locate_failed"}
            else "no_recent_report_earnings"
        )
        return {
            "status": _normalize_source_status(
                SOURCE_REPORT_EARNINGS,
                status,
                count=0,
                error=" | ".join(result.get("errors") or []),
            ),
            "items": [],
            "debug": _build_source_debug(
                SOURCE_REPORT_EARNINGS,
                window_dates=window_dates,
                steps=debug_steps,
            ),
        }

    report = reports[0]
    financial_status = str(report.get("report_status", {}).get("financial_status", "")).strip()
    debug_steps.append(
        _build_debug_entry(
            "select_latest_report",
            data={
                "report_title": report.get("report_title", ""),
                "announcement_date": report.get("announcement_date", ""),
                "financial_status": financial_status,
                "financial_change_count": len(report.get("financial_changes") or []),
            },
        )
    )
    if financial_status == "financial_failed":
        return {
            "status": _normalize_source_status(
                SOURCE_REPORT_EARNINGS,
                "report_earnings_failed",
                count=0,
                error=" | ".join(result.get("errors") or []),
            ),
            "items": [],
            "debug": _build_source_debug(
                SOURCE_REPORT_EARNINGS,
                window_dates=window_dates,
                steps=debug_steps,
            ),
        }

    if financial_status == "financial_partially_loaded":
        status = "report_earnings_partially_loaded"
    else:
        status = "report_earnings_loaded"

    return {
        "status": _normalize_source_status(
            SOURCE_REPORT_EARNINGS,
            status,
            count=1,
            error=" | ".join(result.get("errors") or []),
        ),
        "items": [_normalize_report_earnings_item(report, status)],
        "debug": _build_source_debug(
            SOURCE_REPORT_EARNINGS,
            window_dates=window_dates,
            steps=debug_steps,
        ),
    }


def _build_ai_summary(
    stock_identity: Dict[str, str],
    evidence_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if not evidence_items:
        return {
            "status": "empty",
            "content": "暂无可分析内容",
            "error": "",
        }

    prompt = build_stock_rise_attribution_prompt(
        stock_identity=stock_identity,
        evidence_items=evidence_items,
        window_description=_window_description(),
    )

    try:
        content = run_ai_analysis(
            prompt,
            cache_key=f"stock_rise_attr_{stock_identity['code']}_{hashlib.md5(prompt.encode('utf-8')).hexdigest()[:12]}",
            timeout=180,
        )
    except Exception as exc:
        return {
            "status": "failed",
            "content": "上涨归因分析暂不可用",
            "error": str(exc),
        }

    if not content or "AI分析暂时不可用" in content:
        return {
            "status": "failed",
            "content": "上涨归因分析暂不可用",
            "error": content or "AI 返回为空",
        }

    return {
        "status": "available",
        "content": content,
        "error": "",
    }


def get_stock_rise_attribution(
    stock_identity: Dict[str, str],
    selected_sources: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """获取单只股票上涨归因验证结果。"""
    selected_sources = selected_sources or list(DEFAULT_SELECTED_SOURCES)
    selected_sources = [source for source in selected_sources if source in SOURCE_LABELS]

    if not stock_identity or not stock_identity.get("code") or not stock_identity.get("name"):
        raise ValueError("缺少有效的股票身份信息")

    default_window_dates = _window_dates_for_source(SOURCE_P5W)
    source_windows = {
        source: _window_dates_for_source(source)
        for source in SOURCE_ORDER
    }
    source_results: List[Dict[str, Any]] = []
    source_debugs: List[Dict[str, Any]] = []
    evidence_items: List[Dict[str, Any]] = []

    selected_source_set = set(selected_sources)

    for source in SOURCE_ORDER:
        if source not in selected_source_set:
            source_results.append(_normalize_source_status(source, "not_selected", count=0))
            source_debugs.append(
                _build_source_debug(
                    source,
                    window_dates=source_windows[source],
                    steps=[
                        _build_debug_entry(
                            "selection",
                            message="来源未勾选，跳过执行",
                            data={"selected": False},
                        )
                    ],
                )
            )
            continue
        window_dates = source_windows[source]
        if source == SOURCE_CNINFO:
            result = _run_cninfo(stock_identity, window_dates)
        elif source == SOURCE_RESEARCH:
            result = _run_research(stock_identity, window_dates)
        elif source == SOURCE_P5W:
            result = _run_p5w(stock_identity, window_dates)
        elif source == SOURCE_SEARCH:
            result = _run_search(stock_identity, window_dates)
        elif source == SOURCE_ZSXQ:
            result = _run_zsxq(stock_identity, window_dates)
        elif source == SOURCE_REPORT_EARNINGS:
            result = _run_report_earnings(stock_identity, window_dates)
        else:
            continue

        source_results.append(result["status"])
        source_debugs.append(
            result.get(
                "debug",
                _build_source_debug(source, window_dates=window_dates, steps=[]),
            )
        )
        evidence_items.extend(result["items"])

    evidence_items = _dedupe_evidence_items(evidence_items)
    evidence_items = _sort_evidence_items(evidence_items)
    ai_summary = _build_ai_summary(
        stock_identity=stock_identity,
        evidence_items=evidence_items,
    )

    return {
        "stock_identity": stock_identity,
        "selected_sources": selected_sources,
        "window_dates": default_window_dates,
        "source_windows": source_windows,
        "source_statuses": source_results,
        "source_debugs": source_debugs,
        "evidence_items": evidence_items,
        "ai_summary": ai_summary,
    }
