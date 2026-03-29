#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""全景网互动易抓取原子能力。"""

import datetime as dt
import html
import math
import re
from typing import Any, Dict, List, Optional

import requests


SOURCE = "p5w_interaction"
P5W_URL = "https://ir.p5w.net/interaction/getNewSearchR.shtml"
P5W_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}
TAG_RE = re.compile(r"<[^>]+>")


def _normalize_item(
    *,
    date: str,
    source: str,
    symbol: str,
    company: str,
    title: str,
    summary: str,
    url: str,
    raw: Dict[str, Any],
    category: str,
    subcategory: str,
    rule_id: str,
    excluded: bool,
    exclude_reason: str,
    tags: List[str],
    event_time: str,
) -> Dict[str, Any]:
    return {
        "date": date,
        "source": source,
        "symbol": symbol,
        "company": company,
        "title": title,
        "summary": summary,
        "url": url,
        "raw": raw,
        "category": category,
        "subcategory": subcategory,
        "rule_id": rule_id,
        "excluded": excluded,
        "exclude_reason": exclude_reason,
        "tags": tags,
        "event_time": event_time,
    }


def _adapter_result(
    *, date: str, source: str, items: Optional[List[Dict[str, Any]]] = None, error: str = ""
) -> Dict[str, Any]:
    return {
        "date": date,
        "source": source,
        "items": items or [],
        "error": error,
        "count": len(items or []),
    }


def fetch_page(
    page: int,
    rows: int = 10,
    key_words: str = "",
    company_code: str = "",
    company_baseinfo_id: str = "",
    timeout: int = 20,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    try:
        rows = int(rows)
    except Exception:
        rows = 10
    rows = max(1, min(rows, 10))

    payload = {
        "isPagination": "1",
        "keyWords": key_words or "",
        "companyCode": company_code or "",
        "companyBaseinfoId": company_baseinfo_id or "",
        "page": str(max(0, int(page))),
        "rows": str(rows),
    }

    client = session or requests
    resp = client.post(P5W_URL, data=payload, headers=P5W_HEADERS, timeout=timeout)
    resp.raise_for_status()
    obj = resp.json()
    if not obj.get("success"):
        raise RuntimeError(f"接口返回失败: {obj}")
    return obj


def strip_html(text: Any) -> str:
    if text is None:
        return ""
    return TAG_RE.sub("", html.unescape(str(text))).strip()


def normalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        event_time = (row.get("replyerTimeStr") or row.get("questionerTimeStr") or "").strip()
        event_date = event_time[:10] if len(event_time) >= 10 else ""
        item = dict(row)
        item["event_time"] = event_time
        item["event_date"] = event_date
        item["clean_content"] = strip_html(row.get("content", ""))
        item["clean_reply_content"] = strip_html(row.get("replyContent", ""))
        out.append(item)
    return out


def filter_time(rows: List[Dict[str, Any]], start: str, end: str) -> List[Dict[str, Any]]:
    try:
        start_d = dt.datetime.strptime(start, "%Y-%m-%d").date()
        end_d = dt.datetime.strptime(end, "%Y-%m-%d").date()
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    for row in rows or []:
        event_date = (row.get("event_date") or "").strip()
        try:
            row_date = dt.datetime.strptime(event_date, "%Y-%m-%d").date()
        except Exception:
            continue
        if start_d <= row_date <= end_d:
            out.append(row)
    return out


def collect(
    date: str,
    rows_per_page: int = 10,
    max_pages: int = 30,
    key_words: str = "",
    company_code: str = "",
) -> Dict[str, Any]:
    try:
        try:
            rows_per_page = int(rows_per_page)
        except Exception:
            rows_per_page = 10
        rows_per_page = max(1, min(rows_per_page, 10))

        try:
            max_pages = int(max_pages)
        except Exception:
            max_pages = 30
        max_pages = max(1, max_pages)

        all_rows: List[Dict[str, Any]] = []
        seen_pid = set()

        def append_page_rows(page_rows: List[Dict[str, Any]]) -> None:
            for row in page_rows or []:
                pid = str(row.get("pid") or "").strip()
                if pid:
                    if pid in seen_pid:
                        continue
                    seen_pid.add(pid)
                all_rows.append(row)

        with requests.Session() as session:
            first = fetch_page(
                page=0,
                rows=rows_per_page,
                key_words=key_words,
                company_code=company_code,
                session=session,
            )
            append_page_rows(first.get("rows", []))

            total = int(first.get("total", 0) or 0)
            expected_pages = max(1, int(math.ceil(float(total) / rows_per_page))) if total else 1
            pages_to_fetch = min(max_pages, expected_pages)

            for page in range(1, pages_to_fetch):
                current = fetch_page(
                    page=page,
                    rows=rows_per_page,
                    key_words=key_words,
                    company_code=company_code,
                    session=session,
                )
                append_page_rows(current.get("rows", []))

        norm = normalize_rows(all_rows)
        if company_code:
            norm = [
                item
                for item in norm
                if str(item.get("companyCode", "")).strip() == str(company_code).strip()
            ]
        filtered = filter_time(norm, start=date, end=date)

        items = [
            _normalize_item(
                date=item.get("event_date") or date,
                source=SOURCE,
                symbol=item.get("companyCode", ""),
                company=item.get("companyShortname", ""),
                title=(item.get("clean_content", "") or "")[:80],
                summary=(item.get("clean_reply_content", "") or "")[:300],
                url="https://ir.p5w.net/interaction/",
                raw=item,
                category="上市公司公开信息",
                subcategory="互动问答",
                rule_id="p5w.interaction.fixed.v1",
                excluded=False,
                exclude_reason="",
                tags=["互动问答"],
                event_time=item.get("event_time") or "",
            )
            for item in filtered
        ]

        return _adapter_result(date=date, source=SOURCE, items=items)
    except Exception as exc:
        return _adapter_result(date=date, source=SOURCE, error=str(exc))
