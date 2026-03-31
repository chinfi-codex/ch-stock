#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""内容搜索聚合原子能力。"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlparse

import requests

from infra.config import get_jina_api_key
from infra.web_scraper import scrape_with_jina_reader


SOURCE = "search_aggregation"
SEARCH_ENDPOINT = "https://s.jina.ai"
DEFAULT_RESULT_LIMIT = 5
DEFAULT_HEADERS = {
    "Accept": "text/plain, text/markdown, */*",
    "User-Agent": "Mozilla/5.0",
}
LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")
DATE_RE = re.compile(r"(20\d{2}-\d{2}-\d{2})")
MARKDOWN_URL_RE = re.compile(r"https?://[^\s)]+")


def _adapter_result(
    *,
    query: str,
    items: Optional[List[Dict[str, Any]]] = None,
    error: str = "",
    status: str = "available",
) -> Dict[str, Any]:
    items = items or []
    return {
        "source": SOURCE,
        "query": query,
        "items": items,
        "count": len(items),
        "error": error,
        "status": status,
    }


def _extract_domain(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def _is_domain_allowed(url: str, allowed_domains: List[str]) -> bool:
    if not allowed_domains:
        return True

    domain = _extract_domain(url)
    for allowed in allowed_domains:
        allowed = (allowed or "").strip().lower()
        if not allowed:
            continue
        if domain == allowed or domain.endswith(f".{allowed}"):
            return True
    return False


def _extract_date(text: str) -> str:
    if not text:
        return ""
    match = DATE_RE.search(text)
    return match.group(1) if match else ""


def _normalize_item(
    *,
    title: str,
    url: str,
    summary: str,
    event_date: str,
    query: str,
    raw: Dict[str, Any],
) -> Dict[str, Any]:
    tags = ["搜索聚合"]
    if not event_date:
        tags.append("时间未知")

    return {
        "source": SOURCE,
        "source_label": "内容搜索聚合",
        "title": title or "未命名搜索结果",
        "summary": summary.strip(),
        "url": url,
        "date": event_date,
        "evidence_level": "auxiliary",
        "tags": tags,
        "query": query,
        "raw": raw,
    }


def _parse_search_blocks(content: str) -> List[Dict[str, str]]:
    blocks: List[Dict[str, str]] = []
    current: Dict[str, str] = {}

    for raw_line in (content or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        link_match = LINK_RE.search(line)
        if link_match:
            if current:
                blocks.append(current)
            current = {
                "title": link_match.group(1).strip(),
                "url": link_match.group(2).strip(),
                "summary": "",
                "date": _extract_date(line),
            }
            continue

        if line.startswith(("http://", "https://")) and not current:
            url_match = MARKDOWN_URL_RE.search(line)
            if not url_match:
                continue
            current = {
                "title": line,
                "url": url_match.group(0),
                "summary": "",
                "date": _extract_date(line),
            }
            continue

        if current:
            current["summary"] = f"{current.get('summary', '')} {line}".strip()
            if not current.get("date"):
                current["date"] = _extract_date(line)

    if current:
        blocks.append(current)

    return blocks


def _search_once(query: str, api_key: str, timeout: int = 30) -> str:
    url = f"{SEARCH_ENDPOINT}/{quote(query)}"
    headers = dict(DEFAULT_HEADERS)
    headers["Authorization"] = f"Bearer {api_key}"

    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.text


def search_web_content(
    query: str,
    *,
    allowed_domains: Optional[List[str]] = None,
    limit: int = DEFAULT_RESULT_LIMIT,
    fetch_reader_summary: bool = True,
    timeout: int = 30,
) -> Dict[str, Any]:
    """执行搜索并返回标准化搜索结果。"""
    api_key = get_jina_api_key()
    if not api_key:
        return _adapter_result(
            query=query,
            error="未配置 JINA_API_KEY / JINA_KEY，无法启用内容搜索聚合",
            status="unconfigured",
        )

    try:
        raw_content = _search_once(query, api_key=api_key, timeout=timeout)
    except Exception as exc:
        return _adapter_result(query=query, error=str(exc), status="failed")

    parsed_blocks = _parse_search_blocks(raw_content)
    seen_urls = set()
    items: List[Dict[str, Any]] = []
    domains = allowed_domains or []

    for block in parsed_blocks:
        url = (block.get("url") or "").strip()
        if not url or url in seen_urls:
            continue
        if not _is_domain_allowed(url, domains):
            continue

        seen_urls.add(url)
        summary = (block.get("summary") or "").strip()
        if fetch_reader_summary:
            reader_result = scrape_with_jina_reader(
                url,
                title=block.get("title", ""),
                output_dir="",
                save_to_file=False,
            )
            if reader_result.get("success"):
                reader_content = (reader_result.get("content") or "").strip()
                if reader_content:
                    summary = reader_content[:600]

        items.append(
            _normalize_item(
                title=block.get("title", ""),
                url=url,
                summary=summary[:300],
                event_date=(block.get("date") or "").strip(),
                query=query,
                raw=block,
            )
        )
        if len(items) >= max(1, int(limit)):
            break

    if not items:
        return _adapter_result(query=query, items=[], status="empty")

    return _adapter_result(query=query, items=items, status="available")
