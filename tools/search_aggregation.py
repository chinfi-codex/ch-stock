#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""内容搜索聚合原子能力。"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from infra.llm_client import clean_ai_output, run_kimi_cli


SOURCE = "search_aggregation"
DEFAULT_RESULT_LIMIT = 5
DEFAULT_SEARCH_TIMEOUT = 90


def _adapter_result(
    *,
    query: str,
    content: str = "",
    error: str = "",
    status: str = "available",
) -> Dict[str, Any]:
    return {
        "source": SOURCE,
        "query": query,
        "items": [],
        "count": 0,
        "content": content,
        "error": error,
        "status": status,
    }


def _build_search_prompt(query: str, limit: int, allowed_domains: List[str]) -> str:
    del limit
    del allowed_domains
    return query.strip()


def _search_once(
    query: str,
    *,
    limit: int,
    allowed_domains: List[str],
    timeout: int = DEFAULT_SEARCH_TIMEOUT,
) -> Dict[str, Any]:
    prompt = _build_search_prompt(query, limit=limit, allowed_domains=allowed_domains)
    result = run_kimi_cli(prompt, timeout=timeout)
    if not result["success"]:
        return {
            "success": False,
            "content": "",
            "error_code": result["error_code"],
            "error_message": result["error_message"],
        }
    return {
        "success": True,
        "content": str(result["content"]),
        "error_code": "",
        "error_message": "",
    }


def search_web_content(
    query: str,
    *,
    allowed_domains: Optional[List[str]] = None,
    limit: int = DEFAULT_RESULT_LIMIT,
    fetch_reader_summary: bool = True,
    timeout: int = DEFAULT_SEARCH_TIMEOUT,
) -> Dict[str, Any]:
    """执行搜索并返回 Kimi 原始文本结果。"""
    del fetch_reader_summary
    domains = allowed_domains or []
    result = _search_once(
        query,
        limit=limit,
        allowed_domains=domains,
        timeout=timeout,
    )
    if not result["success"]:
        error_code = str(result["error_code"])
        status = "unconfigured" if error_code == "kimi_not_installed" else "failed"
        error_message = str(result["error_message"] or error_code)
        return _adapter_result(query=query, error=error_message, status=status)

    content = clean_ai_output(str(result["content"] or "")).strip()
    if not content or content == "NO_RESULTS":
        return _adapter_result(query=query, status="empty")
    return _adapter_result(query=query, content=content, status="available")
