#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""知识星球主题抓取原子能力。"""

import hashlib
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from infra.config import (
    get_zsxq_api_timeout,
    get_zsxq_cookie,
    get_zsxq_group_ids,
)


class ZsxqApiClient:
    """知识星球 API 客户端。"""

    def __init__(self, cookie: Optional[str] = None, timeout: Optional[float] = None):
        self.base_url = "https://api.zsxq.com"
        self.app_version = "3.11.0"
        self.platform = "ios"
        self.secret = "zsxqapi2020"
        self.cookie = cookie or get_zsxq_cookie()
        self.timeout = timeout if timeout is not None else get_zsxq_api_timeout()
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 "
                "Mobile/15E148 Safari/604.1"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive",
            "Cookie": self.cookie,
            "Origin": "https://wx.zsxq.com",
            "Referer": "https://wx.zsxq.com/",
        }

    def _generate_signature(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, str]:
        common_params = {
            "app_version": self.app_version,
            "platform": self.platform,
            "timestamp": str(int(time.time() * 1000)),
        }

        all_params = common_params.copy()
        if params and isinstance(params, dict):
            all_params.update(params)

        params_str = urlencode(sorted(all_params.items(), key=lambda item: item[0]))
        sign_str = f"{path}&{params_str}&{self.secret}"
        signature = hashlib.md5(sign_str.encode("utf-8")).hexdigest()
        return signature, common_params["timestamp"]

    def _request(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if not self.cookie:
            return None, "未配置 ZSXQ_COOKIE"

        signature, timestamp = self._generate_signature(path, params)
        headers = self.headers.copy()
        headers["X-Signature"] = signature
        headers["X-Timestamp"] = timestamp

        url = f"{self.base_url}{path}"
        query = urlencode(params) if params else ""
        full_url = f"{url}?{query}" if query else url

        try:
            req = Request(full_url, headers=headers, method="GET")
            with urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            if data.get("succeeded"):
                return data.get("resp_data", {}), None

            err_msg = data.get("error") or data.get("resp_err") or "未知错误"
            return None, err_msg
        except Exception as exc:
            return None, str(exc)

    def get_my_groups(self, count: int = 20) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        resp, err = self._request("/v2/groups", {"count": count})
        if err:
            return [], err
        return resp.get("groups", []), None

    def get_group_topics(
        self, group_id: str, count: int = 20, end_time: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]:
        params: Dict[str, Any] = {"count": count}
        if end_time:
            params["end_time"] = end_time

        resp, err = self._request(f"/v2/groups/{group_id}/topics", params)
        if err:
            return [], None, err

        return resp.get("topics", []), resp.get("end_time"), None

    @staticmethod
    def _parse_create_time(create_time_str: str) -> int:
        if not create_time_str:
            return 0

        try:
            dt = datetime.fromisoformat(create_time_str.replace("+0800", "+08:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            return 0

    def get_topics_by_date(
        self, group_id: str, date: str, limit: int = 50
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        start_time = int(date_obj.timestamp() * 1000)
        end_time_value = int((date_obj + timedelta(days=1)).timestamp() * 1000)

        all_topics: List[Dict[str, Any]] = []
        current_end_time: Optional[str] = None

        for _ in range(10):
            topics, next_end_time, err = self.get_group_topics(
                group_id,
                count=20,
                end_time=current_end_time,
            )
            if err or not topics:
                return all_topics[:limit], err

            for topic in topics:
                topic_time = self._parse_create_time(topic.get("create_time", ""))
                if start_time <= topic_time < end_time_value:
                    all_topics.append(topic)
                elif topic_time < start_time:
                    return all_topics[:limit], None

            if not next_end_time:
                break

            current_end_time = next_end_time
            time.sleep(0.3)

        return all_topics[:limit], None


def parse_topic(topic: Dict[str, Any]) -> Dict[str, Any]:
    """标准化单条主题数据。"""
    topic_id = topic.get("topic_id")
    title = topic.get("title", "")
    content = ""

    talk = topic.get("talk", {})
    if talk:
        content = talk.get("text", "")
        if not title and content:
            title = content[:30] + "..." if len(content) > 30 else content

    author = topic.get("owner", {}).get("name", "")
    create_time = topic.get("create_time", "")
    try:
        dt = datetime.fromisoformat(create_time.replace("+0800", "+08:00"))
        create_time_str = dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        create_time_str = create_time[:16] if create_time else ""

    return {
        "type": "zsxq_topic",
        "id": str(topic_id),
        "title": title or "无标题",
        "content": content,
        "author": author,
        "created_at": create_time_str,
        "url": f"https://wx.zsxq.com/dweb2/index/topic/{topic_id}",
        "source": "知识星球",
    }


def fetch_topics_by_date(date: str, limit: int = 50) -> Dict[str, Any]:
    """抓取指定日期的知识星球主题。"""
    group_ids_raw = get_zsxq_group_ids()
    cookie = get_zsxq_cookie()
    if not cookie or not group_ids_raw:
        return {"items": [], "error": "未配置 ZSXQ_COOKIE 或 ZSXQ_GROUP_IDS"}

    client = ZsxqApiClient(cookie=cookie)
    group_ids = [item.strip() for item in group_ids_raw.split(",") if item.strip()]

    all_items: List[Dict[str, Any]] = []
    errors: List[str] = []
    for group_id in group_ids:
        topics, err = client.get_topics_by_date(group_id, date, limit=limit)
        if err:
            errors.append(f"星球 {group_id}: {err}")
            continue

        for topic in topics:
            item = parse_topic(topic)
            item["group_id"] = group_id
            all_items.append(item)

    return {
        "items": all_items[:limit],
        "error": "; ".join(errors) if errors else "",
    }
