"""
爬虫模块
包含新闻爬虫、公告爬虫、电报爬虫等功能
"""

import time
import datetime
import re
import pandas as pd
import json
import requests
import hashlib
from bs4 import BeautifulSoup
import logging
import os
import math
from urllib.parse import urljoin
from datetime import datetime as dt_datetime, timedelta
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from infra.config import (
    get_zsxq_api_timeout,
    get_zsxq_cookie,
    get_zsxq_group_ids,
)
from infra.web_scraper import scrape_with_jina_reader
from infra.storage import clean_filename

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)


CNINFO_PAGE_SIZE = 30
CNINFO_TIMEOUT = 10
P5W_SOURCE = "p5w_interaction"
P5W_URL = "https://ir.p5w.net/interaction/getNewSearchR.shtml"
P5W_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}
TAG_RE = re.compile(r"<[^>]+>")

INQUIRY_REPLY = [
    r"问询.*回复",
    r"回复.*问询",
    r"问询函回复",
    r"审核问询.*回复",
    r"反馈意见.*回复",
]
ABNORMAL_VOLATILITY = [r"股价异常波动", r"股票交易异常波动"]
REGULATORY = [r"监管函", r"关注函", r"警示函", r"监管工作函", r"纪律处分"]
CAPITAL_EMPLOYEE = [r"员工持股计划"]
CAPITAL_PRIVATE_OFFER = [r"向特定对象发行", r"特定对象发行", r"定向增发"]
CAPITAL_EQUITY_INCENTIVE = [r"股权激励", r"限制性股票", r"股票期权", r"激励计划"]
INCREASE_HOLD = [r"增持", r"拟增持", r"增持计划"]
DECREASE_HOLD = [r"减持", r"拟减持", r"减持计划", r"减持股份"]
DECREASE_PROGRESS = [
    r"减持进展",
    r"减持进度",
    r"减持计划完成",
    r"减持时间过半",
    r"减持计划时间过半",
    r"减持数量过半",
    r"减持计划届满",
    r"实施进展",
]
MAJOR_COOPERATION = [
    r"重大合作",
    r"战略合作",
    r"合作框架",
    r"签署.*协议",
    r"投资.*项目",
    r"项目投资",
    r"中标",
    r"中选",
]
QUICK_REPORT = [r"业绩快报", r"季度业绩快报", r"半年度业绩快报", r"年度业绩快报"]
PERIODIC_REPORT_DISCLOSURE = [
    r"年度报告(摘要)?",
    r"半年度报告(摘要)?",
    r"第一季度报告(全文|正文)?",
    r"第三季度报告(全文|正文)?",
    r"年报(摘要)?",
    r"半年报(摘要)?",
    r"一季报",
    r"三季报",
]
EARNINGS_FORECAST_DISCLOSURE = [
    r"业绩预告",
    r"业绩预告修正",
    r"业绩预告更正",
    r"业绩预告补充",
]


def _cninfo_headers():
    """构建巨潮资讯请求头。"""
    return {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.cninfo.com.cn",
        "Origin": "http://www.cninfo.com.cn",
        "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }


def _hit(title: str, patterns: list[str]) -> bool:
    """检查标题是否命中任一正则。"""
    for pattern in patterns:
        if re.search(pattern, title, flags=re.IGNORECASE):
            return True
    return False


def classify_cninfo_fulltext(title: str | None) -> dict:
    """按公告标题规则分类，并给出是否排除。"""
    normalized_title = (title or "").strip()

    if _hit(normalized_title, INQUIRY_REPLY):
        if _hit(normalized_title, ABNORMAL_VOLATILITY):
            return {
                "category": "上市公司公开信息",
                "subcategory": "其他",
                "rule_id": "cninfo.fulltext.excluded.abnormal_volatility_reply.v1",
                "excluded": True,
                "exclude_reason": "问询回复中的股价或股票交易异常波动公告",
                "tags": ["问询回复", "排除"],
            }
        return {
            "category": "上市公司公开信息",
            "subcategory": "问询回复",
            "rule_id": "cninfo.fulltext.inquiry_reply.v1",
            "excluded": False,
            "exclude_reason": "",
            "tags": ["问询回复"],
        }

    if _hit(normalized_title, REGULATORY):
        return {
            "category": "上市公司公开信息",
            "subcategory": "监管函",
            "rule_id": "cninfo.fulltext.regulatory_letter.v1",
            "excluded": False,
            "exclude_reason": "",
            "tags": ["监管函"],
        }

    if _hit(normalized_title, CAPITAL_EMPLOYEE):
        return {
            "category": "上市公司公开信息",
            "subcategory": "资本运作-员工持股计划",
            "rule_id": "cninfo.fulltext.capital.employee_stock_plan.v1",
            "excluded": False,
            "exclude_reason": "",
            "tags": ["资本运作", "员工持股计划"],
        }

    if _hit(normalized_title, CAPITAL_PRIVATE_OFFER):
        return {
            "category": "上市公司公开信息",
            "subcategory": "资本运作-特定对象发行",
            "rule_id": "cninfo.fulltext.capital.private_offering.v1",
            "excluded": False,
            "exclude_reason": "",
            "tags": ["资本运作", "特定对象发行"],
        }

    if _hit(normalized_title, CAPITAL_EQUITY_INCENTIVE):
        return {
            "category": "上市公司公开信息",
            "subcategory": "资本运作-股权激励",
            "rule_id": "cninfo.fulltext.capital.equity_incentive.v1",
            "excluded": False,
            "exclude_reason": "",
            "tags": ["资本运作", "股权激励"],
        }

    if _hit(normalized_title, INCREASE_HOLD):
        return {
            "category": "上市公司公开信息",
            "subcategory": "增持",
            "rule_id": "cninfo.fulltext.increase_hold.v1",
            "excluded": False,
            "exclude_reason": "",
            "tags": ["增持"],
        }

    if _hit(normalized_title, DECREASE_HOLD):
        if _hit(normalized_title, DECREASE_PROGRESS):
            return {
                "category": "上市公司公开信息",
                "subcategory": "其他",
                "rule_id": "cninfo.fulltext.excluded.decrease_progress.v1",
                "excluded": True,
                "exclude_reason": "减持进度或实施进展类公告",
                "tags": ["减持", "排除"],
            }
        return {
            "category": "上市公司公开信息",
            "subcategory": "减持",
            "rule_id": "cninfo.fulltext.decrease_hold.v1",
            "excluded": False,
            "exclude_reason": "",
            "tags": ["减持"],
        }

    if _hit(normalized_title, MAJOR_COOPERATION):
        return {
            "category": "上市公司公开信息",
            "subcategory": "重大合作/投资项目",
            "rule_id": "cninfo.fulltext.major_cooperation_project.v1",
            "excluded": False,
            "exclude_reason": "",
            "tags": ["合作", "投资项目"],
        }

    if _hit(normalized_title, PERIODIC_REPORT_DISCLOSURE):
        return {
            "category": "上市公司公开信息",
            "subcategory": "其他",
            "rule_id": "cninfo.fulltext.excluded.periodic_report_disclosure.v1",
            "excluded": True,
            "exclude_reason": "定期报告与摘要类披露",
            "tags": ["定期报告", "排除"],
        }

    if _hit(normalized_title, EARNINGS_FORECAST_DISCLOSURE):
        return {
            "category": "上市公司公开信息",
            "subcategory": "其他",
            "rule_id": "cninfo.fulltext.excluded.earnings_forecast_disclosure.v1",
            "excluded": True,
            "exclude_reason": "业绩预告类披露",
            "tags": ["业绩预告", "排除"],
        }

    if _hit(normalized_title, QUICK_REPORT):
        return {
            "category": "上市公司公开信息",
            "subcategory": "快报",
            "rule_id": "cninfo.fulltext.quick_report.v1",
            "excluded": False,
            "exclude_reason": "",
            "tags": ["快报", "业绩快报"],
        }

    return {
        "category": "上市公司公开信息",
        "subcategory": "其他",
        "rule_id": "cninfo.fulltext.excluded.other.v1",
        "excluded": True,
        "exclude_reason": "未命中保留类目的其他公告",
        "tags": ["其他", "排除"],
    }


def build_cninfo_relation_rule_result() -> dict:
    """返回机构调研 tab 的默认规则信息。"""
    return {
        "category": "互动易/调研",
        "subcategory": "未定义",
        "rule_id": "cninfo.relation.unclassified.v1",
        "excluded": False,
        "exclude_reason": "",
        "tags": ["relation"],
    }


def apply_cninfo_rules(tab_type, announcements, *, include_excluded=False):
    """按 tab 类型应用巨潮规则，并可过滤被排除项。"""
    filtered_items = []

    for item in announcements:
        if tab_type == "fulltext":
            rule_result = classify_cninfo_fulltext(item.get("announcementTitle"))
        else:
            rule_result = build_cninfo_relation_rule_result()

        enriched_item = {**item, **rule_result}
        if enriched_item["excluded"] and not include_excluded:
            continue
        filtered_items.append(enriched_item)

    return filtered_items


def _normalize_cninfo_announcements(announcements):
    """标准化巨潮公告字段。"""
    normalized_items = []
    for item in announcements or []:
        announcement_time = item.get("announcementTime")
        publish_date = ""
        if announcement_time:
            publish_date = datetime.datetime.fromtimestamp(announcement_time / 1000).strftime(
                "%Y-%m-%d"
            )

        adjunct_url = item.get("adjunctUrl") or ""
        if adjunct_url and not adjunct_url.startswith("http"):
            adjunct_url = "http://static.cninfo.com.cn/" + adjunct_url.lstrip("/")

        normalized_items.append(
            {
                "announcementTime": publish_date,
                "secName": item.get("secName", ""),
                "secCode": item.get("secCode", ""),
                "announcementTitle": item.get("announcementTitle", ""),
                "adjunctUrl": adjunct_url,
                "category": item.get("category", ""),
            }
        )
    return normalized_items


def cls_telegraphs():
    """
    财联社-电报 https://www.cls.cn/telegraph
    返回dataframe 对象
    """
    current_time = int(time.time())
    url = "https://www.cls.cn/nodeapi/telegraphList"
    params = {
        "app": "CailianpressWeb",
        "category": "",
        "lastTime": current_time,
        "last_time": current_time,
        "os": "web",
        "refresh_type": "1",
        "rn": "2000",
        "sv": "7.7.5",
    }
    text = requests.get(url, params=params).url.split("?")[1]
    if not isinstance(text, bytes):
        text = bytes(text, "utf-8")
    sha1 = hashlib.sha1(text).hexdigest()
    code = hashlib.md5(sha1.encode()).hexdigest()

    params = {
        "app": "CailianpressWeb",
        "category": "",
        "lastTime": current_time,
        "last_time": current_time,
        "os": "web",
        "refresh_type": "1",
        "rn": "2000",
        "sv": "7.7.5",
        "sign": code,
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Type": "application/json;charset=utf-8",
        "Host": "www.cls.cn",
        "Pragma": "no-cache",
        "Referer": "https://www.cls.cn/telegraph",
        "sec-ch-ua": '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
    }
    data = requests.get(url, headers=headers, params=params).json()
    df = pd.DataFrame(data["data"]["roll_data"])

    df = df[["title", "content", "level", "subjects", "ctime"]]
    df["ctime"] = pd.to_datetime(df["ctime"], unit="s", utc=True).dt.tz_convert(
        "Asia/Shanghai"
    )
    df.columns = ["标题", "内容", "等级", "标签", "发布时间"]
    df.sort_values(["发布时间"], ascending=False, inplace=True)
    df.reset_index(inplace=True, drop=True)
    df_tags = df["标签"].to_numpy()
    tags_data = []
    for tags in df_tags:
        if tags:
            ts = ",".join([t["subject_name"] for t in tags])
        else:
            ts = ""
        tags_data.append(ts)
    df["标签"] = tags_data
    df["发布日期"] = df["发布时间"].dt.date
    df["发布时间"] = df["发布时间"].dt.time

    return df


def _p5w_normalize_item(
    *,
    date,
    source,
    symbol,
    company,
    title,
    summary,
    url,
    raw,
    category,
    subcategory,
    rule_id,
    excluded,
    exclude_reason,
    tags,
    event_time,
):
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


def _p5w_adapter_result(*, date, source, items=None, error=""):
    return {
        "date": date,
        "source": source,
        "items": items or [],
        "error": error,
        "count": len(items or []),
    }


def fetch_p5w_interaction_page(
    page,
    rows=10,
    key_words="",
    company_code="",
    company_baseinfo_id="",
    timeout=20,
    session=None,
):
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


def strip_html_tags(text):
    if text is None:
        return ""
    return TAG_RE.sub("", html.unescape(str(text))).strip()


def normalize_p5w_rows(rows):
    out = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        event_time = (row.get("replyerTimeStr") or row.get("questionerTimeStr") or "").strip()
        event_date = event_time[:10] if len(event_time) >= 10 else ""
        item = dict(row)
        item["event_time"] = event_time
        item["event_date"] = event_date
        item["clean_content"] = strip_html_tags(row.get("content", ""))
        item["clean_reply_content"] = strip_html_tags(row.get("replyContent", ""))
        out.append(item)
    return out


def filter_p5w_rows_by_time(rows, start, end):
    try:
        start_d = dt_datetime.strptime(start, "%Y-%m-%d").date()
        end_d = dt_datetime.strptime(end, "%Y-%m-%d").date()
    except Exception:
        return []

    out = []
    for row in rows or []:
        event_date = (row.get("event_date") or "").strip()
        try:
            row_date = dt_datetime.strptime(event_date, "%Y-%m-%d").date()
        except Exception:
            continue
        if start_d <= row_date <= end_d:
            out.append(row)
    return out


def collect_p5w_interaction(
    date,
    rows_per_page=10,
    max_pages=30,
    key_words="",
    company_code="",
):
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

        all_rows = []
        seen_pid = set()

        def append_page_rows(page_rows):
            for row in page_rows or []:
                pid = str(row.get("pid") or "").strip()
                if pid:
                    if pid in seen_pid:
                        continue
                    seen_pid.add(pid)
                all_rows.append(row)

        with requests.Session() as session:
            first = fetch_p5w_interaction_page(
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
                current = fetch_p5w_interaction_page(
                    page=page,
                    rows=rows_per_page,
                    key_words=key_words,
                    company_code=company_code,
                    session=session,
                )
                append_page_rows(current.get("rows", []))

        norm = normalize_p5w_rows(all_rows)
        if company_code:
            norm = [
                item
                for item in norm
                if str(item.get("companyCode", "")).strip() == str(company_code).strip()
            ]
        filtered = filter_p5w_rows_by_time(norm, start=date, end=date)

        items = [
            _p5w_normalize_item(
                date=item.get("event_date") or date,
                source=P5W_SOURCE,
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

        return _p5w_adapter_result(date=date, source=P5W_SOURCE, items=items)
    except Exception as exc:
        return _p5w_adapter_result(date=date, source=P5W_SOURCE, error=str(exc))


class ZsxqApiClient:
    """知识星球 API 客户端。"""

    def __init__(self, cookie=None, timeout=None):
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

    def _generate_signature(self, path, params=None):
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

    def _request(self, path, params=None):
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

    def get_my_groups(self, count=20):
        resp, err = self._request("/v2/groups", {"count": count})
        if err:
            return [], err
        return resp.get("groups", []), None

    def get_group_topics(self, group_id, count=20, end_time=None):
        params = {"count": count}
        if end_time:
            params["end_time"] = end_time

        resp, err = self._request(f"/v2/groups/{group_id}/topics", params)
        if err:
            return [], None, err

        return resp.get("topics", []), resp.get("end_time"), None

    @staticmethod
    def _parse_create_time(create_time_str):
        if not create_time_str:
            return 0

        try:
            parsed = dt_datetime.fromisoformat(create_time_str.replace("+0800", "+08:00"))
            return int(parsed.timestamp() * 1000)
        except Exception:
            return 0

    def get_topics_by_date(self, group_id, date, limit=50):
        date_obj = dt_datetime.strptime(date, "%Y-%m-%d")
        start_time = int(date_obj.timestamp() * 1000)
        end_time_value = int((date_obj + timedelta(days=1)).timestamp() * 1000)

        all_topics = []
        current_end_time = None

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


def parse_zsxq_topic(topic):
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
        parsed = dt_datetime.fromisoformat(create_time.replace("+0800", "+08:00"))
        create_time_str = parsed.strftime("%Y-%m-%d %H:%M")
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


def fetch_topics_by_date(date, limit=50):
    """抓取指定日期的知识星球主题。"""
    group_ids_raw = get_zsxq_group_ids()
    cookie = get_zsxq_cookie()
    if not cookie or not group_ids_raw:
        return {"items": [], "error": "未配置 ZSXQ_COOKIE 或 ZSXQ_GROUP_IDS"}

    client = ZsxqApiClient(cookie=cookie)
    group_ids = [item.strip() for item in group_ids_raw.split(",") if item.strip()]

    all_items = []
    errors = []
    for group_id in group_ids:
        topics, err = client.get_topics_by_date(group_id, date, limit=limit)
        if err:
            errors.append(f"星球 {group_id}: {err}")
            continue

        for topic in topics:
            item = parse_zsxq_topic(topic)
            item["group_id"] = group_id
            all_items.append(item)

    return {
        "items": all_items[:limit],
        "error": "; ".join(errors) if errors else "",
    }


def get_cninfo_orgid(stock_code):
    """
    根据股票代码获取巨潮资讯网的 orgId

    Args:
        stock_code: 股票代码，如 '300017'

    Returns:
        orgId 字符串，如 '9900008387'
    """
    url = "http://www.cninfo.com.cn/new/information/topSearch/query"
    data = {"keyWord": stock_code, "maxNum": 10}

    try:
        resp = requests.post(url, data=data, headers=_cninfo_headers(), timeout=CNINFO_TIMEOUT)
        resp.raise_for_status()
        result = resp.json()
        if result and len(result) > 0:
            # 匹配精确的股票代码
            for item in result:
                if item.get("code") == stock_code:
                    return item.get("orgId")
            # 如果没有精确匹配，返回第一个
            return result[0].get("orgId")
    except Exception as e:
        logger.error(f"获取 orgId 失败: {stock_code}, error: {e}")

    return None


def cninfo_announcement_spider(
    pageNum,
    tabType,
    stock="",
    searchkey="",
    category="",
    trade="",
    seDate=None,
    use_rules=True,
    include_excluded=False,
):
    """
    巨潮资讯网公告爬虫
    tab类型：fulltext 公告；relation  调研
    searchkey: 标题关键字
    trade: 行业
    category:
    - 业绩预告 category_yjygjxz_szsh
    - 年报 category_ndbg_szsh
    - 半年报 category_bndbg_szsh
    - 一季报 category_yjdbg_szsh
    - 三季报 category_sjdbg_szsh
    - 日常经营 category_rcjy_szsh 合同，合作，协议，进展
    - 首发 category_sf_szsh 招股书
    - 股权激励 category_gqjl_szsh

    stock 参数格式支持:
    - 单独股票代码: '300017'
    - code,orgId 格式: '300017,9900008387' (推荐，更精确)
    """
    if seDate is None:
        today_str = datetime.date.today().strftime("%Y-%m-%d")
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        seDate = yesterday_str + "~" + today_str

    url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
    pageNum = int(pageNum)
    data = {
        "pageNum": pageNum,
        "pageSize": CNINFO_PAGE_SIZE,
        "column": "szse",
        "tabName": tabType,
        "plate": "",
        "stock": stock,
        "searchkey": searchkey,
        "secid": "",
        "category": category,
        "trade": trade,
        "seDate": seDate,
        "sortName": "",
        "sortType": "",
        "isHLtitle": "true",
    }

    try:
        response = requests.post(
            url,
            data=data,
            headers=_cninfo_headers(),
            timeout=CNINFO_TIMEOUT,
        )
        response.raise_for_status()
        results = response.json()
        if results.get("announcements"):
            normalized_items = _normalize_cninfo_announcements(results["announcements"])
            if use_rules:
                normalized_items = apply_cninfo_rules(
                    tabType,
                    normalized_items,
                    include_excluded=include_excluded,
                )
            if not normalized_items:
                return None
            df = pd.DataFrame(normalized_items)
            return df
    except Exception as e:
        logger.error(f"cninfo_announcement_spider error: {e}")

    return None
