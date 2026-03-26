#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
行业研究工具模块

提供行业主题、分析框架、输入源、文件快照和分析日志的本地持久化，
以及文件扫描、PDF/文本解析、AI 分析日志生成等能力。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from PyPDF2 import PdfReader

from tools.ai_analysis import analyze_industry_research

BASE_DIR = Path("datas") / "industry_research"
TOPICS_FILE = BASE_DIR / "topics.json"
FRAMEWORKS_FILE = BASE_DIR / "frameworks.json"
SOURCES_FILE = BASE_DIR / "sources.json"
SNAPSHOTS_FILE = BASE_DIR / "snapshots.json"
LOGS_DIR = BASE_DIR / "logs"
UPLOADS_DIR = BASE_DIR / "uploads"

PENDING_REVIEW = "pending_review"
APPROVED = "approved"
DISMISSED = "dismissed"
SUPPORTED_TEXT_EXTENSIONS = {".txt", ".md", ".markdown", ".csv", ".json", ".yaml", ".yml"}


def _now_str() -> str:
    """返回当前时间字符串。"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_industry_research_dirs() -> None:
    """确保行业研究所需目录与基础文件存在。"""
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    for file_path in [TOPICS_FILE, FRAMEWORKS_FILE, SOURCES_FILE, SNAPSHOTS_FILE]:
        if not file_path.exists():
            _write_json_atomic(file_path, [])


def _write_json_atomic(file_path: Path, payload: Any) -> None:
    """以原子方式写入 JSON。"""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, file_path)


def _read_json(file_path: Path, default: Any) -> Any:
    """读取 JSON 文件。"""
    if not file_path.exists():
        return default
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _slugify_topic_name(name: str) -> str:
    """生成适合目录的主题 slug。"""
    safe = "".join(ch if ch.isalnum() else "-" for ch in name.strip().lower())
    safe = "-".join(part for part in safe.split("-") if part)
    return safe or "topic"


def _normalize_sections(sections_text: str) -> List[str]:
    """将多行分析框架文本标准化为条目列表。"""
    lines = [line.strip(" -\t") for line in sections_text.splitlines()]
    return [line for line in lines if line]


def _make_id(prefix: str) -> str:
    """生成带前缀的 ID。"""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def load_frameworks() -> List[Dict[str, Any]]:
    """加载分析框架列表。"""
    ensure_industry_research_dirs()
    return _read_json(FRAMEWORKS_FILE, [])


def load_topics() -> List[Dict[str, Any]]:
    """加载行业主题列表。"""
    ensure_industry_research_dirs()
    return _read_json(TOPICS_FILE, [])


def load_sources() -> List[Dict[str, Any]]:
    """加载输入源列表。"""
    ensure_industry_research_dirs()
    return _read_json(SOURCES_FILE, [])


def load_snapshots() -> List[Dict[str, Any]]:
    """加载文件快照列表。"""
    ensure_industry_research_dirs()
    return _read_json(SNAPSHOTS_FILE, [])


def _save_frameworks(frameworks: List[Dict[str, Any]]) -> None:
    """保存分析框架列表。"""
    _write_json_atomic(FRAMEWORKS_FILE, frameworks)


def _save_topics(topics: List[Dict[str, Any]]) -> None:
    """保存行业主题列表。"""
    _write_json_atomic(TOPICS_FILE, topics)


def _save_sources(sources: List[Dict[str, Any]]) -> None:
    """保存输入源列表。"""
    _write_json_atomic(SOURCES_FILE, sources)


def _save_snapshots(snapshots: List[Dict[str, Any]]) -> None:
    """保存文件快照列表。"""
    _write_json_atomic(SNAPSHOTS_FILE, snapshots)


def list_topic_framework_options() -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """返回主题与框架列表及框架映射。"""
    frameworks = load_frameworks()
    framework_map = {item["id"]: item for item in frameworks}
    topics = load_topics()
    return topics, framework_map


def upsert_framework(
    name: str,
    sections_text: str,
    framework_id: str = "",
    enabled: bool = True,
) -> Dict[str, Any]:
    """新增或更新分析框架。"""
    frameworks = load_frameworks()
    now_str = _now_str()
    payload = {
        "id": framework_id or _make_id("framework"),
        "name": name.strip(),
        "sections": _normalize_sections(sections_text),
        "enabled": bool(enabled),
        "updated_at": now_str,
    }
    for index, item in enumerate(frameworks):
        if item.get("id") == payload["id"]:
            payload["created_at"] = item.get("created_at", now_str)
            frameworks[index] = payload
            _save_frameworks(frameworks)
            return payload
    payload["created_at"] = now_str
    frameworks.append(payload)
    _save_frameworks(frameworks)
    return payload


def upsert_topic(
    name: str,
    description: str,
    framework_id: str,
    topic_id: str = "",
    enabled: bool = True,
) -> Dict[str, Any]:
    """新增或更新行业主题。"""
    topics = load_topics()
    now_str = _now_str()
    payload = {
        "id": topic_id or _make_id("topic"),
        "name": name.strip(),
        "slug": _slugify_topic_name(name),
        "description": description.strip(),
        "framework_id": framework_id,
        "enabled": bool(enabled),
        "updated_at": now_str,
    }
    for index, item in enumerate(topics):
        if item.get("id") == payload["id"]:
            payload["created_at"] = item.get("created_at", now_str)
            topics[index] = payload
            _save_topics(topics)
            return payload
    payload["created_at"] = now_str
    topics.append(payload)
    _save_topics(topics)
    return payload


def upsert_source(
    topic_id: str,
    source_type: str,
    path_value: str,
    source_id: str = "",
    enabled: bool = True,
    display_name: str = "",
) -> Dict[str, Any]:
    """新增或更新输入源。"""
    ensure_industry_research_dirs()
    sources = load_sources()
    normalized_path = os.path.abspath(os.fspath(path_value))
    now_str = _now_str()
    payload = {
        "id": source_id or _make_id("source"),
        "topic_id": topic_id,
        "source_type": source_type,
        "path": normalized_path,
        "display_name": display_name.strip() or os.path.basename(normalized_path) or normalized_path,
        "enabled": bool(enabled),
        "updated_at": now_str,
    }
    for index, item in enumerate(sources):
        if item.get("id") == payload["id"] or (
            item.get("topic_id") == topic_id
            and item.get("source_type") == source_type
            and os.path.abspath(item.get("path", "")) == normalized_path
        ):
            payload["id"] = item.get("id", payload["id"])
            payload["created_at"] = item.get("created_at", now_str)
            sources[index] = payload
            _save_sources(sources)
            return payload
    payload["created_at"] = now_str
    sources.append(payload)
    _save_sources(sources)
    return payload


def register_uploaded_file(topic_id: str, uploaded_file: Any) -> Optional[Dict[str, Any]]:
    """保存上传文件并注册为文件输入源。"""
    if uploaded_file is None:
        return None
    ensure_industry_research_dirs()
    topic_dir = UPLOADS_DIR / topic_id
    topic_dir.mkdir(parents=True, exist_ok=True)
    filename = Path(uploaded_file.name).name
    target_path = topic_dir / filename
    with open(target_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return upsert_source(
        topic_id=topic_id,
        source_type="file",
        path_value=str(target_path),
        display_name=filename,
        enabled=True,
    )


def get_sources_by_topic(topic_id: str) -> List[Dict[str, Any]]:
    """获取主题关联输入源。"""
    return [item for item in load_sources() if item.get("topic_id") == topic_id]


def _topic_logs_dir(topic_id: str) -> Path:
    """获取主题日志目录。"""
    path = LOGS_DIR / topic_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def list_logs(topic_id: str = "", status: str = "") -> List[Dict[str, Any]]:
    """列出行业研究日志。"""
    ensure_industry_research_dirs()
    base_paths = [_topic_logs_dir(topic_id)] if topic_id else [path for path in LOGS_DIR.glob("*") if path.is_dir()]
    logs: List[Dict[str, Any]] = []
    for base_path in base_paths:
        for file_path in base_path.glob("*.json"):
            payload = _read_json(file_path, None)
            if not isinstance(payload, dict):
                continue
            if status and payload.get("status") != status:
                continue
            logs.append(payload)
    logs.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return logs


def get_log_by_id(log_id: str, topic_id: str = "") -> Optional[Dict[str, Any]]:
    """通过 ID 获取日志。"""
    for log in list_logs(topic_id=topic_id):
        if log.get("id") == log_id:
            return log
    return None


def _save_log(log_payload: Dict[str, Any]) -> Dict[str, Any]:
    """保存日志。"""
    topic_id = log_payload["topic_id"]
    file_path = _topic_logs_dir(topic_id) / f"{log_payload['id']}.json"
    _write_json_atomic(file_path, log_payload)
    return log_payload


def update_log_status(log_id: str, topic_id: str, status: str) -> Optional[Dict[str, Any]]:
    """更新日志状态。"""
    log_payload = get_log_by_id(log_id=log_id, topic_id=topic_id)
    if not log_payload:
        return None
    log_payload["status"] = status
    log_payload["reviewed_at"] = _now_str()
    return _save_log(log_payload)


def get_latest_approved_log(topic_id: str) -> Optional[Dict[str, Any]]:
    """获取主题最新已确认日志。"""
    logs = list_logs(topic_id=topic_id, status=APPROVED)
    return logs[0] if logs else None


def _make_snapshot_key(topic_id: str, file_path: str) -> str:
    """生成快照键。"""
    return f"{topic_id}|{os.path.abspath(file_path).lower()}"


def _snapshot_map(topic_id: str = "") -> Dict[str, Dict[str, Any]]:
    """返回快照映射。"""
    snapshots = load_snapshots()
    result = {}
    for snapshot in snapshots:
        if topic_id and snapshot.get("topic_id") != topic_id:
            continue
        result[snapshot.get("snapshot_key", "")] = snapshot
    return result


def _upsert_snapshot(snapshot_payload: Dict[str, Any]) -> None:
    """保存单条快照。"""
    snapshots = load_snapshots()
    snapshot_key = snapshot_payload["snapshot_key"]
    for index, item in enumerate(snapshots):
        if item.get("snapshot_key") == snapshot_key:
            snapshot_payload["created_at"] = item.get("created_at", snapshot_payload.get("created_at"))
            snapshots[index] = snapshot_payload
            _save_snapshots(snapshots)
            return
    snapshots.append(snapshot_payload)
    _save_snapshots(snapshots)


def _hash_text(content: str) -> str:
    """计算文本哈希。"""
    return hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()


def _hash_file(file_path: str) -> str:
    """计算文件二进制哈希。"""
    sha = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def _fallback_fingerprint(file_path: str, stat_result: Optional[os.stat_result] = None) -> str:
    """为不可读取文件生成退化指纹。"""
    parts = [os.path.abspath(file_path).lower()]
    if stat_result is not None:
        parts.append(str(stat_result.st_size))
        parts.append(str(int(stat_result.st_mtime)))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _extract_pdf_text(file_path: str) -> Tuple[str, Dict[str, Any]]:
    """提取 PDF 文本。"""
    page_count = 0
    extracted_pages = 0
    text_chunks: List[str] = []
    try:
        reader = PdfReader(file_path)
        page_count = len(reader.pages)
        for page in reader.pages:
            page_text = page.extract_text() or ""
            page_text = page_text.strip()
            if page_text:
                extracted_pages += 1
                text_chunks.append(page_text)
        text = "\n\n".join(text_chunks).strip()
        status = "success" if text else "empty"
        return text, {
            "parser": "PyPDF2",
            "page_count": page_count,
            "extracted_pages": extracted_pages,
            "extraction_status": status,
        }
    except Exception as exc:
        return "", {
            "parser": "PyPDF2",
            "page_count": page_count,
            "extracted_pages": extracted_pages,
            "extraction_status": "error",
            "error": str(exc),
        }


def _extract_text_file(file_path: str) -> Tuple[str, Dict[str, Any]]:
    """提取文本类文件内容。"""
    suffix = Path(file_path).suffix.lower()
    if suffix in {".json"}:
        try:
            payload = _read_json(Path(file_path), {})
            return json.dumps(payload, ensure_ascii=False, indent=2), {
                "parser": "json",
                "extraction_status": "success",
            }
        except Exception:
            pass
    if suffix in {".csv"}:
        try:
            df = pd.read_csv(file_path)
            preview = df.head(200).to_csv(index=False)
            return preview, {
                "parser": "pandas",
                "row_count": int(len(df)),
                "column_count": int(len(df.columns)),
                "extraction_status": "success",
            }
        except Exception:
            pass
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read(), {"parser": "text", "extraction_status": "success"}
    except Exception as exc:
        return "", {"parser": "text", "extraction_status": "error", "error": str(exc)}


def extract_source_content(file_path: str) -> Dict[str, Any]:
    """提取输入源内容。"""
    absolute_path = os.path.abspath(file_path)
    suffix = Path(absolute_path).suffix.lower()
    try:
        stat = os.stat(absolute_path)
    except OSError as exc:
        meta = {
            "parser": "unreadable",
            "extraction_status": "error",
            "error": str(exc),
        }
        summary = build_source_summary(absolute_path, "", meta)
        return {
            "path": absolute_path,
            "name": Path(absolute_path).name,
            "suffix": suffix,
            "size": 0,
            "modified_at": "",
            "fingerprint": _fallback_fingerprint(absolute_path),
            "text": "",
            "summary": summary,
            "meta": meta,
        }
    if suffix == ".pdf":
        text, meta = _extract_pdf_text(absolute_path)
    elif suffix in SUPPORTED_TEXT_EXTENSIONS:
        text, meta = _extract_text_file(absolute_path)
    else:
        text, meta = "", {
            "parser": "unsupported",
            "extraction_status": "unsupported",
        }

    text = (text or "").strip()
    summary = build_source_summary(absolute_path, text, meta)
    try:
        fingerprint = _hash_text(text) if text else _hash_file(absolute_path)
    except OSError as exc:
        meta = dict(meta)
        meta["extraction_status"] = "error"
        meta["error"] = str(exc)
        summary = build_source_summary(absolute_path, "", meta)
        fingerprint = _fallback_fingerprint(absolute_path, stat)
        text = ""
    return {
        "path": absolute_path,
        "name": Path(absolute_path).name,
        "suffix": suffix,
        "size": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        "fingerprint": fingerprint,
        "text": text,
        "summary": summary,
        "meta": meta,
    }


def build_source_summary(file_path: str, text: str, meta: Dict[str, Any]) -> str:
    """生成输入源摘要。"""
    file_name = Path(file_path).name
    status = meta.get("extraction_status", "unknown")
    lines = [
        f"文件名: {file_name}",
        f"路径: {os.path.abspath(file_path)}",
        f"解析状态: {status}",
    ]
    if meta.get("page_count") is not None:
        lines.append(f"页数: {meta.get('page_count')}")
    if meta.get("row_count") is not None:
        lines.append(f"数据行数: {meta.get('row_count')}")
    if text:
        compact = " ".join(text.split())
        excerpt = compact[:4000]
        lines.append("内容摘要:")
        lines.append(excerpt)
    else:
        lines.append("内容摘要: 无可提取文本，请人工审阅原始文件。")
    return "\n".join(lines)


def _iter_source_files(source: Dict[str, Any]) -> List[str]:
    """展开输入源对应的实际文件。"""
    path_value = source.get("path", "")
    if not path_value or not os.path.exists(path_value):
        return []
    if source.get("source_type") == "directory":
        try:
            files = []
            for item in Path(path_value).iterdir():
                try:
                    if item.is_file():
                        files.append(str(item))
                except OSError:
                    continue
            return sorted(files)
        except OSError:
            return []
    return [os.path.abspath(path_value)]


def _build_log_dedupe_key(topic_id: str, file_path: str, fingerprint: str) -> str:
    """生成日志去重键。"""
    return hashlib.md5(
        f"{topic_id}|{os.path.abspath(file_path).lower()}|{fingerprint}".encode("utf-8")
    ).hexdigest()


def _existing_log_by_dedupe_key(topic_id: str, dedupe_key: str) -> Optional[Dict[str, Any]]:
    """按去重键查找日志。"""
    for log in list_logs(topic_id=topic_id):
        if log.get("dedupe_key") == dedupe_key:
            return log
    return None


def create_pending_log(
    topic: Dict[str, Any],
    framework: Dict[str, Any],
    source_info: Dict[str, Any],
    change_type: str,
    latest_approved_log: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """基于文件变化生成待审阅日志。"""
    dedupe_key = _build_log_dedupe_key(topic["id"], source_info["path"], source_info["fingerprint"])
    existing = _existing_log_by_dedupe_key(topic["id"], dedupe_key)
    if existing:
        return existing

    latest_approved_summary = ""
    if latest_approved_log:
        latest_approved_summary = latest_approved_log.get("analysis_result", "")

    analysis_result = analyze_industry_research(
        topic_name=topic["name"],
        framework_sections=framework.get("sections", []),
        source_name=source_info["name"],
        source_summary=source_info["summary"],
        change_type=change_type,
        latest_approved_summary=latest_approved_summary,
        show_ui=False,
    )
    log_id = _make_id("log")
    payload = {
        "id": log_id,
        "topic_id": topic["id"],
        "topic_name": topic["name"],
        "framework_id": framework.get("id", ""),
        "framework_name": framework.get("name", ""),
        "framework_sections": framework.get("sections", []),
        "status": PENDING_REVIEW,
        "change_type": change_type,
        "created_at": _now_str(),
        "reviewed_at": "",
        "dedupe_key": dedupe_key,
        "source_path": source_info["path"],
        "source_name": source_info["name"],
        "source_suffix": source_info["suffix"],
        "source_fingerprint": source_info["fingerprint"],
        "source_summary": source_info["summary"],
        "source_meta": source_info["meta"],
        "analysis_result": analysis_result,
        "latest_approved_summary": latest_approved_summary,
    }
    return _save_log(payload)


def scan_topic_sources(topic_id: str) -> Dict[str, Any]:
    """扫描主题输入源并生成待审阅日志。"""
    topics = {item["id"]: item for item in load_topics()}
    topic = topics.get(topic_id)
    if not topic:
        return {"error": "未找到行业主题", "created_logs": [], "unchanged_files": []}

    frameworks = {item["id"]: item for item in load_frameworks()}
    framework = frameworks.get(topic.get("framework_id", ""))
    if not framework:
        return {"error": "行业主题未绑定分析框架", "created_logs": [], "unchanged_files": []}

    snapshot_map = _snapshot_map(topic_id=topic_id)
    created_logs: List[Dict[str, Any]] = []
    changed_files: List[Dict[str, Any]] = []
    unchanged_files: List[str] = []
    missing_paths: List[str] = []
    unreadable_files: List[str] = []
    latest_approved_log = get_latest_approved_log(topic_id)

    for source in get_sources_by_topic(topic_id):
        if not source.get("enabled", True):
            continue
        files = _iter_source_files(source)
        if not files:
            missing_paths.append(source.get("path", ""))
            continue
        for file_path in files:
            if not os.path.exists(file_path):
                missing_paths.append(file_path)
                continue
            source_info = extract_source_content(file_path)
            if source_info.get("meta", {}).get("error"):
                unreadable_files.append(file_path)
            snapshot_key = _make_snapshot_key(topic_id, file_path)
            previous = snapshot_map.get(snapshot_key)
            change_type = ""
            if previous is None:
                change_type = "new"
            elif previous.get("fingerprint") != source_info["fingerprint"]:
                change_type = "updated"
            else:
                unchanged_files.append(file_path)

            snapshot_payload = {
                "snapshot_key": snapshot_key,
                "topic_id": topic_id,
                "path": source_info["path"],
                "name": source_info["name"],
                "fingerprint": source_info["fingerprint"],
                "size": source_info["size"],
                "modified_at": source_info["modified_at"],
                "summary": source_info["summary"],
                "source_meta": source_info["meta"],
                "updated_at": _now_str(),
                "created_at": previous.get("created_at", _now_str()) if previous else _now_str(),
            }
            _upsert_snapshot(snapshot_payload)

            if change_type:
                changed_files.append(
                    {"path": source_info["path"], "name": source_info["name"], "change_type": change_type}
                )
                created_logs.append(
                    create_pending_log(
                        topic=topic,
                        framework=framework,
                        source_info=source_info,
                        change_type=change_type,
                        latest_approved_log=latest_approved_log,
                    )
                )

    return {
        "topic": topic,
        "framework": framework,
        "created_logs": created_logs,
        "changed_files": changed_files,
        "unchanged_files": unchanged_files,
        "missing_paths": missing_paths,
        "unreadable_files": unreadable_files,
        "error": "",
    }
