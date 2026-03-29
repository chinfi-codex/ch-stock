#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Prompt 模板基础设施。
"""

from pathlib import Path

from jinja2 import Environment, FileSystemLoader


PROMPTS_DIR = Path(__file__).parent.parent / "prompts"
_jinja_env = None


class EscapedMarkdownLoader(FileSystemLoader):
    """读取模板后移除常见 Markdown 转义字符。"""

    def get_source(self, environment, template):
        source, filename, uptodate = super().get_source(environment, template)
        source = source.replace("\\-", "-")
        source = source.replace("\\*", "*")
        source = source.replace("\\_", "_")
        source = source.replace("\\#", "#")
        source = source.replace("\\>", ">")
        source = source.replace("\\<", "<")
        source = source.replace("\\`", "`")
        source = source.replace("\\[", "[")
        source = source.replace("\\]", "]")
        source = source.replace("\\(", "(")
        source = source.replace("\\)", ")")
        return source, filename, uptodate


def get_jinja_env():
    """获取 prompt 模板环境。"""
    global _jinja_env
    if _jinja_env is None:
        if not PROMPTS_DIR.exists():
            raise FileNotFoundError(f"Prompts directory not found: {PROMPTS_DIR}")
        _jinja_env = Environment(
            loader=EscapedMarkdownLoader(str(PROMPTS_DIR)),
            trim_blocks=True,
            lstrip_blocks=True,
        )
    return _jinja_env


def load_prompt_template(template_name: str) -> str:
    """直接读取并渲染模板。"""
    env = get_jinja_env()
    template = env.get_template(template_name)
    return template.render()
