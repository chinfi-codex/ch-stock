"""
网页爬取模块
包含网页内容获取等基础设施
"""

import logging
import os
import requests
from datetime import datetime
from infra.config import get_jina_api_key
from infra.storage import clean_filename

logger = logging.getLogger(__name__)


def scrape_with_jina_reader(
    url: str, title: str = "", output_dir: str = "", save_to_file: bool = True
) -> dict:
    """
    使用Jina Reader爬取网页内容

    Args:
        url (str): 要爬取的网页URL
        title (str): 文章标题，用于生成文件名
        output_dir (str): 输出目录，如果为空则不保存文件
        save_to_file (bool): 是否保存到文件

    Returns:
        dict: 包含爬取结果的字典
        {
            'success': bool,
            'content': str,
            'filepath': str,
            'error': str
        }
    """
    try:
        # 使用Jina Reader API
        jina_url = f"https://r.jina.ai/{url}"

        # 获取 API Key
        jina_api_key = get_jina_api_key()
        if not jina_api_key:
            result = {
                "success": False,
                "content": "",
                "filepath": "",
                "error": "Missing JINA_API_KEY: 请设置环境变量或在 .streamlit/secrets.toml 中配置",
            }
            logger.error(result["error"])
            return result

        # 设置Jina Reader的请求头
        jina_headers = {
            "Authorization": f"Bearer {jina_api_key}",
            "X-Return-Format": "markdown",
            "X-With-Images-Summary": "true",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

        logger.info(f"使用Jina Reader爬取: {url}")
        response = requests.get(jina_url, headers=jina_headers, timeout=30)
        response.raise_for_status()

        result = {"success": False, "content": "", "filepath": "", "error": ""}

        # 如果返回200，处理返回的markdown内容
        if response.status_code == 200:
            result["success"] = True
            result["content"] = response.text

            # 如果需要保存到文件
            if save_to_file and output_dir and title:
                try:
                    # 确保输出目录存在
                    os.makedirs(output_dir, exist_ok=True)

                    # 清理文件名
                    safe_title = clean_filename(title)
                    filename = f"{safe_title}.md"
                    filepath = os.path.join(output_dir, filename)

                    # 构建完整的markdown内容
                    content = []
                    content.append(f"# {title}\n")
                    content.append(f"Source: {url}\n")
                    content.append(
                        f"Scraped with Jina Reader: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    )
                    content.append(response.text)

                    # 写入文件
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write("".join(content))

                    result["filepath"] = filepath
                    logger.info(f"Jina Reader爬取成功并保存到: {filepath}")

                except Exception as e:
                    logger.error(f"保存文件失败: {e}")
                    result["error"] = f"保存文件失败: {str(e)}"
            else:
                logger.info("Jina Reader爬取成功")
        else:
            result["error"] = f"Jina Reader返回状态码: {response.status_code}"
            logger.error(f"Jina Reader返回状态码: {response.status_code}")

    except requests.exceptions.RequestException as e:
        result = {
            "success": False,
            "content": "",
            "filepath": "",
            "error": f"网络请求失败: {str(e)}",
        }
        logger.error(f"Jina Reader网络请求失败: {e}")
    except Exception as e:
        result = {
            "success": False,
            "content": "",
            "filepath": "",
            "error": f"爬取失败: {str(e)}",
        }
        logger.error(f"Jina Reader爬取失败: {e}")

    return result
