#!/usr/bin/env python3
import json
import sys
import urllib.request

CONFIG_PATH = "/home/admin/.openclaw/openclaw.json"


def load_feishu_app():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    ch = (cfg.get("channels") or {}).get("feishu") or {}
    app_id = ch.get("appId")
    app_secret = ch.get("appSecret")
    if not app_id or not app_secret:
        raise RuntimeError("Feishu appId/appSecret not found in openclaw config")
    return app_id, app_secret


def http_json(method, url, payload=None, headers=None):
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    if len(sys.argv) < 3:
        print("Usage: send_feishu_notify.py <open_id> <message>")
        sys.exit(2)

    open_id = sys.argv[1]
    message = sys.argv[2]
    app_id, app_secret = load_feishu_app()

    token_resp = http_json(
        "POST",
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        {"app_id": app_id, "app_secret": app_secret},
        {"Content-Type": "application/json; charset=utf-8"},
    )
    if token_resp.get("code") != 0:
        raise RuntimeError(f"get token failed: {token_resp}")

    token = token_resp.get("tenant_access_token")
    msg_resp = http_json(
        "POST",
        "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id",
        {
            "receive_id": open_id,
            "msg_type": "text",
            "content": json.dumps({"text": message}, ensure_ascii=False),
        },
        {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    if msg_resp.get("code") != 0:
        raise RuntimeError(f"send message failed: {msg_resp}")


if __name__ == "__main__":
    main()
