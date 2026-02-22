#!/usr/bin/env bash
set -euo pipefail

RECEIVER_OPEN_ID="${FEISHU_OPEN_ID:-ou_473b4e5cd44f97ee5be4834f15bff338}"

cd /home/admin/.openclaw/workspace/projects/ch-stock

# 1) python env
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
pyenv local 3.11.9
source .venv/bin/activate

# 2) trade-day guard (tushare trade_cal)
TRADE_DAY=$(python - <<'PY'
import os
import datetime as dt
import tushare as ts

try:
    import streamlit as st
    token = os.environ.get("TUSHARE_TOKEN") or st.secrets.get("tushare_token")
except Exception:
    token = os.environ.get("TUSHARE_TOKEN")

today = dt.datetime.now().strftime("%Y%m%d")
if not token:
    print("unknown")
else:
    try:
        pro = ts.pro_api(token)
        cal = pro.trade_cal(exchange="", start_date=today, end_date=today)
        if cal is None or cal.empty:
            print("unknown")
        else:
            print("open" if int(cal.iloc[0].get("is_open", 0)) == 1 else "closed")
    except Exception:
        print("unknown")
PY
)

if [ "$TRADE_DAY" = "closed" ]; then
  echo "Non-trading day, skip daily review job."
  exit 0
fi

# 3) run review crawl job
/home/admin/.openclaw/workspace/projects/ch-stock/run_daily_review.sh

# 4) validate saved data in local mysql
TODAY=$(date +%F)
CHECK_RESULT=$(python - <<'PY'
from tools.storage_utils import load_review_data
from datetime import datetime
import json

today = datetime.now().strftime("%Y-%m-%d")
data = load_review_data(today) or {}
integrity = data.get("integrity") or {}
ok = integrity.get("ok")
print(json.dumps({"ok": bool(data), "integrity_ok": ok, "reason": integrity.get("reason", "")}, ensure_ascii=False))
PY
)

OK=$(echo "$CHECK_RESULT" | python -c 'import sys,json; print(json.load(sys.stdin).get("ok", False))')
INTEGRITY_OK=$(echo "$CHECK_RESULT" | python -c 'import sys,json; print(json.load(sys.stdin).get("integrity_ok", None))')
REASON=$(echo "$CHECK_RESULT" | python -c 'import sys,json; print(json.load(sys.stdin).get("reason", ""))')

if [ "$OK" = "True" ]; then
  MSG="✅ ch-stock 每日06:00复盘任务完成（${TODAY}），数据已入库。完整性: ${INTEGRITY_OK} ${REASON}"
  python /home/admin/.openclaw/workspace/projects/ch-stock/send_feishu_notify.py "$RECEIVER_OPEN_ID" "$MSG"
else
  MSG="❌ ch-stock 每日06:00复盘任务失败（${TODAY}），未检测到入库数据。"
  python /home/admin/.openclaw/workspace/projects/ch-stock/send_feishu_notify.py "$RECEIVER_OPEN_ID" "$MSG"
  exit 1
fi
