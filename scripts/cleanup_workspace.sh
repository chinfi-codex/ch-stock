#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/home/admin/.openclaw/workspace/projects/ch-stock"
cd "$PROJECT_DIR"

DRY_RUN="${DRY_RUN:-0}"

echo "[cleanup] project=$PROJECT_DIR dry_run=$DRY_RUN"

# safe cleanup targets only (no source deletion)
TARGETS=(
  "./__pycache__"
  "./tools/__pycache__"
  "./pages/__pycache__"
  "./*.log"
)

remove_path() {
  local p="$1"
  if [ "$DRY_RUN" = "1" ]; then
    echo "[dry-run] rm -rf $p"
  else
    rm -rf $p
    echo "[removed] $p"
  fi
}

# explicit cache dirs
for p in ./__pycache__ ./tools/__pycache__ ./pages/__pycache__; do
  [ -e "$p" ] && remove_path "$p"
done

# old temp files in project root (older than 7 days)
while IFS= read -r -d '' f; do
  remove_path "$f"
done < <(find . -maxdepth 1 -type f \( -name '*.log' -o -name '*.tmp' -o -name '*.bak' \) -mtime +7 -print0)

# stale pyc files older than 7 days
while IFS= read -r -d '' f; do
  remove_path "$f"
done < <(find . -type f -name '*.pyc' -mtime +7 -print0)

echo "[cleanup] done"
